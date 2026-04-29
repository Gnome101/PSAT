"""
DocsDiscoveryPipeline
=====================
Wires DocsSearcher, DocsExtractor, and SeedManifestBuilder into a single
callable that takes a protocol name + address and returns a SeedManifest.

Uses services/discovery/protocol_resolver.resolve_protocol() to get the
DeFiLlama URL as a fallback seed before running Tavily searches.

Note on sync:
  utils/tavily.py and utils/llm.py are both synchronous (requests, not httpx).
  This pipeline is therefore also synchronous and is called directly from
  DocsDiscoveryWorker.process(), which itself runs synchronously under BaseWorker.
"""
from __future__ import annotations

import hashlib
import logging
import re
from typing import Any

from services.discovery.protocol_resolver import resolve_protocol
from services.discovery.docs.extractor import DocsExtractor
from services.discovery.docs.models import (
    DiscoveredDocument,
    ExtractedSignals,
    SearchResult,
    SeedManifest,
)
from services.discovery.docs.searcher import DocsSearcher
from services.discovery.docs.seed_manifest import SeedManifestBuilder, SeedManifestStore

logger = logging.getLogger(__name__)

# Cost control — max extraction calls per pipeline run
MAX_EXTRACTIONS = 15
MIN_RELEVANCE_CONFIDENCE = 0.7


class DocsDiscoveryPipeline:
    """
    Full discovery pipeline for one protocol.
    Synchronous — called directly from DocsDiscoveryWorker.process().
    """

    def __init__(self) -> None:
        self.searcher = DocsSearcher()
        self.extractor = DocsExtractor()
        self.manifest_builder = SeedManifestBuilder()
        self.manifest_store = SeedManifestStore()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self, protocol_name: str, contract_address: str
    ) -> tuple[SeedManifest, list[DiscoveredDocument]]:
        """
        Full pipeline. Returns (SeedManifest, list[DiscoveredDocument]).

        The SeedManifest is consumed by DocsDiscoveryWorker to feed the crawler
        suite. The DiscoveredDocuments are returned to the worker for persistence
        as job artifacts and for cross-run change detection in object storage.
        """
        logger.info("[docs_discovery] Starting pipeline for %s", protocol_name)

        # 1. Resolve protocol via DeFiLlama for a fallback URL
        defillama = self._resolve_defillama(protocol_name)
        defillama_url = defillama.get("url")

        # 2. Run all three Tavily searches
        results = self._search_all(protocol_name)
        logger.info("[docs_discovery] %d raw results from Tavily", len(results))

        # 3. Deduplicate by URL, keep highest score
        results = self._deduplicate(results)

        # 4. Relevance filter + full extraction
        documents = self._filter_and_extract(results, protocol_name)
        logger.info("[docs_discovery] %d documents after extraction", len(documents))

        # 5. Build SeedManifest
        protocol_id = _slugify(protocol_name)
        manifest = self.manifest_builder.build(
            results=results,
            protocol_id=protocol_id,
            protocol_name=protocol_name,
            contract_address=contract_address,
            defillama_url=defillama_url,
        )

        # 6. Persist manifest to object storage (keyed by protocol_id, not job_id)
        self.manifest_store.save(manifest)

        logger.info(
            "[docs_discovery] Complete — %d seeds, %d documents",
            len(manifest.seeds), len(documents),
        )
        return manifest, documents

    # ------------------------------------------------------------------
    # Step 1 — DeFiLlama resolution
    # ------------------------------------------------------------------

    def _resolve_defillama(self, protocol_name: str) -> dict[str, Any]:
        """
        Calls services/discovery/protocol_resolver.resolve_protocol() to get the
        DeFiLlama-registered URL and slug for the protocol.

        resolve_protocol() takes a name (not an address) and handles
        fuzzy matching internally — no pre-processing needed here.

        Returns the full result dict; callers use result.get("url").
        On failure returns an empty dict so the pipeline continues.
        """
        try:
            result = resolve_protocol(protocol_name)
            if result.get("url"):
                logger.info(
                    "[docs_discovery] DeFiLlama resolved %s → %s",
                    protocol_name, result["url"],
                )
            return result
        except Exception as exc:
            logger.warning("[docs_discovery] DeFiLlama resolution failed: %s", exc)
            return {}

    # ------------------------------------------------------------------
    # Step 2 — Tavily searches
    # ------------------------------------------------------------------

    def _search_all(self, protocol_name: str) -> list[SearchResult]:
        """
        Runs all three search methods sequentially.
        Each method catches TavilyError internally and returns [] on failure,
        so one bad search doesn't abort the others.
        Flattens results into a single list.
        """
        docs_results       = self.searcher.search_docs(protocol_name)
        github_results     = self.searcher.search_github(protocol_name)
        governance_results = self.searcher.search_governance(protocol_name)

        all_results = docs_results + github_results + governance_results

        logger.debug(
            "[docs_discovery] _search_all for %r: docs=%d github=%d governance=%d total=%d",
            protocol_name,
            len(docs_results),
            len(github_results),
            len(governance_results),
            len(all_results),
        )
        return all_results

    # ------------------------------------------------------------------
    # Step 3 — Deduplication
    # ------------------------------------------------------------------

    def _deduplicate(self, results: list[SearchResult]) -> list[SearchResult]:
        """
        Deduplicates by URL, keeping the highest-score entry per URL.
        Preserves the source_type of the winner.
        """
        best: dict[str, SearchResult] = {}
        for result in results:
            existing = best.get(result.url)
            if existing is None or result.score > existing.score:
                best[result.url] = result

        deduped = list(best.values())
        dropped = len(results) - len(deduped)
        if dropped:
            logger.debug(
                "[docs_discovery] Dedup: %d → %d results (%d duplicates removed)",
                len(results), len(deduped), dropped,
            )
        return deduped

    # ------------------------------------------------------------------
    # Step 4 — Filter and extract
    # ------------------------------------------------------------------

    def _filter_and_extract(
        self, results: list[SearchResult], protocol_name: str
    ) -> list[DiscoveredDocument]:
        """
        Two-stage LLM processing:
          1. Cheap relevance check  → drop irrelevant results
          2. Full extraction        → only on results that pass (up to MAX_EXTRACTIONS)

        Enforces MAX_EXTRACTIONS as a hard cost cap on the expensive model.
        Relevance checks (cheap classifier model) are not capped.
        """
        protocol_id = _slugify(protocol_name)
        documents: list[DiscoveredDocument] = []
        extraction_count = 0
        relevance_passed = 0

        for result in results:
            # Stage 1: relevance check (cheap classifier model)
            check = self.extractor.is_relevant(result.content, protocol_name)
            if not check.is_relevant:
                logger.debug(
                    "[docs_discovery] Skipping %s — not relevant (%s)",
                    result.url, check.reason,
                )
                continue
            if check.confidence < MIN_RELEVANCE_CONFIDENCE:
                logger.debug(
                    "[docs_discovery] Skipping %s — low confidence %.2f < %.2f (%s)",
                    result.url, check.confidence, MIN_RELEVANCE_CONFIDENCE, check.reason,
                )
                continue

            relevance_passed += 1

            # Hard cost cap — checked after relevance so classifier calls
            # are never wasted on results that would fail extraction anyway.
            if extraction_count >= MAX_EXTRACTIONS:
                logger.warning(
                    "[docs_discovery] Extraction cap (%d) reached — skipping remaining results",
                    MAX_EXTRACTIONS,
                )
                break

            # Stage 2: full structured extraction (capable extractor model)
            signals = self.extractor.extract_signals(result.content, result.source_type)
            document = self._build_document(result, signals, protocol_id)
            documents.append(document)
            extraction_count += 1

            logger.debug(
                "[docs_discovery] Extracted %s (type=%s, security_relevant=%s) [%d/%d]",
                result.url,
                signals.doc_type,
                signals.is_security_relevant,
                extraction_count,
                MAX_EXTRACTIONS,
            )

        logger.info(
            "[docs_discovery] _filter_and_extract: %d/%d passed relevance, "
            "%d extracted (cap=%d)",
            relevance_passed, len(results), extraction_count, MAX_EXTRACTIONS,
        )
        return documents

    def _build_document(
        self,
        result: SearchResult,
        signals: ExtractedSignals,
        protocol_id: str,
    ) -> DiscoveredDocument:
        """
        Assembles a DiscoveredDocument from a SearchResult and its signals.
        SHA-256 hashes the raw content for cross-run change detection.
        """
        content_hash = hashlib.sha256(result.content.encode()).hexdigest()

        return DiscoveredDocument(
            protocol_id=protocol_id,
            source_url=result.url,
            doc_type=signals.doc_type,
            raw_text=result.content,
            content_hash=content_hash,
            tavily_score=result.score,
            signals=signals,
        )


def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
