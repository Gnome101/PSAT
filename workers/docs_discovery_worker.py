"""
DocsDiscoveryWorker
===================
Poll-based worker that runs the two-phase docs discovery pipeline.

Inherits from BaseWorker — the base class owns the poll loop, job claiming,
failure handling, and stage advancement. This worker only implements process().

Phase 1 — Discovery (Tavily + OpenRouter)
  Runs only when the SeedManifest is missing or stale (> 7 days old).
  Produces a SeedManifest with typed seed URLs and persists it to object storage.
  Also produces DiscoveredDocuments from Tavily content, stored as a job artifact
  and written individually to object storage for cross-run change detection.

Phase 2 — Deep crawl
  Reads the SeedManifest and runs the appropriate crawler per seed sequentially.
  Compares each doc's content_hash against the stored record; only writes changed docs.
  Phase 2 runs on every process() call — it is cheap (custom crawlers, no API cost).
  Phase 1 is guarded by the staleness check so it only incurs API cost once per 7 days.

NOTE: Add JobStage.docs_discovery to db/models.py before deploying this worker.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re

from sqlalchemy.orm import Session

from db.models import Job, JobStage
from db.queue import store_artifact
from db.storage import JSON_CONTENT_TYPE, StorageKeyMissing, get_storage_client
from services.crawlers.registry import get_crawler
from services.discovery.docs.models import DiscoveredDocument, SeedManifest
from services.discovery.docs.pipeline import DocsDiscoveryPipeline
from services.discovery.docs.seed_manifest import SeedManifestStore
from workers.base import BaseWorker

logger = logging.getLogger(__name__)

# How old a SeedManifest can be before Phase 1 re-runs (in days)
MANIFEST_MAX_AGE_DAYS = 7

# S3 key prefix for per-document storage (change detection + retrieval)
DOCS_KEY_PREFIX = "docs"


class DocsDiscoveryWorker(BaseWorker):
    """
    Poll-based worker for the two-phase docs discovery pipeline.

    NOTE: Add JobStage.docs_discovery to db/models.py JobStage enum.
    Adjust next_stage if docs discovery feeds into a downstream pipeline stage.
    """

    stage = JobStage.docs_discovery       # add to db/models.py
    next_stage = JobStage.done            # adjust if there is a downstream stage

    # ------------------------------------------------------------------
    # BaseWorker interface
    # ------------------------------------------------------------------

    def process(self, session: Session, job: Job) -> None:
        """
        Full two-phase docs discovery pipeline for one protocol.

        Called synchronously by BaseWorker.run_loop(). BaseWorker handles
        advance_job / fail_job / timing recording after this returns.

        job.name    → protocol_name    (e.g. "Uniswap")
        job.address → contract_address (e.g. "0x1f98...")
        """
        protocol_name = job.name or ""
        contract_address = job.address or ""
        protocol_id = _slugify(protocol_name)

        if not protocol_name:
            raise ValueError(f"Job {job.id} has no protocol name (job.name is empty)")

        logger.info(
            "[docs_worker] Starting for %s (%s) job=%s",
            protocol_name, contract_address, job.id,
        )

        # ── Phase 1: Discovery (conditional on manifest freshness) ──────────

        manifest_store = SeedManifestStore()
        is_stale = manifest_store.is_stale(protocol_id, MANIFEST_MAX_AGE_DAYS)

        if is_stale:
            self.update_detail(session, job, f"Running discovery pipeline for {protocol_name}")
            logger.info("[docs_worker] Manifest stale/missing — running discovery phase")

            pipeline = DocsDiscoveryPipeline()
            manifest, documents = pipeline.run(protocol_name, contract_address)

            # Store manifest + documents as job artifacts for the audit trail.
            store_artifact(
                session, job.id, "docs_manifest",
                data=manifest.model_dump(mode="json"),
            )
            if documents:
                store_artifact(
                    session, job.id, "discovered_documents",
                    data=[doc.model_dump(mode="json") for doc in documents],
                )

            # Write each document to object storage for cross-run change detection.
            self._persist_documents(documents, protocol_id)

            logger.info(
                "[docs_worker] Phase 1 complete — %d seeds, %d documents",
                len(manifest.seeds), len(documents),
            )

        else:
            self.update_detail(session, job, f"Using cached manifest for {protocol_name}")
            logger.info("[docs_worker] Manifest is fresh — skipping discovery phase")
            manifest = manifest_store.load(protocol_id)
            if manifest is None:
                # is_stale() returned False but load() found nothing — storage inconsistency
                raise RuntimeError(
                    f"Manifest reported fresh but not found in storage for {protocol_id}"
                )

        # ── Phase 2: Deep crawl ─────────────────────────────────────────────

        self.update_detail(
            session, job,
            f"Crawling {len(manifest.seeds)} seeds for {protocol_name}",
        )
        docs_found, docs_changed = self._run_crawlers(manifest, protocol_id)

        store_artifact(
            session, job.id, "docs_crawl_summary",
            data={
                "protocol": protocol_name,
                "seeds": len(manifest.seeds),
                "docs_found": docs_found,
                "docs_changed": docs_changed,
            },
        )

        logger.info(
            "[docs_worker] Complete for %s: seeds=%d found=%d changed=%d",
            protocol_name, len(manifest.seeds), docs_found, docs_changed,
        )

    # ------------------------------------------------------------------
    # Phase 2 — Crawlers
    # ------------------------------------------------------------------

    def _run_crawlers(
        self, manifest: SeedManifest, protocol_id: str
    ) -> tuple[int, int]:
        """
        Runs the registered crawler for each seed in the manifest sequentially,
        highest-confidence seeds first.
        Returns (total_docs_found, total_docs_changed).

        Per-seed errors are caught and logged — one failing seed does not
        abort the rest.
        """
        seeds = sorted(manifest.seeds, key=lambda s: s.confidence, reverse=True)
        total_found = 0
        total_changed = 0

        for seed in seeds:
            try:
                crawler = get_crawler(seed)
                docs: list[DiscoveredDocument] = crawler.crawl(seed.url)

                changed_docs = [doc for doc in docs if self._has_changed(doc, protocol_id)]
                if changed_docs:
                    self._persist_documents(changed_docs, protocol_id)

                total_found += len(docs)
                total_changed += len(changed_docs)

                logger.info(
                    "[docs_worker] Crawled %s (%s): %d found, %d changed",
                    seed.url, seed.seed_type, len(docs), len(changed_docs),
                )

            except Exception as exc:
                logger.error(
                    "[docs_worker] Crawl failed for %s (%s): %s",
                    seed.url, seed.seed_type, exc, exc_info=True,
                )
                # Continue — do not let one bad seed abort the others

        return total_found, total_changed

    # ------------------------------------------------------------------
    # Change detection and document persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _doc_key(protocol_id: str, url: str) -> str:
        """
        S3 key for a single discovered document.
        URL is SHA-256 hashed so arbitrary URLs become safe key components.
        """
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:24]
        return f"{DOCS_KEY_PREFIX}/{protocol_id}/{url_hash}.json"

    def _has_changed(self, doc: DiscoveredDocument, protocol_id: str) -> bool:
        """
        Returns True if the document is new or its content_hash differs
        from the record already in object storage.

        On any storage error, returns True conservatively — better to
        re-persist than to silently discard changed content.
        """
        storage = get_storage_client()
        if storage is None:
            return True  # no storage configured; always treat as new

        key = self._doc_key(protocol_id, doc.source_url)
        try:
            body = storage.get(key)
            existing = json.loads(body.decode("utf-8"))
            return existing.get("content_hash") != doc.content_hash
        except StorageKeyMissing:
            return True  # net-new document
        except Exception as exc:
            logger.warning(
                "[docs_worker] Hash lookup failed for %s — treating as changed: %s",
                doc.source_url, exc,
            )
            return True

    def _persist_documents(
        self, documents: list[DiscoveredDocument], protocol_id: str
    ) -> None:
        """
        Writes each document to object storage keyed by protocol + URL hash.
        Failures are per-document warnings — one bad write does not lose the rest.
        """
        storage = get_storage_client()
        if storage is None:
            logger.warning("[docs_worker] No storage client configured — documents not persisted")
            return

        persisted = 0
        for doc in documents:
            try:
                key = self._doc_key(protocol_id, doc.source_url)
                body = json.dumps(doc.model_dump(mode="json")).encode("utf-8")
                storage.put(key, body, JSON_CONTENT_TYPE)
                persisted += 1
            except Exception as exc:
                logger.warning(
                    "[docs_worker] Failed to persist document %s: %s",
                    doc.source_url, exc,
                )

        logger.debug("[docs_worker] Persisted %d/%d documents", persisted, len(documents))


# ---------------------------------------------------------------------------
# Scheduler cadence constants
# ---------------------------------------------------------------------------

class DocsDiscoverySchedule:
    """
    Reference cadence values for integrating with the scheduler.

    Phase 1 (Tavily + OpenRouter) is guarded by SeedManifestStore.is_stale()
    inside process() — jobs can be enqueued on the Phase 2 schedule and Phase 1
    will self-throttle based on manifest age.

    Phase 1 has API cost — enqueue infrequently.
    Phase 2 is free (custom crawlers) — enqueue frequently for freshness.
    """
    DISCOVERY_INTERVAL_DAYS = 7    # mirrors MANIFEST_MAX_AGE_DAYS above
    CRAWL_INTERVAL_HOURS = 6

    DOCS_CRAWL_INTERVAL_HOURS = 24
    GITHUB_CRAWL_INTERVAL_HOURS = 6
    GOVERNANCE_CRAWL_INTERVAL_HOURS = 1


def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
