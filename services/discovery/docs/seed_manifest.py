"""
SeedManifest
============
Converts relevance-filtered SearchResults into typed DiscoveredSeeds
and packages them into a SeedManifest that the crawler worker reads.

Key design:
  - SeedManifestBuilder  → pure logic, no I/O, easily testable
  - SeedManifestStore    → persists/loads manifests via object storage (db/storage.py)

The seed_type strings produced here must match the keys registered
in services/crawlers/registry.py.

Note on protocol_resolver:
  services/discovery/protocol_resolver.resolve_protocol(name) already resolves a
  protocol name → DeFiLlama slug + URL. The pipeline calls this before
  building the manifest so the website URL is available for docs seeding
  even if Tavily didn't surface it directly.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse

from utils.github_urls import github_blob_to_raw  # noqa: F401 — available if needed
from db.storage import JSON_CONTENT_TYPE, StorageKeyMissing, get_storage_client
from services.discovery.docs.models import (
    DiscoveredSeed,
    SearchResult,
    SeedManifest,
)

logger = logging.getLogger(__name__)

# Maps Tavily source_type → crawler seed_type
# Must stay in sync with services/crawlers/registry.py
SOURCE_TO_SEED_TYPE: dict[str, str] = {
    "docs":       "docs_site",
    "github":     "github_org",
    "governance": "governance",
}

# S3 key prefix for persisted manifests
MANIFEST_KEY_PREFIX = "manifests"


class SeedManifestBuilder:
    """
    Pure conversion logic — no I/O, fully unit-testable.
    Takes relevance-filtered SearchResults and produces a SeedManifest.
    """

    def build(
        self,
        results: list[SearchResult],
        protocol_id: str,
        protocol_name: str,
        contract_address: str,
        defillama_url: str | None = None,
    ) -> SeedManifest:
        """
        Converts SearchResults → DiscoveredSeeds → SeedManifest.

        If defillama_url is provided and no docs seed was discovered via
        Tavily, a seed is synthesised from the DeFiLlama URL as a fallback
        so the docs crawler always has at least one starting point.

        Deduplicates by URL, keeping the highest-confidence entry.
        Sorts seeds by confidence descending.
        """
        # Map each result to a DiscoveredSeed
        seeds: list[DiscoveredSeed] = [self._to_seed(r, protocol_id) for r in results]

        # Synthesise a DeFiLlama fallback if no docs_site seed came from Tavily
        has_docs_seed = any(s.seed_type == "docs_site" for s in seeds)
        if defillama_url and not has_docs_seed:
            logger.info(
                "[seed_manifest] No docs_site seed found via Tavily — "
                "synthesising fallback from DeFiLlama URL: %s",
                defillama_url,
            )
            seeds.append(
                DiscoveredSeed(
                    url=defillama_url,
                    seed_type="docs_site",
                    confidence=0.5,
                    protocol_id=protocol_id,
                    metadata={"source": "defillama"},
                )
            )

        # Deduplicate by URL — keep highest confidence per URL
        best: dict[str, DiscoveredSeed] = {}
        for seed in seeds:
            existing = best.get(seed.url)
            if existing is None or seed.confidence > existing.confidence:
                best[seed.url] = seed

        # Sort by confidence descending so the worker crawls most-trusted sources first
        deduped = sorted(best.values(), key=lambda s: s.confidence, reverse=True)

        logger.info(
            "[seed_manifest] Built manifest for %s: %d seeds (%d after dedup)",
            protocol_id, len(seeds), len(deduped),
        )

        return SeedManifest(
            protocol_id=protocol_id,
            protocol_name=protocol_name,
            contract_address=contract_address,
            seeds=deduped,
        )

    def _to_seed(self, result: SearchResult, protocol_id: str) -> DiscoveredSeed:
        """
        Maps one SearchResult to a DiscoveredSeed.

        For github seeds: normalise URL to the org root so GitHubCrawler
        receives the right entry point.
          e.g. https://github.com/Uniswap/v3-core/blob/main/SECURITY.md
            →  https://github.com/Uniswap

        For all seeds: look up seed_type from SOURCE_TO_SEED_TYPE.
        Fall back to "docs_site" for unknown source types.
        """
        seed_type = SOURCE_TO_SEED_TYPE.get(result.source_type, "docs_site")
        url = result.url

        # Normalise GitHub URLs to the org root — GitHubCrawler needs the
        # org entry point, not a specific repo or file path.
        if seed_type == "github_org":
            url = _extract_github_org(result.url)

        return DiscoveredSeed(
            url=url,
            seed_type=seed_type,
            confidence=result.score,
            protocol_id=protocol_id,
        )


# ---------------------------------------------------------------------------
# Object-storage persistence
# ---------------------------------------------------------------------------

class SeedManifestStore:
    """
    Persists and loads SeedManifest objects via the configured object storage
    backend (db/storage.py — Tigris in prod, minio in dev/test).

    Keys are scoped by protocol_id so manifests survive across job runs
    and can be loaded for the staleness check before a new job claims the work.

    If no storage client is configured (ARTIFACT_STORAGE_* env vars not set),
    save() is a no-op and load() always returns None — meaning is_stale()
    always returns True and Phase 1 runs on every job. This is safe for local
    development where storage is not available.
    """

    def _key(self, protocol_id: str) -> str:
        return f"{MANIFEST_KEY_PREFIX}/{protocol_id}.json"

    def save(self, manifest: SeedManifest) -> None:
        """
        Writes the manifest to object storage, keyed by protocol_id.
        Re-running discovery for the same protocol overwrites the previous manifest.
        """
        storage = get_storage_client()
        if storage is None:
            logger.warning(
                "[seed_manifest] No storage client configured — manifest for %s not persisted",
                manifest.protocol_id,
            )
            return

        key = self._key(manifest.protocol_id)
        body = json.dumps(manifest.model_dump(mode="json")).encode("utf-8")
        storage.put(key, body, JSON_CONTENT_TYPE)
        logger.info(
            "[seed_manifest] Saved manifest for %s (%d seeds) → %s",
            manifest.protocol_id, len(manifest.seeds), key,
        )

    def load(self, protocol_id: str) -> SeedManifest | None:
        """
        Loads the manifest for a protocol from object storage.
        Returns None if no manifest exists (first run) or storage is unavailable.
        """
        storage = get_storage_client()
        if storage is None:
            return None

        key = self._key(protocol_id)
        try:
            body = storage.get(key)
            raw = json.loads(body.decode("utf-8"))
            return SeedManifest.model_validate(raw)
        except StorageKeyMissing:
            logger.debug("[seed_manifest] No manifest found for %s", protocol_id)
            return None
        except Exception as exc:
            logger.error(
                "[seed_manifest] Failed to deserialise manifest for %s: %s",
                protocol_id, exc,
            )
            return None

    def is_stale(self, protocol_id: str, max_age_days: int = 7) -> bool:
        """
        Returns True if the manifest is older than max_age_days OR does not exist.
        Used by the worker to decide whether to re-run the discovery phase.
        """
        manifest = self.load(protocol_id)
        if manifest is None:
            return True

        age = datetime.utcnow() - manifest.created_at
        stale = age > timedelta(days=max_age_days)
        if stale:
            logger.debug(
                "[seed_manifest] Manifest for %s is stale (age=%s, max=%d days)",
                protocol_id, age, max_age_days,
            )
        return stale


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def _extract_github_org(url: str) -> str:
    """
    Strips repo name and file path from a GitHub URL, returning the org root.

    Examples:
      https://github.com/Uniswap/v3-core              → https://github.com/Uniswap
      https://github.com/Uniswap/v3-core/blob/main/X  → https://github.com/Uniswap
      https://github.com/Uniswap                       → https://github.com/Uniswap

    Note: utils/github_urls.py only converts blob→raw URLs.
    Org extraction is not in that module so it lives here.
    """
    parsed = urlparse(url)
    # Path segments after splitting: ["Uniswap", "v3-core", "blob", ...]
    # We want only the first non-empty segment (the org name).
    segments = [s for s in parsed.path.split("/") if s]
    if not segments:
        logger.warning("[seed_manifest] Could not extract org from GitHub URL: %s", url)
        return url

    org = segments[0]
    return f"{parsed.scheme}://{parsed.netloc}/{org}"
