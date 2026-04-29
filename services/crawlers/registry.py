"""
Crawler Registry
================
Single lookup table mapping seed_type strings (produced by the discovery
pipeline) to their corresponding crawler classes.

seed_type strings here must match exactly what SeedManifestBuilder writes
into DiscoveredSeed.seed_type in services/discovery/docs/seed_manifest.py.

Adding a new crawler:
  1. Write the crawler class in services/crawlers/
  2. Add one entry to REGISTRY below
  Nothing else needs to change.
"""
from __future__ import annotations

import logging
from services.discovery.docs.models import DiscoveredSeed

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Import crawler classes
#
# Each import is wrapped in its own try/except so the registry module loads
# cleanly even before a crawler subpackage has been written. A missing class
# is set to None; get_crawler() raises ValueError for unresolvable seed types,
# and registered_types() silently omits them — making incremental rollout safe.
# Replace the import paths if the crawlers land somewhere other than the paths
# shown below.
# ---------------------------------------------------------------------------

try:
    from services.crawlers.docs_crawler import DocsSiteCrawler
except ImportError:
    logger.debug("[registry] DocsSiteCrawler not yet available — 'docs_site' unregistered")
    DocsSiteCrawler = None  # type: ignore[assignment,misc]

try:
    from services.crawlers.github_crawler import GitHubCrawler
except ImportError:
    logger.debug("[registry] GitHubCrawler not yet available — 'github_org' unregistered")
    GitHubCrawler = None  # type: ignore[assignment,misc]

try:
    from services.crawlers.governance_crawler import GovernanceCrawler
except ImportError:
    logger.debug("[registry] GovernanceCrawler not yet available — 'governance' unregistered")
    GovernanceCrawler = None  # type: ignore[assignment,misc]


REGISTRY: dict[str, type | None] = {
    "docs_site":  DocsSiteCrawler,
    "github_org": GitHubCrawler,
    "governance": GovernanceCrawler,
}


def get_crawler(seed: DiscoveredSeed):
    """
    Returns an instantiated crawler for the given seed.

    Raises ValueError if:
      - seed.seed_type is not a key in REGISTRY (unknown type — pipeline bug)
      - seed.seed_type is registered but the crawler class failed to import
        (crawler not yet implemented — development-time error)

    Both cases are programming errors rather than runtime data errors, so
    ValueError is appropriate and the caller (the worker) logs and skips.
    """
    # Distinguish "unknown seed_type" from "crawler not yet implemented"
    # so the error message is actionable.
    if seed.seed_type not in REGISTRY:
        raise ValueError(
            f"Unknown seed_type '{seed.seed_type}'. "
            f"Known types: {list(REGISTRY.keys())}. "
            f"Add an entry to REGISTRY in services/crawlers/registry.py."
        )

    crawler_cls = REGISTRY[seed.seed_type]
    if crawler_cls is None:
        raise ValueError(
            f"Crawler for seed_type '{seed.seed_type}' is registered but not yet "
            f"implemented (import failed at module load). "
            f"Create the crawler class and re-check the import path in registry.py."
        )

    logger.debug(
        "[registry] %s → %s for %s",
        seed.seed_type, crawler_cls.__name__, seed.url,
    )
    return crawler_cls(protocol_id=seed.protocol_id)


def registered_types() -> list[str]:
    """
    Returns seed_type keys whose crawler class imported successfully.
    Useful for validation in tests and for health-check endpoints.
    """
    return [k for k, v in REGISTRY.items() if v is not None]
