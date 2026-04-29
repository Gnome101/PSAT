"""
DocsSearcher
============
PSAT-specific search queries built on top of utils.tavily.search().

utils/tavily.py already handles:
  - Auth (TAVILY_API_KEY from .env)
  - Retry with exponential backoff
  - Error normalisation via TavilyError

This module only adds:
  - Purpose-built queries per content category
  - Domain scoping per category
  - Normalisation of raw Tavily dicts → SearchResult models
  - Score threshold filtering
"""
from __future__ import annotations

import logging
from urllib.parse import urlparse
from typing import Any

from utils.tavily import search, TavilyError
from services.discovery.docs.models import SearchResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------

MAX_RESULTS_PER_SEARCH = 5
MIN_SCORE_THRESHOLD = 0.6       # drop results below this before any LLM calls
SEARCH_DEPTH = "advanced"       # "advanced" fetches full page text via Tavily

GOVERNANCE_DOMAINS = [
    "snapshot.org",
    "tally.xyz",
    "commonwealth.im",
    "gov.uniswap.org",
]


class DocsSearcher:
    """
    Runs the three category-specific Tavily searches for a protocol.
    Each method returns a list of SearchResult, already score-filtered.
    """

    def search_docs(self, protocol_name: str) -> list[SearchResult]:
        """
        Finds the protocol's official documentation site.
        Broad query — no domain filter — to surface the primary docs URL
        regardless of where it's hosted (GitBook, Docusaurus, Notion, etc.).
        """
        query = f"{protocol_name} DeFi protocol official documentation site"
        return self._run_search(query, source_type="docs")

    def search_github(self, protocol_name: str) -> list[SearchResult]:
        """
        Finds the protocol's GitHub organisation, SECURITY.md files,
        and audit directories. Scoped to github.com only.
        """
        query = f"{protocol_name} smart contracts github repository security"
        return self._run_search(
            query,
            source_type="github",
            include_domains=["github.com"],
        )

    def search_governance(self, protocol_name: str) -> list[SearchResult]:
        """
        Finds governance forums, Snapshot spaces, and Tally governors.
        Scoped to GOVERNANCE_DOMAINS.
        """
        query = f"{protocol_name} governance proposal forum"
        return self._run_search(
            query,
            source_type="governance",
            include_domains=GOVERNANCE_DOMAINS,
        )

    # ------------------------------------------------------------------
    # Private base method
    # ------------------------------------------------------------------

    def _run_search(
        self,
        query: str,
        source_type: str,
        include_domains: list[str] | None = None,
    ) -> list[SearchResult]:
        """
        Thin wrapper around utils.tavily.search().

        utils/tavily.py does not support include_domains natively so domain
        filtering is applied as a post-filter on the returned URLs. This means
        we always fetch MAX_RESULTS_PER_SEARCH and filter down, so the final
        list may be shorter than requested — that's acceptable.
        """
        try:
            raw_results: list[dict[str, Any]] = search(
                query,
                max_results=MAX_RESULTS_PER_SEARCH,
                search_depth=SEARCH_DEPTH,
                include_raw_content=True,
            )
        except TavilyError as exc:
            logger.warning(
                "[searcher] Tavily search failed for query %r (source=%s): %s",
                query[:80],
                source_type,
                exc,
            )
            return []

        # Post-filter by domain if a whitelist was provided
        if include_domains:
            raw_results = [
                r for r in raw_results
                if _url_matches_any_domain(r.get("url", ""), include_domains)
            ]

        # Drop results below the score threshold before any LLM calls
        before = len(raw_results)
        raw_results = [
            r for r in raw_results
            if isinstance(r.get("score"), (int, float))
            and r["score"] >= MIN_SCORE_THRESHOLD
        ]
        dropped = before - len(raw_results)
        if dropped:
            logger.debug(
                "[searcher] Dropped %d/%d results below score threshold %.2f (source=%s)",
                dropped, before, MIN_SCORE_THRESHOLD, source_type,
            )

        results = [self._normalise(r, source_type, query) for r in raw_results]

        logger.info(
            "[searcher] %s search for %r → %d results",
            source_type, query[:60], len(results),
        )
        return results

    def _normalise(
        self, raw: dict[str, Any], source_type: str, query: str
    ) -> SearchResult:
        """
        Maps a raw Tavily result dict to a SearchResult.

        Tavily returns two text fields:
          raw["raw_content"] — full extracted page text (populated when
                               include_raw_content=True, may still be None
                               if Tavily couldn't extract the page)
          raw["content"]     — short snippet, always present

        We prefer raw_content because it gives the LLM the full page to
        work with. The snippet is only used as a fallback.
        """
        content = raw.get("raw_content") or raw.get("content") or ""
        return SearchResult(
            url=raw.get("url", ""),
            content=content,
            score=float(raw.get("score", 0.0)),
            source_type=source_type,
            query_used=query,
        )


# ---------------------------------------------------------------------------
# URL domain helper
# ---------------------------------------------------------------------------

def _url_matches_any_domain(url: str, domains: list[str]) -> bool:
    """
    Returns True if the URL's netloc ends with any of the given domains.
    Handles both bare domains ("github.com") and subdomains
    ("docs.github.com" matches "github.com").
    """
    try:
        netloc = urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return False
    return any(netloc == d or netloc.endswith(f".{d}") for d in domains)
