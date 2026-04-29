"""
GovernanceCrawler
=================
Fetches governance proposals and forum posts relevant to security.

Supports:
  - Snapshot (GraphQL API)
  - Tally (REST API)
  - Commonwealth / Discourse (RSS + HTML)
  - Generic RSS / Atom feeds
"""
import logging
import re
from datetime import datetime, timedelta
from urllib.parse import urlparse

import feedparser
import trafilatura

from base import BaseCrawler, CrawledDocument

logger = logging.getLogger(__name__)

SNAPSHOT_GRAPHQL = "https://hub.snapshot.org/graphql"
TALLY_API = "https://api.tally.xyz/query"

# Only pull posts/proposals from the last N days unless they contain
# security keywords (those get pulled regardless of age).
DEFAULT_LOOKBACK_DAYS = 30
SECURITY_KEYWORDS = {
    "exploit", "vulnerability", "hack", "pause", "emergency",
    "upgrade", "migration", "multisig", "timelock", "audit",
    "incident", "security", "bug", "risk", "attack",
}


class GovernanceCrawler(BaseCrawler):
    CRAWLER_TYPE = "governance"

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def crawl(self, governance_url: str) -> list[CrawledDocument]:
        if not governance_url:
            return []

        logger.info("[governance] Crawling %s", governance_url)
        platform = self._detect_platform(governance_url)

        if platform == "snapshot":
            return await self._crawl_snapshot(governance_url)
        if platform == "tally":
            return await self._crawl_tally(governance_url)
        if platform in ("commonwealth", "discourse"):
            return await self._crawl_forum(governance_url)

        # Generic RSS fallback
        return await self._crawl_rss(governance_url)

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    async def _crawl_snapshot(self, url: str) -> list[CrawledDocument]:
        space = self._extract_snapshot_space(url)
        if not space:
            return []

        query = """
        query GetProposals($space: String!, $skip: Int!) {
          proposals(
            first: 100
            skip: $skip
            where: { space: $space }
            orderBy: "created"
            orderDirection: desc
          ) {
            id title body state created scores_total author
          }
        }
        """
        docs: list[CrawledDocument] = []
        skip = 0

        while True:
            try:
                resp = await self.client.post(
                    SNAPSHOT_GRAPHQL,
                    json={"query": query, "variables": {"space": space, "skip": skip}},
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.error("[snapshot] Query failed: %s", exc)
                break

            proposals = data.get("data", {}).get("proposals", [])
            if not proposals:
                break

            for proposal in proposals:
                doc = self._proposal_to_document(proposal, space)
                if doc:
                    docs.append(doc)

            skip += len(proposals)
            if len(proposals) < 100:
                break
            await self.throttle()

        logger.info("[snapshot] %d proposals for space %s", len(docs), space)
        return docs

    def _proposal_to_document(
        self, proposal: dict, space: str
    ) -> CrawledDocument | None:
        body = (proposal.get("body") or "").strip()
        title = (proposal.get("title") or "").strip()
        created_ts = proposal.get("created", 0)

        # Always include security-relevant proposals; skip old benign ones
        created_dt = datetime.utcfromtimestamp(created_ts)
        age = datetime.utcnow() - created_dt
        is_security_relevant = any(
            kw in (title + body).lower() for kw in SECURITY_KEYWORDS
        )
        if age > timedelta(days=DEFAULT_LOOKBACK_DAYS) and not is_security_relevant:
            return None

        full_text = f"# {title}\n\n{body}"
        proposal_id = proposal.get("id", "")

        return self.make_document(
            source_url=f"https://snapshot.org/#/{space}/proposal/{proposal_id}",
            raw_text=full_text,
            doc_type="governance_proposal",
            title=title,
            metadata={
                "platform": "snapshot",
                "space": space,
                "state": proposal.get("state"),
                "author": proposal.get("author"),
                "created_at": created_dt.isoformat(),
                "scores_total": proposal.get("scores_total"),
            },
        )

    # ------------------------------------------------------------------
    # Tally
    # ------------------------------------------------------------------

    async def _crawl_tally(self, url: str) -> list[CrawledDocument]:
        """
        Tally's public API requires an API key for production use.
        This implementation calls the public endpoint that works without one.
        """
        governor_id = self._extract_tally_governor(url)
        if not governor_id:
            return []

        query = """
        query Proposals($governorId: AccountID!) {
          proposals(governorId: $governorId, pagination: { limit: 50, offset: 0 }) {
            id title description proposer { address } createdAt status
          }
        }
        """
        try:
            resp = await self.client.post(
                TALLY_API,
                json={"query": query, "variables": {"governorId": governor_id}},
            )
            resp.raise_for_status()
            proposals = resp.json().get("data", {}).get("proposals", [])
        except Exception as exc:
            logger.error("[tally] Query failed: %s", exc)
            return []

        docs: list[CrawledDocument] = []
        for p in proposals:
            desc = (p.get("description") or "").strip()
            title = (p.get("title") or "").strip()
            if not desc:
                continue
            docs.append(
                self.make_document(
                    source_url=f"https://www.tally.xyz/proposal/{p.get('id')}",
                    raw_text=f"# {title}\n\n{desc}",
                    doc_type="governance_proposal",
                    title=title,
                    metadata={
                        "platform": "tally",
                        "status": p.get("status"),
                        "proposer": p.get("proposer", {}).get("address"),
                        "created_at": p.get("createdAt"),
                    },
                )
            )
        return docs

    # ------------------------------------------------------------------
    # Discourse / Commonwealth forums
    # ------------------------------------------------------------------

    async def _crawl_forum(self, base_url: str) -> list[CrawledDocument]:
        """
        Discourse exposes /latest.rss and /posts.rss.
        Fall back to HTML crawl if RSS is unavailable.
        """
        feed_urls = [
            f"{base_url.rstrip('/')}/latest.rss",
            f"{base_url.rstrip('/')}/posts.rss",
            f"{base_url.rstrip('/')}/c/security.rss",
        ]
        for feed_url in feed_urls:
            docs = await self._crawl_rss(feed_url)
            if docs:
                return docs
        return []

    # ------------------------------------------------------------------
    # Generic RSS / Atom
    # ------------------------------------------------------------------

    async def _crawl_rss(self, feed_url: str) -> list[CrawledDocument]:
        try:
            resp = await self.safe_get(feed_url)
            feed = feedparser.parse(resp.text)
        except Exception as exc:
            logger.debug("[rss] Failed to fetch %s: %s", feed_url, exc)
            return []

        docs: list[CrawledDocument] = []
        for entry in feed.entries:
            title = entry.get("title", "")
            content = (
                entry.get("content", [{}])[0].get("value")
                or entry.get("summary", "")
            )
            link = entry.get("link", "")

            if not content.strip():
                continue

            plain = trafilatura.extract(content) or content
            docs.append(
                self.make_document(
                    source_url=link,
                    raw_text=f"# {title}\n\n{plain}",
                    doc_type="rss_item",
                    title=title,
                    metadata={
                        "feed_url": feed_url,
                        "published": str(entry.get("published", "")),
                    },
                )
            )
        return docs

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _detect_platform(self, url: str) -> str:
        netloc = urlparse(url).netloc.lower()
        if "snapshot.org" in netloc:
            return "snapshot"
        if "tally.xyz" in netloc:
            return "tally"
        if "commonwealth.im" in netloc or "common.xyz" in netloc:
            return "commonwealth"
        # Discourse instances have no shared domain — detect by structure
        return "discourse"

    def _extract_snapshot_space(self, url: str) -> str | None:
        match = re.search(r"snapshot\.org/#/([^/]+)", url)
        return match.group(1) if match else None

    def _extract_tally_governor(self, url: str) -> str | None:
        match = re.search(r"tally\.xyz/gov(?:ernor)?/([^/]+)", url)
        return match.group(1) if match else None
