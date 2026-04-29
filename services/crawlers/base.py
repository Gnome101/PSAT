import asyncio
import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class CrawledDocument:
    source_url: str
    protocol_id: str
    doc_type: str          # docs_page | github_readme | github_release |
                           # immunefi_bounty | governance_post | rss_item
    raw_text: str
    content_hash: str
    scraped_at: datetime
    title: str | None = None
    is_new: bool = True
    is_updated: bool = False
    metadata: dict = field(default_factory=dict)


@dataclass
class ProtocolSeeds:
    protocol_id: str
    name: str
    contract_address: str
    website_url: str | None = None
    docs_url: str | None = None
    github_org: str | None = None
    governance_url: str | None = None
    twitter_handle: str | None = None


# ---------------------------------------------------------------------------
# Retry decorator used across all crawlers
# ---------------------------------------------------------------------------

RETRY_POLICY = retry(
    stop=stop_after_attempt(settings.max_retries),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
    reraise=True,
)


# ---------------------------------------------------------------------------
# Base crawler
# ---------------------------------------------------------------------------

class BaseCrawler(ABC):
    """
    Abstract base class for all PSAT crawlers.

    Provides:
      - A shared async httpx client with sensible defaults
      - Rate limiting via asyncio.sleep
      - URL deduplication via a visited set
      - Content hashing for change detection
      - Structured logging
    """

    CRAWLER_TYPE: str = "base"

    def __init__(
        self,
        protocol_id: str,
        rate_limit_rps: float | None = None,
    ):
        self.protocol_id = protocol_id
        self.rate_limit_rps = rate_limit_rps or settings.default_rate_limit_rps
        self._visited: set[str] = set()
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=settings.request_timeout_seconds,
            follow_redirects=True,
            headers={"User-Agent": "PSAT-Crawler/1.0 (security research)"},
        )
        return self

    async def __aexit__(self, *_):
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def crawl(self, seed: str) -> list[CrawledDocument]:
        """
        Entry point for each crawler.  Receives a seed URL / identifier
        and returns a list of crawled documents.
        """

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Use crawler as async context manager")
        return self._client

    def hash_content(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def is_visited(self, url: str) -> bool:
        return url in self._visited

    def mark_visited(self, url: str) -> None:
        self._visited.add(url)

    async def throttle(self) -> None:
        """Sleep to respect the configured requests-per-second rate limit."""
        await asyncio.sleep(1.0 / self.rate_limit_rps)

    def make_document(
        self,
        source_url: str,
        raw_text: str,
        doc_type: str,
        title: str | None = None,
        metadata: dict | None = None,
    ) -> CrawledDocument:
        return CrawledDocument(
            source_url=source_url,
            protocol_id=self.protocol_id,
            doc_type=doc_type,
            raw_text=raw_text,
            content_hash=self.hash_content(raw_text),
            title=title,
            metadata=metadata or {},
            scraped_at=datetime.utcnow(),
        )

    @RETRY_POLICY
    async def safe_get(self, url: str, **kwargs) -> httpx.Response:
        """GET with automatic retry on transient failures."""
        response = await self.client.get(url, **kwargs)
        response.raise_for_status()
        return response
