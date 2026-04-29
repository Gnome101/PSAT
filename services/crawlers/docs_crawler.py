"""
DocsSiteCrawler
===============
Crawls a protocol's documentation website.

Strategy:
  1. Attempt to parse /sitemap.xml for a complete page list (fast, reliable)
  2. Fall back to recursive BFS link-following if no sitemap is found
  3. Use trafilatura for clean text extraction (strips nav / footer boilerplate)
  4. Use Playwright for JS-rendered sites (GitBook, Notion, etc.) when the
     static fetch returns suspiciously little content
"""
import asyncio
import logging
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

import trafilatura
from bs4 import BeautifulSoup

from base import BaseCrawler, CrawledDocument

logger = logging.getLogger(__name__)

MIN_TEXT_LENGTH = 150       # discard pages with fewer characters
MAX_PAGES_PER_DOMAIN = 500  # safety cap — avoids runaway crawls
JS_CONTENT_THRESHOLD = 200  # if static fetch returns less, try Playwright


class DocsSiteCrawler(BaseCrawler):
    CRAWLER_TYPE = "docs_site"

    def __init__(self, protocol_id: str, rate_limit_rps: float = 0.5):
        super().__init__(protocol_id, rate_limit_rps)
        self._playwright_available = False

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def crawl(self, seed_url: str) -> list[CrawledDocument]:
        if not seed_url:
            return []

        seed_url = seed_url.rstrip("/")
        logger.info("[docs] Starting crawl for %s", seed_url)

        # 1. Try sitemap first
        urls = await self._urls_from_sitemap(seed_url)

        # 2. Fall back to recursive crawl from seed
        if not urls:
            logger.info("[docs] No sitemap found — using recursive crawl")
            urls = await self._urls_from_recursive_crawl(seed_url)

        logger.info("[docs] Found %d URLs to crawl", len(urls))

        documents: list[CrawledDocument] = []
        for url in urls[:MAX_PAGES_PER_DOMAIN]:
            if self.is_visited(url):
                continue
            self.mark_visited(url)

            doc = await self._fetch_page(url)
            if doc:
                documents.append(doc)

            await self.throttle()

        logger.info("[docs] Crawl complete — %d documents", len(documents))
        return documents

    # ------------------------------------------------------------------
    # Sitemap parsing
    # ------------------------------------------------------------------

    async def _urls_from_sitemap(self, base_url: str) -> list[str]:
        candidates = [
            f"{base_url}/sitemap.xml",
            f"{base_url}/sitemap_index.xml",
            f"{base_url}/sitemap/sitemap.xml",
        ]
        for url in candidates:
            try:
                resp = await self.safe_get(url)
                return self._parse_sitemap_xml(resp.text, base_url)
            except Exception:
                continue
        return []

    def _parse_sitemap_xml(self, xml_text: str, base_url: str) -> list[str]:
        urls: list[str] = []
        try:
            root = ET.fromstring(xml_text)
            # Strip namespace for easier querying
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for loc in root.findall(".//sm:loc", ns):
                href = (loc.text or "").strip()
                # For sitemap indexes, recurse (handled inline here)
                if href:
                    urls.append(href)
        except ET.ParseError as exc:
            logger.warning("Sitemap parse error: %s", exc)
        return self._filter_same_domain(urls, base_url)

    # ------------------------------------------------------------------
    # Recursive BFS crawl
    # ------------------------------------------------------------------

    async def _urls_from_recursive_crawl(
        self, seed_url: str, max_depth: int = 3
    ) -> list[str]:
        """BFS from seed — follows only same-domain href links.

        Uses a local visited set so the BFS link-extraction pass does NOT
        pollute self._visited.  The main crawl() loop owns self._visited and
        uses it to deduplicate the content-fetch pass; if we mark URLs here
        the main loop would skip every discovered URL and produce nothing.
        """
        queue: list[tuple[str, int]] = [(seed_url, 0)]
        # Local set — completely separate from self._visited
        bfs_visited: set[str] = set()
        # Seed is the primary page; include it so the content loop fetches it.
        found: list[str] = [seed_url]

        while queue and len(found) < MAX_PAGES_PER_DOMAIN:
            url, depth = queue.pop(0)
            if url in bfs_visited or depth > max_depth:
                continue
            bfs_visited.add(url)

            try:
                resp = await self.safe_get(url)
                links = self._extract_links(resp.text, url)
                same_domain_links = self._filter_same_domain(links, seed_url)

                for link in same_domain_links:
                    if link not in found:
                        found.append(link)
                        queue.append((link, depth + 1))

                await self.throttle()
            except Exception as exc:
                logger.debug("BFS fetch error for %s: %s", url, exc)

        return found

    # ------------------------------------------------------------------
    # Page fetching and text extraction
    # ------------------------------------------------------------------

    async def _fetch_page(self, url: str) -> CrawledDocument | None:
        try:
            resp = await self.safe_get(url)
            html = resp.text

            # Try lightweight extraction first
            raw_text = trafilatura.extract(
                html,
                include_comments=False,
                include_tables=True,
                no_fallback=False,
            )

            # If we got very little content, the page may be JS-rendered
            if not raw_text or len(raw_text) < JS_CONTENT_THRESHOLD:
                raw_text = await self._playwright_extract(url)

            if not raw_text or len(raw_text) < MIN_TEXT_LENGTH:
                logger.debug("Skipping %s — insufficient content", url)
                return None

            title = self._extract_title(html)
            return self.make_document(
                source_url=url,
                raw_text=raw_text,
                doc_type="docs_page",
                title=title,
                metadata={"status_code": resp.status_code},
            )

        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", url, exc)
            return None

    async def _playwright_extract(self, url: str) -> str | None:
        """
        Render the page with Playwright and extract visible text.
        Only invoked when static fetch returns thin content.
        """
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(url, wait_until="networkidle", timeout=20_000)
                html = await page.content()
                await browser.close()

            return trafilatura.extract(html, include_tables=True)
        except Exception as exc:
            logger.debug("Playwright unavailable for %s: %s", url, exc)
            return None

    # ------------------------------------------------------------------
    # HTML utilities
    # ------------------------------------------------------------------

    def _extract_links(self, html: str, base_url: str) -> list[str]:
        soup = BeautifulSoup(html, "lxml")
        links: list[str] = []
        for a_tag in soup.find_all("a", href=True):
            raw_href = a_tag["href"]
            # BS4 returns list[str] for multi-valued attributes in rare cases
            href = raw_href[0] if isinstance(raw_href, list) else raw_href
            if href.startswith("#") or href.startswith("mailto:"):
                continue
            full_url = urljoin(base_url, href).split("#")[0]
            links.append(full_url)
        return links

    def _extract_title(self, html: str) -> str | None:
        soup = BeautifulSoup(html, "lxml")
        tag = soup.find("title")
        return tag.get_text(strip=True) if tag else None

    def _filter_same_domain(self, urls: list[str], base_url: str) -> list[str]:
        base_netloc = urlparse(base_url).netloc
        filtered: list[str] = []
        for url in urls:
            parsed = urlparse(url)
            if parsed.netloc == base_netloc and parsed.scheme in ("http", "https"):
                filtered.append(url)
        return list(dict.fromkeys(filtered))  # dedup preserving order
