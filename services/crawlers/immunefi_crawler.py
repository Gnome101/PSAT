"""
ImmunefiCrawler
===============
Scrapes the Immunefi bug bounty platform for a given protocol's program.

Extracts:
  - Bounty status (active / paused / archived)
  - Maximum bounty payout
  - In-scope contracts and assets
  - Out-of-scope items
  - Program description and rules
  - KYC / PoC requirements
"""
import logging
import json
import re

import trafilatura
from bs4 import BeautifulSoup

from base import BaseCrawler, CrawledDocument

logger = logging.getLogger(__name__)

IMMUNEFI_BASE = "https://immunefi.com"
IMMUNEFI_BOUNTY_LIST = "https://immunefi.com/explore/"


class ImmunefiCrawler(BaseCrawler):
    CRAWLER_TYPE = "immunefi"

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def crawl(self, protocol_name: str) -> list[CrawledDocument]:
        """
        protocol_name: the protocol's canonical name (matched against
        Immunefi slugs, case-insensitively).
        """
        if not protocol_name:
            return []

        logger.info("[immunefi] Looking up bounty for %s", protocol_name)

        # 1. Find the program slug from the explore page
        slug = await self._find_slug(protocol_name)
        if not slug:
            logger.info("[immunefi] No bounty program found for %s", protocol_name)
            return []

        # 2. Fetch the full program page
        program_url = f"{IMMUNEFI_BASE}/bounty/{slug}/"
        return await self._parse_program_page(program_url, slug)

    # ------------------------------------------------------------------
    # Slug discovery
    # ------------------------------------------------------------------

    async def _find_slug(self, protocol_name: str) -> str | None:
        """
        Immunefi's explore page is JS-rendered.  We attempt a direct
        URL guess first (most reliable), then fall back to the API
        endpoint that backs the explore page.
        """
        # Direct guess
        guess_slug = self._name_to_slug(protocol_name)
        url = f"{IMMUNEFI_BASE}/bounty/{guess_slug}/"
        try:
            resp = await self.safe_get(url)
            if resp.status_code == 200:
                return guess_slug
        except Exception:
            pass

        # Immunefi exposes a public JSON endpoint that lists all programs
        try:
            api_url = "https://immunefi.com/immunefi.json"
            resp = await self.safe_get(api_url)
            programs: list[dict] = resp.json()
            for prog in programs:
                name: str = prog.get("project", "")
                if name.lower() == protocol_name.lower():
                    return prog.get("id") or self._name_to_slug(name)
        except Exception as exc:
            logger.debug("[immunefi] JSON endpoint unavailable: %s", exc)

        return None

    # ------------------------------------------------------------------
    # Program page parsing
    # ------------------------------------------------------------------

    async def _parse_program_page(
        self, url: str, slug: str
    ) -> list[CrawledDocument]:
        try:
            resp = await self.safe_get(url)
        except Exception as exc:
            logger.error("[immunefi] Could not fetch %s: %s", url, exc)
            return []

        html = resp.text
        soup = BeautifulSoup(html, "lxml")

        # Extract structured metadata from Next.js __NEXT_DATA__ script
        meta = self._extract_next_data(soup)

        # Fall back to trafilatura text extraction
        raw_text = trafilatura.extract(html, include_tables=True) or ""

        # Parse key fields
        status = self._parse_status(soup, meta)
        max_bounty = self._parse_max_bounty(soup, meta)
        in_scope = self._parse_scope(soup, meta, in_scope=True)
        out_of_scope = self._parse_scope(soup, meta, in_scope=False)

        # Build enriched text block for storage
        enriched = self._build_enriched_text(
            slug=slug,
            status=status,
            max_bounty=max_bounty,
            in_scope=in_scope,
            out_of_scope=out_of_scope,
            raw_text=raw_text,
        )

        doc = self.make_document(
            source_url=url,
            raw_text=enriched,
            doc_type="immunefi_bounty",
            title=f"Immunefi Bug Bounty: {slug}",
            metadata={
                "slug": slug,
                "status": status,
                "max_bounty_usd": max_bounty,
                "in_scope_contracts": in_scope,
                "out_of_scope": out_of_scope,
            },
        )
        return [doc]

    # ------------------------------------------------------------------
    # Structured data helpers
    # ------------------------------------------------------------------

    def _extract_next_data(self, soup: BeautifulSoup) -> dict:
        script = soup.find("script", id="__NEXT_DATA__")
        if script:
            try:
                return json.loads(script.string or "{}")
            except json.JSONDecodeError:
                pass
        return {}

    def _parse_status(self, soup: BeautifulSoup, meta: dict) -> str:
        # Look for common status indicators in the page
        text = soup.get_text(" ", strip=True).lower()
        if "paused" in text:
            return "paused"
        if "inactive" in text or "archived" in text:
            return "inactive"
        return "active"

    def _parse_max_bounty(self, soup: BeautifulSoup, meta: dict) -> str | None:
        # Try structured data first
        try:
            props = meta["props"]["pageProps"]["bounty"]
            return props.get("maxBounty") or props.get("max_bounty")
        except (KeyError, TypeError):
            pass

        # Fall back to regex on visible text
        text = soup.get_text(" ")
        match = re.search(r"\$[\d,]+(?:\.\d+)?(?:\s*(?:USD|USDC|USDT))?", text)
        return match.group(0) if match else None

    def _parse_scope(
        self, soup: BeautifulSoup, meta: dict, in_scope: bool
    ) -> list[str]:
        try:
            props = meta["props"]["pageProps"]["bounty"]
            key = "assets" if in_scope else "outOfScope"
            items = props.get(key, [])
            return [str(item.get("target") or item) for item in items]
        except (KeyError, TypeError):
            pass
        return []

    def _build_enriched_text(
        self,
        slug: str,
        status: str,
        max_bounty: str | None,
        in_scope: list[str],
        out_of_scope: list[str],
        raw_text: str,
    ) -> str:
        lines = [
            f"# Immunefi Bug Bounty: {slug}",
            f"Status: {status}",
            f"Max Bounty: {max_bounty or 'unknown'}",
        ]
        if in_scope:
            lines.append("\n## In-Scope Assets")
            lines.extend(f"- {s}" for s in in_scope)
        if out_of_scope:
            lines.append("\n## Out-of-Scope")
            lines.extend(f"- {s}" for s in out_of_scope)
        if raw_text:
            lines.append("\n## Program Description")
            lines.append(raw_text)
        return "\n".join(lines)

    def _name_to_slug(self, name: str) -> str:
        return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
