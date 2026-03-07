"""Evidence extraction, scoring helpers, and shared constants for AI discovery."""

from __future__ import annotations

import re
import sys
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import requests as _requests

from services.dependent_contracts import normalize_address as _normalize_address

ADDRESS_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
URL_RE = re.compile(r"https?://[^\s\"'<>)]+")
DOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9-]*(?:\.[a-z0-9-]+)+$", re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")

EXPLORER_CHAINS = {
    "etherscan.io": "ethereum",
    "eth.blockscout.com": "ethereum",
    "arbiscan.io": "arbitrum",
    "arbitrum.blockscout.com": "arbitrum",
    "optimistic.etherscan.io": "optimism",
    "optimism.blockscout.com": "optimism",
    "polygonscan.com": "polygon",
    "polygon.blockscout.com": "polygon",
    "basescan.org": "base",
    "base.blockscout.com": "base",
}

LOW_TRUST_DOMAINS = {
    "coingecko.com",
    "coinmarketcap.com",
    "defillama.com",
    "x.com",
    "twitter.com",
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "wikipedia.org",
    "reddit.com",
}

CHAIN_SORT_ORDER = {"ethereum": 0, "arbitrum": 1, "optimism": 2, "polygon": 3, "base": 4, "unknown": 99}


def _debug_log(enabled: bool, message: str) -> None:
    if enabled:
        ts = datetime.now().isoformat(timespec="seconds")
        print(f"[{ts}] [debug] {message}", file=sys.stderr)


# -- Domain helpers ----------------------------------------------------------

def _get_domain(url: str) -> str:
    try:
        domain = urlparse(url).netloc.lower()
    except ValueError:
        return ""
    return domain[4:] if domain.startswith("www.") else domain


def _domain_matches(domain: str, known: str) -> bool:
    return domain == known or domain.endswith(f".{known}")


def _is_explorer_domain(domain: str) -> bool:
    return any(_domain_matches(domain, k) for k in EXPLORER_CHAINS)


def _is_low_trust_domain(domain: str) -> bool:
    return any(_domain_matches(domain, k) for k in LOW_TRUST_DOMAINS)


def _is_allowed_domain(domain: str, allowed: list[str]) -> bool:
    return any(_domain_matches(domain, a) for a in allowed)


# -- Address / chain helpers -------------------------------------------------

def _extract_addresses(*values: str) -> set[str]:
    out: set[str] = set()
    for value in values:
        if value:
            for match in ADDRESS_RE.findall(value):
                out.add(_normalize_address(match))
    return out


def _extract_addresses_near_name(text: str, contract_name: str) -> set[str]:
    """Find addresses closely following each mention of contract_name.

    Strips HTML tags first so proximity reflects visual distance.
    Uses strict line/cell-start matching to avoid substring false positives
    (e.g. "L2 KING Distributor" vs "KING Distributor"), with a proximity
    fallback for prose text.
    """
    if not text:
        return set()
    clean = " ".join(contract_name.strip().split())
    if not clean:
        return set()
    plain = TAG_RE.sub(" ", text)
    name_re = re.escape(clean).replace(r"\ ", r"\s+")
    matches: set[str] = set()

    # Strict: name at start of line or table cell — avoids substring matches.
    strict = re.compile(r"(?:^|[\n|])\s*" + name_re, re.IGNORECASE | re.MULTILINE)
    for mention in strict.finditer(plain):
        window = plain[mention.end() : mention.end() + 120]
        near = ADDRESS_RE.search(window)
        if near:
            matches.add(_normalize_address(near.group()))
    if matches:
        return matches

    # Fallback: proximity for prose where name may appear mid-sentence.
    loose = re.compile(name_re, re.IGNORECASE)
    for mention in loose.finditer(plain):
        window = plain[mention.end() : mention.end() + 200]
        near = ADDRESS_RE.search(window)
        if near:
            matches.add(_normalize_address(near.group()))
    return matches


def _is_name_near_substring(text: str, contract_name: str, needle: str, radius: int = 900) -> bool:
    """Check if contract_name appears within radius chars of needle in text."""
    if not text or not contract_name or not needle:
        return False
    phrase_re = re.compile(re.escape(" ".join(contract_name.strip().split())).replace(r"\ ", r"\s+"), re.IGNORECASE)
    start = 0
    while True:
        idx = text.find(needle, start)
        if idx < 0:
            return False
        left = max(0, idx - radius)
        right = min(len(text), idx + len(needle) + radius)
        if phrase_re.search(text[left:right]):
            return True
        start = idx + max(1, len(needle))


def _infer_chain(url: str, text: str) -> str:
    domain = _get_domain(url)
    for known, chain in EXPLORER_CHAINS.items():
        if _domain_matches(domain, known):
            return chain
    lowered = text.lower()
    if "arbitrum" in lowered:
        return "arbitrum"
    if "optimism" in lowered or "optimistic" in lowered:
        return "optimism"
    if "polygon" in lowered or "matic" in lowered:
        return "polygon"
    if "base" in lowered:
        return "base"
    if "ethereum" in lowered or "mainnet" in lowered:
        return "ethereum"
    return "unknown"


def _resolve_chain(inferred: str, requested: str | None) -> tuple[str | None, bool]:
    if not requested:
        return inferred, False
    if inferred not in {requested, "unknown"}:
        return None, False
    return requested, inferred == "unknown"


# -- HTTP / enrichment / result processing -----------------------------------

def _fetch_page(url: str, debug: bool = False) -> str | None:
    """Fetch a page via HTTP and return its text, or None on failure."""
    try:
        resp = _requests.get(url, timeout=15, headers={"User-Agent": "PSAT/0.1"})
        if resp.status_code == 200:
            _debug_log(debug, f"Fetched {url} ({len(resp.text)} chars)")
            return resp.text
        _debug_log(debug, f"Fetch {url}: HTTP {resp.status_code}")
    except _requests.RequestException as exc:
        _debug_log(debug, f"Fetch {url} failed: {exc!r}")
    return None


def _enrich_with_fetched_content(
    results: list[dict],
    evidence_domains: list[str],
    debug: bool = False,
) -> None:
    """Fetch full page content for evidence-domain results via HTTP.

    Mutates each result dict in-place, adding ``raw_content`` so that
    downstream ``_process_results`` has full page text to extract addresses
    from — without requiring Tavily's ``include_raw_content`` flag.
    """
    for result in results:
        url = str(result.get("url", "")).strip()
        if not url:
            continue
        domain = _get_domain(url)
        if not _is_allowed_domain(domain, evidence_domains):
            continue
        if result.get("raw_content"):
            continue
        content = _fetch_page(url, debug=debug)
        if content:
            result["raw_content"] = content


def _process_results(
    results: list[dict],
    contract_name: str,
    evidence_domains: list[str],
    requested_chain: str | None,
    grouped: dict[tuple[str, str], list[dict[str, Any]]],
    kind: str,
    debug: bool = False,
) -> None:
    """Extract address evidence from Tavily results (no crawling needed)."""
    before = sum(len(v) for v in grouped.values())
    for result in results:
        url = str(result.get("url", "")).strip()
        if not url:
            continue
        domain = _get_domain(url)
        title = str(result.get("title", "")).strip()
        content = str(result.get("content", "")).strip()
        raw = str(result.get("raw_content", "")).strip()
        blob = f"{title} {content} {raw}"

        # Extract addresses from official domain pages near the contract name
        if _is_allowed_domain(domain, evidence_domains):
            page_chain = _infer_chain(url, blob)
            for address in _extract_addresses_near_name(blob, contract_name):
                resolved, hint = _resolve_chain(page_chain, requested_chain)
                if resolved is not None:
                    grouped[(resolved, address)].append(
                        {"kind": kind, "url": url, "chain_from_hint": hint}
                    )

        # Find explorer links in content that are near the contract name
        for raw_url in URL_RE.findall(blob):
            explorer_url = raw_url.rstrip(".,;:!?)")
            if not _is_explorer_domain(_get_domain(explorer_url)):
                continue
            if not _is_name_near_substring(blob, contract_name, explorer_url):
                continue
            linked_chain = _infer_chain(explorer_url, "")
            for address in _extract_addresses(explorer_url):
                resolved, hint = _resolve_chain(linked_chain, requested_chain)
                if resolved is not None:
                    grouped[(resolved, address)].append(
                        {
                            "kind": f"{kind}_linked_explorer",
                            "url": explorer_url,
                            "referrer_url": url,
                            "chain_from_hint": hint,
                        }
                    )
    after = sum(len(v) for v in grouped.values())
    _debug_log(
        debug,
        f"Processed {len(results)} result(s) for kind={kind}; added {after - before} evidence item(s)",
    )


# -- Higher-level pipeline helpers -------------------------------------------

def _build_evidence_domains(
    official_domain: str,
    broad_results: list[dict[str, Any]],
) -> list[str]:
    """Collect up to 3 trusted domains for evidence extraction."""
    domains: list[str] = [official_domain]
    for result in broad_results:
        url = str(result.get("url", "")).strip()
        if not url:
            continue
        d = _get_domain(url)
        if d and d not in domains and not _is_explorer_domain(d) and not _is_low_trust_domain(d):
            if "github.com" not in d:
                domains.append(d)
        if len(domains) >= 3:
            break
    return domains


def _extract_addresses_from_recommended_pages(
    rec_urls: list[str],
    clean_contract: str,
    requested_chain: str | None,
    grouped: dict[tuple[str, str], list[dict[str, Any]]],
    debug: bool = False,
) -> None:
    """Fetch LLM-recommended pages and extract address evidence near the contract name."""
    for rec_url in rec_urls:
        page_text = _fetch_page(rec_url, debug=debug)
        if not page_text:
            continue
        plain = TAG_RE.sub(" ", page_text)
        page_chain = _infer_chain(rec_url, plain[:2000])

        name_clean = " ".join(clean_contract.split())
        name_pat = re.escape(name_clean).replace(r"\ ", r"\s+")
        seen: set[str] = set()

        for pattern, window_size in [
            (re.compile(r"(?:^|[\n|])\s*" + name_pat, re.IGNORECASE | re.MULTILINE), 120),
            (re.compile(name_pat, re.IGNORECASE), 200),
        ]:
            for mention in pattern.finditer(plain):
                window = plain[mention.end() : mention.end() + window_size]
                near = ADDRESS_RE.search(window)
                if not near:
                    continue
                addr = _normalize_address(near.group())
                if addr in seen:
                    continue
                seen.add(addr)
                resolved, hint = _resolve_chain(page_chain, requested_chain)
                if resolved is not None:
                    kind = "llm_recommended_page" if len(seen) == 1 else "official_page"
                    grouped[(resolved, addr)].append(
                        {"kind": kind, "url": rec_url, "chain_from_hint": hint}
                    )
            if seen:
                break
        _debug_log(debug, f"Recommended page {rec_url} produced {len(seen)} address match(es)")


