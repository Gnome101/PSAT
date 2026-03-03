#!/usr/bin/env python3
"""Resolve contract addresses from explicit `company` + `contract_name`.

How it works:
1. Broad Tavily search to discover official domains and collect initial evidence.
2. LLM-guided page discovery — searches the official domain for pages, then asks
   the Kimi model (from ``utils.llm``) which page(s) most likely list deployed
   contract addresses. The
   recommended page(s) are fetched directly (bypassing Tavily's content
   truncation) and addresses extracted near the contract name are scored as
   high-confidence ``llm_recommended_page`` evidence.
3. Site-scoped Tavily queries on top domains for additional evidence.
4. Optional explorer confirmation for already-found addresses.
5. Scoring, deduplication by address (merging chain info), and ranking.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import requests as _requests

from utils import llm, tavily

ADDRESS_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
URL_RE = re.compile(r"https?://[^\s\"'<>)]+")
DOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9-]*(?:\.[a-z0-9-]+)+$", re.IGNORECASE)

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


def _tokenize(value: str) -> set[str]:
    return {part for part in re.split(r"[^a-z0-9]+", value.lower()) if part}


def _normalize_address(value: str) -> str:
    return "0x" + value.lower().replace("0x", "", 1)


def _extract_addresses(*values: str) -> set[str]:
    out: set[str] = set()
    for value in values:
        if value:
            for match in ADDRESS_RE.findall(value):
                out.add(_normalize_address(match))
    return out


TAG_RE = re.compile(r"<[^>]+>")


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


def _maybe_domain(value: str) -> str | None:
    clean = value.strip().lower().replace("https://", "").replace("http://", "").split("/")[0]
    if clean.startswith("www."):
        clean = clean[4:]
    if " " in clean or not DOMAIN_RE.match(clean) or _is_explorer_domain(clean):
        return None
    return clean


def _tavily_search(
    query: str,
    max_results: int,
    queries_used: list[int],
    max_queries: int,
    errors: list[dict],
    debug: bool = False,
) -> list[dict]:
    """Run a single Tavily search, respecting query budget."""
    if queries_used[0] >= max_queries:
        _debug_log(debug, f"Skipping Tavily query (budget exhausted): {query!r}")
        return []
    queries_used[0] += 1
    _debug_log(
        debug,
        f"Tavily query {queries_used[0]}/{max_queries}: {query!r} (max_results={max_results})",
    )
    try:
        results = tavily.search(
            query,
            max_results=max_results,
            topic="general",
            search_depth="advanced",
            include_raw_content=True,
        )
        _debug_log(debug, f"Tavily returned {len(results)} result(s)")
        return results
    except Exception as exc:  # noqa: BLE001
        errors.append(tavily.error_from_exception(exc))
        _debug_log(debug, f"Tavily query failed: {exc!r}")
        return []


def _discover_contract_listing_pages(
    domain: str,
    company: str,
    queries_used: list[int],
    max_queries: int,
    errors: list[dict],
    debug: bool = False,
) -> tuple[list[dict], list[str]]:
    """Search the official domain for pages, then ask the LLM which ones list contract addresses.

    Returns (tavily_results, recommended_urls).  Falls back gracefully if the
    LLM call fails — recommended_urls will be empty.
    """
    _debug_log(debug, f"Phase 2.5: discovering contract-listing pages on domain={domain}")
    results = _tavily_search(
        f"site:{domain} {company} smart contract addresses",
        max_results=10,
        queries_used=queries_used,
        max_queries=max_queries,
        errors=errors,
        debug=debug,
    )
    if not results:
        _debug_log(debug, "No page-discovery results returned")
        return [], []

    # Collect unique URLs from this domain with their titles
    seen_urls: set[str] = set()
    page_info: list[dict[str, str]] = []
    for r in results:
        url = str(r.get("url", "")).strip()
        title = str(r.get("title", "")).strip()
        if not url or url in seen_urls:
            continue
        if _is_allowed_domain(_get_domain(url), [domain]):
            seen_urls.add(url)
            page_info.append({"url": url, "title": title})

    if not page_info:
        _debug_log(debug, "No unique in-domain pages found for LLM recommendation")
        return results, []

    # Ask the LLM to pick the most relevant page(s)
    page_list = "\n".join(f"- {p['title']}: {p['url']}" for p in page_info)
    prompt = (
        f"Below are pages from the {company} documentation site ({domain}).\n"
        f"Which of these pages is most likely to contain a comprehensive, "
        f"authoritative list of deployed smart contract addresses?\n\n"
        f"{page_list}\n\n"
        f"Reply with ONLY the best URL(s), one per line — nothing else."
    )

    try:
        response = llm.nim.chat(
            [{"role": "user", "content": prompt}],
            model=llm.nim.default_model,
            max_tokens=2048,
            temperature=0.0,
        )
        recommended: list[str] = []
        for url_match in URL_RE.findall(response):
            clean_url = url_match.rstrip(".,;:!?)")
            if _is_allowed_domain(_get_domain(clean_url), [domain]):
                recommended.append(clean_url)
        _debug_log(debug, f"LLM recommended {len(recommended)} in-domain URL(s)")
        return results, recommended
    except Exception as exc:  # noqa: BLE001
        # LLM unavailable — return results without recommendations
        _debug_log(debug, f"LLM recommendation step failed: {exc!r}")
        return results, []


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


def search_contract_name_ai(
    company: str,
    contract_name: str,
    chain: str | None = None,
    limit: int = 10,
    max_queries: int = 8,
    debug: bool = False,
) -> dict[str, Any]:
    clean_company = company.strip()
    clean_contract = contract_name.strip()
    if not clean_company:
        raise ValueError("company must not be empty")
    if not clean_contract:
        raise ValueError("contract_name must not be empty")
    if limit < 1:
        raise ValueError("limit must be >= 1")

    requested_chain = chain.lower().strip() if isinstance(chain, str) and chain.strip() else None
    errors: list[dict[str, Any]] = []
    notes: list[str] = []
    queries_used: list[int] = [0]
    _debug_log(
        debug,
        (
            "Starting discovery: "
            f"company={clean_company!r}, contract={clean_contract!r}, "
            f"chain={requested_chain or 'any'}, limit={limit}, max_queries={max_queries}"
        ),
    )

    # --- Phase 1: Broad search (doubles as domain discovery + initial evidence) ---
    _debug_log(debug, "Phase 1: broad Tavily search for domain discovery + initial evidence")
    broad_results = _tavily_search(
        f'"{clean_company}" "{clean_contract}" contract address',
        max_results=10,
        queries_used=queries_used,
        max_queries=max_queries,
        errors=errors,
        debug=debug,
    )

    # Identify official domains from results
    official_domain = _maybe_domain(clean_company)
    if official_domain:
        notes.append(f"Using provided company domain: {official_domain}")
        domain_candidates = [official_domain]
    else:
        company_tokens = {t for t in _tokenize(clean_company) if len(t) >= 3}
        domain_scores: dict[str, float] = defaultdict(float)
        for result in broad_results:
            url = str(result.get("url", "")).strip()
            if not url:
                continue
            domain = _get_domain(url)
            if not domain or _is_explorer_domain(domain) or _is_low_trust_domain(domain):
                continue
            tokens = _tokenize(f"{domain} {result.get('title', '')} {result.get('content', '')}")
            overlap = len(company_tokens & tokens) / len(company_tokens) if company_tokens else 0
            if overlap > 0:
                domain_scores[domain] += overlap

        domain_candidates = [
            d
            for d, s in sorted(domain_scores.items(), key=lambda x: x[1], reverse=True)
            if s >= 0.3
        ][:3]
        official_domain = domain_candidates[0] if domain_candidates else None

    if domain_candidates:
        notes.append(f"Domain candidates: {', '.join(domain_candidates)}")
    _debug_log(
        debug,
        (
            f"Domain selection complete: official_domain={official_domain!r}, "
            f"candidates={domain_candidates}"
        ),
    )

    if not official_domain:
        notes.append("Could not identify an official domain")
        notes.append(f"Tavily queries used: {queries_used[0]}/{max_queries}")
        _debug_log(debug, "Stopping early: official domain not identified")
        return {
            "query": clean_contract,
            "company": clean_company,
            "chain": requested_chain or "any",
            "official_domain": None,
            "domain_candidates": [],
            "best_candidate": None,
            "candidates": [],
            "errors": errors[:12],
            "notes": notes[:12],
        }

    # Build evidence domains list
    evidence_domains: list[str] = []
    for d in [official_domain] + domain_candidates:
        if d and not _is_low_trust_domain(d) and d not in evidence_domains:
            if "github.com" in d and d != official_domain:
                continue
            evidence_domains.append(d)
    notes.append(f"Evidence domains: {', '.join(evidence_domains)}")
    _debug_log(debug, f"Evidence domains: {evidence_domains}")

    # --- Phase 2: Extract evidence from broad results ---
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    _debug_log(debug, "Phase 2: extracting evidence from broad search results")
    _process_results(
        broad_results,
        clean_contract,
        evidence_domains,
        requested_chain,
        grouped,
        "official_page",
        debug=debug,
    )

    # --- Phase 2.5: LLM-guided page discovery ---
    # Search the primary domain for pages, ask the LLM which ones list
    # contract addresses, then extract from those pages with boosted weight.
    recommended_urls: set[str] = set()
    if evidence_domains:
        _debug_log(debug, "Phase 2.5: LLM-guided page discovery on primary evidence domain")
        disc_results, rec_urls = _discover_contract_listing_pages(
            evidence_domains[0],
            clean_company,
            queries_used,
            max_queries,
            errors,
            debug=debug,
        )
        recommended_urls = {u.lower().rstrip("/") for u in rec_urls}
        if recommended_urls:
            notes.append(f"LLM-recommended pages: {len(recommended_urls)}")
            _debug_log(debug, f"Recommended URLs: {sorted(recommended_urls)}")

        # Process all discovery results normally (Tavily content, may be truncated)
        _process_results(
            disc_results,
            clean_contract,
            evidence_domains,
            requested_chain,
            grouped,
            "official_page",
            debug=debug,
        )

        # Fetch LLM-recommended pages directly to get full content (Tavily
        # truncates large pages like contract listings, losing the data we need).
        # Contract names can appear multiple times on a listing page (e.g.
        # mainnet deploy + chain-specific subsystem).  The first occurrence is
        # typically the primary deployment, so it gets full weight; later
        # matches get regular weight.
        for rec_url in rec_urls:
            try:
                _debug_log(debug, f"Fetching recommended page: {rec_url}")
                page_resp = _requests.get(rec_url, timeout=15, headers={"User-Agent": "PSAT/0.1"})
                if page_resp.status_code != 200:
                    _debug_log(debug, f"Skipping {rec_url}: HTTP {page_resp.status_code}")
                    continue
                plain = TAG_RE.sub(" ", page_resp.text)
                page_chain = _infer_chain(rec_url, plain[:2000])

                # Ordered extraction — first unique address gets the boost.
                name_clean = " ".join(clean_contract.split())
                name_pat = re.escape(name_clean).replace(r"\ ", r"\s+")
                seen: set[str] = set()

                # Try strict (line/cell start) first, then loose (anywhere).
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
                        break  # strict matched, skip loose
                _debug_log(debug, f"Recommended page {rec_url} produced {len(seen)} address match(es)")
            except Exception as exc:  # noqa: BLE001
                _debug_log(debug, f"Failed to process recommended page {rec_url}: {exc!r}")
                continue

    # --- Phase 3: Site-scoped queries on top domains ---
    # Two query variations per domain to surface different URL variants.
    _debug_log(debug, "Phase 3: running site-scoped Tavily queries")
    for domain in evidence_domains[:2]:
        for query_text in [
            f'site:{domain} "{clean_contract}" contract address',
            f'site:{domain} "{clean_company}" "{clean_contract}" address',
        ]:
            site_results = _tavily_search(
                query_text,
                max_results=8,
                queries_used=queries_used,
                max_queries=max_queries,
                errors=errors,
                debug=debug,
            )
            _process_results(
                site_results,
                clean_contract,
                evidence_domains,
                requested_chain,
                grouped,
                "official_page",
                debug=debug,
            )

    notes.append(f"Evidence keys: {len(grouped)}")
    _debug_log(debug, f"Evidence keys after Phase 3: {len(grouped)}")

    # --- Phase 4: Optional explorer confirmation for already-found addresses ---
    if grouped:
        _debug_log(debug, "Phase 4: explorer confirmation for discovered addresses")
        explorer_query = f'"{clean_company}" "{clean_contract}" contract address etherscan'
        if requested_chain:
            explorer_query = f'"{clean_company}" "{clean_contract}" {requested_chain} contract address'
        explorer_results = _tavily_search(
            explorer_query,
            max_results=8,
            queries_used=queries_used,
            max_queries=max_queries,
            errors=errors,
            debug=debug,
        )
        allowed_keys = set(grouped.keys())
        company_tokens = _tokenize(clean_company)
        contract_tokens = _tokenize(clean_contract)
        confirmations_added = 0
        for result in explorer_results:
            url = str(result.get("url", "")).strip()
            if not _is_explorer_domain(_get_domain(url)):
                continue
            blob = f"{url} {result.get('title', '')} {result.get('content', '')} {result.get('raw_content', '')}"
            blob_tokens = _tokenize(blob)
            if company_tokens and not (company_tokens & blob_tokens):
                continue
            if contract_tokens and not (contract_tokens & blob_tokens):
                continue
            inferred = _infer_chain(url, blob)
            for address in _extract_addresses(blob):
                resolved, hint = _resolve_chain(inferred, requested_chain)
                if resolved is None:
                    continue
                key = (resolved, address)
                if key not in allowed_keys:
                    continue
                grouped[key].append({"kind": "explorer_confirmation", "url": url, "chain_from_hint": hint})
                confirmations_added += 1
        _debug_log(debug, f"Explorer confirmations added: {confirmations_added}")

    # --- Phase 5: Score and rank ---
    _debug_log(debug, "Phase 5: scoring, deduplication, and ranking")
    candidates: list[dict[str, Any]] = []
    for (cand_chain, address), evidence in grouped.items():
        seen: set[tuple[str, str]] = set()
        deduped: list[dict[str, Any]] = []
        for item in evidence:
            sig = (str(item.get("kind", "")), str(item.get("url", "")))
            if sig in seen:
                continue
            seen.add(sig)
            deduped.append(item)

        official_page = sum(1 for e in deduped if e["kind"] == "official_page")
        llm_recommended = sum(1 for e in deduped if e["kind"] == "llm_recommended_page")
        linked_explorer = sum(1 for e in deduped if e["kind"].endswith("_linked_explorer"))
        confirmations = sum(1 for e in deduped if e["kind"] == "explorer_confirmation")
        unique_urls = {str(e.get("url", "")) for e in deduped if e.get("url")}

        if official_page == 0 and linked_explorer == 0 and llm_recommended == 0:
            continue

        confidence = 0.25
        confidence += min(0.44, official_page * 0.22)
        confidence += min(0.50, llm_recommended * 0.50)
        confidence += min(0.24, linked_explorer * 0.12)
        confidence += min(0.12, confirmations * 0.06)
        confidence += min(0.12, max(0, len(unique_urls) - 1) * 0.04)
        confidence = min(confidence, 0.99)

        reasons = [
            f"Official page evidence: {official_page}",
            f"Official linked explorer evidence: {linked_explorer}",
        ]
        if llm_recommended:
            reasons.append(f"LLM-recommended page evidence: {llm_recommended}")
        if confirmations:
            reasons.append(f"Explorer confirmations: {confirmations}")
        if len(unique_urls) > 1:
            reasons.append(f"Confirmed by {len(unique_urls)} unique URLs")
        if any(e.get("chain_from_hint") for e in deduped):
            reasons.append("Applied requested chain to chain-agnostic evidence")

        link_scores: dict[str, float] = {}
        for item in deduped:
            url = str(item.get("url", "")).strip()
            if url:
                base = {
                    "llm_recommended_page": 0.8,
                    "official_page": 0.6,
                    "official_page_linked_explorer": 0.5,
                    "explorer_confirmation": 0.4,
                }.get(item["kind"], 0.3)
                link_scores[url] = max(link_scores.get(url, 0.0), base)
            ref = str(item.get("referrer_url", "")).strip()
            if ref and ref.lower() != "none":
                link_scores[ref] = max(link_scores.get(ref, 0.0), 0.7)
        links = {
            f"source_{i + 1}": u
            for i, (u, _) in enumerate(sorted(link_scores.items(), key=lambda p: p[1], reverse=True)[:5])
        }
        if not links:
            continue

        candidates.append(
            {
                "display_name": f"{clean_company} - {clean_contract}",
                "symbol": None,
                "address": address,
                "chain": cand_chain,
                "confidence": round(confidence, 4),
                "source": "tavily_ai",
                "reasons": reasons,
                "links": links,
            }
        )

    # Deduplicate by address — merge entries that only differ by chain,
    # preferring a specific chain over "unknown" and keeping the best confidence.
    by_address: dict[str, dict[str, Any]] = {}
    for cand in candidates:
        addr = cand["address"]
        prev = by_address.get(addr)
        if prev is None:
            by_address[addr] = cand
        else:
            # Merge: keep higher confidence, prefer specific chain
            winner = cand if cand["confidence"] > prev["confidence"] else prev
            loser = prev if winner is cand else cand
            if winner["chain"] == "unknown" and loser["chain"] != "unknown":
                winner["chain"] = loser["chain"]
            # Merge links (winner takes priority for key collisions)
            winner["links"] = {**loser["links"], **winner["links"]}
            by_address[addr] = winner

    ranked = sorted(
        by_address.values(),
        key=lambda c: (-c["confidence"], CHAIN_SORT_ORDER.get(c["chain"], 50), c["address"]),
    )[:limit]

    best_candidate = None
    if len(ranked) == 1:
        best_candidate = ranked[0]
    elif len(ranked) > 1:
        first, second = ranked[0], ranked[1]
        if first["confidence"] >= 0.9 and (first["confidence"] - second["confidence"]) >= 0.1:
            best_candidate = first

    if not ranked:
        notes.append("No address met official-domain evidence requirements")
    notes.append(f"Tavily queries used: {queries_used[0]}/{max_queries}")
    _debug_log(
        debug,
        (
            f"Completed discovery: candidates={len(candidates)}, ranked={len(ranked)}, "
            f"best_candidate={'yes' if best_candidate else 'no'}, "
            f"queries_used={queries_used[0]}/{max_queries}, errors={len(errors)}"
        ),
    )

    return {
        "query": clean_contract,
        "company": clean_company,
        "chain": requested_chain or "any",
        "official_domain": official_domain,
        "domain_candidates": domain_candidates,
        "best_candidate": best_candidate,
        "candidates": ranked,
        "errors": errors[:12],
        "notes": notes[:12],
        "warning": (
            "AI-powered discovery searches official docs and web sources but may return "
            "incorrect results. Always verify addresses against official project documentation."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Contract discovery with explicit company + contract name")
    parser.add_argument("company", help="Company/protocol name")
    parser.add_argument("contract_name", help="Contract name to search for")
    parser.add_argument("--chain", default=None, help="Optional chain filter")
    parser.add_argument("--limit", type=int, default=10, help="Max candidates to return")
    parser.add_argument("--max-queries", type=int, default=8, help="Tavily query cap (default: 8)")
    parser.add_argument("--debug", action="store_true", help="Print phase-by-phase debug logs to stderr")
    args = parser.parse_args()

    try:
        result = search_contract_name_ai(
            args.company,
            args.contract_name,
            chain=args.chain,
            limit=args.limit,
            max_queries=args.max_queries,
            debug=args.debug,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
