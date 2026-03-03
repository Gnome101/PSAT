#!/usr/bin/env python3
"""Resolve contract addresses from explicit `company` + `contract_name`.

How it works:
1. Runs a broad Tavily search to discover official domains and initial evidence.
2. Runs targeted site-scoped queries on discovered domains.
3. Extracts EVM addresses near contract name mentions in page content.
4. Optionally confirms via explorer search.
5. Scores and ranks candidates by evidence count.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils import tavily

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
) -> list[dict]:
    """Run a single Tavily search, respecting query budget."""
    if queries_used[0] >= max_queries:
        return []
    queries_used[0] += 1
    try:
        return tavily.search(
            query,
            max_results=max_results,
            topic="general",
            search_depth="advanced",
            include_raw_content=True,
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(tavily.error_from_exception(exc))
        return []


def _process_results(
    results: list[dict],
    contract_name: str,
    evidence_domains: list[str],
    requested_chain: str | None,
    grouped: dict[tuple[str, str], list[dict[str, Any]]],
    kind: str,
) -> None:
    """Extract address evidence from Tavily results (no crawling needed)."""
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


def search_contract_name_ai(
    company: str,
    contract_name: str,
    chain: str | None = None,
    limit: int = 10,
    max_queries: int = 8,
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

    # --- Phase 1: Broad search (doubles as domain discovery + initial evidence) ---
    broad_results = _tavily_search(
        f'"{clean_company}" "{clean_contract}" contract address',
        max_results=10,
        queries_used=queries_used,
        max_queries=max_queries,
        errors=errors,
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

    if not official_domain:
        notes.append("Could not identify an official domain")
        notes.append(f"Tavily queries used: {queries_used[0]}/{max_queries}")
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

    # --- Phase 2: Extract evidence from broad results ---
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    _process_results(broad_results, clean_contract, evidence_domains, requested_chain, grouped, "official_page")

    # --- Phase 3: Site-scoped queries on top domains ---
    # Two query variations per domain to surface different URL variants.
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
            )
            _process_results(site_results, clean_contract, evidence_domains, requested_chain, grouped, "official_page")

    notes.append(f"Evidence keys: {len(grouped)}")

    # --- Phase 4: Optional explorer confirmation for already-found addresses ---
    if grouped:
        explorer_query = f'"{clean_company}" "{clean_contract}" contract address etherscan'
        if requested_chain:
            explorer_query = f'"{clean_company}" "{clean_contract}" {requested_chain} contract address'
        explorer_results = _tavily_search(
            explorer_query,
            max_results=8,
            queries_used=queries_used,
            max_queries=max_queries,
            errors=errors,
        )
        allowed_keys = set(grouped.keys())
        company_tokens = _tokenize(clean_company)
        contract_tokens = _tokenize(clean_contract)
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

    # --- Phase 5: Score and rank ---
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
        linked_explorer = sum(1 for e in deduped if e["kind"].endswith("_linked_explorer"))
        confirmations = sum(1 for e in deduped if e["kind"] == "explorer_confirmation")
        unique_urls = {str(e.get("url", "")) for e in deduped if e.get("url")}

        if official_page == 0 and linked_explorer == 0:
            continue

        confidence = 0.25
        confidence += min(0.44, official_page * 0.22)
        confidence += min(0.24, linked_explorer * 0.12)
        confidence += min(0.12, confirmations * 0.06)
        confidence += min(0.12, max(0, len(unique_urls) - 1) * 0.04)
        confidence = min(confidence, 0.99)

        reasons = [
            f"Official page evidence: {official_page}",
            f"Official linked explorer evidence: {linked_explorer}",
        ]
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
                base = {"official_page": 0.6, "official_page_linked_explorer": 0.5, "explorer_confirmation": 0.4}.get(
                    item["kind"], 0.3
                )
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

    ranked = sorted(
        candidates,
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
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Contract discovery with explicit company + contract name")
    parser.add_argument("company", help="Company/protocol name")
    parser.add_argument("contract_name", help="Contract name to search for")
    parser.add_argument("--chain", default=None, help="Optional chain filter")
    parser.add_argument("--limit", type=int, default=10, help="Max candidates to return")
    parser.add_argument("--max-queries", type=int, default=8, help="Tavily query cap (default: 8)")
    args = parser.parse_args()

    try:
        result = search_contract_name_ai(
            args.company,
            args.contract_name,
            chain=args.chain,
            limit=args.limit,
            max_queries=args.max_queries,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
