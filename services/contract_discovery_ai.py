#!/usr/bin/env python3
"""Resolve contract addresses from explicit `company` + `contract_name`.

How it works:
1. Uses Tavily to identify likely official company/docs domains.
2. Searches those domains for contract pages and crawls internal docs links.
3. Extracts EVM addresses only from retrieved page content/URLs.
4. Adds optional explorer confirmation and ranks candidates by evidence count.
5. Returns structured output with confidence, reasons, and source links.
"""

from __future__ import annotations

import argparse
import json
from html import unescape
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils import tavily

ADDRESS_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
URL_RE = re.compile(r"https?://[^\s\"'<>)]+" )
HREF_RE = re.compile(r"""href=["']([^"']+)["']""", re.IGNORECASE)
DOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9-]*(?:\.[a-z0-9-]+)+$", re.IGNORECASE)
HTTP_TIMEOUT_SECONDS = 12

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

CRYPTO_HINTS = (
    "smart contract",
    "contract address",
    "token",
    "protocol",
    "defi",
    "mainnet",
    "ethereum",
    "arbitrum",
    "optimism",
    "polygon",
    "base",
    "etherscan",
    "arbiscan",
)

GENERIC_COMPANY_TERMS = {
    "llc",
    "inc",
    "incorporated",
    "ltd",
    "limited",
    "company",
    "co",
    "corp",
    "corporation",
    "group",
}

CHAIN_SORT_ORDER = {"ethereum": 0, "arbitrum": 1, "optimism": 2, "polygon": 3, "base": 4, "unknown": 99}


def _tokenize(value: str) -> set[str]:
    return {part for part in re.split(r"[^a-z0-9]+", value.lower()) if part}


def _normalize_address(value: str) -> str:
    return "0x" + value.lower().replace("0x", "", 1)


def _extract_addresses(*values: str) -> set[str]:
    out: set[str] = set()
    for value in values:
        if not value:
            continue
        for match in ADDRESS_RE.findall(value):
            out.add(_normalize_address(match))
    return out


def _extract_urls(text: str) -> list[str]:
    return [match.rstrip(".,;:!?)") for match in URL_RE.findall(text or "")]


def _extract_href_urls(html: str, base_url: str) -> list[str]:
    urls: list[str] = []
    for raw in HREF_RE.findall(html or ""):
        href = unescape(str(raw).strip())
        if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        urls.append(urljoin(base_url, href))
    return urls


def _get_domain(url: str) -> str:
    domain = urlparse(url).netloc.lower()
    return domain[4:] if domain.startswith("www.") else domain


def _domain_matches(domain: str, known: str) -> bool:
    return domain == known or domain.endswith(f".{known}")


def _is_explorer_domain(domain: str) -> bool:
    return any(_domain_matches(domain, known) for known in EXPLORER_CHAINS)


def _is_explorer_url(url: str) -> bool:
    return _is_explorer_domain(_get_domain(url))


def _is_low_trust_domain(domain: str) -> bool:
    return any(_domain_matches(domain, known) for known in LOW_TRUST_DOMAINS)


def _is_allowed_domain(domain: str, allowed_domains: list[str]) -> bool:
    return any(_domain_matches(domain, allowed) for allowed in allowed_domains)


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
    clean = value.strip().lower().replace("https://", "").replace("http://", "")
    clean = clean.split("/")[0]
    if clean.startswith("www."):
        clean = clean[4:]
    if " " in clean or not DOMAIN_RE.match(clean) or _is_explorer_domain(clean):
        return None
    return clean


class _QueryRunner:
    def __init__(self, max_queries: int):
        self.max_queries = max(1, max_queries)
        self.used = 0
        self.failures = 0
        self.halted = False

    def search(self, query: str, *, max_results: int, errors: list[dict[str, Any]], notes: list[str]) -> list[dict[str, Any]]:
        if self.halted:
            return []
        if self.used >= self.max_queries:
            notes.append(f"Query budget reached ({self.max_queries})")
            self.halted = True
            return []
        self.used += 1
        try:
            out = tavily.search(
                query,
                max_results=max_results,
                topic="general",
                search_depth="advanced",
                include_raw_content=True,
            )
            self.failures = 0
            return out
        except Exception as exc:  # noqa: BLE001
            errors.append(tavily.error_from_exception(exc))
            self.failures += 1
            if self.failures >= 2:
                notes.append("Halting after repeated Tavily failures")
                self.halted = True
            return []


def _crawl_pages_for_evidence(
    seed_urls: set[str],
    allowed_domains: list[str],
    requested_chain: str | None,
    grouped: dict[tuple[str, str], list[dict[str, Any]]],
    errors: list[dict[str, Any]],
    notes: list[str],
    max_pages: int,
) -> None:
    queue = [url.split("#")[0] for url in seed_urls if url]
    visited: set[str] = set()
    pages_crawled = 0

    while queue and pages_crawled < max_pages:
        current = queue.pop(0)
        if not current or current in visited:
            continue
        current_domain = _get_domain(current)
        if not _is_allowed_domain(current_domain, allowed_domains):
            continue
        visited.add(current)
        pages_crawled += 1

        try:
            response = requests.get(
                current,
                headers={"User-Agent": "PSAT/0.1"},
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            html = response.text
        except requests.RequestException as exc:
            errors.append({"provider": "crawler", "error": f"{current}: {exc}"})
            continue

        text = html[:700000]

        page_chain = _infer_chain(current, text)
        for address in _extract_addresses(current, text):
            resolved_chain, chain_from_hint = _resolve_chain(page_chain, requested_chain)
            if resolved_chain is None:
                continue
            grouped[(resolved_chain, address)].append(
                {
                    "kind": "crawled_page",
                    "url": current,
                    "referrer_url": None,
                    "chain_from_hint": chain_from_hint,
                }
            )

        discovered_urls: set[str] = set(_extract_urls(text))
        discovered_urls.update(_extract_href_urls(html, current))

        priority: list[str] = []
        normal: list[str] = []
        for discovered in discovered_urls:
            clean = discovered.split("#")[0].strip()
            if not clean:
                continue

            if _is_explorer_url(clean):
                linked_chain = _infer_chain(clean, clean)
                for address in _extract_addresses(clean, text):
                    resolved_chain, chain_from_hint = _resolve_chain(linked_chain, requested_chain)
                    if resolved_chain is None:
                        continue
                    grouped[(resolved_chain, address)].append(
                        {
                            "kind": "crawled_linked_explorer",
                            "url": clean,
                            "referrer_url": current,
                            "chain_from_hint": chain_from_hint,
                        }
                    )
                continue

            discovered_domain = _get_domain(clean)
            if not _is_allowed_domain(discovered_domain, allowed_domains):
                continue
            if clean in visited:
                continue

            lowered = clean.lower()
            if any(keyword in lowered for keyword in ("contract", "deployed", "integration", "docs", "address", "token")):
                priority.append(clean)
            else:
                normal.append(clean)

        queue = priority + queue + normal

    notes.append(f"Crawled pages: {pages_crawled}/{max_pages}")


def search_contract_name_ai(
    company: str,
    contract_name: str,
    chain: str | None = None,
    limit: int = 10,
    max_queries: int = 12,
    max_pages: int = 10,
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
    runner = _QueryRunner(max_queries=max_queries)

    # 1) Resolve official domain.
    domain_candidates: list[str] = []
    official_domain = _maybe_domain(clean_company)
    if official_domain:
        notes.append(f"Using provided company domain: {official_domain}")
        domain_candidates = [official_domain]
    else:
        company_tokens = {tok for tok in _tokenize(clean_company) if len(tok) >= 3 and tok not in GENERIC_COMPANY_TERMS}
        contract_tokens = {tok for tok in _tokenize(clean_contract) if len(tok) >= 3 and tok not in {"smart", "contract", "token"}}
        scores: dict[str, float] = defaultdict(float)
        queries = [
            f"{clean_company} official website crypto protocol",
            f"{clean_company} {clean_contract} smart contract",
        ]
        notes.append(f"Domain discovery queries planned: {len(queries)}")
        for query_text in queries:
            results = runner.search(query_text, max_results=8, errors=errors, notes=notes)
            for result in results:
                url = str(result.get("url", "")).strip()
                if not url:
                    continue
                title = str(result.get("title", "")).strip()
                content = str(result.get("content", "")).strip()
                raw = str(result.get("raw_content", "")).strip()
                text = f"{title} {content} {raw}"
                domain = _get_domain(url)
                if _is_explorer_domain(domain) or _is_low_trust_domain(domain):
                    continue
                tokens = _tokenize(f"{domain} {text}")
                company_overlap = len(company_tokens & tokens) / len(company_tokens) if company_tokens else 0.0
                contract_overlap = len(contract_tokens & tokens) / len(contract_tokens) if contract_tokens else 0.0
                hint_hits = sum(1 for hint in CRYPTO_HINTS if hint in text.lower())
                score = 0.6 * company_overlap + 0.2 * contract_overlap + min(0.05 * hint_hits, 0.3)
                if domain.endswith(".fi") or domain.endswith(".io"):
                    score += 0.05
                if any(term in domain for term in GENERIC_COMPANY_TERMS) and hint_hits == 0:
                    score -= 0.2
                if score > 0:
                    scores[domain] += score
                for linked_url in _extract_urls(text):
                    linked_domain = _get_domain(linked_url)
                    if linked_domain and not _is_explorer_domain(linked_domain) and not _is_low_trust_domain(linked_domain):
                        scores[linked_domain] += max(score * 0.4, 0.0)
            if runner.halted:
                break
        domain_candidates = [domain for domain, score in sorted(scores.items(), key=lambda item: item[1], reverse=True) if score >= 0.25][:3]
        official_domain = domain_candidates[0] if domain_candidates else None
        if domain_candidates:
            notes.append(f"Domain candidates: {', '.join(domain_candidates)}")
        else:
            notes.append("Could not identify an official domain")

    if not official_domain:
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

    # 2) Gather evidence from official domains (protocol site + docs domains).
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    company_tokens = _tokenize(clean_company)
    contract_tokens = _tokenize(clean_contract)

    evidence_domains: list[str] = []
    for domain in [official_domain] + domain_candidates:
        if not domain:
            continue
        if _is_low_trust_domain(domain):
            continue
        if domain in evidence_domains:
            continue
        if "github.com" in domain and official_domain != domain:
            continue
        evidence_domains.append(domain)
    notes.append(f"Evidence domains: {', '.join(evidence_domains)}")

    seed_urls: set[str] = set()
    for domain in evidence_domains:
        evidence_queries = [
            f'site:{domain} "{clean_contract}" contract address',
            f'site:{domain} "{clean_contract}" deployed contracts',
            f'site:{domain} "{clean_contract}" contracts integrations',
            f'site:{domain} "{clean_company}" "{clean_contract}"',
        ]
        for official_query in evidence_queries:
            official_results = runner.search(official_query, max_results=8, errors=errors, notes=notes)
            for result in official_results:
                page_url = str(result.get("url", "")).strip()
                if not page_url or not _is_allowed_domain(_get_domain(page_url), evidence_domains):
                    continue
                title = str(result.get("title", "")).strip()
                content = str(result.get("content", "")).strip()
                raw = str(result.get("raw_content", "")).strip()
                blob = f"{title} {content} {raw}"
                blob_tokens = _tokenize(blob)
                if company_tokens and not (company_tokens & blob_tokens):
                    continue
                if contract_tokens and not (contract_tokens & blob_tokens):
                    continue

                seed_urls.add(page_url)
                page_chain = _infer_chain(page_url, blob)
                for address in _extract_addresses(page_url, blob):
                    resolved_chain, chain_from_hint = _resolve_chain(page_chain, requested_chain)
                    if resolved_chain is None:
                        continue
                    grouped[(resolved_chain, address)].append(
                        {"kind": "official_page", "url": page_url, "referrer_url": None, "chain_from_hint": chain_from_hint}
                    )

                for linked_url in _extract_urls(blob):
                    clean_link = linked_url.split("#")[0]
                    if _is_explorer_url(clean_link):
                        linked_chain = _infer_chain(clean_link, clean_link)
                        for address in _extract_addresses(clean_link, blob):
                            resolved_chain, chain_from_hint = _resolve_chain(linked_chain, requested_chain)
                            if resolved_chain is None:
                                continue
                            grouped[(resolved_chain, address)].append(
                                {
                                    "kind": "official_linked_explorer",
                                    "url": clean_link,
                                    "referrer_url": page_url,
                                    "chain_from_hint": chain_from_hint,
                                }
                            )
                    elif _is_allowed_domain(_get_domain(clean_link), evidence_domains):
                        seed_urls.add(clean_link)

            if runner.halted:
                break
        if runner.halted:
            break

    _crawl_pages_for_evidence(
        seed_urls=seed_urls,
        allowed_domains=evidence_domains,
        requested_chain=requested_chain,
        grouped=grouped,
        errors=errors,
        notes=notes,
        max_pages=max_pages,
    )

    notes.append(f"Official evidence keys: {len(grouped)}")

    # 3) Optional explorer confirmation for already-found addresses.
    if grouped:
        explorer_query = f'"{clean_company}" "{clean_contract}" contract address etherscan'
        if requested_chain:
            explorer_query = f'"{clean_company}" "{clean_contract}" {requested_chain} contract address'
        explorer_results = runner.search(explorer_query, max_results=8, errors=errors, notes=notes)
        allowed_keys = set(grouped.keys())
        for result in explorer_results:
            url = str(result.get("url", "")).strip()
            if not _is_explorer_url(url):
                continue
            title = str(result.get("title", "")).strip()
            content = str(result.get("content", "")).strip()
            raw = str(result.get("raw_content", "")).strip()
            blob = f"{url} {title} {content} {raw}"
            blob_tokens = _tokenize(blob)
            if company_tokens and not (company_tokens & blob_tokens):
                continue
            if contract_tokens and not (contract_tokens & blob_tokens):
                continue
            inferred = _infer_chain(url, blob)
            for address in _extract_addresses(blob):
                resolved_chain, chain_from_hint = _resolve_chain(inferred, requested_chain)
                if resolved_chain is None:
                    continue
                key = (resolved_chain, address)
                if key not in allowed_keys:
                    continue
                grouped[key].append(
                    {"kind": "explorer_confirmation", "url": url, "referrer_url": None, "chain_from_hint": chain_from_hint}
                )

    # 4) Score candidates.
    candidates: list[dict[str, Any]] = []
    for (candidate_chain, address), evidence in grouped.items():
        seen: set[tuple[str, str, str]] = set()
        deduped: list[dict[str, Any]] = []
        for item in evidence:
            sig = (str(item.get("kind", "")), str(item.get("url", "")), str(item.get("referrer_url", "")))
            if sig in seen:
                continue
            seen.add(sig)
            deduped.append(item)

        official_page = sum(1 for item in deduped if item.get("kind") in {"official_page", "crawled_page"})
        official_linked = sum(
            1 for item in deduped if item.get("kind") in {"official_linked_explorer", "crawled_linked_explorer"}
        )
        confirmations = sum(1 for item in deduped if item.get("kind") == "explorer_confirmation")
        unique_urls = {str(item.get("url", "")) for item in deduped if item.get("url")}

        if official_page == 0 and official_linked == 0:
            continue

        confidence = 0.25
        confidence += min(0.44, official_page * 0.22)
        confidence += min(0.24, official_linked * 0.12)
        confidence += min(0.12, confirmations * 0.06)
        confidence += min(0.12, max(0, len(unique_urls) - 1) * 0.04)
        confidence = min(confidence, 0.99)

        reasons = [
            f"Official page evidence: {official_page}",
            f"Official linked explorer evidence: {official_linked}",
        ]
        if confirmations:
            reasons.append(f"Explorer confirmations: {confirmations}")
        if len(unique_urls) > 1:
            reasons.append(f"Confirmed by {len(unique_urls)} unique URLs")
        if any(item.get("chain_from_hint") for item in deduped):
            reasons.append("Applied requested chain to chain-agnostic evidence")

        link_scores: dict[str, float] = {}
        for item in deduped:
            kind = str(item.get("kind", ""))
            url = str(item.get("url", "")).strip()
            if url:
                base = {
                    "official_page": 0.6,
                    "crawled_page": 0.58,
                    "official_linked_explorer": 0.5,
                    "crawled_linked_explorer": 0.48,
                    "explorer_confirmation": 0.4,
                }.get(kind, 0.0)
                link_scores[url] = max(link_scores.get(url, 0.0), base)
            referrer = str(item.get("referrer_url", "")).strip()
            if referrer:
                link_scores[referrer] = max(link_scores.get(referrer, 0.0), 0.7)
        links = {f"source_{idx + 1}": url for idx, (url, _) in enumerate(sorted(link_scores.items(), key=lambda p: p[1], reverse=True)[:5])}
        if not links:
            continue

        candidates.append(
            {
                "display_name": f"{clean_company} - {clean_contract}",
                "symbol": None,
                "address": address,
                "chain": candidate_chain,
                "confidence": round(confidence, 4),
                "source": "tavily_ai",
                "reasons": reasons,
                "links": links,
            }
        )

    ranked = sorted(candidates, key=lambda item: (-item["confidence"], CHAIN_SORT_ORDER.get(item["chain"], 50), item["address"]))[:limit]
    best_candidate = None
    if len(ranked) == 1:
        best_candidate = ranked[0]
    elif len(ranked) > 1:
        first, second = ranked[0], ranked[1]
        if first["confidence"] >= 0.9 and (first["confidence"] - second["confidence"]) >= 0.1:
            best_candidate = first

    if not ranked:
        notes.append("No address met official-domain evidence requirements")
    notes.append(f"Tavily queries used: {runner.used}/{runner.max_queries}")

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
    parser.add_argument("company", help="Company/protocol (e.g., etherfi)")
    parser.add_argument("contract_name", help='Contract name (e.g., "KING Distributor")')
    parser.add_argument("--chain", default=None, help="Optional chain filter")
    parser.add_argument("--limit", type=int, default=10, help="Max candidates to return")
    parser.add_argument("--max-queries", type=int, default=12, help="Hard Tavily query cap (default: 12)")
    parser.add_argument("--max-pages", type=int, default=10, help="Max official/docs pages to crawl (default: 10)")
    args = parser.parse_args()

    try:
        result = search_contract_name_ai(
            args.company,
            args.contract_name,
            chain=args.chain,
            limit=args.limit,
            max_queries=args.max_queries,
            max_pages=args.max_pages,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
