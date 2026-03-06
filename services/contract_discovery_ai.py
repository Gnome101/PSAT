#!/usr/bin/env python3
"""Resolve contract addresses from explicit `company` + `contract_name`.

How it works:
1. Broad Tavily search (no raw content) to discover URLs and collect snippets.
   LLM selects the official domain from the search results.
2. LLM-guided page discovery — searches the official domain for pages, then asks
   the LLM which page(s) most likely list deployed contract addresses.  The
   recommended page(s) are fetched directly via HTTP and addresses extracted
   near the contract name are scored as high-confidence evidence.
3. Scoring, deduplication by address (merging chain info), and ranking.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from services.discovery_ai_evidence import (
    CHAIN_SORT_ORDER,
    _build_evidence_domains,
    _debug_log,
    _enrich_with_fetched_content,
    _extract_addresses_from_recommended_pages,
    _process_results,
)
from services.discovery_ai_domain import (
    _discover_contract_listing_pages,
    _llm_select_domain,
    _maybe_domain,
    _tavily_search,
)


def _build_source_links(deduped: list[dict[str, Any]]) -> dict[str, str]:
    """Build ranked source links from deduplicated evidence items."""
    link_scores: dict[str, float] = {}
    for item in deduped:
        url = str(item.get("url", "")).strip()
        if url:
            base = {
                "llm_recommended_page": 0.8,
                "official_page": 0.6,
                "official_page_linked_explorer": 0.5,
            }.get(item["kind"], 0.3)
            link_scores[url] = max(link_scores.get(url, 0.0), base)
        ref = str(item.get("referrer_url", "")).strip()
        if ref and ref.lower() != "none":
            link_scores[ref] = max(link_scores.get(ref, 0.0), 0.7)
    return {
        f"source_{i + 1}": u
        for i, (u, _) in enumerate(sorted(link_scores.items(), key=lambda p: p[1], reverse=True)[:5])
    }


def _score_and_build_candidates(
    grouped: dict[tuple[str, str], list[dict[str, Any]]],
    clean_company: str,
    clean_contract: str,
) -> list[dict[str, Any]]:
    """Score evidence groups and build candidate dicts."""
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
        unique_urls = {str(e.get("url", "")) for e in deduped if e.get("url")}

        if official_page == 0 and linked_explorer == 0 and llm_recommended == 0:
            continue

        confidence = 0.25
        confidence += min(0.44, official_page * 0.22)
        confidence += min(0.50, llm_recommended * 0.50)
        confidence += min(0.24, linked_explorer * 0.12)
        confidence += min(0.12, max(0, len(unique_urls) - 1) * 0.04)
        confidence = min(confidence, 0.99)

        reasons = [
            f"Official page evidence: {official_page}",
            f"Official linked explorer evidence: {linked_explorer}",
        ]
        if llm_recommended:
            reasons.append(f"LLM-recommended page evidence: {llm_recommended}")
        if len(unique_urls) > 1:
            reasons.append(f"Confirmed by {len(unique_urls)} unique URLs")
        if any(e.get("chain_from_hint") for e in deduped):
            reasons.append("Applied requested chain to chain-agnostic evidence")

        links = _build_source_links(deduped)
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
    return candidates


def _deduplicate_by_address(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge candidates that share the same address, preferring specific chains and higher confidence."""
    by_address: dict[str, dict[str, Any]] = {}
    for cand in candidates:
        addr = cand["address"]
        prev = by_address.get(addr)
        if prev is None:
            by_address[addr] = cand
        else:
            winner = cand if cand["confidence"] > prev["confidence"] else prev
            loser = prev if winner is cand else cand
            if winner["chain"] == "unknown" and loser["chain"] != "unknown":
                winner["chain"] = loser["chain"]
            winner["links"] = {**loser["links"], **winner["links"]}
            by_address[addr] = winner
    return list(by_address.values())


def search_contract_name_ai(
    company: str,
    contract_name: str,
    chain: str | None = None,
    limit: int = 10,
    max_queries: int = 4,
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

    # --- Phase 1: Broad search (domain discovery + initial evidence) ---
    _debug_log(debug, "Phase 1: broad Tavily search for domain discovery + initial evidence")
    broad_results = _tavily_search(
        f'"{clean_company}" "{clean_contract}" contract address',
        max_results=10,
        queries_used=queries_used,
        max_queries=max_queries,
        errors=errors,
        debug=debug,
    )

    # --- Domain selection via LLM ---
    official_domain = _maybe_domain(clean_company)
    if official_domain:
        notes.append(f"Using provided company domain: {official_domain}")
        domain_candidates = [official_domain]
    else:
        llm_domain = _llm_select_domain(broad_results, clean_company, debug=debug)
        if llm_domain:
            domain_candidates = [llm_domain]
            official_domain = llm_domain
        else:
            domain_candidates = []
            official_domain = None

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

    evidence_domains = _build_evidence_domains(official_domain, broad_results)
    notes.append(f"Evidence domains: {', '.join(evidence_domains)}")
    _debug_log(debug, f"Evidence domains: {evidence_domains}")

    # Extract evidence from broad results.
    # Fetch full page content for evidence-domain URLs (replaces Tavily raw_content)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    _debug_log(debug, "Phase 1 (cont): enriching + extracting evidence from broad search results")
    _enrich_with_fetched_content(broad_results, evidence_domains, debug=debug)
    _process_results(
        broad_results,
        clean_contract,
        evidence_domains,
        requested_chain,
        grouped,
        "official_page",
        debug=debug,
    )

    # --- Phase 2: LLM-guided page discovery ---
    # Search the primary domain for pages, ask the LLM which ones list
    # contract addresses, then fetch those pages directly for extraction.
    if evidence_domains:
        _debug_log(debug, "Phase 2: LLM-guided page discovery on primary evidence domain")
        disc_results, rec_urls = _discover_contract_listing_pages(
            evidence_domains[0],
            clean_company,
            queries_used,
            max_queries,
            errors,
            debug=debug,
        )
        if rec_urls:
            notes.append(f"LLM-recommended pages: {len(rec_urls)}")
            _debug_log(debug, f"Recommended URLs: {rec_urls}")

        # Enrich discovery results with fetched content, then process
        _enrich_with_fetched_content(disc_results, evidence_domains, debug=debug)
        _process_results(
            disc_results,
            clean_contract,
            evidence_domains,
            requested_chain,
            grouped,
            "official_page",
            debug=debug,
        )

        # Fetch LLM-recommended pages directly to get full content.
        _extract_addresses_from_recommended_pages(
            rec_urls, clean_contract, requested_chain, grouped, debug=debug,
        )

    notes.append(f"Evidence keys: {len(grouped)}")
    _debug_log(debug, f"Evidence keys after Phase 2: {len(grouped)}")

    # --- Phase 3: Score and rank ---
    _debug_log(debug, "Phase 3: scoring, deduplication, and ranking")
    candidates = _score_and_build_candidates(grouped, clean_company, clean_contract)
    ranked = sorted(
        _deduplicate_by_address(candidates),
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
    parser.add_argument("--max-queries", type=int, default=4, help="Tavily query cap (default: 4)")
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
