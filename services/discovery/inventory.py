#!/usr/bin/env python3
"""Orchestrator for protocol contract inventory discovery.

Given a company/protocol name or domain, this module:
  1. Identifies the official domain via Tavily search + LLM  (inventory_domain.py)
  2. Selects pages likely to contain contract inventories     (inventory_domain.py)
  3. Extracts contract entries from those pages               (inventory_extract.py)
  4. Scores, deduplicates, and ranks the results
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from .activity import enrich_with_activity  # noqa: E402
from .chain_resolver import resolve_unknown_chains  # noqa: E402
from .deployer import expand_from_deployers  # noqa: E402
from .inventory_domain import (  # noqa: E402
    CHAIN_SORT_ORDER,
    _debug_log,
    _discover_contract_inventory_pages,
    _domain_candidates_from_results,
    _llm_select_domain,
    _maybe_domain,
    _tavily_search,
)
from .inventory_extract import extract_inventory_entries_from_pages  # noqa: E402


def _collect_source_urls(evidence: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    """Extract deduplicated page URLs and explorer URLs from evidence."""
    page_urls: list[str] = []
    explorer_urls: list[str] = []
    seen_pages: set[str] = set()
    seen_explorers: set[str] = set()

    for item in evidence:
        page_url = str(item.get("url", "")).strip()
        if page_url and page_url not in seen_pages:
            seen_pages.add(page_url)
            page_urls.append(page_url)
        explorer_raw = item.get("explorer_url")
        explorer_url = str(explorer_raw).strip() if explorer_raw else ""
        if explorer_url and explorer_url not in seen_explorers:
            seen_explorers.add(explorer_url)
            explorer_urls.append(explorer_url)

    return page_urls[:3], explorer_urls[:2]


def _register_sources(
    sources_map: dict[str, str],
    page_urls: list[str],
    explorer_urls: list[str],
) -> list[str]:
    """Register URLs in the top-level sources map and return their IDs."""
    source_ids: list[str] = []
    for url in page_urls + explorer_urls:
        if url not in sources_map:
            sid = f"s{len(sources_map) + 1}"
            sources_map[url] = sid
        source_ids.append(sources_map[url])
    return source_ids


def _collapse_unknown_chain_entries(entries: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Merge unknown-chain evidence per address while preserving multi-chain evidence."""
    by_address: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        by_address[entry["address"]].append(entry)

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for address, items in by_address.items():
        specific = {item["chain"] for item in items if item["chain"] != "unknown"}
        remapped_chain = next(iter(specific)) if len(specific) == 1 else None
        for item in items:
            chain = item["chain"]
            if chain == "unknown" and remapped_chain:
                chain = remapped_chain
            grouped[address].append({**item, "chain": chain})
    return grouped


def _sorted_chains(chains: set[str]) -> list[str]:
    return sorted(chains, key=lambda chain: (CHAIN_SORT_ORDER.get(chain, 50), chain))


def _select_chain_summary(evidence: list[dict[str, Any]]) -> tuple[str, list[str]]:
    specific = _sorted_chains({str(item["chain"]) for item in evidence if item.get("chain") != "unknown"})
    if specific:
        if len(specific) == 1:
            return specific[0], specific
        return "multiple", specific

    if any(item.get("chain") == "unknown" for item in evidence):
        return "unknown", ["unknown"]
    return "unknown", []


def _select_name(evidence: list[dict[str, Any]]) -> tuple[str | None, list[str]]:
    names = [str(item["name"]).strip() for item in evidence if item.get("name")]
    if not names:
        return None, []
    counts = Counter(names)
    primary = max(counts, key=lambda name: (counts[name], len(name)))
    aliases = sorted(name for name in counts if name != primary)
    return primary, aliases


def _score_inventory_item(chain: str, evidence: list[dict[str, Any]]) -> tuple[float, dict[str, Any]]:
    page_count = len({str(item.get("url", "")) for item in evidence if item.get("url")})
    named_count = sum(1 for item in evidence if item.get("name"))
    table_count = sum(1 for item in evidence if item.get("kind") == "official_inventory_table")
    link_count = sum(1 for item in evidence if item.get("kind") == "official_inventory_link")
    text_count = sum(1 for item in evidence if item.get("kind") == "official_inventory_text")
    deployer_count = sum(1 for item in evidence if item.get("kind") == "deployer_expansion")
    explorer_count = sum(1 for item in evidence if item.get("explorer_url"))

    confidence = 0.35
    if named_count:
        confidence += 0.20
    if table_count:
        confidence += 0.18
    if link_count:
        confidence += 0.12
    if text_count and not table_count and not link_count:
        confidence += 0.05
    if deployer_count:
        confidence += 0.15
    confidence += min(0.12, max(0, page_count - 1) * 0.06)
    if explorer_count:
        confidence += 0.06
    if chain != "unknown":
        confidence += 0.05
    confidence = min(confidence, 0.99)

    evidence_counts: dict[str, Any] = {"official": page_count, "named": named_count}
    if table_count:
        evidence_counts["table"] = table_count
    if link_count:
        evidence_counts["link"] = link_count
    if text_count and not table_count:
        evidence_counts["text"] = text_count
    if deployer_count:
        evidence_counts["deployer"] = deployer_count
    if explorer_count:
        evidence_counts["explorer"] = explorer_count
    if any(item.get("chain_from_hint") for item in evidence):
        evidence_counts["chain_hinted"] = True

    return round(confidence, 4), evidence_counts


def _determine_sources(evidence: list[dict[str, Any]]) -> list[str]:
    """Derive the source list from evidence kinds present for an address."""
    _KIND_TO_SOURCE = {
        "official_inventory_table": "tavily_ai_inventory",
        "official_inventory_link": "tavily_ai_inventory",
        "official_inventory_text": "tavily_ai_inventory",
        "deployer_expansion": "deployer_expansion",
    }
    sources: list[str] = []
    seen: set[str] = set()
    for item in evidence:
        source = _KIND_TO_SOURCE.get(item.get("kind", ""), "tavily_ai_inventory")
        if source not in seen:
            seen.add(source)
            sources.append(source)
    return sources


def _build_contracts(
    entries: list[dict[str, Any]], limit: int
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Build the contract list and a top-level sources map.

    Returns (contracts, sources_map) where sources_map is ``{url: id}``
    and each contract references source IDs instead of full URLs.
    """
    grouped = _collapse_unknown_chain_entries(entries)
    sources_map: dict[str, str] = {}  # url → id
    contracts: list[dict[str, Any]] = []
    for address, evidence in grouped.items():
        _chain, chains = _select_chain_summary(evidence)
        name, aliases = _select_name(evidence)
        confidence, evidence_counts = _score_inventory_item(_chain, evidence)
        page_urls, explorer_urls = _collect_source_urls(evidence)
        if not page_urls and not explorer_urls:
            continue
        source_types = _determine_sources(evidence)
        # Drop unnamed deployer-only contracts — without a name they can't be
        # catalogued or fed into the analysis pipeline (which needs verified source).
        if not name and source_types == ["deployer_expansion"]:
            continue
        source_ids = _register_sources(sources_map, page_urls, explorer_urls)
        contract: dict[str, Any] = {
            "name": name,
            "address": address,
            "chains": chains,
            "confidence": confidence,
            "source": source_types,
            "evidence": evidence_counts,
            "source_ids": source_ids,
        }
        if aliases:
            contract["aliases"] = aliases
        contracts.append(contract)

    sorted_contracts = sorted(
        contracts,
        key=lambda item: (
            -float(item["confidence"]),
            item["name"] is None,
            str(item.get("name") or ""),
            CHAIN_SORT_ORDER.get(item["chains"][0] if item["chains"] else "unknown", 50),
            item["address"],
        ),
    )[:limit]
    return sorted_contracts, sources_map


def _group_multi_deployments(contracts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group contracts that share the same name and appear on multiple chains.

    Contracts with the same name but different addresses across chains are
    collapsed into a single entry with a ``deployments`` array.
    """
    # Index by lowercase name — only group named contracts.
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    ungroupable: list[dict[str, Any]] = []
    for contract in contracts:
        name = contract.get("name")
        if not name:
            ungroupable.append(contract)
            continue
        by_name[name.lower()].append(contract)

    result: list[dict[str, Any]] = []
    for _key, group in by_name.items():
        if len(group) == 1:
            result.append(group[0])
            continue

        # Check if these are actually different addresses (multi-chain deploys).
        unique_addresses = {c["address"] for c in group}
        if len(unique_addresses) == 1:
            # Same address listed multiple times — just keep the best one.
            result.append(group[0])
            continue

        # Group into a single entry with deployments array.
        # Use the highest-confidence entry as the base.
        group.sort(key=lambda c: -c.get("confidence", 0))
        base = group[0].copy()
        all_chains: list[str] = []
        seen_chains: set[str] = set()
        deployments: list[dict[str, Any]] = []
        all_source_ids: list[str] = []
        seen_source_ids: set[str] = set()
        max_confidence = 0.0

        for contract in group:
            dep: dict[str, Any] = {"address": contract["address"]}
            dep_chains = contract.get("chains", ["unknown"])
            dep["chains"] = dep_chains
            for ch in dep_chains:
                if ch not in seen_chains:
                    all_chains.append(ch)
                    seen_chains.add(ch)
            if contract.get("activity"):
                dep["activity"] = contract["activity"]
            if contract.get("rank_score") is not None:
                dep["rank_score"] = contract["rank_score"]
            deployments.append(dep)
            max_confidence = max(max_confidence, contract.get("confidence", 0))
            for sid in contract.get("source_ids", []):
                if sid not in seen_source_ids:
                    all_source_ids.append(sid)
                    seen_source_ids.add(sid)

        base["chains"] = all_chains
        base["confidence"] = max_confidence
        base["source_ids"] = all_source_ids
        base["deployments"] = deployments
        # Remove single-address field — use deployments instead.
        base.pop("address", None)
        result.append(base)

    result.extend(ungroupable)
    # Re-sort after grouping.
    result.sort(
        key=lambda item: (
            -float(item.get("rank_score", item.get("confidence", 0))),
            item.get("name") is None,
            str(item.get("name") or ""),
            CHAIN_SORT_ORDER.get(
                item["chains"][0] if item.get("chains") else "unknown", 50
            ),
            item.get("address", ""),
        ),
    )
    return result


def search_protocol_inventory(
    company: str,
    chain: str | None = None,
    limit: int = 100,
    max_queries: int = 4,
    run_deployer: bool = True,
    run_activity_ranking: bool = True,
    debug: bool = False,
) -> dict[str, Any]:
    clean_company = company.strip()
    if not clean_company:
        raise ValueError("company must not be empty")
    if limit < 1:
        raise ValueError("limit must be >= 1")

    requested_chain = chain.lower().strip() if isinstance(chain, str) and chain.strip() else None
    errors: list[dict[str, Any]] = []
    notes: list[str] = []
    queries_used = [0]
    broad_results: list[dict[str, Any]] = []

    _debug_log(
        debug,
        (
            "Starting inventory discovery: "
            f"company={clean_company!r}, chain={requested_chain or 'any'}, "
            f"limit={limit}, max_queries={max_queries}"
        ),
    )

    official_domain = _maybe_domain(clean_company)
    if official_domain:
        notes.append(f"Using provided company domain: {official_domain}")
        domain_candidates = [official_domain]
    else:
        broad_results = _tavily_search(
            f'"{clean_company}" protocol smart contract addresses deployments docs',
            max_results=10,
            queries_used=queries_used,
            max_queries=max_queries,
            errors=errors,
            debug=debug,
        )
        domain_candidates = _domain_candidates_from_results(broad_results)
        if domain_candidates:
            notes.append(f"Domain candidates: {', '.join(domain_candidates[:5])}")
        official_domain = _llm_select_domain(broad_results, clean_company, debug=debug)
        if not official_domain and len(domain_candidates) == 1:
            official_domain = domain_candidates[0]
            notes.append(f"Falling back to sole domain candidate: {official_domain}")

    if not official_domain:
        notes.append("Could not identify an official domain")
        notes.append(f"Tavily queries used: {queries_used[0]}/{max_queries}")
        return {
            "company": clean_company,
            "chain": requested_chain or "any",
            "official_domain": None,
            "domain_candidates": domain_candidates if "domain_candidates" in locals() else [],
            "pages_considered": [],
            "pages_selected": [],
            "contracts": [],
            "errors": errors[:12],
            "notes": notes[:12],
        }

    notes.append(f"Official domain: {official_domain}")
    page_results, selected_urls = _discover_contract_inventory_pages(
        official_domain,
        clean_company,
        broad_results,
        queries_used,
        max_queries,
        errors,
        debug=debug,
    )

    considered_urls = [
        str(result.get("url", "")).strip() for result in page_results if str(result.get("url", "")).strip()
    ]
    if not selected_urls:
        selected_urls = considered_urls[:3]
        if selected_urls:
            notes.append("LLM page selection unavailable; fell back to top in-domain page candidates")

    if selected_urls:
        notes.append(f"Selected pages: {len(selected_urls)}")
    tavily_entries = extract_inventory_entries_from_pages(selected_urls, requested_chain, debug=debug)

    deployer_entries: list[dict[str, Any]] = []
    if run_deployer and tavily_entries:
        seed_addresses = sorted({e["address"] for e in tavily_entries})
        _debug_log(debug, f"Running deployer expansion with {len(seed_addresses)} seed(s)")
        try:
            deployer_entries = expand_from_deployers(seed_addresses, debug=debug)
            notes.append(f"Deployer expansion: {len(deployer_entries)} contract(s)")
        except Exception as exc:
            _debug_log(debug, f"Deployer expansion failed: {exc!r}")
            notes.append(f"Deployer expansion failed: {exc}")

    entries = tavily_entries + deployer_entries
    contracts, sources_map = _build_contracts(entries, limit=limit)

    # Resolve unknown chains before activity ranking (activity needs correct chain_id).
    # With the new format, "unknown" is chains == ["unknown"].
    def _primary_chain(c: dict[str, Any]) -> str:
        chains = c.get("chains", [])
        return chains[0] if chains else "unknown"

    unknown_count = sum(1 for c in contracts if _primary_chain(c) == "unknown")
    if unknown_count:
        _debug_log(debug, f"Resolving chain for {unknown_count} unknown-chain contract(s)")
        try:
            # resolve_unknown_chains expects/updates "chain" key — derive it temporarily.
            for c in contracts:
                c["chain"] = _primary_chain(c)
            contracts = resolve_unknown_chains(contracts, debug=debug)
            # Sync chains back from resolver updates.
            for c in contracts:
                if "chain" in c:
                    resolved_chain = c.pop("chain")
                    if resolved_chain != "unknown":
                        c["chains"] = c.get("chains", [])
                        if c["chains"] == ["unknown"]:
                            c["chains"] = [resolved_chain]
            resolved = unknown_count - sum(1 for c in contracts if _primary_chain(c) == "unknown")
            notes.append(f"Chain resolution: resolved {resolved}/{unknown_count} unknown chain(s)")
        except Exception as exc:
            _debug_log(debug, f"Chain resolution failed: {exc!r}")
            notes.append(f"Chain resolution failed: {exc}")
            # Clean up temporary chain keys on failure.
            for c in contracts:
                c.pop("chain", None)

    if run_activity_ranking and contracts:
        _debug_log(debug, "Running on-chain activity ranking")
        try:
            # enrich_with_activity expects "chain" key — derive from chains.
            for c in contracts:
                c["chain"] = _primary_chain(c)
            contracts = enrich_with_activity(contracts, debug=debug)
            # Remove the temporary "chain" key after enrichment.
            for c in contracts:
                c.pop("chain", None)
            notes.append(f"Activity ranking: enriched {len(contracts)} contract(s)")
        except Exception as exc:
            _debug_log(debug, f"Activity ranking failed: {exc!r}")
            notes.append(f"Activity ranking failed: {exc}")
            for c in contracts:
                c.pop("chain", None)

    # Group multi-chain deployments of the same contract.
    contracts = _group_multi_deployments(contracts)

    if not contracts:
        notes.append("No inventory contracts extracted from selected pages")
    notes.append(f"Tavily queries used: {queries_used[0]}/{max_queries}")
    _debug_log(
        debug,
        (
            f"Completed inventory discovery: pages={len(selected_urls)}, "
            f"entries={len(entries)}, contracts={len(contracts)}, "
            f"queries_used={queries_used[0]}/{max_queries}, errors={len(errors)}"
        ),
    )

    # Invert sources map for output: {id: url}.
    sources_by_id = {sid: url for url, sid in sources_map.items()}

    return {
        "company": clean_company,
        "chain": requested_chain or "any",
        "official_domain": official_domain,
        "domain_candidates": domain_candidates,
        "pages_considered": considered_urls[:10],
        "pages_selected": selected_urls[:5],
        "sources": sources_by_id,
        "contracts": contracts,
        "errors": errors[:12],
        "notes": notes[:12],
        "warning": (
            "Inventory discovery extracts officially published contract addresses from "
            "selected protocol pages but may miss contracts or mislabel entries. Always "
            "verify critical addresses against the protocol's canonical documentation."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover protocol contract inventories")
    parser.add_argument("company", help="Company, protocol name, or official domain")
    parser.add_argument("--chain", default=None, help="Optional chain filter")
    parser.add_argument("--limit", type=int, default=100, help="Max contracts to return")
    parser.add_argument("--max-queries", type=int, default=4, help="Tavily query cap (default: 4)")
    parser.add_argument("--debug", action="store_true", help="Print debug logs to stderr")
    args = parser.parse_args()

    try:
        result = search_protocol_inventory(
            args.company,
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
