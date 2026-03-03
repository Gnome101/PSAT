#!/usr/bin/env python3
"""Contract discovery by name using Blockscout search.

How it works:
1. Queries Blockscout's search API across supported chains (Ethereum, Arbitrum,
   Optimism, Polygon, Base) for the given contract/token name.
2. Scores each result using mutually exclusive name/symbol matching tiers with a
   length penalty for names much longer than the query (prevents scam tokens like
   "$1,000 USDC Reward" from outranking real "USDC").
3. Deduplicates same-address results across chains and collapses multi-chain
   deployments into a single candidate.
4. Returns ranked candidates with confidence scores, source links, and an
   auto-selected best_candidate when one result clearly dominates.
"""

from __future__ import annotations

import argparse
import json
import re
from typing import Any

import requests

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
HTTP_TIMEOUT_SECONDS = 20

BLOCKSCOUT_BY_CHAIN = {
    "ethereum": "https://eth.blockscout.com",
    "arbitrum": "https://arbitrum.blockscout.com",
    "optimism": "https://optimism.blockscout.com",
    "polygon": "https://polygon.blockscout.com",
    "base": "https://base.blockscout.com",
}

CHAIN_TO_EXPLORER = {
    "ethereum": "https://etherscan.io/address/{address}",
    "arbitrum": "https://arbiscan.io/address/{address}",
    "optimism": "https://optimistic.etherscan.io/address/{address}",
    "polygon": "https://polygonscan.com/address/{address}",
    "base": "https://basescan.org/address/{address}",
}

CHAIN_SORT_ORDER = {
    "ethereum": 0,
    "arbitrum": 1,
    "optimism": 2,
    "polygon": 3,
    "base": 4,
    "unknown": 99,
}


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _tokenize(value: str) -> set[str]:
    return {part for part in re.split(r"[^a-z0-9]+", value.lower()) if part}


def _is_evm_address(value: str) -> bool:
    return bool(ADDRESS_RE.match(value.strip()))


def _normalize_address(value: str) -> str:
    return "0x" + value.lower().replace("0x", "", 1)


def _chain_sort_key(chain: str) -> tuple[int, str]:
    return (CHAIN_SORT_ORDER.get(chain, 50), chain)


def _safe_get_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    response = requests.get(
        url,
        params=params,
        headers={"Accept": "application/json", "User-Agent": "PSAT/0.1"},
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def _extract_display_name(item: dict[str, Any], address: str) -> str:
    metadata = item.get("metadata")
    if isinstance(metadata, dict):
        metadata_name = metadata.get("name")
        if isinstance(metadata_name, str) and metadata_name.strip():
            return metadata_name.strip()

    name = item.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()

    return address


def _score_candidate(
    query: str,
    name: str,
    symbol: str | None,
    item_type: str,
    verified: bool,
) -> tuple[float, list[str]]:
    query_n = _normalize_text(query)
    name_n = _normalize_text(name)
    symbol_n = _normalize_text(symbol or "")

    query_tokens = _tokenize(query)
    name_tokens = _tokenize(name)

    score = 0.0
    reasons: list[str] = []

    # --- Name matching (mutually exclusive tiers) ---
    if query_n and name_n and query_n == name_n:
        score += 0.55
        reasons.append("Exact name match")
    elif query_tokens and name_tokens and query_tokens.issubset(name_tokens):
        # All query tokens found in name — penalize if name is much longer.
        extra = len(name_tokens) - len(query_tokens)
        if extra == 0:
            score += 0.50
            reasons.append("Exact token match")
        elif extra <= 2:
            score += 0.35
            reasons.append("Close name match")
        else:
            score += 0.15
            reasons.append(f"Substring match ({extra} extra tokens)")
    elif query_tokens and name_tokens:
        overlap = len(query_tokens & name_tokens) / len(query_tokens)
        if overlap >= 0.5:
            score += 0.10
            reasons.append(f"Partial token overlap ({overlap:.0%})")

    # --- Symbol matching (mutually exclusive) ---
    if query_n and symbol_n:
        if query_n == symbol_n:
            score += 0.20
            reasons.append("Exact symbol match")
        elif query_n in symbol_n or symbol_n in query_n:
            score += 0.08
            reasons.append("Partial symbol match")

    # --- Type bonus ---
    if item_type in {"metadata_tag", "contract"}:
        score += 0.20
        reasons.append(f"Explorer {item_type} match")
    elif item_type in {"token", "erc20", "erc721", "erc1155"}:
        score += 0.05
        reasons.append(f"Explorer {item_type} match")

    # --- Verified ---
    if verified:
        score += 0.05
        reasons.append("Verified contract")

    return min(score, 0.99), reasons


def _search_blockscout(query: str, chain: str | None, per_chain_limit: int = 25) -> tuple[list[dict], list[dict]]:
    if chain:
        base_url = BLOCKSCOUT_BY_CHAIN.get(chain)
        if not base_url:
            return [], [{"provider": "blockscout", "error": f"No endpoint for chain '{chain}'"}]
        targets = [(chain, base_url)]
    else:
        targets = sorted(BLOCKSCOUT_BY_CHAIN.items())

    candidates: list[dict] = []
    errors: list[dict] = []

    for chain_name, base_url in targets:
        try:
            payload = _safe_get_json(f"{base_url}/api/v2/search", params={"q": query})
        except requests.RequestException as exc:
            errors.append({"provider": "blockscout", "error": f"{chain_name}: {exc}"})
            continue

        items = payload.get("items", [])
        if not isinstance(items, list):
            continue

        for item in items[:per_chain_limit]:
            if not isinstance(item, dict):
                continue

            address = item.get("address_hash")
            if not isinstance(address, str) or not _is_evm_address(address):
                continue
            address = _normalize_address(address)

            display_name = _extract_display_name(item, address)
            symbol = item.get("symbol")
            symbol = symbol.strip() if isinstance(symbol, str) and symbol.strip() else None
            item_type = str(item.get("type", "")).lower()
            verified = bool(item.get("is_smart_contract_verified"))

            confidence, reasons = _score_candidate(query, display_name, symbol, item_type, verified)
            if confidence <= 0:
                continue

            explorer_template = CHAIN_TO_EXPLORER.get(chain_name)
            links: dict[str, str] = {"blockscout": f"{base_url}/address/{address}"}
            if explorer_template:
                links["explorer"] = explorer_template.format(address=address)

            candidates.append(
                {
                    "display_name": display_name,
                    "symbol": symbol.upper() if symbol else None,
                    "address": address,
                    "chain": chain_name,
                    "confidence": round(confidence, 4),
                    "source": "blockscout",
                    "reasons": reasons,
                    "links": links,
                }
            )

    return candidates, errors


def _dedup_and_collapse(candidates: list[dict], collapse_chains: bool) -> list[dict]:
    """Merge duplicate (chain, address) pairs and optionally collapse cross-chain."""
    # Step 1: merge same (chain, address)
    by_chain_addr: dict[tuple[str, str], dict] = {}
    for c in candidates:
        key = (c["chain"], c["address"])
        existing = by_chain_addr.get(key)
        if not existing or c["confidence"] > existing["confidence"]:
            by_chain_addr[key] = c
        elif existing:
            existing["reasons"] = sorted(set(existing["reasons"] + c["reasons"]))

    if not collapse_chains:
        return list(by_chain_addr.values())

    # Step 2: collapse same address across chains
    by_address: dict[str, list[dict]] = {}
    for c in by_chain_addr.values():
        by_address.setdefault(c["address"], []).append(c)

    collapsed: list[dict] = []
    for address, group in by_address.items():
        primary = max(group, key=lambda c: (c["confidence"], -_chain_sort_key(c["chain"])[0]))
        chains = sorted({c["chain"] for c in group}, key=_chain_sort_key)
        reasons = sorted({r for c in group for r in c["reasons"]})
        if len(chains) > 1:
            reasons.append(f"Found on {len(chains)} chains: {', '.join(chains)}")

        collapsed.append(
            {
                "display_name": primary["display_name"],
                "symbol": primary["symbol"],
                "address": address,
                "chain": primary["chain"] if len(chains) == 1 else "multi",
                "confidence": primary["confidence"],
                "source": "blockscout",
                "reasons": reasons,
                "links": primary["links"],
                **({"chains": chains} if len(chains) > 1 else {}),
            }
        )

    return collapsed


def search_contract_name(query: str, chain: str | None = None, limit: int = 10) -> dict[str, Any]:
    clean_query = query.strip()
    if not clean_query:
        raise ValueError("query must not be empty")
    if limit < 1:
        raise ValueError("limit must be >= 1")

    normalized_chain = chain.lower().strip() if isinstance(chain, str) and chain.strip() else None

    if _is_evm_address(clean_query):
        raise ValueError("query looks like an address; this script only supports name-based discovery")

    candidates, errors = _search_blockscout(clean_query, normalized_chain)
    candidates = _dedup_and_collapse(candidates, collapse_chains=normalized_chain is None)

    ranked = sorted(
        candidates,
        key=lambda c: (-c["confidence"], c["display_name"].lower(), c["address"]),
    )[:limit]

    # Auto-select if one candidate clearly dominates
    best_candidate = None
    if len(ranked) == 1:
        best_candidate = ranked[0]
    elif len(ranked) >= 2:
        first, second = ranked[0], ranked[1]
        if first["confidence"] >= 0.90 and (first["confidence"] - second["confidence"]) >= 0.12:
            best_candidate = first

    return {
        "query": clean_query,
        "chain": normalized_chain or "any",
        "best_candidate": best_candidate,
        "candidates": ranked,
        "errors": errors[:5],
        "warning": (
            "Name-based discovery relies on block explorer indexing and may return "
            "incorrect results. Always verify addresses against official project documentation."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Search contract addresses by contract name")
    parser.add_argument("query", help="Contract or token name")
    parser.add_argument("--chain", default=None, help="Optional chain filter (default: all known chains)")
    parser.add_argument("--limit", type=int, default=10, help="Max candidates to return")
    args = parser.parse_args()

    try:
        result = search_contract_name(args.query, chain=args.chain, limit=args.limit)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
