#!/usr/bin/env python3
"""Discover dynamic runtime dependencies by tracing representative transactions."""

import argparse
import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from utils.etherscan import get as etherscan_get

from .static_dependencies import (
    get_code,
    has_deployed_code,
    normalize_address,
    rpc_call,
)

TRACE_OPS = {"CALL", "STATICCALL", "DELEGATECALL", "CALLCODE", "CREATE", "CREATE2"}

# EVM precompile addresses (ecrecover, sha256, ripemd160, identity, etc.)
_MAX_PRECOMPILE_ADDR = 9


def _is_precompile(address: str) -> bool:
    """Return True if address is an EVM precompile (0x01-0x09)."""
    try:
        val = int(address, 16)
        return 0 < val <= _MAX_PRECOMPILE_ADDR
    except (ValueError, TypeError):
        return False


def _normalize_maybe_address(address: Any) -> str | None:
    if not isinstance(address, str):
        return None
    raw = address.lower()
    if raw.startswith("0x"):
        raw = raw[2:]
    if len(raw) != 40:
        return None
    try:
        bytes.fromhex(raw)
    except ValueError:
        return None
    return "0x" + raw


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        if value.startswith("0x"):
            return int(value, 16)
        return int(value)
    raise RuntimeError(f"Unsupported numeric value: {value!r}")


def _tx_selector(input_data: Any) -> str:
    if isinstance(input_data, str) and input_data.startswith("0x") and len(input_data) >= 10:
        return input_data[:10].lower()
    return "0x"


def fetch_contract_transactions(address: str, limit: int = 30) -> list[dict]:
    """Fetch recent normal and internal transactions for an address from Etherscan."""
    txs: list[dict] = []
    for action in ("txlist", "txlistinternal"):
        try:
            data = etherscan_get(
                "account",
                action,
                address=address,
                startblock=0,
                endblock=99_999_999,
                page=1,
                offset=max(20, limit),
                sort="desc",
            )
        except RuntimeError as exc:
            if "No transactions found" in str(exc):
                continue
            raise
        result = data.get("result", [])
        if isinstance(result, list):
            txs.extend(result)
    return txs


def pick_representative_transactions(address: str, transactions: list[dict], max_txs: int = 5) -> list[dict]:
    """Select representative successful txs to the target, prioritizing selector diversity."""
    target = normalize_address(address)
    unique_selector_seen = set()
    selected = []
    fallback = []

    for tx in transactions:
        tx_to = _normalize_maybe_address(tx.get("to"))
        if tx_to != target:
            continue
        if str(tx.get("isError", "0")) != "0":
            continue

        tx_hash = tx.get("hash")
        if not tx_hash:
            continue

        record = {
            "tx_hash": tx_hash,
            "block_number": _to_int(tx.get("blockNumber")),
            "method_selector": _tx_selector(tx.get("input")),
        }
        fallback.append(record)

        selector = record["method_selector"]
        if selector in unique_selector_seen:
            continue
        unique_selector_seen.add(selector)
        selected.append(record)
        if len(selected) >= max_txs:
            return selected

    for record in fallback:
        if len(selected) >= max_txs:
            break
        if record in selected:
            continue
        selected.append(record)

    return selected


def resolve_trace_rpc(rpc_url: str | None = None) -> str:
    """Resolve a tracing-capable RPC URL from arg or ETH_RPC."""
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    if rpc_url:
        return rpc_url
    env_rpc = os.getenv("ETH_RPC")
    if env_rpc:
        return env_rpc
    raise RuntimeError("Dynamic dependency discovery requires --dynamic-rpc or ETH_RPC.")


def _fetch_tx_metadata_from_rpc(rpc_url: str, tx_hash: str) -> dict:
    tx = rpc_call(rpc_url, "eth_getTransactionByHash", [tx_hash], retries=1)
    if not isinstance(tx, dict) or not tx.get("hash"):
        raise RuntimeError(f"Could not fetch transaction metadata for {tx_hash}")
    return {
        "tx_hash": tx["hash"],
        "block_number": _to_int(tx.get("blockNumber")),
        "method_selector": _tx_selector(tx.get("input")),
    }


def trace_transaction(rpc_url: str, tx_hash: str) -> tuple[str, Any]:
    """Trace a transaction using debug_traceTransaction or trace_transaction."""
    attempts = [
        ("debug_traceTransaction", [tx_hash, {"tracer": "callTracer", "timeout": "20s"}]),
        ("debug_traceTransaction", [tx_hash, {"tracer": "callTracer"}]),
        ("trace_transaction", [tx_hash]),
    ]
    errors = []
    for method, params in attempts:
        try:
            return method, rpc_call(rpc_url, method, params, retries=0)
        except RuntimeError as exc:
            errors.append(f"{method}: {exc}")
    raise RuntimeError(f"Tracing failed for {tx_hash}: {' | '.join(errors)}")


def _parse_debug_call_tree(node: Any, tx_hash: str, block_number: int | None, out_edges: list[dict]) -> None:
    if not isinstance(node, dict):
        return

    op = str(node.get("type", "")).upper()
    src = _normalize_maybe_address(node.get("from"))
    dst = _normalize_maybe_address(node.get("to"))
    if op in TRACE_OPS and src and dst:
        out_edges.append(
            {
                "from": src,
                "to": dst,
                "op": op,
                "tx_hash": tx_hash,
                "block_number": block_number,
            }
        )

    for child in node.get("calls", []) or []:
        _parse_debug_call_tree(child, tx_hash, block_number, out_edges)


def _parse_parity_trace_entries(entries: Any, tx_hash: str, block_number: int | None, out_edges: list[dict]) -> None:
    if not isinstance(entries, list):
        return

    for item in entries:
        if not isinstance(item, dict):
            continue
        trace_type = str(item.get("type", "")).lower()
        action = item.get("action", {}) or {}

        src = _normalize_maybe_address(action.get("from"))
        dst = None
        op = None

        if trace_type == "call":
            op = str(action.get("callType", "call")).upper()
            dst = _normalize_maybe_address(action.get("to"))
        elif trace_type == "create":
            creation_method = str(action.get("creationMethod", "create")).upper()
            op = "CREATE2" if creation_method == "CREATE2" else "CREATE"
            result = item.get("result", {}) or {}
            dst = _normalize_maybe_address(result.get("address"))

        if op in TRACE_OPS and src and dst:
            out_edges.append(
                {
                    "from": src,
                    "to": dst,
                    "op": op,
                    "tx_hash": tx_hash,
                    "block_number": block_number,
                }
            )


def _dedupe_edges(edges: list[dict]) -> list[dict]:
    seen = set()
    deduped = []
    for edge in edges:
        key = (
            edge["from"],
            edge["to"],
            edge["op"],
            edge["tx_hash"],
            edge["block_number"],
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(edge)
    return deduped


def extract_edges_from_trace(
    trace_method: str, trace_result: Any, tx_hash: str, block_number: int | None
) -> list[dict]:
    """Extract call/create edges from a trace result."""
    out_edges = []
    if trace_method == "trace_transaction":
        _parse_parity_trace_entries(trace_result, tx_hash, block_number, out_edges)
    else:
        _parse_debug_call_tree(trace_result, tx_hash, block_number, out_edges)
    return _dedupe_edges(out_edges)


def _build_graph(edges: list[dict]) -> list[dict]:
    graph_map: dict[tuple[str, str, str], list[dict]] = {}
    for edge in edges:
        key = (edge["from"], edge["to"], edge["op"])
        provenance = {"tx_hash": edge["tx_hash"], "block_number": edge["block_number"]}
        graph_map.setdefault(key, [])
        if provenance not in graph_map[key]:
            graph_map[key].append(provenance)

    graph = []
    for key in sorted(graph_map.keys()):
        src, dst, op = key
        graph.append(
            {
                "from": src,
                "to": dst,
                "op": op,
                "provenance": sorted(
                    graph_map[key],
                    key=lambda p: (
                        p["block_number"] if p["block_number"] is not None else -1,
                        p["tx_hash"],
                    ),
                ),
            }
        )
    return graph


def find_dynamic_dependencies(
    address: str,
    rpc_url: str | None = None,
    tx_limit: int = 5,
    tx_hashes: list[str] | None = None,
) -> dict:
    """Trace representative transactions and return a dynamic dependency graph."""
    if tx_limit < 1:
        raise RuntimeError("tx_limit must be >= 1")

    target = normalize_address(address)
    trace_rpc = resolve_trace_rpc(rpc_url)

    if tx_hashes:
        selected_txs = [_fetch_tx_metadata_from_rpc(trace_rpc, tx_hash.strip()) for tx_hash in tx_hashes]
    else:
        txs = fetch_contract_transactions(target, limit=max(20, tx_limit * 6))
        selected_txs = pick_representative_transactions(target, txs, max_txs=tx_limit)

    if not selected_txs:
        raise RuntimeError(f"No representative transactions found for {target}")

    all_edges = []
    trace_methods = set()
    trace_errors: list[dict] = []
    for tx in selected_txs:
        tx_hash = tx["tx_hash"]
        block_number = tx["block_number"]
        try:
            method, trace_result = trace_transaction(trace_rpc, tx_hash)
            trace_methods.add(method)
            all_edges.extend(extract_edges_from_trace(method, trace_result, tx_hash, block_number))
        except RuntimeError as exc:
            trace_errors.append({"tx_hash": tx_hash, "error": str(exc)})
            print(f"         Warning: trace failed for {tx_hash}: {exc}")

    if not trace_methods and trace_errors:
        raise RuntimeError(
            f"All {len(trace_errors)} transaction trace(s) failed. First error: {trace_errors[0]['error']}"
        )

    edges = _dedupe_edges(all_edges)

    # Keep only direct calls from the target contract.  Intermediate calls
    # between dependencies (e.g. DEX pool → oracle) are trace noise — they
    # don't represent what the *target* depends on.
    direct_edges = [edge for edge in edges if edge["from"] == target and edge["to"] != target]

    # Filter out precompiles and addresses with no deployed code (EOAs)
    dep_candidates = sorted({edge["to"] for edge in direct_edges})
    contracts = {
        addr for addr in dep_candidates if not _is_precompile(addr) and has_deployed_code(get_code(trace_rpc, addr))
    }
    direct_edges = [edge for edge in direct_edges if edge["to"] in contracts]
    dependencies = sorted(contracts)

    provenance: dict[str, list[dict]] = {}
    for edge in direct_edges:
        dep = edge["to"]
        record = {
            "tx_hash": edge["tx_hash"],
            "block_number": edge["block_number"],
            "from": edge["from"],
            "op": edge["op"],
        }
        provenance.setdefault(dep, [])
        if record not in provenance[dep]:
            provenance[dep].append(record)

    for dep in provenance:
        provenance[dep] = sorted(
            provenance[dep],
            key=lambda p: (
                p["block_number"] if p["block_number"] is not None else -1,
                p["tx_hash"],
                p["from"],
                p["op"],
            ),
        )

    return {
        "address": target,
        "rpc": trace_rpc,
        "transactions_analyzed": selected_txs,
        "trace_methods": sorted(trace_methods),
        "dependencies": dependencies,
        "provenance": provenance,
        "dependency_graph": _build_graph(direct_edges),
        "trace_errors": trace_errors,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("address")
    parser.add_argument("--rpc")
    parser.add_argument("--tx-limit", type=int, default=5)
    parser.add_argument("--tx-hash", action="append", dest="tx_hashes")
    args = parser.parse_args()

    try:
        output = find_dynamic_dependencies(
            args.address.strip(),
            rpc_url=args.rpc,
            tx_limit=args.tx_limit,
            tx_hashes=args.tx_hashes,
        )
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    print(json.dumps(output))


if __name__ == "__main__":
    main()
