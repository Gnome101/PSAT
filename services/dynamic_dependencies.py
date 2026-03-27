#!/usr/bin/env python3
"""Discover dynamic runtime dependencies by tracing representative transactions."""

import argparse
import json
import os
import sys
import hashlib
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from db.db_manager import DatabaseManager
from services.dependent_contracts import normalize_address, rpc_call
from utils.etherscan import get as etherscan_get

TRACE_OPS = {"CALL", "STATICCALL", "DELEGATECALL", "CALLCODE", "CREATE", "CREATE2"}


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
    """Fetch recent normal transactions for an address from Etherscan."""
    try:
        data = etherscan_get(
            "account",
            "txlist",
            address=address,
            startblock=0,
            endblock=99_999_999,
            page=1,
            offset=max(20, limit),
            sort="desc",
        )
    except RuntimeError as exc:
        # Etherscan returns status=0 for empty tx history.
        if "No transactions found" in str(exc):
            return []
        raise

    result = data.get("result", [])
    if not isinstance(result, list):
        return []
    return result


def pick_representative_transactions(
    address: str, transactions: list[dict], max_txs: int = 5
) -> list[dict]:
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


def _parse_debug_call_tree(
    node: Any, tx_hash: str, block_number: int | None, out_edges: list[dict]
) -> None:
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


def _parse_parity_trace_entries(
    entries: Any, tx_hash: str, block_number: int | None, out_edges: list[dict]
) -> None:
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
    address: str, rpc_url: str | None = None, tx_limit: int = 5, tx_hashes: list[str] | None = None,
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
            f"All {len(trace_errors)} transaction trace(s) failed. "
            f"First error: {trace_errors[0]['error']}"
        )

    edges = _dedupe_edges(all_edges)
    dependency_edges = [edge for edge in edges if edge["to"] != target]
    dependencies = sorted({edge["to"] for edge in dependency_edges})

    provenance: dict[str, list[dict]] = {}
    for edge in dependency_edges:
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
        "dependency_graph": _build_graph(dependency_edges),
        "trace_errors": trace_errors,
    }

def persist_dynamic_results(
    db: DatabaseManager,
    protocol_id: int,
    results: dict,
) -> dict:
    """
    Persist the output of find_dynamic_dependencies() into the PSAT database and 
    returns a summary dict with inserted row counts and IDs.
    """
    summary: dict[str, Any] = {
        "protocol_id": protocol_id,
        "target_contract_id": None,
        "source_ids": [],
        "document_ids": [],
        "dependency_contract_ids": [],
        "claim_ids": [],
        "evidence_ids": [],
        "links_created": 0,
    }

    chain = results.get("chain", results.get("network", "ethereum"))

    target_contract = db.insert("contract", {
        "protocol_id": protocol_id,
        "address": results["address"],
        "chain": chain,
        "is_proxy": False,
    })
    summary["target_contract_id"] = target_contract["id"]
    db.add_chain(protocol_id, chain)

    rpc_source = db.insert("source", {
        "protocol_id": protocol_id,
        "type": "rpc_trace",
        "url": results["rpc"],
        "authority_score": 90.0,
    })
    summary["source_ids"].append(rpc_source["id"])

    etherscan_source = None
    if not any(e.get("error") for e in results.get("trace_errors", [])):
        etherscan_source = db.insert("source", {
            "protocol_id": protocol_id,
            "type": "etherscan",
            "url": f"https://api.etherscan.io/api?module=account&action=txlist&address={results['address']}",
            "authority_score": 85.0,
        })
        summary["source_ids"].append(etherscan_source["id"])

    tx_document_map: dict[str, int] = {}
    source_id_for_docs = rpc_source["id"]

    for tx in results.get("transactions_analyzed", []):
        tx_hash = tx["tx_hash"]
        content = json.dumps(tx, sort_keys=True)
        content_hash = hashlib.sha256(content.encode()).hexdigest()

        doc = db.insert("document", {
            "source_id": source_id_for_docs,
            "format": "json",
            "content_hash": f"sha256:{content_hash}",
            "storage_path": f"traces/{results['address']}/{tx_hash}.json",
        })
        tx_document_map[tx_hash] = doc["id"]
        summary["document_ids"].append(doc["id"])

    for dep_address in results.get("dependencies", []):
        dep_contract = db.insert("contract", {
            "protocol_id": protocol_id,
            "address": dep_address,
            "chain": chain,
            "is_proxy": False,
        })
        summary["dependency_contract_ids"].append(dep_contract["id"])

        prov_records = results.get("provenance", {}).get(dep_address, [])
        evidence_ids_for_dep: list[int] = []

        for prov in prov_records:
            evidence = db.insert("evidence", {
                "reference": (
                    f"tx:{prov['tx_hash']} "
                    f"block:{prov['block_number']} "
                    f"from:{prov['from']} "
                    f"op:{prov['op']}"
                ),
                "type": prov["op"].lower(),
                "checksum": prov["tx_hash"],
            })
            evidence_ids_for_dep.append(evidence["id"])
            summary["evidence_ids"].append(evidence["id"])

        first_tx_hash = prov_records[0]["tx_hash"] if prov_records else None
        doc_id = tx_document_map.get(first_tx_hash)

        if doc_id is None and summary["document_ids"]:
            doc_id = summary["document_ids"][0]

        if doc_id is not None:
            total_txs = len(results.get("transactions_analyzed", []))
            observed_txs = len({p["tx_hash"] for p in prov_records})
            confidence = round(observed_txs / max(total_txs, 1), 4)

            claim = db.insert("claims", {
                "document_id": doc_id,
                "category": "dynamic_dependency",
                "value": json.dumps({
                    "target": results["address"],
                    "dependency": dep_address,
                    "ops": sorted({p["op"] for p in prov_records}),
                    "tx_count": observed_txs,
                }),
                "confidence": confidence,
            })
            summary["claim_ids"].append(claim["id"])

            for eid in evidence_ids_for_dep:
                db.link_claim_evidence(claim["id"], eid)
                summary["links_created"] += 1

    return summary

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("address")
    parser.add_argument("--rpc")
    parser.add_argument("--tx-limit", type=int, default=5)
    parser.add_argument("--tx-hash", action="append", dest="tx_hashes")

    parser.add_argument("--save", action="store_true", default=False, help="Persist results to the PSAT database")
    parser.add_argument("--protocol-id", type=int, default=None, help="Protocol ID to associate results with (required with --save)")
    parser.add_argument("--db-name", default="psat_db", help="Database name")
    parser.add_argument("--db-user", default="postgres", help="Database user")
    parser.add_argument("--db-password", default="postgres", help="Database password")
    parser.add_argument("--db-host", default="localhost", help="Database host")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port")

    parser.add_argument("--db-url", default=None, help="PostgreSQL connection string (overrides individual --db-* args)")

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

    if args.save:
        if args.protocol_id is None:
            raise SystemExit("--protocol-id is required when using --save")
        
        if args.db_url:
            db = DatabaseManager(dsn=args.db_url)
        else:
            db = DatabaseManager(
                dbname=args.db_name,
                user=args.db_user,
                password=args.db_password,
                host=args.db_host,
                port=args.db_port,
            )

        try:
            db.initialize()

            protocol = db.get_by_id("protocol", args.protocol_id)
            if protocol is None:
                raise SystemExit(
                    f"Protocol with id={args.protocol_id} not found. "
                    f"Create it first:\n"
                    f"  python -c \"from db.db_manager import DatabaseManager; "
                    f"db = DatabaseManager(); db.initialize(); "
                    f"print(db.insert('protocol', {{'name': 'MyProtocol', 'chain': 'ethereum'}}))\""
                )

            summary = persist_dynamic_results(db, args.protocol_id, output)

            print(f"\nSaved to database:", file=sys.stderr)
            print(f"  Target contract ID : {summary['target_contract_id']}", file=sys.stderr)
            print(f"  Sources created    : {len(summary['source_ids'])}", file=sys.stderr)
            print(f"  Documents created  : {len(summary['document_ids'])}", file=sys.stderr)
            print(f"  Dependencies stored: {len(summary['dependency_contract_ids'])}", file=sys.stderr)
            print(f"  Claims created     : {len(summary['claim_ids'])}", file=sys.stderr)
            print(f"  Evidence created   : {len(summary['evidence_ids'])}", file=sys.stderr)
            print(f"  Links created      : {summary['links_created']}", file=sys.stderr)

        except Exception as exc:
            raise SystemExit(f"Database error: {exc}") from exc
        finally:
            db.close()


if __name__ == "__main__":
    main()
