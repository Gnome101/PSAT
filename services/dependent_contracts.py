#!/usr/bin/env python3
"""Discover statically embedded dependent contract addresses from EVM bytecode."""

import argparse
import json
import os
import time
import sys
import hashlib
import urllib.error
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

from db.db_manager import DatabaseManager

EMPTY_CODE_VALUES = {"0x", "0x0"}
RETRYABLE_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}
RPC_TIMEOUT_SECONDS = 8
DEFAULT_PUBLIC_RPCS_BY_NETWORK = {
    "ethereum": (
        "https://eth.llamarpc.com",
        "https://rpc.flashbots.net",
        "https://eth-mainnet.public.blastapi.io",
        "https://cloudflare-eth.com",
        "https://ethereum-rpc.publicnode.com",
    ),
    "base": (
        "https://mainnet.base.org",
        "https://base.llamarpc.com",
        "https://base-rpc.publicnode.com",
    ),
    "optimism": (
        "https://mainnet.optimism.io",
        "https://optimism.llamarpc.com",
        "https://optimism-rpc.publicnode.com",
    ),
    "arbitrum": (
        "https://arb1.arbitrum.io/rpc",
        "https://arbitrum.llamarpc.com",
        "https://arbitrum-one-rpc.publicnode.com",
    ),
    "polygon": (
        "https://polygon-rpc.com",
        "https://polygon.llamarpc.com",
        "https://polygon-bor-rpc.publicnode.com",
    ),
}

# Normalize an Ethereum address to lowercase with a single 0x prefix for consistent comparisons/caching.
def normalize_address(address: str) -> str:
    return "0x" + address.lower().replace("0x", "", 1)

# Return True if an eth_getCode response represents deployed contract bytecode (not an empty/non-contract result).
def has_deployed_code(bytecode_hex: str) -> bool:
    return bytecode_hex not in EMPTY_CODE_VALUES

# Send a JSON-RPC POST request to the given RPC endpoint with retries/backoff and return the response "result" field.
def rpc_call(rpc_url: str, method: str, params: list, retries: int = 1) -> str:
    payload = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    ).encode("utf-8")
    request = urllib.request.Request(
        rpc_url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "getContractAddresses/1.0",
        },
    )
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=RPC_TIMEOUT_SECONDS) as response:
                body = json.loads(response.read().decode("utf-8"))
            if body.get("error"):
                raise RuntimeError(str(body["error"]))
            return body.get("result") or "0x"
        except urllib.error.HTTPError as exc:
            if exc.code in RETRYABLE_HTTP_CODES and attempt < retries:
                time.sleep(0.3 * (2**attempt))
                continue
            raise RuntimeError(f"RPC request failed for {rpc_url}: HTTP Error {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt < retries:
                time.sleep(0.3 * (2**attempt))
                continue
            raise RuntimeError(f"RPC request failed for {rpc_url}: {exc}") from exc

# Fetch the deployed EVM bytecode at an address via eth_getCode.
def get_code(rpc_url: str, address: str) -> str:
    return rpc_call(rpc_url, "eth_getCode", [address, "latest"])

# Pick an RPC endpoint (custom or from known public lists) where the given address has deployed bytecode, returning (network, rpc_url).
def resolve_rpc_for_address(address: str, rpc_url: str | None = None) -> tuple[str, str]:
    address = normalize_address(address)
    if rpc_url:
        if not has_deployed_code(get_code(rpc_url, address)):
            raise RuntimeError(f"Address {address} has no deployed bytecode.")
        return "custom", rpc_url

    errors = []
    for network, candidates in DEFAULT_PUBLIC_RPCS_BY_NETWORK.items():
        for candidate_rpc in candidates:
            try:
                if has_deployed_code(get_code(candidate_rpc, address)):
                    return network, candidate_rpc
            except RuntimeError as exc:
                errors.append(f"{network}: {exc}")

    message = f"Could not find deployed bytecode for {address} on known public RPC endpoints."
    if errors:
        message += " " + " | ".join(errors[:3])
    raise RuntimeError(message)

# Parse EVM bytecode and extract any 20-byte constants pushed with PUSH20 (0x73) that look like embedded addresses.
def extract_push20_addresses(bytecode_hex: str) -> set[str]:
    raw = bytecode_hex[2:] if bytecode_hex.startswith("0x") else bytecode_hex
    if len(raw) % 2 != 0:
        return set()
    code = bytes.fromhex(raw) if raw else b""

    out = set()
    i = 0
    while i < len(code):
        op = code[i]
        if op == 0x73 and i + 20 < len(code):
            out.add("0x" + code[i + 1 : i + 21].hex())
            i += 21
            continue
        if 0x60 <= op <= 0x7F:
            i += 1 + (op - 0x5F)
            continue
        i += 1

    out.discard("0x" + ("0" * 40))
    return out

# Starting from a root contract, traverse reachable embedded PUSH20 addresses and return the set of those that are deployed contracts.
def discover_dependencies(rpc_url: str, root: str) -> tuple[list[str], dict[str, str], dict[str, str]]:
    root = normalize_address(root)
    code_cache: dict[str, str] = {}

    # Cache eth_getCode lookups so repeated scans don’t spam the RPC endpoint.
    def cached_get_code(address: str) -> str:
        normalized = normalize_address(address)
        if normalized not in code_cache:
            code_cache[normalized] = get_code(rpc_url, normalized)
        return code_cache[normalized]

    if not has_deployed_code(cached_get_code(root)):
        raise RuntimeError(f"Address {root} has no deployed bytecode.")

    stack = [root]
    seen = {root}
    deps = set()
    parent_map: dict[str, str] = {}

    while stack:
        current = stack.pop()
        for candidate in extract_push20_addresses(cached_get_code(current)):
            candidate = normalize_address(candidate)
            if candidate in seen:
                continue
            seen.add(candidate)
            if has_deployed_code(cached_get_code(candidate)):
                deps.add(candidate)
                parent_map[candidate] = current
                stack.append(candidate)

    return sorted(deps), code_cache, parent_map


def find_dependencies(address: str, rpc_url: str | None = None) -> dict:
    """Resolve an RPC endpoint and return discovered static contract dependencies."""
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    env_rpc = os.getenv("ETH_RPC")
    effective_rpc = rpc_url or env_rpc

    if effective_rpc:
        try:
            network, resolved_rpc = resolve_rpc_for_address(address, effective_rpc)
        except RuntimeError:
            if rpc_url:
                raise
            network, resolved_rpc = resolve_rpc_for_address(address, None)
    else:
        network, resolved_rpc = resolve_rpc_for_address(address, None)

    deps = discover_dependencies(resolved_rpc, address)

    output = {
        "address": normalize_address(address),
        "dependencies": deps,
        "rpc": resolved_rpc,
    }
    if network != "custom":
        output["network"] = network
    return output

def persist_static_results(db: DatabaseManager, protocol_id: int, results: dict,) -> dict:
    """
    Persist the output of find_dependencies() into the PSAT database and 
    returns a summary dict with inserted row counts and IDs.
    """
    from typing import Any

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

    chain = results.get("chain", results.get("network", "unknown"))
    code_cache: dict[str, str] = results.get("code_cache", {})
    parent_map: dict[str, str] = results.get("parent_map", {})

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
        "type": "rpc_getcode",
        "url": results["rpc"],
        "authority_score": 95.0,
    })
    summary["source_ids"].append(rpc_source["id"])

    address_doc_map: dict[str, int] = {}

    for address, bytecode in code_cache.items():
        if not has_deployed_code(bytecode):
            continue

        content_hash = hashlib.sha256(bytecode.encode()).hexdigest()
        doc = db.insert("document", {
            "source_id": rpc_source["id"],
            "format": "evm_bytecode",
            "content_hash": f"sha256:{content_hash}",
            "storage_path": f"bytecode/{chain}/{address}.bin",
        })
        address_doc_map[address] = doc["id"]
        summary["document_ids"].append(doc["id"])

    for dep_address in results.get("dependencies", []):
        dep_contract = db.insert("contract", {
            "protocol_id": protocol_id,
            "address": dep_address,
            "chain": chain,
            "is_proxy": False,
        })
        summary["dependency_contract_ids"].append(dep_contract["id"])

        parent_address = parent_map.get(dep_address, results["address"])

        evidence = db.insert("evidence", {
            "reference": f"PUSH20 {dep_address} in bytecode of {parent_address}",
            "type": "push20",
            "checksum": hashlib.sha256(
                f"{parent_address}:{dep_address}".encode()
            ).hexdigest(),
        })
        summary["evidence_ids"].append(evidence["id"])

        doc_id = address_doc_map.get(parent_address)
        if doc_id is None and summary["document_ids"]:
            doc_id = summary["document_ids"][0]

        if doc_id is not None:
            claim = db.insert("claims", {
                "document_id": doc_id,
                "category": "static_dependency",
                "value": json.dumps({
                    "target": results["address"],
                    "dependency": dep_address,
                    "found_in": parent_address,
                    "method": "push20_extraction",
                }),
                "confidence": 1.0,
            })
            summary["claim_ids"].append(claim["id"])

            db.link_claim_evidence(claim["id"], evidence["id"])
            summary["links_created"] += 1

    return summary

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("address")
    parser.add_argument("--rpc")

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
        output = find_dependencies(args.address.strip(), args.rpc)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    printable = {k: v for k, v in output.items() if k not in ("code_cache", "parent_map")}
    print(json.dumps(printable))

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

            # Verify the protocol exists
            protocol = db.get_by_id("protocol", args.protocol_id)
            if protocol is None:
                raise SystemExit(
                    f"Protocol with id={args.protocol_id} not found. "
                    f"Create it first:\n"
                    f'  python -c "from db.db_manager import DatabaseManager; '
                    f"db = DatabaseManager(); db.initialize(); "
                    f"print(db.insert('protocol', {{'name': 'MyProtocol', 'chains': ['ethereum']}}))"
                )

            summary = persist_static_results(db, args.protocol_id, output)

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
