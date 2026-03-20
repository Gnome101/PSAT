#!/usr/bin/env python3
"""
PSAT - Pipeline for Smart Contract Analysis

Usage:
    # Single address
    python main.py 0xdAC17F958D2ee523a2206206994597C13D831ec7

    # From a JSON file (list of {address, name})
    python main.py --file addresses.json

    # From a CSV file (columns: address, name)
    python main.py --file addresses.csv

    # Skip LLM analysis (slither only)
    python main.py 0x... --no-llm

    # Skip dependency discovery
    python main.py 0x... --no-deps

    # Dynamic dependency tracing with a tracing-enabled RPC
    python main.py 0x... --dynamic-rpc https://your-trace-rpc --dynamic-tx-limit 5
"""

import argparse
import csv
import json
import sys
from pathlib import Path

from services.analyzer import analyze
from services.contract_inventory_ai import search_protocol_inventory
from services.dependent_contracts import find_dependencies
from services.dynamic_dependencies import find_dynamic_dependencies
from services.fetcher import fetch, scaffold
from services.llm_analyzer import analyze_with_llm


def load_addresses(filepath: str) -> list[dict]:
    """Load addresses from a JSON or CSV file.

    Expected formats:
        JSON: [{"address": "0x...", "name": "USDT"}, ...]
        CSV:  address,name  (header row)
    """
    path = Path(filepath)
    if not path.exists():
        sys.exit(f"File not found: {filepath}")

    if path.suffix == ".json":
        with open(path) as f:
            data = json.load(f)
        return [{"address": e["address"], "name": e.get("name")} for e in data]

    if path.suffix == ".csv":
        with open(path) as f:
            reader = csv.DictReader(f)
            return [{"address": row["address"], "name": row.get("name")} for row in reader]

    sys.exit(f"Unsupported file type: {path.suffix} (use .json or .csv)")


def process(
    address: str,
    name: str | None = None,
    run_llm: bool = True,
    run_deps: bool = True,
    deps_rpc: str | None = None,
    run_dynamic_deps: bool = True,
    dynamic_rpc: str | None = None,
    dynamic_tx_limit: int = 5,
    dynamic_tx_hashes: list[str] | None = None,
):
    """Fetch, scaffold, discover dependencies, and run analyzers."""
    steps = 3 + int(run_deps) + int(run_dynamic_deps) + int(run_llm)
    step = 1

    print(f"\n{'─' * 50}")
    print(f"[{step}/{steps}] Fetching source for {address} ...")
    step += 1
    result = fetch(address)
    contract_name = result.get("ContractName", "Unknown")
    name = name or contract_name
    print(f"         Contract: {contract_name}")

    print(f"[{step}/{steps}] Setting up project ...")
    step += 1
    project_dir = scaffold(address, name, result)
    print(f"         -> {project_dir}")

    if run_deps:
        print(f"[{step}/{steps}] Discovering static contract dependencies ...")
        step += 1
        try:
            deps_output = find_dependencies(address, deps_rpc)
            deps_path = project_dir / "dependencies.json"
            deps_path.write_text(json.dumps(deps_output, indent=2) + "\n")
            print(f"         Dependencies: {len(deps_output['dependencies'])}")
            print(f"         Output: {deps_path}")
        except RuntimeError as exc:
            print(f"         Dependency discovery skipped: {exc}")

    if run_dynamic_deps:
        print(f"[{step}/{steps}] Discovering dynamic contract dependencies via traces ...")
        step += 1
        try:
            dyn_output = find_dynamic_dependencies(
                address,
                rpc_url=(dynamic_rpc or deps_rpc),
                tx_limit=dynamic_tx_limit,
                tx_hashes=dynamic_tx_hashes,
            )
            dyn_path = project_dir / "dynamic_dependencies.json"
            dyn_path.write_text(json.dumps(dyn_output, indent=2) + "\n")
            print(f"         Dependencies: {len(dyn_output['dependencies'])}")
            print(f"         Transactions traced: {len(dyn_output['transactions_analyzed'])}")
            print(f"         Output: {dyn_path}")
        except RuntimeError as exc:
            print(f"         Dynamic dependency discovery skipped: {exc}")

    print(f"[{step}/{steps}] Running Slither analysis ...")
    step += 1
    report_path = analyze(project_dir, contract_name, address)
    print(f"         Report: {report_path}")

    if run_llm:
        print(f"[{step}/{steps}] Running LLM flow analysis (NVIDIA NIM) ...")
        llm_report = analyze_with_llm(project_dir)
        llm_path = project_dir / "llm_analysis.md"
        llm_path.write_text(llm_report + "\n")
        print(f"         LLM report: {llm_path}")

    return project_dir


def run_discovery(args) -> None:
    """Run contract inventory discovery and print JSON results."""
    chain = getattr(args, "discover_chain", None)
    limit = getattr(args, "discover_limit", None) or 100

    result = search_protocol_inventory(args.discover_inventory, chain=chain, limit=limit)

    output = json.dumps(result, indent=2)
    print(output)

    safe_name = args.discover_inventory.replace("/", "_").replace(" ", "_")
    out_dir = Path("protocols") / safe_name
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "contract_inventory.json").write_text(output + "\n")
    print(f"\nSaved to {out_dir / 'contract_inventory.json'}")


def main():
    parser = argparse.ArgumentParser(description="Fetch and analyze smart contracts")
    parser.add_argument("address", nargs="?", help="Single Ethereum contract address")
    parser.add_argument("--name", help="Project name (single address mode)")
    parser.add_argument("--file", help="Path to JSON or CSV file with addresses")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM analysis")
    parser.add_argument("--no-deps", action="store_true", help="Skip all dependency discovery")
    parser.add_argument(
        "--no-dynamic-deps",
        action="store_true",
        help="Skip dynamic dependency discovery via transaction tracing",
    )
    parser.add_argument("--deps-rpc", help="Optional RPC URL for dependency discovery")
    parser.add_argument("--dynamic-rpc", help="Tracing-enabled RPC URL for dynamic dependency discovery")
    parser.add_argument(
        "--dynamic-tx-limit",
        type=int,
        default=5,
        help="Number of representative transactions to trace (default: 5)",
    )
    parser.add_argument(
        "--dynamic-tx-hash",
        action="append",
        dest="dynamic_tx_hashes",
        help="Specific transaction hash to trace (repeatable)",
    )

    # Contract inventory discovery flags
    parser.add_argument(
        "--discover-inventory",
        metavar="COMPANY_OR_DOMAIN",
        help="AI-powered official contract inventory discovery for a protocol (standalone)",
    )
    parser.add_argument("--discover-chain", help="Chain filter for discovery")
    parser.add_argument(
        "--discover-limit",
        type=int,
        default=None,
        help="Max discovery results to return (default: 100)",
    )

    args = parser.parse_args()

    # Discovery mode — standalone, mutually exclusive with pipeline
    if args.discover_inventory:
        try:
            run_discovery(args)
        except ValueError as exc:
            sys.exit(str(exc))
        return

    if not args.address and not args.file:
        parser.print_help()
        sys.exit(1)

    run_llm = not args.no_llm
    run_deps = not args.no_deps
    run_dynamic_deps = (not args.no_deps) and (not args.no_dynamic_deps)

    if args.dynamic_tx_limit < 1:
        sys.exit("--dynamic-tx-limit must be >= 1")

    if args.file:
        entries = load_addresses(args.file)
        print(f"Loaded {len(entries)} address(es) from {args.file}")
        for entry in entries:
            try:
                process(
                    entry["address"],
                    entry.get("name"),
                    run_llm=run_llm,
                    run_deps=run_deps,
                    deps_rpc=args.deps_rpc,
                    run_dynamic_deps=run_dynamic_deps,
                    dynamic_rpc=args.dynamic_rpc,
                    dynamic_tx_limit=args.dynamic_tx_limit,
                    dynamic_tx_hashes=args.dynamic_tx_hashes,
                )
            except Exception as e:
                print(f"  FAILED: {e}")
    else:
        process(
            args.address,
            args.name,
            run_llm=run_llm,
            run_deps=run_deps,
            deps_rpc=args.deps_rpc,
            run_dynamic_deps=run_dynamic_deps,
            dynamic_rpc=args.dynamic_rpc,
            dynamic_tx_limit=args.dynamic_tx_limit,
            dynamic_tx_hashes=args.dynamic_tx_hashes,
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
