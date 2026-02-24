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
"""

import argparse
import csv
import json
import sys
from pathlib import Path

from services.fetcher import fetch, scaffold
from services.analyzer import analyze
from services.llm_analyzer import analyze_with_llm
from services.dependent_contracts import find_dependencies


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
):
    """Fetch, scaffold, discover dependencies, and run analyzers."""
    steps = 3 + int(run_deps) + int(run_llm)
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


def main():
    parser = argparse.ArgumentParser(description="Fetch and analyze smart contracts")
    parser.add_argument("address", nargs="?", help="Single Ethereum contract address")
    parser.add_argument("--name", help="Project name (single address mode)")
    parser.add_argument("--file", help="Path to JSON or CSV file with addresses")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM analysis")
    parser.add_argument("--no-deps", action="store_true", help="Skip static dependency discovery")
    parser.add_argument("--deps-rpc", help="Optional RPC URL for dependency discovery")
    args = parser.parse_args()

    if not args.address and not args.file:
        parser.print_help()
        sys.exit(1)

    run_llm = not args.no_llm
    run_deps = not args.no_deps

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
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
