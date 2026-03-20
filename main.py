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
from services.classifier import classify_contracts
from services.contract_inventory_ai import search_protocol_inventory
from services.dependent_contracts import find_dependencies, normalize_address
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
            return [
                {"address": row["address"], "name": row.get("name")} for row in reader
            ]

    sys.exit(f"Unsupported file type: {path.suffix} (use .json or .csv)")


_CLS_KEYS = ("proxy_type", "implementation", "beacon", "admin", "proxies", "facets")


def _build_unified_deps(
    address: str,
    static_deps: dict | None,
    dynamic_deps: dict | None,
    classifications: dict | None,
) -> dict:
    """Merge static deps, dynamic deps, and classifications into one output."""
    target = normalize_address(address)

    # Collect dependency addresses with their discovery source
    dep_sources: dict[str, set[str]] = {}
    if static_deps:
        for dep in static_deps.get("dependencies", []):
            dep_sources.setdefault(normalize_address(dep), set()).add("static")
    if dynamic_deps:
        for dep in dynamic_deps.get("dependencies", []):
            dep_sources.setdefault(normalize_address(dep), set()).add("dynamic")

    cls_map = (classifications or {}).get("classifications", {})
    dyn_provenance = (dynamic_deps or {}).get("provenance", {})

    def _dep_entry(addr: str, sources: list[str]) -> dict:
        entry: dict = {"type": "regular", "source": sources}
        if addr in cls_map:
            c = cls_map[addr]
            entry["type"] = c.get("type", "regular")
            for k in _CLS_KEYS:
                if k in c:
                    entry[k] = c[k]
        if addr in dyn_provenance:
            entry["provenance"] = dyn_provenance[addr]
        return entry

    deps = {
        addr: _dep_entry(addr, sorted(sources))
        for addr, sources in sorted(dep_sources.items())
    }

    # Include addresses discovered by classification (not in original dep lists)
    discovered = (classifications or {}).get("discovered_addresses", [])
    for addr in discovered:
        addr = normalize_address(addr)
        if addr not in deps:
            deps[addr] = _dep_entry(addr, ["classification"])

    output: dict = {"address": target}

    # Target contract classification (only if non-regular)
    if target in cls_map and cls_map[target].get("type", "regular") != "regular":
        tc = cls_map[target]
        info = {"type": tc["type"]}
        for k in ("proxy_type", "implementation", "beacon", "admin"):
            if k in tc:
                info[k] = tc[k]
        output["target_classification"] = info

    output["dependencies"] = deps

    if dynamic_deps:
        output["dependency_graph"] = dynamic_deps.get("dependency_graph", [])
        output["transactions_analyzed"] = dynamic_deps.get("transactions_analyzed", [])
        output["trace_methods"] = dynamic_deps.get("trace_methods", [])
        output["trace_errors"] = dynamic_deps.get("trace_errors", [])

    if discovered:
        output["discovered_addresses"] = sorted(discovered)

    if static_deps and static_deps.get("network"):
        output["network"] = static_deps["network"]

    return output


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
    run_classify: bool = True,
):
    """Fetch, scaffold, discover dependencies, classify, and run analyzers."""
    do_classify = run_classify and (run_deps or run_dynamic_deps)
    steps = 3 + int(run_deps) + int(run_dynamic_deps) + int(do_classify) + int(run_llm)
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

    deps_output = None
    dyn_output = None
    cls_output = None
    resolved_rpc = None

    if run_deps:
        print(f"[{step}/{steps}] Discovering static contract dependencies ...")
        step += 1
        try:
            deps_output = find_dependencies(address, deps_rpc)
            resolved_rpc = deps_output.get("rpc")
            print(
                f"         Found {len(deps_output['dependencies'])} static dependenc(ies)"
            )
        except RuntimeError as exc:
            print(f"         Dependency discovery skipped: {exc}")

    if run_dynamic_deps:
        print(
            f"[{step}/{steps}] Discovering dynamic contract dependencies via traces ..."
        )
        step += 1
        try:
            dyn_output = find_dynamic_dependencies(
                address,
                rpc_url=(dynamic_rpc or deps_rpc),
                tx_limit=dynamic_tx_limit,
                tx_hashes=dynamic_tx_hashes,
            )
            if not resolved_rpc:
                resolved_rpc = dyn_output.get("rpc")
            print(
                f"         Found {len(dyn_output['dependencies'])} dynamic dependenc(ies)"
            )
            print(
                f"         Transactions traced: {len(dyn_output['transactions_analyzed'])}"
            )
        except RuntimeError as exc:
            print(f"         Dynamic dependency discovery skipped: {exc}")

    if do_classify:
        print(f"[{step}/{steps}] Classifying contract dependencies ...")
        step += 1
        try:
            if not resolved_rpc:
                from services.dependent_contracts import resolve_rpc_for_address

                _, resolved_rpc = resolve_rpc_for_address(
                    address, deps_rpc or dynamic_rpc
                )
            unique_deps = sorted(
                set(
                    (deps_output or {}).get("dependencies", [])
                    + (dyn_output or {}).get("dependencies", [])
                )
            )
            cls_output = classify_contracts(
                address,
                unique_deps,
                resolved_rpc,
                dynamic_edges=(dyn_output or {}).get("dependency_graph"),
            )
        except RuntimeError as exc:
            print(f"         Classification skipped: {exc}")

    # Write unified dependencies output
    if deps_output or dyn_output:
        unified = _build_unified_deps(address, deps_output, dyn_output, cls_output)
        deps_path = project_dir / "dependencies.json"
        deps_path.write_text(json.dumps(unified, indent=2) + "\n")
        type_counts: dict[str, int] = {}
        for info in unified["dependencies"].values():
            t = info.get("type", "regular")
            type_counts[t] = type_counts.get(t, 0) + 1
        dep_count = len(unified["dependencies"])
        parts = [f"{dep_count} total"]
        parts.extend(f"{v} {k}" for k, v in sorted(type_counts.items()))
        print(f"         Dependencies: {', '.join(parts)}")
        if unified.get("discovered_addresses"):
            n = len(unified["discovered_addresses"])
            print(f"         Discovered {n} new address(es) via proxy slots")
        print(f"         Output: {deps_path}")

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
    parser.add_argument(
        "--no-deps", action="store_true", help="Skip all dependency discovery"
    )
    parser.add_argument(
        "--no-dynamic-deps",
        action="store_true",
        help="Skip dynamic dependency discovery via transaction tracing",
    )
    parser.add_argument(
        "--no-classify",
        action="store_true",
        help="Skip dependency classification",
    )
    parser.add_argument("--deps-rpc", help="Optional RPC URL for dependency discovery")
    parser.add_argument(
        "--dynamic-rpc", help="Tracing-enabled RPC URL for dynamic dependency discovery"
    )
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
    run_classify = not args.no_classify

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
                    run_classify=run_classify,
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
            run_classify=run_classify,
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
