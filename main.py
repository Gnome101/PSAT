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
    python main.py 0x... --dynamic-rpc https://your-trace-rpc --dynamic-tx-limit 10
"""

import argparse
import csv
import json
import sys
from pathlib import Path

from services.demo.runner import run_protocol_analysis
from services.discovery import fetch, find_dependencies, find_dynamic_dependencies, scaffold, search_protocol_inventory
from services.discovery.classifier import classify_contracts
from services.discovery.dependency_graph_builder import write_dependency_visualization
from services.discovery.static_dependencies import normalize_address, resolve_rpc_for_address
from services.resolution import write_control_tracking_plan
from services.static import analyze, analyze_contract, analyze_with_llm
from utils.etherscan import get_contract_info


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


_CLS_KEYS = ("proxy_type", "implementation", "beacon", "admin", "facets")


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

    def _dep_entry(addr: str, sources: list[str]) -> dict:
        entry: dict = {"type": "regular", "source": sources}
        if addr in cls_map:
            c = cls_map[addr]
            entry["type"] = c.get("type", "regular")
            for k in _CLS_KEYS:
                if k in c:
                    entry[k] = c[k]
        return entry

    deps = {addr: _dep_entry(addr, sorted(sources)) for addr, sources in sorted(dep_sources.items())}

    # Include addresses discovered by classification (not in original dep lists)
    discovered = (classifications or {}).get("discovered_addresses", [])
    for addr in discovered:
        addr = normalize_address(addr)
        if addr not in deps:
            deps[addr] = _dep_entry(addr, ["classification"])

    # Nest implementation entries under their proxy instead of top-level keys
    impl_to_remove: set[str] = set()
    for addr, info in deps.items():
        if info.get("type") != "proxy":
            continue
        impl_addr = info.get("implementation")
        if not isinstance(impl_addr, str) or impl_addr not in deps:
            continue
        impl_entry = deps[impl_addr].copy()
        impl_entry["address"] = impl_addr
        info["implementation"] = impl_entry
        impl_to_remove.add(impl_addr)
    for addr in impl_to_remove:
        del deps[addr]

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
        # Key dependency_graph by from|to pair for O(1) lookups
        raw_graph = dynamic_deps.get("dependency_graph", [])
        keyed_graph: dict[str, list[dict]] = {}
        for edge in raw_graph:
            key = f"{edge['from']}|{edge['to']}"
            entry: dict = {"op": edge["op"], "provenance": edge.get("provenance", [])}
            if edge.get("selector"):
                entry["selector"] = edge["selector"]
            if edge.get("function_name"):
                entry["function_name"] = edge["function_name"]
            keyed_graph.setdefault(key, []).append(entry)
        output["dependency_graph"] = keyed_graph
        output["transactions_analyzed"] = dynamic_deps.get("transactions_analyzed", [])
        output["trace_methods"] = dynamic_deps.get("trace_methods", [])
        output["trace_errors"] = dynamic_deps.get("trace_errors", [])

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
    dynamic_tx_limit: int = 10,
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
            print(f"         Found {len(deps_output['dependencies'])} static dependenc(ies)")
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
            if not resolved_rpc:
                resolved_rpc = dyn_output.get("rpc")
            print(f"         Found {len(dyn_output['dependencies'])} dynamic dependenc(ies)")
            print(f"         Transactions traced: {len(dyn_output['transactions_analyzed'])}")
        except RuntimeError as exc:
            print(f"         Dynamic dependency discovery skipped: {exc}")

    if do_classify:
        print(f"[{step}/{steps}] Classifying contract dependencies ...")
        step += 1
        try:
            if not resolved_rpc:
                _, resolved_rpc = resolve_rpc_for_address(address, deps_rpc or dynamic_rpc)
            unique_deps = sorted(
                set((deps_output or {}).get("dependencies", []) + (dyn_output or {}).get("dependencies", []))
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

        # Resolve contract names and function selectors from Etherscan.
        # Uses get_contract_info() which fetches name + ABI in one call per address.
        deps = unified.get("dependencies", {})
        keyed_graph = unified.get("dependency_graph", {})

        # Collect all addresses we need info for: deps + nested implementations
        addrs_to_fetch: set[str] = set(deps.keys())
        for info in deps.values():
            impl = info.get("implementation")
            if isinstance(impl, dict):
                addrs_to_fetch.add(impl["address"])

        # Single Etherscan call per address → name + selector map
        info_cache: dict[str, tuple[str | None, dict[str, str]]] = {}
        for addr in sorted(addrs_to_fetch):
            info_cache[addr] = get_contract_info(addr)

        # Apply contract names
        resolved_names = 0
        for addr, dep_info in deps.items():
            cname = info_cache.get(addr, (None, {}))[0]
            if cname:
                dep_info["contract_name"] = cname
                resolved_names += 1
            impl = dep_info.get("implementation")
            if isinstance(impl, dict):
                impl_name = info_cache.get(impl["address"], (None, {}))[0]
                if impl_name:
                    impl["contract_name"] = impl_name
                    resolved_names += 1
        if resolved_names:
            print(f"         Resolved {resolved_names}/{len(addrs_to_fetch)} contract name(s)")

        # Apply function names to graph edges
        if keyed_graph:
            resolved_fns = 0
            for key, edges in keyed_graph.items():
                to_addr = key.split("|")[1]
                for edge in edges:
                    sel = edge.get("selector")
                    if not sel or sel == "0x":
                        continue
                    # Try the target's ABI first, then its implementation's
                    fn_name = info_cache.get(to_addr, (None, {}))[1].get(sel)
                    if not fn_name:
                        impl = deps.get(to_addr, {}).get("implementation")
                        impl_addr = impl["address"] if isinstance(impl, dict) else impl
                        if impl_addr:
                            fn_name = info_cache.get(impl_addr, (None, {}))[1].get(sel)
                    if fn_name:
                        edge["function_name"] = fn_name
                        resolved_fns += 1
            if resolved_fns:
                print(f"         Resolved {resolved_fns} function selector(s)")

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
        # Derive discovered addresses from deps with classification source
        n_discovered = sum(
            1
            for info in unified["dependencies"].values()
            if "classification" in info.get("source", [])
            or (
                isinstance(info.get("implementation"), dict)
                and "classification" in info["implementation"].get("source", [])
            )
        )
        if n_discovered:
            print(f"         Discovered {n_discovered} new address(es) via proxy slots")
        print(f"         Output: {deps_path}")

        viz_path = write_dependency_visualization(project_dir)
        if viz_path:
            print(f"         Dependency graph visualization: {viz_path}")

    print(f"[{step}/{steps}] Running Slither analysis ...")
    step += 1
    report_path = analyze(project_dir, contract_name, address)
    print(f"         Report: {report_path}")

    print(f"[{step}/{steps}] Building structured contract analysis ...")
    step += 1
    try:
        contract_analysis_path = analyze_contract(project_dir)
        print(f"         Contract analysis: {contract_analysis_path}")
        try:
            tracking_plan_path = write_control_tracking_plan(contract_analysis_path)
            print(f"         Control tracking plan: {tracking_plan_path}")
        except Exception as exc:
            print(f"         Control tracking plan skipped: {exc}")
    except RuntimeError as exc:
        print(f"         Contract analysis skipped: {exc}")

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
    run_activity = not getattr(args, "no_activity_ranking", False)
    debug = getattr(args, "debug", False)

    result = search_protocol_inventory(
        args.discover_inventory, chain=chain, limit=limit, run_activity_ranking=run_activity, debug=debug
    )

    output = json.dumps(result, indent=2)

    safe_name = args.discover_inventory.replace("/", "_").replace(" ", "_")
    out_dir = Path("protocols") / safe_name
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "contract_inventory.json").write_text(output + "\n")
    print(f"\nSaved to {out_dir / 'contract_inventory.json'}")


def main():
    parser = argparse.ArgumentParser(description="Fetch and analyze smart contracts")
    parser.add_argument("address", nargs="?", help="Single Ethereum contract address")
    parser.add_argument("--company", help="Company or protocol name for discovery + full analysis")
    parser.add_argument("--name", help="Project name (single address mode)")
    parser.add_argument("--file", help="Path to JSON or CSV file with addresses")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM analysis")
    parser.add_argument("--no-deps", action="store_true", help="Skip all dependency discovery")
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
    parser.add_argument("--dynamic-rpc", help="Tracing-enabled RPC URL for dynamic dependency discovery")
    parser.add_argument(
        "--dynamic-tx-limit",
        type=int,
        default=10,
        help="Number of representative transactions to trace (default: 10)",
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
    parser.add_argument(
        "--analyze-limit",
        type=int,
        default=5,
        help="When running company discovery, analyze the top N discovered contracts (default: 5)",
    )
    parser.add_argument(
        "--no-activity-ranking",
        action="store_true",
        help="Skip on-chain activity ranking for discovered contracts",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug logging to stderr",
    )

    args = parser.parse_args()

    # Discovery mode — standalone, mutually exclusive with pipeline
    if args.discover_inventory:
        try:
            run_discovery(args)
        except ValueError as exc:
            sys.exit(str(exc))
        return

    if args.company:
        try:
            result = run_protocol_analysis(
                args.company,
                chain=args.discover_chain,
                discover_limit=args.discover_limit or 25,
                analyze_limit=args.analyze_limit,
                rpc_url=args.dynamic_rpc or args.deps_rpc,
            )
        except ValueError as exc:
            sys.exit(str(exc))
        print(json.dumps(result, indent=2))
        print("\nDone.")
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
