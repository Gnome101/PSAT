"""Helpers for merging and enriching dependency-discovery outputs."""

from __future__ import annotations

from utils.etherscan import get_contract_info

from .static_dependencies import normalize_address

_CLS_KEYS = ("proxy_type", "implementation", "beacon", "admin", "proxies", "facets")


def build_unified_dependencies(
    address: str,
    static_deps: dict | None,
    dynamic_deps: dict | None,
    classifications: dict | None,
) -> dict:
    """Merge static deps, dynamic deps, and classifications into one output."""
    target = normalize_address(address)

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
            classification = cls_map[addr]
            entry["type"] = classification.get("type", "regular")
            for key in _CLS_KEYS:
                if key in classification:
                    entry[key] = classification[key]
        if addr in dyn_provenance:
            entry["provenance"] = dyn_provenance[addr]
        return entry

    deps = {addr: _dep_entry(addr, sorted(sources)) for addr, sources in sorted(dep_sources.items())}

    discovered = (classifications or {}).get("discovered_addresses", [])
    for addr in discovered:
        addr = normalize_address(addr)
        if addr not in deps:
            deps[addr] = _dep_entry(addr, ["classification"])

    output: dict = {"address": target, "dependencies": deps}

    if target in cls_map and cls_map[target].get("type", "regular") != "regular":
        target_classification = cls_map[target]
        info = {"type": target_classification["type"]}
        for key in ("proxy_type", "implementation", "beacon", "admin"):
            if key in target_classification:
                info[key] = target_classification[key]
        output["target_classification"] = info

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


def enrich_dependency_metadata(unified: dict) -> dict:
    """Resolve contract names and selectors in-place for a unified dependency output."""
    deps = unified.get("dependencies", {})
    if not isinstance(deps, dict) or not deps:
        return unified

    graph_edges = unified.get("dependency_graph", [])

    addrs_to_fetch: set[str] = set(deps.keys())
    for info in deps.values():
        if not isinstance(info, dict):
            continue
        implementation = info.get("implementation")
        if implementation:
            addrs_to_fetch.add(implementation)

    info_cache: dict[str, tuple[str | None, dict[str, str]]] = {}
    for addr in sorted(addrs_to_fetch):
        info_cache[addr] = get_contract_info(addr)

    for addr, info in deps.items():
        if not isinstance(info, dict):
            continue
        contract_name = info_cache.get(addr, (None, {}))[0]
        if contract_name:
            info["contract_name"] = contract_name

    if isinstance(graph_edges, list):
        for edge in graph_edges:
            if not isinstance(edge, dict):
                continue
            selector = edge.get("selector")
            if not selector or selector == "0x":
                continue
            target_addr = edge.get("to")
            if not isinstance(target_addr, str):
                continue
            function_name = info_cache.get(target_addr, (None, {}))[1].get(selector)
            if not function_name:
                implementation = deps.get(target_addr, {}).get("implementation")
                if implementation:
                    function_name = info_cache.get(implementation, (None, {}))[1].get(selector)
            if function_name:
                edge["function_name"] = function_name

    return unified
