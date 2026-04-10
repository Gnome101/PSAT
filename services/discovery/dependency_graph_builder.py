#!/usr/bin/env python3
"""Build a visualization-ready dependency graph from pipeline outputs.

Reads the unified ``dependencies.json`` produced by ``_build_unified_deps()``
in ``main.py`` and converts it into a graph structure suitable for the
frontend visualization layer.

Output: ``dependency_graph_viz.json`` written to the contract directory.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    with open(path) as fh:
        return json.load(fh)


def _shorten(address: str) -> str:
    if len(address) >= 12:
        return f"{address[:6]}...{address[-4:]}"
    return address


_GENERIC_PROXY_NAMES = {
    "uupsproxy",
    "erc1967proxy",
    "transparentupgradeableproxy",
    "proxy",
    "beaconproxy",
    "ossifiableproxy",
    "upgradeablebeacon",
}


def _contract_label(contract_dir: Path) -> str:
    """Derive a human label from contract metadata, falling back to directory name."""
    meta = _load_json(contract_dir / "contract_meta.json")
    if not meta:
        return contract_dir.name
    name = meta.get("contract_name", "")
    # For generic proxy contracts, prefer the display name (job name)
    if name.lower().replace("_", "") in _GENERIC_PROXY_NAMES and meta.get("display_name"):
        return meta["display_name"]
    return name or contract_dir.name


def _derive_discovered(unified: dict) -> list[str]:
    """Derive discovered addresses from deps whose source includes 'classification'."""
    result: list[str] = []
    for addr, info in unified.get("dependencies", {}).items():
        if "classification" in info.get("source", []):
            result.append(addr)
        impl = info.get("implementation")
        if isinstance(impl, dict) and "classification" in impl.get("source", []):
            result.append(impl["address"])
    return sorted(result)


# ---------------------------------------------------------------------------
# Node / edge construction
# ---------------------------------------------------------------------------


def _build_nodes(
    target: str,
    contract_dir: Path,
    unified: dict,
    proxy_address: str | None = None,
    proxy_name: str | None = None,
    proxy_type: str | None = None,
) -> list[dict]:
    nodes: list[dict] = []

    # Proxy node — shown as context when the target is an implementation
    if proxy_address:
        nodes.append(
            {
                "id": f"addr:{proxy_address}",
                "address": proxy_address,
                "label": proxy_name or _shorten(proxy_address),
                "type": "proxy",
                "proxy_type": proxy_type,
                "is_target": False,
                "is_proxy_context": True,
                "source": [],
            }
        )

    # Target (root) node
    target_cls = unified.get("target_classification", {})
    default_type = "implementation" if proxy_address else "target"
    nodes.append(
        {
            "id": f"addr:{target}",
            "address": target,
            "label": _contract_label(contract_dir),
            "type": target_cls.get("type", default_type),
            "proxy_type": target_cls.get("proxy_type"),
            "is_target": True,
            "source": [],
        }
    )

    deps = unified.get("dependencies", {})
    for addr in sorted(deps):
        info = deps[addr]
        impl = info.get("implementation")
        impl_addr = impl["address"] if isinstance(impl, dict) else impl

        nodes.append(
            {
                "id": f"addr:{addr}",
                "address": addr,
                "label": info.get("contract_name") or _shorten(addr),
                "type": info.get("type", "regular"),
                "proxy_type": info.get("proxy_type"),
                "implementation": impl_addr,
                "beacon": info.get("beacon"),
                "admin": info.get("admin"),
                "is_target": False,
                "source": info.get("source", []),
            }
        )

        # Nested implementation → separate node for visualization
        if isinstance(impl, dict):
            nodes.append(
                {
                    "id": f"addr:{impl['address']}",
                    "address": impl["address"],
                    "label": impl.get("contract_name") or _shorten(impl["address"]),
                    "type": impl.get("type", "implementation"),
                    "proxy_type": None,
                    "implementation": None,
                    "beacon": None,
                    "admin": None,
                    "is_target": False,
                    "source": impl.get("source", []),
                }
            )

    return nodes


def _build_edges(
    target: str,
    unified: dict,
    node_ids: set[str],
    proxy_address: str | None = None,
) -> list[dict]:
    edges: list[dict] = []
    seen: set[tuple[str, str, str, str]] = set()

    def _add(
        src: str,
        dst: str,
        op: str,
        provenance: list | None = None,
        selector: str | None = None,
        function_name: str | None = None,
    ) -> None:
        key = (src, dst, op, selector or "")
        if key in seen:
            return
        if src not in node_ids or dst not in node_ids:
            return
        seen.add(key)
        entry: dict = {
            "from": src,
            "to": dst,
            "op": op,
            "provenance": provenance or [],
        }
        if selector and selector != "0x":
            entry["selector"] = selector
        if function_name:
            entry["function_name"] = function_name
        edges.append(entry)

    # Dynamic call-graph edges — supports both keyed dict (new) and flat list (old) formats
    dep_graph = unified.get("dependency_graph", {})
    if isinstance(dep_graph, dict):
        for graph_key, edge_list in dep_graph.items():
            parts = graph_key.split("|")
            from_addr, to_addr = parts[0], parts[1]
            for edge in edge_list:
                _add(
                    f"addr:{from_addr}",
                    f"addr:{to_addr}",
                    edge["op"],
                    edge.get("provenance", []),
                    selector=edge.get("selector"),
                    function_name=edge.get("function_name"),
                )
    elif isinstance(dep_graph, list):
        for edge in dep_graph:
            _add(
                f"addr:{edge['from']}",
                f"addr:{edge['to']}",
                edge["op"],
                edge.get("provenance", []),
                selector=edge.get("selector"),
                function_name=edge.get("function_name"),
            )

    # For deps found via static bytecode scan (not trace or classification),
    # create implicit edges from target.  Skip classification-only discoveries
    # — those are reachable through their proxy's DELEGATES_TO / BEACON edge.
    deps_with_edges: set[str] = set()
    if isinstance(dep_graph, dict):
        for graph_key in dep_graph:
            deps_with_edges.add(graph_key.split("|")[1])
    elif isinstance(dep_graph, list):
        for edge in dep_graph:
            deps_with_edges.add(edge["to"])
    deps = unified.get("dependencies", {})
    for addr in sorted(deps):
        if addr in deps_with_edges:
            continue
        source = deps[addr].get("source", [])
        if source == ["classification"]:
            continue
        static_root = proxy_address if proxy_address else target
        _add(f"addr:{static_root}", f"addr:{addr}", "STATIC_REF")

    # Proxy relationship edges
    for addr, info in deps.items():
        impl = info.get("implementation")
        if isinstance(impl, dict):
            _add(f"addr:{addr}", f"addr:{impl['address']}", "DELEGATES_TO")
        elif isinstance(impl, str) and impl:
            _add(f"addr:{addr}", f"addr:{impl}", "DELEGATES_TO")
        if info.get("beacon"):
            _add(f"addr:{addr}", f"addr:{info['beacon']}", "BEACON")

    # Proxy → implementation root edge (when this target is behind a proxy)
    if proxy_address:
        _add(f"addr:{proxy_address}", f"addr:{target}", "DELEGATES_TO")

    return edges


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_dependency_visualization(
    contract_dir: str | Path,
    proxy_address: str | None = None,
    proxy_name: str | None = None,
    proxy_type: str | None = None,
) -> dict:
    """Read the unified dependencies.json and produce a visualization graph.

    Returns a dict with ``nodes``, ``edges``, and ``metadata`` keys ready for
    the JS visualization layer.

    When *proxy_address* is provided, a proxy context node and DELEGATES_TO
    edge are added so the graph shows the proxy → implementation relationship.
    """
    contract_dir = Path(contract_dir)

    unified = _load_json(contract_dir / "dependencies.json")
    if not unified or not unified.get("dependencies"):
        return {"nodes": [], "edges": [], "metadata": {"error": "no dependency data found"}}

    target = unified.get("address", "")

    nodes = _build_nodes(target, contract_dir, unified, proxy_address, proxy_name, proxy_type)
    node_ids = {n["id"] for n in nodes}
    edges = _build_edges(target, unified, node_ids, proxy_address)

    metadata: dict[str, Any] = {
        "target": target,
        "proxy_address": proxy_address,
        "network": unified.get("network"),
        "transactions_analyzed": unified.get("transactions_analyzed", []),
        "trace_methods": unified.get("trace_methods", []),
        "trace_errors": unified.get("trace_errors", []),
        "discovered_addresses": _derive_discovered(unified),
    }

    return {"nodes": nodes, "edges": edges, "metadata": metadata}


def write_dependency_visualization(
    contract_dir: str | Path,
    proxy_address: str | None = None,
    proxy_name: str | None = None,
    proxy_type: str | None = None,
) -> Path | None:
    """Build the visualization graph and write it to *contract_dir*.

    Returns the output path, or ``None`` if no dependency data was found.
    """
    contract_dir = Path(contract_dir)
    result = build_dependency_visualization(contract_dir, proxy_address, proxy_name, proxy_type)
    if not result["nodes"]:
        return None

    out_path = contract_dir / "dependency_graph_viz.json"
    out_path.write_text(json.dumps(result, indent=2) + "\n")
    return out_path
