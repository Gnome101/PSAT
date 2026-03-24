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


def _contract_label(contract_dir: Path) -> str:
    """Derive a human label from the contract output directory name."""
    return contract_dir.name


# ---------------------------------------------------------------------------
# Node / edge construction
# ---------------------------------------------------------------------------


def _build_nodes(
    target: str,
    contract_dir: Path,
    unified: dict,
) -> list[dict]:
    nodes: list[dict] = []

    # Target (root) node
    target_cls = unified.get("target_classification", {})
    nodes.append(
        {
            "id": f"addr:{target}",
            "address": target,
            "label": _contract_label(contract_dir),
            "type": target_cls.get("type", "target"),
            "proxy_type": target_cls.get("proxy_type"),
            "is_target": True,
            "source": [],
        }
    )

    deps = unified.get("dependencies", {})
    for addr in sorted(deps):
        info = deps[addr]
        nodes.append(
            {
                "id": f"addr:{addr}",
                "address": addr,
                "label": info.get("contract_name") or _shorten(addr),
                "type": info.get("type", "regular"),
                "proxy_type": info.get("proxy_type"),
                "implementation": info.get("implementation"),
                "beacon": info.get("beacon"),
                "admin": info.get("admin"),
                "is_target": False,
                "source": info.get("source", []),
            }
        )

    return nodes


def _build_edges(
    target: str,
    unified: dict,
    node_ids: set[str],
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

    # Dynamic call-graph edges
    for edge in unified.get("dependency_graph", []):
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
    deps_with_edges = set()
    for edge in unified.get("dependency_graph", []):
        deps_with_edges.add(edge["to"])
    deps = unified.get("dependencies", {})
    for addr in sorted(deps):
        if addr in deps_with_edges:
            continue
        source = deps[addr].get("source", [])
        if source == ["classification"]:
            continue
        _add(f"addr:{target}", f"addr:{addr}", "STATIC_REF")

    # Proxy relationship edges
    for addr, info in deps.items():
        if info.get("implementation"):
            _add(f"addr:{addr}", f"addr:{info['implementation']}", "DELEGATES_TO")
        if info.get("beacon"):
            _add(f"addr:{addr}", f"addr:{info['beacon']}", "BEACON")

    return edges


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_dependency_visualization(contract_dir: str | Path) -> dict:
    """Read the unified dependencies.json and produce a visualization graph.

    Returns a dict with ``nodes``, ``edges``, and ``metadata`` keys ready for
    the JS visualization layer.
    """
    contract_dir = Path(contract_dir)

    unified = _load_json(contract_dir / "dependencies.json")
    if not unified or not unified.get("dependencies"):
        return {"nodes": [], "edges": [], "metadata": {"error": "no dependency data found"}}

    target = unified.get("address", "")

    nodes = _build_nodes(target, contract_dir, unified)
    node_ids = {n["id"] for n in nodes}
    edges = _build_edges(target, unified, node_ids)

    metadata: dict[str, Any] = {
        "target": target,
        "network": unified.get("network"),
        "transactions_analyzed": unified.get("transactions_analyzed", []),
        "trace_methods": unified.get("trace_methods", []),
        "trace_errors": unified.get("trace_errors", []),
        "discovered_addresses": unified.get("discovered_addresses", []),
    }

    return {"nodes": nodes, "edges": edges, "metadata": metadata}


def write_dependency_visualization(contract_dir: str | Path) -> Path | None:
    """Build the visualization graph and write it to *contract_dir*.

    Returns the output path, or ``None`` if no dependency data was found.
    """
    contract_dir = Path(contract_dir)
    result = build_dependency_visualization(contract_dir)
    if not result["nodes"]:
        return None

    out_path = contract_dir / "dependency_graph_viz.json"
    out_path.write_text(json.dumps(result, indent=2) + "\n")
    return out_path
