#!/usr/bin/env python3
"""Recursively resolve contract control chains into a reusable graph artifact."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import deque
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from schemas.resolved_control_graph import ResolvedControlGraph, ResolvedGraphEdge, ResolvedGraphNode
from services.analyzer import analyze
from services.contract_analysis import analyze_contract
from services.control_tracker import build_control_snapshot, classify_resolved_address, load_control_tracking_plan, write_control_snapshot
from services.control_tracking_plan import write_control_tracking_plan
from services.fetcher import CONTRACTS_DIR, fetch, scaffold


ANALYZABLE_TYPES = {"contract", "timelock", "proxy_admin"}


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def _address_node_id(address: str) -> str:
    return f"address:{address.lower()}"


def _sanitize_name(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "_", value).strip("_")
    return cleaned or "contract"


def _workspace_name(contract_name: str, address: str, prefix: str) -> str:
    return f"{_sanitize_name(prefix)}_{_sanitize_name(contract_name)}_{address.lower()[2:10]}"


def _load_or_build_artifacts(
    analysis_path: Path,
    rpc_url: str,
    *,
    refresh_snapshots: bool,
) -> dict:
    analysis = _load_json(analysis_path)
    project_dir = analysis_path.parent
    plan_path = project_dir / "control_tracking_plan.json"
    if not plan_path.exists():
        write_control_tracking_plan(analysis_path, plan_path)

    snapshot_path = project_dir / "control_snapshot.json"
    if refresh_snapshots or not snapshot_path.exists():
        plan = load_control_tracking_plan(plan_path)
        snapshot = build_control_snapshot(plan, rpc_url)
        write_control_snapshot(snapshot, snapshot_path)
    else:
        snapshot = _load_json(snapshot_path)

    return {
        "project_dir": project_dir,
        "analysis_path": analysis_path,
        "plan_path": plan_path,
        "snapshot_path": snapshot_path,
        "analysis": analysis,
        "snapshot": snapshot,
    }


def _materialize_contract_artifacts(
    address: str,
    rpc_url: str,
    *,
    workspace_prefix: str,
    refresh_snapshots: bool,
) -> dict:
    result = fetch(address)
    contract_name = result.get("ContractName", "Contract")
    project_name = _workspace_name(contract_name, address, workspace_prefix)
    project_dir = CONTRACTS_DIR / project_name
    analysis_path = project_dir / "contract_analysis.json"

    if not analysis_path.exists():
        scaffold(address, project_name, result)
        analyze(project_dir, contract_name, address)
        analysis_path = analyze_contract(project_dir)
        write_control_tracking_plan(analysis_path, project_dir / "control_tracking_plan.json")

    return _load_or_build_artifacts(analysis_path, rpc_url, refresh_snapshots=refresh_snapshots)


def _ensure_node(
    nodes: dict[str, ResolvedGraphNode],
    *,
    address: str,
    resolved_type: str,
    label: str,
    depth: int,
    node_type: str,
    contract_name: str | None = None,
    analyzed: bool = False,
    details: dict[str, object] | None = None,
    artifacts: dict[str, str] | None = None,
) -> str:
    normalized = address.lower()
    node_id = _address_node_id(normalized)
    current = nodes.get(node_id)
    payload: ResolvedGraphNode = {
        "id": node_id,
        "address": normalized,
        "node_type": node_type,
        "resolved_type": resolved_type,  # type: ignore[typeddict-item]
        "label": label,
        "contract_name": contract_name,
        "depth": depth,
        "analyzed": analyzed,
        "details": details or {},
        "artifacts": artifacts or {},
    }
    if current is None:
        nodes[node_id] = payload
        return node_id

    current["depth"] = min(current.get("depth", depth), depth)
    if contract_name:
        current["contract_name"] = contract_name
    if analyzed:
        current["analyzed"] = True
        current["node_type"] = "contract"
    if resolved_type != "unknown" or not current.get("resolved_type"):
        current["resolved_type"] = resolved_type  # type: ignore[typeddict-item]
    if label:
        current["label"] = label
    if details:
        merged_details = dict(current.get("details", {}))
        merged_details.update(details)
        current["details"] = merged_details
    if artifacts:
        merged_artifacts = dict(current.get("artifacts", {}))
        merged_artifacts.update(artifacts)
        current["artifacts"] = merged_artifacts
    return node_id


def _edge_key(edge: ResolvedGraphEdge) -> tuple:
    return (
        edge["from_id"],
        edge["to_id"],
        edge["relation"],
        edge.get("label"),
        edge.get("source_controller_id"),
    )


def _add_edge(edges: dict[tuple, ResolvedGraphEdge], edge: ResolvedGraphEdge) -> None:
    key = _edge_key(edge)
    if key in edges:
        existing_notes = set(edges[key].get("notes", []))
        existing_notes.update(edge.get("notes", []))
        edges[key]["notes"] = sorted(existing_notes)
        return
    edges[key] = edge


def _nested_principals_for_details(resolved_type: str, details: dict[str, object]) -> list[tuple[str, str, str]]:
    principals: list[tuple[str, str, str]] = []
    if resolved_type == "safe":
        for owner in details.get("owners", []) or []:
            if isinstance(owner, str) and owner.startswith("0x"):
                principals.append((owner.lower(), "safe_owner", "safe owner"))
    elif resolved_type == "timelock":
        owner = details.get("owner")
        if isinstance(owner, str) and owner.startswith("0x"):
            principals.append((owner.lower(), "timelock_owner", "timelock owner"))
    elif resolved_type == "proxy_admin":
        owner = details.get("owner")
        if isinstance(owner, str) and owner.startswith("0x"):
            principals.append((owner.lower(), "proxy_admin_owner", "proxy admin owner"))
    return principals


def resolve_control_graph(
    root_analysis_path: Path,
    *,
    rpc_url: str,
    max_depth: int = 3,
    workspace_prefix: str = "recursive",
    refresh_snapshots: bool = True,
) -> ResolvedControlGraph:
    root_artifacts = _load_or_build_artifacts(root_analysis_path, rpc_url, refresh_snapshots=refresh_snapshots)
    root_analysis = root_artifacts["analysis"]
    root_address = root_analysis["subject"]["address"].lower()

    queue = deque(
        [
            {
                "address": root_address,
                "depth": 0,
                "analysis_path": root_analysis_path,
            }
        ]
    )
    queued = {root_address}
    processed: set[str] = set()

    nodes: dict[str, ResolvedGraphNode] = {}
    edges: dict[tuple, ResolvedGraphEdge] = {}

    while queue:
        pending = queue.popleft()
        address = pending["address"]
        depth = pending["depth"]
        if address in processed or depth > max_depth:
            continue

        if pending.get("analysis_path") is not None:
            artifacts = _load_or_build_artifacts(
                Path(pending["analysis_path"]),
                rpc_url,
                refresh_snapshots=refresh_snapshots,
            )
        else:
            artifacts = _materialize_contract_artifacts(
                address,
                rpc_url,
                workspace_prefix=workspace_prefix,
                refresh_snapshots=refresh_snapshots,
            )

        processed.add(address)
        analysis = artifacts["analysis"]
        snapshot = artifacts["snapshot"]
        contract_name = analysis["subject"]["name"]
        contract_node_id = _ensure_node(
            nodes,
            address=address,
            resolved_type="contract",
            label=contract_name,
            depth=depth,
            node_type="contract",
            contract_name=contract_name,
            analyzed=True,
            details={"address": address},
            artifacts={
                "analysis": str(artifacts["analysis_path"]),
                "tracking_plan": str(artifacts["plan_path"]),
                "snapshot": str(artifacts["snapshot_path"]),
            },
        )

        for controller_id, controller_value in snapshot.get("controller_values", {}).items():
            controller_address = str(controller_value.get("value", "")).lower()
            if not controller_address.startswith("0x"):
                continue
            resolved_type = str(controller_value.get("resolved_type", "unknown"))
            details = dict(controller_value.get("details", {}))
            controller_label = str(controller_value.get("source", controller_id))
            controller_node_type = "contract" if resolved_type in ANALYZABLE_TYPES else "principal"
            controller_node_id = _ensure_node(
                nodes,
                address=controller_address,
                resolved_type=resolved_type,
                label=controller_label,
                depth=depth + 1,
                node_type=controller_node_type,
                details=details,
            )
            _add_edge(
                edges,
                {
                    "from_id": contract_node_id,
                    "to_id": controller_node_id,
                    "relation": "controller_value",
                    "label": controller_label,
                    "source_controller_id": controller_id,
                    "notes": [f"resolved_type={resolved_type}"],
                },
            )

            if resolved_type in ANALYZABLE_TYPES and controller_address not in queued and depth + 1 <= max_depth:
                queue.append({"address": controller_address, "depth": depth + 1})
                queued.add(controller_address)

            for nested_address, relation, label in _nested_principals_for_details(resolved_type, details):
                nested_type, nested_details = classify_resolved_address(rpc_url, nested_address)
                nested_node_type = "contract" if nested_type in ANALYZABLE_TYPES else "principal"
                nested_node_id = _ensure_node(
                    nodes,
                    address=nested_address,
                    resolved_type=nested_type,
                    label=label,
                    depth=depth + 2,
                    node_type=nested_node_type,
                    details=nested_details,
                )
                _add_edge(
                    edges,
                    {
                        "from_id": controller_node_id,
                        "to_id": nested_node_id,
                        "relation": relation,  # type: ignore[typeddict-item]
                        "label": label,
                        "source_controller_id": controller_id,
                        "notes": [],
                    },
                )

                if nested_type in ANALYZABLE_TYPES and nested_address not in queued and depth + 2 <= max_depth:
                    queue.append({"address": nested_address, "depth": depth + 2})
                    queued.add(nested_address)

    return {
        "schema_version": "0.1",
        "root_contract_address": root_address,
        "max_depth": max_depth,
        "nodes": sorted(nodes.values(), key=lambda item: item["id"]),
        "edges": sorted(edges.values(), key=lambda item: (item["from_id"], item["relation"], item["to_id"])),
    }


def write_resolved_control_graph(
    root_analysis_path: Path,
    *,
    rpc_url: str,
    output_path: Path | None = None,
    max_depth: int = 3,
    workspace_prefix: str = "recursive",
    refresh_snapshots: bool = True,
) -> Path:
    graph = resolve_control_graph(
        root_analysis_path,
        rpc_url=rpc_url,
        max_depth=max_depth,
        workspace_prefix=workspace_prefix,
        refresh_snapshots=refresh_snapshots,
    )
    if output_path is None:
        output_path = root_analysis_path.with_name("resolved_control_graph.json")
    output_path.write_text(json.dumps(graph, indent=2) + "\n")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Recursively resolve contract control chains into a graph artifact.")
    parser.add_argument("analysis", help="Path to root contract_analysis.json")
    parser.add_argument("--rpc", required=True, help="HTTP RPC URL for state reads")
    parser.add_argument("--out", help="Optional output path for resolved_control_graph.json")
    parser.add_argument("--max-depth", type=int, default=3, help="Maximum recursion depth (default: 3)")
    parser.add_argument(
        "--workspace-prefix",
        default="recursive",
        help="Prefix for auto-materialized recursive contract workspaces (default: recursive)",
    )
    parser.add_argument(
        "--reuse-snapshots",
        action="store_true",
        help="Reuse existing control_snapshot.json files when present instead of refreshing them",
    )
    args = parser.parse_args()

    output_path = write_resolved_control_graph(
        Path(args.analysis),
        rpc_url=args.rpc,
        output_path=Path(args.out) if args.out else None,
        max_depth=args.max_depth,
        workspace_prefix=args.workspace_prefix,
        refresh_snapshots=not args.reuse_snapshots,
    )
    print(f"Resolved control graph: {output_path}")


if __name__ == "__main__":
    main()
