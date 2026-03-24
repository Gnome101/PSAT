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
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from schemas.resolved_control_graph import ResolvedControlGraph, ResolvedGraphEdge, ResolvedGraphNode
from services.discovery.fetch import CONTRACTS_DIR, fetch, scaffold
from services.static import analyze, analyze_contract

from .tracking import (
    build_control_snapshot,
    classify_resolved_address,
    load_control_tracking_plan,
    write_control_snapshot,
)
from .tracking_plan import write_control_tracking_plan

ANALYZABLE_TYPES = {"contract", "timelock", "proxy_admin"}


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def _load_effective_permissions(project_dir: Path) -> dict | None:
    path = project_dir / "effective_permissions.json"
    if not path.exists():
        return None
    return _load_json(path)


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


def _role_principals_from_effective_permissions(effective_permissions: dict) -> list[dict[str, object]]:
    principals: dict[str, dict[str, object]] = {}
    for function in effective_permissions.get("functions", []):
        function_signature = str(function.get("function", ""))
        for role_grant in function.get("authority_roles", []):
            role = int(role_grant["role"])
            for principal in role_grant.get("principals", []):
                address = str(principal.get("address", "")).lower()
                if not address.startswith("0x"):
                    continue
                payload = principals.setdefault(
                    address,
                    {
                        "address": address,
                        "resolved_type": str(principal.get("resolved_type", "unknown")),
                        "details": dict(principal.get("details", {})),
                        "roles": set(),
                        "functions": set(),
                    },
                )
                payload["roles"].add(role)
                if function_signature:
                    payload["functions"].add(function_signature)
                if payload.get("resolved_type") in {None, "", "unknown"} and principal.get("resolved_type"):
                    payload["resolved_type"] = str(principal.get("resolved_type"))
                merged_details = dict(payload.get("details", {}))
                merged_details.update(dict(principal.get("details", {})))
                payload["details"] = merged_details

    serialized: list[dict[str, object]] = []
    for payload in principals.values():
        serialized.append(
            {
                "address": payload["address"],
                "resolved_type": payload["resolved_type"],
                "details": dict(payload.get("details", {})),
                "roles": sorted(payload.get("roles", set())),
                "functions": sorted(payload.get("functions", set())),
            }
        )
    return sorted(serialized, key=lambda item: str(item["address"]))


def _maybe_queue_address(queue: deque, queued: set[str], address: str, depth: int, max_depth: int) -> None:
    if address in queued or depth > max_depth:
        return
    queue.append({"address": address, "depth": depth})
    queued.add(address)


def _add_nested_principals(
    *,
    nodes: dict[str, ResolvedGraphNode],
    edges: dict[tuple, ResolvedGraphEdge],
    queue: deque,
    queued: set[str],
    rpc_url: str,
    from_node_id: str,
    source_controller_id: str | None,
    resolved_type: str,
    details: dict[str, object],
    depth: int,
    max_depth: int,
) -> None:
    for nested_address, relation, label in _nested_principals_for_details(resolved_type, details):
        nested_type, nested_details = classify_resolved_address(rpc_url, nested_address)
        nested_node_type = "contract" if nested_type in ANALYZABLE_TYPES else "principal"
        nested_node_id = _ensure_node(
            nodes,
            address=nested_address,
            resolved_type=nested_type,
            label=label,
            depth=depth + 1,
            node_type=nested_node_type,
            details=nested_details,
        )
        _add_edge(
            edges,
            {
                "from_id": from_node_id,
                "to_id": nested_node_id,
                "relation": relation,  # type: ignore[typeddict-item]
                "label": label,
                "source_controller_id": source_controller_id,
                "notes": [],
            },
        )
        if nested_type in ANALYZABLE_TYPES:
            _maybe_queue_address(queue, queued, nested_address, depth + 1, max_depth)


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
        effective_permissions = _load_effective_permissions(artifacts["project_dir"])
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

            if resolved_type in ANALYZABLE_TYPES:
                _maybe_queue_address(queue, queued, controller_address, depth + 1, max_depth)

            _add_nested_principals(
                nodes=nodes,
                edges=edges,
                queue=queue,
                queued=queued,
                rpc_url=rpc_url,
                from_node_id=controller_node_id,
                source_controller_id=controller_id,
                resolved_type=resolved_type,
                details=details,
                depth=depth + 1,
                max_depth=max_depth,
            )

        for principal_value in _role_principals_from_effective_permissions(effective_permissions or {}):
            principal_address = str(principal_value["address"]).lower()
            resolved_type = str(principal_value.get("resolved_type", "unknown"))
            details = dict(principal_value.get("details", {}))
            if resolved_type == "unknown":
                resolved_type, classified_details = classify_resolved_address(rpc_url, principal_address)
                merged_details = dict(details)
                merged_details.update(classified_details)
                details = merged_details

            node_type = "contract" if resolved_type in ANALYZABLE_TYPES else "principal"
            principal_node_id = _ensure_node(
                nodes,
                address=principal_address,
                resolved_type=resolved_type,
                label="role principal",
                depth=depth + 1,
                node_type=node_type,
                details=details,
            )
            roles = principal_value.get("roles", [])
            functions = principal_value.get("functions", [])
            _add_edge(
                edges,
                {
                    "from_id": contract_node_id,
                    "to_id": principal_node_id,
                    "relation": "role_principal",
                    "label": f"roles {','.join(str(role) for role in roles)}" if roles else "role principal",
                    "source_controller_id": None,
                    "notes": [f"functions={len(functions)}", *(f"role={role}" for role in roles)],
                },
            )
            if resolved_type in ANALYZABLE_TYPES:
                _maybe_queue_address(queue, queued, principal_address, depth + 1, max_depth)
            _add_nested_principals(
                nodes=nodes,
                edges=edges,
                queue=queue,
                queued=queued,
                rpc_url=rpc_url,
                from_node_id=principal_node_id,
                source_controller_id=None,
                resolved_type=resolved_type,
                details=details,
                depth=depth + 1,
                max_depth=max_depth,
            )

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
