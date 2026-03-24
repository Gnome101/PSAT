#!/usr/bin/env python3
"""Build frontend-friendly principal labels from effective permissions and resolved control graphs."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from schemas.principal_labels import PrincipalLabels, PrincipalPermission, PrincipalProfile
from services.resolution.tracking import classify_resolved_address


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _slug(value: str) -> str:
    lowered = value.lower()
    lowered = re.sub(r"[^a-z0-9]+", "_", lowered)
    return lowered.strip("_")


def _display_from_type(resolved_type: str) -> str:
    return {
        "safe": "Safe",
        "timelock": "Timelock",
        "proxy_admin": "Proxy admin",
        "eoa": "Externally owned account",
        "contract": "Contract",
        "zero": "Zero address",
        "unknown": "Unknown principal",
    }.get(resolved_type, "Unknown principal")


def _collect_permissions(
    effective_permissions: dict[str, Any],
) -> tuple[dict[str, list[PrincipalPermission]], dict[str, str]]:
    by_address: dict[str, list[PrincipalPermission]] = defaultdict(list)
    contract_name = effective_permissions["contract_name"]
    contract_slug = _slug(contract_name)
    permission_labels: dict[str, set[str]] = defaultdict(set)

    for function in effective_permissions.get("functions", []):
        function_name = str(function.get("function", ""))
        effect_labels = [str(label) for label in function.get("effect_labels", [])]
        authority_public = bool(function.get("authority_public", False))
        direct_owner = function.get("direct_owner")
        if direct_owner:
            address = direct_owner["address"].lower()
            permission: PrincipalPermission = {
                "function": function_name,
                "effect_labels": effect_labels,
                "authority_public": authority_public,
                "role": None,
            }
            by_address[address].append(permission)
            permission_labels[address].update({f"{contract_slug}_direct_owner", f"{contract_slug}_permissioned"})

        for role_grant in function.get("authority_roles", []):
            role = int(role_grant["role"])
            for principal in role_grant.get("principals", []):
                address = principal["address"].lower()
                permission: PrincipalPermission = {
                    "function": function_name,
                    "effect_labels": effect_labels,
                    "authority_public": authority_public,
                    "role": role,
                }
                by_address[address].append(permission)
                permission_labels[address].update(
                    {
                        f"{contract_slug}_permissioned",
                        f"{contract_slug}_role_{role}_holder",
                    }
                )
                effect_labels_set = set(effect_labels)
                if "arbitrary_external_call" in effect_labels_set:
                    permission_labels[address].add(f"{contract_slug}_manager")
                if effect_labels_set.intersection({"asset_pull", "asset_send", "mint", "burn"}):
                    permission_labels[address].add(f"{contract_slug}_operator")
                if effect_labels_set.intersection(
                    {
                        "authority_update",
                        "ownership_transfer",
                        "hook_update",
                        "implementation_update",
                        "role_management",
                    }
                ):
                    permission_labels[address].add(f"{contract_slug}_admin")

    return by_address, {address: ",".join(sorted(labels)) for address, labels in permission_labels.items()}


def _incoming_edges(graph: dict) -> dict[str, list[dict]]:
    incoming: dict[str, list[dict]] = defaultdict(list)
    for edge in graph.get("edges", []):
        incoming[edge["to_id"]].append(edge)
    return incoming


def _node_by_id(graph: dict) -> dict[str, dict]:
    return {node["id"]: node for node in graph.get("nodes", [])}


def _graph_labels_for_node(
    node: dict, incoming_edges: list[dict], node_index: dict[str, dict]
) -> tuple[set[str], list[str]]:
    labels = {node.get("resolved_type", "unknown")}
    context: list[str] = []
    if node.get("resolved_type") == "safe":
        labels.add("safe_multisig")
    elif node.get("resolved_type") == "eoa":
        labels.add("likely_eoa")
    elif node.get("resolved_type") == "contract":
        labels.add("contract_controller")
    elif node.get("resolved_type") == "zero":
        labels.add("zero_address")

    for edge in incoming_edges:
        source_node = node_index.get(edge["from_id"], {})
        source_contract_name = source_node.get("contract_name") or source_node.get("label") or "contract"
        source_slug = _slug(str(source_contract_name))
        relation = edge["relation"]
        edge_label = _slug(edge.get("label") or relation)
        context.append(f"{source_contract_name}:{edge.get('label') or relation}")
        if relation == "controller_value":
            labels.add("controller_value")
            labels.add(f"{source_slug}_{edge_label}")
            if edge_label == "authority":
                labels.add("authority_controller")
            if edge_label == "owner":
                labels.add("owner_controller")
        elif relation == "safe_owner":
            labels.add("safe_signer")
        elif relation == "timelock_owner":
            labels.add("timelock_owner")
        elif relation == "proxy_admin_owner":
            labels.add("proxy_admin_owner")
        elif relation == "role_principal":
            labels.add("role_principal")

    return labels, sorted(set(context))


def _display_name(
    address: str,
    resolved_type: str,
    labels: set[str],
    graph_context: list[str],
    permissions: list[PrincipalPermission],
    contract_name: str,
) -> tuple[str, str]:
    contract_slug = _slug(contract_name)
    permission_effects = {effect for permission in permissions for effect in permission.get("effect_labels", [])}

    if resolved_type == "zero":
        return "Zero address", "high"
    if f"{contract_slug}_admin" in labels:
        if resolved_type == "safe":
            return f"{contract_name} admin Safe", "high"
        if resolved_type == "contract":
            return f"{contract_name} admin contract", "high"
        return f"{contract_name} admin", "high"
    if f"{contract_slug}_manager" in labels:
        if resolved_type == "contract":
            return f"{contract_name} manager contract", "high"
        return f"{contract_name} manager", "high"
    if f"{contract_slug}_operator" in labels:
        function_names = sorted({permission["function"].split("(", 1)[0] for permission in permissions})
        if len(function_names) <= 2 and function_names:
            joined = "/".join(function_names)
            if resolved_type == "contract":
                return f"{contract_name} {joined} contract", "medium"
            return f"{contract_name} {joined} operator", "medium"
        if resolved_type == "contract":
            return f"{contract_name} operator contract", "medium"
        return f"{contract_name} operator", "medium"
    if "authority_controller" in labels:
        return f"{contract_name} authority", "high"
    if "owner_controller" in labels and resolved_type == "safe":
        owner_of = graph_context[0].split(":", 1)[0] if graph_context else contract_name
        return f"{owner_of} owner Safe", "high"
    if "safe_signer" in labels:
        return "Safe signer", "high"
    if permission_effects:
        return f"{contract_name} permissioned principal", "medium"
    return _display_from_type(resolved_type), "high" if resolved_type != "unknown" else "low"


def build_principal_labels(
    effective_permissions: dict,
    *,
    resolved_control_graph: dict | None = None,
    rpc_url: str | None = None,
) -> PrincipalLabels:
    nodes_by_id = _node_by_id(resolved_control_graph or {})
    nodes_by_address = {node["address"].lower(): node for node in (resolved_control_graph or {}).get("nodes", [])}
    incoming_by_id = _incoming_edges(resolved_control_graph or {})
    permissions_by_address, permission_label_hints = _collect_permissions(effective_permissions)

    addresses = set(nodes_by_address)
    addresses.update(permissions_by_address)

    principals: list[PrincipalProfile] = []
    for address in sorted(addresses):
        if address == effective_permissions["contract_address"].lower():
            continue
        node = nodes_by_address.get(address)
        resolved_type = str(node.get("resolved_type", "unknown")) if node else "unknown"
        details = dict(node.get("details", {})) if node else {}

        if resolved_type == "unknown" and rpc_url:
            resolved_type, details = classify_resolved_address(rpc_url, address)

        labels, graph_context = _graph_labels_for_node(
            node or {"resolved_type": resolved_type}, incoming_by_id.get((node or {}).get("id", ""), []), nodes_by_id
        )
        hint_string = permission_label_hints.get(address)
        if hint_string:
            labels.update(hint_string.split(","))

        permissions = sorted(
            permissions_by_address.get(address, []),
            key=lambda item: (item["function"], -1 if item["role"] is None else item["role"]),
        )
        display_name, confidence = _display_name(
            address,
            resolved_type,
            labels,
            graph_context,
            permissions,
            effective_permissions["contract_name"],
        )

        principals.append(
            {
                "address": address,
                "resolved_type": resolved_type,  # type: ignore[typeddict-item]
                "display_name": display_name,
                "labels": sorted(label for label in labels if label),
                "confidence": confidence,  # type: ignore[typeddict-item]
                "details": details,
                "graph_context": graph_context,
                "permissions": permissions,
            }
        )

    return {
        "schema_version": "0.1",
        "contract_address": effective_permissions["contract_address"],
        "contract_name": effective_permissions["contract_name"],
        "principals": principals,
    }


def write_principal_labels_from_files(
    effective_permissions_path: Path,
    *,
    resolved_control_graph_path: Path | None = None,
    rpc_url: str | None = None,
    output_path: Path | None = None,
) -> Path:
    effective_permissions = _load_json(effective_permissions_path)
    resolved_control_graph = _load_json(resolved_control_graph_path) if resolved_control_graph_path else None

    payload = build_principal_labels(
        effective_permissions,
        resolved_control_graph=resolved_control_graph,
        rpc_url=rpc_url,
    )
    if output_path is None:
        output_path = effective_permissions_path.with_name("principal_labels.json")
    output_path.write_text(json.dumps(payload, indent=2) + "\n")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build frontend-friendly principal labels from permission and control-graph artifacts."
    )
    parser.add_argument("effective_permissions", help="Path to effective_permissions.json")
    parser.add_argument("--resolved-graph", help="Optional path to resolved_control_graph.json")
    parser.add_argument("--rpc", help="Optional RPC URL for classifying addresses missing from the resolved graph")
    parser.add_argument("--out", help="Optional output path for principal_labels.json")
    args = parser.parse_args()

    output_path = write_principal_labels_from_files(
        Path(args.effective_permissions),
        resolved_control_graph_path=Path(args.resolved_graph) if args.resolved_graph else None,
        rpc_url=args.rpc,
        output_path=Path(args.out) if args.out else None,
    )
    print(f"Principal labels: {output_path}")


if __name__ == "__main__":
    main()
