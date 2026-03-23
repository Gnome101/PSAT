#!/usr/bin/env python3
"""Join protected-contract analysis with authority policy state."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from eth_utils import keccak

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from schemas.effective_permissions import (
    AuthorityRoleGrant,
    EffectiveFunctionPermission,
    EffectivePermissions,
    PrincipalResolution,
    ResolvedPrincipal,
)

ELEMENTARY_TYPE_PREFIXES = (
    "address",
    "uint",
    "int",
    "bool",
    "bytes",
    "string",
    "fixed",
    "ufixed",
    "tuple",
)


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def _normalize_abi_type(type_name: str) -> str:
    stripped = type_name.strip()
    if not stripped:
        return stripped

    if stripped.endswith("]"):
        base, suffix = stripped.split("[", 1)
        return f"{_normalize_abi_type(base)}[{suffix}"

    if stripped.startswith(ELEMENTARY_TYPE_PREFIXES):
        return stripped

    return "address"


def _abi_signature(function_signature: str) -> str:
    if "(" not in function_signature or not function_signature.endswith(")"):
        return function_signature
    name, args = function_signature.split("(", 1)
    args = args[:-1]
    if not args:
        return f"{name}()"
    normalized_args = ",".join(_normalize_abi_type(arg) for arg in args.split(","))
    return f"{name}({normalized_args})"


def _selector(function_signature: str) -> str:
    return "0x" + keccak(text=_abi_signature(function_signature)).hex()[:8]


def _known_principals(*snapshots: dict | None) -> dict[str, ResolvedPrincipal]:
    known: dict[str, ResolvedPrincipal] = {}
    for snapshot in snapshots:
        if not snapshot:
            continue
        contract_name = snapshot.get("contract_name", "")
        for controller_id, value in snapshot.get("controller_values", {}).items():
            address = str(value.get("value", "")).lower()
            if not address.startswith("0x"):
                continue
            known[address] = {
                "address": address,
                "resolved_type": value.get("resolved_type", "unknown"),
                "details": dict(value.get("details", {})),
                "source_contract": contract_name,
                "source_controller_id": controller_id,
            }
    return known


def _principal_for_address(address: str, known: dict[str, ResolvedPrincipal]) -> ResolvedPrincipal:
    normalized = address.lower()
    return dict(known.get(normalized) or {"address": normalized, "resolved_type": "unknown", "details": {}})


def build_effective_permissions(
    target_analysis: dict,
    *,
    target_snapshot: dict | None = None,
    authority_snapshot: dict | None = None,
    policy_state: dict | None = None,
    artifact_paths: dict[str, str] | None = None,
    principal_resolution: PrincipalResolution | None = None,
) -> EffectivePermissions:
    contract_address = target_analysis["subject"]["address"].lower()
    contract_name = target_analysis["subject"]["name"]
    policy_state = policy_state or {"public_capabilities": [], "role_capabilities": [], "user_roles": []}

    known = _known_principals(target_snapshot, authority_snapshot)
    target_controller_values = (target_snapshot or {}).get("controller_values", {})
    owner_value = next(
        (value.get("value", "").lower() for key, value in target_controller_values.items() if key.endswith(":owner")),
        None,
    )
    authority_value = next(
        (value.get("value", "").lower() for key, value in target_controller_values.items() if key.endswith(":authority")),
        None,
    )

    public_by_selector = {
        entry["function_sig"].lower(): bool(entry["enabled"])
        for entry in policy_state.get("public_capabilities", [])
        if entry["target"].lower() == contract_address
    }

    role_capabilities_by_selector: dict[str, list[dict]] = {}
    for entry in policy_state.get("role_capabilities", []):
        if entry["target"].lower() != contract_address or not entry.get("enabled"):
            continue
        role_capabilities_by_selector.setdefault(entry["function_sig"].lower(), []).append(entry)

    users_by_role: dict[int, list[dict]] = {}
    for entry in policy_state.get("user_roles", []):
        if not entry.get("enabled"):
            continue
        users_by_role.setdefault(int(entry["role"]), []).append(entry)

    functions: list[EffectiveFunctionPermission] = []
    for privileged in target_analysis["access_control"]["privileged_functions"]:
        selector = _selector(privileged["function"])
        direct_owner = None
        if "owner" in privileged.get("controller_refs", []) and owner_value and owner_value != "0x0000000000000000000000000000000000000000":
            direct_owner = _principal_for_address(owner_value, known)

        role_grants: list[AuthorityRoleGrant] = []
        for capability in sorted(role_capabilities_by_selector.get(selector, []), key=lambda item: item["role"]):
            role = int(capability["role"])
            principals = [
                _principal_for_address(user_entry["user"], known)
                for user_entry in sorted(users_by_role.get(role, []), key=lambda item: item["user"])
            ]
            role_grants.append(
                {
                    "role": role,
                    "principals": principals,
                }
            )

        notes = []
        if "authority" in privileged.get("controller_refs", []) and authority_value:
            notes.append(f"authority={authority_value}")
        if direct_owner is None and "owner" in privileged.get("controller_refs", []) and owner_value:
            notes.append(f"owner={owner_value}")

        functions.append(
            {
                "function": privileged["function"],
                "abi_signature": _abi_signature(privileged["function"]),
                "selector": selector,
                "direct_owner": direct_owner,
                "authority_public": bool(public_by_selector.get(selector, False)),
                "authority_roles": role_grants,
                "effect_targets": list(privileged.get("effect_targets", [])),
                "effect_labels": list(privileged.get("effect_labels", [])),
                "action_summary": privileged.get("action_summary", "Performs a permissioned contract action."),
                "notes": notes,
            }
        )

    return {
        "schema_version": "0.1",
        "contract_address": contract_address,
        "contract_name": contract_name,
        "authority_contract": authority_value if authority_value else None,
        "principal_resolution": principal_resolution
        or {
            "status": "complete" if policy_state else "missing_policy_state",
            "reason": "Authority policy state was joined into the permission view."
            if policy_state
            else "No authority policy state was provided for this artifact.",
        },
        "artifacts": artifact_paths or {},
        "functions": functions,
    }


def write_effective_permissions_from_files(
    target_analysis_path: Path,
    *,
    target_snapshot_path: Path | None = None,
    authority_snapshot_path: Path | None = None,
    policy_state_path: Path | None = None,
    output_path: Path | None = None,
    principal_resolution: PrincipalResolution | None = None,
) -> Path:
    target_analysis = _load_json(target_analysis_path)
    target_snapshot = _load_json(target_snapshot_path) if target_snapshot_path else None
    authority_snapshot = _load_json(authority_snapshot_path) if authority_snapshot_path else None
    policy_state = _load_json(policy_state_path) if policy_state_path else None

    artifact_paths = {
        "target_analysis": str(target_analysis_path),
    }
    if target_snapshot_path:
        artifact_paths["target_snapshot"] = str(target_snapshot_path)
    if authority_snapshot_path:
        artifact_paths["authority_snapshot"] = str(authority_snapshot_path)
    if policy_state_path:
        artifact_paths["policy_state"] = str(policy_state_path)

    payload = build_effective_permissions(
        target_analysis,
        target_snapshot=target_snapshot,
        authority_snapshot=authority_snapshot,
        policy_state=policy_state,
        artifact_paths=artifact_paths,
        principal_resolution=principal_resolution,
    )
    if output_path is None:
        output_path = target_analysis_path.with_name("effective_permissions.json")
    output_path.write_text(json.dumps(payload, indent=2) + "\n")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve effective permissions from contract analysis and authority policy state.")
    parser.add_argument("target_analysis", help="Path to target contract_analysis.json")
    parser.add_argument("--target-snapshot", help="Optional path to target control_snapshot.json")
    parser.add_argument("--authority-snapshot", help="Optional path to authority control_snapshot.json")
    parser.add_argument("--policy-state", help="Optional path to authority policy_state.json")
    parser.add_argument("--out", help="Optional path to effective_permissions.json")
    args = parser.parse_args()

    output_path = write_effective_permissions_from_files(
        Path(args.target_analysis),
        target_snapshot_path=Path(args.target_snapshot) if args.target_snapshot else None,
        authority_snapshot_path=Path(args.authority_snapshot) if args.authority_snapshot else None,
        policy_state_path=Path(args.policy_state) if args.policy_state else None,
        output_path=Path(args.out) if args.out else None,
    )
    print(f"Effective permissions: {output_path}")


if __name__ == "__main__":
    main()
