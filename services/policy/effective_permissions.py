#!/usr/bin/env python3
"""Join protected-contract analysis with authority policy state."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, cast

from eth_utils.crypto import keccak

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from schemas.contract_analysis import ContractAnalysis
from schemas.control_tracking import ControlSnapshot
from schemas.effective_permissions import (
    AuthorityRoleGrant,
    EffectiveFunctionPermission,
    EffectivePermissions,
    PrincipalResolution,
    ResolvedAddressType,
    ResolvedControllerGrant,
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


def _lower_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).lower()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _normalize_abi_type(type_name: str) -> str:
    stripped = type_name.strip()
    if not stripped:
        return stripped

    if stripped.startswith("DynArray[") and stripped.endswith("]"):
        inner = stripped[len("DynArray[") : -1]
        parts = inner.split(",", 1)
        return f"{_normalize_abi_type(parts[0])}[]"
    if stripped.startswith("HashMap[") and stripped.endswith("]"):
        return "mapping"
    if stripped.startswith("String[") and stripped.endswith("]"):
        return "string"
    if stripped.startswith("Bytes[") and stripped.endswith("]"):
        return "bytes"
    if stripped.endswith("]"):
        if "[" not in stripped:
            return "address"
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
    raw_args: list[str] = []
    current: list[str] = []
    depth = 0
    for char in args:
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth = max(depth - 1, 0)
        if char == "," and depth == 0:
            piece = "".join(current).strip()
            if piece:
                raw_args.append(piece)
            current = []
            continue
        current.append(char)
    piece = "".join(current).strip()
    if piece:
        raw_args.append(piece)
    normalized_args = ",".join(_normalize_abi_type(arg) for arg in raw_args)
    return f"{name}({normalized_args})"


def _selector(function_signature: str) -> str:
    return "0x" + keccak(text=_abi_signature(function_signature)).hex()[:8]


def _resolved_principal(
    address: str,
    resolved_type: ResolvedAddressType,
    details: dict[str, object],
    *,
    source_contract: str | None = None,
    source_controller_id: str | None = None,
) -> ResolvedPrincipal:
    payload: ResolvedPrincipal = {
        "address": address,
        "resolved_type": resolved_type,
        "details": details,
    }
    if source_contract is not None:
        payload["source_contract"] = source_contract
    if source_controller_id is not None:
        payload["source_controller_id"] = source_controller_id
    return payload


def _known_principals(*snapshots: Mapping[str, Any] | None) -> dict[str, ResolvedPrincipal]:
    known: dict[str, ResolvedPrincipal] = {}
    for snapshot in snapshots:
        if not snapshot:
            continue
        contract_name = str(snapshot.get("contract_name", ""))
        controller_values = snapshot.get("controller_values", {})
        if not isinstance(controller_values, dict):
            continue
        for controller_id, value in controller_values.items():
            if not isinstance(controller_id, str) or not isinstance(value, dict):
                continue
            address_raw = value.get("value", "")
            address = _lower_string(address_raw)
            if not address.startswith("0x"):
                continue
            details_raw = value.get("details", {})
            details = dict(details_raw) if isinstance(details_raw, dict) else {}
            known[address] = _resolved_principal(
                address,
                cast(ResolvedAddressType, str(value.get("resolved_type", "unknown"))),
                details,
                source_contract=contract_name,
                source_controller_id=controller_id,
            )
    return known


def _principal_for_address(address: str, known: dict[str, ResolvedPrincipal]) -> ResolvedPrincipal:
    normalized = address.lower()
    return known.get(normalized) or _resolved_principal(normalized, "unknown", {})


def _controller_lookup(snapshot: Mapping[str, Any] | None) -> dict[str, list[tuple[str, dict[str, Any]]]]:
    lookup: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    controller_values = (snapshot or {}).get("controller_values", {})
    if not isinstance(controller_values, dict):
        return lookup
    for controller_id, value in controller_values.items():
        if not isinstance(controller_id, str) or not isinstance(value, dict):
            continue
        source = str(value.get("source", controller_id))
        lookup.setdefault(source, []).append((controller_id, value))
    return lookup


def _snapshot_address(snapshot: Mapping[str, Any] | None) -> str | None:
    if not snapshot:
        return None
    address = _lower_string(snapshot.get("contract_address", ""))
    if address.startswith("0x") and len(address) == 42:
        return address
    return None


def _external_snapshot_for_source(
    source: str,
    controller_lookup: dict[str, list[tuple[str, dict[str, Any]]]],
    external_snapshots: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    for controller_id, value in controller_lookup.get(source, []):
        if not isinstance(controller_id, str) or not isinstance(value, dict):
            continue
        address = _lower_string(value.get("value", ""))
        if address.startswith("0x") and len(address) == 42 and address in external_snapshots:
            return external_snapshots[address]
    return None


def _external_policy_state_for_source(
    source: str,
    controller_lookup: dict[str, list[tuple[str, dict[str, Any]]]],
    external_policy_states: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    for controller_id, value in controller_lookup.get(source, []):
        if not isinstance(controller_id, str) or not isinstance(value, dict):
            continue
        address = _lower_string(value.get("value", ""))
        if address.startswith("0x") and len(address) == 42 and address in external_policy_states:
            return external_policy_states[address]
    return None


def _controller_grants_for_refs(
    controller_refs: list[str],
    controller_lookup: dict[str, list[tuple[str, dict[str, Any]]]],
    known: dict[str, ResolvedPrincipal],
) -> list[ResolvedControllerGrant]:
    grants: list[ResolvedControllerGrant] = []
    seen: set[str] = set()
    for ref in controller_refs:
        if ref in {"owner", "authority"}:
            continue
        for controller_id, value in controller_lookup.get(ref, []):
            if controller_id in seen:
                continue
            seen.add(controller_id)
            raw_value = _lower_string(value.get("value", ""))
            principals: list[ResolvedPrincipal] = []
            notes: list[str] = []
            details_raw = value.get("details", {})
            details = dict(details_raw) if isinstance(details_raw, dict) else {}
            raw_principals = details.get("resolved_principals", [])
            if isinstance(raw_principals, list):
                for principal in raw_principals:
                    if not isinstance(principal, dict):
                        continue
                    address = str(principal.get("address", "")).lower()
                    if not address.startswith("0x"):
                        continue
                    principal_details_raw = principal.get("details", {})
                    principal_details = dict(principal_details_raw) if isinstance(principal_details_raw, dict) else {}
                    principals.append(
                        _resolved_principal(
                            address,
                            cast(ResolvedAddressType, str(principal.get("resolved_type", "unknown"))),
                            principal_details,
                            source_controller_id=controller_id,
                        )
                    )
            if (
                raw_value.startswith("0x")
                and len(raw_value) == 42
                and raw_value != "0x0000000000000000000000000000000000000000"
            ):
                kind = controller_id.split(":", 1)[0] if ":" in controller_id else "unknown"
                ref_lower = ref.lower()
                auth_like = any(
                    token in ref_lower
                    for token in ("owner", "admin", "govern", "guardian", "authority", "committee", "timelock")
                )
                if not principals and (kind in {"state_variable", "singleton_slot", "computed"} or auth_like):
                    principals.append(_principal_for_address(raw_value, known))
            elif raw_value and not principals:
                notes.append(f"value={raw_value}")
            kind = controller_id.split(":", 1)[0] if ":" in controller_id else "unknown"
            if not principals:
                continue
            grants.append(
                {
                    "controller_id": controller_id,
                    "label": ref,
                    "source": ref,
                    "kind": kind,
                    "principals": principals,
                    "notes": notes,
                }
            )
    return grants


def _controller_refs_from_effect_targets(effect_targets: list[str]) -> list[str]:
    refs: list[str] = []
    for target in effect_targets:
        lowered = str(target or "").lower()
        if ".onlyprotocolupgrader" in lowered and "roleregistry" in lowered:
            refs.append("roleRegistry")
    return sorted(set(refs))


def _semantic_guards_by_function(semantic_guards: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(semantic_guards, dict):
        return {}
    entries = semantic_guards.get("functions", [])
    if not isinstance(entries, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        function = str(entry.get("function", ""))
        if function:
            result[function] = entry
    return result


def _semantic_controller_grants(
    semantic_entry: dict[str, Any],
    *,
    target_function_selector: str,
    target_snapshot: Mapping[str, Any] | None,
    controller_lookup: dict[str, list[tuple[str, dict[str, Any]]]],
    external_snapshots: dict[str, dict[str, Any]],
    external_policy_states: dict[str, dict[str, Any]],
    known: dict[str, ResolvedPrincipal],
) -> tuple[
    ResolvedPrincipal | None,
    list[ResolvedControllerGrant],
    list[str],
    bool | None,
    list[AuthorityRoleGrant] | None,
]:
    direct_owner: ResolvedPrincipal | None = None
    controller_grants: list[ResolvedControllerGrant] = []
    notes = list(semantic_entry.get("notes", [])) if isinstance(semantic_entry.get("notes"), list) else []
    semantic_status = str(semantic_entry.get("status", ""))
    semantic_authority_public: bool | None = None
    semantic_role_grants: list[AuthorityRoleGrant] | None = None
    predicates = semantic_entry.get("predicates", [])
    if not isinstance(predicates, list):
        return direct_owner, controller_grants, notes, semantic_authority_public, semantic_role_grants

    for predicate in predicates:
        if not isinstance(predicate, dict):
            continue
        kind = str(predicate.get("kind", ""))

        if kind == "role_member":
            role_source = str(predicate.get("role_source", ""))
            authority_source_raw = predicate.get("authority_source", "")
            if isinstance(authority_source_raw, list):
                authority_sources = [str(item) for item in authority_source_raw if str(item)]
                authority_source = authority_sources[0] if len(authority_sources) == 1 else ""
            else:
                authority_source = str(authority_source_raw or "")
            if role_source:
                external_snapshot = (
                    _external_snapshot_for_source(authority_source, controller_lookup, external_snapshots)
                    if authority_source
                    else None
                )
                if external_snapshot is not None:
                    external_known = dict(known)
                    external_known.update(_known_principals(external_snapshot))
                    external_lookup = _controller_lookup(external_snapshot)
                    controller_grants.extend(
                        _controller_grants_for_refs([role_source], external_lookup, external_known)
                    )
                else:
                    controller_grants.extend(_controller_grants_for_refs([role_source], controller_lookup, known))
            continue

        if kind == "mapping_membership":
            source = str(predicate.get("controller_source", ""))
            if source:
                controller_grants.extend(_controller_grants_for_refs([source], controller_lookup, known))
            continue

        if kind == "caller_equals_controller":
            controller_source = str(predicate.get("controller_source", ""))
            read_spec = predicate.get("read_spec")
            contract_source = read_spec.get("contract_source") if isinstance(read_spec, dict) else None
            contract_source = str(contract_source) if isinstance(contract_source, str) and contract_source else None

            if contract_source:
                external_snapshot = _external_snapshot_for_source(
                    contract_source,
                    controller_lookup,
                    external_snapshots,
                )
                if external_snapshot is None:
                    continue
                external_known = dict(known)
                external_known.update(_known_principals(external_snapshot))
                external_lookup = _controller_lookup(external_snapshot)
                external_owner_value = next(
                    (
                        _lower_string(value.get("value", ""))
                        for key, value in (external_snapshot.get("controller_values", {}) or {}).items()
                        if isinstance(key, str) and key.endswith(":owner") and isinstance(value, dict)
                    ),
                    None,
                )
                if controller_source == "owner" and external_owner_value:
                    principal = _principal_for_address(external_owner_value, external_known)
                    controller_grants.append(
                        {
                            "controller_id": "external_owner:owner",
                            "label": "owner",
                            "source": "owner",
                            "kind": "state_variable",
                            "principals": [principal],
                            "notes": [f"contract_source={contract_source}"],
                        }
                    )
                else:
                    controller_grants.extend(
                        _controller_grants_for_refs([controller_source], external_lookup, external_known)
                    )
                continue

            if controller_source == "owner" and target_snapshot:
                owner_value = next(
                    (
                        _lower_string(value.get("value", ""))
                        for key, value in (target_snapshot.get("controller_values", {}) or {}).items()
                        if isinstance(key, str) and key.endswith(":owner") and isinstance(value, dict)
                    ),
                    None,
                )
                if owner_value and owner_value != "0x0000000000000000000000000000000000000000":
                    direct_owner = _principal_for_address(owner_value, known)
                    continue

            if controller_source:
                controller_grants.extend(_controller_grants_for_refs([controller_source], controller_lookup, known))
            continue

        if kind == "policy_check":
            authority_source_raw = predicate.get("authority_source", "")
            if isinstance(authority_source_raw, list):
                authority_sources = [str(item) for item in authority_source_raw if str(item)]
                authority_source = authority_sources[0] if len(authority_sources) == 1 else ""
            else:
                authority_source = str(authority_source_raw or "")
            if not authority_source:
                continue
            policy_state = _external_policy_state_for_source(
                authority_source,
                controller_lookup,
                external_policy_states,
            )
            if policy_state is None:
                continue

            target_address = _snapshot_address(target_snapshot)
            public_enabled = any(
                str(entry.get("target", "")).lower() == target_address
                and str(entry.get("function_sig", "")).lower() == target_function_selector
                and bool(entry.get("enabled"))
                for entry in policy_state.get("public_capabilities", [])
                if isinstance(entry, dict)
            )
            role_capabilities = [
                entry
                for entry in policy_state.get("role_capabilities", [])
                if isinstance(entry, dict)
                and str(entry.get("target", "")).lower() == target_address
                and str(entry.get("function_sig", "")).lower() == target_function_selector
                and bool(entry.get("enabled"))
            ]
            users_by_role: dict[int, list[dict[str, Any]]] = {}
            for entry in policy_state.get("user_roles", []):
                if not isinstance(entry, dict) or not entry.get("enabled"):
                    continue
                users_by_role.setdefault(int(entry["role"]), []).append(entry)

            policy_snapshot = _external_snapshot_for_source(authority_source, controller_lookup, external_snapshots)
            policy_known = dict(known)
            if policy_snapshot:
                policy_known.update(_known_principals(policy_snapshot))

            semantic_role_grants = []
            for capability in sorted(role_capabilities, key=lambda item: int(item["role"])):
                role = int(capability["role"])
                principals = [
                    _principal_for_address(user_entry["user"], policy_known)
                    for user_entry in sorted(users_by_role.get(role, []), key=lambda item: item["user"])
                ]
                semantic_role_grants.append({"role": role, "principals": principals})

            semantic_authority_public = public_enabled
            notes.append(f"policy_check={authority_source}")

    if semantic_status == "public":
        notes.append("semantic_guards=public")

    deduped: list[ResolvedControllerGrant] = []
    seen_ids: set[str] = set()
    for grant in controller_grants:
        if grant["controller_id"] in seen_ids:
            continue
        seen_ids.add(grant["controller_id"])
        deduped.append(grant)
    return direct_owner, deduped, notes, semantic_authority_public, semantic_role_grants


def build_effective_permissions(
    target_analysis: Mapping[str, Any] | ContractAnalysis,
    *,
    target_snapshot: Mapping[str, Any] | ControlSnapshot | None = None,
    authority_snapshot: Mapping[str, Any] | ControlSnapshot | None = None,
    policy_state: Mapping[str, Any] | None = None,
    semantic_guards: Mapping[str, Any] | None = None,
    external_snapshots: dict[str, dict[str, Any]] | None = None,
    external_policy_states: dict[str, dict[str, Any]] | None = None,
    artifact_paths: dict[str, str] | None = None,
    principal_resolution: PrincipalResolution | None = None,
) -> EffectivePermissions:
    contract_address = target_analysis["subject"]["address"].lower()
    contract_name = target_analysis["subject"]["name"]
    policy_state = policy_state or {"public_capabilities": [], "role_capabilities": [], "user_roles": []}
    external_snapshots = external_snapshots or {}
    external_policy_states = external_policy_states or {}

    known = _known_principals(target_snapshot, authority_snapshot)
    target_controller_values = (target_snapshot or {}).get("controller_values", {})
    controller_lookup = _controller_lookup(target_snapshot)
    semantic_by_function = _semantic_guards_by_function(semantic_guards)
    owner_value = next(
        (
            _lower_string(value.get("value", ""))
            for key, value in target_controller_values.items()
            if key.endswith(":owner")
        ),
        None,
    )
    authority_value = next(
        (
            _lower_string(value.get("value", ""))
            for key, value in target_controller_values.items()
            if key.endswith(":authority")
        ),
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
        controller_refs = list(privileged.get("controller_refs", [])) + _controller_refs_from_effect_targets(
            list(privileged.get("effect_targets", []))
        )
        controller_refs = sorted(set(controller_refs))
        direct_owner = None
        if "owner" in controller_refs and owner_value and owner_value != "0x0000000000000000000000000000000000000000":
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

        semantic_entry = semantic_by_function.get(privileged["function"])
        semantic_direct_owner = None
        semantic_controller_grants: list[ResolvedControllerGrant] = []
        semantic_authority_public: bool | None = None
        semantic_role_grants: list[AuthorityRoleGrant] | None = None
        notes: list[str] = []
        if semantic_entry:
            (
                semantic_direct_owner,
                semantic_controller_grants,
                semantic_notes,
                semantic_authority_public,
                semantic_role_grants,
            ) = _semantic_controller_grants(
                semantic_entry,
                target_function_selector=selector,
                target_snapshot=target_snapshot,
                controller_lookup=controller_lookup,
                external_snapshots=external_snapshots,
                external_policy_states=external_policy_states,
                known=known,
            )
            notes.extend(semantic_notes)
        controller_grants = (
            semantic_controller_grants
            if semantic_entry
            and (semantic_controller_grants or semantic_direct_owner or semantic_entry.get("status") != "unresolved")
            else _controller_grants_for_refs(controller_refs, controller_lookup, known)
        )
        if semantic_direct_owner is not None:
            direct_owner = semantic_direct_owner
        if semantic_role_grants is not None:
            role_grants = semantic_role_grants
        if "authority" in controller_refs and authority_value:
            notes.append(f"authority={authority_value}")
        if direct_owner is None and "owner" in controller_refs and owner_value:
            notes.append(f"owner={owner_value}")

        function_permission: EffectiveFunctionPermission = {
            "function": privileged["function"],
            "abi_signature": _abi_signature(privileged["function"]),
            "selector": selector,
            "direct_owner": direct_owner,
            "authority_public": (
                semantic_authority_public
                if semantic_authority_public is not None
                else bool(public_by_selector.get(selector, False))
            ),
            "authority_roles": role_grants,
            "controllers": controller_grants,
            "effect_targets": list(privileged.get("effect_targets", [])),
            "effect_labels": list(privileged.get("effect_labels", [])),
            "action_summary": privileged.get("action_summary", "Performs a permissioned contract action."),
            "notes": notes,
        }
        external_call_guards = list(privileged.get("external_call_guards") or [])
        if external_call_guards:
            function_permission["external_call_guards"] = [dict(g) for g in external_call_guards]
        # Phase 4: pass the full sinks list through so policy_worker's
        # sink-dispatch bridge can route caller_equals / caller_in_mapping
        # sinks that the legacy external_call_guards projection drops.
        caller_sinks = list(privileged.get("sinks") or [])
        if caller_sinks:
            function_permission["sinks"] = [dict(s) for s in caller_sinks]
        functions.append(function_permission)

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
    semantic_guards_path: Path | None = None,
    resolved_control_graph_path: Path | None = None,
    output_path: Path | None = None,
    principal_resolution: PrincipalResolution | None = None,
) -> Path:
    target_analysis = _load_json(target_analysis_path)
    target_snapshot = _load_json(target_snapshot_path) if target_snapshot_path else None
    authority_snapshot = _load_json(authority_snapshot_path) if authority_snapshot_path else None
    policy_state = _load_json(policy_state_path) if policy_state_path else None
    semantic_guards = _load_json(semantic_guards_path) if semantic_guards_path else None
    external_snapshots: dict[str, dict[str, Any]] = {}
    external_policy_states: dict[str, dict[str, Any]] = {}
    if resolved_control_graph_path:
        resolved_control_graph = _load_json(resolved_control_graph_path)
        for node in resolved_control_graph.get("nodes", []):
            if not isinstance(node, dict):
                continue
            address = _lower_string(node.get("address", ""))
            artifacts_obj = node.get("artifacts")
            artifacts: dict[str, Any] = artifacts_obj if isinstance(artifacts_obj, dict) else {}
            snapshot_ref = artifacts.get("snapshot")
            if (
                address.startswith("0x")
                and len(address) == 42
                and isinstance(snapshot_ref, str)
                and Path(snapshot_ref).exists()
            ):
                external_snapshots[address] = _load_json(Path(snapshot_ref))
                policy_path = Path(snapshot_ref).with_name("policy_state.json")
                if policy_path.exists():
                    external_policy_states[address] = _load_json(policy_path)

    artifact_paths = {
        "target_analysis": str(target_analysis_path),
    }
    if target_snapshot_path:
        artifact_paths["target_snapshot"] = str(target_snapshot_path)
    if authority_snapshot_path:
        artifact_paths["authority_snapshot"] = str(authority_snapshot_path)
    if policy_state_path:
        artifact_paths["policy_state"] = str(policy_state_path)
    if semantic_guards_path:
        artifact_paths["semantic_guards"] = str(semantic_guards_path)
    if resolved_control_graph_path:
        artifact_paths["resolved_control_graph"] = str(resolved_control_graph_path)

    payload = build_effective_permissions(
        target_analysis,
        target_snapshot=target_snapshot,
        authority_snapshot=authority_snapshot,
        policy_state=policy_state,
        semantic_guards=semantic_guards,
        external_snapshots=external_snapshots,
        external_policy_states=external_policy_states,
        artifact_paths=artifact_paths,
        principal_resolution=principal_resolution,
    )
    if output_path is None:
        output_path = target_analysis_path.with_name("effective_permissions.json")
    output_path.write_text(json.dumps(payload, indent=2) + "\n")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve effective permissions from contract analysis and authority policy state."
    )
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
