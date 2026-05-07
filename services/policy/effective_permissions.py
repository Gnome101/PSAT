#!/usr/bin/env python3
"""Join protected-contract analysis with authority policy state."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import is_dataclass
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

logger = logging.getLogger("services.policy.effective_permissions")

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


def _normalize_capability_output(
    capability_resolver_output: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    """``capability_resolver_output`` may carry either dataclass
    ``CapabilityExpr`` instances (from a direct resolver call) or
    already-serialized dicts (from a persisted artifact / test
    fixture). Normalize to dicts up-front so downstream column shaping
    has one shape to handle."""
    if not capability_resolver_output:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for fn_signature, cap in capability_resolver_output.items():
        if cap is None:
            continue
        if isinstance(cap, dict):
            out[str(fn_signature)] = dict(cap)
            continue
        if is_dataclass(cap):
            try:
                from services.resolution.capability_resolver import capability_to_dict

                out[str(fn_signature)] = capability_to_dict(cap)  # type: ignore[arg-type]
            except Exception as exc:
                logger.warning(
                    "Failed to serialize CapabilityExpr for function %s: %s",
                    fn_signature,
                    exc,
                )
                continue
    return out


def _effects_by_function(
    effects: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    """The v2 ``effects`` artifact (Wave 2 T1) keyed by function full-name.
    Returns a flat ``{function_signature: effect_record}`` dict where each
    record carries ``effect_labels`` / ``effect_targets`` / ``action_summary``.

    Falls back to ``{}`` if the artifact is missing or malformed."""
    if not isinstance(effects, dict):
        return {}
    functions = effects.get("functions")
    if not isinstance(functions, dict):
        return {}
    return {str(fn_sig): record for fn_sig, record in functions.items() if isinstance(record, dict)}


_SENSITIVE_SINK_KINDS = frozenset({"state_write", "external_call", "delegatecall", "contract_creation", "selfdestruct"})


def _effect_record_has_sensitive_sink(record: Mapping[str, Any]) -> bool:
    for sink in record.get("sinks") or []:
        if isinstance(sink, dict) and sink.get("kind") in _SENSITIVE_SINK_KINDS:
            return True
    return False


def _function_records_from_v2(
    *,
    capability_dicts: Mapping[str, dict[str, Any]],
    effects_by_function: Mapping[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build effective-permission function records from v2 resolver/effects data."""
    signatures = set(capability_dicts)
    signatures.update(
        signature for signature, record in effects_by_function.items() if _effect_record_has_sensitive_sink(record)
    )

    records: list[dict[str, Any]] = []
    for signature in sorted(signatures):
        effect_info = effects_by_function.get(signature) or {}
        record: dict[str, Any] = {
            "function": signature,
            "controller_refs": [],
            "effect_targets": list(effect_info.get("effect_targets") or []),
            "effect_labels": list(effect_info.get("effect_labels") or []),
            "action_summary": effect_info.get("action_summary") or "Performs a permissioned contract action.",
        }
        records.append(record)
    return records


def _function_records_from_analysis(target_analysis: Mapping[str, Any] | ContractAnalysis) -> list[dict[str, Any]]:
    access_control = target_analysis.get("access_control") or {}
    if not isinstance(access_control, dict):
        return []
    return [dict(item) for item in access_control.get("privileged_functions") or [] if isinstance(item, dict)]


def _column_values_for_capability(cap_dict: dict[str, Any]) -> dict[str, Any]:
    """Mirror of the writer's per-kind column rules — kept here so the
    artifact dict carries the right shape even when the writer isn't
    invoked (read-only callers like the recursive resolver)."""
    kind = cap_dict.get("kind")
    out: dict[str, Any] = {
        "capability_expr": dict(cap_dict),
        "conditions": None,
        "status": None,
        "authority_public": False,
    }
    if kind == "conditional_universal":
        out["conditions"] = list(cap_dict.get("conditions") or [])
        out["status"] = "public"
        out["authority_public"] = True
    elif kind == "unsupported":
        out["status"] = "unsupported"
    return out


def build_effective_permissions(
    target_analysis: Mapping[str, Any] | ContractAnalysis,
    *,
    target_snapshot: Mapping[str, Any] | ControlSnapshot | None = None,
    authority_snapshot: Mapping[str, Any] | ControlSnapshot | None = None,
    policy_state: Mapping[str, Any] | None = None,
    artifact_paths: dict[str, str] | None = None,
    principal_resolution: PrincipalResolution | None = None,
    predicate_trees: Mapping[str, Any] | None = None,
    capability_resolver_output: Mapping[str, Any] | None = None,
    effects: Mapping[str, Any] | None = None,
) -> EffectivePermissions:
    """Build the ``effective_permissions`` artifact from v2 resolver/effects
    inputs. If no v2 inputs are supplied, old fixture/read paths can still
    provide ``access_control.privileged_functions`` as a compatibility source.

    ``capability_resolver_output`` is the per-function CapabilityExpr
    dict the resolver produces. Tests typically supply it directly to
    avoid spinning up Slither + the full adapter chain.

    ``effects`` is the v2 ``effects`` artifact keyed by function full-name.
    """
    # ``predicate_trees`` is accepted for cutover signalling; the resolver
    # consumes the trees and passes the normalized capability output here.
    del predicate_trees

    contract_address = target_analysis["subject"]["address"].lower()
    contract_name = target_analysis["subject"]["name"]
    policy_state = policy_state or {"public_capabilities": [], "role_capabilities": [], "user_roles": []}

    known = _known_principals(target_snapshot, authority_snapshot)
    target_controller_values = (target_snapshot or {}).get("controller_values", {})
    controller_lookup = _controller_lookup(target_snapshot)
    capability_dicts = _normalize_capability_output(capability_resolver_output)
    effects_by_function = _effects_by_function(effects)
    if capability_dicts or effects_by_function:
        function_records = _function_records_from_v2(
            capability_dicts=capability_dicts,
            effects_by_function=effects_by_function,
        )
    else:
        function_records = _function_records_from_analysis(target_analysis)
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
    for privileged in function_records:
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

        notes: list[str] = []
        controller_grants = _controller_grants_for_refs(controller_refs, controller_lookup, known)
        if "authority" in controller_refs and authority_value:
            notes.append(f"authority={authority_value}")
        if direct_owner is None and "owner" in controller_refs and owner_value:
            notes.append(f"owner={owner_value}")

        # The ``effects`` artifact is the source of truth for effect labels,
        # targets, and summaries when present; compatibility records may carry
        # the same fields directly.
        fn_signature = privileged["function"]
        effects_record = effects_by_function.get(fn_signature) or {}
        v2_effect_labels = effects_record.get("effect_labels") if effects_record else None
        v2_effect_targets = effects_record.get("effect_targets") if effects_record else None
        v2_action_summary = effects_record.get("action_summary") if effects_record else None

        effect_labels_out = (
            list(v2_effect_labels) if isinstance(v2_effect_labels, list) else list(privileged.get("effect_labels", []))
        )
        effect_targets_out = (
            list(v2_effect_targets)
            if isinstance(v2_effect_targets, list)
            else list(privileged.get("effect_targets", []))
        )
        action_summary_out = (
            v2_action_summary
            if isinstance(v2_action_summary, str) and v2_action_summary
            else privileged.get("action_summary", "Performs a permissioned contract action.")
        )

        function_permission: EffectiveFunctionPermission = {
            "function": fn_signature,
            "abi_signature": _abi_signature(fn_signature),
            "selector": selector,
            "direct_owner": direct_owner,
            "authority_public": bool(public_by_selector.get(selector, False)),
            "authority_roles": role_grants,
            "controllers": controller_grants,
            "effect_targets": effect_targets_out,
            "effect_labels": effect_labels_out,
            "action_summary": action_summary_out,
            "notes": notes,
        }

        # v2 capability columns: when a CapabilityExpr is supplied for this
        # function, it dictates capability_expr / conditions / status /
        # authority_public. The dict-form override here lets the writer
        # propagate these columns onto EffectiveFunction without re-resolving.
        cap_dict = capability_dicts.get(fn_signature)
        if cap_dict is not None:
            cap_columns = _column_values_for_capability(cap_dict)
            function_permission["capability_expr"] = cap_columns["capability_expr"]
            if cap_columns["conditions"] is not None:
                function_permission["conditions"] = cap_columns["conditions"]
            if cap_columns["status"] is not None:
                function_permission["status"] = cap_columns["status"]
            # conditional_universal short-circuits authority_public.
            if cap_columns["authority_public"]:
                function_permission["authority_public"] = True

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
    resolved_control_graph_path: Path | None = None,
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
    if resolved_control_graph_path:
        artifact_paths["resolved_control_graph"] = str(resolved_control_graph_path)

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
