"""Join protected-contract analysis with authority policy state."""

from __future__ import annotations

from typing import Any, cast

from eth_utils.crypto import keccak

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


def _known_principals(*snapshots: dict[str, Any] | None) -> dict[str, ResolvedPrincipal]:
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


def _controller_lookup(snapshot: dict[str, Any] | None) -> dict[str, list[tuple[str, dict[str, Any]]]]:
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
    controller_lookup = _controller_lookup(target_snapshot)
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

        controller_grants = _controller_grants_for_refs(controller_refs, controller_lookup, known)
        notes = []
        if "authority" in controller_refs and authority_value:
            notes.append(f"authority={authority_value}")
        if direct_owner is None and "owner" in controller_refs and owner_value:
            notes.append(f"owner={owner_value}")

        function_permission: EffectiveFunctionPermission = {
            "function": privileged["function"],
            "abi_signature": _abi_signature(privileged["function"]),
            "selector": selector,
            "direct_owner": direct_owner,
            "authority_public": bool(public_by_selector.get(selector, False)),
            "authority_roles": role_grants,
            "controllers": controller_grants,
            "effect_targets": list(privileged.get("effect_targets", [])),
            "effect_labels": list(privileged.get("effect_labels", [])),
            "action_summary": privileged.get("action_summary", "Performs a permissioned contract action."),
            "notes": notes,
        }
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
