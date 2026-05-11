"""Projection helpers for semantic capability trees.

The resolver preserves the full capability algebra. Policy rows and API
payloads need a narrower view: materializable caller rows, public paths,
and residual unresolved checks. Keep that interpretation in one place so
DB and artifact paths do not drift.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class CapabilitySurface:
    principal_rows: list[dict[str, Any]] = field(default_factory=list)
    public_paths: list[list[dict[str, Any]]] = field(default_factory=list)
    residual: list[dict[str, Any]] = field(default_factory=list)

    @property
    def authority_public(self) -> bool:
        return bool(self.public_paths)

    @property
    def conditions(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in self.principal_rows:
            details = row.get("details")
            if isinstance(details, dict):
                out.extend(_condition_dicts(details.get("conditions")))
        for path in self.public_paths:
            out.extend(path)
        return _unique_conditions(out)


def project_capability_surface(
    cap_dict: dict[str, Any],
    *,
    safe_address_lookup: dict[str, str] | None = None,
    function_signature: str | None = None,
) -> CapabilitySurface:
    surface = _project_node(
        cap_dict,
        safe_address_lookup=safe_address_lookup,
        function_signature=function_signature,
    )
    surface.principal_rows = _dedupe_rows(surface.principal_rows)
    surface.public_paths = [_unique_conditions(path) for path in surface.public_paths]
    return surface


def _project_node(
    cap_dict: dict[str, Any],
    *,
    safe_address_lookup: dict[str, str] | None,
    function_signature: str | None,
) -> CapabilitySurface:
    kind = cap_dict.get("kind")
    node_conditions = _condition_dicts(cap_dict.get("conditions"))

    if kind == "finite_set":
        return CapabilitySurface(principal_rows=_rows_for_finite_set(cap_dict, node_conditions))
    if kind == "threshold_group":
        return CapabilitySurface(
            principal_rows=_rows_for_threshold_group(
                cap_dict,
                conditions=node_conditions,
                safe_address_lookup=safe_address_lookup,
                function_signature=function_signature,
            )
        )
    if kind == "signature_witness":
        return CapabilitySurface(principal_rows=_rows_for_signature_witness(cap_dict, node_conditions))
    if kind == "conditional_universal":
        return CapabilitySurface(public_paths=[node_conditions])
    if kind == "OR":
        surface = CapabilitySurface()
        for child in _child_dicts(cap_dict):
            child_surface = _project_node(
                child,
                safe_address_lookup=safe_address_lookup,
                function_signature=function_signature,
            )
            surface = _or_surface(surface, child_surface)
        if node_conditions:
            surface = _and_surface(CapabilitySurface(public_paths=[node_conditions]), surface)
        return surface
    if kind == "AND":
        surface = CapabilitySurface(public_paths=[node_conditions])
        for child in _child_dicts(cap_dict):
            child_surface = _project_node(
                child,
                safe_address_lookup=safe_address_lookup,
                function_signature=function_signature,
            )
            surface = _and_surface(surface, child_surface)
        return surface
    return CapabilitySurface(residual=[dict(cap_dict)])


def _or_surface(left: CapabilitySurface, right: CapabilitySurface) -> CapabilitySurface:
    return CapabilitySurface(
        principal_rows=left.principal_rows + right.principal_rows,
        public_paths=left.public_paths + right.public_paths,
        residual=left.residual + right.residual,
    )


def _and_surface(left: CapabilitySurface, right: CapabilitySurface) -> CapabilitySurface:
    if not _has_valid_path(left) or not _has_valid_path(right):
        return CapabilitySurface(residual=left.residual + right.residual)

    public_paths: list[list[dict[str, Any]]] = []
    for left_path in left.public_paths:
        for right_path in right.public_paths:
            public_paths.append(_unique_conditions(left_path + right_path))

    rows: list[dict[str, Any]] = []
    for row in left.principal_rows:
        for path in right.public_paths:
            rows.append(_row_with_conditions(row, path))
    for row in right.principal_rows:
        for path in left.public_paths:
            rows.append(_row_with_conditions(row, path))

    residual = left.residual + right.residual
    if left.principal_rows and right.principal_rows:
        residual.append({"kind": "unsupported", "unsupported_reason": "and_multiple_principal_shapes"})

    return CapabilitySurface(principal_rows=rows, public_paths=public_paths, residual=residual)


def _has_valid_path(surface: CapabilitySurface) -> bool:
    return bool(surface.principal_rows or surface.public_paths)


def _rows_for_finite_set(cap_dict: dict[str, Any], conditions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    members = cap_dict.get("members") or []
    for member in members:
        if not isinstance(member, str) or not member.startswith("0x") or len(member) != 42:
            continue
        rows.append(
            {
                "address": member.lower(),
                "resolved_type": None,
                "origin": "semantic_capability:finite_set",
                "principal_type": "controller",
                "details": _details_with_conditions(
                    {
                        "source": "semantic_predicate_capability_resolver",
                        "membership_quality": cap_dict.get("membership_quality"),
                        "confidence": cap_dict.get("confidence"),
                        "trace": cap_dict.get("trace") or [],
                    },
                    conditions,
                ),
            }
        )
    return rows


def _rows_for_threshold_group(
    cap_dict: dict[str, Any],
    *,
    conditions: list[dict[str, Any]],
    safe_address_lookup: dict[str, str] | None,
    function_signature: str | None,
) -> list[dict[str, Any]]:
    threshold = cap_dict.get("threshold") or {}
    if not isinstance(threshold, dict):
        return []
    m = threshold.get("m")
    signers = threshold.get("signers") or []
    if not isinstance(signers, list):
        signers = []
    owners = [s.lower() for s in signers if isinstance(s, str) and s.startswith("0x") and len(s) == 42]
    safe_address = None
    if safe_address_lookup:
        if function_signature and function_signature in safe_address_lookup:
            safe_address = safe_address_lookup[function_signature]
        elif "default" in safe_address_lookup:
            safe_address = safe_address_lookup["default"]
    if not safe_address:
        safe_address = "0x" + "0" * 40
    return [
        {
            "address": safe_address.lower(),
            "resolved_type": "safe",
            "origin": "semantic_capability:threshold_group",
            "principal_type": "controller",
            "details": _details_with_conditions(
                {
                    "threshold": int(m) if isinstance(m, int) else None,
                    "owners": owners,
                    "total_signers": len(owners),
                    "source": "semantic_predicate_capability_resolver",
                },
                conditions,
            ),
        }
    ]


def _rows_for_signature_witness(cap_dict: dict[str, Any], conditions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    signer = cap_dict.get("signer")
    if not isinstance(signer, dict) or signer.get("kind") != "finite_set":
        return []
    signer_conditions = _condition_dicts(signer.get("conditions"))
    rows: list[dict[str, Any]] = []
    for member in signer.get("members") or []:
        if not isinstance(member, str) or not member.startswith("0x") or len(member) != 42:
            continue
        rows.append(
            {
                "address": member.lower(),
                "resolved_type": None,
                "origin": "semantic_capability:signature_witness",
                "principal_type": "signature_witness",
                "details": _details_with_conditions(
                    {
                        "signer_kind": "finite_set",
                        "source": "semantic_predicate_capability_resolver",
                    },
                    conditions + signer_conditions,
                ),
            }
        )
    return rows


def _row_with_conditions(row: dict[str, Any], conditions: list[dict[str, Any]]) -> dict[str, Any]:
    out = dict(row)
    details = dict(out.get("details") or {})
    out["details"] = _details_with_conditions(details, conditions)
    return out


def _details_with_conditions(details: dict[str, Any], conditions: list[dict[str, Any]]) -> dict[str, Any]:
    if conditions:
        existing = _condition_dicts(details.get("conditions"))
        details["conditions"] = _unique_conditions(existing + conditions)
    return details


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("address") or "").lower(),
            str(row.get("origin") or ""),
            str(row.get("principal_type") or ""),
        )
        if key in by_key:
            existing = by_key[key]
            existing_details = dict(existing.get("details") or {})
            row_details = row.get("details") if isinstance(row.get("details"), dict) else {}
            if isinstance(row_details, dict):
                existing_details["conditions"] = _unique_conditions(
                    _condition_dicts(existing_details.get("conditions"))
                    + _condition_dicts(row_details.get("conditions"))
                )
                trace = list(existing_details.get("trace") or [])
                trace.extend(item for item in row_details.get("trace") or [] if item not in trace)
                if trace:
                    existing_details["trace"] = trace
            existing["details"] = existing_details
            continue
        copied = dict(row)
        copied["details"] = dict(copied.get("details") or {})
        by_key[key] = copied
        out.append(copied)
    return out


def _condition_dicts(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    return [{key: value for key, value in item.items() if value is not None} for item in raw if isinstance(item, dict)]


def _unique_conditions(conditions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for condition in conditions:
        key = repr(sorted(condition.items()))
        if key in seen:
            continue
        seen.add(key)
        out.append(dict(condition))
    return out


def _child_dicts(cap_dict: dict[str, Any]) -> list[dict[str, Any]]:
    children = cap_dict.get("children")
    if not isinstance(children, list):
        return []
    return [child for child in children if isinstance(child, dict)]
