"""Row writer for semantic per-function capabilities.

This module drives ``EffectiveFunction`` and ``FunctionPrincipal`` rows
directly from per-function ``CapabilityExpr`` shapes.

Per-kind row representation:

  finite_set                    -> N rows, principal_type=controller
  threshold_group (Safe)        -> 1 row,  resolved_type=safe, details.owners[]
  signature_witness(finite)     -> N rows, principal_type=signature_witness
  signature_witness(non-finite) -> 0 rows
  cofinite_blacklist            -> 0 rows
  external_check_only           -> 0 rows
  conditional_universal         -> 0 rows + status='public', authority_public=True
  unsupported                   -> 0 rows + status='unsupported'
  OR with resolved caller/public paths -> resolved path rows/public marker
  AND with caller path + side conditions -> caller rows with conditions
  AND/OR irreducible residuals -> 0 rows + capability_expr=full tree

Caller-shaped kinds (``finite_set``, ``threshold_group``,
``signature_witness(finite)``) are the only leaf kinds that produce
``FunctionPrincipal`` rows, either directly or through a composite path.
``FunctionPrincipal.address`` semantically means "this address can call
as itself"; putting blacklists, registry contracts, or external-check
targets there is a category error that produces false-authority claims
downstream
(``ProtocolSurface.jsx:303``, ``protocolScore.js:124``).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import is_dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import EffectiveFunction, FunctionPrincipal
from services.policy.capability_surface import project_capability_surface
from services.resolution.capabilities import CapabilityExpr
from services.resolution.capability_resolver import capability_to_dict


def _to_dict(cap: CapabilityExpr | dict[str, Any] | None) -> dict[str, Any] | None:
    """Normalize ``cap`` to its serialized dict form.

    Accepts either a real ``CapabilityExpr`` (e.g. from the resolver) or
    an already-serialized dict (e.g. handed back through a fixture or
    persisted artifact). ``None`` propagates."""
    if cap is None:
        return None
    if is_dataclass(cap):
        return capability_to_dict(cap)  # type: ignore[arg-type]
    if isinstance(cap, dict):
        return dict(cap)
    return None


def _kind_of(cap_dict: dict[str, Any] | None) -> str | None:
    if not isinstance(cap_dict, dict):
        return None
    kind = cap_dict.get("kind")
    return str(kind) if isinstance(kind, str) else None


def _principal_rows_for_capability(
    cap_dict: dict[str, Any],
    *,
    safe_address_lookup: dict[str, str] | None = None,
    function_signature: str | None = None,
) -> list[dict[str, Any]]:
    """Translate a serialized CapabilityExpr to the principal-row tuples
    that should be written for the function.

    Returns a list of dicts with keys ``address``, ``resolved_type``,
    ``origin``, ``principal_type``, ``details``. Caller persists them.
    """
    return project_capability_surface(
        cap_dict,
        safe_address_lookup=safe_address_lookup,
        function_signature=function_signature,
    ).principal_rows


def _column_values_for_capability(
    cap_dict: dict[str, Any],
) -> dict[str, Any]:
    """Compute ``EffectiveFunction`` column overrides from the resolved
    capability. Always populates ``capability_expr``; ``conditions`` /
    ``status`` / ``authority_public`` only set for the kinds that
    require them."""
    surface = project_capability_surface(cap_dict)
    kind = _kind_of(cap_dict)
    conditions = surface.conditions
    out: dict[str, Any] = {
        "capability_expr": dict(cap_dict),
        "conditions": conditions or None,
        "status": "public" if surface.authority_public else None,
        "authority_public": surface.authority_public,
    }
    if kind == "unsupported" and not surface.authority_public and not surface.principal_rows:
        out["status"] = "unsupported"
    return out


def write_effective_function_rows(
    session: Session,
    *,
    contract_id: int,
    function_records: list[dict[str, Any]],
    capability_by_function: Mapping[str, CapabilityExpr | dict[str, Any]] | None,
    safe_address_lookup: dict[str, str] | None = None,
) -> int:
    """Replace this contract's ``EffectiveFunction`` rows with semantic
    rows and their associated ``FunctionPrincipal`` rows.

    ``function_records`` is the list of per-function dicts emitted by
    ``build_effective_permissions``. Each must carry at minimum
    ``function`` / ``abi_signature`` and the column overrides
    (``capability_expr``, ``conditions``, ``status``,
    ``authority_public``); optional compatibility fields (``effect_labels``,
    ``effect_targets``, ``action_summary``, ``authority_roles``) ride
    through unchanged.

    ``capability_by_function`` maps function full-name to the resolved
    capability (dict or dataclass). When None / missing for a
    particular function, no principal rows are written for that function.

    Returns the number of FunctionPrincipal rows added.
    """
    capability_by_function = capability_by_function or {}

    # Replace the contract's effective_functions wholesale (matches the
    # pre-B.1 worker behavior; FunctionPrincipal rows cascade-delete via
    # the relationship's cascade="all, delete-orphan").
    session.query(EffectiveFunction).filter(EffectiveFunction.contract_id == contract_id).delete()
    session.flush()

    added_principals = 0
    for fn in function_records:
        fn_signature = str(fn.get("function") or fn.get("abi_signature") or "")
        function_name = fn_signature.split("(")[0] if "(" in fn_signature else fn_signature

        cap = capability_by_function.get(fn_signature)
        cap_dict = _to_dict(cap)

        # Column values: prefer resolved capability columns; otherwise use
        # explicit per-function compatibility fields.
        if cap_dict is not None:
            cap_columns = _column_values_for_capability(cap_dict)
        else:
            cap_columns = {
                "capability_expr": fn.get("capability_expr"),
                "conditions": fn.get("conditions"),
                "status": fn.get("status"),
                "authority_public": bool(fn.get("authority_public", False)),
            }
        # Per-function explicit override applies when the capability
        # itself didn't pin the column. ``conditional_universal``
        # should keep ``authority_public=True`` even if the per-function dict
        # carries the default ``False``.
        if cap_dict is None and "authority_public" in fn and fn.get("authority_public") is not None:
            cap_columns["authority_public"] = bool(fn["authority_public"])
        elif cap_dict is not None and bool(fn.get("authority_public", False)) and not cap_columns["authority_public"]:
            # Per-function explicit True (e.g. policy_check public capability)
            # ORs in even when the cap shape doesn't say public.
            cap_columns["authority_public"] = True
        if cap_dict is None:
            if fn.get("status") is not None:
                cap_columns["status"] = fn["status"]
            if fn.get("conditions") is not None:
                cap_columns["conditions"] = fn["conditions"]
            if fn.get("capability_expr") is not None:
                cap_columns["capability_expr"] = fn["capability_expr"]

        ef_kwargs: dict[str, Any] = {
            "contract_id": contract_id,
            "function_name": function_name,
            "selector": fn.get("selector"),
            "abi_signature": fn_signature,
            "effect_labels": fn.get("effect_labels", []),
            "effect_targets": fn.get("effect_targets", []),
            "action_summary": fn.get("action_summary"),
            "authority_public": cap_columns["authority_public"],
            "authority_roles": fn.get("authority_roles"),
        }
        # Optional columns may be absent in older test metadata.
        for col_name in ("capability_expr", "conditions", "status"):
            if hasattr(EffectiveFunction, col_name):
                ef_kwargs[col_name] = cap_columns.get(col_name)
        ef = EffectiveFunction(**ef_kwargs)
        session.add(ef)
        session.flush()

        # Semantic caller-shaped principals. ``ON CONFLICT DO NOTHING`` is
        # implemented at the (function_id, address, origin, principal_type)
        # level via an in-memory dedup set — the row schema has no UNIQUE
        # constraint so we can't lean on Postgres for it.
        seen: set[tuple[int, str, str, str]] = set()

        if cap_dict is not None:
            semantic_rows = _principal_rows_for_capability(
                cap_dict,
                safe_address_lookup=safe_address_lookup,
                function_signature=fn_signature,
            )
            for row in semantic_rows:
                key = (
                    ef.id,
                    row["address"],
                    row.get("origin") or "",
                    row.get("principal_type") or "",
                )
                if key in seen:
                    continue
                seen.add(key)
                session.add(
                    FunctionPrincipal(
                        function_id=ef.id,
                        address=row["address"],
                        resolved_type=row.get("resolved_type"),
                        origin=row.get("origin"),
                        principal_type=row.get("principal_type"),
                        details=row.get("details"),
                    )
                )
                added_principals += 1

    return added_principals


def function_principal_count(session: Session, function_id: int) -> int:
    """Test helper: number of ``FunctionPrincipal`` rows for a function."""
    return int(
        session.execute(select(FunctionPrincipal).where(FunctionPrincipal.function_id == function_id)).all().__len__()
    )
