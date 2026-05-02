"""Translate a v2 ``predicate_trees`` artifact into the v1
``semantic_guards`` shape so policy_worker (and any other v1
consumer) can adopt v2 without rewriting its data model.

This is the bridge that unblocks #17's first deletion checkpoint
("policy_worker switches from semantic_guards to predicate_trees").
With this shim:

  - The v1 effective_permissions logic keeps reading semantic_guards
    unchanged.
  - The static stage can EITHER emit native semantic_guards
    (legacy path) OR derive synthetic semantic_guards from
    predicate_trees (v2 path).
  - CI can diff the two for equivalence on the corpus.

Mapping per v2 leaf::

    v2 leaf shape                         → v1 predicate kind
    ──────────────────────────────────────────────────────────
    kind=membership, authority_role=
      caller_authority                    → mapping_membership
    kind=equality, op=eq,
      authority_role=caller_authority,
      one operand state_variable          → caller_equals_controller
    kind=signature_auth (ecrecover)       → policy_check
    kind=external_bool,
      authority_role=delegated_authority  → external_helper
    kind=unsupported                      → status: unresolved
    authority_role=reentrancy / pause /
      time / business                     → no v1 predicate
                                            (drop; v1 didn't model
                                            these as auth-shaped
                                            anyway, just notes them)

Function-level status:

  - tree is None / function unguarded        → status: public
  - all leaves mapped to a v1 predicate kind → status: resolved
  - any leaf is unsupported / dropped        → status: partial
  - no leaves mapped                         → status: unresolved
"""

from __future__ import annotations

from typing import Any


def synthesize_semantic_guards_from_predicate_trees(
    predicate_trees: dict[str, Any],
    *,
    contract_address: str,
    contract_name: str,
) -> dict[str, Any]:
    """Build a v1-shaped ``semantic_guards`` dict from a v2
    ``predicate_trees`` artifact.

    Args:
      predicate_trees: the v2 artifact dict (``{"trees": {fn: tree, ...}, ...}``).
      contract_address: contract address (echoed into the v1 wrapper).
      contract_name: contract name (echoed into the v1 wrapper).

    Returns the v1 ``semantic_guards`` dict shape — drop-in for
    consumers that read ``semantic_guards.json`` artifact today.
    """
    trees = predicate_trees.get("trees") or {}
    functions: list[dict[str, Any]] = []
    for fn_signature, tree in sorted(trees.items()):
        functions.append(_synthesize_function_entry(fn_signature, tree))
    return {
        "schema_version": "0.1",
        "contract_address": contract_address,
        "contract_name": contract_name,
        "functions": functions,
        # Marker so consumers know this is v2-derived, not the
        # native v1 emit. Lets a CI diff job filter accordingly.
        "_synthetic_from": "v2_predicate_trees",
    }


def _synthesize_function_entry(fn_signature: str, tree: dict[str, Any]) -> dict[str, Any]:
    leaves = list(_walk_leaves(tree))
    predicates: list[dict[str, Any]] = []
    notes: list[str] = []
    unsupported_count = 0
    dropped_count = 0

    for leaf in leaves:
        result = _leaf_to_v1_predicate(leaf)
        if result is None:
            # Side-condition leaf (reentrancy / pause / time /
            # business) — v1 didn't model these as auth predicates.
            dropped_count += 1
            continue
        if result.get("_unsupported"):
            unsupported_count += 1
            reason = result.get("reason") or "unsupported_v2_leaf"
            notes.append(f"v2 leaf with reason={reason} could not be mapped")
            continue
        # Strip internal markers before adding.
        result.pop("_unsupported", None)
        result.pop("reason", None)
        predicates.append(result)

    if not leaves:
        status = "public"
    elif predicates and unsupported_count == 0:
        status = "resolved"
    elif predicates and unsupported_count > 0:
        status = "partial"
    elif unsupported_count > 0:
        status = "unresolved"
    elif dropped_count > 0 and not predicates:
        # All side-condition (e.g. reentrancy + pause) — auth still
        # missing per v1's lens.
        status = "unresolved"
        notes.append("All v2 leaves were side-conditions (reentrancy / pause / time / business)")
    else:
        status = "unresolved"

    return {
        "function": fn_signature,
        "status": status,
        "predicates": predicates,
        "guard_ids": [],  # v2 doesn't carry the v1 graph IDs
        "guard_kinds": _derive_guard_kinds(predicates),
        "controller_refs": [],
        "notes": notes,
    }


def _leaf_to_v1_predicate(leaf: dict[str, Any]) -> dict[str, Any] | None:
    """Map a single v2 leaf to a v1 predicate dict, or to None
    when the leaf doesn't translate cleanly.

    Return values:
      - dict (no ``_unsupported`` flag) — successfully mapped.
      - dict with ``_unsupported`` = True — leaf was kind=unsupported;
        mark the function partial/unresolved.
      - None — leaf is a side-condition that v1 didn't model as
        an auth predicate (caller drops it).
    """
    kind = leaf.get("kind")
    role = leaf.get("authority_role")
    operator = leaf.get("operator")

    if kind == "unsupported":
        return {"_unsupported": True, "reason": leaf.get("unsupported_reason")}

    if role in ("reentrancy", "pause", "time", "business"):
        # v1 didn't include these in semantic_guards.predicates.
        return None

    operands = leaf.get("operands", []) or []

    if kind == "membership" and role == "caller_authority":
        # mapping_membership: pick the non-caller key as the
        # controller. NOTE: v1's heuristic is name-driven and emits
        # role_member vs mapping_membership inconsistently across
        # AC patterns — we standardize on mapping_membership here
        # since downstream effective_permissions handles BOTH kinds
        # (role_member via role_source path, mapping_membership via
        # controller_source path) and the v2 leaf doesn't carry the
        # contract-name-based heuristic v1 used to discriminate. The
        # equivalence comparator in tests treats role_member and
        # mapping_membership as semantically equivalent for cutover
        # purposes.
        keys = (leaf.get("set_descriptor") or {}).get("key_sources") or operands
        controller_op = next(
            (
                k
                for k in keys
                if k.get("source") not in ("msg_sender", "tx_origin", "signature_recovery")
            ),
            None,
        )
        return {
            "kind": "mapping_membership",
            "controller_kind": _operand_to_controller_kind(controller_op),
            "controller_label": _operand_label(controller_op),
            "controller_source": _operand_source_id(controller_op),
            "read_spec": None,
        }

    if kind == "equality" and operator == "eq" and role == "caller_authority":
        # caller_equals_controller: the non-caller operand is the
        # controller.
        non_caller = [
            o
            for o in operands
            if o.get("source") not in ("msg_sender", "tx_origin", "signature_recovery")
        ]
        controller_op = non_caller[0] if non_caller else None
        return {
            "kind": "caller_equals_controller",
            "controller_kind": _operand_to_controller_kind(controller_op),
            "controller_label": _operand_label(controller_op),
            "controller_source": _operand_source_id(controller_op),
            "read_spec": None,
        }

    if kind == "signature_auth":
        return {
            "kind": "policy_check",
            "authority_source": [],
            "helper": None,
            "status": "resolved",
        }

    if kind == "external_bool" and role == "delegated_authority":
        return {
            "kind": "external_helper",
            "authority_source": [],
            "helper": leaf.get("expression") or None,
            "status": "unresolved",
        }

    # Fall-through: leaf had a kind/role we don't have a v1 mapping
    # for. Treat as unsupported so the function shows partial.
    return {"_unsupported": True, "reason": f"unmapped_kind:{kind}/role:{role}"}


def _operand_to_controller_kind(operand: dict[str, Any] | None) -> str:
    if not operand:
        return "unknown"
    src = operand.get("source")
    if src == "state_variable":
        return "state_variable"
    if src == "view_call":
        return "view_call"
    if src == "parameter":
        return "parameter"
    if src == "constant":
        return "constant"
    return src or "unknown"


def _operand_label(operand: dict[str, Any] | None) -> str | None:
    if not operand:
        return None
    return (
        operand.get("state_variable_name")
        or operand.get("parameter_name")
        or operand.get("callee")
        or operand.get("constant_value")
    )


def _operand_source_id(operand: dict[str, Any] | None) -> str | None:
    """Stable identifier for the controller — what v1 calls
    ``controller_source``. Best-effort: state-var name, parameter
    name, callee name, or hex constant."""
    return _operand_label(operand)


def _derive_guard_kinds(predicates: list[dict[str, Any]]) -> list[str]:
    """v1's ``guard_kinds`` field is a list of human-readable
    labels (access_control / pause / roles / ...). Synthesize from
    the predicate kinds — minimal, since the v2 path doesn't
    propagate v1's loose label set."""
    kinds: list[str] = []
    for p in predicates:
        if p.get("kind") in ("caller_equals_controller", "mapping_membership", "role_member"):
            if "access_control" not in kinds:
                kinds.append("access_control")
        elif p.get("kind") == "policy_check":
            if "policy" not in kinds:
                kinds.append("policy")
        elif p.get("kind") == "external_helper":
            if "external_helper" not in kinds:
                kinds.append("external_helper")
    return kinds


def _walk_leaves(tree: dict[str, Any] | None):
    if tree is None:
        return
    if tree.get("op") == "LEAF":
        leaf = tree.get("leaf")
        if leaf is not None:
            yield leaf
        return
    for child in tree.get("children") or []:
        yield from _walk_leaves(child)


__all__ = ["synthesize_semantic_guards_from_predicate_trees"]
