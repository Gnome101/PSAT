"""Deterministic controller tracking metadata for event-first, polling-backed monitoring.

Schema-v2 cutover (Wave 3 Track 2 — A.1):
    ``build_controller_tracking`` and ``build_policy_tracking`` source their
    inputs from the v2 ``predicate_trees`` and ``effects`` artifacts
    instead of the v1 ``permission_graph``. The walker enumerates
    every state-variable operand referenced by any predicate-tree leaf
    (catching inherited Ownable's ``_owner`` and other shapes the v1
    heuristic missed), then pulls writers from ``effects.functions``
    state_write sinks.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterable, cast

from eth_utils.crypto import keccak

from schemas.contract_analysis import (
    AccessControlAnalysis,
    AssociatedEvent,
    AssociatedEventInput,
    ControllerReadSpec,
    ControllerTrackingTarget,
    ControllerWriterFunction,
    Evidence,
    PolicyTrackingTarget,
)

from .shared import (
    _contract_functions,
    _declaring_contract_name,
    _source_evidence,
)


def _unit_key(unit) -> str:
    return (
        getattr(unit, "canonical_name", None)
        or getattr(unit, "full_name", None)
        or getattr(unit, "name", str(id(unit)))
    )


def _abi_type(type_obj) -> str:
    if type_obj is None:
        return "unknown"

    type_name = type(type_obj).__name__
    if type_name == "ElementaryType":
        return str(type_obj)
    if type_name == "UserDefinedType":
        underlying = getattr(type_obj, "type", None)
        underlying_name = type(underlying).__name__
        if underlying_name == "Contract":
            return "address"
        if underlying_name == "Enum":
            return "uint8"
        return str(type_obj)
    if type_name == "ArrayType":
        base = _abi_type(getattr(type_obj, "type", None))
        length = getattr(type_obj, "length", None)
        if length is None:
            return f"{base}[]"
        return f"{base}[{length}]"
    return str(type_obj)


def _event_signature(event_decl) -> str:
    arg_types = [_abi_type(getattr(elem, "type", None)) for elem in getattr(event_decl, "elems", [])]
    return f"{event_decl.name}({','.join(arg_types)})"


def _event_inputs(event_decl) -> list[AssociatedEventInput]:
    return [
        {
            "name": getattr(elem, "name", "") or "",
            "type": _abi_type(getattr(elem, "type", None)),
            "indexed": bool(getattr(elem, "indexed", False)),
        }
        for elem in getattr(event_decl, "elems", [])
    ]


def _event_reference(event_decl) -> AssociatedEvent:
    signature = _event_signature(event_decl)
    return {
        "name": event_decl.name,
        "signature": signature,
        "topic0": "0x" + keccak(text=signature).hex(),
        "inputs": _event_inputs(event_decl),
    }


def _event_index(contract) -> dict[str, list]:
    by_name: dict[str, list] = {}
    for current in [contract, *getattr(contract, "inheritance", [])]:
        events = getattr(current, "events", []) or getattr(current, "events_declared", [])
        for event_decl in events:
            by_name.setdefault(event_decl.name, []).append(event_decl)
    return by_name


def _ir_argument_types(arguments: Iterable) -> list[str]:
    return [_abi_type(getattr(argument, "type", None)) for argument in arguments]


def _resolve_event_refs(event_name: str, arguments: list, event_index: dict[str, list]) -> list[AssociatedEvent]:
    emitted_types = _ir_argument_types(arguments)
    matches = [
        event_decl
        for event_decl in event_index.get(event_name, [])
        if [_abi_type(getattr(elem, "type", None)) for elem in getattr(event_decl, "elems", [])] == emitted_types
    ]
    if not matches:
        matches = [
            event_decl
            for event_decl in event_index.get(event_name, [])
            if len(getattr(event_decl, "elems", []) or []) == len(arguments)
        ] or event_index.get(event_name, [])
    deduped: dict[str, AssociatedEvent] = {}
    for event_decl in matches:
        event_ref = _event_reference(event_decl)
        deduped[event_ref["signature"]] = event_ref
    return sorted(deduped.values(), key=lambda item: item["signature"])


def _collect_events(
    unit, project_dir: Path, event_index: dict[str, list], seen: set[str]
) -> list[tuple[AssociatedEvent, Evidence]]:
    key = _unit_key(unit)
    if key in seen:
        return []
    seen.add(key)

    events: list[tuple[AssociatedEvent, Evidence]] = []
    for node in getattr(unit, "nodes", []):
        for ir in getattr(node, "irs", []) or []:
            if type(ir).__name__ == "EventCall":
                event_name = getattr(ir, "name", None) or ""
                if event_name:
                    event_refs = _resolve_event_refs(event_name, list(getattr(ir, "arguments", []) or []), event_index)
                    evidence = _source_evidence(node, project_dir, detail=f"emit {event_name}")
                    events.extend((event_ref, evidence) for event_ref in event_refs)
            if type(ir).__name__ != "InternalCall":
                continue
            callee = getattr(ir, "function", None)
            if callee is None:
                continue
            events.extend(_collect_events(callee, project_dir, event_index, seen))
    return events


def _dedupe_event_refs(events: list[tuple[AssociatedEvent, Evidence]]) -> list[AssociatedEvent]:
    deduped: dict[str, AssociatedEvent] = {}
    for event_ref, _ in events:
        deduped[event_ref["signature"]] = event_ref
    return sorted(deduped.values(), key=lambda item: item["signature"])


def _functions_by_signature(contract) -> dict[str, object]:
    return {
        getattr(function, "full_name", getattr(function, "name", "")): function
        for function in _contract_functions(contract)
    }


# ---------------------------------------------------------------------------
# v2 source extraction: walk predicate_trees / effects for raw signals.
# ---------------------------------------------------------------------------


def _walk_leaves(node: Any, callback) -> None:
    """Walk every LEAF descendant of ``node`` and invoke ``callback(leaf)``."""
    if not isinstance(node, dict):
        return
    if node.get("op") == "LEAF":
        leaf = node.get("leaf")
        if leaf is not None:
            callback(leaf)
        return
    for child in node.get("children") or []:
        _walk_leaves(child, callback)


def _collect_external_call_role_callees(predicate_trees: Mapping[str, Any] | None) -> set[str]:
    """External-call role keys from OZ-style mapping descriptors."""
    if not isinstance(predicate_trees, dict):
        return set()
    trees = predicate_trees.get("trees")
    if not isinstance(trees, dict):
        return set()

    oz_role_key_callees: set[str] = set()

    def visit(leaf: dict[str, Any]) -> None:
        descriptor = leaf.get("set_descriptor") or {}
        if not isinstance(descriptor, dict):
            return
        is_oz_role = descriptor.get("kind") == "mapping_membership" and any(
            isinstance(h, dict) and h.get("topic0") == _OZ_ROLE_GRANTED_TOPIC0
            for h in descriptor.get("enumeration_hint") or []
        )
        for key_source in descriptor.get("key_sources") or []:
            if isinstance(key_source, dict) and key_source.get("source") == "external_call":
                callee = key_source.get("callee")
                if is_oz_role and isinstance(callee, str) and callee:
                    oz_role_key_callees.add(callee)

    for tree in trees.values():
        _walk_leaves(tree, visit)
    return oz_role_key_callees


def _collect_state_var_operands(predicate_trees: Mapping[str, Any] | None) -> set[str]:
    """Every state_variable operand surfaced by any leaf — direct
    operands AND set_descriptor.storage_var / key_sources AND
    authority_contract.address_source. Catches inherited Ownable's
    ``_owner`` and similar shapes the v1 permission-graph heuristic
    missed."""
    if not isinstance(predicate_trees, dict):
        return set()
    trees = predicate_trees.get("trees")
    if not isinstance(trees, dict):
        return set()

    state_vars: set[str] = set()

    def visit(leaf: dict[str, Any]) -> None:
        for operand in leaf.get("operands") or []:
            if isinstance(operand, dict) and operand.get("source") == "state_variable":
                name = operand.get("state_variable_name")
                if isinstance(name, str) and name:
                    state_vars.add(name)
        descriptor = leaf.get("set_descriptor") or {}
        if isinstance(descriptor, dict):
            storage_var = descriptor.get("storage_var")
            if isinstance(storage_var, str) and storage_var:
                state_vars.add(storage_var)
            authority = descriptor.get("authority_contract") or {}
            if isinstance(authority, dict):
                address_source = authority.get("address_source") or {}
                if isinstance(address_source, dict) and address_source.get("source") == "state_variable":
                    sv = address_source.get("state_variable_name")
                    if isinstance(sv, str) and sv:
                        state_vars.add(sv)
            for key_source in descriptor.get("key_sources") or []:
                if isinstance(key_source, dict) and key_source.get("source") == "state_variable":
                    sv = key_source.get("state_variable_name")
                    if isinstance(sv, str) and sv:
                        state_vars.add(sv)

    for tree in trees.values():
        _walk_leaves(tree, visit)
    return state_vars


_OZ_ROLE_GRANTED_TOPIC0 = "0x2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d"


def _collect_role_key_state_vars(predicate_trees: Mapping[str, Any] | None) -> set[str]:
    """State-variable names that appear as a ``key_source`` of a
    ``mapping_membership`` set_descriptor whose enumeration_hint
    matches the OZ ``RoleGranted`` topic. These are role-identifier
    bytes32 constants used in cross-contract ``hasRole(role, account)``
    checks; v1 flagged them as role_identifier through the graph's
    role-argument detection."""
    if not isinstance(predicate_trees, dict):
        return set()
    trees = predicate_trees.get("trees")
    if not isinstance(trees, dict):
        return set()

    role_vars: set[str] = set()

    def visit(leaf: dict[str, Any]) -> None:
        descriptor = leaf.get("set_descriptor") or {}
        if not isinstance(descriptor, dict) or descriptor.get("kind") != "mapping_membership":
            return
        hints = descriptor.get("enumeration_hint") or []
        is_oz_role = any(isinstance(h, dict) and h.get("topic0") == _OZ_ROLE_GRANTED_TOPIC0 for h in hints)
        if not is_oz_role:
            return
        for key_source in descriptor.get("key_sources") or []:
            if isinstance(key_source, dict) and key_source.get("source") == "state_variable":
                name = key_source.get("state_variable_name")
                if isinstance(name, str) and name:
                    role_vars.add(name)

    for tree in trees.values():
        _walk_leaves(tree, visit)
    return role_vars


def _collect_authority_state_vars(predicate_trees: Mapping[str, Any] | None) -> set[str]:
    """State-variable names appearing as
    ``set_descriptor.authority_contract.address_source.state_variable_name``.
    These are external authority registries (e.g. ``roleRegistry``);
    promote them from ``state_variable`` to ``external_contract`` kind so
    downstream resolution treats them as cross-contract delegates."""
    if not isinstance(predicate_trees, dict):
        return set()
    trees = predicate_trees.get("trees")
    if not isinstance(trees, dict):
        return set()

    authority_vars: set[str] = set()

    def visit(leaf: dict[str, Any]) -> None:
        descriptor = leaf.get("set_descriptor") or {}
        if not isinstance(descriptor, dict):
            return
        authority = descriptor.get("authority_contract") or {}
        if not isinstance(authority, dict):
            return
        address_source = authority.get("address_source") or {}
        if isinstance(address_source, dict) and address_source.get("source") == "state_variable":
            sv = address_source.get("state_variable_name")
            if isinstance(sv, str) and sv:
                authority_vars.add(sv)

    for tree in trees.values():
        _walk_leaves(tree, visit)
    return authority_vars


def _collect_external_contract_state_vars_from_effects(
    effects: Mapping[str, Any] | None,
    state_var_names: set[str],
) -> set[str]:
    """State-variable names invoked as external-call destinations
    (``authority.canCall(...)``, ``hook.beforeTransfer(...)``). Sourced
    from ``effects.functions[*].sinks`` filtered to ``external_call``;
    we keep only sinks whose target's leading prefix matches an actual
    state-var name (so ``msg.sender.transfer`` etc. don't false-positive)."""
    if not isinstance(effects, dict):
        return set()
    out: set[str] = set()
    for info in (effects.get("functions") or {}).values():
        if not isinstance(info, dict):
            continue
        for sink in info.get("sinks") or []:
            if not isinstance(sink, dict):
                continue
            if sink.get("kind") != "external_call":
                continue
            target = sink.get("target")
            if not isinstance(target, str) or "." not in target:
                continue
            prefix = target.split(".", 1)[0]
            if prefix in state_var_names:
                out.add(prefix)
    return out


def _state_writers_from_effects(
    effects: Mapping[str, Any] | None,
) -> dict[str, set[str]]:
    """Map state-variable name → set of function signatures that write it,
    sourced from ``effects.functions[*].sinks`` filtered to
    ``kind == "state_write"``. Excludes constructors."""
    if not isinstance(effects, dict):
        return {}
    by_var: dict[str, set[str]] = {}
    for fn_sig, info in (effects.get("functions") or {}).items():
        if not isinstance(fn_sig, str) or fn_sig.startswith("constructor("):
            continue
        if not isinstance(info, dict):
            continue
        for sink in info.get("sinks") or []:
            if not isinstance(sink, dict):
                continue
            if sink.get("kind") != "state_write":
                continue
            target = sink.get("target")
            if isinstance(target, str) and target:
                by_var.setdefault(target, set()).add(fn_sig)
    return by_var


def _writer_records_from_effects(
    contract,
    project_dir: Path,
    target_state_vars: Iterable[str],
    event_lookup: dict[str, list],
    effects: Mapping[str, Any] | None,
) -> tuple[list[ControllerWriterFunction], list[AssociatedEvent]]:
    """Build ``ControllerWriterFunction`` records for the given state-variable
    targets, using ``effects`` as the writer-discovery source."""
    target_set = {var for var in target_state_vars if var}
    if not target_set:
        return [], []
    writers_by_var = _state_writers_from_effects(effects)
    # Invert: function-signature → set of vars it writes that we care about.
    writes_by_signature: dict[str, set[str]] = {}
    for var in target_set:
        for signature in writers_by_var.get(var, set()):
            writes_by_signature.setdefault(signature, set()).add(var)

    functions_by_signature = _functions_by_signature(contract)
    writer_functions: list[ControllerWriterFunction] = []
    aggregated_events: dict[str, AssociatedEvent] = {}
    for signature in sorted(writes_by_signature):
        function = functions_by_signature.get(signature)
        if function is None:
            continue
        writes = sorted(writes_by_signature[signature])
        event_records = _collect_events(function, project_dir, event_lookup, set())
        event_refs = _dedupe_event_refs(event_records)
        for event_ref in event_refs:
            aggregated_events[event_ref["signature"]] = event_ref
        writer_functions.append(
            {
                "contract": _declaring_contract_name(function, contract.name),
                "function": signature,
                "visibility": getattr(function, "visibility", "unknown"),
                "writes": writes,
                "associated_events": event_refs,
                "evidence": [
                    _source_evidence(
                        function,
                        project_dir,
                        detail=f"writes tracked state {', '.join(writes)}",
                    )
                ],
            }
        )

    associated_events = sorted(aggregated_events.values(), key=lambda item: item["signature"])
    return writer_functions, associated_events


# ---------------------------------------------------------------------------
# Helpers for read_spec resolution. Public state-vars get an auto-generated
# getter named after the var; private vars need an explicit one. We pick
# the getter via a small set of conventions the static pipeline can spot
# without RPC: a same-contract view function returning the var.
# ---------------------------------------------------------------------------


def _build_getter_index(contract) -> dict[str, str]:
    """Map private state-var name → its public getter function name.

    Walks the subject's view/pure functions whose body is
    ``return <state_var>;``. OZ Ownable's ``owner()`` returning ``_owner``
    is the canonical case."""
    out: dict[str, str] = {}
    for fn in getattr(contract, "functions", []) or []:
        visibility = getattr(fn, "visibility", None)
        if visibility not in ("public", "external"):
            continue
        if getattr(fn, "view", False) is False and getattr(fn, "pure", False) is False:
            continue
        if getattr(fn, "parameters", None):
            continue
        return_vars = list(getattr(fn, "returns", []) or [])
        if not return_vars:
            continue
        for node in getattr(fn, "nodes", []) or []:
            expr = getattr(node, "expression", None)
            text = str(expr) if expr is not None else ""
            if not text:
                continue
            for sv in getattr(contract, "state_variables_ordered", []) or []:
                if sv.name and text.strip() == sv.name:
                    out.setdefault(sv.name, fn.name)
                    break
    return out


def _state_var_read_spec(
    name: str,
    state_vars_by_name: dict[str, Any],
    getter_by_var: dict[str, str],
) -> ControllerReadSpec:
    sv = state_vars_by_name.get(name)
    type_str = str(getattr(sv, "type", "")) if sv is not None else ""
    is_public = bool(getattr(sv, "visibility", None) == "public") if sv is not None else False
    getter_name = name if is_public else getter_by_var.get(name, name)
    spec: ControllerReadSpec = cast(
        ControllerReadSpec,
        {
            "strategy": "getter_call",
            "target": getter_name,
            "kind": "state_variable",
            "state_variable_name": name,
            "type": type_str,
        },
    )
    return spec


def _role_authority_for_external_callee(callee_name: str, predicate_trees: Mapping[str, Any] | None) -> str | None:
    """Like ``_role_authority_for`` but matches the role identifier
    against external-call callee names instead of state-variable
    operands. Used for cross-contract role getters (``registry.ROLE()``).
    """
    if not isinstance(predicate_trees, dict):
        return None
    trees = predicate_trees.get("trees")
    if not isinstance(trees, dict):
        return None

    candidates: dict[str, set[str]] = {}

    def visit(leaf: dict[str, Any]) -> None:
        descriptor = leaf.get("set_descriptor") or {}
        if not isinstance(descriptor, dict):
            return
        authority = descriptor.get("authority_contract") or {}
        if not isinstance(authority, dict):
            return
        address_source = authority.get("address_source") or {}
        if not isinstance(address_source, dict) or address_source.get("source") != "state_variable":
            return
        registry = address_source.get("state_variable_name")
        if not isinstance(registry, str) or not registry:
            return
        is_oz_role = descriptor.get("kind") == "mapping_membership" and any(
            isinstance(h, dict) and h.get("topic0") == _OZ_ROLE_GRANTED_TOPIC0
            for h in descriptor.get("enumeration_hint") or []
        )
        for source in list(leaf.get("operands") or []) + list(descriptor.get("key_sources") or []):
            if isinstance(source, dict) and source.get("source") == "external_call":
                op_name = source.get("callee")
                if not isinstance(op_name, str):
                    continue
                if is_oz_role:
                    candidates.setdefault(op_name, set()).add(registry)

    for tree in trees.values():
        _walk_leaves(tree, visit)

    sources = candidates.get(callee_name)
    if not sources or len(sources) != 1:
        return None
    return next(iter(sources))


def _role_authority_for(role_name: str, predicate_trees: Mapping[str, Any] | None) -> str | None:
    """Look at predicate_trees for a leaf that pairs ``role_name`` with a
    state-variable-backed authority_contract address_source. Returns the
    state-variable name (the registry source). Mirrors v1 behavior on a
    per-role basis: only attaches a contract_source when exactly ONE
    registry pairs with the role across the tree."""
    if not isinstance(predicate_trees, dict):
        return None
    trees = predicate_trees.get("trees")
    if not isinstance(trees, dict):
        return None

    candidates: dict[str, set[str]] = {}

    def visit(leaf: dict[str, Any]) -> None:
        descriptor = leaf.get("set_descriptor") or {}
        if not isinstance(descriptor, dict):
            return
        authority = descriptor.get("authority_contract") or {}
        if not isinstance(authority, dict):
            return
        address_source = authority.get("address_source") or {}
        if not isinstance(address_source, dict) or address_source.get("source") != "state_variable":
            return
        registry = address_source.get("state_variable_name")
        if not isinstance(registry, str) or not registry:
            return
        is_oz_role = descriptor.get("kind") == "mapping_membership" and any(
            isinstance(h, dict) and h.get("topic0") == _OZ_ROLE_GRANTED_TOPIC0
            for h in descriptor.get("enumeration_hint") or []
        )
        if not is_oz_role:
            return
        for key_source in descriptor.get("key_sources") or []:
            if isinstance(key_source, dict) and key_source.get("source") == "state_variable":
                op_name = key_source.get("state_variable_name")
                if isinstance(op_name, str):
                    candidates.setdefault(op_name, set()).add(registry)

    for tree in trees.values():
        _walk_leaves(tree, visit)

    sources = candidates.get(role_name)
    if not sources or len(sources) != 1:
        return None
    return next(iter(sources))


# ---------------------------------------------------------------------------
# Top-level builders.
# ---------------------------------------------------------------------------


def build_controller_tracking(
    contract,
    project_dir: Path,
    predicate_trees: Mapping[str, Any] | None,
    effects: Mapping[str, Any] | None,
    access_control: AccessControlAnalysis | None = None,
) -> list[ControllerTrackingTarget]:
    """Build event-first tracking metadata from the v2 predicate-tree +
    effects artifacts.

    Inputs:
      * ``predicate_trees`` — v2 artifact from ``build_predicate_artifacts``.
        Walked for every state-variable operand referenced by a leaf
        (direct operand, set-descriptor storage_var/key_source, or
        authority_contract.address_source). Each unique name becomes a
        ``ControllerTrackingTarget``.
      * ``effects`` — v2 artifact from ``build_effects``. Filtered to
        ``state_write`` sinks per externally-callable function; supplies
        the writer functions for each state-variable target.
      * ``access_control`` — supplies role definitions for
        ``role_identifier`` targets found from predicate-tree role keys.

    The result is a SUPERSET of the v1 permission-graph + augment pass —
    by construction, since predicate_trees catches the inherited Ownable
    ``_owner`` shapes the v1 heuristic missed.
    """
    event_lookup = _event_index(contract)
    state_vars_by_name = {sv.name: sv for sv in getattr(contract, "state_variables_ordered", [])}
    getter_by_var = _build_getter_index(contract)

    referenced_state_vars = _collect_state_var_operands(predicate_trees)
    external_contract_vars_from_effects = _collect_external_contract_state_vars_from_effects(
        effects,
        set(state_vars_by_name.keys()),
    )
    # Union of two sources for "this state-var is an external contract":
    #   1. predicate_trees flagged it as authority_contract.address_source
    #   2. effects records an external_call from a function on it
    authority_state_vars = _collect_authority_state_vars(predicate_trees) | external_contract_vars_from_effects
    # Effects-discovered external-contract vars get added to the
    # referenced set so Pass 2 emits a tracking target for them even if
    # they don't appear as a leaf operand (e.g. ``hook`` written by
    # ``setHook(address)`` and called from the body, but not gated).
    referenced_state_vars |= external_contract_vars_from_effects
    external_role_callees = _collect_external_call_role_callees(predicate_trees)
    role_key_state_vars = _collect_role_key_state_vars(predicate_trees)

    role_definitions = list(access_control.get("role_definitions", []) if access_control else [])

    tracking_targets: list[ControllerTrackingTarget] = []
    seen_ids: set[str] = set()

    # Pass 1: role identifiers (drawn from access_control.role_definitions —
    # the static pipeline's bytes32-constant scan). Each becomes a
    # role_identifier ControllerTrackingTarget; if predicate_trees
    # disambiguates a single registry source, attach contract_source.
    #
    # Skip role_definitions whose name is also an authority-registry state var.
    # The external_contract target gets writer/events through Pass 2; emitting
    # a state_only role_identifier for the same source would mask that row.
    for role_def in role_definitions:
        role_name = role_def.get("role")
        if not role_name:
            continue
        if role_name in authority_state_vars:
            continue
        controller_id = f"role_identifier:{role_name}"
        if controller_id in seen_ids:
            continue
        read_spec: ControllerReadSpec = {"strategy": "getter_call", "target": role_name}
        authority_source = _role_authority_for(role_name, predicate_trees) or _role_authority_for_external_callee(
            role_name,
            predicate_trees,
        )
        if authority_source:
            read_spec["contract_source"] = authority_source
        tracking_targets.append(
            {
                "controller_id": controller_id,
                "label": role_name,
                "source": role_name,
                "kind": "role_identifier",
                "read_spec": read_spec,
                "confidence": None,
                "tracking_mode": "state_only",
                "writer_functions": [],
                "associated_events": [],
                "polling_sources": [role_name],
                "notes": [
                    "Resolve the role identifier via eth_call and expand current "
                    "members through the authority adapter when supported."
                ],
            }
        )
        seen_ids.add(controller_id)

    role_def_names = {
        role_def.get("role")
        for role_def in role_definitions
        if role_def.get("role") and role_def.get("role") not in authority_state_vars
    }

    # Pass 2: every state-variable operand referenced by a leaf.
    # Authority-registry vars (authority_contract.address_source) get
    # promoted to ``external_contract`` kind; everything else is
    # ``state_variable``. Role-identifier-named vars not declared as
    # state vars (compile-time bytes32 constants referenced by name) get
    # role_identifier targets here too.
    for name in sorted(referenced_state_vars):
        if name in role_def_names:
            continue
        sv = state_vars_by_name.get(name)
        # Role-identifier classification is structural: a var is a role when
        # it is used as an OZ role key or is a referenced bytes32 constant.
        is_bytes32_constant = (
            sv is not None and str(getattr(sv, "type", "")) == "bytes32" and bool(getattr(sv, "is_constant", False))
        )
        is_role = name in role_key_state_vars or is_bytes32_constant
        if is_role:
            controller_id = f"role_identifier:{name}"
            if controller_id in seen_ids:
                continue
            read_spec_role: ControllerReadSpec = {"strategy": "getter_call", "target": name}
            authority_source = _role_authority_for(name, predicate_trees)
            if authority_source:
                read_spec_role["contract_source"] = authority_source
            tracking_targets.append(
                {
                    "controller_id": controller_id,
                    "label": name,
                    "source": name,
                    "kind": "role_identifier",
                    "read_spec": read_spec_role,
                    "confidence": None,
                    "tracking_mode": "state_only",
                    "writer_functions": [],
                    "associated_events": [],
                    "polling_sources": [name],
                    "notes": [
                        "Resolve the role identifier via eth_call and expand current "
                        "members through the authority adapter when supported."
                    ],
                }
            )
            seen_ids.add(controller_id)
            continue

        kind: str = "external_contract" if name in authority_state_vars else "state_variable"
        controller_id = f"{kind}:{name}"
        if controller_id in seen_ids:
            continue
        read_spec_var = _state_var_read_spec(name, state_vars_by_name, getter_by_var)
        writer_functions, associated_events = _writer_records_from_effects(
            contract,
            project_dir,
            [name],
            event_lookup,
            effects,
        )
        if associated_events:
            tracking_mode: str = "event_plus_state"
            notes = [
                "Monitor associated events for low-latency detection and confirm "
                "the resulting controller state with RPC reads."
            ]
        else:
            tracking_mode = "state_only"
            notes = [
                "No deterministically associated post-deploy events were found "
                "for this controller state; rely on periodic RPC reads and "
                "reconciliation."
            ]
            if not writer_functions:
                notes.append(
                    "No post-deploy writer functions were found from static "
                    "analysis; continue polling the current value and reanalyze "
                    "on implementation changes."
                )

        tracking_targets.append(
            {
                "controller_id": controller_id,
                "label": name,
                "source": name,
                "kind": kind,  # type: ignore[typeddict-item]
                "read_spec": read_spec_var,
                "confidence": None,
                "tracking_mode": tracking_mode,  # type: ignore[typeddict-item]
                "writer_functions": writer_functions,
                "associated_events": associated_events,
                "polling_sources": [name],
                "notes": notes,
            }
        )
        seen_ids.add(controller_id)

    # Pass 3: external-call callees used as OZ role keys (e.g.
    # ``roleRegistry.BREAK_GLASS()``). The role constant lives on the registry
    # contract; read it via getter on the registry. Pair with the registry
    # source from authority_contract.address_source when disambiguated.
    for callee_name in sorted(external_role_callees):
        controller_id = f"role_identifier:{callee_name}"
        if controller_id in seen_ids:
            continue
        if callee_name in role_def_names:
            continue
        read_spec_external: ControllerReadSpec = {"strategy": "getter_call", "target": callee_name}
        authority_source = _role_authority_for_external_callee(callee_name, predicate_trees)
        if authority_source:
            read_spec_external["contract_source"] = authority_source
        tracking_targets.append(
            {
                "controller_id": controller_id,
                "label": callee_name,
                "source": callee_name,
                "kind": "role_identifier",
                "read_spec": read_spec_external,
                "confidence": None,
                "tracking_mode": "state_only",
                "writer_functions": [],
                "associated_events": [],
                "polling_sources": [callee_name],
                "notes": [
                    "Role identifier sourced via cross-contract call "
                    "(e.g. ``registry.ROLE()``); resolve through the "
                    "authority adapter."
                ],
            }
        )
        seen_ids.add(controller_id)

    return sorted(tracking_targets, key=lambda item: item["label"])


def build_policy_tracking(
    contract,
    project_dir: Path,
    effects: Mapping[str, Any] | None = None,
) -> list[PolicyTrackingTarget]:
    """Build event-driven tracking metadata for table-backed authorization
    policies like ``canCall``. Sources writers from the v2 ``effects``
    artifact (no permission_graph dependency)."""
    event_lookup = _event_index(contract)
    policy_targets: list[PolicyTrackingTarget] = []

    for function in _contract_functions(contract):
        if getattr(function, "full_name", "") != "canCall(address,address,bytes4)":
            continue

        tracked_state_targets = sorted(
            {
                getattr(variable, "name", "")
                for variable in getattr(function, "state_variables_read", [])
                if getattr(variable, "name", "")
            }
        )
        if not tracked_state_targets:
            continue

        writer_functions, associated_events = _writer_records_from_effects(
            contract,
            project_dir,
            tracked_state_targets,
            event_lookup,
            effects,
        )
        if not writer_functions or not associated_events:
            continue

        policy_targets.append(
            {
                "policy_id": "canCall_policy",
                "label": "canCall policy",
                "policy_function": getattr(function, "full_name", getattr(function, "name", "canCall")),
                "tracked_state_targets": tracked_state_targets,
                "writer_functions": writer_functions,
                "associated_events": associated_events,
                "notes": [
                    "Track authorization-policy mutations through emitted events; "
                    "the underlying table-backed state is non-enumerable for "
                    "generic polling."
                ],
            }
        )

    return sorted(policy_targets, key=lambda item: item["label"])
