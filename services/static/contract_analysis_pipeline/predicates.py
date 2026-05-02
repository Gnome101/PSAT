"""Predicate builder — produces a ``PredicateTree`` per function.

For each function:
  1. Run ``RevertDetector`` to find all gated revert paths.
  2. Run ``ProvenanceEngine`` to classify every SSA value's source(s).
  3. For each RevertGate, walk the condition value's defining IR back
     to its structural shape (Binary equality, Index membership, Unary
     negation, HighLevelCall returning bool, ecrecover comparison) and
     emit a ``LeafPredicate`` with kind + operator + operands.
  4. Apply polarity normalization: ``if (R) revert`` becomes the leaf
     for the allowed condition (NOT R), with the NOT pushed into the
     leaf's operator (no NOT survives in the tree).
  5. Combine leaves into a tree: multiple sequential gates AND at the
     root.

This module is the main user of ``ProvenanceEngine`` + ``RevertDetector``
and the producer of the v2 schema's ``predicate_tree`` artifact field.

Scope of this initial cut: equality / membership leaves with the
caller_authority detection rules from v6 round-5 #1. external_bool /
signature_auth / comparison / unsupported leaves are added in
follow-ups (this commit lays the scaffold + the two most common kinds).
"""

from __future__ import annotations

from typing import Any

try:
    from slither.core.declarations import SolidityVariable  # type: ignore[import]
    from slither.core.variables.state_variable import StateVariable  # type: ignore[import]
    from slither.slithir.operations import (  # type: ignore[import]
        Binary,
        HighLevelCall,
        Index,
        SolidityCall,
        Unary,
        UnaryType,
    )
    from slither.slithir.variables import Constant  # type: ignore[import]

    SLITHER_AVAILABLE = True
except Exception:  # pragma: no cover
    SLITHER_AVAILABLE = False

from .predicate_types import (
    AuthorityRole,
    LeafKind,
    LeafOperator,
    LeafPredicate,
    Operand,
    PredicateTree,
    SetDescriptor,
    make_and_node,
    make_leaf_node,
    make_or_node,
)
from .provenance import (
    EMPTY,
    TOP,
    ProvenanceEngine,
    ProvenanceMap,
    Source,
    SourceSet,
    is_top,
)
from .revert_detect import RevertDetector, RevertGate


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_predicate_tree(function: Any) -> PredicateTree | None:
    """Construct a PredicateTree for one function. Returns None if
    the function has no revert paths (i.e., is unguarded — not in
    privileged_functions)."""
    if not SLITHER_AVAILABLE:
        raise RuntimeError("predicate builder requires slither")
    detector = RevertDetector(function)
    gates = detector.run()
    if not gates:
        return None

    engine = ProvenanceEngine(function)
    engine.run()
    prov = engine.provenance

    subtrees: list[PredicateTree] = []
    for gate in gates:
        subtree = _build_subtree_from_gate(gate, prov, function)
        if subtree is not None:
            subtrees.append(subtree)

    if not subtrees:
        return None
    return make_and_node(subtrees)


def _build_subtree_from_gate(
    gate: RevertGate,
    prov: ProvenanceMap,
    function: Any,
) -> PredicateTree | None:
    """Like ``_build_leaf_from_gate``, but returns a PredicateTree
    so binary ``&&`` / ``||`` at the gate's condition can split into
    AND/OR tree nodes instead of collapsing into a single
    ``unsupported`` leaf."""
    if gate.kind == "opaque":
        leaf = _unsupported_leaf(
            reason=gate.unsupported_reason or "opaque_control_flow",
            expression=gate.expression_text,
        )
        return make_leaf_node(leaf)

    cond = gate.condition_value
    if cond is None:
        return make_leaf_node(_unsupported_leaf(reason="missing_condition", expression=gate.expression_text))

    return _build_subtree_from_value(cond, prov, gate, function)


def _build_subtree_from_value(
    cond_value: Any,
    prov: ProvenanceMap,
    gate: RevertGate,
    function: Any,
) -> PredicateTree:
    """Walk back from ``cond_value`` to its defining IR. If the IR
    is a Binary with type ANDAND / OROR, split into a subtree
    recursively. Otherwise build a single LeafPredicate."""
    defining_ir = _find_defining_ir(cond_value, gate.node, function)
    if defining_ir is None:
        leaf = _build_truthy_leaf(cond_value, prov, gate)
        return make_leaf_node(leaf)

    if isinstance(defining_ir, Binary):
        op_name = _binary_op(getattr(defining_ir, "type", None))
        if op_name in ("and", "or"):
            left_tree = _build_subtree_from_value(defining_ir.variable_left, prov, gate, function)
            right_tree = _build_subtree_from_value(defining_ir.variable_right, prov, gate, function)
            children = [left_tree, right_tree]
            # Apply if-revert polarity flip at the AND/OR level too:
            # `if (A || B) revert` means allowed iff !A && !B (De
            # Morgan). For now, polarity is propagated to leaves via
            # _build_leaf_from_gate; AND/OR composition uses the
            # source-level connective.
            if gate.polarity == "allowed_when_true":
                return make_and_node(children) if op_name == "and" else make_or_node(children)
            # if-revert polarity flips AND ↔ OR via De Morgan.
            return make_or_node(children) if op_name == "and" else make_and_node(children)

    leaf = _classify_leaf_from_ir(defining_ir, prov, gate, function)
    if leaf is None:
        return make_leaf_node(
            _unsupported_leaf(
                reason="unrecognized_condition_shape",
                expression=gate.expression_text,
            )
        )
    return make_leaf_node(leaf)


# ---------------------------------------------------------------------------
# Leaf construction per gate
# ---------------------------------------------------------------------------


def _build_leaf_from_gate(
    gate: RevertGate,
    prov: ProvenanceMap,
    function: Any,
) -> LeafPredicate | None:
    """Walk back from the gate's condition value to its defining IR
    and produce a typed LeafPredicate. The operator captures the
    original-source polarity AND the if-revert flip, so by the time
    this returns the polarity is fully baked into the operator (no
    NOT survives downstream).
    """
    if gate.kind == "opaque":
        return _unsupported_leaf(
            reason=gate.unsupported_reason or "opaque_control_flow", expression=gate.expression_text
        )

    cond = gate.condition_value
    if cond is None:
        return _unsupported_leaf(reason="missing_condition", expression=gate.expression_text)

    # Walk back to find the defining IR for the condition.
    defining_ir = _find_defining_ir(cond, gate.node, function)
    if defining_ir is None:
        # The condition is a bare value (parameter / state-var read /
        # constant). For ``require(boolFlag)`` or
        # ``require(_blacklist[msg.sender] == false)`` this is the
        # case — the leaf is a truthy/falsy check on the value.
        return _build_truthy_leaf(cond, prov, gate)

    leaf = _classify_leaf_from_ir(defining_ir, prov, gate, function)
    if leaf is None:
        return _unsupported_leaf(
            reason="unrecognized_condition_shape",
            expression=gate.expression_text,
        )
    return leaf


def _classify_leaf_from_ir(
    defining_ir: Any,
    prov: ProvenanceMap,
    gate: RevertGate,
    function: Any | None = None,
) -> LeafPredicate | None:
    """Dispatch on the defining IR class to build a LeafPredicate."""
    if isinstance(defining_ir, Binary):
        return _build_binary_leaf(defining_ir, prov, gate)
    if isinstance(defining_ir, Unary):
        return _build_unary_leaf(defining_ir, prov, gate, function)
    if isinstance(defining_ir, Index):
        # Direct mapping membership: ``require(map[k][m])`` — the
        # condition value is the Index lvalue, classified as a
        # truthy/falsy membership leaf.
        return _build_index_membership_leaf(defining_ir, prov, gate, function)
    if isinstance(defining_ir, HighLevelCall):
        return _build_external_bool_leaf(defining_ir, prov, gate)
    if isinstance(defining_ir, SolidityCall):
        return _build_solidity_call_leaf(defining_ir, prov, gate)
    return None


# ---------------------------------------------------------------------------
# Per-IR-kind leaf builders
# ---------------------------------------------------------------------------


def _build_binary_leaf(ir: Any, prov: ProvenanceMap, gate: RevertGate) -> LeafPredicate:
    """A Binary IR drives the gate. Type maps to operator; operands
    are classified via provenance."""
    bt = getattr(ir, "type", None)
    op_name = _binary_op(bt)
    left = _operand_for_value(ir.variable_left, prov)
    right = _operand_for_value(ir.variable_right, prov)
    operands = [left, right]

    if op_name in ("eq", "ne", "lt", "lte", "gt", "gte"):
        # Apply if-revert polarity flip to operator.
        operator = _apply_polarity(op_name, gate.polarity)
        kind: LeafKind = "equality" if operator in ("eq", "ne") else "comparison"
        leaf = _make_leaf(
            kind=kind,
            operator=operator,
            operands=operands,
            gate=gate,
        )
        leaf["authority_role"] = _classify_authority_equality(leaf, kind)
        return leaf
    # AND/OR at the binary level — these would normally be handled by
    # short-circuit evaluation; for now we treat as unsupported and
    # let the predicate-tree composition layer (week 2) split them
    # into AND/OR tree nodes properly.
    return _unsupported_leaf(reason=f"binary_op_{op_name}_unsupported", expression=str(ir))


def _build_unary_leaf(ir: Any, prov: ProvenanceMap, gate: RevertGate, function: Any | None) -> LeafPredicate:
    """``require(!X)`` — condition is a Unary NOT. Recurse on the
    operand with the polarity flipped, so the resulting operator is
    correctly inverted."""
    op_type = getattr(ir, "type", None)
    if op_type == getattr(UnaryType, "BANG", "!"):
        inner = ir.rvalue
        flipped_polarity = "allowed_when_true" if gate.polarity == "allowed_when_false" else "allowed_when_false"
        new_gate = RevertGate(
            kind=gate.kind,
            condition_value=inner,
            polarity=flipped_polarity,
            node=gate.node,
            expression_text=gate.expression_text,
            basis=gate.basis,
        )
        return _build_leaf_from_gate(new_gate, prov, function) or _unsupported_leaf(
            reason="negated_unknown", expression=str(ir)
        )
    return _unsupported_leaf(reason=f"unary_{op_type}_unsupported", expression=str(ir))


def _build_index_membership_leaf(
    ir: Any, prov: ProvenanceMap, gate: RevertGate, function: Any | None = None
) -> LeafPredicate:
    """``require(map[k][m])`` style. Operator is truthy when polarity
    is allowed_when_true, falsy otherwise. For multi-key mappings
    (``map[a][b]``) we walk through chained Index IRs to collect
    every key in source order."""
    operator: LeafOperator = "truthy" if gate.polarity == "allowed_when_true" else "falsy"
    keys = _reconstruct_index_chain(ir, prov, function)
    descriptor: SetDescriptor = {
        "kind": "mapping_membership",
        "key_sources": keys,
    }
    base_var = _find_index_base(ir, function)
    if base_var is not None:
        descriptor["storage_var"] = getattr(base_var, "name", None)
    leaf = _make_leaf(
        kind="membership",
        operator=operator,
        operands=keys,
        gate=gate,
    )
    leaf["set_descriptor"] = descriptor
    leaf["authority_role"] = _classify_authority_membership(leaf, descriptor)
    return leaf


def _build_external_bool_leaf(ir: Any, prov: ProvenanceMap, gate: RevertGate) -> LeafPredicate:
    """``require(other.canCall(...))`` — HighLevelCall whose result
    drives the gate."""
    callee_name = getattr(getattr(ir, "function", None), "name", None) or getattr(ir, "function_name", None)
    args_operands = [_operand_for_value(a, prov) for a in getattr(ir, "arguments", ())]
    operator: LeafOperator = "truthy" if gate.polarity == "allowed_when_true" else "falsy"
    leaf = _make_leaf(
        kind="external_bool",
        operator=operator,
        operands=args_operands,
        gate=gate,
    )
    # Authority classification for external_bool: delegated_authority
    # if the call target traces to a state_variable AND any arg
    # traces to msg_sender or signature_recovery.
    target_sources = _sources_from_destination(ir, prov)
    has_state_target = any(s.kind == "state_variable" for s in target_sources)
    has_caller_arg = any(
        any(s.kind in ("msg_sender", "tx_origin", "signature_recovery") for s in _sources_for_value(a, prov))
        for a in getattr(ir, "arguments", ())
    )
    if has_state_target and has_caller_arg:
        leaf["authority_role"] = "delegated_authority"
    else:
        leaf["authority_role"] = "business"
    leaf["expression"] = f"{callee_name}(...)"
    return leaf


def _build_solidity_call_leaf(ir: Any, prov: ProvenanceMap, gate: RevertGate) -> LeafPredicate:
    """SolidityCall returning a bool used as a gate (rare). Treat as
    business by default; specific SolidityCalls (ecrecover) are
    classified by the operand provenance, not here."""
    fn = getattr(ir, "function", None)
    name = getattr(fn, "name", None) or str(fn or "")
    return _unsupported_leaf(
        reason=f"solidity_call_{name}_unsupported_as_gate",
        expression=str(ir),
    )


def _build_truthy_leaf(cond: Any, prov: ProvenanceMap, gate: RevertGate) -> LeafPredicate:
    """``require(boolFlag)`` where ``cond`` is a bare variable, not
    the result of a Binary/Index/etc. We treat as a unary boolean
    check on the value with operator=truthy/falsy by polarity."""
    operator: LeafOperator = "truthy" if gate.polarity == "allowed_when_true" else "falsy"
    operand = _operand_for_value(cond, prov)
    leaf = _make_leaf(
        kind="equality",
        operator=operator,
        operands=[operand],
        gate=gate,
    )
    leaf["authority_role"] = "business"  # bare-bool gates rarely auth
    return leaf


# ---------------------------------------------------------------------------
# Operand classification
# ---------------------------------------------------------------------------


def _operand_for_value(value: Any, prov: ProvenanceMap) -> Operand:
    """Translate a Slither IR value's source set into the v2 Operand
    record. Picks the most informative source if multiple are
    present."""
    sources = _sources_for_value(value, prov)
    if not sources:
        return {"source": "constant", "constant_value": str(value) if value is not None else ""}
    # Priority: msg_sender > signature_recovery > parameter > state_variable
    # > view_call > external_call > computed > constant > block_context > top.
    priority = (
        "msg_sender",
        "tx_origin",
        "signature_recovery",
        "parameter",
        "state_variable",
        "view_call",
        "external_call",
        "computed",
        "constant",
        "block_context",
        "top",
    )
    for kind in priority:
        for s in sources:
            if s.kind == kind:
                return _source_to_operand(s)
    # Fallback: any source.
    return _source_to_operand(next(iter(sources)))


def _source_to_operand(source: Source) -> Operand:
    op: Operand = {"source": source.kind}  # type: ignore[typeddict-item]
    if source.parameter_index is not None:
        op["parameter_index"] = source.parameter_index
    if source.parameter_name is not None:
        op["parameter_name"] = source.parameter_name
    if source.state_variable_name is not None:
        op["state_variable_name"] = source.state_variable_name
    if source.callee is not None:
        op["callee"] = source.callee
    if source.constant_value is not None:
        op["constant_value"] = source.constant_value
    if source.computed_kind is not None:
        op["computed_kind"] = source.computed_kind
    if source.block_context_kind is not None:
        op["block_context_kind"] = source.block_context_kind
    return op


def _sources_for_value(value: Any, prov: ProvenanceMap) -> SourceSet:
    """Read provenance for a Slither value.

    For SolidityVariables (msg.sender / tx.origin / block.*) we
    classify on-demand — they don't appear as SSA lvalues in the
    provenance map. For StateVariables we emit a state_variable
    source directly. For Constants we emit a constant source. For
    everything else (LocalIRVariables, ReferenceVariables, TMPs,
    Phi outputs) we look up the name in the provenance map.
    """
    if value is None:
        return EMPTY
    if isinstance(value, Constant):
        return frozenset({Source(kind="constant", constant_value=str(value.value))})
    if isinstance(value, SolidityVariable):
        return _classify_solidity_variable(value)
    if isinstance(value, StateVariable):
        return frozenset({Source(kind="state_variable", state_variable_name=value.name)})
    name = getattr(value, "name", None)
    if name is None:
        return EMPTY
    return prov.get(name)


def _classify_solidity_variable(var: Any) -> SourceSet:
    """Same logic as ProvenanceEngine._classify_solidity_variable but
    re-implemented here so the predicate builder can call it on
    operands without needing the engine instance."""
    name = getattr(var, "name", "")
    if name == "msg.sender":
        return frozenset({Source(kind="msg_sender")})
    if name == "tx.origin":
        return frozenset({Source(kind="tx_origin")})
    if name in (
        "block.timestamp",
        "block.number",
        "block.chainid",
        "block.coinbase",
        "block.difficulty",
        "block.gaslimit",
        "now",
        "block.basefee",
        "block.prevrandao",
    ):
        return frozenset(
            {
                Source(
                    kind="block_context",
                    block_context_kind=name.split(".", 1)[-1] if "." in name else name,
                )
            }
        )
    if name in ("msg.value", "msg.data", "msg.sig", "msg.gas"):
        return frozenset({Source(kind="computed", computed_kind=name)})
    return TOP


def _sources_from_destination(ir: Any, prov: ProvenanceMap) -> SourceSet:
    """For a HighLevelCall, return the destination (call target)'s
    provenance. Slither exposes this as ``destination``."""
    dest = getattr(ir, "destination", None)
    return _sources_for_value(dest, prov) if dest is not None else EMPTY


# ---------------------------------------------------------------------------
# Authority classification (v5/v6 round-2 fix; minimal cut)
# ---------------------------------------------------------------------------


def _classify_authority_equality(leaf: LeafPredicate, kind: LeafKind) -> AuthorityRole:
    """Rule A (caller equality): kind=="equality", op=="eq", one
    operand is msg_sender/tx_origin/signature_recovery, the OTHER is
    address-typed (state/view/parameter/sig). Otherwise business.

    Time gate: at least one operand sources from block_context AND
    no operand sources from msg.sender/tx.origin/signature_recovery
    (the caller takes priority — `require(block.timestamp >
    cooldown[msg.sender])` is still primarily a caller-keyed check).
    """
    operands = leaf.get("operands", [])
    if not operands:
        return "business"
    has_caller = any(o["source"] in ("msg_sender", "tx_origin", "signature_recovery") for o in operands)
    has_block_context = any(o["source"] == "block_context" for o in operands)
    if has_block_context and not has_caller:
        return "time"
    if kind == "equality" and leaf["operator"] == "eq" and has_caller:
        return "caller_authority"
    return "business"


def _classify_authority_membership(leaf: LeafPredicate, descriptor: SetDescriptor) -> AuthorityRole:
    """Rule B (auth-shaped membership): membership op=truthy/falsy,
    msg.sender as a key, multi-key direct-promote (>=2 keys is a
    permission table by structure). 1-key requires the writer-key
    two-pass (week 3 deliverable) — until then default to business.
    """
    keys = descriptor.get("key_sources", [])
    if not keys:
        return "business"
    has_caller_key = any(k["source"] in ("msg_sender", "tx_origin", "signature_recovery") for k in keys)
    if not has_caller_key:
        return "business"
    if len(keys) >= 2:
        # Multi-key with caller as one key: permission table.
        return "caller_authority"
    # 1-key caller-only: needs writer-key analysis (week 3).
    # For now, default to business so we don't over-admit. The
    # writer-key two-pass will promote when applicable.
    return "business"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _binary_op(bt: Any) -> str:
    """Map Slither BinaryType → leaf operator string."""
    if bt is None:
        return "unknown"
    name = getattr(bt, "name", str(bt)).upper()
    return {
        "EQUAL": "eq",
        "NOT_EQUAL": "ne",
        "LESS": "lt",
        "LESS_EQUAL": "lte",
        "GREATER": "gt",
        "GREATER_EQUAL": "gte",
        "ANDAND": "and",
        "OROR": "or",
    }.get(name, name.lower())


def _apply_polarity(operator: str, polarity: str) -> LeafOperator:
    """If polarity is allowed_when_false (if-revert), invert the
    operator. The inversion table: eq↔ne, lt↔gte, lte↔gt."""
    if polarity == "allowed_when_true":
        return operator  # type: ignore[return-value]
    inv = {"eq": "ne", "ne": "eq", "lt": "gte", "gte": "lt", "lte": "gt", "gt": "lte"}
    return inv.get(operator, operator)  # type: ignore[return-value]


def _make_leaf(
    *,
    kind: LeafKind,
    operator: LeafOperator,
    operands: list[Operand],
    gate: RevertGate,
) -> LeafPredicate:
    refs_caller = any(o["source"] in ("msg_sender", "tx_origin") for o in operands)
    param_indices: list[int] = [
        idx for o in operands if o["source"] == "parameter" and (idx := o.get("parameter_index")) is not None
    ]
    return {
        "kind": kind,
        "operator": operator,
        "authority_role": "business",  # filled in by caller
        "operands": operands,
        "references_msg_sender": refs_caller,
        "parameter_indices": param_indices,
        "expression": gate.expression_text or "",
        "basis": list(gate.basis),
    }


def _unsupported_leaf(reason: str, expression: str) -> LeafPredicate:
    return {
        "kind": "unsupported",
        "operator": "truthy",  # placeholder; ignored for unsupported
        "authority_role": "business",
        "operands": [],
        "unsupported_reason": reason,
        "references_msg_sender": False,
        "parameter_indices": [],
        "expression": expression,
        "basis": [reason],
    }


def _find_defining_ir(value: Any, node: Any, function: Any) -> Any | None:
    """Find the IR opcode whose lvalue equals ``value``. Looks in
    the gate's home node first, then walks back through the
    function's nodes (covers cases where the condition is computed
    a few lines before the gate)."""
    name = getattr(value, "name", None)
    if name is None:
        return None
    # Search in CFG order, starting from the gate's node and walking
    # backward. Slither's nodes list is in source order, so the
    # defining IR for a temp/local appears earlier than its use.
    nodes = list(getattr(function, "nodes", []) or [])
    if node is not None and node in nodes:
        idx = nodes.index(node)
        candidate_nodes = nodes[idx::-1]
    else:
        candidate_nodes = list(reversed(nodes))
    for n in candidate_nodes:
        for ir in reversed(getattr(n, "irs_ssa", None) or getattr(n, "irs", []) or []):
            lv = getattr(ir, "lvalue", None)
            if lv is not None and getattr(lv, "name", None) == name:
                return ir
    return None


def _reconstruct_index_chain(ir: Any, prov: ProvenanceMap, function: Any | None = None) -> list[Operand]:
    """Walk an Index IR's variable_left chain to assemble all keys
    (outer → inner). For an N-level mapping like ``map[a][b][c]``,
    Slither emits N nested Index IRs, each whose variable_left is
    the previous Index's lvalue. We walk back through the function
    to collect each key in source order.
    """
    keys: list[Operand] = []
    visited: set[str] = set()
    current = ir
    while isinstance(current, Index):
        keys.insert(0, _operand_for_value(current.variable_right, prov))
        left = current.variable_left
        left_name = getattr(left, "name", None)
        if left_name in visited:
            break  # cycle guard
        if left_name is not None:
            visited.add(left_name)
        # If the left is itself the lvalue of an outer Index, find
        # that IR and continue the walk.
        if function is None:
            break
        defining = _find_defining_ir(left, None, function)
        if not isinstance(defining, Index):
            break
        current = defining
    return keys


def _find_index_base(ir: Any, function: Any | None = None) -> Any | None:
    """Walk back through chained Index IRs to the underlying storage
    variable (StateVariable). Returns the variable_left of the
    outermost Index in the chain.
    """
    current = ir
    visited: set[str] = set()
    while isinstance(current, Index):
        left = current.variable_left
        left_name = getattr(left, "name", None)
        if left_name in visited:
            return left
        if left_name is not None:
            visited.add(left_name)
        if function is None:
            return left
        defining = _find_defining_ir(left, None, function)
        if not isinstance(defining, Index):
            return left
        current = defining
    return None


# Re-export for tests / consumers.
__all__ = [
    "build_predicate_tree",
    "ProvenanceMap",
    "TOP",
    "EMPTY",
    "is_top",
]
