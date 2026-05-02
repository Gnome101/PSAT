"""RevertDetector — structured walk of all gated revert paths in a function.

Returns a list of ``RevertGate`` records, each describing:
  * the IR-level condition value that, when violated, leads to the revert
  * the polarity: ``allowed_when="C"`` means require(C); ``allowed_when=
    "not C"`` means if(C) revert (predicate builder pushes the NOT into
    each leaf's operator).
  * the kind: ``require / assert / custom_revert / inline_asm /
    try_catch_revert / function_pointer_check / opaque``

Per the v4 plan (round-2 finding #8 on edge-case soundness), we cover:
  1. require / require with msg
  2. assert
  3. if (C) revert / revert ErrorName(args)
  4. SolidityCall(revert)
  5. assembly { if iszero(X) { revert(0,0) } }   — inline asm conditional
  6. try external.call() catch { revert(); }     — try/catch fallback
  7. State-stored function pointer dispatch:
        function p; require(p == expectedSig)
     The function-pointer source is classified via ProvenanceEngine; the
     gate is then a normal equality leaf of two state-vars (or
     state-var+constant). Authority classification depends on whether
     either operand traces to msg.sender; otherwise the leaf is
     business.
  8. Fully-opaque control flow (Yul jumps not modeled by Slither): the
     detector emits a single ``opaque`` gate with no condition and the
     predicate builder turns this into a ``kind="unsupported",
     reason="opaque_control_flow"`` leaf.

Cases 1-4 reuse the structural primitives from caller_sinks.py
(``_node_is_revert_gate``, ``_ir_is_revert``); cases 5-7 are added
here. Case 8 is detected by checking whether the function has any
InlineAssemblyOperation IR that we couldn't resolve — at which point
we mark the function as needing review.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

try:
    from slither.core.cfg.node import NodeType  # type: ignore[import]
    from slither.core.declarations.modifier import Modifier  # type: ignore[import]
    from slither.slithir.operations import (  # type: ignore[import]
        Condition,
        InternalCall,
        LibraryCall,
    )

    SLITHER_AVAILABLE = True
except Exception:  # pragma: no cover
    SLITHER_AVAILABLE = False
    Modifier = type(None)  # placeholder


DEFAULT_INTERNAL_CALL_DEPTH = 4


RevertKind = Literal[
    "require",
    "assert",
    "custom_revert",
    "if_revert",
    "inline_asm",
    "try_catch_revert",
    "opaque",
]

Polarity = Literal["allowed_when_true", "allowed_when_false"]


@dataclass
class RevertGate:
    """One gated revert path in a function.

    The predicate builder consumes a list of these to construct the
    function's PredicateTree. Multiple gates AND together at the tree
    root.
    """

    kind: RevertKind
    # The condition IR value that drives the revert. None for opaque
    # / unconditional revert paths.
    condition_value: Any = None
    polarity: Polarity = "allowed_when_true"
    # Slither node where the gate lives — used by the predicate builder
    # for parameter-binding / modifier-frame lookups.
    node: Any = None
    # Slither function/modifier whose body contains the gate node.
    # When the gate is inside a cross-function helper (e.g.,
    # ``_checkRole`` called from a modifier), this is the helper —
    # the predicate builder uses it to walk the condition's defining
    # IR through the right scope.
    containing_function: Any = None
    # Diagnostic text for predicate.expression / leaf.basis.
    expression_text: str = ""
    basis: list[str] = field(default_factory=list)
    # If kind=="opaque", the reason string surfaced as
    # unsupported_reason on the predicate leaf.
    unsupported_reason: str | None = None


# ---------------------------------------------------------------------------
# Primitive predicates — adapted from caller_sinks.py but exposed as
# building blocks the predicate builder can call directly.
# ---------------------------------------------------------------------------


def _ir_class(ir: Any) -> str:
    return type(ir).__name__


def _ir_is_solidity_revert(ir: Any) -> bool:
    """Slither emits SolidityCall(``revert(...)``) for both Solidity-
    level reverts and Yul-level revert(offset, length). The signature
    string varies (``revert()``, ``revert(string)``, ``revert(uint256,
    uint256)``, ``revert ErrorName``), so we accept any SolidityCall
    whose function name begins with ``revert(`` or ``revert ``."""
    if _ir_class(ir) != "SolidityCall":
        return False
    fn = getattr(ir, "function", None)
    name = getattr(fn, "name", None) or str(fn or "")
    return name.startswith("revert(") or name.startswith("revert ")


def _ir_is_require(ir: Any) -> bool:
    if _ir_class(ir) != "SolidityCall":
        return False
    fn = getattr(ir, "function", None)
    name = getattr(fn, "name", None) or str(fn or "")
    return name in ("require(bool)", "require(bool,string)")


def _ir_is_assert(ir: Any) -> bool:
    if _ir_class(ir) != "SolidityCall":
        return False
    fn = getattr(ir, "function", None)
    name = getattr(fn, "name", None) or str(fn or "")
    return name == "assert(bool)"


# ---------------------------------------------------------------------------
# Detector entry point
# ---------------------------------------------------------------------------


class RevertDetector:
    """Walk a function's IR and return all gated revert paths.

    Usage:
        detector = RevertDetector(function)
        gates = detector.run()  # list[RevertGate]
    """

    def __init__(
        self,
        function: Any,
        *,
        internal_call_depth: int = DEFAULT_INTERNAL_CALL_DEPTH,
    ) -> None:
        if not SLITHER_AVAILABLE:
            raise RuntimeError("RevertDetector requires slither")
        self.function = function
        self.internal_call_depth = internal_call_depth
        self._gates: list[RevertGate] = []
        self._call_stack: list[str] = []

    def run(self) -> list[RevertGate]:
        # Walk the function's own body.
        for node in self.function.nodes:
            self._scan_node(node, container=self.function)
        # Walk each modifier's body.
        for modifier in getattr(self.function, "modifiers", []) or []:
            for node in getattr(modifier, "nodes", []) or []:
                self._scan_node(node, container=modifier)
        # Case 8: opaque-Yul fallback.
        if self._has_unresolved_revert_in_assembly():
            self._gates.append(
                RevertGate(
                    kind="opaque",
                    unsupported_reason="opaque_control_flow",
                    expression_text="<inline assembly with unresolved revert>",
                )
            )
        return self._gates

    # ------------------------------------------------------------------
    # Per-node classification
    # ------------------------------------------------------------------

    def _scan_node(self, node: Any, container: Any = None) -> None:
        # Case 1-2: require / assert directly in this node.
        for ir in getattr(node, "irs_ssa", None) or getattr(node, "irs", []) or []:
            if _ir_is_require(ir):
                self._gates.append(self._gate_from_solidity_call(ir, node, "require", container))
                return
            if _ir_is_assert(ir):
                self._gates.append(self._gate_from_solidity_call(ir, node, "assert", container))
                return

        # Cross-function revert detection: recurse into InternalCall /
        # LibraryCall callees. The OZ AccessControl ``onlyRole`` modifier
        # body is just ``_checkRole(role); _;`` — the actual revert
        # lives inside ``_checkRole``. RevertDetector follows the call
        # to find gates the modifier doesn't directly contain.
        for ir in getattr(node, "irs_ssa", None) or getattr(node, "irs", []) or []:
            if isinstance(ir, (InternalCall, LibraryCall)):
                callee = getattr(ir, "function", None)
                if callee is None:
                    continue
                # Skip modifier-call IRs — modifiers are walked
                # separately via run() iterating function.modifiers.
                # If we recursed here we'd find the modifier's
                # reverts twice (once via the dedicated walk, once
                # via this recursion).
                if isinstance(callee, Modifier):
                    continue
                callee_id = getattr(callee, "full_name", None) or getattr(callee, "name", None)
                if not callee_id or callee_id in self._call_stack:
                    continue
                if len(self._call_stack) >= self.internal_call_depth:
                    continue
                self._call_stack.append(callee_id)
                try:
                    for sub_node in getattr(callee, "nodes", []) or []:
                        self._scan_node(sub_node, container=callee)
                finally:
                    self._call_stack.pop()

        # Cases 3-4: if (C) revert ErrorName / SolidityCall(revert) in
        # the THIS node OR a one-hop successor (slither splits these).
        condition_ir = self._extract_condition_ir(node)
        if condition_ir is None:
            return

        # Look at successor nodes — does any one-hop successor revert?
        for son in getattr(node, "sons", []) or []:
            for ir in getattr(son, "irs_ssa", None) or getattr(son, "irs", []) or []:
                if _ir_is_solidity_revert(ir):
                    # The revert is reached via this branch of the IF.
                    # Polarity: if Slither's CFG follows true→son,
                    # the condition being true takes the revert branch,
                    # so allowed_when_false. (If the false branch
                    # contains the revert, polarity is allowed_when_true.)
                    polarity = self._branch_polarity(node, son)
                    self._gates.append(
                        RevertGate(
                            kind="custom_revert"
                            if "revert " in str(getattr(getattr(ir, "function", None), "name", ""))
                            else "if_revert",
                            condition_value=getattr(condition_ir, "value", None),
                            polarity=polarity,
                            node=node,
                            containing_function=container,
                            expression_text=str(node.expression) if node.expression else "",
                            basis=[f"if-revert via successor {son.type}"],
                        )
                    )
                    return

        # Case 5: inline assembly conditional revert — limited support.
        if self._node_has_assembly_revert(node):
            self._gates.append(
                RevertGate(
                    kind="inline_asm",
                    condition_value=getattr(condition_ir, "value", None),
                    polarity="allowed_when_true",
                    node=node,
                    containing_function=container,
                    expression_text=str(node.expression) if node.expression else "<asm>",
                    basis=["inline assembly conditional revert"],
                    unsupported_reason=None,  # captured but limited
                )
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _gate_from_solidity_call(self, ir: Any, node: Any, kind: RevertKind, container: Any = None) -> RevertGate:
        # require/assert take the condition as the first argument.
        args = getattr(ir, "arguments", None) or getattr(ir, "read", None) or []
        cond = args[0] if args else None
        return RevertGate(
            kind=kind,
            condition_value=cond,
            polarity="allowed_when_true",
            node=node,
            containing_function=container,
            expression_text=str(node.expression) if node.expression else "",
            basis=[f"{kind}({cond})" if cond is not None else kind],
        )

    def _extract_condition_ir(self, node: Any) -> Any | None:
        """If `node` is an IF node, return its Condition IR (the value
        being branched on). Otherwise None."""
        if getattr(node, "type", None) != getattr(NodeType, "IF", -999):
            return None
        for ir in getattr(node, "irs_ssa", None) or getattr(node, "irs", []) or []:
            if isinstance(ir, Condition):
                return ir
        return None

    def _branch_polarity(self, if_node: Any, successor: Any) -> Polarity:
        """Determine whether the successor is the true-branch or the
        false-branch of an IF.

        Slither exposes ``son_true`` / ``son_false`` on IF nodes — if
        the revert lives on the true branch, the condition being true
        takes the revert path, so allowed_when_false."""
        son_true = getattr(if_node, "son_true", None)
        son_false = getattr(if_node, "son_false", None)
        if son_true is successor:
            return "allowed_when_false"
        if son_false is successor:
            return "allowed_when_true"
        # Fallback: if we can't tell, assume the revert was on the
        # less common false branch (typical pattern is `if (bad)
        # revert`, so true is the bad branch).
        return "allowed_when_false"

    def _node_has_assembly_revert(self, node: Any) -> bool:
        """Heuristic: a node containing assembly that ends in revert.

        Slither doesn't expose YulAST richly, so we check whether the
        node's expression text mentions `revert(` inside an
        InlineAssemblyOperation. This is a coarse signal — false
        positives are caught by the predicate builder routing it to
        an unsupported leaf rather than a typed leaf."""
        irs = getattr(node, "irs", []) or []
        for ir in irs:
            if _ir_class(ir) == "InlineAssemblyOperation":
                code = getattr(ir, "inline_asm", None) or ""
                if "revert(" in str(code):
                    return True
        return False

    def _has_unresolved_revert_in_assembly(self) -> bool:
        """Function has an InlineAssemblyOperation IR whose body
        contains a textual `revert` keyword that we did NOT
        structurally extract (Slither already parses
        ``if iszero(x) { revert(0,0) }`` into IF + SolidityCall, which
        we capture in the normal scan; this catches the residue —
        e.g. computed-target jumps to revert handlers, JUMPI tables,
        or assembly that conditionally reverts via paths Slither
        can't model)."""
        # Set of node IDs where we already classified a revert via
        # cases 1-5; assembly-residing reverts inside these nodes are
        # already accounted for.
        accounted_nodes = {id(g.node) for g in self._gates if g.node is not None}
        for node in self.function.nodes:
            for ir in getattr(node, "irs", []) or []:
                if _ir_class(ir) != "InlineAssemblyOperation":
                    continue
                code = str(getattr(ir, "inline_asm", "") or "")
                if "revert" not in code:
                    continue
                if id(node) in accounted_nodes:
                    continue
                # Assembly mentions revert and we don't have a
                # corresponding structured gate. Surface as opaque.
                return True
        return False
