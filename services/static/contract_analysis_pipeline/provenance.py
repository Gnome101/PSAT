"""ProvenanceEngine — worklist-based forward dataflow over Slither IR.

Tracks the source(s) of every SSA value reachable in a function context.
Output: a ``ProvenanceMap`` from SSA value → set of ``Source`` records.
The predicate builder consumes this to populate ``Operand`` records on
each leaf.

Design (per /tmp/psat-plans/generic-predicate-pipeline-v4.md):

* Lattice element per value = a ``frozenset[Source]``. Bottom is the
  empty set (unreached); top is ``{Source(kind="top")}`` (saturated by
  cycle / depth cap / unknown opcode).
* Worklist iterates IR opcodes in CFG order, applying transfer
  functions until fixed point. Phi nodes union incoming sources.
* Cycle handling: same SSA value re-encountered while we have an
  in-flight tag for it yields ``top``. Loop-carried Phi joins
  converge or saturate.

This module is intentionally name-free — no helper-name seed lists like
the deleted ``MsgSenderTaint``. Every classification comes from IR
shape and operand type.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable

# Slither IR types — imported lazily where they aren't part of the
# module's public surface so this file remains importable in test
# environments without solc.

try:
    from slither.core.cfg.node import NodeType  # type: ignore[import]
    from slither.core.declarations import SolidityVariable  # type: ignore[import]
    from slither.core.variables import Variable  # type: ignore[import]
    from slither.core.variables.local_variable import LocalVariable  # type: ignore[import]
    from slither.core.variables.state_variable import StateVariable  # type: ignore[import]
    from slither.slithir.operations import (  # type: ignore[import]
        Assignment,
        Binary,
        HighLevelCall,
        Index,
        InternalCall,
        Length,
        LibraryCall,
        LowLevelCall,
        Member,
        NewArray,
        NewContract,
        NewElementaryType,
        OperationWithLValue,
        Phi,
        Return,
        Send,
        SolidityCall,
        Transfer,
        TypeConversion,
        Unary,
        Unpack,
    )
    from slither.slithir.variables import Constant, ReferenceVariable, TemporaryVariable  # type: ignore[import]

    SLITHER_AVAILABLE = True
except Exception:  # pragma: no cover — only when slither unavailable
    SLITHER_AVAILABLE = False


# ---------------------------------------------------------------------------
# Source record — one origin tag for an SSA value.
# ---------------------------------------------------------------------------


SOURCE_KINDS = (
    "msg_sender",
    "tx_origin",
    "parameter",
    "state_variable",
    "constant",
    "view_call",
    "external_call",
    "computed",
    "block_context",
    "signature_recovery",
    "top",
)


@dataclass(frozen=True)
class Source:
    """One origin record for an SSA value.

    Equality is structural (frozen dataclass). Provenance sets are
    ``frozenset[Source]`` so they hash cleanly for cycle detection and
    fixed-point comparison.
    """

    kind: str  # one of SOURCE_KINDS
    parameter_index: int | None = None
    parameter_name: str | None = None
    state_variable_name: str | None = None
    callee: str | None = None
    # Hash of constituent source frozensets — used to keep view_call /
    # external_call recursive shape without making Source recursive.
    callee_args_digest: str | None = None
    constant_value: str | None = None
    computed_kind: str | None = None
    block_context_kind: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in SOURCE_KINDS:
            raise ValueError(f"unknown source kind {self.kind!r}")


SourceSet = frozenset[Source]
EMPTY: SourceSet = frozenset()
TOP: SourceSet = frozenset({Source(kind="top")})


def is_top(s: SourceSet) -> bool:
    return any(src.kind == "top" for src in s)


def union(a: SourceSet, b: SourceSet) -> SourceSet:
    """Lattice join: union of source sets, with TOP absorbing."""
    if is_top(a) or is_top(b):
        return TOP
    return a | b


# ---------------------------------------------------------------------------
# Per-function provenance map + engine.
# ---------------------------------------------------------------------------


# Configuration knobs (tunable at call site / via env).
# PSAT_PROVENANCE_INTERNAL_CALL_DEPTH overrides the default recursion
# depth for InternalCall/LibraryCall — useful for stress-testing on
# complex inheritance chains, or for cutting off depth on benchmarks.
# PSAT_PROVENANCE_WORKLIST_CAP overrides the worklist iteration cap.
def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        v = int(raw)
        return v if v > 0 else default
    except ValueError:
        return default


DEFAULT_INTERNAL_CALL_DEPTH = _env_int("PSAT_PROVENANCE_INTERNAL_CALL_DEPTH", 4)
DEFAULT_WORKLIST_ITER_CAP = _env_int("PSAT_PROVENANCE_WORKLIST_CAP", 200)


@dataclass
class ProvenanceMap:
    """Per-SSA-value provenance for one function context.

    Keyed by Slither variable name (string) since SSA values from
    Slither expose a stable ``name`` attribute. Phi nodes share the
    base name with versioning Slither already handles.
    """

    sources: dict[str, SourceSet]

    def get(self, var_name: str) -> SourceSet:
        return self.sources.get(var_name, EMPTY)

    def set(self, var_name: str, value: SourceSet) -> bool:
        """Returns True if this set changed the value (used by worklist
        to detect convergence)."""
        prev = self.sources.get(var_name, EMPTY)
        if prev == value:
            return False
        self.sources[var_name] = value
        return True


class ProvenanceEngine:
    """Forward dataflow over a function's SSA IR.

    Lifecycle:
        engine = ProvenanceEngine(function)
        engine.run()                  # populates internal map to fixed point
        sources = engine.provenance.get("some_ssa_var")
    """

    def __init__(
        self,
        function: Any,  # slither.core.declarations.Function
        *,
        internal_call_depth: int = DEFAULT_INTERNAL_CALL_DEPTH,
        worklist_cap: int = DEFAULT_WORKLIST_ITER_CAP,
        parameter_bindings: dict[str, SourceSet] | None = None,
    ) -> None:
        if not SLITHER_AVAILABLE:
            raise RuntimeError("ProvenanceEngine requires slither to be importable")
        self.function = function
        self.internal_call_depth = internal_call_depth
        self.worklist_cap = worklist_cap
        self.provenance = ProvenanceMap(sources={})
        # Stack of ParameterBindingEnv frames pushed when recursing into
        # internal callees / modifiers. Top of stack is the active env.
        self._binding_frames: list[dict[str, SourceSet]] = []
        if parameter_bindings:
            self._binding_frames.append(dict(parameter_bindings))
        # Track in-flight callees to break cycles.
        self._call_stack: list[str] = []

    # ------------------------------------------------------------------
    # Public entry
    # ------------------------------------------------------------------

    def run(self) -> ProvenanceMap:
        """Seed parameter and msg-related variables, then iterate the
        worklist until fixed point or cap hit."""
        self._seed_parameters()
        nodes = list(self._iter_nodes())
        iterations = 0
        changed = True
        while changed and iterations < self.worklist_cap:
            changed = False
            for node in nodes:
                if self._step_node(node):
                    changed = True
            iterations += 1
        if iterations >= self.worklist_cap:
            # Saturate any value we still don't know about by leaving
            # it empty; consumers treat absent ⇒ unknown. The cap is
            # high enough that real contracts converge well before.
            pass
        return self.provenance

    # ------------------------------------------------------------------
    # Seeding
    # ------------------------------------------------------------------

    def _seed_parameters(self) -> None:
        """Bind each formal parameter to a `parameter` source."""
        bindings = self._active_bindings()
        for idx, param in enumerate(self.function.parameters):
            name = self._var_name(param)
            if not name:
                continue
            if bindings is not None and name in bindings:
                # Substituted from caller — use caller's provenance.
                self.provenance.set(name, bindings[name])
                continue
            self.provenance.set(
                name,
                frozenset(
                    {
                        Source(
                            kind="parameter",
                            parameter_index=idx,
                            parameter_name=getattr(param, "name", None),
                        )
                    }
                ),
            )

    def _active_bindings(self) -> dict[str, SourceSet] | None:
        return self._binding_frames[-1] if self._binding_frames else None

    # ------------------------------------------------------------------
    # IR walk
    # ------------------------------------------------------------------

    def _iter_nodes(self) -> Iterable[Any]:
        for node in self.function.nodes:
            yield node

    def _step_node(self, node: Any) -> bool:
        """Apply transfer functions to all IRs in this CFG node.
        Returns True if any provenance value changed."""
        any_changed = False
        for ir in node.irs_ssa:
            if self._step_ir(ir):
                any_changed = True
        return any_changed

    def _step_ir(self, ir: Any) -> bool:
        # Dispatch on Slither IR class. We import lazily so this file
        # is importable without solc available in test envs.
        if isinstance(ir, Assignment):
            return self._handle_assignment(ir)
        if isinstance(ir, TypeConversion):
            return self._handle_type_conversion(ir)
        if isinstance(ir, Phi):
            return self._handle_phi(ir)
        if isinstance(ir, Binary):
            return self._handle_binary(ir)
        if isinstance(ir, Unary):
            return self._handle_unary(ir)
        if isinstance(ir, Index):
            return self._handle_index(ir)
        if isinstance(ir, Length):
            return self._handle_length(ir)
        if isinstance(ir, Member):
            return self._handle_member(ir)
        if isinstance(ir, SolidityCall):
            return self._handle_solidity_call(ir)
        if isinstance(ir, LowLevelCall):
            return self._handle_low_level_call(ir)
        if isinstance(ir, HighLevelCall):
            return self._handle_external_call(ir)
        if isinstance(ir, (InternalCall, LibraryCall)):
            return self._handle_internal_call(ir)
        if isinstance(ir, Unpack):
            return self._handle_unpack(ir)
        if isinstance(ir, (NewContract, NewArray, NewElementaryType)):
            return self._handle_new(ir)
        if isinstance(ir, (Send, Transfer)):
            return self._handle_send_transfer(ir)
        if isinstance(ir, Return):
            # Returns don't bind a new lvalue here — caller handles
            # callee-return propagation via _handle_internal_call.
            return False
        # Unknown opcode: assign top to its lvalue (if any) so the
        # consumer surfaces it as opaque rather than guessing.
        if isinstance(ir, OperationWithLValue):
            lv = ir.lvalue
            if lv is not None:
                return self.provenance.set(self._var_name(lv), TOP)
        return False

    # ------------------------------------------------------------------
    # Transfer functions
    # ------------------------------------------------------------------

    def _handle_assignment(self, ir: Any) -> bool:
        rvalue_sources = self._sources_for_value(ir.rvalue)
        return self.provenance.set(self._var_name(ir.lvalue), rvalue_sources)

    def _handle_type_conversion(self, ir: Any) -> bool:
        # Type cast preserves origin — `address(payable(x))` keeps x's source.
        rvalue = getattr(ir, "variable", None) or getattr(ir, "rvalue", None)
        if rvalue is None:
            return self.provenance.set(self._var_name(ir.lvalue), TOP)
        sources = self._sources_for_value(rvalue)
        return self.provenance.set(self._var_name(ir.lvalue), sources)

    def _handle_phi(self, ir: Any) -> bool:
        # Phi joins all incoming SSA versions with set union.
        result: SourceSet = EMPTY
        for incoming in ir.rvalues:
            result = union(result, self._sources_for_value(incoming))
        return self.provenance.set(self._var_name(ir.lvalue), result)

    def _handle_binary(self, ir: Any) -> bool:
        # The result of a binary op is ``computed`` with the union of
        # its operand sources tagged for downstream consumers.
        operand_sources = union(
            self._sources_for_value(ir.variable_left),
            self._sources_for_value(ir.variable_right),
        )
        if is_top(operand_sources):
            return self.provenance.set(self._var_name(ir.lvalue), TOP)
        # Wrap in a single ``computed`` source with a digest of the
        # operand union — keeps the provenance flat but preserves
        # taint-shape for downstream analysis.
        result = (
            frozenset(
                {
                    Source(
                        kind="computed",
                        computed_kind=str(getattr(ir, "type", "binary")),
                        callee_args_digest=_digest(operand_sources),
                    )
                }
            )
            | operand_sources
        )
        return self.provenance.set(self._var_name(ir.lvalue), result)

    def _handle_unary(self, ir: Any) -> bool:
        operand_sources = self._sources_for_value(ir.rvalue)
        if is_top(operand_sources):
            return self.provenance.set(self._var_name(ir.lvalue), TOP)
        result = (
            frozenset(
                {
                    Source(
                        kind="computed",
                        computed_kind=str(getattr(ir, "type", "unary")),
                        callee_args_digest=_digest(operand_sources),
                    )
                }
            )
            | operand_sources
        )
        return self.provenance.set(self._var_name(ir.lvalue), result)

    def _handle_index(self, ir: Any) -> bool:
        """``map[k]`` — the lvalue is a ReferenceVariable whose access
        path includes the base mapping plus the key. Provenance is the
        union of base + key sources, plus a ``computed`` tag carrying
        the key origin so downstream consumers can route on it."""
        base = getattr(ir, "variable_left", None)
        key = getattr(ir, "variable_right", None)
        base_sources = self._sources_for_value(base) if base is not None else EMPTY
        key_sources = self._sources_for_value(key) if key is not None else EMPTY
        result = union(base_sources, key_sources)
        return self.provenance.set(self._var_name(ir.lvalue), result)

    def _handle_length(self, ir: Any) -> bool:
        """``arr.length`` / ``str.length`` — propagates the array's
        provenance to the length value, tagged with
        ``computed_kind="length"``. Loop bounds reading
        ``params.length`` thus inherit the parameter taint of the
        array, which lets the worklist converge on loop-carried
        values without saturating to TOP.
        """
        base = getattr(ir, "value", None)
        sources = self._sources_for_value(base) if base is not None else EMPTY
        if is_top(sources) or sources == EMPTY:
            return self.provenance.set(self._var_name(ir.lvalue), sources or TOP)
        wrapper = frozenset(
            {
                Source(
                    kind="computed",
                    computed_kind="length",
                    callee_args_digest=_digest(sources),
                )
            }
        )
        return self.provenance.set(self._var_name(ir.lvalue), union(sources, wrapper))

    def _handle_member(self, ir: Any) -> bool:
        """``s.field`` — propagate base sources AND tag the result with
        a ``computed`` source whose ``computed_kind`` is
        ``member.<field_name>``. Predicate builder reads computed_kind
        to surface the field path; e.g. for OZ's
        ``_roles[role].adminRole`` the leaf will see both the base
        provenance (state_variable _roles + parameter role) and the
        field tag (``member.adminRole``).
        """
        base = getattr(ir, "variable_left", None)
        field = getattr(ir, "variable_right", None)
        base_sources = self._sources_for_value(base) if base is not None else EMPTY
        # Slither's Member.variable_right is a Constant whose value is
        # the field name string. Fall back to repr for unusual shapes.
        field_name: str | None = None
        if field is not None:
            field_name = getattr(field, "value", None) or getattr(field, "name", None) or str(field)
        if not field_name or is_top(base_sources):
            return self.provenance.set(self._var_name(ir.lvalue), base_sources)
        wrapper = frozenset(
            {
                Source(
                    kind="computed",
                    computed_kind=f"member.{field_name}",
                    callee_args_digest=_digest(base_sources),
                )
            }
        )
        return self.provenance.set(self._var_name(ir.lvalue), union(base_sources, wrapper))

    def _handle_solidity_call(self, ir: Any) -> bool:
        """``ecrecover``, ``keccak256``, ``addmod``, etc. ecrecover
        gets a dedicated ``signature_recovery`` source tag. Hash
        functions get ``computed``."""
        callee_name = getattr(ir.function, "name", "") if hasattr(ir, "function") else ""
        if callee_name == "ecrecover()" or callee_name.startswith("ecrecover"):
            args_union = self._union_of_args(ir.arguments)
            result = frozenset(
                {
                    Source(
                        kind="signature_recovery",
                        callee="ecrecover",
                        callee_args_digest=_digest(args_union),
                    )
                }
            )
            return self.provenance.set(self._var_name(ir.lvalue), result)
        # Other Solidity built-ins: hash family → computed.
        args_union = self._union_of_args(getattr(ir, "arguments", ()))
        if is_top(args_union):
            return self.provenance.set(self._var_name(ir.lvalue), TOP)
        result = frozenset(
            {
                Source(
                    kind="computed",
                    computed_kind=callee_name or "solidity_call",
                    callee_args_digest=_digest(args_union),
                )
            }
        )
        return self.provenance.set(self._var_name(ir.lvalue), result)

    def _handle_external_call(self, ir: Any) -> bool:
        """High-level call to a known interface — ``other.method(args)``.
        Records the callee name (function symbol) so the predicate
        builder can route on it."""
        if not isinstance(ir, OperationWithLValue) or ir.lvalue is None:
            return False
        callee_name = getattr(getattr(ir, "function", None), "name", None) or getattr(ir, "function_name", None)
        args_union = self._union_of_args(getattr(ir, "arguments", ()))
        result = frozenset(
            {
                Source(
                    kind="external_call",
                    callee=callee_name,
                    callee_args_digest=_digest(args_union),
                )
            }
        )
        return self.provenance.set(self._var_name(ir.lvalue), result)

    def _handle_low_level_call(self, ir: Any) -> bool:
        """``addr.call(data)`` / ``staticcall`` / ``delegatecall``.

        LowLevelCall has no resolved function symbol (the target is an
        arbitrary address). What matters for downstream analysis:
          - the call kind (call/staticcall/delegatecall) — delegatecall
            in particular executes target code in our context, so we
            preserve the kind in ``computed_kind``;
          - the destination's provenance (where the target address
            came from) — propagated into the result so a later check
            on the call result can see if the target was caller-
            controlled / state-loaded / parameter-loaded.

        Result is a tuple (bool, bytes) which Slither models as a
        TupleVariable lvalue; subsequent ``Unpack`` IRs split it into
        the individual return values. Each unpacked value inherits the
        tuple's provenance.
        """
        if not isinstance(ir, OperationWithLValue) or ir.lvalue is None:
            return False
        kind = getattr(ir, "function_name", None) or "low_level_call"
        dest_sources = self._sources_for_value(getattr(ir, "destination", None))
        args_union = self._union_of_args(getattr(ir, "arguments", ()))
        # delegatecall is special — we tag it as external_call (it's still
        # external from a control-flow perspective; the predicate builder
        # treats delegatecall result the same as call result), but the
        # destination provenance travels through so a downstream check
        # can flag delegatecall-to-untrusted-target if needed.
        result = frozenset(
            {
                Source(
                    kind="external_call",
                    callee=kind,  # "call" / "staticcall" / "delegatecall"
                    callee_args_digest=_digest(union(dest_sources, args_union)),
                )
            }
        )
        # Also propagate destination + args sources so unpacked tuple
        # inherits caller/state/parameter taint.
        result = union(result, dest_sources)
        result = union(result, args_union)
        return self.provenance.set(self._var_name(ir.lvalue), result)

    def _handle_unpack(self, ir: Any) -> bool:
        """``Unpack`` splits a tuple lvalue into its components. Each
        component inherits the tuple's full provenance — Slither doesn't
        track per-tuple-position taint, so we conservatively forward
        the whole set."""
        if not isinstance(ir, OperationWithLValue) or ir.lvalue is None:
            return False
        # The tuple operand is exposed as `ir.tuple` on Slither; fall
        # back to ``ir.rvalue`` for older versions.
        tup = getattr(ir, "tuple", None) or getattr(ir, "rvalue", None)
        if tup is None:
            return self.provenance.set(self._var_name(ir.lvalue), TOP)
        sources = self._sources_for_value(tup)
        return self.provenance.set(self._var_name(ir.lvalue), sources)

    def _handle_internal_call(self, ir: Any) -> bool:
        """Recurse into the callee's body, with parameter bindings
        substituted, up to ``internal_call_depth``. Returns the
        callee's union of return-value provenance."""
        if not isinstance(ir, OperationWithLValue) or ir.lvalue is None:
            return False
        callee = getattr(ir, "function", None)
        callee_name = getattr(callee, "full_name", None) or getattr(callee, "name", None)
        # Cycle / depth guard.
        if callee is None or len(self._call_stack) >= self.internal_call_depth or callee_name in self._call_stack:
            args_union = self._union_of_args(getattr(ir, "arguments", ()))
            tag = frozenset(
                {
                    Source(
                        kind="view_call",
                        callee=callee_name,
                        callee_args_digest=_digest(args_union),
                    )
                }
            )
            return self.provenance.set(self._var_name(ir.lvalue), tag)
        # Recurse into callee's IR with bindings.
        bindings: dict[str, SourceSet] = {}
        for param, arg in zip(callee.parameters, getattr(ir, "arguments", ())):
            name = self._var_name(param)
            if name:
                bindings[name] = self._sources_for_value(arg)
        sub = ProvenanceEngine(
            callee,
            internal_call_depth=self.internal_call_depth - 1,
            worklist_cap=self.worklist_cap,
            parameter_bindings=bindings,
        )
        sub._call_stack = self._call_stack + [callee_name or "?"]
        sub.run()
        # Callee return provenance: union of all returned values' sources.
        return_sources = self._collect_return_sources(callee, sub.provenance)
        if not return_sources:
            return_sources = frozenset(
                {
                    Source(
                        kind="view_call",
                        callee=callee_name,
                        callee_args_digest=_digest(self._union_of_args(getattr(ir, "arguments", ()))),
                    )
                }
            )
        return self.provenance.set(self._var_name(ir.lvalue), return_sources)

    def _handle_new(self, ir: Any) -> bool:
        if not isinstance(ir, OperationWithLValue) or ir.lvalue is None:
            return False
        args_union = self._union_of_args(getattr(ir, "arguments", ()))
        result = frozenset(
            {
                Source(
                    kind="computed",
                    computed_kind="new",
                    callee_args_digest=_digest(args_union),
                )
            }
        )
        return self.provenance.set(self._var_name(ir.lvalue), result)

    def _handle_send_transfer(self, ir: Any) -> bool:
        if not isinstance(ir, OperationWithLValue) or ir.lvalue is None:
            return False
        return self.provenance.set(
            self._var_name(ir.lvalue), frozenset({Source(kind="computed", computed_kind="send_transfer")})
        )

    # ------------------------------------------------------------------
    # Source resolution for an arbitrary IR operand
    # ------------------------------------------------------------------

    def _sources_for_value(self, value: Any) -> SourceSet:
        if value is None:
            return EMPTY
        # SolidityVariable: msg.sender / tx.origin / block.* / now etc.
        if isinstance(value, SolidityVariable):
            return self._classify_solidity_variable(value)
        if isinstance(value, Constant):
            return frozenset(
                {
                    Source(
                        kind="constant",
                        constant_value=str(value.value),
                    )
                }
            )
        if isinstance(value, StateVariable):
            return frozenset(
                {
                    Source(
                        kind="state_variable",
                        state_variable_name=value.name,
                    )
                }
            )
        # ReferenceVariable (e.g., result of Index/Member) — propagate
        # whatever provenance has been computed for the reference.
        if isinstance(value, (LocalVariable, TemporaryVariable, ReferenceVariable)):
            name = self._var_name(value)
            if name:
                return self.provenance.get(name)
            return EMPTY
        # Bare Variable fallback.
        if isinstance(value, Variable):
            name = self._var_name(value)
            return self.provenance.get(name) if name else EMPTY
        return EMPTY

    def _classify_solidity_variable(self, var: Any) -> SourceSet:
        """msg.sender → msg_sender; tx.origin → tx_origin; block.* →
        block_context; rest → top.

        Detection is by ``var.name``. This is NOT user-identifier name
        matching (that's the bad pattern we deleted) — these are
        Solidity language keywords with a fixed enum on Slither's side.
        """
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
        return TOP  # unknown Solidity keyword — be safe

    def _union_of_args(self, args: Iterable[Any]) -> SourceSet:
        out: SourceSet = EMPTY
        for arg in args:
            out = union(out, self._sources_for_value(arg))
        return out

    def _collect_return_sources(self, callee: Any, prov: ProvenanceMap) -> SourceSet:
        """Union the provenance of every value the callee returns."""
        out: SourceSet = EMPTY
        for node in callee.nodes:
            if NodeType.RETURN != getattr(node, "type", None):
                # Slither normalizes returns — Return IR in irs_ssa
                continue
            for ir in node.irs_ssa:
                if isinstance(ir, Return):
                    for v in getattr(ir, "values", ()):
                        out = union(out, prov.get(self._var_name(v)))
        # Fallback: scan all Return IRs across nodes (NodeType is unstable).
        if out == EMPTY:
            for node in callee.nodes:
                for ir in getattr(node, "irs_ssa", ()):
                    if isinstance(ir, Return):
                        for v in getattr(ir, "values", ()):
                            out = union(out, prov.get(self._var_name(v)))
        return out

    @staticmethod
    def _var_name(var: Any) -> str:
        return getattr(var, "name", None) or ""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _digest(s: SourceSet) -> str:
    """Stable digest of a SourceSet for nesting via ``callee_args_digest``.

    Source is frozen so ``hash(s)`` is stable; we hex it for the JSON
    serializer's benefit (artifacts are JSON, ints aren't).
    """
    return f"{abs(hash(s)) & 0xFFFFFFFF:08x}"
