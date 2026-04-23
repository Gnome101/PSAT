"""Universal caller-reach analysis.

One pass over slither IR that captures every way `msg.sender` reaches
a gating predicate. Emits `CallerSink` records in a closed vocabulary
(see `schemas.contract_analysis.CallerSink`). Replaces the eleven
pattern-specific guard matchers scattered across summaries.py and
graph.py with one analyzer — see `plans/...-tender-hippo.md` for the
architectural motivation.

The primitive this module adds that the existing detectors lack:
**proper forward taint on msg.sender**. Today's code checks whether an
argument's string name is in `{"msg.sender", "_msgSender()", ...}` —
which silently misses every call where slither has lowered msg.sender
into a TMP (`TMP_0 = msg.sender; X.call(TMP_0)`). The taint walker
here follows every assignment whose RHS references a tainted variable,
so the downstream classifiers can rely on the tainted set being
complete.

Phase 1 scope (this module, as shipped): msg.sender taint + two sink
kinds — `caller_equals` and `caller_in_mapping`. Later phases add
`caller_external_call`, `caller_internal_call`, `caller_signature`,
`caller_merkle`, and `caller_unknown`. All kinds share the taint
primitive, so adding each one is a self-contained classifier on top
of the same traversal.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from schemas.contract_analysis import CallerSink

from .shared import _node_contains_require_or_assert, _source_evidence

# Aliases slither gives back for msg.sender-ish values. Used to seed
# the taint set. Kept short on purpose — anything downstream that
# resolves to one of these in expression form gets tainted, which is
# the point of this analysis.
_MSG_SENDER_NAMES: frozenset[str] = frozenset(
    {
        "msg.sender",
        "msg.sender()",  # slither sometimes renders the access this way
        "_msgSender()",
        "_msgSender",
        "sender",  # fallback for contracts that alias msg.sender via `address sender = msg.sender`
    }
)

# Internal/library callees whose return value IS msg.sender — their
# `lvalue` must be tainted because the whole point of these helpers
# is to abstract the caller. OZ ERC-2771's `_msgSender()` is the
# canonical case: `address x = _msgSender()` makes `x` the caller,
# and the Binary check downstream reads `x == owner`.
_CALLEE_NAMES_RETURNING_CALLER: frozenset[str] = frozenset({"_msgSender", "_msgSender()", "msgSender", "msgSender()"})

# IR types whose presence inside a node's `irs` indicates a revert-on-
# mismatch context alongside `_node_contains_require_or_assert`. Handled
# separately because `if (cond) revert E()` lowers to a sequence of IR
# ops ending in a SolidityCall to `revert Error`, not a Require IR.
_REVERT_SOLIDITY_CALLS: frozenset[str] = frozenset({"revert()", "revert(string)", "revert Error()"})

# Signature-recovery entry points we recognize. All three reduce to
# "whoever holds the private key for the recovered address" — not a
# finite on-chain principal set, so the sink is terminal-non-enumerable.
_SIGNATURE_SOLIDITY_CALLS: frozenset[str] = frozenset({"ecrecover(bytes32,uint8,bytes32,bytes32)"})
_SIGNATURE_HELPER_METHODS: frozenset[str] = frozenset(
    {
        "isValidSignatureNow",  # OZ SignatureChecker
        "recover",  # OZ ECDSA.recover
        "tryRecover",
        "isValidERC1271SignatureNow",
    }
)

# Merkle-proof verify calls. Leaves live off-chain — we can record the
# root but never enumerate.
_MERKLE_HELPER_METHODS: frozenset[str] = frozenset(
    {
        "verify",  # OZ MerkleProof.verify(proof, root, leaf)
        "verifyCalldata",
        "multiProofVerify",
    }
)
_MERKLE_HELPER_LIBRARIES: frozenset[str] = frozenset({"MerkleProof"})


def _ir_name(ir: Any) -> str:
    """Class-name of a slither IR op, or '' if ir is None/invalid.

    Filters use `type(ir).__name__` throughout the existing pipeline
    (`graph.py:429`, `summaries.py:182`), so we stay consistent rather
    than importing the IR classes which would tie test mocks to
    slither's class hierarchy.
    """
    if ir is None:
        return ""
    return type(ir).__name__


def _var_name(item: Any) -> str:
    """Human-readable name for a slither variable/IR-LHS/RHS, or ''.

    Mirrors `graph.py:_variable_name` — we intentionally do not import
    it so this module stays self-contained and testable with minimal
    mocks. State variables, local variables, TMPs, and SolidityVariables
    all expose `.name`; calls without a name fall back to `str()`.
    """
    if item is None:
        return ""
    name = getattr(item, "name", None)
    if isinstance(name, str) and name:
        return name
    return str(item)


def _var_type(item: Any) -> str:
    if item is None:
        return ""
    return str(getattr(item, "type", "") or "")


def _is_state_variable(item: Any) -> bool:
    return _ir_name(item) == "StateVariable"


def _is_mapping_type(item: Any) -> bool:
    """Slither exposes mapping types with a `str()` that starts with
    `mapping(`. The type-class check would be tighter but couples us
    to slither's type module; string match is what `graph.py:
    _is_mapping_variable` already uses, so we stay consistent."""
    t = _var_type(item)
    return t.startswith("mapping(")


class MsgSenderTaint:
    """Forward taint set for msg.sender values inside one slither
    Function (or Modifier) body.

    Construction seeds the set with any variable whose `_var_name` is
    in `_MSG_SENDER_NAMES`, plus any extra `initial_tainted` names the
    caller passes in — used for cross-function taint propagation when
    a callee's parameter is bound to a caller-tainted argument.

    Then `propagate_through_node(node)` walks the node's IR ops once,
    extending the set via every assignment / type conversion / phi
    whose RHS touches a tainted variable. The traversal order matters
    only within a single basic block — across blocks, slither's IR
    already respects CFG order, so calling `propagate_through_node`
    on each node in order yields a fixed-point taint set without a
    separate worklist. Monotonic, sufficient for the guard-detection
    use case.
    """

    def __init__(self, initial_tainted: frozenset[str] | None = None) -> None:
        self._tainted: set[str] = set(_MSG_SENDER_NAMES)
        if initial_tainted:
            self._tainted.update(initial_tainted)

    def is_tainted(self, variable: Any) -> bool:
        return _var_name(variable) in self._tainted

    def any_tainted(self, variables: list[Any]) -> bool:
        return any(self.is_tainted(v) for v in variables)

    def add(self, variable: Any) -> None:
        name = _var_name(variable)
        if name:
            self._tainted.add(name)

    def snapshot(self) -> frozenset[str]:
        """Read-only view for tests / diagnostics."""
        return frozenset(self._tainted)

    def propagate_through_node(self, node: Any) -> None:
        """Walk a single CFG node's IRs, extending the taint set.

        Handled IR shapes — chosen to cover the common lowerings slither
        produces for `address caller = msg.sender; ...`:
        - **Assignment / TypeConversion**: LHS becomes tainted if RHS
          is tainted. Covers `TMP_0 = msg.sender`, `caller = msg.sender`.
        - **Phi**: LHS becomes tainted if any source is tainted. Covers
          SSA merges after if/else branches that assign msg.sender on
          one side.
        - **Index**: we do NOT propagate here; `mapping[msg.sender]`
          reads into an lvalue whose *value* is the mapping-value, not
          the caller. Index uses of tainted vars are classified at the
          sink level, not propagated further.
        - **HighLevelCall/InternalCall/LibraryCall**: do NOT propagate
          the lvalue — the return value isn't the caller. Arg passing
          across function boundaries is handled by recursing into the
          callee (future phase), not by forward-propagation here.
        """
        for ir in getattr(node, "irs", []) or []:
            kind = _ir_name(ir)
            if kind in ("Assignment", "TypeConversion"):
                rhs = getattr(ir, "rvalue", None)
                if rhs is not None and self.is_tainted(rhs):
                    self.add(getattr(ir, "lvalue", None))
            elif kind == "Phi":
                sources = list(getattr(ir, "rvalues", []) or [])
                if self.any_tainted(sources):
                    self.add(getattr(ir, "lvalue", None))
            elif kind in ("InternalCall", "LibraryCall", "SolidityCall"):
                # Taint the return value when the callee is a known
                # caller-returning helper (OZ ERC-2771 `_msgSender()`
                # and friends). Without this, `address x = _msgSender()`
                # leaves `x` untainted and the downstream Binary check
                # fails to fire.
                fn_ref = getattr(ir, "function_name", None) or getattr(ir, "function", None)
                callee_name = _var_name(fn_ref)
                if callee_name in _CALLEE_NAMES_RETURNING_CALLER:
                    self.add(getattr(ir, "lvalue", None))


def _ir_is_revert(ir: Any) -> bool:
    """Is `ir` a SolidityCall that triggers a revert (custom error
    or plain revert)?"""
    if _ir_name(ir) != "SolidityCall":
        return False
    function_ref = getattr(ir, "function", None)
    name = getattr(function_ref, "name", None) or str(function_ref or "")
    return name in _REVERT_SOLIDITY_CALLS or name.startswith("revert ")


def _node_is_revert_gate(node: Any) -> bool:
    """Does reaching this node's condition's `false` branch trigger a
    revert?

    Covers:
    - `require(cond)` / `assert(cond)`: slither marks the node with a
      `contains_require_or_assert` predicate we reuse from `shared.py`.
    - `if (!cond) revert E()` inline (IF and revert fused in one node).
    - `if (cond) revert E()` across nodes — slither lowers this into an
      IF node with the binary condition, whose `sons` successor contains
      the revert SolidityCall. Phase 6: walk direct successors (one hop)
      and check if any is a revert-terminating branch.

    Returning False flips `revert_on_mismatch=False` on the emitted
    sink so downstream can tell "observed msg.sender read" from "gating
    check". Same-node revert wins over cross-node — cheaper to detect
    and covers the most common shapes.
    """
    if _node_contains_require_or_assert(node):
        return True
    for ir in getattr(node, "irs", []) or []:
        if _ir_is_revert(ir):
            return True
    # Cross-node: slither splits `if (bad) revert E()` into an IF node
    # containing the Binary + a successor EXPRESSION node containing the
    # SolidityCall. One hop is enough — deeper control flow is rare for
    # gating checks and would need a dedicated reachability pass.
    for son in getattr(node, "sons", []) or []:
        for ir in getattr(son, "irs", []) or []:
            if _ir_is_revert(ir):
                return True
    return False


def _resolve_tmp_to_state_var(tmp_var: Any, recent_irs: list[Any]) -> tuple[str, str]:
    """If `tmp_var` was the lvalue of an InternalCall/HighLevelCall
    whose callee is a single-state-variable getter (`return _owner;`),
    return (state_var_name, type). Otherwise returns ('', '').

    The OZ Ownable pattern is the canonical case: the binary
    `owner() == _msgSender()` has `owner()` as a TMP in slither IR,
    but underneath `owner()` is `return _owner;`. We walk the callee's
    `state_variables_read` (slither API) and if exactly one state
    variable is read across the whole callee, use it as the target.
    That narrowness avoids false matches on getters that combine
    multiple state reads.
    """
    tmp_name = _var_name(tmp_var)
    if not tmp_name:
        return "", ""
    for ir in recent_irs:
        if _ir_name(ir) not in ("InternalCall", "HighLevelCall", "LibraryCall"):
            continue
        if _var_name(getattr(ir, "lvalue", None)) != tmp_name:
            continue
        callee = getattr(ir, "function", None)
        if callee is None:
            continue
        reads: set[str] = set()
        read_type: str = ""
        for inner_node in getattr(callee, "nodes", []) or []:
            for var in getattr(inner_node, "state_variables_read", []) or []:
                name = _var_name(var)
                if name:
                    reads.add(name)
                    read_type = _var_type(var) or read_type
        if len(reads) == 1:
            return next(iter(reads)), read_type
    return "", ""


def _resolve_ref_to_struct_field(ref_var: Any, recent_irs: list[Any]) -> tuple[str, str]:
    """If `ref_var` is the lvalue of a preceding Member IR (struct
    field access like `contracts.accountingOracle`), return
    (`<base>.<field>`, base_type_or_empty). Otherwise ('', '').

    Phase 6: accounting_proxy and friends gate functions with
    `msg.sender != contracts.oracle`. Slither lowers `contracts.oracle`
    into a Member IR producing a REF_N lvalue that the Binary compares
    against. We rebuild the dotted name so the downstream resolver can
    RPC-read the struct field on-chain.
    """
    ref_name = _var_name(ref_var)
    if not ref_name:
        return "", ""
    for ir in recent_irs:
        if _ir_name(ir) != "Member":
            continue
        if _var_name(getattr(ir, "lvalue", None)) != ref_name:
            continue
        base = getattr(ir, "variable_left", None)
        field = getattr(ir, "variable_right", None)
        base_name = _var_name(base)
        field_name = _var_name(field)
        if base_name and field_name:
            return f"{base_name}.{field_name}", _var_type(base)
    return "", ""


def _classify_caller_equals(node: Any, taint: MsgSenderTaint, project_dir: Path) -> list[CallerSink]:
    """Find `msg.sender == X` / `X == msg.sender` comparisons in this node.

    Emits `caller_equals` sinks. The counterparty `X` can be a state
    variable (most common — `msg.sender == owner`), a StateVariable
    read through an intermediate TMP (slither lowering — we rely on
    `_ir_name(...) == "StateVariable"` for direct hits, otherwise the
    classifier stores the name without type), or a compile-time
    constant. Anything else is silently skipped here and will
    (eventually, in a later phase) surface as `caller_unknown`.
    """
    sinks: list[CallerSink] = []
    revert_on_mismatch = _node_is_revert_gate(node)
    evidence = _source_evidence(node, project_dir, detail=f"caller_equals node {node.node_id}")
    node_irs = list(getattr(node, "irs", []) or [])
    for ir in node_irs:
        if _ir_name(ir) != "Binary":
            continue
        left = getattr(ir, "variable_left", None)
        right = getattr(ir, "variable_right", None)
        # We accept either side; the counterparty is whichever is NOT
        # tainted. Binary ops between two tainted vars don't happen in
        # practice (nothing compares msg.sender to msg.sender), so
        # skipping that edge case keeps the classifier tight.
        left_tainted = taint.is_tainted(left)
        right_tainted = taint.is_tainted(right)
        if left_tainted == right_tainted:
            continue
        other = right if left_tainted else left
        sink: CallerSink = {
            "kind": "caller_equals",
            "evidence": evidence,
            "revert_on_mismatch": revert_on_mismatch,
        }
        if _is_state_variable(other):
            sink["target_state_var"] = _var_name(other)
            sink["target_type"] = _var_type(other)
        else:
            value = getattr(other, "value", None)
            if value is not None:
                # Compile-time constant counterparty.
                sink["constant_value"] = str(value)
            else:
                # Phase 6: struct-field access (`contracts.oracle`)
                # resolves to `<base>.<field>` — caught before the
                # single-state-var-getter path because the Member IR is
                # more specific than a generic TMP.
                struct_name, struct_type = _resolve_ref_to_struct_field(other, node_irs)
                if struct_name:
                    sink["target_state_var"] = struct_name
                    if struct_type:
                        sink["target_type"] = struct_type
                else:
                    # Local/TMP counterparty. If it's the lvalue of a
                    # preceding InternalCall to a single-state-var getter
                    # (`owner()` returning `_owner`), resolve to that
                    # state var — otherwise we'd emit a useless target
                    # like "TMP_1".
                    resolved_name, resolved_type = _resolve_tmp_to_state_var(other, node_irs)
                    if resolved_name:
                        sink["target_state_var"] = resolved_name
                        if resolved_type:
                            sink["target_type"] = resolved_type
                    else:
                        sink["target_state_var"] = _var_name(other)
        sinks.append(sink)
    return sinks


# UPPER_SNAKE regex for role_args recovery. Same heuristic surface the
# rest of the pipeline uses (`shared._AUTHISH_ROLE_IDENTIFIER_KEYWORDS`),
# kept name-based for now — replacing with slither type info (bytes32
# public constant) is Phase 6+ per the plan.
def _is_role_constant_name(name: str) -> bool:
    # Import locally to avoid a top-level dep cycle — summaries.py
    # already imports from this module's sibling shared.py.
    from .shared import _looks_like_role_identifier_name

    return bool(name) and _looks_like_role_identifier_name(name)


def _classify_caller_external_call(node: Any, taint: MsgSenderTaint, project_dir: Path) -> list[CallerSink]:
    """`X.method(..., msg.sender, ...)` — caller flows into an
    external contract call on a state variable.

    Subsumes the existing `ExternalCallGuard` shape. Emits one record
    per matching HighLevelCall. The target state variable is extracted
    from the IR's `destination`; anything where the destination isn't
    a StateVariable gets skipped because the cross-contract join has
    no handle to resolve it (consistent with `graph.py:_resolve_target`).

    `external_role_args` captures any non-sender argument whose name
    matches the role-identifier heuristic — covers Pattern B
    (`hasRole(PROTOCOL_PAUSER, msg.sender)`) semantically, though
    still via the keyword list for now.
    """
    sinks: list[CallerSink] = []
    revert_on_mismatch = _node_is_revert_gate(node)
    evidence = _source_evidence(node, project_dir, detail=f"caller_external_call node {node.node_id}")
    for ir in getattr(node, "irs", []) or []:
        if _ir_name(ir) != "HighLevelCall":
            continue
        destination = getattr(ir, "destination", None)
        if not _is_state_variable(destination):
            continue
        arguments = list(getattr(ir, "arguments", []) or [])
        if not any(taint.is_tainted(arg) for arg in arguments):
            continue
        function_ref = getattr(ir, "function_name", None) or getattr(ir, "function", None)
        method = _var_name(function_ref) if function_ref is not None else ""
        if not method:
            continue
        role_args: list[str] = []
        for arg in arguments:
            if taint.is_tainted(arg):
                continue
            arg_name = _var_name(arg)
            if _is_role_constant_name(arg_name) and arg_name not in role_args:
                role_args.append(arg_name)
        sink: CallerSink = {
            "kind": "caller_external_call",
            "evidence": evidence,
            "revert_on_mismatch": revert_on_mismatch,
            "external_target_state_var": _var_name(destination),
            "target_type": _var_type(destination),
            "external_method": method,
        }
        if role_args:
            sink["external_role_args"] = role_args
        sinks.append(sink)
    return sinks


def _classify_caller_internal_call(node: Any, taint: MsgSenderTaint, project_dir: Path) -> list[CallerSink]:
    """`_helper(..., msg.sender, ...)` — caller flows into an internal
    or library call on this contract (or an inherited one).

    Records the callee name so the downstream join can decide whether
    to follow. For Phase 1 we do NOT recurse into the callee's body
    to propagate taint cross-function — that's a Phase 2 enhancement
    once we have a cross-function walker. Emitting this sink today is
    still useful: the policy stage can inspect what the callee does
    and attribute guards via the existing `_guards_from_internal_call`
    path while we migrate.
    """
    sinks: list[CallerSink] = []
    revert_on_mismatch = _node_is_revert_gate(node)
    evidence = _source_evidence(node, project_dir, detail=f"caller_internal_call node {node.node_id}")
    for ir in getattr(node, "irs", []) or []:
        kind = _ir_name(ir)
        if kind not in ("InternalCall", "LibraryCall"):
            continue
        arguments = list(getattr(ir, "arguments", []) or [])
        if not any(taint.is_tainted(arg) for arg in arguments):
            continue
        callee = getattr(ir, "function", None)
        callee_name = _var_name(callee)
        if not callee_name:
            continue
        sinks.append(
            {
                "kind": "caller_internal_call",
                "evidence": evidence,
                "revert_on_mismatch": revert_on_mismatch,
                "internal_callee": callee_name,
            }
        )
    return sinks


def _classify_caller_in_mapping(node: Any, taint: MsgSenderTaint, project_dir: Path) -> list[CallerSink]:
    """Find `mapping[msg.sender] <predicate>` patterns.

    Slither lowers `require(wards[msg.sender] == 1)` into:
    - a state-variable read of `wards`
    - an Index IR: `TMP = wards[msg.sender]`
    - a Binary IR: `TMP == 1`
    - a SolidityCall to require(...) if that Binary is the condition

    We match the Index IR whose second operand is tainted, and look at
    the enclosing predicate to fill `mapping_predicate`. For the first
    pass we record the predicate as the literal Binary shape on the
    Index's lvalue; a more precise "what does it compare to" can be
    layered on later.
    """
    sinks: list[CallerSink] = []
    revert_on_mismatch = _node_is_revert_gate(node)
    evidence = _source_evidence(node, project_dir, detail=f"caller_in_mapping node {node.node_id}")
    # Collect Index IRs first so we can link their lvalues to the
    # predicate that gates them in the same node.
    index_lvalues: dict[str, tuple[Any, str]] = {}
    for ir in getattr(node, "irs", []) or []:
        if _ir_name(ir) != "Index":
            continue
        key = getattr(ir, "variable_right", None)
        base = getattr(ir, "variable_left", None)
        if not taint.is_tainted(key):
            continue
        if not _is_mapping_type(base):
            continue
        index_lvalues[_var_name(getattr(ir, "lvalue", None))] = (base, _var_name(base))
    if not index_lvalues:
        return sinks
    # Now find Binary ops whose operand is one of the tainted-index
    # lvalues — this captures `TMP == 1`, `TMP > 0`, etc. When the
    # binary isn't present (the mapping-read value flows straight into
    # a bool context), we still emit with an empty predicate; the sink
    # exists, just without the comparator spelled out.
    predicates_by_mapping: dict[str, str] = {}
    for ir in getattr(node, "irs", []) or []:
        if _ir_name(ir) != "Binary":
            continue
        left_name = _var_name(getattr(ir, "variable_left", None))
        right_name = _var_name(getattr(ir, "variable_right", None))
        op = getattr(ir, "type", None) or getattr(ir, "type_str", None) or "=="
        if left_name in index_lvalues:
            _base, mapping_name = index_lvalues[left_name]
            predicates_by_mapping[mapping_name] = f"{op} {right_name}"
        elif right_name in index_lvalues:
            _base, mapping_name = index_lvalues[right_name]
            predicates_by_mapping[mapping_name] = f"{left_name} {op}"
    for _lv, (_base, mapping_name) in index_lvalues.items():
        sink: CallerSink = {
            "kind": "caller_in_mapping",
            "evidence": evidence,
            "revert_on_mismatch": revert_on_mismatch,
            "mapping_name": mapping_name,
        }
        predicate = predicates_by_mapping.get(mapping_name)
        if predicate:
            sink["mapping_predicate"] = predicate
        sinks.append(sink)
    return sinks


def _classify_caller_signature(node: Any, taint: MsgSenderTaint, project_dir: Path) -> list[CallerSink]:
    """`ecrecover(...)` or OZ-family signature helpers where the
    recovered address is later compared to msg.sender.

    Principle: signature auth's principal set is "holder of the signer
    private key" — not enumerable from on-chain state. We emit a
    terminal-non-enumerable sink, which the resolver will render as
    `resolved_type="off_chain_witness"` instead of trying to resolve.

    We record `signature_source_var` as the state variable that holds
    the expected signer (when identifiable) so the UI can link to it.
    """
    sinks: list[CallerSink] = []
    revert_on_mismatch = _node_is_revert_gate(node)
    evidence = _source_evidence(node, project_dir, detail=f"caller_signature node {node.node_id}")
    # ecrecover in SolidityCall form
    for ir in getattr(node, "irs", []) or []:
        kind = _ir_name(ir)
        if kind == "SolidityCall":
            fn_ref = getattr(ir, "function", None)
            fn_name = _var_name(fn_ref)
            if fn_name in _SIGNATURE_SOLIDITY_CALLS:
                sinks.append(
                    {
                        "kind": "caller_signature",
                        "evidence": evidence,
                        "revert_on_mismatch": revert_on_mismatch,
                        "signature_source_var": "ecrecover",
                    }
                )
            continue
        if kind not in ("HighLevelCall", "LibraryCall", "InternalCall"):
            continue
        fn_ref = getattr(ir, "function_name", None) or getattr(ir, "function", None)
        fn_name = _var_name(fn_ref)
        if fn_name not in _SIGNATURE_HELPER_METHODS:
            continue
        # Record the destination (library / helper contract name) so the
        # resolver has something to link to. For OZ SignatureChecker
        # called on a state var, that's the signer slot.
        destination = getattr(ir, "destination", None)
        source_name = _var_name(destination) if destination is not None else fn_name
        sinks.append(
            {
                "kind": "caller_signature",
                "evidence": evidence,
                "revert_on_mismatch": revert_on_mismatch,
                "signature_source_var": source_name or fn_name,
            }
        )
    return sinks


def _classify_caller_merkle(node: Any, taint: MsgSenderTaint, project_dir: Path) -> list[CallerSink]:
    """`MerkleProof.verify(proof, root, leaf)` where the leaf is
    derived from msg.sender — typical airdrop/allowlist drop pattern.

    Again non-enumerable: leaves live off-chain. We record the `root`
    state variable (third arg in OZ's signature, often state-stored)
    so the UI can at least point at the on-chain commitment.
    """
    sinks: list[CallerSink] = []
    revert_on_mismatch = _node_is_revert_gate(node)
    evidence = _source_evidence(node, project_dir, detail=f"caller_merkle node {node.node_id}")
    for ir in getattr(node, "irs", []) or []:
        kind = _ir_name(ir)
        if kind not in ("LibraryCall", "InternalCall", "HighLevelCall"):
            continue
        fn_ref = getattr(ir, "function_name", None) or getattr(ir, "function", None)
        fn_name = _var_name(fn_ref)
        if fn_name not in _MERKLE_HELPER_METHODS:
            continue
        # Only match when the library/contract is MerkleProof-shaped.
        destination = getattr(ir, "destination", None)
        lib_name = _var_name(destination) if destination is not None else ""
        contract_of_fn = getattr(getattr(fn_ref, "contract", None), "name", "") if fn_ref is not None else ""
        if lib_name not in _MERKLE_HELPER_LIBRARIES and contract_of_fn not in _MERKLE_HELPER_LIBRARIES:
            continue
        arguments = list(getattr(ir, "arguments", []) or [])
        # Require that at least one argument is caller-derived — merkle
        # verify calls on arbitrary data aren't caller auth.
        if not any(taint.is_tainted(arg) for arg in arguments):
            continue
        # Root is typically the second arg in OZ's verify(proof, root, leaf).
        root_name = _var_name(arguments[1]) if len(arguments) >= 2 else ""
        sinks.append(
            {
                "kind": "caller_merkle",
                "evidence": evidence,
                "revert_on_mismatch": revert_on_mismatch,
                "merkle_root_var": root_name or fn_name,
            }
        )
    return sinks


def _classify_caller_unknown(
    node: Any,
    taint: MsgSenderTaint,
    project_dir: Path,
    already_emitted: bool,
) -> list[CallerSink]:
    """Escape hatch: the node gates on something involving msg.sender
    but none of the structured classifiers matched. Emit a record with
    evidence so operators can see WHERE we lost classification without
    silently missing the guard.

    Trigger conditions:
    - `revert_on_mismatch` is True (the node is a gating context)
    - msg.sender appears SOMEWHERE in the node (tainted var read as an
      expression child of a Binary, a Call arg, etc.)
    - No other classifier emitted for this node (`already_emitted=False`)

    This is what turns the previous silent-miss behavior into an
    explicit "we see the guard exists, we can't name it" signal.
    """
    if already_emitted:
        return []
    if not _node_is_revert_gate(node):
        return []
    # Does any IR in this node reference a tainted variable?
    for ir in getattr(node, "irs", []) or []:
        # Scan every attribute that slither typically uses for operands
        # — this is intentionally broad because the whole point of
        # "unknown" is we don't know the shape.
        for attr in ("variable_left", "variable_right", "variable", "rvalue", "lvalue"):
            if taint.is_tainted(getattr(ir, attr, None)):
                return [
                    {
                        "kind": "caller_unknown",
                        "evidence": _source_evidence(node, project_dir, detail=f"caller_unknown node {node.node_id}"),
                        "revert_on_mismatch": True,
                    }
                ]
        for arg in getattr(ir, "arguments", []) or []:
            if taint.is_tainted(arg):
                return [
                    {
                        "kind": "caller_unknown",
                        "evidence": _source_evidence(node, project_dir, detail=f"caller_unknown node {node.node_id}"),
                        "revert_on_mismatch": True,
                    }
                ]
    return []


# Maximum depth of cross-function recursion when following internal
# calls into their callees. 3 is enough to unwind OZ's `onlyOwner ->
# _checkOwner -> _msgSender -> owner() == _msgSender` chain; deeper
# would just blow compile time on recursive libraries without
# adding real guard coverage.
_MAX_CROSS_FUNCTION_DEPTH = 3


def _caller_tainted_param_names(
    ir: Any,
    callee: Any,
    taint: MsgSenderTaint,
) -> frozenset[str]:
    """Given an InternalCall IR whose `.arguments` may include tainted
    vars, return the set of parameter names in the CALLEE that should
    start as tainted.

    Positional binding: the i-th argument binds to the i-th parameter.
    Extra arguments or parameter mismatches are ignored — slither IR
    preserves parameter lists on the Function object.
    """
    args = list(getattr(ir, "arguments", []) or [])
    params = list(getattr(callee, "parameters", []) or [])
    tainted: set[str] = set()
    for i, arg in enumerate(args):
        if i >= len(params):
            break
        if taint.is_tainted(arg):
            name = _var_name(params[i])
            if name:
                tainted.add(name)
    return frozenset(tainted)


def sinks_to_external_call_guards(sinks: list[CallerSink]) -> list[dict]:
    """Project `CallerSink` records back into the legacy
    `ExternalCallGuard` shape used by `PrivilegedFunction` consumers.

    Exists so we can keep the legacy `external_call_guards` field
    populated as a derived view during the retirement of the parallel
    detector — the policy worker's v5 bridge reads this shape, and
    downstream consumers shouldn't notice the swap until Phase 4
    replaces the bridge itself.

    Only emits entries for `caller_external_call` sinks, which is the
    one-to-one correspondence: the other sink kinds live in
    `PrivilegedFunction.sinks` only and weren't visible to the legacy
    external-call-guard path.
    """
    out: list[dict] = []
    for sink in sinks:
        if sink.get("kind") != "caller_external_call":
            continue
        target_var = sink.get("external_target_state_var", "") or ""
        method = sink.get("external_method", "") or ""
        if not target_var or not method:
            continue
        record: dict = {
            # Legacy ExternalCallGuard had `kind` = "modifier" | "inline".
            # Sinks don't carry that explicitly, but we can infer from
            # the evidence detail when present. Default to "inline" —
            # modifier-vs-inline mattered for the old bridge but the
            # unified Phase 4 bridge doesn't care.
            "kind": "inline",
            "target_state_var": target_var,
            "target_type": sink.get("target_type", "") or "",
            "method": method,
            "sender_in_args": True,  # caller_external_call only emitted when msg.sender flowed in
        }
        role_args = list(sink.get("external_role_args") or [])
        if role_args:
            record["role_args"] = role_args
        out.append(record)
    return out


def caller_reach_analysis(
    function: Any,
    project_dir: Path,
    *,
    _depth: int = 0,
    _seen: set[int] | None = None,
    _initial_tainted: frozenset[str] | None = None,
) -> list[CallerSink]:
    """Walk a function's IR and emit every point where msg.sender
    reaches a gating predicate.

    Returns sinks in the order encountered across the function's CFG
    nodes + every modifier body + every internal/library call the
    function makes (up to `_MAX_CROSS_FUNCTION_DEPTH`).

    The cross-function recursion is what lets us resolve patterns
    like OZ's `onlyOwner` modifier — the modifier body is just
    `_checkOwner(); _;`, the real `msg.sender == owner` check lives
    inside `_checkOwner`. Without recursion we'd silently miss every
    function gated by an Ownable-style helper.

    Cycle detection: we track callee Function object ids in `_seen`
    so mutually-recursive helpers (rare but they exist) don't blow
    the stack. Depth cap backs it up.
    """
    sinks: list[CallerSink] = []
    seen = _seen if _seen is not None else set()

    def _walk_body(body_function: Any) -> None:
        taint = MsgSenderTaint(initial_tainted=_initial_tainted)
        for node in getattr(body_function, "nodes", []) or []:
            # Propagate taint BEFORE classifying — a node that does
            # `TMP = msg.sender; require(X == TMP)` needs TMP tainted
            # before the Binary check runs.
            taint.propagate_through_node(node)
            before_len = len(sinks)
            sinks.extend(_classify_caller_equals(node, taint, project_dir))
            sinks.extend(_classify_caller_in_mapping(node, taint, project_dir))
            sinks.extend(_classify_caller_external_call(node, taint, project_dir))
            sinks.extend(_classify_caller_internal_call(node, taint, project_dir))
            sinks.extend(_classify_caller_signature(node, taint, project_dir))
            sinks.extend(_classify_caller_merkle(node, taint, project_dir))
            # Cross-function recursion: for every internal/library
            # call, walk into the callee with the right taint seed
            # so any guards discovered inside count as guards on
            # the outer function.
            if _depth < _MAX_CROSS_FUNCTION_DEPTH:
                for ir in getattr(node, "irs", []) or []:
                    kind = _ir_name(ir)
                    if kind not in ("InternalCall", "LibraryCall"):
                        continue
                    callee = getattr(ir, "function", None)
                    if callee is None:
                        continue
                    callee_id = id(callee)
                    if callee_id in seen:
                        continue
                    # NB: copy seen before recursing so parallel callees
                    # at the same depth don't block each other.
                    next_seen = seen | {callee_id}
                    initial_for_callee = _caller_tainted_param_names(ir, callee, taint)
                    sinks.extend(
                        caller_reach_analysis(
                            callee,
                            project_dir,
                            _depth=_depth + 1,
                            _seen=next_seen,
                            _initial_tainted=initial_for_callee,
                        )
                    )
            # Unknown-fallback ONLY when none of the structured
            # classifiers fired for this node — otherwise the guard
            # is already named and we'd double-emit.
            already_emitted = len(sinks) > before_len
            sinks.extend(_classify_caller_unknown(node, taint, project_dir, already_emitted))

    _walk_body(function)
    for modifier in getattr(function, "modifiers", []) or []:
        _walk_body(modifier)
    return sinks
