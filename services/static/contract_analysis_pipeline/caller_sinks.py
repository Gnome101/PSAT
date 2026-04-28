"""Forward-taint analysis over slither IR that emits `CallerSink` records
for every way `msg.sender` reaches a gating predicate.

The taint walker follows assignments whose RHS references a tainted
variable, so TMP-lowered `msg.sender` (e.g. `TMP_0 = msg.sender;
X.call(TMP_0)`) is captured — plain name-matching misses that case.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from schemas.contract_analysis import CallerSink

from .shared import _node_contains_require_or_assert, _source_evidence

# Initial taint seed. Aliases slither renders for msg.sender-equivalents.
_MSG_SENDER_NAMES: frozenset[str] = frozenset(
    {
        "msg.sender",
        "msg.sender()",
        "_msgSender()",
        "_msgSender",
        "sender",
    }
)

# Helpers whose return value is msg.sender (ERC-2771 style): their lvalue
# must be tainted so `address x = _msgSender(); require(x == owner)` gets caught.
_CALLEE_NAMES_RETURNING_CALLER: frozenset[str] = frozenset({"_msgSender", "_msgSender()", "msgSender", "msgSender()"})

# `if (cond) revert E()` lowers to a SolidityCall, not a Require IR, so we
# detect revert contexts by scanning for these calls separately.
_REVERT_SOLIDITY_CALLS: frozenset[str] = frozenset({"revert()", "revert(string)", "revert Error()"})

_SIGNATURE_SOLIDITY_CALLS: frozenset[str] = frozenset({"ecrecover(bytes32,uint8,bytes32,bytes32)"})
_SIGNATURE_HELPER_METHODS: frozenset[str] = frozenset(
    {
        "isValidSignatureNow",  # OZ SignatureChecker
        "recover",  # OZ ECDSA.recover
        "tryRecover",
        "isValidERC1271SignatureNow",
    }
)

_MERKLE_HELPER_METHODS: frozenset[str] = frozenset(
    {
        "verify",
        "verifyCalldata",
        "multiProofVerify",
    }
)
_MERKLE_HELPER_LIBRARIES: frozenset[str] = frozenset({"MerkleProof"})


def _ir_name(ir: Any) -> str:
    if ir is None:
        return ""
    return type(ir).__name__


def _var_name(item: Any) -> str:
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
    return _var_type(item).startswith("mapping(")


class MsgSenderTaint:
    """Forward taint set for msg.sender-equivalent values in one function body.

    Monotonic single-pass taint: extend `_tainted` via every assignment whose
    RHS touches a tainted var, in slither's CFG order. Sufficient for
    guard detection; cross-function propagation is handled by the caller
    via `initial_tainted`.
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
        return frozenset(self._tainted)

    def propagate_through_node(self, node: Any) -> None:
        for ir in getattr(node, "irs", []) or []:
            kind = _ir_name(ir)
            if kind == "Assignment":
                rhs = getattr(ir, "rvalue", None)
                if rhs is not None and self.is_tainted(rhs):
                    self.add(getattr(ir, "lvalue", None))
            elif kind == "TypeConversion":
                rhs = getattr(ir, "variable", None) or getattr(ir, "rvalue", None)
                if rhs is not None and self.is_tainted(rhs):
                    self.add(getattr(ir, "lvalue", None))
            elif kind == "Phi":
                sources = list(getattr(ir, "rvalues", []) or [])
                if self.any_tainted(sources):
                    self.add(getattr(ir, "lvalue", None))
            elif kind in ("InternalCall", "LibraryCall", "SolidityCall"):
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
    """Resolve a TMP bound to a single-state-variable getter (e.g. OZ
    `owner() { return _owner; }`) to that state variable's (name, type).
    Returns ('', '') if the callee reads zero or multiple state vars."""
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
    """Resolve a REF from a preceding Member IR (struct field access like
    `contracts.oracle`) to (`<base>.<field>`, base_type), or ('', '')."""
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
    """Emit a `caller_equals` sink for every `msg.sender == X` Binary
    comparison (or mirrored), where X is a state variable, TMP-lowered
    getter, or constant."""
    sinks: list[CallerSink] = []
    revert_on_mismatch = _node_is_revert_gate(node)
    evidence = _source_evidence(node, project_dir, detail=f"caller_equals node {node.node_id}")
    node_irs = list(getattr(node, "irs", []) or [])
    for ir in node_irs:
        if _ir_name(ir) != "Binary":
            continue
        left = getattr(ir, "variable_left", None)
        right = getattr(ir, "variable_right", None)
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
                sink["constant_value"] = str(value)
            else:
                # Struct-field (`contracts.oracle`) takes precedence over
                # the single-state-var-getter fallback — Member IR is more
                # specific than a generic TMP.
                struct_name, struct_type = _resolve_ref_to_struct_field(other, node_irs)
                if struct_name:
                    sink["target_state_var"] = struct_name
                    if struct_type:
                        sink["target_type"] = struct_type
                else:
                    resolved_name, resolved_type = _resolve_tmp_to_state_var(other, node_irs)
                    if resolved_name:
                        sink["target_state_var"] = resolved_name
                        if resolved_type:
                            sink["target_type"] = resolved_type
                    else:
                        sink["target_state_var"] = _var_name(other)
        sinks.append(sink)
    return sinks


def _is_role_constant_name(name: str) -> bool:
    from .shared import _looks_like_role_identifier_name

    return bool(name) and _looks_like_role_identifier_name(name)


def _classify_caller_external_call(node: Any, taint: MsgSenderTaint, project_dir: Path) -> list[CallerSink]:
    """Emit `caller_external_call` for `X.method(..., msg.sender, ...)` where
    X is a state variable. Captures `external_role_args` for any non-sender
    argument matching the role-identifier name heuristic (Pattern B:
    `hasRole(ROLE, msg.sender)`)."""
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
    """Emit `caller_internal_call` for `_helper(..., msg.sender, ...)`.
    Records only the callee name — cross-function taint propagation
    happens at the caller of `caller_reach_analysis`."""
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
    """Emit `caller_in_mapping` for `mapping[msg.sender]` reads, filling
    `mapping_predicate` from the Binary that consumes the Index lvalue
    (e.g. `TMP == 1` for `wards[msg.sender] == 1`)."""
    sinks: list[CallerSink] = []
    revert_on_mismatch = _node_is_revert_gate(node)
    evidence = _source_evidence(node, project_dir, detail=f"caller_in_mapping node {node.node_id}")
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
    """Emit `caller_signature` for ecrecover / OZ signature-helper calls.
    The principal is the private-key holder — not enumerable from on-chain
    state — so downstream renders this as `off_chain_witness`."""
    sinks: list[CallerSink] = []
    revert_on_mismatch = _node_is_revert_gate(node)
    evidence = _source_evidence(node, project_dir, detail=f"caller_signature node {node.node_id}")
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
    """Emit `caller_merkle` for `MerkleProof.verify(proof, root, leaf)` with
    a caller-derived leaf. Records `merkle_root_var` so the UI can point
    at the on-chain commitment."""
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
        destination = getattr(ir, "destination", None)
        lib_name = _var_name(destination) if destination is not None else ""
        contract_of_fn = getattr(getattr(fn_ref, "contract", None), "name", "") if fn_ref is not None else ""
        if lib_name not in _MERKLE_HELPER_LIBRARIES and contract_of_fn not in _MERKLE_HELPER_LIBRARIES:
            continue
        arguments = list(getattr(ir, "arguments", []) or [])
        if not any(taint.is_tainted(arg) for arg in arguments):
            continue
        # OZ verify(proof, root, leaf) — root is arg index 1.
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
    """Emit `caller_unknown` when the node is a revert-gate that references
    a tainted variable but no structured classifier fired — turns a silent
    miss into an explicit "guard exists, shape unrecognized"."""
    if already_emitted:
        return []
    if not _node_is_revert_gate(node):
        return []
    for ir in getattr(node, "irs", []) or []:
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


# Depth 3 unwinds OZ `onlyOwner -> _checkOwner -> _msgSender -> owner() == _msgSender`.
_MAX_CROSS_FUNCTION_DEPTH = 3


def _caller_tainted_param_names(
    ir: Any,
    callee: Any,
    taint: MsgSenderTaint,
) -> frozenset[str]:
    """Return the callee parameter names that bind (positionally) to
    tainted arguments at this call site."""
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
    """Project `caller_external_call` sinks into the legacy
    `ExternalCallGuard` shape for `PrivilegedFunction` consumers."""
    out: list[dict] = []
    for sink in sinks:
        if sink.get("kind") != "caller_external_call":
            continue
        if not sink.get("revert_on_mismatch"):
            continue
        target_var = sink.get("external_target_state_var", "") or ""
        method = sink.get("external_method", "") or ""
        if not target_var or not method:
            continue
        record: dict = {
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
    """Walk a function and emit every point where msg.sender reaches a gating
    predicate. Recurses into modifiers and internal/library callees up to
    `_MAX_CROSS_FUNCTION_DEPTH` — essential for OZ `onlyOwner` whose actual
    `msg.sender == owner` check lives inside `_checkOwner`."""
    sinks: list[CallerSink] = []
    seen = _seen if _seen is not None else set()

    def _walk_body(body_function: Any) -> None:
        taint = MsgSenderTaint(initial_tainted=_initial_tainted)
        for node in getattr(body_function, "nodes", []) or []:
            taint.propagate_through_node(node)
            before_len = len(sinks)
            sinks.extend(_classify_caller_equals(node, taint, project_dir))
            sinks.extend(_classify_caller_in_mapping(node, taint, project_dir))
            sinks.extend(_classify_caller_external_call(node, taint, project_dir))
            sinks.extend(_classify_caller_internal_call(node, taint, project_dir))
            sinks.extend(_classify_caller_signature(node, taint, project_dir))
            sinks.extend(_classify_caller_merkle(node, taint, project_dir))
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
            already_emitted = len(sinks) > before_len
            sinks.extend(_classify_caller_unknown(node, taint, project_dir, already_emitted))

    _walk_body(function)
    for modifier in getattr(function, "modifiers", []) or []:
        _walk_body(modifier)
    return sinks
