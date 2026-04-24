"""Unit tests for the universal caller-reach analyzer.

Phase 1 coverage: `MsgSenderTaint` + `caller_equals` + `caller_in_mapping`.
Slither IR types are mocked via `SimpleNamespace` with aliased
class names so the extractor's `type(ir).__name__ == "X"` filters
match the shapes slither actually emits.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static.contract_analysis_pipeline.caller_sinks import (  # noqa: E402
    MsgSenderTaint,
    caller_reach_analysis,
)

# ---------------------------------------------------------------------------
# Slither IR mocks
# ---------------------------------------------------------------------------


def _named(cls_name: str, **attrs: Any) -> SimpleNamespace:
    """SimpleNamespace with a forced `type(obj).__name__`. The
    analyzer filters on `type(ir).__name__` throughout, so every mock
    has to present the right class name string — a raw SimpleNamespace
    always reads back as "SimpleNamespace"."""

    subclass = type(cls_name, (SimpleNamespace,), {})
    subclass.__name__ = cls_name
    return subclass(**attrs)


def _state_var(name: str, type_str: str = "address") -> SimpleNamespace:
    subclass = type("StateVariable", (SimpleNamespace,), {})
    subclass.__name__ = "StateVariable"
    return subclass(name=name, type=type_str)


def _local(name: str, type_str: str = "address") -> SimpleNamespace:
    subclass = type("LocalVariable", (SimpleNamespace,), {})
    subclass.__name__ = "LocalVariable"
    return subclass(name=name, type=type_str)


def _tmp(name: str, type_str: str = "address") -> SimpleNamespace:
    subclass = type("TemporaryVariable", (SimpleNamespace,), {})
    subclass.__name__ = "TemporaryVariable"
    return subclass(name=name, type=type_str)


def _msg_sender() -> SimpleNamespace:
    """SolidityVariableComposed for msg.sender — what slither emits
    at the source of the taint."""
    subclass = type("SolidityVariableComposed", (SimpleNamespace,), {})
    subclass.__name__ = "SolidityVariableComposed"
    return subclass(name="msg.sender", type="address")


def _mapping(name: str, key: str = "address", value: str = "uint256") -> SimpleNamespace:
    subclass = type("StateVariable", (SimpleNamespace,), {})
    subclass.__name__ = "StateVariable"
    return subclass(name=name, type=f"mapping({key} => {value})")


def _constant(value: Any, type_str: str = "uint256") -> SimpleNamespace:
    subclass = type("Constant", (SimpleNamespace,), {})
    subclass.__name__ = "Constant"
    return subclass(name=str(value), type=type_str, value=value)


def _assignment(lvalue: Any, rvalue: Any) -> SimpleNamespace:
    return _named("Assignment", lvalue=lvalue, rvalue=rvalue)


def _binary(left: Any, right: Any, op: str = "==") -> SimpleNamespace:
    return _named("Binary", variable_left=left, variable_right=right, type=op)


def _index(base: Any, key: Any, lvalue: Any) -> SimpleNamespace:
    return _named("Index", variable_left=base, variable_right=key, lvalue=lvalue)


def _solidity_call(function_name: str) -> SimpleNamespace:
    fn_ref = SimpleNamespace(name=function_name)
    return _named("SolidityCall", function=fn_ref)


def _require_call() -> SimpleNamespace:
    return _named("SolidityCall", function=SimpleNamespace(name="require(bool,string)"))


def _node(irs: list[Any], *, node_id: int = 0, require: bool = False) -> SimpleNamespace:
    """A mock IR node. When `require=True` we append a Require IR so
    `_node_contains_require_or_assert` flips true."""
    if require:
        irs = list(irs) + [_require_call()]
    # `shared._node_contains_require_or_assert` checks node.contains_require_or_assert()
    # if present; fall back to scanning irs for Require. We set the flag directly.
    return SimpleNamespace(
        irs=irs,
        node_id=node_id,
        state_variables_read=[],
        contains_require_or_assert=lambda is_require=require: is_require,
    )


def _fn(nodes: list[Any], modifiers: list[Any] | None = None) -> SimpleNamespace:
    return SimpleNamespace(nodes=nodes, modifiers=modifiers or [])


def _run(function) -> list[dict]:
    # cast result to list[dict] for test assertions; CallerSink is a TypedDict.
    return [dict(s) for s in caller_reach_analysis(function, Path("/tmp/irrelevant"))]


# ---------------------------------------------------------------------------
# MsgSenderTaint unit tests
# ---------------------------------------------------------------------------


def test_taint_seeds_with_msgsender_aliases():
    """Out of the box, the canonical msg.sender variants are tainted."""
    t = MsgSenderTaint()
    assert t.is_tainted(_msg_sender())
    assert t.is_tainted(SimpleNamespace(name="_msgSender()"))
    assert t.is_tainted(SimpleNamespace(name="_msgSender"))


def test_taint_does_not_flag_unrelated_vars():
    t = MsgSenderTaint()
    assert not t.is_tainted(_state_var("owner"))
    assert not t.is_tainted(_local("caller"))
    assert not t.is_tainted(_tmp("TMP_0"))


def test_taint_propagates_through_assignment():
    """`TMP = msg.sender` taints TMP."""
    t = MsgSenderTaint()
    tmp0 = _tmp("TMP_0")
    assign = _assignment(lvalue=tmp0, rvalue=_msg_sender())
    node = _node([assign])
    t.propagate_through_node(node)
    assert t.is_tainted(tmp0)


def test_taint_propagates_through_type_conversion():
    """OZ `_msgSender()` often lowers to a TypeConversion. Must taint."""
    t = MsgSenderTaint()
    tmp0 = _tmp("TMP_0")
    conv = _named("TypeConversion", lvalue=tmp0, rvalue=_msg_sender())
    node = _node([conv])
    t.propagate_through_node(node)
    assert t.is_tainted(tmp0)


def test_taint_chain_propagation_across_ops_in_same_node():
    """`TMP_0 = msg.sender; TMP_1 = TMP_0` — both tainted."""
    t = MsgSenderTaint()
    tmp0 = _tmp("TMP_0")
    tmp1 = _tmp("TMP_1")
    node = _node(
        [
            _assignment(lvalue=tmp0, rvalue=_msg_sender()),
            _assignment(lvalue=tmp1, rvalue=tmp0),
        ]
    )
    t.propagate_through_node(node)
    assert t.is_tainted(tmp0)
    assert t.is_tainted(tmp1)


def test_taint_phi_propagates_when_any_source_tainted():
    """SSA merges after if/else where only one branch assigned
    msg.sender — the merged value is still tainted."""
    t = MsgSenderTaint()
    merged = _tmp("TMP_MERGED")
    phi = _named("Phi", lvalue=merged, rvalues=[_local("other"), _msg_sender()])
    t.propagate_through_node(_node([phi]))
    assert t.is_tainted(merged)


def test_taint_call_lvalue_not_propagated():
    """The return value of `X.getOwner()` is NOT msg.sender, even if
    msg.sender was passed in. Don't poison downstream reads."""
    t = MsgSenderTaint()
    lvalue = _tmp("TMP_RET")
    call = _named(
        "HighLevelCall",
        lvalue=lvalue,
        arguments=[_msg_sender()],
        function_name=SimpleNamespace(name="getOwner"),
    )
    t.propagate_through_node(_node([call]))
    assert not t.is_tainted(lvalue)


# ---------------------------------------------------------------------------
# caller_equals sink
# ---------------------------------------------------------------------------


def test_caller_equals_direct_state_var():
    """`require(msg.sender == owner)` — emits one caller_equals sink
    targeting `owner` with revert_on_mismatch=True."""
    owner = _state_var("owner")
    node = _node([_binary(_msg_sender(), owner)], require=True)
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    s = sinks[0]
    assert s["kind"] == "caller_equals"
    assert s["target_state_var"] == "owner"
    assert s["target_type"] == "address"
    assert s["revert_on_mismatch"] is True


def test_caller_equals_other_side():
    """`require(owner == msg.sender)` — same sink, sides swapped."""
    owner = _state_var("owner")
    node = _node([_binary(owner, _msg_sender())], require=True)
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    assert sinks[0]["target_state_var"] == "owner"


def test_caller_equals_through_tmp_slither_lowering():
    """`address caller = msg.sender; require(caller == owner)` —
    slither lowers via Assignment → TMP. Must still produce one
    caller_equals sink on `owner`."""
    owner = _state_var("owner")
    tmp = _local("caller")
    node = _node(
        [
            _assignment(lvalue=tmp, rvalue=_msg_sender()),
            _binary(tmp, owner),
        ],
        require=True,
    )
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    assert sinks[0]["target_state_var"] == "owner"


def test_caller_equals_observational_when_no_revert():
    """Binary compare without require/revert context — still emitted,
    but `revert_on_mismatch=False`. Downstream decides whether to
    treat it as gating."""
    owner = _state_var("owner")
    node = _node([_binary(_msg_sender(), owner)], require=False)
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    assert sinks[0]["revert_on_mismatch"] is False


def test_caller_equals_if_revert_branch():
    """`if (msg.sender != X) revert NotAuthorized()` — the node ends
    in a SolidityCall to `revert Error`. Must still be flagged as a
    gating check."""
    owner = _state_var("owner")
    node = _node(
        [
            _binary(_msg_sender(), owner, op="!="),
            _solidity_call("revert Error()"),
        ]
    )
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    assert sinks[0]["revert_on_mismatch"] is True


def test_caller_equals_with_constant_counterparty():
    """`require(msg.sender == 0x1234…)` — counterparty is a Constant,
    not a state variable. Emit with `constant_value` populated."""
    constant = _constant("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef", "address")
    node = _node([_binary(_msg_sender(), constant)], require=True)
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    assert sinks[0].get("constant_value", "").startswith("0xdeadbeef")
    assert "target_state_var" not in sinks[0]


def test_caller_equals_two_tainted_sides_skipped():
    """`msg.sender == msg.sender` — structured caller_equals correctly
    refuses to emit. With Batch 3's unknown fallback in place, the
    node DOES flip to caller_unknown (msg.sender appears in a gating
    context, we just can't name the shape). That's the intended
    no-silent-miss behavior."""
    node = _node([_binary(_msg_sender(), _msg_sender())], require=True)
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    assert sinks[0]["kind"] == "caller_unknown"


def test_caller_equals_neither_side_tainted_skipped():
    """A comparison between two non-caller vars isn't a caller sink."""
    owner = _state_var("owner")
    admin = _state_var("admin")
    node = _node([_binary(owner, admin)], require=True)
    sinks = _run(_fn([node]))
    assert sinks == []


# ---------------------------------------------------------------------------
# caller_in_mapping sink
# ---------------------------------------------------------------------------


def test_caller_in_mapping_wards_pattern():
    """MakerDAO-style `require(wards[msg.sender] == 1)`. Slither
    lowers the indexed read into an Index IR then a Binary. Must emit
    one caller_in_mapping with predicate `== 1`."""
    wards = _mapping("wards", key="address", value="uint256")
    tmp_read = _tmp("TMP_0", type_str="uint256")
    one = _constant(1)
    node = _node(
        [
            _index(wards, _msg_sender(), tmp_read),
            _binary(tmp_read, one),
        ],
        require=True,
    )
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    s = sinks[0]
    assert s["kind"] == "caller_in_mapping"
    assert s["mapping_name"] == "wards"
    assert "1" in s["mapping_predicate"]
    assert s["revert_on_mismatch"] is True


def test_caller_in_mapping_bool_mapping_pattern():
    """`require(whitelist[msg.sender])` — no explicit Binary; the
    Index lvalue (bool) flows straight into require. We still emit
    the sink, just without a spelled-out predicate."""
    whitelist = _mapping("whitelist", key="address", value="bool")
    tmp = _tmp("TMP_0", type_str="bool")
    node = _node(
        [
            _index(whitelist, _msg_sender(), tmp),
        ],
        require=True,
    )
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    s = sinks[0]
    assert s["mapping_name"] == "whitelist"
    # mapping_predicate is optional when absent
    assert "mapping_predicate" not in s or s["mapping_predicate"]


def test_caller_in_mapping_key_through_tmp():
    """`TMP = msg.sender; wards[TMP]` — taint propagates to TMP, so
    the Index lookup still registers as caller-keyed."""
    wards = _mapping("wards", key="address", value="uint256")
    key_tmp = _tmp("TMP_K", type_str="address")
    read_tmp = _tmp("TMP_R", type_str="uint256")
    node = _node(
        [
            _assignment(lvalue=key_tmp, rvalue=_msg_sender()),
            _index(wards, key_tmp, read_tmp),
            _binary(read_tmp, _constant(1)),
        ],
        require=True,
    )
    sinks = _run(_fn([node]))
    assert any(s["mapping_name"] == "wards" for s in sinks)


def test_caller_in_mapping_key_is_not_caller_skipped():
    """`wards[guy]` where `guy` is a parameter, not msg.sender — not
    a caller sink. Skipped."""
    wards = _mapping("wards")
    guy = _local("guy", type_str="address")
    read_tmp = _tmp("TMP_R", type_str="uint256")
    node = _node(
        [_index(wards, guy, read_tmp), _binary(read_tmp, _constant(1))],
        require=True,
    )
    sinks = _run(_fn([node]))
    assert sinks == []


def test_caller_in_mapping_requires_mapping_type():
    """Index on an array (not a mapping) shouldn't register as a
    caller_in_mapping — those aren't allowlist patterns. The unknown
    fallback still fires because msg.sender appears in a gating
    context, which is the desired no-silent-miss behavior."""
    array_var = _state_var("queue", type_str="uint256[]")
    tmp = _tmp("TMP_R", type_str="uint256")
    node = _node([_index(array_var, _msg_sender(), tmp)], require=True)
    sinks = _run(_fn([node]))
    assert [s for s in sinks if s["kind"] == "caller_in_mapping"] == []
    # Unknown fires because msg.sender is still reaching a revert.
    assert any(s["kind"] == "caller_unknown" for s in sinks)


# ---------------------------------------------------------------------------
# Modifier bodies
# ---------------------------------------------------------------------------


def test_modifier_body_contributes_sinks():
    """Guards often live in modifier bodies. The walker must traverse
    every modifier attached to the function, not just the body."""
    owner = _state_var("owner")
    modifier = SimpleNamespace(
        name="onlyOwner",
        nodes=[_node([_binary(_msg_sender(), owner)], require=True)],
    )
    fn = _fn(nodes=[], modifiers=[modifier])
    sinks = _run(fn)
    assert len(sinks) == 1
    assert sinks[0]["target_state_var"] == "owner"


def test_empty_function_emits_nothing():
    assert _run(_fn([])) == []


# ---------------------------------------------------------------------------
# caller_external_call sink (Batch 2)
# ---------------------------------------------------------------------------


def _highlevel_call(destination, method_name, arguments, lvalue=None):
    return _named(
        "HighLevelCall",
        destination=destination,
        function_name=SimpleNamespace(name=method_name),
        arguments=arguments,
        lvalue=lvalue,
    )


def test_caller_external_call_renzo_pattern():
    """`roleManager.onlyDepositWithdrawPauser(msg.sender)` — pattern A.
    Target state var captured, method name captured, no role_args
    because the role is encoded in the method name."""
    role_mgr = _state_var("roleManager", "IRoleManager")
    node = _node(
        [_highlevel_call(role_mgr, "onlyDepositWithdrawPauser", [_msg_sender()])],
        require=True,
    )
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    s = sinks[0]
    assert s["kind"] == "caller_external_call"
    assert s["external_target_state_var"] == "roleManager"
    assert s["external_method"] == "onlyDepositWithdrawPauser"
    assert s["target_type"] == "IRoleManager"
    assert "external_role_args" not in s


def test_caller_external_call_hasrole_pattern_b():
    """`roleRegistry.hasRole(PROTOCOL_PAUSER, msg.sender)` — pattern B.
    Role argument captured as `external_role_args`."""
    role_reg = _state_var("roleRegistry", "IRoleRegistry")
    role_const = _state_var("PROTOCOL_PAUSER", "bytes32")
    node = _node(
        [_highlevel_call(role_reg, "hasRole", [role_const, _msg_sender()])],
        require=True,
    )
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    s = sinks[0]
    assert s["external_method"] == "hasRole"
    assert s.get("external_role_args") == ["PROTOCOL_PAUSER"]


def test_caller_external_call_tmp_sender_detected():
    """Slither-lowered `TMP_0 = msg.sender; X.check(TMP_0)` — taint
    must follow the TMP, so the sink still fires. This is the case
    the old name-match matcher silently missed."""
    gate = _state_var("gate", "IAccessGate")
    tmp = _tmp("TMP_0")
    node = _node(
        [
            _assignment(lvalue=tmp, rvalue=_msg_sender()),
            _highlevel_call(gate, "check", [tmp]),
        ],
        require=True,
    )
    sinks = _run(_fn([node]))
    assert any(s["kind"] == "caller_external_call" for s in sinks)


def test_caller_external_call_skips_non_state_destination():
    """If the call target is a local/temp (not traceable to a state
    var), we can't resolve the authority address later — skip."""
    local_addr = _local("tmp_target", "address")
    node = _node(
        [_highlevel_call(local_addr, "check", [_msg_sender()])],
        require=True,
    )
    sinks = _run(_fn([node]))
    assert [s for s in sinks if s["kind"] == "caller_external_call"] == []


def test_caller_external_call_skips_when_no_sender_arg():
    """Regular external call unrelated to auth — `token.transfer(to,
    amount)` — no caller flow, no sink."""
    token = _state_var("token", "IERC20")
    to = _local("to", "address")
    amount = _local("amount", "uint256")
    node = _node([_highlevel_call(token, "transfer", [to, amount])], require=False)
    sinks = _run(_fn([node]))
    assert [s for s in sinks if s["kind"] == "caller_external_call"] == []


# ---------------------------------------------------------------------------
# caller_internal_call sink (Batch 2)
# ---------------------------------------------------------------------------


def _internal_call(callee_name: str, arguments):
    return _named(
        "InternalCall",
        function=SimpleNamespace(name=callee_name),
        arguments=arguments,
    )


def test_caller_internal_call_helper_with_sender():
    """`_checkOnlyAdmin(msg.sender)` — internal helper, caller flows
    in. Records the callee."""
    node = _node([_internal_call("_checkOnlyAdmin", [_msg_sender()])], require=True)
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    s = sinks[0]
    assert s["kind"] == "caller_internal_call"
    assert s["internal_callee"] == "_checkOnlyAdmin"


def test_caller_internal_call_skips_when_no_sender_arg():
    node = _node([_internal_call("_computeFees", [_local("amount", "uint256")])])
    sinks = _run(_fn([node]))
    assert [s for s in sinks if s["kind"] == "caller_internal_call"] == []


# ---------------------------------------------------------------------------
# caller_signature sink (Batch 3)
# ---------------------------------------------------------------------------


def test_caller_signature_ecrecover_direct():
    """`ecrecover(hash, v, r, s)` — terminal non-enumerable."""
    eh = _tmp("hash", "bytes32")
    v = _local("v", "uint8")
    r = _local("r", "bytes32")
    s = _local("s", "bytes32")
    ecr = _named(
        "SolidityCall",
        function=SimpleNamespace(name="ecrecover(bytes32,uint8,bytes32,bytes32)"),
        arguments=[eh, v, r, s],
    )
    node = _node([ecr])
    sinks = _run(_fn([node]))
    assert any(x["kind"] == "caller_signature" for x in sinks)


def test_caller_signature_oz_signaturechecker():
    """`signer.isValidSignatureNow(hash, sig)` — OZ family. Source
    var captured as the helper contract."""
    signer = _state_var("signer", "address")
    call = _highlevel_call(signer, "isValidSignatureNow", [_local("h", "bytes32"), _local("sig", "bytes")])
    node = _node([call])
    sinks = _run(_fn([node]))
    matching = [x for x in sinks if x["kind"] == "caller_signature"]
    assert len(matching) == 1
    assert matching[0]["signature_source_var"] == "signer"


# ---------------------------------------------------------------------------
# caller_merkle sink (Batch 3)
# ---------------------------------------------------------------------------


def _library_call(lib_name, method, arguments):
    """LibraryCall with a `destination` pointing at the library and
    a function with `.contract.name=<lib>` (slither reports the
    owning contract for library functions)."""
    lib = SimpleNamespace(name=lib_name)
    fn_ref = SimpleNamespace(name=method, contract=SimpleNamespace(name=lib_name))
    return _named(
        "LibraryCall",
        destination=lib,
        function_name=fn_ref,
        arguments=arguments,
    )


def test_caller_merkle_verify_with_caller_leaf():
    """`MerkleProof.verify(proof, root, leaf)` where leaf comes from
    msg.sender — classic allowlist-drop. Emits merkle sink with root."""
    proof = _local("proof", "bytes32[]")
    root = _state_var("merkleRoot", "bytes32")
    # leaf is usually a keccak of msg.sender — here we just make it
    # a tainted local so the classifier sees it.
    leaf_tmp = _tmp("leaf", "bytes32")
    node = _node(
        [
            _assignment(lvalue=leaf_tmp, rvalue=_msg_sender()),
            _library_call("MerkleProof", "verify", [proof, root, leaf_tmp]),
        ],
        require=True,
    )
    sinks = _run(_fn([node]))
    matching = [x for x in sinks if x["kind"] == "caller_merkle"]
    assert len(matching) == 1
    assert matching[0]["merkle_root_var"] == "merkleRoot"


def test_caller_merkle_skips_when_no_caller_in_args():
    proof = _local("proof", "bytes32[]")
    root = _state_var("merkleRoot", "bytes32")
    leaf = _local("leaf", "bytes32")
    node = _node([_library_call("MerkleProof", "verify", [proof, root, leaf])], require=True)
    sinks = _run(_fn([node]))
    assert [x for x in sinks if x["kind"] == "caller_merkle"] == []


# ---------------------------------------------------------------------------
# caller_unknown fallback (Batch 3)
# ---------------------------------------------------------------------------


def test_caller_unknown_fires_when_structured_detectors_miss():
    """A require that touches msg.sender but doesn't match any known
    shape — we still want to report it as a guard, just as `unknown`.
    Otherwise it silently disappears."""
    # UnaryOp-style check that we don't classify — e.g. calling a view
    # function on a local that was derived from msg.sender — represented
    # here as a Binary between two taint-polluted temporaries neither of
    # which the structured classifiers can name.
    tmp_a = _tmp("TMP_A")
    tmp_b = _tmp("TMP_B")
    # Weird shape: the Binary compares two tainted vars — structured
    # equals refuses to emit, so caller_unknown takes over.
    node = _node(
        [
            _assignment(lvalue=tmp_a, rvalue=_msg_sender()),
            _assignment(lvalue=tmp_b, rvalue=_msg_sender()),
            _binary(tmp_a, tmp_b),
        ],
        require=True,
    )
    sinks = _run(_fn([node]))
    assert len(sinks) == 1
    assert sinks[0]["kind"] == "caller_unknown"
    assert sinks[0]["revert_on_mismatch"] is True


def test_caller_unknown_suppressed_when_structured_match_exists():
    """If any structured classifier emitted, unknown stays quiet —
    we don't want double-reports."""
    owner = _state_var("owner")
    node = _node([_binary(_msg_sender(), owner)], require=True)
    sinks = _run(_fn([node]))
    kinds = [s["kind"] for s in sinks]
    assert "caller_equals" in kinds
    assert "caller_unknown" not in kinds


def _callee_fn(name: str, nodes: list[Any], parameters: list[Any] | None = None) -> SimpleNamespace:
    """A mock slither Function/Callee object with `.nodes`, `.parameters`,
    `.modifiers`. The cross-function walker reads these attributes."""
    return SimpleNamespace(
        name=name,
        nodes=nodes,
        modifiers=[],
        parameters=parameters or [],
    )


def _internal_call_to(callee_fn: SimpleNamespace, arguments: list[Any]) -> SimpleNamespace:
    """An InternalCall IR whose `.function` is the callee Function
    object, so the walker can recurse into `callee_fn.nodes`."""
    return _named(
        "InternalCall",
        function=callee_fn,
        arguments=arguments,
    )


# ---------------------------------------------------------------------------
# Phase 2a: cross-function taint
# ---------------------------------------------------------------------------


def test_cross_function_oz_check_owner_pattern():
    """OZ Ownable: `onlyOwner` modifier calls `_checkOwner()` which
    does `require(_msgSender() == owner)`. The modifier body has
    an InternalCall with no args; the caller_equals check lives in
    the callee. Cross-function recursion must discover it."""
    # Callee `_checkOwner` does: require(msg.sender == _owner)
    owner = _state_var("_owner", "address")
    check_owner_body = _node(
        [_binary(_msg_sender(), owner)],
        require=True,
    )
    check_owner = _callee_fn("_checkOwner", nodes=[check_owner_body], parameters=[])

    # Modifier body: `_checkOwner(); _;` — one internal call with no args
    modifier_body = _node([_internal_call_to(check_owner, [])])
    only_owner = SimpleNamespace(name="onlyOwner", nodes=[modifier_body])

    # Outer function with `onlyOwner` modifier.
    fn = _fn(nodes=[], modifiers=[only_owner])
    sinks = _run(fn)
    # Expect a caller_equals sink on `_owner` propagated up from
    # `_checkOwner`'s body.
    equals = [s for s in sinks if s["kind"] == "caller_equals"]
    assert len(equals) == 1
    assert equals[0]["target_state_var"] == "_owner"


def test_cross_function_taint_via_tainted_argument():
    """`_requireAdmin(msg.sender)` — caller passes msg.sender as an
    arg. The callee's first parameter must be seeded as tainted so
    the `require(param == admin)` inside the callee fires."""
    admin = _state_var("admin", "address")
    caller_param = _local("caller", "address")
    require_admin_body = _node(
        [_binary(caller_param, admin)],
        require=True,
    )
    require_admin = _callee_fn(
        "_requireAdmin",
        nodes=[require_admin_body],
        parameters=[caller_param],
    )

    node = _node([_internal_call_to(require_admin, [_msg_sender()])], require=True)
    sinks = _run(_fn([node]))
    equals = [s for s in sinks if s["kind"] == "caller_equals"]
    assert len(equals) == 1
    assert equals[0]["target_state_var"] == "admin"


def test_cross_function_cycle_detection():
    """Two mutually-recursive helpers — shouldn't blow the stack."""
    owner = _state_var("owner", "address")
    # Build a cycle: A -> B -> A
    a = _callee_fn("a", nodes=[], parameters=[])
    b = _callee_fn("b", nodes=[], parameters=[])
    a.nodes = [_node([_internal_call_to(b, []), _binary(_msg_sender(), owner)], require=True)]
    b.nodes = [_node([_internal_call_to(a, [])])]

    modifier_body = _node([_internal_call_to(a, [])])
    mod = SimpleNamespace(name="weird", nodes=[modifier_body])
    fn = _fn(nodes=[], modifiers=[mod])

    sinks = _run(fn)
    # Should complete without recursion error and still find the
    # owner equality inside A.
    equals = [s for s in sinks if s["kind"] == "caller_equals"]
    assert len(equals) == 1


def test_msg_sender_helper_return_value_is_tainted():
    """`address x = _msgSender(); require(x == owner)` — OZ ERC-2771.
    The InternalCall's lvalue must become tainted so the downstream
    Binary registers as caller_equals."""
    owner = _state_var("owner")
    tmp = _tmp("TMP_0")
    msg_sender_callee = _callee_fn("_msgSender", nodes=[], parameters=[])
    call_ir = _named("InternalCall", function=msg_sender_callee, arguments=[], lvalue=tmp)
    node = _node([call_ir, _binary(tmp, owner)], require=True)
    sinks = _run(_fn([node]))
    equals = [s for s in sinks if s["kind"] == "caller_equals"]
    assert len(equals) == 1
    assert equals[0]["target_state_var"] == "owner"


def test_caller_equals_resolves_tmp_to_state_var_via_getter():
    """`require(owner() == _msgSender())` — both sides are TMPs. The
    left-side TMP is the return of `owner()`, a getter that reads
    `_owner`. Resolver must walk into the callee and surface `_owner`
    as the target state var instead of emitting `TMP_1`."""
    _owner = _state_var("_owner")
    # Build a getter function whose body reads `_owner`. Slither
    # exposes state_variables_read on each node.
    owner_getter_node = SimpleNamespace(
        irs=[],
        node_id=0,
        state_variables_read=[_owner],
        contains_require_or_assert=lambda: False,
    )
    owner_getter = _callee_fn("owner", nodes=[owner_getter_node], parameters=[])

    tmp_owner = _tmp("TMP_1")
    tmp_sender = _tmp("TMP_0")
    msg_sender_callee = _callee_fn("_msgSender", nodes=[], parameters=[])

    node = _node(
        [
            _named("InternalCall", function=owner_getter, arguments=[], lvalue=tmp_owner),
            _named("InternalCall", function=msg_sender_callee, arguments=[], lvalue=tmp_sender),
            _binary(tmp_owner, tmp_sender),
        ],
        require=True,
    )
    sinks = _run(_fn([node]))
    equals = [s for s in sinks if s["kind"] == "caller_equals"]
    assert len(equals) == 1
    assert equals[0]["target_state_var"] == "_owner"


def test_cross_function_depth_cap():
    """A deep call chain beyond the cap should not emit sinks from
    the deepest layer. 3 levels is the current cap."""
    owner = _state_var("owner", "address")
    # Build 5-deep chain: a -> b -> c -> d -> e
    e = _callee_fn("e", nodes=[_node([_binary(_msg_sender(), owner)], require=True)], parameters=[])
    d = _callee_fn("d", nodes=[_node([_internal_call_to(e, [])])], parameters=[])
    c = _callee_fn("c", nodes=[_node([_internal_call_to(d, [])])], parameters=[])
    b = _callee_fn("b", nodes=[_node([_internal_call_to(c, [])])], parameters=[])
    a = _callee_fn("a", nodes=[_node([_internal_call_to(b, [])])], parameters=[])

    fn = _fn(nodes=[_node([_internal_call_to(a, [])])])
    sinks = _run(fn)
    # The guard in `e` is 5 levels deep — past the cap. Expect no
    # caller_equals sink found via recursion.
    assert not any(s["kind"] == "caller_equals" and s.get("target_state_var") == "owner" for s in sinks)


def test_caller_unknown_requires_revert_context():
    """Non-gating reads of msg.sender (e.g. `emit Log(msg.sender)` in
    a write path) don't become unknowns. We only flag as an
    unresolved GUARD, not any use."""
    tmp_a = _tmp("TMP_A")
    node = _node(
        [_assignment(lvalue=tmp_a, rvalue=_msg_sender())],
        require=False,
    )
    sinks = _run(_fn([node]))
    assert sinks == []
