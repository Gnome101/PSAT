"""Unit tests for ``ProvenanceEngine`` over Slither IR.

Each fixture is a tiny Solidity contract; we run Slither on it, pick a
function, run the engine, and assert provenance for specific SSA
values. We do NOT assert internal SSA naming — Slither's SSA renaming
isn't part of the contract; tests find values by their semantic
position (function parameter / state read / call return).

The fixtures cover the IR opcodes the predicate builder will read in
weeks 2-3:
  - Assignment / TypeConversion / Phi
  - Binary / Unary
  - Index / Member (for mapping/struct access)
  - SolidityCall (ecrecover, keccak256)
  - InternalCall (recursion + parameter binding)
  - HighLevelCall (external bool)

These are the foundation that the predicate builder (week 2) sits on
top of. We don't test the predicate builder here — that's a separate
test suite.
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

slither = pytest.importorskip("slither")
from slither import Slither  # noqa: E402

from services.static.contract_analysis_pipeline.provenance import (  # noqa: E402
    EMPTY,
    TOP,
    ProvenanceEngine,
    Source,
    is_top,
    union,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compile(tmp_path: Path, source: str) -> Slither:
    src = textwrap.dedent(source).strip() + "\n"
    f = tmp_path / "C.sol"
    f.write_text(src)
    return Slither(str(f))


def _function(sl: Slither, fn_name: str):
    for c in sl.contracts:
        for f in c.functions:
            if f.name == fn_name:
                return f
    raise LookupError(fn_name)


def _has_source_kind(sources, kind: str) -> bool:
    return any(s.kind == kind for s in sources)


def _find_source_with_kind(sources, kind: str) -> Source | None:
    for s in sources:
        if s.kind == kind:
            return s
    return None


# ---------------------------------------------------------------------------
# Pure lattice tests (no Slither needed).
# ---------------------------------------------------------------------------


def test_lattice_union_basic():
    a = frozenset({Source(kind="msg_sender")})
    b = frozenset({Source(kind="parameter", parameter_index=0)})
    out = union(a, b)
    assert _has_source_kind(out, "msg_sender")
    assert _has_source_kind(out, "parameter")
    assert not is_top(out)


def test_lattice_top_absorbs():
    a = frozenset({Source(kind="msg_sender")})
    out = union(a, TOP)
    assert is_top(out)
    out2 = union(TOP, a)
    assert is_top(out2)


def test_lattice_empty_identity():
    a = frozenset({Source(kind="parameter", parameter_index=0)})
    assert union(a, EMPTY) == a
    assert union(EMPTY, a) == a


def test_source_unknown_kind_raises():
    with pytest.raises(ValueError):
        Source(kind="not_a_real_kind")


# ---------------------------------------------------------------------------
# Slither-driven IR tests.
# ---------------------------------------------------------------------------


def test_parameter_seeded(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function f(address account, uint256 amount) external {
                account; amount;
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    # Every parameter should have a `parameter` source with the right index.
    for idx, param in enumerate(fn.parameters):
        sources = eng.provenance.get(param.name)
        param_src = _find_source_with_kind(sources, "parameter")
        assert param_src is not None, f"parameter {param.name} not seeded"
        assert param_src.parameter_index == idx


def test_assignment_propagates(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function f() external view {
                address a = msg.sender;
                a;
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    # Find the SSA value for `a` — it'll have a name like `a` or `a_1`.
    found = False
    for name, sources in eng.provenance.sources.items():
        if name.startswith("a") and _has_source_kind(sources, "msg_sender"):
            found = True
            break
    assert found, f"no SSA value for `a` got msg_sender source. map={dict(eng.provenance.sources)}"


def test_type_conversion_preserves_source(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function f() external view {
                bytes32 b = bytes32(uint256(uint160(msg.sender)));
                b;
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    # The chain of type conversions should land msg.sender provenance
    # somewhere reachable.
    has_caller = any(_has_source_kind(srcs, "msg_sender") for srcs in eng.provenance.sources.values())
    assert has_caller, "type conversion chain dropped msg_sender source"


def test_binary_combines_operand_sources(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            uint256 public threshold;
            function f(uint256 amount) external view {
                bool ok = amount > threshold;
                ok;
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    # The `ok` value should have a `computed` source whose taint
    # includes both parameter and state_variable.
    found_computed = False
    for sources in eng.provenance.sources.values():
        if (
            _has_source_kind(sources, "parameter")
            and _has_source_kind(sources, "state_variable")
            and _has_source_kind(sources, "computed")
        ):
            found_computed = True
            break
    assert found_computed, (
        f"binary op didn't produce computed+parameter+state_variable taint. map={dict(eng.provenance.sources)}"
    )


def test_state_variable_read(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            function f() external view returns (address) {
                return ownerVar;
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    has_state = any(_has_source_kind(srcs, "state_variable") for srcs in eng.provenance.sources.values())
    assert has_state


def test_index_propagates_base_and_key(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            mapping(address => uint256) public balances;
            function f() external view returns (uint256) {
                return balances[msg.sender];
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    # The reference for balances[msg.sender] should carry both
    # state_variable (from the mapping base) and msg_sender (from the key).
    has_state_and_caller = any(
        _has_source_kind(srcs, "state_variable") and _has_source_kind(srcs, "msg_sender")
        for srcs in eng.provenance.sources.values()
    )
    assert has_state_and_caller, f"Index didn't propagate base+key sources. map={dict(eng.provenance.sources)}"


def test_solidity_call_ecrecover_classified_as_signature(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function f(bytes32 h, uint8 v, bytes32 r, bytes32 s) external pure returns (address) {
                return ecrecover(h, v, r, s);
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    has_sig = any(_has_source_kind(srcs, "signature_recovery") for srcs in eng.provenance.sources.values())
    assert has_sig, f"ecrecover didn't produce signature_recovery source. map={dict(eng.provenance.sources)}"


def test_solidity_call_keccak_classified_as_computed(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function f(uint256 x) external pure returns (bytes32) {
                return keccak256(abi.encode(x));
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    # keccak256 result should be `computed`, not signature_recovery.
    has_computed = any(_has_source_kind(srcs, "computed") for srcs in eng.provenance.sources.values())
    has_sig = any(_has_source_kind(srcs, "signature_recovery") for srcs in eng.provenance.sources.values())
    assert has_computed
    assert not has_sig, "keccak256 was misclassified as signature_recovery"


def test_external_call_classified(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        interface IOther {
            function answer() external view returns (uint256);
        }
        contract C {
            IOther public other;
            function f() external view returns (uint256) {
                return other.answer();
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    has_external = any(_has_source_kind(srcs, "external_call") for srcs in eng.provenance.sources.values())
    assert has_external


def test_internal_call_recurses_into_callee(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function _helper(address a) internal view returns (address) {
                return a;
            }
            function f() external view returns (address) {
                return _helper(msg.sender);
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    # The return of _helper(msg.sender) should land msg_sender provenance.
    has_caller = any(_has_source_kind(srcs, "msg_sender") for srcs in eng.provenance.sources.values())
    assert has_caller, f"internal call didn't propagate msg_sender. map={dict(eng.provenance.sources)}"


def test_internal_call_depth_cap_does_not_crash(tmp_path):
    """Mutual recursion past the depth cap must terminate cleanly.

    Even though Solidity rarely has infinite mutual recursion in
    practice, the engine must guard against it to avoid blowing the
    stack on adversarial fixtures.
    """
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function a(address x) internal view returns (address) { return b(x); }
            function b(address x) internal view returns (address) { return a(x); }
            function f() external view returns (address) {
                return a(msg.sender);
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn, internal_call_depth=3)
    # Should not raise / loop forever.
    eng.run()
    # We just want termination — don't assert specific provenance.


def test_block_timestamp_classified(tmp_path):
    """``block.timestamp`` is a SolidityVariable. When it appears as
    an operand to a Binary op (the typical require pattern), the
    binary's SSA result carries a ``block_context`` source through
    the operand-union path."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            uint256 public unlockTime;
            function f() external view returns (bool) {
                return block.timestamp > unlockTime;
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    has_block = any(_has_source_kind(srcs, "block_context") for srcs in eng.provenance.sources.values())
    assert has_block, (
        f"binary op with block.timestamp didn't yield block_context source. map={dict(eng.provenance.sources)}"
    )


# ---------------------------------------------------------------------------
# Low-level calls + tuple unpacking.
# ---------------------------------------------------------------------------


def test_low_level_call_classified(tmp_path):
    """``target.call(data)`` produces a tuple lvalue. The tuple's
    provenance must include ``external_call`` AND the destination/args
    taint (both parameters here)."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function f(address target, bytes calldata data) external returns (bool, bytes memory) {
                (bool ok, bytes memory r) = target.call(data);
                return (ok, r);
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    # Find a value with external_call source AND parameter taint.
    found = False
    for srcs in eng.provenance.sources.values():
        if _has_source_kind(srcs, "external_call") and _has_source_kind(srcs, "parameter"):
            external = _find_source_with_kind(srcs, "external_call")
            assert external is not None
            assert external.callee == "call", f"expected callee='call', got {external.callee!r}"
            found = True
            break
    assert found, f"low_level_call didn't produce external_call+parameter taint. map={dict(eng.provenance.sources)}"


def test_staticcall_classified(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function f(address target, bytes calldata data) external view returns (bool, bytes memory) {
                return target.staticcall(data);
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    found_staticcall = False
    for srcs in eng.provenance.sources.values():
        ext = _find_source_with_kind(srcs, "external_call")
        if ext and ext.callee == "staticcall":
            found_staticcall = True
            break
    assert found_staticcall, f"staticcall not classified with callee='staticcall'. map={dict(eng.provenance.sources)}"


def test_delegatecall_preserves_destination_taint(tmp_path):
    """delegatecall is structurally distinguished by ``callee==
    'delegatecall'``. The destination's provenance must travel
    through into the result so a downstream analyzer can see if the
    target was caller-controlled."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function f(bytes calldata data) external returns (bool) {
                (bool ok, ) = msg.sender.delegatecall(data);
                return ok;
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    # The result must carry both external_call(callee=delegatecall)
    # AND msg_sender (because the destination was msg.sender).
    found = False
    for srcs in eng.provenance.sources.values():
        ext = _find_source_with_kind(srcs, "external_call")
        if ext and ext.callee == "delegatecall" and _has_source_kind(srcs, "msg_sender"):
            found = True
            break
    assert found, (
        f"delegatecall with msg.sender destination didn't preserve msg_sender taint. map={dict(eng.provenance.sources)}"
    )


def test_unpack_propagates_tuple_provenance(tmp_path):
    """After ``(bool ok, ) = target.call(data);``, the unpacked ``ok``
    SSA value inherits the tuple's full provenance set."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function f(address target, bytes calldata data) external returns (bool) {
                (bool ok, ) = target.call(data);
                return ok;
            }
        }
    """,
    )
    fn = _function(sl, "f")
    eng = ProvenanceEngine(fn)
    eng.run()
    # `ok` is unpacked from the tuple; it should carry external_call source.
    has_ok_with_external = any(
        name.startswith("ok") and _has_source_kind(srcs, "external_call")
        for name, srcs in eng.provenance.sources.items()
    )
    assert has_ok_with_external, (
        f"unpacked `ok` didn't inherit external_call source. map={dict(eng.provenance.sources)}"
    )
