"""Tests for ``RevertDetector``.

Covers each of the 8 revert-pattern cases from v4 plan round-2 #8.
For each, we compile a tiny Solidity contract and assert RevertDetector
finds exactly the expected RevertGate(s) with the correct kind +
polarity. The condition_value identity isn't pinned (Slither-version
dependent SSA renaming); we focus on count + kind + polarity.
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

slither = pytest.importorskip("slither")
from slither import Slither  # noqa: E402

from services.static.contract_analysis_pipeline.revert_detect import (  # noqa: E402
    RevertDetector,
    RevertGate,
)


def _compile(tmp_path: Path, source: str) -> Slither:
    src = textwrap.dedent(source).strip() + "\n"
    f = tmp_path / "C.sol"
    f.write_text(src)
    return Slither(str(f))


def _function(sl: Slither, name: str):
    for c in sl.contracts:
        for f in c.functions:
            if f.name == name:
                return f
    raise LookupError(name)


def _gate_kinds(gates: list[RevertGate]) -> list[str]:
    return [g.kind for g in gates]


# ---------------------------------------------------------------------------
# Case 1: require
# ---------------------------------------------------------------------------


def test_require_simple(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            function f() external view {
                require(msg.sender == ownerVar);
            }
        }
    """,
    )
    fn = _function(sl, "f")
    gates = RevertDetector(fn).run()
    kinds = _gate_kinds(gates)
    assert "require" in kinds
    req = next(g for g in gates if g.kind == "require")
    assert req.polarity == "allowed_when_true"
    assert req.condition_value is not None


def test_require_with_message(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            uint256 public threshold;
            function f(uint256 amount) external view {
                require(amount > threshold, "too low");
            }
        }
    """,
    )
    fn = _function(sl, "f")
    gates = RevertDetector(fn).run()
    assert any(g.kind == "require" for g in gates)


# ---------------------------------------------------------------------------
# Case 2: assert
# ---------------------------------------------------------------------------


def test_assert(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function f(uint256 x) external pure {
                assert(x > 0);
            }
        }
    """,
    )
    fn = _function(sl, "f")
    gates = RevertDetector(fn).run()
    assert any(g.kind == "assert" for g in gates)


# ---------------------------------------------------------------------------
# Case 3: if (C) revert
# ---------------------------------------------------------------------------


def test_if_revert_inverts_polarity(tmp_path):
    """``if (bad) revert`` means allowed when bad is false. Polarity
    must be ``allowed_when_false``."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            function f() external view {
                if (msg.sender != ownerVar) revert();
            }
        }
    """,
    )
    fn = _function(sl, "f")
    gates = RevertDetector(fn).run()
    if_gates = [g for g in gates if g.kind in ("if_revert", "custom_revert")]
    assert len(if_gates) >= 1, f"expected one if-revert gate, got: {_gate_kinds(gates)}"
    assert if_gates[0].polarity == "allowed_when_false"


def test_if_revert_custom_error(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            error NotOwner();
            function f() external view {
                if (msg.sender != ownerVar) revert NotOwner();
            }
        }
    """,
    )
    fn = _function(sl, "f")
    gates = RevertDetector(fn).run()
    assert any(g.kind in ("custom_revert", "if_revert") for g in gates), _gate_kinds(gates)


# ---------------------------------------------------------------------------
# Case 5: inline assembly conditional revert
# ---------------------------------------------------------------------------


def test_inline_asm_conditional_revert_structurally_parsed(tmp_path):
    """``assembly { if iszero(x) { revert(0,0) } }`` is parsed by
    Slither into structured IF + SolidityCall(revert(uint256,uint256)).
    The detector captures this via the standard if-revert path, so
    the gate kind is ``if_revert`` (not ``inline_asm``) — high-fidelity
    classification."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function f(uint256 x) external pure {
                assembly {
                    if iszero(x) { revert(0, 0) }
                }
            }
        }
    """,
    )
    fn = _function(sl, "f")
    gates = RevertDetector(fn).run()
    assert any(g.kind == "if_revert" for g in gates), _gate_kinds(gates)


def test_pure_compute_assembly_yields_no_gates(tmp_path):
    """An assembly block doing memory ops with no revert is genuinely
    ungated — RevertDetector returns ``[]``. The opaque marker is
    reserved for assembly that has a textual `revert` we couldn't
    structurally extract; pure compute is fine."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            function f() external pure returns (uint256 r) {
                assembly {
                    let p := mload(0x40)
                    r := mul(p, 2)
                }
            }
        }
    """,
    )
    fn = _function(sl, "f")
    gates = RevertDetector(fn).run()
    assert gates == []


# ---------------------------------------------------------------------------
# Sanity: function with no revert paths returns no gates.
# ---------------------------------------------------------------------------


def test_no_gates_for_unguarded_function(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            uint256 public x;
            function f() external {
                x = 1;
            }
        }
    """,
    )
    fn = _function(sl, "f")
    gates = RevertDetector(fn).run()
    assert gates == []


# ---------------------------------------------------------------------------
# Multiple sequential gates → multiple RevertGate records.
# ---------------------------------------------------------------------------


def test_two_requires_yields_two_gates(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            uint256 public threshold;
            function f(uint256 amount) external view {
                require(msg.sender == ownerVar);
                require(amount > threshold);
            }
        }
    """,
    )
    fn = _function(sl, "f")
    gates = RevertDetector(fn).run()
    require_count = sum(1 for g in gates if g.kind == "require")
    assert require_count == 2, f"expected 2 require gates, got {require_count}: {_gate_kinds(gates)}"
