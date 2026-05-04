"""End-to-end tests: Solidity source → PredicateTree → CapabilityExpr.

Covers the bridge from week-2 (predicate builder) through week-3
(reentrancy/pause + writer-gate) to week-4 (capability evaluator)."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

slither = pytest.importorskip("slither")
from slither import Slither  # noqa: E402

from services.resolution.predicate_evaluator import (  # noqa: E402
    evaluate_tree,
)
from services.static.contract_analysis_pipeline.predicates import (  # noqa: E402
    build_predicate_tree,
)
from services.static.contract_analysis_pipeline.reentrancy_pause import (  # noqa: E402
    apply_reentrancy_pause_pass,
)
from services.static.contract_analysis_pipeline.writer_gate import (  # noqa: E402
    apply_writer_gate_pass,
)


def _compile(tmp_path: Path, source: str) -> Slither:
    src = textwrap.dedent(source).strip() + "\n"
    f = tmp_path / "C.sol"
    f.write_text(src)
    return Slither(str(f))


def _build_pipeline(contract):
    """Run the full week-1-through-3 pipeline to produce per-function
    PredicateTrees with classification mutations applied."""
    trees = {}
    for fn in contract.functions:
        if fn.is_constructor:
            continue
        trees[fn.full_name] = build_predicate_tree(fn)
    apply_writer_gate_pass(contract, trees)
    apply_reentrancy_pause_pass(contract, trees)
    return trees


# ---------------------------------------------------------------------------
# End-to-end pipeline tests
# ---------------------------------------------------------------------------


def test_unguarded_function_yields_conditional_universal(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            uint256 public x;
            function f() external { x = 1; }
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_pipeline(contract)
    cap = evaluate_tree(trees["f()"])
    assert cap.kind == "conditional_universal"


def test_caller_equals_state_var_yields_finite_set_placeholder(tmp_path):
    """``require(msg.sender == owner)`` resolves to a finite_set
    placeholder (members empty until week-5 adapter reads
    state_variable on-chain). Confidence=partial reflects the gap."""
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
    contract = sl.contracts[0]
    trees = _build_pipeline(contract)
    cap = evaluate_tree(trees["f()"])
    assert cap.kind == "finite_set"
    assert cap.confidence == "partial"
    assert cap.membership_quality == "lower_bound"


def test_renounce_role_self_service_pattern(tmp_path):
    """``require(account == msg.sender)`` — the canonical self-service
    pattern. Resolves to conditional_universal(self_service)."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            uint256 public x;
            function renounce(address account) external {
                require(account == msg.sender);
                x = 1;
            }
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_pipeline(contract)
    cap = evaluate_tree(trees["renounce(address)"])
    assert cap.kind == "conditional_universal"
    assert any(c.kind == "self_service" for c in cap.conditions)


def test_or_owner_or_business_yields_structural_or(tmp_path):
    """``require(msg.sender == owner || amount > cap)`` → OR root in
    the predicate tree → structural OR in the capability (per v3
    blocker #2 fix: business preserved under OR)."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            uint256 public cap;
            uint256 public x;
            function f(uint256 amount) external {
                require(msg.sender == ownerVar || amount > cap);
                x = amount;
            }
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_pipeline(contract)
    cap = evaluate_tree(trees["f(uint256)"])
    assert cap.kind == "OR"
    # finite_set (owner-resolved placeholder) + conditional_universal (business amount cap).
    assert len(cap.children) == 2
    kinds = sorted(c.kind for c in cap.children)
    assert "conditional_universal" in kinds


def test_two_keys_membership_yields_finite_set_lower(tmp_path):
    """``require(_members[role][msg.sender])`` — 2-key direct-promote
    to caller_authority. Without an adapter, the evaluator returns
    a lower_bound finite_set placeholder."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            mapping(bytes32 => mapping(address => bool)) _members;
            function f(bytes32 role) external view {
                require(_members[role][msg.sender]);
            }
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_pipeline(contract)
    cap = evaluate_tree(trees["f(bytes32)"])
    assert cap.kind == "finite_set"
    assert cap.confidence == "partial"
    assert cap.membership_quality == "lower_bound"


def test_negated_membership_partial_yields_unsupported(tmp_path):
    """``require(!_blacklist[msg.sender])`` (writer-gate b.i admin-
    written). The adapter returns an empty lower_bound finite_set
    placeholder; negating a partial-quality finite_set is unsound
    (we don't know the full member list, so we can't safely express
    its complement). Negate emits unsupported(negate_partial_set).
    Week-5 adapter fills in the real list, after which negation is
    sound and the leaf becomes cofinite_blacklist."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            mapping(address => bool) public _blacklist;
            function setBlacklist(address user, bool val) external {
                require(msg.sender == ownerVar);
                _blacklist[user] = val;
            }
            function someAction() external view {
                require(!_blacklist[msg.sender]);
            }
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_pipeline(contract)
    cap = evaluate_tree(trees["someAction()"])
    # Pre-adapter: negate(lower_bound finite_set) → unsupported.
    assert cap.kind == "unsupported"
    assert cap.unsupported_reason is not None
    assert "negate_partial_set" in cap.unsupported_reason


def test_external_bool_yields_check_only(tmp_path):
    """``require(authority.canCall(msg.sender))`` →
    external_check_only capability with placeholder check info."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        interface IAuthority {
            function canCall(address) external view returns (bool);
        }
        contract C {
            IAuthority public authority;
            function f() external view {
                require(authority.canCall(msg.sender));
            }
        }
    """,
    )
    # Pick the implementing contract, not the interface.
    contract = next(c for c in sl.contracts if c.name == "C")
    trees = _build_pipeline(contract)
    cap = evaluate_tree(trees["f()"])
    assert cap.kind == "external_check_only"


def test_reentrancy_yields_conditional_universal(tmp_path):
    """A function with only a reentrancy guard yields
    conditional_universal — anyone, but reentrancy must hold."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            uint256 private _status;
            uint256 private constant _NOT_ENTERED = 1;
            uint256 private constant _ENTERED = 2;
            modifier nonReentrant() {
                require(_status != _ENTERED);
                _status = _ENTERED;
                _;
                _status = _NOT_ENTERED;
            }
            function f() external nonReentrant {}
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_pipeline(contract)
    cap = evaluate_tree(trees["f()"])
    assert cap.kind == "conditional_universal"
    assert any(c.kind == "reentrancy" for c in cap.conditions)


def test_signature_auth_yields_signature_witness(tmp_path):
    """``require(msg.sender == ecrecover(...))`` → signature_witness
    with the signer being whatever the signature is required to
    match (in this case, msg.sender — odd but valid)."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public signerAddr;
            function f(bytes32 h, uint8 v, bytes32 r, bytes32 s) external view {
                require(signerAddr == ecrecover(h, v, r, s));
            }
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_pipeline(contract)
    cap = evaluate_tree(trees["f(bytes32,uint8,bytes32,bytes32)"])
    assert cap.kind == "signature_witness"


# ---------------------------------------------------------------------------
# Direct evaluator tests (no Slither needed)
# ---------------------------------------------------------------------------


def test_evaluate_none_tree_yields_conditional_universal():
    cap = evaluate_tree(None)
    assert cap.kind == "conditional_universal"


def test_evaluator_dispatches_on_unsupported_first():
    """Per v6 round-5 #3 fix, kind == unsupported is checked before
    authority_role-based side-condition routing."""
    tree = {
        "op": "LEAF",
        "leaf": {
            "kind": "unsupported",
            "operator": "truthy",
            "authority_role": "business",  # would otherwise route to conditional_universal
            "operands": [],
            "unsupported_reason": "test_unsupported",
            "references_msg_sender": False,
            "parameter_indices": [],
            "expression": "",
            "basis": [],
        },
    }
    cap = evaluate_tree(tree)  # type: ignore[arg-type]
    assert cap.kind == "unsupported"
    assert cap.unsupported_reason == "test_unsupported"
