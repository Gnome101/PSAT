"""Tests for ReentrancyAnalyzer + PauseAnalyzer.

Validates the structural detection rules don't depend on identifier
names (so a renamed-equivalent contract classifies the same way as
the canonical OZ source)."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

slither = pytest.importorskip("slither")
from slither import Slither  # noqa: E402

from services.static.contract_analysis_pipeline.predicates import (  # noqa: E402
    build_predicate_tree,
)
from services.static.contract_analysis_pipeline.reentrancy_pause import (  # noqa: E402
    PauseAnalyzer,
    ReentrancyAnalyzer,
    apply_reentrancy_pause_pass,
)


def _compile(tmp_path: Path, source: str) -> Slither:
    src = textwrap.dedent(source).strip() + "\n"
    f = tmp_path / "C.sol"
    f.write_text(src)
    return Slither(str(f))


def _build_trees(contract):
    trees = {}
    for fn in contract.functions:
        if fn.is_constructor:
            continue
        trees[fn.full_name] = build_predicate_tree(fn)
    return trees


def _all_leaves(tree):
    if tree is None:
        return []
    if tree.get("op") == "LEAF":
        return [tree["leaf"]] if tree.get("leaf") else []
    out = []
    for child in tree.get("children") or []:
        out.extend(_all_leaves(child))
    return out


# ---------------------------------------------------------------------------
# ReentrancyAnalyzer
# ---------------------------------------------------------------------------


def test_canonical_oz_reentrancy_guard_detected(tmp_path):
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
    guards = ReentrancyAnalyzer(contract).run()
    assert "_status" in guards


def test_renamed_reentrancy_guard_detected(tmp_path):
    """Renamed-equivalent contract: same structural pattern, different
    identifier names. Detection must be name-free."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            uint256 private _foo;
            uint256 private constant _A = 1;
            uint256 private constant _B = 2;
            modifier myModifier() {
                require(_foo != _B);
                _foo = _B;
                _;
                _foo = _A;
            }
            function f() external myModifier {}
        }
    """,
    )
    contract = sl.contracts[0]
    guards = ReentrancyAnalyzer(contract).run()
    assert "_foo" in guards


def test_no_reentrancy_pattern_returns_empty(tmp_path):
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
    guards = ReentrancyAnalyzer(contract).run()
    assert guards == set()


# ---------------------------------------------------------------------------
# PauseAnalyzer
# ---------------------------------------------------------------------------


def test_canonical_oz_pause_detected(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            bool public _paused;
            modifier whenNotPaused() {
                require(!_paused);
                _;
            }
            function pause() external {
                require(msg.sender == ownerVar);
                _paused = true;
            }
            function someAction() external whenNotPaused {}
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_trees(contract)
    pause_vars = PauseAnalyzer(contract, trees).run()
    assert "_paused" in pause_vars


def test_renamed_pause_detected(tmp_path):
    """Renamed pattern: pause var is `flag`, modifier `gate`, admin
    function `freeze`. Detection name-free."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            bool public flag;
            modifier gate() {
                require(!flag);
                _;
            }
            function freeze() external {
                require(msg.sender == ownerVar);
                flag = true;
            }
            function someAction() external gate {}
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_trees(contract)
    pause_vars = PauseAnalyzer(contract, trees).run()
    assert "flag" in pause_vars


def test_unauth_writer_does_not_trigger_pause(tmp_path):
    """A bool toggled by anyone isn't a pause flag — needs an
    auth-gated writer."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            bool public _paused;
            function pause() external { _paused = true; }
            function someAction() external view {
                require(!_paused);
            }
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_trees(contract)
    pause_vars = PauseAnalyzer(contract, trees).run()
    assert pause_vars == set()


# ---------------------------------------------------------------------------
# Apply pass: leaves get reclassified
# ---------------------------------------------------------------------------


def test_apply_pass_classifies_reentrancy_leaf(tmp_path):
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
    trees = _build_trees(contract)
    apply_reentrancy_pause_pass(contract, trees)
    leaves = _all_leaves(trees["f()"])
    assert len(leaves) == 1
    leaf = leaves[0]
    # The leaf reads _status (via the modifier require) and should
    # now be classified as reentrancy.
    assert leaf["authority_role"] == "reentrancy", leaf


def test_apply_pass_classifies_pause_leaf(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            bool public _paused;
            modifier whenNotPaused() {
                require(!_paused);
                _;
            }
            function pause() external {
                require(msg.sender == ownerVar);
                _paused = true;
            }
            function someAction() external whenNotPaused {}
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_trees(contract)
    apply_reentrancy_pause_pass(contract, trees)
    leaves = _all_leaves(trees["someAction()"])
    assert len(leaves) == 1
    leaf = leaves[0]
    assert leaf["authority_role"] == "pause", leaf
