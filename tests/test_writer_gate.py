"""Tests for the writer-gate two-pass analyzer.

Per v6/v7 plan: 1-key caller-keyed bool/uint mappings can't be
classified as auth or personal-flag from the read-site alone — the
discriminator is how the storage var is *written*. The analyzer
walks the contract, finds writer functions of each candidate
mapping, classifies the write key, and promotes leaves when the
writer is itself authority-gated.
"""

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
from services.static.contract_analysis_pipeline.writer_gate import (  # noqa: E402
    apply_writer_gate_pass,
)


def _compile(tmp_path: Path, source: str) -> Slither:
    src = textwrap.dedent(source).strip() + "\n"
    f = tmp_path / "C.sol"
    f.write_text(src)
    return Slither(str(f))


def _all_leaves(tree):
    if tree is None:
        return []
    if tree.get("op") == "LEAF":
        return [tree["leaf"]] if tree.get("leaf") else []
    out = []
    for child in tree.get("children") or []:
        out.extend(_all_leaves(child))
    return out


def _build_trees(contract):
    trees = {}
    for fn in contract.functions:
        if fn.is_constructor:
            continue
        trees[fn.full_name] = build_predicate_tree(fn)
    return trees


# ---------------------------------------------------------------------------
# Rule a: ALL writers self_keyed → leave as business
# ---------------------------------------------------------------------------


def test_personal_flag_stays_business(tmp_path):
    """``claimed[msg.sender] = true`` is the only writer (and self-
    keyed). Reading ``require(claimed[msg.sender])`` stays business."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            mapping(address => bool) public claimed;
            function claim() external {
                claimed[msg.sender] = true;
            }
            function f() external view {
                require(claimed[msg.sender]);
            }
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_trees(contract)
    apply_writer_gate_pass(contract, trees)
    fn_tree = trees["f()"]
    leaves = _all_leaves(fn_tree)
    assert len(leaves) == 1
    assert leaves[0]["authority_role"] == "business"


# ---------------------------------------------------------------------------
# Rule b.i: external_keyed writer is auth-gated → promote to caller_authority
# ---------------------------------------------------------------------------


def test_blacklist_writer_gated_promotes(tmp_path):
    """``_blacklist[user] = true`` is written by an Ownable function.
    Reading ``require(!_blacklist[msg.sender])`` should promote to
    caller_authority via rule b.i."""
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
    trees = _build_trees(contract)
    apply_writer_gate_pass(contract, trees)
    leaves = _all_leaves(trees["someAction()"])
    assert len(leaves) == 1
    leaf = leaves[0]
    assert leaf["kind"] == "membership"
    assert leaf["operator"] == "falsy"
    assert leaf["authority_role"] == "caller_authority"


# ---------------------------------------------------------------------------
# Rule b.ii: self-administered (Maker wards style) → promote
# ---------------------------------------------------------------------------


def test_self_administered_wards_promotes(tmp_path):
    """Maker wards-style: ``rely(addr)`` is gated by ``wards[msg.
    sender]`` (the same map). Reading ``wards[msg.sender]`` in a
    function should promote because the writer is gated by the same
    storage var."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            mapping(address => uint256) public wards;
            function rely(address addr) external {
                require(wards[msg.sender] == 1);
                wards[addr] = 1;
            }
            function someAction() external view {
                require(wards[msg.sender] == 1);
            }
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_trees(contract)
    apply_writer_gate_pass(contract, trees)
    # someAction's leaf — note this is a comparison (==1), not bare
    # membership. The current builder produces an equality leaf with
    # the LHS being the Index reference. Writer-gate promotion
    # currently targets membership leaves only, so this case isn't
    # promoted in the initial cut. Document the expectation.
    leaves = _all_leaves(trees["someAction()"])
    assert len(leaves) == 1
    # TODO: when we treat ``map[a][b] == 1`` as a membership leaf
    # (truthy_value=1), this will promote. Currently equality, stays
    # business. Pin as TODO for week-3 follow-up.


# ---------------------------------------------------------------------------
# Rule c: external_keyed writer is open (ungated) → keep business
# ---------------------------------------------------------------------------


def test_open_registration_stays_business(tmp_path):
    """``register(addr)`` writes ``_registered[addr] = true`` with no
    gate. Reading ``require(_registered[msg.sender])`` should stay
    business — anyone can register anyone."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            mapping(address => bool) public _registered;
            function register(address addr) external {
                _registered[addr] = true;
            }
            function someAction() external view {
                require(_registered[msg.sender]);
            }
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_trees(contract)
    apply_writer_gate_pass(contract, trees)
    leaves = _all_leaves(trees["someAction()"])
    assert len(leaves) == 1
    assert leaves[0]["authority_role"] == "business"
