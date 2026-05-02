"""Tests for ``build_predicate_tree`` (services.static…predicates).

End-to-end: from a Solidity function source through ProvenanceEngine +
RevertDetector to a fully-typed PredicateTree. Focuses on:
  - basic equality + membership leaves
  - polarity normalization (require vs if-revert)
  - authority_role classification (Rule A: caller equality;
    Rule B: auth-shaped membership for multi-key)
  - 1-key caller-only membership defaults to business (week-3
    writer-key two-pass promotes if applicable)
  - external_bool delegated_authority (state-var target + sender arg)
  - unguarded function returns None
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


def _all_leaves(tree):
    """Flatten a PredicateTree into its LEAF nodes."""
    if tree is None:
        return []
    if tree.get("op") == "LEAF":
        leaf = tree.get("leaf")
        return [leaf] if leaf else []
    out = []
    for child in tree.get("children") or []:
        out.extend(_all_leaves(child))
    return out


# ---------------------------------------------------------------------------
# Equality leaves
# ---------------------------------------------------------------------------


def test_caller_equals_state_var_classifies_caller_authority(tmp_path):
    """``require(msg.sender == owner)`` is the canonical Rule A
    case: equality, op=eq, msg_sender vs state_variable."""
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
    tree = build_predicate_tree(fn)
    leaves = _all_leaves(tree)
    assert len(leaves) == 1, leaves
    leaf = leaves[0]
    assert leaf["kind"] == "equality"
    assert leaf["operator"] == "eq"
    assert leaf["authority_role"] == "caller_authority"
    assert leaf["references_msg_sender"] is True


def test_if_revert_inverts_operator(tmp_path):
    """``if (msg.sender != owner) revert()`` — polarity
    allowed_when_false. After normalization the leaf is
    equality, op=eq (the original ``!=`` is flipped via the
    polarity rule, NOT via a NOT node)."""
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
    tree = build_predicate_tree(fn)
    leaves = _all_leaves(tree)
    assert len(leaves) == 1
    leaf = leaves[0]
    # Source: if (a != b) revert  ⇒ allowed when a == b.
    # ne with allowed_when_false flips to eq. caller_authority because
    # one operand is msg.sender, the other is state_var.
    assert leaf["kind"] == "equality"
    assert leaf["operator"] == "eq"
    assert leaf["authority_role"] == "caller_authority"


def test_caller_equals_parameter_classifies_caller_authority(tmp_path):
    """``require(account == msg.sender)`` (renounceRole-style) — the
    other operand is a parameter (an address-typed parameter is
    treated as 'who is allowed', so this is caller_authority)."""
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
    fn = _function(sl, "renounce")
    tree = build_predicate_tree(fn)
    leaves = _all_leaves(tree)
    assert len(leaves) == 1
    leaf = leaves[0]
    assert leaf["kind"] == "equality"
    assert leaf["operator"] == "eq"
    assert leaf["authority_role"] == "caller_authority"


# ---------------------------------------------------------------------------
# Membership leaves
# ---------------------------------------------------------------------------


def test_two_key_membership_with_caller_promotes_to_caller_authority(tmp_path):
    """``require(_members[role][msg.sender])`` is a 2-key mapping
    with msg.sender as a key — Rule B's multi-key direct promotion
    to caller_authority (a permission table by structure)."""
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
    fn = _function(sl, "f")
    tree = build_predicate_tree(fn)
    leaves = _all_leaves(tree)
    assert len(leaves) == 1
    leaf = leaves[0]
    assert leaf["kind"] == "membership"
    assert leaf["operator"] == "truthy"
    assert leaf["authority_role"] == "caller_authority"


def test_one_key_caller_membership_defaults_to_business(tmp_path):
    """``require(claimed[msg.sender])`` is a 1-key caller-only bool
    map — could be auth (blacklist) or business (claim flag).
    Without writer-key analysis (week 3), default to business so we
    don't over-admit."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            mapping(address => bool) claimed;
            function f() external view {
                require(claimed[msg.sender]);
            }
        }
    """,
    )
    fn = _function(sl, "f")
    tree = build_predicate_tree(fn)
    leaves = _all_leaves(tree)
    assert len(leaves) == 1
    leaf = leaves[0]
    assert leaf["kind"] == "membership"
    assert leaf["authority_role"] == "business"


# ---------------------------------------------------------------------------
# Multiple gates → AND tree
# ---------------------------------------------------------------------------


def test_two_requires_combine_via_and(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            uint256 public minAmount;
            function f(uint256 amount) external view {
                require(msg.sender == ownerVar);
                require(amount > minAmount);
            }
        }
    """,
    )
    fn = _function(sl, "f")
    tree = build_predicate_tree(fn)
    assert tree is not None
    assert tree["op"] == "AND"
    assert len(tree["children"]) == 2
    leaves = _all_leaves(tree)
    kinds = sorted(leaf["kind"] for leaf in leaves)
    assert kinds == ["comparison", "equality"]


# ---------------------------------------------------------------------------
# Unguarded function
# ---------------------------------------------------------------------------


def test_unguarded_function_returns_none(tmp_path):
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
    tree = build_predicate_tree(fn)
    assert tree is None
