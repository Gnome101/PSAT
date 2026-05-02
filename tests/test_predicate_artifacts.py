"""Tests for ``build_predicate_artifacts``.

End-to-end: compile a Solidity fixture, run the artifact builder,
assert the trees include the expected functions, exclude unguarded
ones, and serialize cleanly via ``json.dumps``.
"""

from __future__ import annotations

import json
import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

slither = pytest.importorskip("slither")
from slither import Slither  # noqa: E402

from services.static.contract_analysis_pipeline.predicate_artifacts import (  # noqa: E402
    SCHEMA_VERSION,
    build_predicate_artifacts,
)


def _compile(tmp_path: Path, source: str) -> Slither:
    src = textwrap.dedent(source).strip() + "\n"
    f = tmp_path / "C.sol"
    f.write_text(src)
    return Slither(str(f))


def _contract(sl: Slither, name: str | None = None):
    if name is None:
        return sl.contracts[0]
    return next(c for c in sl.contracts if c.name == name)


def test_artifact_includes_only_guarded_external_functions(tmp_path):
    """The artifact dict has trees for guarded external/public
    functions and OMITS unguarded ones (resolver convention:
    absent = unguarded)."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            uint256 public x;
            function guarded() external {
                require(msg.sender == ownerVar);
                x = 1;
            }
            function open() external {
                x = 2;
            }
        }
    """,
    )
    artifact = build_predicate_artifacts(_contract(sl))
    assert artifact["schema_version"] == SCHEMA_VERSION
    assert artifact["contract_name"] == "C"
    assert "guarded()" in artifact["trees"]
    assert "open()" not in artifact["trees"]


def test_artifact_omits_internal_functions(tmp_path):
    """Internal/private helpers don't appear at the boundary the
    resolver consumes — only external/public surface."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            uint256 public x;
            function _helper() internal view {
                require(msg.sender == ownerVar);
            }
            function entry() external {
                _helper();
                x = 1;
            }
        }
    """,
    )
    artifact = build_predicate_artifacts(_contract(sl))
    assert "entry()" in artifact["trees"]
    assert "_helper()" not in artifact["trees"]


def test_artifact_omits_constructor(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            constructor(address o) {
                require(o != address(0));
                ownerVar = o;
            }
            function f() external {
                require(msg.sender == ownerVar);
            }
        }
    """,
    )
    artifact = build_predicate_artifacts(_contract(sl))
    # Constructors aren't part of the public ABI surface; the
    # artifact must omit them so the resolver doesn't conflate
    # construction-time gates with runtime gates.
    keys = list(artifact["trees"].keys())
    assert all("constructor" not in k for k in keys)
    assert "f()" in artifact["trees"]


def test_artifact_serializes_cleanly_to_json(tmp_path):
    """The trees use TypedDict shapes that ``json.dumps`` can
    handle. The set_descriptor's role bytes are encoded as
    str(constant_value) per the existing predicate builder, so the
    JSON roundtrip works end-to-end."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            mapping(bytes32 => mapping(address => bool)) private _roles;
            bytes32 constant MINTER = keccak256("MINTER");
            uint256 public x;
            function mint(uint256 v) external {
                require(_roles[MINTER][msg.sender]);
                x = v;
            }
        }
    """,
    )
    artifact = build_predicate_artifacts(_contract(sl))
    encoded = json.dumps(artifact)
    decoded = json.loads(encoded)
    assert decoded["schema_version"] == SCHEMA_VERSION
    assert "mint(uint256)" in decoded["trees"]


def test_artifact_writer_gate_runs_on_full_contract(tmp_path):
    """Writer-gate pass needs every function's tree to evaluate
    writer authority. The artifact builder runs it AFTER collecting
    all trees, so a 1-key blacklist promoted via writer-gate shows
    caller_authority in the output (not the un-promoted business)."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            mapping(address => bool) public _blacklist;
            uint256 public x;
            function setBlacklist(address u, bool v) external {
                require(msg.sender == ownerVar);
                _blacklist[u] = v;
            }
            function someAction() external {
                require(!_blacklist[msg.sender]);
                x = 1;
            }
        }
    """,
    )
    artifact = build_predicate_artifacts(_contract(sl))
    action_tree = artifact["trees"]["someAction()"]
    leaves: list = []

    def _walk(node):
        if node.get("op") == "LEAF":
            leaf = node.get("leaf")
            if leaf:
                leaves.append(leaf)
            return
        for c in node.get("children", []) or []:
            _walk(c)

    _walk(action_tree)
    assert leaves, "expected a membership leaf"
    assert leaves[0]["authority_role"] == "caller_authority"
    assert leaves[0]["confidence"] == "medium"  # rule b.i = MEDIUM


def test_artifact_runs_reentrancy_pause_pass(tmp_path):
    """OZ Pausable pattern — the v1 ``whenNotPaused`` modifier's
    leaf classifies as ``pause`` only after the cross-function
    pass. Confirms the artifact builder runs that pass."""
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
            function transfer() external whenNotPaused {}
        }
    """,
    )
    artifact = build_predicate_artifacts(_contract(sl))
    transfer_tree = artifact["trees"]["transfer()"]
    leaf = transfer_tree["leaf"]
    assert leaf["authority_role"] == "pause"
    assert leaf["confidence"] == "high"


def test_artifact_empty_contract(tmp_path):
    """An interface-only contract with no externally-callable
    body produces an empty trees dict (still valid)."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        interface IThing { function foo() external; }
    """,
    )
    artifact = build_predicate_artifacts(_contract(sl, "IThing"))
    assert artifact["trees"] == {}
    assert artifact["schema_version"] == SCHEMA_VERSION
