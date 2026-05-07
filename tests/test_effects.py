"""Tests for ``build_effects``.

End-to-end: compile a Solidity fixture, run the effects builder, and
assert sink discovery + label inference + the v1-superset property.
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

from services.static.contract_analysis_pipeline.effects import (  # noqa: E402
    SCHEMA_VERSION,
    EffectInfo,
    EffectsArtifact,
    build_effects,
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


def _info(artifact: EffectsArtifact, signature: str) -> EffectInfo:
    info = artifact["functions"].get(signature)
    assert info is not None, f"expected {signature} in {sorted(artifact['functions'])}"
    return info


def test_basic_state_write_emits_state_write_sink(tmp_path):
    """A bare setter writing one storage slot produces exactly one
    state_write sink targeting that slot."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            uint256 public x;
            function setX(uint256 v) external {
                x = v;
            }
        }
        """,
    )
    artifact = build_effects(_contract(sl))

    assert artifact["schema_version"] == SCHEMA_VERSION
    assert artifact["contract_name"] == "C"

    info = _info(artifact, "setX(uint256)")
    state_writes = [s for s in info["sinks"] if s["kind"] == "state_write"]
    assert len(state_writes) == 1
    assert state_writes[0]["target"] == "x"
    assert "x" in info["effect_targets"]
    # Selector + writer_selectors populated for state writers.
    assert info["selector"].startswith("0x") and len(info["selector"]) == 10
    assert info["writer_selectors"] == [info["selector"]]


def test_internal_helper_writes_surface_on_caller(tmp_path):
    """An external function that delegates writing to an internal
    helper still sees the helper's write in its sinks list."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            uint256 public x;
            uint256 public y;
            function _bumpInternally(uint256 v) internal {
                y = v;
            }
            function bump(uint256 v) external {
                x = v;
                _bumpInternally(v + 1);
            }
        }
        """,
    )
    artifact = build_effects(_contract(sl))
    info = _info(artifact, "bump(uint256)")
    targets = {s["target"] for s in info["sinks"] if s["kind"] == "state_write"}
    assert {"x", "y"}.issubset(targets), f"expected x and y, got {targets}"
    # Internal helpers don't appear as their own entry — their effects
    # propagate to external callers only.
    assert "_bumpInternally(uint256)" not in artifact["functions"]


def test_external_call_sink_classification(tmp_path):
    """A function calling into another contract emits an external_call
    sink with the dotted ``destVar.method`` target."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        interface IToken {
            function mint(address to, uint256 amount) external;
        }
        contract C {
            IToken public token;
            function poke(address to, uint256 amount) external {
                token.mint(to, amount);
            }
        }
        """,
    )
    artifact = build_effects(_contract(sl, "C"))
    info = _info(artifact, "poke(address,uint256)")
    external_calls = [s for s in info["sinks"] if s["kind"] == "external_call"]
    assert any(s["target"] == "token.mint" for s in external_calls), info["sinks"]


def test_effect_label_recognition_pause_toggle(tmp_path):
    """A function that writes a bool state-var read by a modifier
    gating other functions earns the ``pause_toggle`` label."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public owner;
            bool public stopped;
            modifier whenNotStopped() {
                require(!stopped, "stopped");
                _;
            }
            constructor() {
                owner = msg.sender;
            }
            function trip() external {
                require(msg.sender == owner);
                stopped = true;
            }
            function action() external whenNotStopped {
                // pretends to do work
            }
        }
        """,
    )
    artifact = build_effects(_contract(sl))
    info = _info(artifact, "trip()")
    assert "pause_toggle" in info["effect_labels"], info["effect_labels"]


def test_v2_effects_includes_unguarded_public_function(tmp_path):
    """Sanity: the v2 effects artifact MUST surface unguarded
    externally-callable functions (e.g. an unprotected ``publicSetter``).
    ``predicate_trees`` deliberately omits these (no revert path) but
    consumers still need to see the sink."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public owner;
            uint256 public x;
            uint256 public y;
            mapping(address => bool) public allowed;

            constructor() {
                owner = msg.sender;
            }

            modifier onlyOwner() {
                require(msg.sender == owner);
                _;
            }

            function setX(uint256 v) external onlyOwner { x = v; }
            function _setYInternally(uint256 v) internal { y = v; }
            function setBoth(uint256 a, uint256 b) external onlyOwner {
                x = a;
                _setYInternally(b);
            }
            function allow(address a) external onlyOwner { allowed[a] = true; }
            function publicSetter(uint256 v) external { x = v; }
        }
        """,
    )
    contract = _contract(sl, "C")
    v2 = build_effects(contract)

    assert "publicSetter(uint256)" in v2["functions"]
    public_setter = v2["functions"]["publicSetter(uint256)"]
    sinks = {(s["kind"], s["target"]) for s in public_setter["sinks"]}
    assert ("state_write", "x") in sinks

    # setBoth writes x directly and y transitively via the internal helper.
    set_both_sinks = {(s["kind"], s["target"]) for s in v2["functions"]["setBoth(uint256,uint256)"]["sinks"]}
    assert ("state_write", "x") in set_both_sinks
    assert ("state_write", "y") in set_both_sinks


def test_artifact_is_json_serializable(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            uint256 public x;
            function setX(uint256 v) external { x = v; }
        }
        """,
    )
    artifact = build_effects(_contract(sl))
    # Round-trips cleanly — no slither-bound objects leaking through.
    json.dumps(artifact)


def test_fallback_and_receive_included(tmp_path):
    """Per module docstring: fallback + receive are real sink-bearing
    surfaces and MUST be emitted, even though the predicate-tree
    builder skips them."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public impl;
            uint256 public ethBalance;
            receive() external payable { ethBalance = msg.value; }
            fallback() external {
                (bool ok, ) = impl.delegatecall(msg.data);
                require(ok);
            }
        }
        """,
    )
    artifact = build_effects(_contract(sl))
    fns = set(artifact["functions"].keys())
    # Slither typically emits fallback as ``fallback()`` and receive as
    # ``receive()``; allow both spellings just in case.
    assert any("fallback" in f for f in fns), fns
    assert any("receive" in f for f in fns), fns


def test_constructor_skipped(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public owner;
            constructor() { owner = msg.sender; }
            function poke() external {}
        }
        """,
    )
    artifact = build_effects(_contract(sl))
    assert not any(name.startswith("constructor") for name in artifact["functions"]), artifact["functions"]
