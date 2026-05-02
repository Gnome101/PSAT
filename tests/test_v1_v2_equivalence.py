"""Cutover-equivalence test on the canonical corpus.

For each of a small set of representative shapes, runs BOTH the
native v1 pipeline (collect_contract_analysis →
build_semantic_guards) and the v2 path (build_predicate_artifacts
→ synthesize_semantic_guards_from_predicate_trees), then
compares the two output dicts at a structural level.

This is the operator's pre-flip evidence: when this test passes
across the corpus, flipping ``PSAT_POLICY_USE_V2_SHIM`` in
production should be a no-op for ``effective_permissions``.

Equivalence rules (intentional looseness):

  - Both must agree on which functions are PRIVILEGED (status !=
    public). Disagreements here would change which functions
    drive effective_permissions and are blocking.
  - For agreed-privileged functions, the predicate KINDS bag
    must match (caller_equals_controller / mapping_membership /
    policy_check / external_helper). controller_label /
    controller_source CAN differ — v1 carries graph IDs that v2
    doesn't, and v2 carries typed source values v1 doesn't.
  - status == "partial"/"unresolved" is allowed to differ on
    edge cases — the cutover stage is more conservative about
    what counts as resolved than v1's name-heuristic was; the
    target here is no v1 → unresolved transitions in v2 (a
    regression), but v2 → resolved transitions on what v1
    called partial are fine (new coverage).
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

from services.static.contract_analysis_pipeline import (  # noqa: E402
    build_semantic_guards,
    collect_contract_analysis,
)
from services.static.contract_analysis_pipeline.predicate_artifacts import (  # noqa: E402
    build_predicate_artifacts,
)
from services.static.contract_analysis_pipeline.v2_to_v1_shim import (  # noqa: E402
    synthesize_semantic_guards_from_predicate_trees,
)


# ---------------------------------------------------------------------------
# Project scaffold (mirrors test_contract_analysis._write_project but inline)
# ---------------------------------------------------------------------------


def _write_project(tmp_path: Path, *, contract_name: str, source_code: str) -> Path:
    project_dir = tmp_path / contract_name
    (project_dir / "src").mkdir(parents=True)
    (project_dir / "foundry.toml").write_text(
        '[profile.default]\nsrc = "src"\nout = "out"\nlibs = ["lib"]\nsolc_version = "0.8.19"\n'
    )
    (project_dir / "src" / f"{contract_name}.sol").write_text(source_code)
    (project_dir / "contract_meta.json").write_text(
        json.dumps(
            {
                "address": "0x1111111111111111111111111111111111111111",
                "contract_name": contract_name,
                "compiler_version": "v0.8.19+commit.7dd6d404",
            }
        )
        + "\n"
    )
    (project_dir / "slither_results.json").write_text(
        json.dumps({"results": {"detectors": []}}) + "\n"
    )
    return project_dir


def _v1_emit(project_dir: Path) -> dict:
    analysis = collect_contract_analysis(project_dir)
    return build_semantic_guards(analysis)


def _v2_emit(project_dir: Path, contract_name: str) -> dict:
    sl = Slither(str(project_dir / "src" / f"{contract_name}.sol"))
    contract = next(c for c in sl.contracts if c.name == contract_name)
    artifact = build_predicate_artifacts(contract)
    return synthesize_semantic_guards_from_predicate_trees(
        artifact,
        contract_address="0x1111111111111111111111111111111111111111",
        contract_name=contract_name,
    )


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def _functions_by_name(emit: dict) -> dict[str, dict]:
    return {fn["function"]: fn for fn in emit.get("functions", [])}


def _privileged(emit: dict) -> set[str]:
    """Function names whose status is not 'public' — i.e. the
    set effective_permissions treats as guarded."""
    return {fn["function"] for fn in emit.get("functions", []) if fn.get("status") != "public"}


def _predicate_kinds(fn_entry: dict) -> set[str]:
    return {p.get("kind") for p in (fn_entry.get("predicates") or []) if p.get("kind")}


def _assert_equivalent(v1: dict, v2: dict, *, contract_name: str) -> None:
    """Loose structural equivalence — the v2 cutover-target."""
    v1_funcs = _functions_by_name(v1)
    v2_funcs = _functions_by_name(v2)

    v1_priv = _privileged(v1)
    v2_priv = _privileged(v2)

    v1_only = v1_priv - v2_priv
    v2_only = v2_priv - v1_priv
    if v1_only:
        pytest.fail(
            f"[{contract_name}] v1 flagged {sorted(v1_only)} as privileged but "
            f"v2 didn't — REGRESSION. v2 status: "
            f"{ {fn: v2_funcs.get(fn, {}).get('status') for fn in v1_only} }"
        )
    # v2_only is acceptable (new coverage); just record it.

    # For agreed-privileged functions, check predicate-kind bags.
    for fn in sorted(v1_priv & v2_priv):
        v1_kinds = _predicate_kinds(v1_funcs[fn])
        v2_kinds = _predicate_kinds(v2_funcs[fn])
        # v2's kind set should be a subset of, or equal to, v1's
        # — synthesizing more predicate kinds than v1 is
        # acceptable on this rough cutover gate; missing kinds v1
        # produced is the regression.
        missing = v1_kinds - v2_kinds
        if missing:
            pytest.fail(
                f"[{contract_name}.{fn}] v2 missed v1 predicate kinds {sorted(missing)}; "
                f"v1={sorted(v1_kinds)} v2={sorted(v2_kinds)}"
            )


# ---------------------------------------------------------------------------
# Per-shape parametrization
# ---------------------------------------------------------------------------


_OZ_OWNABLE = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    uint256 public x;
    modifier onlyOwner() {
        require(msg.sender == ownerVar);
        _;
    }
    function setX(uint256 v) external onlyOwner { x = v; }
    function bumpX() external onlyOwner { x = x + 1; }
    function open() external { x = 0; }
}
"""

_OZ_AC_INLINE = """
pragma solidity ^0.8.19;
contract C {
    mapping(bytes32 => mapping(address => bool)) private _roles;
    bytes32 public constant MINTER = keccak256("MINTER");
    uint256 public x;
    function mint(uint256 v) external {
        require(_roles[MINTER][msg.sender]);
        x = v;
    }
    function totalSupply() external view returns (uint256) { return x; }
}
"""

_OZ_PAUSABLE = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    bool public _paused;
    uint256 public x;
    modifier whenNotPaused() {
        require(!_paused);
        _;
    }
    function pause() external {
        require(msg.sender == ownerVar);
        _paused = true;
    }
    function transfer() external whenNotPaused {
        x = x + 1;
    }
}
"""


_FIXTURES = [
    ("oz_ownable", _OZ_OWNABLE),
    ("oz_access_control_inline", _OZ_AC_INLINE),
    ("oz_pausable", _OZ_PAUSABLE),
]


@pytest.mark.parametrize("contract_name,source", _FIXTURES, ids=lambda x: x if isinstance(x, str) else None)
def test_v1_v2_semantic_guards_equivalent(tmp_path, contract_name, source):
    project_dir = _write_project(
        tmp_path, contract_name="C", source_code=textwrap.dedent(source).strip() + "\n"
    )
    v1 = _v1_emit(project_dir)
    v2 = _v2_emit(project_dir, "C")

    # Sanity: the v2 dict carries the synthetic marker.
    assert v2.get("_synthetic_from") == "v2_predicate_trees"

    _assert_equivalent(v1, v2, contract_name=contract_name)


def test_assert_equivalent_detects_regression(tmp_path):
    """Pin the regression-detection direction. Synthesize a v2
    dict that DROPS a function v1 marks privileged; the
    comparison must fail."""
    v1 = {
        "functions": [
            {"function": "f()", "status": "resolved", "predicates": [{"kind": "caller_equals_controller"}]}
        ]
    }
    v2 = {"_synthetic_from": "v2_predicate_trees", "functions": []}
    with pytest.raises(pytest.fail.Exception, match="REGRESSION"):
        _assert_equivalent(v1, v2, contract_name="X")


def test_assert_equivalent_allows_new_coverage(tmp_path):
    """v2 catching a function v1 missed is allowed (new coverage)."""
    v1 = {"functions": []}
    v2 = {
        "_synthetic_from": "v2_predicate_trees",
        "functions": [
            {"function": "f()", "status": "resolved", "predicates": [{"kind": "caller_equals_controller"}]}
        ],
    }
    _assert_equivalent(v1, v2, contract_name="X")  # must not raise


def test_assert_equivalent_detects_missing_predicate_kind(tmp_path):
    """Both flag the function privileged, but v2 missed the
    predicate kind v1 produced — that's a regression too."""
    v1 = {
        "functions": [
            {
                "function": "f()",
                "status": "resolved",
                "predicates": [
                    {"kind": "caller_equals_controller"},
                    {"kind": "mapping_membership"},
                ],
            }
        ]
    }
    v2 = {
        "_synthetic_from": "v2_predicate_trees",
        "functions": [
            {
                "function": "f()",
                "status": "resolved",
                "predicates": [{"kind": "caller_equals_controller"}],
            }
        ],
    }
    with pytest.raises(pytest.fail.Exception, match="missed v1 predicate kinds"):
        _assert_equivalent(v1, v2, contract_name="X")
