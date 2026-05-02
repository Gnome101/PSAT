"""Unit tests for the v1/v2 cutover-gate diff harness.

Two layers of coverage:

  * Synthetic dicts exercising every branch of ``classify_diff_severity``
    (``regression`` / ``new_coverage`` / ``role_drift`` / ``clean``).
  * End-to-end against the real v1 + v2 pipelines: compile a
    fixture contract, build both outputs, diff them, assert the
    result is sane (no v1_only regressions on the canonical
    OZ patterns).
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

slither = pytest.importorskip("slither")
from slither import Slither  # noqa: E402

from services.static.contract_analysis_pipeline.predicate_artifacts import (  # noqa: E402
    build_predicate_artifacts,
)
from services.static.contract_analysis_pipeline.v1_v2_diff import (  # noqa: E402
    DiffReport,
    classify_diff_severity,
    diff_artifacts,
)


# ---------------------------------------------------------------------------
# Synthetic dict tests
# ---------------------------------------------------------------------------


def _v1(name: str, *fns_with_kinds: tuple[str, list[str]]) -> dict:
    return {
        "subject": {"name": name},
        "access_control": {
            "privileged_functions": [
                {"function": fn, "guard_kinds": list(kinds)}
                for fn, kinds in fns_with_kinds
            ]
        },
    }


def _v2(name: str, *fns_with_roles: tuple[str, list[str]]) -> dict:
    def _tree(roles: list[str]) -> dict:
        leaf = {
            "kind": "equality",
            "operator": "eq",
            "authority_role": roles[0] if roles else "business",
            "operands": [],
            "references_msg_sender": True,
            "parameter_indices": [],
            "expression": "",
            "basis": [],
        }
        return {"op": "LEAF", "leaf": leaf}

    return {
        "schema_version": "v2",
        "contract_name": name,
        "trees": {fn: _tree(list(roles)) for fn, roles in fns_with_roles},
    }


def test_clean_when_v1_v2_exactly_match():
    v1 = _v1("C", ("f()", ["access_control"]))
    v2 = _v2("C", ("f()", ["caller_authority"]))
    rep = diff_artifacts(v1, v2)
    assert classify_diff_severity(rep) == "clean"
    assert rep.agreed == {"f()"}
    assert rep.v1_only == set()
    assert rep.v2_only == set()


def test_regression_when_v1_flags_more_than_v2():
    """v1 saw a guard the v2 pipeline missed — this MUST block the
    cutover until the v2 pipeline extends to cover that case."""
    v1 = _v1("C", ("f()", ["access_control"]), ("g()", ["pause"]))
    v2 = _v2("C", ("f()", ["caller_authority"]))
    rep = diff_artifacts(v1, v2)
    assert classify_diff_severity(rep) == "regression"
    assert rep.v1_only == {"g()"}


def test_new_coverage_when_v2_catches_gates_v1_missed():
    """v2 catches gates v1 missed — safe to cut over (the v1
    pipeline was loose; v2 is strictly more thorough)."""
    v1 = _v1("C", ("f()", ["access_control"]))
    v2 = _v2("C", ("f()", ["caller_authority"]), ("g()", ["caller_authority"]))
    rep = diff_artifacts(v1, v2)
    assert classify_diff_severity(rep) == "new_coverage"
    assert rep.v2_only == {"g()"}


def test_role_drift_when_classification_differs():
    """Both schemas agree the function is guarded, but v2 calls it
    pause while v1 called it access_control. Surface for review."""
    v1 = _v1("C", ("f()", ["access_control"]))
    v2 = _v2("C", ("f()", ["pause"]))
    rep = diff_artifacts(v1, v2)
    assert classify_diff_severity(rep) == "role_drift"
    assert "f()" in rep.role_disagreements
    v1_kinds, v2_roles = rep.role_disagreements["f()"]
    assert v1_kinds == ["access_control"]
    assert v2_roles == ["pause"]


def test_unknown_v1_kind_does_not_trip_role_drift():
    """If v1 emitted a guard_kind we don't have a mapping for, we
    treat it as 'unrecognized' rather than flagging role_drift —
    avoids spurious noise from new v1 kinds the diff hasn't been
    taught about."""
    v1 = _v1("C", ("f()", ["mystery_kind"]))
    v2 = _v2("C", ("f()", ["caller_authority"]))
    rep = diff_artifacts(v1, v2)
    assert classify_diff_severity(rep) == "clean"


def test_subject_name_falls_back_to_v1():
    """If the v2 artifact didn't carry a contract_name (older
    artifacts), pick it up from v1."""
    v1 = _v1("MyContract", ("f()", ["access_control"]))
    v2 = {"schema_version": "v2", "trees": {"f()": _v2("X", ("f()", ["caller_authority"]))["trees"]["f()"]}}
    rep = diff_artifacts(v1, v2)
    assert rep.contract_name == "MyContract"


# ---------------------------------------------------------------------------
# End-to-end against real v1+v2 outputs
# ---------------------------------------------------------------------------


def _compile(tmp_path: Path, source: str) -> Slither:
    src = textwrap.dedent(source).strip() + "\n"
    f = tmp_path / "C.sol"
    f.write_text(src)
    return Slither(str(f))


def test_e2e_oz_ownable_no_regression(tmp_path):
    """Canonical OZ Ownable: a single ``onlyOwner`` modifier guard.
    The v2 artifact must capture this — no v1_only regression."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            uint256 public x;
            modifier onlyOwner() {
                require(msg.sender == ownerVar);
                _;
            }
            function f() external onlyOwner {
                x = 1;
            }
            function g() external {
                x = 2;
            }
        }
    """,
    )
    contract = sl.contracts[0]

    # Build a synthetic v1 that mirrors what the legacy stack
    # would emit for this shape (privileged_functions: f, not g).
    v1 = _v1("C", ("f()", ["access_control"]))
    v2 = build_predicate_artifacts(contract)

    rep = diff_artifacts(v1, v2)
    assert "f()" in rep.agreed
    assert rep.v1_only == set(), f"v2 regressed on Ownable: {rep.v1_only}"


def test_e2e_pausable_classifies_as_pause_not_regression(tmp_path):
    """Pausable's whenNotPaused modifier on transfer() — both
    schemas should flag it. v2 classifies as 'pause' specifically;
    v1 likely calls it 'pause' too, so it's clean. If v1 used a
    different label the diff classifies role_drift, not regression."""
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
    v1 = _v1("C", ("transfer()", ["pause"]), ("pause()", ["access_control"]))
    v2 = build_predicate_artifacts(sl.contracts[0])
    rep = diff_artifacts(v1, v2)
    assert "transfer()" in rep.agreed
    assert "pause()" in rep.agreed
    assert classify_diff_severity(rep) in ("clean", "new_coverage")


def test_e2e_unguarded_function_is_v2_absent(tmp_path):
    """An open function ``g() { x = 2; }`` is correctly omitted
    from v2's trees dict and shouldn't appear in the diff at all.
    Pinned because earlier shadow-mode iterations had a temptation
    to emit a null leaf for unguarded — the convention is OMIT."""
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            uint256 public x;
            function g() external { x = 1; }
        }
    """,
    )
    v1 = {"subject": {"name": "C"}, "access_control": {"privileged_functions": []}}
    v2 = build_predicate_artifacts(sl.contracts[0])
    assert "g()" not in v2["trees"]
    rep = diff_artifacts(v1, v2)
    assert classify_diff_severity(rep) == "clean"
