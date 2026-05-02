"""End-to-end equivalence test: ``build_effective_permissions`` output
must be structurally equivalent between the v1-native semantic_guards
path and the v2-shim-derived path.

This is the load-bearing cutover test. Equivalent kind-name bags
(test_v1_v2_equivalence.py) only verify the SHAPE of the synthetic
v1 dict the shim produces — they don't verify that the downstream
consumer (effective_permissions) produces the same authoritative
output. That's where the recent shim bugs hid:

  - Shim emitting controller_source=null for 1-key caller-only
    mappings caused effective_permissions to lose controllers in
    Maker wards / OZ Pausable / blacklist contracts. The kind-name
    bag still matched (mapping_membership both sides), but the
    resolved-controllers list differed.

This test catches that class of bug by comparing the actual
effective_permissions output dict per privileged function.
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

from services.policy.effective_permissions import build_effective_permissions  # noqa: E402
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
# Project scaffold (mirrors test_v1_v2_equivalence._write_project)
# ---------------------------------------------------------------------------


_TARGET_ADDRESS = "0x1111111111111111111111111111111111111111"
_OWNER_ADDRESS = "0x2222222222222222222222222222222222222222"


def _write_project(tmp_path: Path, *, source_code: str) -> Path:
    project_dir = tmp_path / "C"
    (project_dir / "src").mkdir(parents=True)
    (project_dir / "foundry.toml").write_text(
        '[profile.default]\nsrc = "src"\nout = "out"\nlibs = ["lib"]\nsolc_version = "0.8.19"\n'
    )
    (project_dir / "src" / "C.sol").write_text(source_code)
    (project_dir / "contract_meta.json").write_text(
        json.dumps(
            {
                "address": _TARGET_ADDRESS,
                "contract_name": "C",
                "compiler_version": "v0.8.19+commit.7dd6d404",
            }
        )
        + "\n"
    )
    (project_dir / "slither_results.json").write_text(
        json.dumps({"results": {"detectors": []}}) + "\n"
    )
    return project_dir


def _v1_emit(project_dir: Path):
    analysis = collect_contract_analysis(project_dir)
    return analysis, build_semantic_guards(analysis)


def _v2_emit(project_dir: Path):
    sl = Slither(str(project_dir / "src" / "C.sol"))
    contract = next(c for c in sl.contracts if c.name == "C")
    artifact = build_predicate_artifacts(contract)
    return synthesize_semantic_guards_from_predicate_trees(
        artifact,
        contract_address=_TARGET_ADDRESS,
        contract_name="C",
    )


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def _function_controllers(payload, fn_signature: str) -> list[dict]:
    """Pull the resolved controllers list for the function — the
    field downstream consumers (UI, policy decisions) read."""
    for fn in payload.get("functions", []) or []:
        if fn.get("function") == fn_signature:
            return fn.get("controllers") or []
    return []


def _privileged_signatures(payload) -> set[str]:
    """Functions that have at least one resolved controller — i.e.
    the set effective_permissions treats as guarded."""
    out = set()
    for fn in payload.get("functions", []) or []:
        if fn.get("controllers"):
            out.add(fn.get("function"))
    return out


def _normalize_controller(c: dict) -> dict:
    """Strip metadata that legitimately differs between v1-native
    and v2-shim derivations. We compare semantic content: the set
    of resolved principals + the controller's source label."""
    return {
        "source": c.get("source"),
        "label": c.get("label"),
        "kind": c.get("kind"),
        "principal_addresses": sorted(
            (p.get("address") or "").lower()
            for p in (c.get("principals") or [])
            if isinstance(p, dict)
        ),
    }


def _compare(v1_payload, v2_payload, *, fixture: str) -> None:
    v1_priv = _privileged_signatures(v1_payload)
    v2_priv = _privileged_signatures(v2_payload)

    # v2 catching what v1 missed is acceptable (new_coverage); v1
    # flagging more than v2 is a regression.
    v1_only = v1_priv - v2_priv
    if v1_only:
        pytest.fail(
            f"[{fixture}] v1 flagged {sorted(v1_only)} as having "
            f"controllers but v2 produced empty controllers — REGRESSION"
        )

    for fn in sorted(v1_priv & v2_priv):
        v1_ctrls = sorted(
            (json.dumps(_normalize_controller(c), sort_keys=True))
            for c in _function_controllers(v1_payload, fn)
        )
        v2_ctrls = sorted(
            (json.dumps(_normalize_controller(c), sort_keys=True))
            for c in _function_controllers(v2_payload, fn)
        )
        # v2 may have a STRICT-SUPERSET of v1 controllers (more typed
        # grants from the predicate tree's structural data) — that's
        # acceptable. v1 having a controller v2 lost is the
        # regression direction.
        v1_set = set(v1_ctrls)
        v2_set = set(v2_ctrls)
        missing = v1_set - v2_set
        if missing:
            pytest.fail(
                f"[{fixture}.{fn}] v2 lost controllers v1 had: "
                f"{sorted(missing)}\n"
                f"  v1 controllers: {v1_ctrls}\n"
                f"  v2 controllers: {v2_ctrls}"
            )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


# Maker DSS wards — the fixture that surfaced the storage_var
# fallback bug. v1 emits controller_source='wards'; the shim before
# the fix emitted null. Pinning this case ensures the regression
# stays fixed.
_MAKER_WARDS = """
pragma solidity ^0.8.19;
contract C {
    mapping(address => uint256) public wards;
    uint256 public x;
    function rely(address addr) external {
        require(wards[msg.sender] == 1);
        wards[addr] = 1;
    }
    function mint(uint256 v) external {
        require(wards[msg.sender] == 1);
        x = v;
    }
}
"""

# OZ Ownable — caller_equals_controller path through both pipelines.
_OZ_OWNABLE = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    uint256 public x;
    modifier onlyOwner() { require(msg.sender == ownerVar); _; }
    function setX(uint256 v) external onlyOwner { x = v; }
}
"""

# OZ AccessControl inline — multi-key membership; controller_source
# resolves to the constant role bytes.
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
}
"""


_FIXTURES = [
    ("maker_wards", _MAKER_WARDS),
    ("oz_ownable", _OZ_OWNABLE),
    ("oz_ac_inline", _OZ_AC_INLINE),
]


@pytest.mark.parametrize("fixture_name,source", _FIXTURES, ids=[f[0] for f in _FIXTURES])
def test_effective_permissions_equivalent(tmp_path: Path, fixture_name: str, source: str):
    project_dir = _write_project(
        tmp_path,
        source_code=textwrap.dedent(source).strip() + "\n",
    )
    target_analysis, v1_sg = _v1_emit(project_dir)
    v2_sg = _v2_emit(project_dir)

    # Same target_snapshot for both — sets up controller_lookup with
    # owner / wards / role values so resolved-principals are
    # populated. Without this the test only verifies the privileged-
    # set intersection, not the controller dicts.
    # _controller_lookup keys by value['source'] (or by the dict key
    # if no source field). Adding source matching the predicate's
    # controller_source makes the lookup hit and produce resolved
    # principals — without it both v1 and v2 produce empty controllers
    # and the comparator can't distinguish a real bug from "neither
    # side has signal".
    target_snapshot = {
        "contract_name": "C",
        "controller_values": {
            "state_variable:ownerVar": {
                "source": "ownerVar",
                "kind": "state_variable",
                "label": "ownerVar",
                "value": _OWNER_ADDRESS,
                "resolved_type": "eoa",
                "details": {"address": _OWNER_ADDRESS},
            },
            "state_variable:wards": {
                "source": "wards",
                "kind": "state_variable",
                "label": "wards",
                "value": _OWNER_ADDRESS,
                "resolved_type": "eoa",
                "details": {"address": _OWNER_ADDRESS},
            },
            "constant:0x" + "01" * 32: {
                "source": "0x" + "01" * 32,
                "kind": "constant",
                "label": "0x" + "01" * 32,
                "value": _OWNER_ADDRESS,
                "resolved_type": "eoa",
                "details": {"address": _OWNER_ADDRESS},
            },
        },
    }

    v1_payload = build_effective_permissions(
        target_analysis,
        target_snapshot=target_snapshot,
        semantic_guards=v1_sg,
    )
    v2_payload = build_effective_permissions(
        target_analysis,
        target_snapshot=target_snapshot,
        semantic_guards=v2_sg,
    )

    _compare(v1_payload, v2_payload, fixture=fixture_name)
