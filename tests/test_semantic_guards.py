"""``build_semantic_guards`` smoke tests under the schema-v2 cutover.

Post-cutover ``build_semantic_guards`` is a thin wrapper that pulls
the embedded ``_v2_predicate_trees`` out of a contract_analysis dict
and runs the v2 shim. Comprehensive shim-shape coverage lives in
``test_v2_to_v1_shim.py``; this file only spot-checks that the
end-to-end ``collect_contract_analysis -> build_semantic_guards``
path round-trips for a canonical Ownable-style contract.

Earlier v1-specific tests pinning ``role_member`` / ``external_helper``
shapes (which v1 derived from name-heuristic pattern matching on
``roleRegistry.hasRole`` / ``onlyProtocolUpgrader`` / ``policy.q``
function names) were removed — those heuristics are gone and v2
classifies via structural shape (``external_authority_check``
emitted as ``external_helper`` only when it matches structurally,
not by function-name match).
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static import collect_contract_analysis
from services.static.contract_analysis_pipeline import build_semantic_guards

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "contracts"


def _write_project(tmp_path: Path, contract_name: str, source_code: str) -> Path:
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
    (project_dir / "slither_results.json").write_text(json.dumps({"results": {"detectors": []}}) + "\n")
    return project_dir


def _fixture_source(relative_path: str) -> str:
    return (FIXTURES_DIR / relative_path).read_text()


def _semantic_function(payload: dict, signature: str) -> dict:
    for function in payload["functions"]:
        if function["function"] == signature:
            return function
    raise AssertionError(f"Semantic guard entry {signature} not found")


def test_semantic_guards_direct_owner_pause(tmp_path):
    project_dir = _write_project(tmp_path, "DirectOwnerPause", _fixture_source("pause/direct_owner_pause.sol"))
    analysis = collect_contract_analysis(project_dir)

    payload = build_semantic_guards(analysis)
    pause = _semantic_function(payload, "pause()")

    assert pause["status"] == "resolved"
    assert pause["predicates"] == [
        {
            "kind": "caller_equals_controller",
            "controller_kind": "state_variable",
            "controller_label": "owner",
            "controller_source": "owner",
            "read_spec": None,
        }
    ]
    # Synthetic marker pinned: every post-cutover semantic_guards
    # output is shim-derived. Earlier v1 native emit didn't carry
    # this key.
    assert payload.get("_synthetic_from") == "v2_predicate_trees"
