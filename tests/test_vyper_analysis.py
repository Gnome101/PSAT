import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static import collect_contract_analysis


def test_collect_contract_analysis_uses_vyper_fallback(tmp_path):
    project_dir = tmp_path / "GateSeal"
    (project_dir / "src").mkdir(parents=True)
    (project_dir / "src" / "GateSeal.vy").write_text(
        """# @version 0.3.7

SEALING_COMMITTEE: immutable(address)

@external
def __init__(_committee: address):
    SEALING_COMMITTEE = _committee

@external
@view
def get_sealing_committee() -> address:
    return SEALING_COMMITTEE

@external
def seal():
    assert msg.sender == SEALING_COMMITTEE, "not committee"
"""
    )
    (project_dir / "contract_meta.json").write_text(
        json.dumps(
            {
                "address": "0x1111111111111111111111111111111111111111",
                "contract_name": "GateSeal",
                "compiler_version": "vyper:0.3.7",
                "language": "vyper",
            }
        )
        + "\n"
    )

    analysis = collect_contract_analysis(project_dir)

    assert analysis["subject"]["name"] == "GateSeal"
    assert analysis["subject"]["source_verified"] is True
    assert analysis["analysis_status"]["slither_completed"] is False
    assert analysis["summary"]["control_model"] == "governance"

    privileged = next(item for item in analysis["access_control"]["privileged_functions"] if item["function"] == "seal()")
    assert privileged["controller_refs"] == ["SEALING_COMMITTEE"]
    assert privileged["guard_kinds"] == ["caller_equals_storage"]

    tracked = {item["label"]: item for item in analysis["controller_tracking"]}
    assert "SEALING_COMMITTEE" in tracked
    assert tracked["SEALING_COMMITTEE"]["read_spec"] == {
        "strategy": "getter_call",
        "target": "get_sealing_committee",
    }
