import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.contract_analysis import collect_contract_analysis
from services.control_tracking_plan import (
    build_control_tracking_plan,
    build_control_tracking_plan_from_file,
    write_control_tracking_plan,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "contracts"


def _write_project(tmp_path: Path, contract_name: str, source_code: str, slither_output: dict | None = None) -> Path:
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
    (project_dir / "slither_results.json").write_text(json.dumps(slither_output or {"results": {"detectors": []}}) + "\n")
    return project_dir


def _fixture_source(relative_path: str) -> str:
    return (FIXTURES_DIR / relative_path).read_text()


def _tracked_controller(plan: dict, label: str) -> dict:
    for controller in plan["tracked_controllers"]:
        if controller["label"] == label:
            return controller
    raise AssertionError(f"Tracked controller {label} not found")


def test_build_control_tracking_plan_uses_event_watch_when_available(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "AuthModifierController",
        _fixture_source("composed/auth_modifier_controller.sol"),
    )
    analysis = collect_contract_analysis(project_dir)

    plan = build_control_tracking_plan(analysis)

    owner = _tracked_controller(plan, "owner")
    assert owner["tracking_mode"] == "event_plus_state"
    assert owner["event_watch"] == {
        "transport": "wss_logs",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "events": [
            {
                "name": "OwnershipTransferred",
                "signature": "OwnershipTransferred(address,address)",
                "topic0": "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0",
                "inputs": [
                    {"name": "user", "type": "address", "indexed": True},
                    {"name": "newOwner", "type": "address", "indexed": True},
                ],
            }
        ],
        "writer_functions": ["transferOwnership(address)"],
    }
    assert owner["polling_fallback"]["cadence"] == "realtime_confirm"

    authority = _tracked_controller(plan, "authority")
    assert authority["event_watch"] == {
        "transport": "wss_logs",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "events": [
            {
                "name": "AuthorityUpdated",
                "signature": "AuthorityUpdated(address,address)",
                "topic0": "0xa3396fd7f6e0a21b50e5089d2da70d5ac0a3bbbd1f617a93f134b76389980198",
                "inputs": [
                    {"name": "user", "type": "address", "indexed": True},
                    {"name": "newAuthority", "type": "address", "indexed": True},
                ],
            }
        ],
        "writer_functions": ["setAuthority(AuthorityLike)"],
    }
    assert plan["tracked_policies"] == []


def test_build_control_tracking_plan_falls_back_to_state_only(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "OwnerNoEvent",
        _fixture_source("tracking/owner_update_no_event.sol"),
    )
    analysis = collect_contract_analysis(project_dir)

    plan = build_control_tracking_plan(analysis)

    owner = _tracked_controller(plan, "owner")
    assert owner["tracking_mode"] == "state_only"
    assert owner["event_watch"] is None
    assert owner["polling_fallback"]["cadence"] == "state_only"
    assert owner["polling_fallback"]["polling_sources"] == ["owner"]
    assert plan["tracked_policies"] == []


def test_build_control_tracking_plan_includes_can_call_policy_events(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "RolesAuthorityPolicy",
        _fixture_source("tracking/roles_authority_policy.sol"),
    )
    analysis = collect_contract_analysis(project_dir)

    plan = build_control_tracking_plan(analysis)

    policy = next(item for item in plan["tracked_policies"] if item["label"] == "canCall policy")
    assert policy["policy_function"] == "canCall(address,address,bytes4)"
    assert policy["tracked_state_targets"] == [
        "getRolesWithCapability",
        "getUserRoles",
        "isCapabilityPublic",
    ]
    assert {event["signature"] for event in policy["event_watch"]["events"]} == {
        "PublicCapabilityUpdated(address,bytes4,bool)",
        "RoleCapabilityUpdated(uint8,address,bytes4,bool)",
        "UserRoleUpdated(address,uint8,bool)",
    }


def test_write_control_tracking_plan_writes_json_file(tmp_path):
    contract_analysis_path = tmp_path / "contract_analysis.json"
    contract_analysis_path.write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "subject": {
                    "address": "0x1111111111111111111111111111111111111111",
                    "name": "Example",
                    "compiler_version": "v0.8.19",
                    "source_verified": True,
                },
                "controller_tracking": [
                    {
                        "controller_id": "state_variable:owner",
                        "label": "owner",
                        "source": "owner",
                        "kind": "state_variable",
                        "tracking_mode": "event_plus_state",
                        "writer_functions": [
                            {
                                "contract": "OwnableLike",
                                "function": "transferOwnership(address)",
                                "visibility": "public",
                                "writes": ["owner"],
                                "associated_events": [
                                    {
                                        "name": "OwnershipTransferred",
                                        "signature": "OwnershipTransferred(address,address)",
                                        "topic0": "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0",
                                        "inputs": [
                                            {"name": "user", "type": "address", "indexed": True},
                                            {"name": "newOwner", "type": "address", "indexed": True},
                                        ],
                                    }
                                ],
                                "evidence": [],
                            }
                        ],
                        "associated_events": [
                            {
                                "name": "OwnershipTransferred",
                                "signature": "OwnershipTransferred(address,address)",
                                "topic0": "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0",
                                "inputs": [
                                    {"name": "user", "type": "address", "indexed": True},
                                    {"name": "newOwner", "type": "address", "indexed": True},
                                ],
                            }
                        ],
                        "polling_sources": ["owner"],
                        "notes": ["Monitor associated events for low-latency detection and confirm state via RPC."],
                    }
                ],
                "policy_tracking": [],
            }
        )
        + "\n"
    )

    written = write_control_tracking_plan(contract_analysis_path)

    assert written.name == "control_tracking_plan.json"
    payload = json.loads(written.read_text())
    assert payload["tracking_strategy"] == "event_first_with_polling_fallback"
    assert payload["tracked_controllers"][0]["event_watch"]["events"][0]["name"] == "OwnershipTransferred"
    assert payload["tracked_policies"] == []
    assert build_control_tracking_plan_from_file(contract_analysis_path) == payload
