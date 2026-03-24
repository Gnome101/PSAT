import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from schemas.control_tracking import ControlTrackingPlan
from services.resolution.tracking import (
    _classify_resolved_address,
    build_control_snapshot,
    diff_control_snapshots,
    grouped_event_filters,
    matching_controllers_for_log,
    matching_policies_for_log,
    policy_change_events,
    run_control_tracker,
)
from services.resolution.tracking_plan import build_control_tracking_plan
from services.static import collect_contract_analysis

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
    (project_dir / "slither_results.json").write_text(
        json.dumps(slither_output or {"results": {"detectors": []}}) + "\n"
    )
    return project_dir


def _fixture_source(relative_path: str) -> str:
    return (FIXTURES_DIR / relative_path).read_text()


def test_grouped_event_filters_from_plan(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "AuthModifierController",
        _fixture_source("composed/auth_modifier_controller.sol"),
    )
    analysis = collect_contract_analysis(project_dir)
    plan = build_control_tracking_plan(analysis)

    filters = grouped_event_filters(plan)

    assert filters == [
        {
            "address": "0x1111111111111111111111111111111111111111",
            "topics": [
                [
                    "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0",
                    "0xa3396fd7f6e0a21b50e5089d2da70d5ac0a3bbbd1f617a93f134b76389980198",
                ]
            ],
            "controller_ids": ["external_contract:authority", "state_variable:owner"],
        }
    ]


def test_matching_controllers_for_log(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "AuthModifierController",
        _fixture_source("composed/auth_modifier_controller.sol"),
    )
    analysis = collect_contract_analysis(project_dir)
    plan = build_control_tracking_plan(analysis)

    log_entry = {
        "address": "0x1111111111111111111111111111111111111111",
        "topics": ["0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0"],
    }

    matches = matching_controllers_for_log(plan, log_entry)

    assert [item["label"] for item in matches] == ["owner"]


def test_matching_policies_for_log_and_decode_fields(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "RolesAuthorityPolicy",
        _fixture_source("tracking/roles_authority_policy.sol"),
    )
    analysis = collect_contract_analysis(project_dir)
    plan = build_control_tracking_plan(analysis)
    policy = next(item for item in plan["tracked_policies"] if item["label"] == "canCall policy")
    role_event = next(
        event
        for event in policy["event_watch"]["events"]
        if event["signature"] == "RoleCapabilityUpdated(uint8,address,bytes4,bool)"
    )

    log_entry = {
        "address": "0x1111111111111111111111111111111111111111",
        "topics": [
            role_event["topic0"],
            "0x" + "00" * 31 + "01",
            "0x" + "00" * 12 + "2222222222222222222222222222222222222222",
            "0x12345678" + "00" * 28,
        ],
        "data": "0x" + "0" * 63 + "1",
        "blockNumber": "0x2a",
        "transactionHash": "0x" + "ab" * 32,
    }

    matches = matching_policies_for_log(plan, log_entry)
    assert [item["label"] for item in matches] == ["canCall policy"]

    changes = policy_change_events(plan, matches, log_entry)
    assert changes == [
        {
            "schema_version": "0.1",
            "contract_address": "0x1111111111111111111111111111111111111111",
            "contract_name": "RolesAuthorityPolicy",
            "change_kind": "policy_event_observed",
            "controller_id": "canCall_policy",
            "block_number": 42,
            "tx_hash": "0x" + "ab" * 32,
            "old_value": None,
            "new_value": None,
            "observed_via": "wss_logs",
            "notes": [
                (
                    "Track authorization-policy mutations through emitted events; "
                    "the underlying table-backed state is non-enumerable for "
                    "generic polling."
                ),
                (
                    "decoded_fields=enabled=True, functionSig=0x12345678, role=1, "
                    "target=0x2222222222222222222222222222222222222222"
                ),
            ],
            "event_signature": "RoleCapabilityUpdated(uint8,address,bytes4,bool)",
        }
    ]


def test_build_control_snapshot_and_diff(monkeypatch):
    plan: ControlTrackingPlan = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Mock",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [
            {
                "controller_id": "state_variable:owner",
                "label": "owner",
                "source": "owner",
                "kind": "state_variable",
                "tracking_mode": "event_plus_state",
                "event_watch": {
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
                },
                "polling_fallback": {
                    "contract_address": "0x1111111111111111111111111111111111111111",
                    "polling_sources": ["owner"],
                    "cadence": "realtime_confirm",
                    "notes": [],
                },
                "notes": [],
            }
        ],
        "tracked_policies": [],
    }

    state = {"owner": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}

    def fake_rpc(_rpc_url, method, params):
        if method == "eth_blockNumber":
            return "0x10"
        if method == "eth_call":
            return "0x" + "00" * 12 + state["owner"]
        if method == "eth_getCode":
            return "0x"
        raise AssertionError(f"Unexpected RPC call: {method} {params}")

    monkeypatch.setattr("services.resolution.tracking._rpc_request", fake_rpc)

    first = build_control_snapshot(plan, "https://rpc.example")
    state["owner"] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    second = build_control_snapshot(plan, "https://rpc.example")
    changes = diff_control_snapshots(first, second)

    assert first["controller_values"]["state_variable:owner"]["value"] == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert first["controller_values"]["state_variable:owner"]["resolved_type"] == "eoa"
    assert second["controller_values"]["state_variable:owner"]["value"] == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    assert changes == [
        {
            "schema_version": "0.1",
            "contract_address": "0x1111111111111111111111111111111111111111",
            "contract_name": "Mock",
            "change_kind": "controller_value_changed",
            "controller_id": "state_variable:owner",
            "block_number": 16,
            "tx_hash": None,
            "old_value": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "new_value": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "observed_via": "eth_call",
            "notes": [],
            "event_signature": None,
        }
    ]


def test_build_control_snapshot_handles_reverting_getter(monkeypatch):
    plan: ControlTrackingPlan = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Mock",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [
            {
                "controller_id": "state_variable:owner",
                "label": "owner",
                "source": "owner",
                "kind": "state_variable",
                "tracking_mode": "event_plus_state",
                "event_watch": None,
                "polling_fallback": {
                    "contract_address": "0x1111111111111111111111111111111111111111",
                    "polling_sources": ["owner"],
                    "cadence": "state_only",
                    "notes": [],
                },
                "notes": [],
            }
        ],
        "tracked_policies": [],
    }

    def fake_rpc(_rpc_url, method, _params):
        if method == "eth_blockNumber":
            return "0x10"
        if method == "eth_call":
            raise RuntimeError("{'code': 3, 'message': 'execution reverted', 'data': '0x'}")
        raise AssertionError(f"Unexpected RPC call: {method}")

    monkeypatch.setattr("services.resolution.tracking._rpc_request", fake_rpc)

    snapshot = build_control_snapshot(plan, "https://rpc.example")
    value = snapshot["controller_values"]["state_variable:owner"]

    assert value["value"] is None
    assert value["observed_via"] == "eth_call_error"
    assert value["resolved_type"] == "unknown"
    assert "execution reverted" in str(value["details"]["error"])


def test_classify_resolved_address_detects_safe(monkeypatch):
    def fake_rpc(_rpc_url, method, params):
        if method == "eth_getCode":
            return "0x6000"
        if method == "eth_call":
            data = params[0]["data"]
            if data == "0xa0e67e2b":
                encoded = (
                    "0000000000000000000000000000000000000000000000000000000000000020"
                    "0000000000000000000000000000000000000000000000000000000000000002"
                    "0000000000000000000000001111111111111111111111111111111111111111"
                    "0000000000000000000000002222222222222222222222222222222222222222"
                )
                return "0x" + encoded
            if data == "0xe75235b8":
                return "0x" + "0" * 63 + "2"
            return "0x"
        raise AssertionError(f"Unexpected RPC call: {method} {params}")

    monkeypatch.setattr("services.resolution.tracking._rpc_request", fake_rpc)

    resolved_type, details = _classify_resolved_address(
        "https://rpc.example",
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )

    assert resolved_type == "safe"
    assert details == {
        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "owners": [
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
        ],
        "threshold": 2,
    }


def test_classify_resolved_address_detects_timelock(monkeypatch):
    def fake_rpc(_rpc_url, method, params):
        if method == "eth_getCode":
            return "0x6000"
        if method == "eth_call":
            data = params[0]["data"]
            if data == "0xf27a0c92":
                return "0x" + "0" * 60 + "2a30"
            if data == "0x8da5cb5b":
                return "0x" + "00" * 12 + "3333333333333333333333333333333333333333"
            return "0x"
        raise AssertionError(f"Unexpected RPC call: {method} {params}")

    monkeypatch.setattr("services.resolution.tracking._rpc_request", fake_rpc)

    resolved_type, details = _classify_resolved_address(
        "https://rpc.example",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )

    assert resolved_type == "timelock"
    assert details == {
        "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "delay": 10800,
        "owner": "0x3333333333333333333333333333333333333333",
    }


def test_classify_resolved_address_detects_proxy_admin(monkeypatch):
    def fake_rpc(_rpc_url, method, params):
        if method == "eth_getCode":
            return "0x6000"
        if method == "eth_call":
            data = params[0]["data"]
            if data == "0xad3cb1cc":
                encoded = (
                    "0000000000000000000000000000000000000000000000000000000000000020"
                    "0000000000000000000000000000000000000000000000000000000000000001"
                    "3500000000000000000000000000000000000000000000000000000000000000"
                )
                return "0x" + encoded
            if data == "0x8da5cb5b":
                return "0x" + "00" * 12 + "4444444444444444444444444444444444444444"
            return "0x"
        raise AssertionError(f"Unexpected RPC call: {method} {params}")

    monkeypatch.setattr("services.resolution.tracking._rpc_request", fake_rpc)

    resolved_type, details = _classify_resolved_address(
        "https://rpc.example",
        "0xcccccccccccccccccccccccccccccccccccccccc",
    )

    assert resolved_type == "proxy_admin"
    assert details == {
        "address": "0xcccccccccccccccccccccccccccccccccccccccc",
        "upgrade_interface_version": "5",
        "owner": "0x4444444444444444444444444444444444444444",
    }


def test_run_control_tracker_once_writes_snapshot(monkeypatch, tmp_path):
    plan_path = tmp_path / "control_tracking_plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "contract_address": "0x1111111111111111111111111111111111111111",
                "contract_name": "Mock",
                "tracking_strategy": "event_first_with_polling_fallback",
                "tracked_controllers": [
                    {
                        "controller_id": "state_variable:owner",
                        "label": "owner",
                        "source": "owner",
                        "kind": "state_variable",
                        "tracking_mode": "state_only",
                        "event_watch": None,
                        "polling_fallback": {
                            "contract_address": "0x1111111111111111111111111111111111111111",
                            "polling_sources": ["owner"],
                            "cadence": "state_only",
                            "notes": [],
                        },
                        "notes": [],
                    }
                ],
                "tracked_policies": [],
            }
        )
        + "\n"
    )

    def fake_rpc(_rpc_url, method, params):
        if method == "eth_blockNumber":
            return "0x20"
        if method == "eth_call":
            return "0x" + "00" * 12 + "cccccccccccccccccccccccccccccccccccccccc"
        if method == "eth_getCode":
            return "0x"
        raise AssertionError(f"Unexpected RPC call: {method} {params}")

    monkeypatch.setattr("services.resolution.tracking._rpc_request", fake_rpc)

    snapshot_path = tmp_path / "snapshot.json"
    changes_path = tmp_path / "changes.jsonl"
    asyncio.run(
        run_control_tracker(
            plan_path,
            rpc_url="https://rpc.example",
            snapshot_path=snapshot_path,
            change_events_path=changes_path,
            once=True,
        )
    )

    assert snapshot_path.exists()
    payload = json.loads(snapshot_path.read_text())
    assert payload["controller_values"]["state_variable:owner"]["value"] == "0xcccccccccccccccccccccccccccccccccccccccc"
    assert not changes_path.exists()
