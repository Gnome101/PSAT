import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from schemas.control_tracking import ControlTrackingPlan
from services.resolution.tracking import build_control_snapshot, clear_classify_cache
from services.resolution.tracking import (
    classify_resolved_address as _classify_resolved_address,
)


@pytest.fixture(autouse=True)
def _isolated_classify_cache():
    # Process-wide classify cache leaks across test files when xdist/pytest-cov
    # serializes them in worker order — clear before each test so mocked RPC
    # responses aren't shadowed by a stale entry from a sibling file.
    clear_classify_cache()
    yield
    clear_classify_cache()


def test_build_control_snapshot(monkeypatch):
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
                "read_spec": None,
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
    monkeypatch.setattr(
        "services.resolution.controller_adapters._eth_call_raw",
        lambda rpc_url, contract_address, calldata, block_tag="latest": (
            "0x" + "00" * 12 + "22" * 20
            if contract_address.lower() == "0x1111111111111111111111111111111111111111"
            else "0x" + "00" * 12 + "33" * 20
            if contract_address.lower() == "0x2222222222222222222222222222222222222222"
            else "0x" + "44" * 32
        ),
    )
    monkeypatch.setattr("services.resolution.controller_adapters._rpc_request", fake_rpc)
    monkeypatch.setattr("services.resolution.controller_adapters._rpc_request", fake_rpc)

    first = build_control_snapshot(plan, "https://rpc.example")
    state["owner"] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    second = build_control_snapshot(plan, "https://rpc.example")

    assert first["controller_values"]["state_variable:owner"]["value"] == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert first["controller_values"]["state_variable:owner"]["resolved_type"] == "eoa"
    assert second["controller_values"]["state_variable:owner"]["value"] == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


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
                "read_spec": None,
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


def test_build_control_snapshot_expands_role_identifier_principals(monkeypatch):
    plan: ControlTrackingPlan = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Mock",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [
            {
                "controller_id": "role_identifier:PAUSE_ROLE",
                "label": "PAUSE_ROLE",
                "source": "PAUSE_ROLE",
                "kind": "role_identifier",
                "read_spec": None,
                "tracking_mode": "manual_review",
                "event_watch": None,
                "polling_fallback": {
                    "contract_address": "0x1111111111111111111111111111111111111111",
                    "polling_sources": ["PAUSE_ROLE"],
                    "cadence": "periodic_reconciliation",
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
            return "0x" + "11" * 32
        raise AssertionError(f"Unexpected RPC call: {method}")

    monkeypatch.setattr("services.resolution.tracking._rpc_request", fake_rpc)
    monkeypatch.setattr(
        "services.resolution.tracking.expand_role_identifier_principals",
        lambda rpc_url, contract_address, role_id, block_tag="latest": (
            ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
            {"adapter": "access_control_enumerable", "member_count": 1},
        ),
    )
    monkeypatch.setattr(
        "services.resolution.tracking.classify_resolved_address",
        lambda rpc_url, address, block_tag="latest": ("eoa", {"address": address}),
    )

    snapshot = build_control_snapshot(plan, "https://rpc.example")
    value = snapshot["controller_values"]["role_identifier:PAUSE_ROLE"]

    assert value["value"] == "0x" + "11" * 32
    assert value["observed_via"] == "eth_call+access_control_enumerable"
    assert value["details"]["adapter"] == "access_control_enumerable"
    assert value["details"]["resolved_principals"] == [
        {
            "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "resolved_type": "eoa",
            "details": {"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
        }
    ]


def test_build_control_snapshot_reads_role_identifier_from_external_controller(monkeypatch):
    target = "0x1111111111111111111111111111111111111111"
    authority = "0x2222222222222222222222222222222222222222"
    plan: ControlTrackingPlan = {
        "schema_version": "0.1",
        "contract_address": target,
        "contract_name": "Mock",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [
            {
                "controller_id": "state_variable:roleRegistry",
                "label": "roleRegistry",
                "source": "roleRegistry",
                "kind": "state_variable",
                "read_spec": None,
                "tracking_mode": "state_only",
                "event_watch": None,
                "polling_fallback": {
                    "contract_address": target,
                    "polling_sources": ["roleRegistry"],
                    "cadence": "state_only",
                    "notes": [],
                },
                "notes": [],
            },
            {
                "controller_id": "role_identifier:BREAK_GLASS",
                "label": "BREAK_GLASS",
                "source": "BREAK_GLASS",
                "kind": "role_identifier",
                "read_spec": {
                    "strategy": "getter_call",
                    "target": "BREAK_GLASS",
                    "contract_source": "roleRegistry",
                },
                "tracking_mode": "manual_review",
                "event_watch": None,
                "polling_fallback": {
                    "contract_address": target,
                    "polling_sources": ["BREAK_GLASS"],
                    "cadence": "periodic_reconciliation",
                    "notes": [],
                },
                "notes": [],
            },
        ],
        "tracked_policies": [],
    }

    def fake_rpc(_rpc_url, method, params):
        if method == "eth_blockNumber":
            return "0x10"
        if method == "eth_call":
            to = params[0]["to"].lower()
            if to == target:
                return "0x" + "00" * 12 + authority[2:]
            if to == authority:
                return "0x" + "11" * 32
            raise AssertionError(f"Unexpected eth_call target: {to}")
        raise AssertionError(f"Unexpected RPC call: {method}")

    captured: dict[str, str] = {}

    monkeypatch.setattr("services.resolution.tracking._rpc_request", fake_rpc)
    monkeypatch.setattr(
        "services.resolution.tracking.classify_resolved_address",
        lambda rpc_url, address, block_tag="latest": ("contract", {"address": address}),
    )

    def fake_expand(_rpc_url, contract_address, role_id, block_tag="latest"):
        captured["contract_address"] = contract_address
        captured["role_id"] = role_id
        return (
            ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
            {"adapter": "access_control_enumerable", "member_count": 1},
        )

    monkeypatch.setattr("services.resolution.tracking.expand_role_identifier_principals", fake_expand)

    snapshot = build_control_snapshot(plan, "https://rpc.example")
    value = snapshot["controller_values"]["role_identifier:BREAK_GLASS"]

    assert value["value"] == "0x" + "11" * 32
    assert captured["contract_address"] == authority
    assert captured["role_id"] == "0x" + "11" * 32


def test_build_control_snapshot_expands_aragon_acl_role_principals(monkeypatch):
    from services.resolution.controller_adapters import SET_PERMISSION_TOPIC0

    plan: ControlTrackingPlan = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Voting",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [
            {
                "controller_id": "role_identifier:CREATE_VOTES_ROLE",
                "label": "CREATE_VOTES_ROLE",
                "source": "CREATE_VOTES_ROLE",
                "kind": "role_identifier",
                "read_spec": None,
                "tracking_mode": "manual_review",
                "event_watch": None,
                "polling_fallback": {
                    "contract_address": "0x1111111111111111111111111111111111111111",
                    "polling_sources": ["CREATE_VOTES_ROLE"],
                    "cadence": "periodic_reconciliation",
                    "notes": [],
                },
                "notes": [],
            }
        ],
        "tracked_policies": [],
    }

    def fake_rpc(_rpc_url, method, params):
        if method == "eth_blockNumber":
            return "0x10"
        if method == "eth_call":
            call = params[0]
            target = call["to"].lower()
            if target == "0x1111111111111111111111111111111111111111" and call["data"] == "0x01977c99":
                return "0x" + "00" * 12 + "22" * 20
            if target == "0x2222222222222222222222222222222222222222":
                return "0x" + "00" * 12 + "33" * 20
            return "0x" + "44" * 32
        if method == "eth_getLogs":
            topic0 = params[0]["topics"][0]
            if topic0 != SET_PERMISSION_TOPIC0:
                return []
            return [
                {
                    "blockNumber": "0x1",
                    "transactionIndex": "0x0",
                    "logIndex": "0x0",
                    "topics": [
                        SET_PERMISSION_TOPIC0,
                        "0x" + "0" * 24 + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "0x" + "0" * 24 + "1111111111111111111111111111111111111111",
                        "0x" + "44" * 32,
                    ],
                    "data": "0x" + "0" * 63 + "1",
                }
            ]
        raise AssertionError(f"Unexpected RPC call: {method} {params}")

    monkeypatch.setattr("services.resolution.tracking._rpc_request", fake_rpc)
    monkeypatch.setattr("services.resolution.controller_adapters._rpc_request", fake_rpc)
    monkeypatch.setattr(
        "services.resolution.controller_adapters._code_start_block",
        lambda rpc_url, address, block_tag="latest": 0,
    )
    monkeypatch.setattr(
        "services.resolution.controller_adapters._eth_call_raw",
        lambda rpc_url, contract_address, calldata, block_tag="latest": (
            "0x" + "00" * 12 + "22" * 20
            if contract_address.lower() == "0x1111111111111111111111111111111111111111"
            else "0x" + "00" * 12 + "33" * 20
            if contract_address.lower() == "0x2222222222222222222222222222222222222222"
            else "0x" + "44" * 32
        ),
    )
    monkeypatch.setattr(
        "services.resolution.tracking.classify_resolved_address",
        lambda rpc_url, address, block_tag="latest": ("eoa", {"address": address}),
    )

    snapshot = build_control_snapshot(plan, "https://rpc.example")
    value = snapshot["controller_values"]["role_identifier:CREATE_VOTES_ROLE"]

    assert value["observed_via"] == "eth_call+aragon_acl"
    assert value["details"]["adapter"] == "aragon_acl"
    assert value["details"]["kernel"] == "0x2222222222222222222222222222222222222222"
    assert value["details"]["acl"] == "0x3333333333333333333333333333333333333333"
    assert value["details"]["resolved_principals"] == [
        {
            "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "resolved_type": "eoa",
            "details": {"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
        }
    ]


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
