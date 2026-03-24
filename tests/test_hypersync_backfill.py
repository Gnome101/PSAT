import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.policy.hypersync_backfill import (
    fetch_policy_event_history,
    reconstruct_policy_state,
    run_hypersync_policy_backfill,
)


def _plan() -> dict:
    return {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "RolesAuthority",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [],
        "tracked_policies": [
            {
                "policy_id": "canCall_policy",
                "label": "canCall policy",
                "policy_function": "canCall(address,address,bytes4)",
                "tracked_state_targets": [
                    "getRolesWithCapability",
                    "getUserRoles",
                    "isCapabilityPublic",
                ],
                "event_watch": {
                    "transport": "wss_logs",
                    "contract_address": "0x1111111111111111111111111111111111111111",
                    "events": [
                        {
                            "name": "PublicCapabilityUpdated",
                            "signature": "PublicCapabilityUpdated(address,bytes4,bool)",
                            "topic0": "0x950a343f5d10445e82a71036d3f4fb3016180a25805141932543b83e2078a93e",
                            "inputs": [
                                {"name": "target", "type": "address", "indexed": True},
                                {"name": "functionSig", "type": "bytes4", "indexed": True},
                                {"name": "enabled", "type": "bool", "indexed": False},
                            ],
                        },
                        {
                            "name": "RoleCapabilityUpdated",
                            "signature": "RoleCapabilityUpdated(uint8,address,bytes4,bool)",
                            "topic0": "0xa52ea92e6e955aa8ac66420b86350f7139959adfcc7e6a14eee1bd116d09860e",
                            "inputs": [
                                {"name": "role", "type": "uint8", "indexed": True},
                                {"name": "target", "type": "address", "indexed": True},
                                {"name": "functionSig", "type": "bytes4", "indexed": True},
                                {"name": "enabled", "type": "bool", "indexed": False},
                            ],
                        },
                        {
                            "name": "UserRoleUpdated",
                            "signature": "UserRoleUpdated(address,uint8,bool)",
                            "topic0": "0x4c9bdd0c8e073eb5eda2250b18d8e5121ff27b62064fbeeeed4869bb99bc5bf2",
                            "inputs": [
                                {"name": "user", "type": "address", "indexed": True},
                                {"name": "role", "type": "uint8", "indexed": True},
                                {"name": "enabled", "type": "bool", "indexed": False},
                            ],
                        },
                    ],
                    "writer_functions": [
                        "setPublicCapability(address,bytes4,bool)",
                        "setRoleCapability(uint8,address,bytes4,bool)",
                        "setUserRole(address,uint8,bool)",
                    ],
                },
                "notes": [
                    "Track authorization-policy mutations through emitted events; "
                    "the underlying table-backed state is non-enumerable for "
                    "generic polling."
                ],
            }
        ],
    }


def test_fetch_policy_event_history_decodes_paginated_logs():
    plan = _plan()

    class FakeClient:
        async def get(self, query):
            if query.from_block == 0:
                logs = [
                    SimpleNamespace(
                        address=plan["contract_address"],
                        topics=[
                            "0xa52ea92e6e955aa8ac66420b86350f7139959adfcc7e6a14eee1bd116d09860e",
                            "0x" + "00" * 31 + "02",
                            "0x" + "00" * 12 + "2222222222222222222222222222222222222222",
                            "0x12345678" + "00" * 28,
                        ],
                        data="0x" + "0" * 63 + "1",
                        block_number=12,
                        transaction_hash="0x" + "ab" * 32,
                        log_index=1,
                    )
                ]
                return SimpleNamespace(data=SimpleNamespace(logs=logs), next_block=20)

            logs = [
                SimpleNamespace(
                    address=plan["contract_address"],
                    topics=[
                        "0x4c9bdd0c8e073eb5eda2250b18d8e5121ff27b62064fbeeeed4869bb99bc5bf2",
                        "0x" + "00" * 12 + "3333333333333333333333333333333333333333",
                        "0x" + "00" * 31 + "02",
                    ],
                    data="0x" + "0" * 63 + "1",
                    block_number=25,
                    transaction_hash="0x" + "cd" * 32,
                    log_index=2,
                )
            ]
            return SimpleNamespace(data=SimpleNamespace(logs=logs), next_block=20)

    fake_module = SimpleNamespace(
        LogField=[SimpleNamespace(value="block_number"), SimpleNamespace(value="transaction_hash")],
        FieldSelection=lambda **kwargs: SimpleNamespace(**kwargs),
        LogSelection=lambda **kwargs: SimpleNamespace(**kwargs),
        Query=lambda **kwargs: SimpleNamespace(**kwargs),
    )

    records = __import__("asyncio").run(
        fetch_policy_event_history(
            plan,
            bearer_token="token",
            client=FakeClient(),
            hypersync_module=fake_module,
        )
    )

    assert records == [
        {
            "schema_version": "0.1",
            "contract_address": "0x1111111111111111111111111111111111111111",
            "contract_name": "RolesAuthority",
            "policy_id": "canCall_policy",
            "policy_label": "canCall policy",
            "event_signature": "RoleCapabilityUpdated(uint8,address,bytes4,bool)",
            "block_number": 12,
            "tx_hash": "0x" + "ab" * 32,
            "log_index": 1,
            "decoded_fields": {
                "role": 2,
                "target": "0x2222222222222222222222222222222222222222",
                "functionSig": "0x12345678",
                "enabled": True,
            },
        },
        {
            "schema_version": "0.1",
            "contract_address": "0x1111111111111111111111111111111111111111",
            "contract_name": "RolesAuthority",
            "policy_id": "canCall_policy",
            "policy_label": "canCall policy",
            "event_signature": "UserRoleUpdated(address,uint8,bool)",
            "block_number": 25,
            "tx_hash": "0x" + "cd" * 32,
            "log_index": 2,
            "decoded_fields": {
                "user": "0x3333333333333333333333333333333333333333",
                "role": 2,
                "enabled": True,
            },
        },
    ]


def test_reconstruct_policy_state():
    plan = _plan()
    records = [
        {
            "schema_version": "0.1",
            "contract_address": plan["contract_address"],
            "contract_name": plan["contract_name"],
            "policy_id": "canCall_policy",
            "policy_label": "canCall policy",
            "event_signature": "PublicCapabilityUpdated(address,bytes4,bool)",
            "block_number": 10,
            "tx_hash": "0x" + "aa" * 32,
            "log_index": 0,
            "decoded_fields": {
                "target": "0x1111111111111111111111111111111111111111",
                "functionSig": "0xdeadbeef",
                "enabled": True,
            },
        },
        {
            "schema_version": "0.1",
            "contract_address": plan["contract_address"],
            "contract_name": plan["contract_name"],
            "policy_id": "canCall_policy",
            "policy_label": "canCall policy",
            "event_signature": "RoleCapabilityUpdated(uint8,address,bytes4,bool)",
            "block_number": 11,
            "tx_hash": "0x" + "bb" * 32,
            "log_index": 1,
            "decoded_fields": {
                "role": 3,
                "target": "0x1111111111111111111111111111111111111111",
                "functionSig": "0x12345678",
                "enabled": True,
            },
        },
        {
            "schema_version": "0.1",
            "contract_address": plan["contract_address"],
            "contract_name": plan["contract_name"],
            "policy_id": "canCall_policy",
            "policy_label": "canCall policy",
            "event_signature": "UserRoleUpdated(address,uint8,bool)",
            "block_number": 12,
            "tx_hash": "0x" + "cc" * 32,
            "log_index": 2,
            "decoded_fields": {
                "user": "0x4444444444444444444444444444444444444444",
                "role": 3,
                "enabled": True,
            },
        },
    ]

    snapshot = reconstruct_policy_state(plan, records)

    assert snapshot == {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "RolesAuthority",
        "source": "hypersync",
        "event_count": 3,
        "public_capabilities": [
            {
                "target": "0x1111111111111111111111111111111111111111",
                "function_sig": "0xdeadbeef",
                "enabled": True,
                "last_updated_block": 10,
                "tx_hash": "0x" + "aa" * 32,
            }
        ],
        "role_capabilities": [
            {
                "role": 3,
                "target": "0x1111111111111111111111111111111111111111",
                "function_sig": "0x12345678",
                "enabled": True,
                "last_updated_block": 11,
                "tx_hash": "0x" + "bb" * 32,
            }
        ],
        "user_roles": [
            {
                "user": "0x4444444444444444444444444444444444444444",
                "role": 3,
                "enabled": True,
                "last_updated_block": 12,
                "tx_hash": "0x" + "cc" * 32,
            }
        ],
    }


def test_run_hypersync_policy_backfill_writes_outputs(tmp_path, monkeypatch):
    plan_path = tmp_path / "control_tracking_plan.json"
    plan_path.write_text(json.dumps(_plan()) + "\n")

    records = [
        {
            "schema_version": "0.1",
            "contract_address": "0x1111111111111111111111111111111111111111",
            "contract_name": "RolesAuthority",
            "policy_id": "canCall_policy",
            "policy_label": "canCall policy",
            "event_signature": "UserRoleUpdated(address,uint8,bool)",
            "block_number": 12,
            "tx_hash": "0x" + "cc" * 32,
            "log_index": 2,
            "decoded_fields": {
                "user": "0x4444444444444444444444444444444444444444",
                "role": 3,
                "enabled": True,
            },
        }
    ]

    async def fake_fetch(*args, **kwargs):
        return records

    monkeypatch.setattr("services.policy.hypersync_backfill.fetch_policy_event_history", fake_fetch)

    events_path, state_path = run_hypersync_policy_backfill(
        plan_path,
        bearer_token="token",
    )

    assert events_path.exists()
    assert state_path.exists()
    assert "UserRoleUpdated(address,uint8,bool)" in events_path.read_text()
    payload = json.loads(state_path.read_text())
    assert payload["event_count"] == 1
    assert payload["user_roles"][0]["role"] == 3
