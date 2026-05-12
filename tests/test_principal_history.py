from __future__ import annotations

from eth_utils.crypto import keccak

from services.policy.principal_history import build_role_authority_history

AUTHORITY = "0x" + "aa" * 20
TARGET = "0x" + "bb" * 20
USER = "0x" + "cc" * 20
SELECTOR = "0x12345678"


def _topic(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


def _address_topic(address: str) -> str:
    return "0x" + address[2:].rjust(64, "0")


def _uint_topic(value: int) -> str:
    return "0x" + hex(value)[2:].rjust(64, "0")


def _bytes4_topic(selector: str) -> str:
    return "0x" + selector[2:].ljust(64, "0")


def _bool_word(value: bool) -> str:
    return "0x" + ("1" if value else "0").rjust(64, "0")


def _log(topic0: str, topics: list[str], enabled: bool, block: int, log_index: int) -> dict:
    return {
        "blockNumber": hex(block),
        "transactionIndex": "0x0",
        "logIndex": hex(log_index),
        "transactionHash": "0x" + f"{block:064x}",
        "topics": [topic0, *topics],
        "data": _bool_word(enabled),
    }


def test_role_authority_history_uses_event_shapes_not_names():
    """The history reducer keys off indexed ABI shape, not event names."""
    abi = [
        {
            "type": "event",
            "name": "WhateverA",
            "inputs": [
                {"type": "address", "indexed": True},
                {"type": "uint8", "indexed": True},
                {"type": "bool", "indexed": False},
            ],
        },
        {
            "type": "event",
            "name": "WhateverB",
            "inputs": [
                {"type": "uint8", "indexed": True},
                {"type": "address", "indexed": True},
                {"type": "bytes4", "indexed": True},
                {"type": "bool", "indexed": False},
            ],
        },
        {
            "type": "event",
            "name": "WhateverC",
            "inputs": [
                {"type": "address", "indexed": True},
                {"type": "bytes4", "indexed": True},
                {"type": "bool", "indexed": False},
            ],
        },
    ]
    user_role_topic = _topic("WhateverA(address,uint8,bool)")
    role_cap_topic = _topic("WhateverB(uint8,address,bytes4,bool)")
    public_cap_topic = _topic("WhateverC(address,bytes4,bool)")

    logs_by_topic = {
        user_role_topic: [
            _log(user_role_topic, [_address_topic(USER), _uint_topic(5)], True, 12, 0),
            _log(user_role_topic, [_address_topic(USER), _uint_topic(5)], False, 20, 0),
            _log(user_role_topic, [_address_topic(USER), _uint_topic(5)], True, 25, 0),
        ],
        role_cap_topic: [
            _log(role_cap_topic, [_uint_topic(5), _address_topic(TARGET), _bytes4_topic(SELECTOR)], True, 10, 0),
            _log(role_cap_topic, [_uint_topic(5), _address_topic(TARGET), _bytes4_topic(SELECTOR)], False, 30, 0),
        ],
        public_cap_topic: [],
    }

    payload = build_role_authority_history(
        authority_address=AUTHORITY,
        chain_id=1,
        functions={(TARGET, SELECTOR): "pause()"},
        abi=abi,
        logs_by_topic=logs_by_topic,
    )

    assert payload["source"]["status"] == "ok"
    assert payload["source"]["event_topics"] == {
        "user_role": user_role_topic,
        "role_capability": role_cap_topic,
        "public_capability": public_cap_topic,
    }

    permissions = payload["function_permissions"]
    assert len(permissions) == 2
    assert permissions[0]["function"] == "pause()"
    assert permissions[0]["principal"] == USER
    assert permissions[0]["roles"] == [5]
    assert permissions[0]["granted_at_block"] == 12
    assert permissions[0]["revoked_at_block"] == 20
    assert permissions[0]["status"] == "revoked"
    assert permissions[1]["granted_at_block"] == 25
    assert permissions[1]["revoked_at_block"] == 30

    role_intervals = payload["role_membership"]
    assert [item["status"] for item in role_intervals] == ["revoked", "active"]
    assert role_intervals[1]["principal"] == USER
    assert role_intervals[1]["role"] == 5
