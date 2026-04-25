from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from typing import cast  # noqa: E402

from services.resolution.mapping_enumerator import (  # noqa: E402
    _decode_address_arg_from_data,
    _decode_address_topic,
    _event_topic0,
)
from services.resolution.mapping_enumerator import (
    enumerate_mapping_allowlist as _enumerate,
)


def enumerate_mapping_allowlist(contract_address, writer_specs, **kwargs):
    return _enumerate(contract_address, cast(Any, writer_specs), **kwargs)


def _addr(hex_suffix: str) -> str:
    return "0x" + hex_suffix.lower().rjust(40, "0")


def _indexed_topic(addr: str) -> str:
    return "0x" + addr[2:].rjust(64, "0")


def _uint_topic(value: int) -> str:
    return "0x" + f"{value:064x}"


def _address_data(addr: str) -> str:
    return "0x" + addr[2:].rjust(64, "0")


def _log(topic0: str, indexed_args: list[str] | None = None, data: str = "0x", block: int = 1):
    topics = [topic0] + [_indexed_topic(a) for a in (indexed_args or [])]
    return SimpleNamespace(
        topics=topics,
        data=data,
        block_number=block,
        transaction_hash="0x" + "f" * 64,
        log_index=0,
    )


def _fake_client(batches: list[tuple[list[Any], int | None]]):
    calls: dict[str, int] = {"n": 0}

    class _Client:
        async def get(self, _query):
            i = calls["n"]
            calls["n"] += 1
            if i >= len(batches):
                return SimpleNamespace(data=[], next_block=None)
            logs, next_block = batches[i]
            return SimpleNamespace(data=logs, next_block=next_block)

    return _Client(), calls


class _FakeFieldEnumMeta(type):
    _members = ("address", "topic0", "data", "block_number")

    def __iter__(cls):
        for name in cls._members:
            yield cls(name)


class _FakeFieldEnum(metaclass=_FakeFieldEnumMeta):
    def __init__(self, name: str):
        self.value = name


class _FakeHypersyncModule:
    Query = SimpleNamespace
    LogSelection = SimpleNamespace
    FieldSelection = SimpleNamespace
    LogField = _FakeFieldEnum


def _run(coroutine):
    return asyncio.run(coroutine)


def test_event_topic0_hashes_canonical_signature():
    expected = "0xdd0e34038ac38b2a1ce960229778ac48a8719bc900b6c4f8d0475c6e8b385a60"
    assert _event_topic0("Rely(address)") == expected


def test_decode_address_topic_strips_padding():
    padded = _indexed_topic(_addr("dead1234"))
    assert _decode_address_topic(padded) == _addr("dead1234")


def test_decode_address_topic_rejects_wrong_length():
    assert _decode_address_topic("0xdead") == ""


def test_decode_address_arg_from_data_position_0():
    a = _addr("aa11")
    padded = "0x" + a[2:].rjust(64, "0")
    assert _decode_address_arg_from_data(padded, 0) == a


def test_decode_address_arg_from_data_position_1():
    a = _addr("aa11")
    b = _addr("bb22")
    data = "0x" + a[2:].rjust(64, "0") + b[2:].rjust(64, "0")
    assert _decode_address_arg_from_data(data, 1) == b


def _rely_spec():
    return {
        "mapping_name": "wards",
        "event_signature": "Rely(address)",
        "event_name": "Rely",
        "key_position": 0,
        "indexed_positions": [0],
        "direction": "add",
        "writer_function": "rely(address)",
    }


def _deny_spec():
    return {
        "mapping_name": "wards",
        "event_signature": "Deny(address)",
        "event_name": "Deny",
        "key_position": 0,
        "indexed_positions": [0],
        "direction": "remove",
        "writer_function": "deny(address)",
    }


def test_single_add_appears_in_output():
    rely_topic = _event_topic0("Rely(address)")
    alice = _addr("a11ce")
    client, _ = _fake_client(
        [
            ([_log(rely_topic, indexed_args=[alice], block=10)], None),
        ]
    )
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [_rely_spec()],
            client=client,
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    addresses = [p["address"] for p in out]
    assert addresses == [alice]
    assert out[0]["direction_history"] == ["add"]
    assert out[0]["last_seen_block"] == 10


def test_add_then_remove_leaves_empty():
    rely_topic = _event_topic0("Rely(address)")
    deny_topic = _event_topic0("Deny(address)")
    alice = _addr("a11ce")
    client, _ = _fake_client(
        [
            (
                [
                    _log(rely_topic, indexed_args=[alice], block=10),
                    _log(deny_topic, indexed_args=[alice], block=20),
                ],
                None,
            ),
        ]
    )
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [_rely_spec(), _deny_spec()],
            client=client,
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    assert out == []


def test_conflicting_directions_for_same_event_topic_are_rejected():
    topic = _event_topic0("WhitelistSet(address,bool)")
    alice = _addr("a11ce")
    client, calls = _fake_client([([_log(topic, data=_address_data(alice), block=10)], None)])
    add_spec = {
        "mapping_name": "whitelist",
        "event_signature": "WhitelistSet(address,bool)",
        "event_name": "WhitelistSet",
        "key_position": 0,
        "indexed_positions": [],
        "direction": "add",
        "writer_function": "setWhitelisted(address,bool)",
    }
    remove_spec = {
        **add_spec,
        "direction": "remove",
        "writer_function": "unsetWhitelisted(address,bool)",
    }
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [add_spec, remove_spec],
            client=client,
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    assert out == []
    assert calls["n"] == 0


def test_add_remove_add_ends_present():
    rely_topic = _event_topic0("Rely(address)")
    deny_topic = _event_topic0("Deny(address)")
    alice = _addr("a11ce")
    client, _ = _fake_client(
        [
            (
                [
                    _log(rely_topic, indexed_args=[alice], block=10),
                    _log(deny_topic, indexed_args=[alice], block=20),
                    _log(rely_topic, indexed_args=[alice], block=30),
                ],
                None,
            ),
        ]
    )
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [_rely_spec(), _deny_spec()],
            client=client,
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    assert [p["address"] for p in out] == [alice]
    assert out[0]["direction_history"] == ["add", "remove", "add"]
    assert out[0]["last_seen_block"] == 30


def test_multiple_principals_independent():
    rely_topic = _event_topic0("Rely(address)")
    alice = _addr("a11ce")
    bob = _addr("b0b")
    client, _ = _fake_client(
        [
            (
                [
                    _log(rely_topic, indexed_args=[alice], block=10),
                    _log(rely_topic, indexed_args=[bob], block=11),
                ],
                None,
            ),
        ]
    )
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [_rely_spec()],
            client=client,
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    addresses = sorted(p["address"] for p in out)
    assert addresses == sorted([alice, bob])


def test_non_indexed_key_decodes_from_data_slot():
    topic = _event_topic0("SetAuthorized(address)")
    alice = _addr("a11ce")
    client, _ = _fake_client([([_log(topic, data=_address_data(alice), block=10)], None)])
    spec = {
        "mapping_name": "authorized",
        "event_signature": "SetAuthorized(address)",
        "event_name": "SetAuthorized",
        "key_position": 0,
        "indexed_positions": [],
        "direction": "add",
        "writer_function": "setAuthorized(address)",
    }
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [spec],
            client=client,
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    assert [p["address"] for p in out] == [alice]


def test_indexed_argument_before_non_indexed_key_uses_data_slot_zero():
    topic = _event_topic0("Foo(uint256,address)")
    alice = _addr("a11ce")
    log = SimpleNamespace(
        topics=[topic, _uint_topic(42)],
        data=_address_data(alice),
        block_number=10,
        transaction_hash="0x" + "f" * 64,
        log_index=0,
    )
    client, _ = _fake_client([([log], None)])
    spec = {
        "mapping_name": "authorized",
        "event_signature": "Foo(uint256,address)",
        "event_name": "Foo",
        "key_position": 1,
        "indexed_positions": [0],
        "direction": "add",
        "writer_function": "setAuthorized(address)",
    }
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [spec],
            client=client,
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    assert [p["address"] for p in out] == [alice]


def test_empty_history_returns_empty():
    client, _ = _fake_client([])
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [_rely_spec()],
            client=client,
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    assert out == []


def test_no_specs_returns_empty_without_client():
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [],
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    assert out == []


def test_pagination_via_next_block():
    rely_topic = _event_topic0("Rely(address)")
    alice = _addr("a11ce")
    bob = _addr("b0b")
    carol = _addr("ca201")
    client, calls = _fake_client(
        [
            ([_log(rely_topic, indexed_args=[alice], block=10)], 11),
            ([_log(rely_topic, indexed_args=[bob], block=50)], 51),
            ([_log(rely_topic, indexed_args=[carol], block=99)], None),
        ]
    )
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [_rely_spec()],
            client=client,
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    addresses = sorted(p["address"] for p in out)
    assert addresses == sorted([alice, bob, carol])
    assert calls["n"] == 3


def test_unknown_topic_ignored():
    rely_topic = _event_topic0("Rely(address)")
    other_topic = _event_topic0("Unrelated(uint256)")
    alice = _addr("a11ce")
    client, _ = _fake_client(
        [
            (
                [
                    _log(other_topic, indexed_args=[alice], block=5),
                    _log(rely_topic, indexed_args=[alice], block=10),
                ],
                None,
            ),
        ]
    )
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [_rely_spec()],
            client=client,
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    assert [p["address"] for p in out] == [alice]


def test_malformed_address_topic_skipped():
    rely_topic = _event_topic0("Rely(address)")
    bad_log = SimpleNamespace(
        topics=[rely_topic, "0xdead"],
        data="0x",
        block_number=10,
        transaction_hash="0x" + "f" * 64,
        log_index=0,
    )
    alice = _addr("a11ce")
    good_log = _log(rely_topic, indexed_args=[alice], block=11)
    client, _ = _fake_client([([bad_log, good_log], None)])
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [_rely_spec()],
            client=client,
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    assert [p["address"] for p in out] == [alice]
