"""Phase 3b — Hypersync set-replay enumeration of mapping allowlists.

Exercises `enumerate_mapping_allowlist` with a fake Hypersync client.
Covers set-semantics (add/remove/re-add), address decoding from
indexed topics vs. data-field slots, multi-event mappings (rely +
deny), and pagination via `next_block`.
"""

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
    """Test shim — pyright's TypedDict invariance rejects list[dict]
    passed where list[WriterEventSpec] is declared. Cast once here."""
    return _enumerate(contract_address, cast(Any, writer_specs), **kwargs)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _addr(hex_suffix: str) -> str:
    """20-byte address with zero-padded high bytes."""
    return "0x" + hex_suffix.lower().rjust(40, "0")


def _indexed_topic(addr: str) -> str:
    """Address padded to 32 bytes — how indexed address args appear in
    Ethereum log topics."""
    return "0x" + addr[2:].rjust(64, "0")


def _log(topic0: str, indexed_args: list[str] | None = None, data: str = "0x", block: int = 1):
    """Mock log matching Hypersync's `data` / `topics` shape."""
    topics = [topic0] + [_indexed_topic(a) for a in (indexed_args or [])]
    return SimpleNamespace(
        topics=topics,
        data=data,
        block_number=block,
        transaction_hash="0x" + "f" * 64,
        log_index=0,
    )


def _fake_client(batches: list[tuple[list[Any], int | None]]):
    """Client whose `.get(query)` returns successive batches. Each
    batch is `(logs, next_block)` — when `next_block` is None, the
    loop terminates."""
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
    """Metaclass so `for x in LogField` iterates the members — same
    shape Hypersync's real enum presents to callers."""

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


# ---------------------------------------------------------------------------
# Topic / data decoding
# ---------------------------------------------------------------------------


def test_event_topic0_hashes_canonical_signature():
    """`keccak('Rely(address)')` is the well-known topic0."""
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


# ---------------------------------------------------------------------------
# Set-semantics
# ---------------------------------------------------------------------------


def _rely_spec():
    return {
        "mapping_name": "wards",
        "event_signature": "Rely(address)",
        "event_name": "Rely",
        "key_position": 0,
        "direction": "add",
        "writer_function": "rely(address)",
    }


def _deny_spec():
    return {
        "mapping_name": "wards",
        "event_signature": "Deny(address)",
        "event_name": "Deny",
        "key_position": 0,
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
    """Early-out: no specs → no query, no token required."""
    out = _run(
        enumerate_mapping_allowlist(
            "0xCC00000000000000000000000000000000000001",
            [],
            hypersync_module=_FakeHypersyncModule(),
        )
    )
    assert out == []


def test_pagination_via_next_block():
    """Hypersync returns results in chunks — the enumerator must page
    through until next_block stops advancing."""
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
    """A log with a topic0 that isn't in our spec set shouldn't
    pollute the output — e.g. if the contract is noisy with other
    events, we filter by topic0 at the query level, but this is a
    defense-in-depth check."""
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
    """Topics that don't decode to a valid 20-byte address get
    skipped rather than crashing the whole enumeration."""
    rely_topic = _event_topic0("Rely(address)")
    bad_log = SimpleNamespace(
        topics=[rely_topic, "0xdead"],  # topic1 is malformed
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
