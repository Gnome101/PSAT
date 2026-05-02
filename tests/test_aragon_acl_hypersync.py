"""Tests for ``HyperSyncAragonACLLogFetcher`` with a stubbed
hypersync module. Mirrors the role_grants HyperSync test set."""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution.repos.aragon_acl_hypersync import (  # noqa: E402
    HyperSyncAragonACLLogFetcher,
)
from workers.aragon_acl_indexer import SET_PERMISSION_TOPIC0  # noqa: E402


# ---------------------------------------------------------------------------
# Stub HyperSync surface (same pattern as test_role_grants_hypersync).
# ---------------------------------------------------------------------------


@dataclass
class _StubLog:
    topics: list[str]
    block_number: int
    block_hash: str
    transaction_hash: str
    log_index: int
    transaction_index: int
    data: str
    address: str = "0x" + "ab" * 20


@dataclass
class _StubData:
    logs: list[_StubLog] = field(default_factory=list)


@dataclass
class _StubResponse:
    data: _StubData
    next_block: int | None = None


@dataclass
class _StubLogField:
    value: str


class _StubLogFieldEnum:
    ADDRESS = _StubLogField("address")
    BLOCK_HASH = _StubLogField("block_hash")
    BLOCK_NUMBER = _StubLogField("block_number")
    DATA = _StubLogField("data")
    LOG_INDEX = _StubLogField("log_index")
    REMOVED = _StubLogField("removed")
    TOPIC0 = _StubLogField("topic0")
    TOPIC1 = _StubLogField("topic1")
    TOPIC2 = _StubLogField("topic2")
    TOPIC3 = _StubLogField("topic3")
    TRANSACTION_HASH = _StubLogField("transaction_hash")
    TRANSACTION_INDEX = _StubLogField("transaction_index")

    def __iter__(self):
        return iter(
            [
                self.ADDRESS,
                self.BLOCK_HASH,
                self.BLOCK_NUMBER,
                self.DATA,
                self.LOG_INDEX,
                self.REMOVED,
                self.TOPIC0,
                self.TOPIC1,
                self.TOPIC2,
                self.TOPIC3,
                self.TRANSACTION_HASH,
                self.TRANSACTION_INDEX,
            ]
        )


@dataclass
class _StubQuery:
    from_block: int
    to_block: int
    logs: list[Any]
    field_selection: Any


@dataclass
class _StubLogSelection:
    address: list[str]
    topics: list[Any]


@dataclass
class _StubFieldSelection:
    log: list[str]


@dataclass
class _StubClientConfig:
    url: str
    bearer_token: str


class _StubClient:
    def __init__(self, config: _StubClientConfig):
        self.config = config
        self.queries_seen: list[_StubQuery] = []
        self._responses: list[_StubResponse] = []

    async def get(self, query: _StubQuery):
        self.queries_seen.append(query)
        if not self._responses:
            return _StubResponse(data=_StubData(logs=[]), next_block=None)
        return self._responses.pop(0)


class _StubHypersync:
    LogField = _StubLogFieldEnum()
    Query = _StubQuery
    LogSelection = _StubLogSelection
    FieldSelection = _StubFieldSelection
    ClientConfig = _StubClientConfig

    def __init__(self):
        self.last_client: _StubClient | None = None

    def HypersyncClient(self, config: _StubClientConfig) -> _StubClient:  # noqa: N802
        client = _StubClient(config)
        self.last_client = client
        return client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _addr_topic(addr: str) -> str:
    return "0x" + "00" * 12 + addr[2:].zfill(40)


def _bool_data(allowed: bool) -> str:
    word = bytes(31) + bytes([1 if allowed else 0])
    return "0x" + word.hex()


def _make_log(
    *,
    entity: str,
    app: str,
    role: bytes,
    allowed: bool,
    block_number: int,
    block_hash: bytes,
    tx_hash: bytes,
    log_index: int,
    transaction_index: int = 0,
) -> _StubLog:
    return _StubLog(
        topics=[
            "0x" + SET_PERMISSION_TOPIC0.hex(),
            _addr_topic(entity),
            _addr_topic(app),
            "0x" + role.hex(),
        ],
        block_number=block_number,
        block_hash="0x" + block_hash.hex(),
        transaction_hash="0x" + tx_hash.hex(),
        log_index=log_index,
        transaction_index=transaction_index,
        data=_bool_data(allowed),
    )


def _make_fetcher_with_responses(stub: _StubHypersync, pages: list[_StubResponse]):
    def make_client(config):
        client = _StubClient(config)
        client._responses = list(pages)
        stub.last_client = client
        return client

    stub.HypersyncClient = make_client  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_decode_set_permission_grant_via_hypersync():
    role = b"\x01" * 32
    entity = "0x" + "11" * 20
    app = "0x" + "22" * 20
    stub = _StubHypersync()
    _make_fetcher_with_responses(
        stub,
        [
            _StubResponse(
                data=_StubData(
                    logs=[
                        _make_log(
                            entity=entity,
                            app=app,
                            role=role,
                            allowed=True,
                            block_number=100,
                            block_hash=b"\xaa" * 32,
                            tx_hash=b"\xbb" * 32,
                            log_index=3,
                            transaction_index=2,
                        )
                    ]
                ),
                next_block=None,
            )
        ],
    )
    fetcher = HyperSyncAragonACLLogFetcher(bearer_token="t", hypersync_module=stub)
    logs = fetcher.fetch_logs(
        chain_id=1, contract_address="0x" + "ee" * 20, from_block=0, to_block=200
    )
    assert len(logs) == 1
    out = logs[0]
    assert out.allowed is True
    assert out.entity == entity
    assert out.app == app
    assert out.role == role
    assert out.block_number == 100
    assert out.log_index == 3


def test_decode_set_permission_revoke_via_hypersync():
    role = b"\x02" * 32
    stub = _StubHypersync()
    _make_fetcher_with_responses(
        stub,
        [
            _StubResponse(
                data=_StubData(
                    logs=[
                        _make_log(
                            entity="0x" + "33" * 20,
                            app="0x" + "44" * 20,
                            role=role,
                            allowed=False,
                            block_number=200,
                            block_hash=b"\xcc" * 32,
                            tx_hash=b"\xdd" * 32,
                            log_index=0,
                        )
                    ]
                ),
                next_block=None,
            )
        ],
    )
    fetcher = HyperSyncAragonACLLogFetcher(bearer_token="t", hypersync_module=stub)
    logs = fetcher.fetch_logs(
        chain_id=1, contract_address="0x" + "ee" * 20, from_block=0, to_block=300
    )
    assert len(logs) == 1
    assert logs[0].allowed is False


def test_pagination_follows_next_block():
    role = b"\x03" * 32
    stub = _StubHypersync()
    _make_fetcher_with_responses(
        stub,
        [
            _StubResponse(
                data=_StubData(
                    logs=[
                        _make_log(
                            entity="0x" + "11" * 20,
                            app="0x" + "22" * 20,
                            role=role,
                            allowed=True,
                            block_number=50,
                            block_hash=b"\x11" * 32,
                            tx_hash=b"\x22" * 32,
                            log_index=0,
                        )
                    ]
                ),
                next_block=51,
            ),
            _StubResponse(
                data=_StubData(
                    logs=[
                        _make_log(
                            entity="0x" + "44" * 20,
                            app="0x" + "55" * 20,
                            role=role,
                            allowed=True,
                            block_number=60,
                            block_hash=b"\x33" * 32,
                            tx_hash=b"\x44" * 32,
                            log_index=0,
                        )
                    ]
                ),
                next_block=None,
            ),
        ],
    )
    fetcher = HyperSyncAragonACLLogFetcher(bearer_token="t", hypersync_module=stub)
    logs = fetcher.fetch_logs(
        chain_id=1, contract_address="0x" + "ee" * 20, from_block=0, to_block=200
    )
    assert len(logs) == 2
    queries = stub.last_client.queries_seen  # type: ignore[union-attr]
    assert len(queries) == 2
    assert queries[0].from_block == 0
    assert queries[1].from_block == 51


def test_topic_filter_pins_set_permission_topic0():
    stub = _StubHypersync()
    _make_fetcher_with_responses(
        stub,
        [_StubResponse(data=_StubData(logs=[]), next_block=None)],
    )
    fetcher = HyperSyncAragonACLLogFetcher(bearer_token="t", hypersync_module=stub)
    fetcher.fetch_logs(
        chain_id=1, contract_address="0x" + "ee" * 20, from_block=0, to_block=10
    )
    q = stub.last_client.queries_seen[0]  # type: ignore[union-attr]
    log_selection = q.logs[0]
    assert log_selection.topics[0] == ["0x" + SET_PERMISSION_TOPIC0.hex()]


def test_chain_id_to_url_mapping():
    stub = _StubHypersync()
    captured: list[str] = []

    def make_client(config):
        captured.append(config.url)
        client = _StubClient(config)
        client._responses = [_StubResponse(data=_StubData(logs=[]), next_block=None)]
        return client

    stub.HypersyncClient = make_client  # type: ignore[assignment]
    fetcher = HyperSyncAragonACLLogFetcher(bearer_token="t", hypersync_module=stub)
    fetcher.fetch_logs(
        chain_id=137, contract_address="0x" + "ee" * 20, from_block=0, to_block=10
    )
    assert captured == ["https://polygon.hypersync.xyz"]

    fetcher2 = HyperSyncAragonACLLogFetcher(bearer_token="t", hypersync_module=stub)
    with pytest.raises(RuntimeError, match="HyperSync URL not configured"):
        fetcher2.fetch_logs(
            chain_id=99999,
            contract_address="0x" + "ee" * 20,
            from_block=0,
            to_block=10,
        )


def test_missing_token_raises():
    stub = _StubHypersync()
    fetcher = HyperSyncAragonACLLogFetcher(bearer_token=None, hypersync_module=stub)
    fetcher.bearer_token = None
    with pytest.raises(RuntimeError, match="API token"):
        fetcher.fetch_logs(
            chain_id=1, contract_address="0x" + "ee" * 20, from_block=0, to_block=10
        )


def test_empty_range_returns_empty():
    stub = _StubHypersync()
    fetcher = HyperSyncAragonACLLogFetcher(bearer_token="t", hypersync_module=stub)
    assert (
        fetcher.fetch_logs(
            chain_id=1, contract_address="0x" + "ee" * 20, from_block=100, to_block=50
        )
        == []
    )


def test_malformed_logs_skipped_via_hypersync():
    """Wrong topic0 / short data / wrong topic count are all
    skipped — only the well-formed entry survives."""
    role = b"\x77" * 32
    good = _make_log(
        entity="0x" + "77" * 20,
        app="0x" + "88" * 20,
        role=role,
        allowed=True,
        block_number=40,
        block_hash=b"\x55" * 32,
        tx_hash=b"\x66" * 32,
        log_index=0,
    )
    bad_topic0 = _make_log(
        entity="0x" + "11" * 20,
        app="0x" + "22" * 20,
        role=role,
        allowed=True,
        block_number=10,
        block_hash=b"\x11" * 32,
        tx_hash=b"\x22" * 32,
        log_index=0,
    )
    bad_topic0.topics[0] = "0x" + "ff" * 32
    short_topics = _StubLog(
        topics=[],
        block_number=20,
        block_hash="0x" + "00" * 32,
        transaction_hash="0x" + "00" * 32,
        log_index=0,
        transaction_index=0,
        data="0x" + bytes(32).hex(),
    )
    garbled_data = _make_log(
        entity="0x" + "33" * 20,
        app="0x" + "44" * 20,
        role=role,
        allowed=True,
        block_number=30,
        block_hash=b"\x33" * 32,
        tx_hash=b"\x44" * 32,
        log_index=0,
    )
    garbled_data.data = "0x" + "ff" * 32
    stub = _StubHypersync()
    _make_fetcher_with_responses(
        stub,
        [
            _StubResponse(
                data=_StubData(logs=[bad_topic0, short_topics, garbled_data, good]),
                next_block=None,
            )
        ],
    )
    fetcher = HyperSyncAragonACLLogFetcher(bearer_token="t", hypersync_module=stub)
    logs = fetcher.fetch_logs(
        chain_id=1, contract_address="0x" + "ee" * 20, from_block=0, to_block=100
    )
    assert len(logs) == 1
    assert logs[0].block_number == 40
