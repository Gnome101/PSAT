"""Tests for the HyperSync-backed log fetcher.

No live HyperSync — a stub module is injected via the
``hypersync_module=`` parameter and asserts on:

  * topic decoding (RoleGranted vs RoleRevoked, member from
    indexed account topic)
  * malformed log entries are skipped (defensive decode)
  * pagination via ``next_block`` continues until the page reaches
    ``to_block`` or signals exhaustion
  * chain_id → URL mapping is honored
  * ``ENVIO_API_TOKEN`` is required (else RuntimeError)
  * the request specifies the topic0-OR filter for both events"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution.repos.role_grants_hypersync import (  # noqa: E402
    HyperSyncLogFetcher,
)
from workers.role_grants_indexer import (  # noqa: E402
    ROLE_GRANTED_TOPIC0,
    ROLE_REVOKED_TOPIC0,
)

# ---------------------------------------------------------------------------
# Stub HyperSync module
# ---------------------------------------------------------------------------


@dataclass
class _StubLog:
    topics: list[str]
    block_number: int
    block_hash: str
    transaction_hash: str
    log_index: int
    transaction_index: int
    address: str = "0x" + "ab" * 20
    data: str = "0x"


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
    """Mirrors the ``hypersync.LogField`` enum surface the fetcher
    iterates over to build ``field_selection``."""

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
    """Returns a queue of _StubResponses on successive ``get`` calls.
    Each test pre-loads the queue and asserts the queries it sees."""

    def __init__(self, config: _StubClientConfig):
        self.config = config
        self.queries_seen: list[_StubQuery] = []
        # Tests poke ``_responses`` after construction; the module
        # stub keeps a singleton client so the hook works.
        self._responses: list[_StubResponse] = []

    async def get(self, query: _StubQuery):
        self.queries_seen.append(query)
        if not self._responses:
            return _StubResponse(data=_StubData(logs=[]), next_block=None)
        return self._responses.pop(0)


class _StubHypersync:
    """Drop-in replacement for ``import hypersync``."""

    LogField = _StubLogFieldEnum()
    Query = _StubQuery
    LogSelection = _StubLogSelection
    FieldSelection = _StubFieldSelection
    ClientConfig = _StubClientConfig

    def __init__(self):
        self.last_client: _StubClient | None = None

    def HypersyncClient(self, config: _StubClientConfig) -> _StubClient:  # noqa: N802 — mirror the real surface
        client = _StubClient(config)
        self.last_client = client
        return client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_log(
    *,
    topic0: bytes,
    role: bytes,
    member: str,
    block_number: int,
    block_hash: bytes,
    tx_hash: bytes,
    log_index: int,
    transaction_index: int = 0,
) -> _StubLog:
    member_padded = "0x" + "00" * 12 + member[2:].zfill(40)
    return _StubLog(
        topics=[
            "0x" + topic0.hex(),
            "0x" + role.hex(),
            member_padded,
        ],
        block_number=block_number,
        block_hash="0x" + block_hash.hex(),
        transaction_hash="0x" + tx_hash.hex(),
        log_index=log_index,
        transaction_index=transaction_index,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_decode_role_granted_via_hypersync():
    role = b"\x01" * 32
    member = "0x" + "11" * 20
    block_hash = b"\xaa" * 32
    tx_hash = b"\xbb" * 32

    stub = _StubHypersync()
    fetcher = HyperSyncLogFetcher(
        bearer_token="fake-token",
        hypersync_module=stub,
    )
    # We need the client to exist before we can populate _responses,
    # so prime it with a placeholder fetch then re-prime. Simpler:
    # patch the fetcher's _resolve_module to wire the responses in
    # via a factory. For now use a flat single-page response.
    page = _StubResponse(
        data=_StubData(
            logs=[
                _make_log(
                    topic0=ROLE_GRANTED_TOPIC0,
                    role=role,
                    member=member,
                    block_number=100,
                    block_hash=block_hash,
                    tx_hash=tx_hash,
                    log_index=3,
                    transaction_index=2,
                )
            ]
        ),
        next_block=None,  # exhausted
    )
    # Wire the response queue at the module level — the stub's
    # HypersyncClient constructor returns a fresh _StubClient each
    # time, so we override that by attaching the queue to the stub
    # itself. Patch the constructor.
    stub._next_responses = [page]  # type: ignore[attr-defined]

    def make_client(config):
        client = _StubClient(config)
        client._responses = list(stub._next_responses)  # type: ignore[attr-defined]
        stub.last_client = client
        return client

    stub.HypersyncClient = make_client  # type: ignore[assignment]

    logs = fetcher.fetch_logs(
        chain_id=1,
        contract_address="0x" + "ab" * 20,
        from_block=0,
        to_block=200,
    )
    assert len(logs) == 1
    out = logs[0]
    assert out.direction == "grant"
    assert out.role == role
    assert out.member == member
    assert out.block_number == 100
    assert out.block_hash == block_hash
    assert out.tx_hash == tx_hash
    assert out.log_index == 3
    assert out.transaction_index == 2


def _make_fetcher_with_responses(stub: _StubHypersync, pages: list[_StubResponse]):
    def make_client(config):
        client = _StubClient(config)
        client._responses = list(pages)
        stub.last_client = client
        return client

    stub.HypersyncClient = make_client  # type: ignore[assignment]


def test_decode_role_revoked_via_hypersync():
    role = b"\x02" * 32
    member = "0x" + "22" * 20
    stub = _StubHypersync()
    _make_fetcher_with_responses(
        stub,
        [
            _StubResponse(
                data=_StubData(
                    logs=[
                        _make_log(
                            topic0=ROLE_REVOKED_TOPIC0,
                            role=role,
                            member=member,
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
    fetcher = HyperSyncLogFetcher(bearer_token="fake-token", hypersync_module=stub)
    logs = fetcher.fetch_logs(chain_id=1, contract_address="0x" + "ab" * 20, from_block=0, to_block=300)
    assert len(logs) == 1
    assert logs[0].direction == "revoke"
    assert logs[0].member == member


def test_pagination_follows_next_block():
    """A response with ``next_block`` triggers a follow-up query
    starting at that block. Stop when next_block is None or has
    advanced past the requested ``to_block``."""
    role = b"\x33" * 32
    a = "0x" + "33" * 20
    b = "0x" + "44" * 20
    stub = _StubHypersync()
    pages = [
        _StubResponse(
            data=_StubData(
                logs=[
                    _make_log(
                        topic0=ROLE_GRANTED_TOPIC0,
                        role=role,
                        member=a,
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
                        topic0=ROLE_GRANTED_TOPIC0,
                        role=role,
                        member=b,
                        block_number=60,
                        block_hash=b"\x33" * 32,
                        tx_hash=b"\x44" * 32,
                        log_index=0,
                    )
                ]
            ),
            next_block=None,
        ),
    ]
    _make_fetcher_with_responses(stub, pages)
    fetcher = HyperSyncLogFetcher(bearer_token="fake-token", hypersync_module=stub)
    logs = fetcher.fetch_logs(chain_id=1, contract_address="0x" + "ab" * 20, from_block=0, to_block=200)
    assert [log.member for log in logs] == [a, b]
    queries = stub.last_client.queries_seen  # type: ignore[union-attr]
    assert len(queries) == 2
    # First query starts at 0, second resumes at 51 (the next_block
    # cursor returned by the first page).
    assert queries[0].from_block == 0
    assert queries[1].from_block == 51


def test_topic_filter_includes_both_events():
    """The HyperSync query must include both RoleGranted and
    RoleRevoked topic0 values as a topic-OR. Pinned so refactors
    don't accidentally narrow the filter."""
    stub = _StubHypersync()
    _make_fetcher_with_responses(
        stub,
        [_StubResponse(data=_StubData(logs=[]), next_block=None)],
    )
    fetcher = HyperSyncLogFetcher(bearer_token="fake-token", hypersync_module=stub)
    fetcher.fetch_logs(chain_id=1, contract_address="0x" + "ab" * 20, from_block=0, to_block=10)
    q = stub.last_client.queries_seen[0]  # type: ignore[union-attr]
    log_selection = q.logs[0]
    assert log_selection.address == ["0x" + "ab" * 20]
    topic_or = log_selection.topics[0]
    assert "0x" + ROLE_GRANTED_TOPIC0.hex() in topic_or
    assert "0x" + ROLE_REVOKED_TOPIC0.hex() in topic_or


def test_malformed_logs_skipped_via_hypersync():
    """Malformed entries (missing topics, wrong byte length,
    unknown topic0) are silently skipped; one good entry survives."""
    role = b"\x55" * 32
    member = "0x" + "55" * 20
    stub = _StubHypersync()
    bad_topics = _StubLog(
        topics=[],
        block_number=10,
        block_hash="0x" + "00" * 32,
        transaction_hash="0x" + "00" * 32,
        log_index=0,
        transaction_index=0,
    )
    unknown_topic0 = _make_log(
        topic0=b"\xff" * 32,
        role=role,
        member=member,
        block_number=20,
        block_hash=b"\x11" * 32,
        tx_hash=b"\x22" * 32,
        log_index=0,
    )
    short_block_hash = _make_log(
        topic0=ROLE_GRANTED_TOPIC0,
        role=role,
        member=member,
        block_number=30,
        block_hash=b"\x33" * 32,
        tx_hash=b"\x44" * 32,
        log_index=0,
    )
    short_block_hash.block_hash = "0xdead"  # malformed (not 32 bytes)
    good = _make_log(
        topic0=ROLE_GRANTED_TOPIC0,
        role=role,
        member=member,
        block_number=40,
        block_hash=b"\x55" * 32,
        tx_hash=b"\x66" * 32,
        log_index=0,
    )
    _make_fetcher_with_responses(
        stub,
        [
            _StubResponse(
                data=_StubData(logs=[bad_topics, unknown_topic0, short_block_hash, good]),
                next_block=None,
            )
        ],
    )
    fetcher = HyperSyncLogFetcher(bearer_token="fake-token", hypersync_module=stub)
    logs = fetcher.fetch_logs(chain_id=1, contract_address="0x" + "ab" * 20, from_block=0, to_block=100)
    assert len(logs) == 1
    assert logs[0].block_number == 40


def test_chain_id_to_url_mapping():
    """The fetcher picks the right HyperSync URL per chain_id and
    fails with a clear message if a chain isn't mapped."""
    stub = _StubHypersync()
    captured: list[str] = []

    def make_client(config):
        captured.append(config.url)
        client = _StubClient(config)
        client._responses = [_StubResponse(data=_StubData(logs=[]), next_block=None)]
        return client

    stub.HypersyncClient = make_client  # type: ignore[assignment]
    fetcher = HyperSyncLogFetcher(bearer_token="t", hypersync_module=stub)
    fetcher.fetch_logs(chain_id=137, contract_address="0x" + "ab" * 20, from_block=0, to_block=10)
    assert captured == ["https://polygon.hypersync.xyz"]

    # Unmapped chain id raises with a clear message.
    fetcher2 = HyperSyncLogFetcher(bearer_token="t", hypersync_module=stub)
    with pytest.raises(RuntimeError, match="HyperSync URL not configured"):
        fetcher2.fetch_logs(
            chain_id=99999,
            contract_address="0x" + "ab" * 20,
            from_block=0,
            to_block=10,
        )


def test_missing_token_raises():
    """Without ``ENVIO_API_TOKEN`` (and no explicit token), the
    fetcher refuses to talk to HyperSync rather than silently
    sending an unauthenticated request."""
    stub = _StubHypersync()
    fetcher = HyperSyncLogFetcher(bearer_token=None, hypersync_module=stub)
    fetcher.bearer_token = None  # belt-and-suspenders for env-var fallback
    with pytest.raises(RuntimeError, match="API token"):
        fetcher.fetch_logs(chain_id=1, contract_address="0x" + "ab" * 20, from_block=0, to_block=10)


def test_empty_range_returns_empty():
    stub = _StubHypersync()
    fetcher = HyperSyncLogFetcher(bearer_token="t", hypersync_module=stub)
    assert fetcher.fetch_logs(chain_id=1, contract_address="0x" + "ab" * 20, from_block=100, to_block=50) == []
