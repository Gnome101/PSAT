"""Decode + chunking tests for the RPC-backed role_grants
fetcher.

No live RPC — ``utils.rpc.rpc_request`` is monkeypatched to return
fixture payloads. Unit-tests cover:

  * Topic decoding (RoleGranted vs RoleRevoked, member extraction)
  * Malformed log entries are skipped (defensive decode)
  * The fetcher chunks ranges by ``max_block_range``
  * BlockHashFetcher decodes the canonical hash
  * Errors return ``None`` rather than raising"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution.repos.role_grants_rpc import (  # noqa: E402
    RpcBlockHashFetcher,
    RpcLogFetcher,
)
from workers.role_grants_indexer import (  # noqa: E402
    ROLE_GRANTED_TOPIC0,
    ROLE_REVOKED_TOPIC0,
)


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
) -> dict:
    """Build a raw eth_getLogs entry mirroring node output shape."""
    member_padded = "0x" + "00" * 12 + member[2:].zfill(40)
    return {
        "topics": [
            "0x" + topic0.hex(),
            "0x" + role.hex(),
            member_padded,
        ],
        "blockNumber": hex(block_number),
        "blockHash": "0x" + block_hash.hex(),
        "transactionHash": "0x" + tx_hash.hex(),
        "logIndex": hex(log_index),
        "transactionIndex": hex(transaction_index),
        "address": "0x" + "ab" * 20,
        "data": "0x",
    }


def test_decode_role_granted(monkeypatch):
    role = b"\x01" * 32
    member = "0x" + "11" * 20
    block_hash = b"\xaa" * 32
    tx_hash = b"\xbb" * 32

    def fake_rpc(_url, method, params, retries=1):
        assert method == "eth_getLogs"
        return [
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

    monkeypatch.setattr("services.resolution.repos.role_grants_rpc.rpc_request", fake_rpc)
    fetcher = RpcLogFetcher("http://node.invalid")
    logs = fetcher.fetch_logs(
        chain_id=1, contract_address="0x" + "ab" * 20, from_block=0, to_block=200
    )
    assert len(logs) == 1
    log = logs[0]
    assert log.direction == "grant"
    assert log.role == role
    assert log.member == member
    assert log.block_number == 100
    assert log.block_hash == block_hash
    assert log.tx_hash == tx_hash
    assert log.log_index == 3
    assert log.transaction_index == 2


def test_decode_role_revoked(monkeypatch):
    role = b"\x02" * 32
    member = "0x" + "22" * 20

    def fake_rpc(_url, method, params, retries=1):
        return [
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

    monkeypatch.setattr("services.resolution.repos.role_grants_rpc.rpc_request", fake_rpc)
    fetcher = RpcLogFetcher("http://node.invalid")
    logs = fetcher.fetch_logs(
        chain_id=1, contract_address="0x" + "ab" * 20, from_block=0, to_block=200
    )
    assert len(logs) == 1
    assert logs[0].direction == "revoke"


def test_malformed_logs_skipped(monkeypatch):
    """A bad-log entry shouldn't bring down the entire batch — the
    decoder defensively skips entries with missing topics, wrong
    lengths, or unknown topic0."""
    good_role = b"\x33" * 32
    good_member = "0x" + "33" * 20

    def fake_rpc(_url, method, params, retries=1):
        return [
            # Missing topics — skipped.
            {"topics": []},
            # Unknown topic0 (not RoleGranted/Revoked) — skipped.
            _make_log(
                topic0=b"\xff" * 32,
                role=good_role,
                member=good_member,
                block_number=50,
                block_hash=b"\x11" * 32,
                tx_hash=b"\x22" * 32,
                log_index=0,
            ),
            # Block-number missing — skipped.
            {
                "topics": [
                    "0x" + ROLE_GRANTED_TOPIC0.hex(),
                    "0x" + good_role.hex(),
                    "0x" + "00" * 12 + good_member[2:],
                ],
                "blockHash": "0x" + "00" * 32,
                "transactionHash": "0x" + "11" * 32,
                "logIndex": "0x0",
                "transactionIndex": "0x0",
            },
            # Good entry — kept.
            _make_log(
                topic0=ROLE_GRANTED_TOPIC0,
                role=good_role,
                member=good_member,
                block_number=100,
                block_hash=b"\x33" * 32,
                tx_hash=b"\x44" * 32,
                log_index=0,
            ),
        ]

    monkeypatch.setattr("services.resolution.repos.role_grants_rpc.rpc_request", fake_rpc)
    fetcher = RpcLogFetcher("http://node.invalid")
    logs = fetcher.fetch_logs(
        chain_id=1, contract_address="0x" + "ab" * 20, from_block=0, to_block=200
    )
    assert len(logs) == 1
    assert logs[0].member == good_member


def test_fetcher_chunks_large_range(monkeypatch):
    """A range exceeding ``max_block_range`` is split into multiple
    eth_getLogs calls. Verifies the from/to arithmetic matches the
    same boundary scheme as the existing watchers (no off-by-one
    overlap)."""
    calls: list[tuple[int, int]] = []

    def fake_rpc(_url, method, params, retries=1):
        flt = params[0]
        calls.append((int(flt["fromBlock"], 16), int(flt["toBlock"], 16)))
        return []

    monkeypatch.setattr("services.resolution.repos.role_grants_rpc.rpc_request", fake_rpc)
    fetcher = RpcLogFetcher("http://node.invalid", max_block_range=500)
    fetcher.fetch_logs(
        chain_id=1, contract_address="0x" + "ab" * 20, from_block=1, to_block=1500
    )
    assert calls == [(1, 500), (501, 1000), (1001, 1500)]


def test_block_hash_fetch_decodes(monkeypatch):
    block_hash = b"\xee" * 32

    def fake_rpc(_url, method, params, retries=1):
        assert method == "eth_getBlockByNumber"
        assert params == [hex(123), False]
        return {"hash": "0x" + block_hash.hex(), "number": hex(123)}

    monkeypatch.setattr("services.resolution.repos.role_grants_rpc.rpc_request", fake_rpc)
    fetcher = RpcBlockHashFetcher("http://node.invalid")
    assert fetcher.block_hash(chain_id=1, block_number=123) == block_hash


def test_block_hash_fetch_missing_returns_none(monkeypatch):
    """Pruned/unknown blocks return ``None`` rather than raising,
    so the indexer can decide whether to skip the reorg check."""

    def fake_rpc(_url, method, params, retries=1):
        return None  # node returned null for unknown block

    monkeypatch.setattr("services.resolution.repos.role_grants_rpc.rpc_request", fake_rpc)
    fetcher = RpcBlockHashFetcher("http://node.invalid")
    assert fetcher.block_hash(chain_id=1, block_number=123) is None


def test_block_hash_fetch_rpc_error_returns_none(monkeypatch):
    def fake_rpc(_url, method, params, retries=1):
        raise RuntimeError("rate limited")

    monkeypatch.setattr("services.resolution.repos.role_grants_rpc.rpc_request", fake_rpc)
    fetcher = RpcBlockHashFetcher("http://node.invalid")
    assert fetcher.block_hash(chain_id=1, block_number=123) is None


def test_fetcher_uses_role_topics_in_filter(monkeypatch):
    """The eth_getLogs filter must include both RoleGranted and
    RoleRevoked topic0 values as a topic-OR. Pinned so refactors
    don't accidentally narrow the filter."""
    captured: list[dict] = []

    def fake_rpc(_url, method, params, retries=1):
        captured.append(params[0])
        return []

    monkeypatch.setattr("services.resolution.repos.role_grants_rpc.rpc_request", fake_rpc)
    fetcher = RpcLogFetcher("http://node.invalid")
    fetcher.fetch_logs(
        chain_id=1, contract_address="0x" + "ab" * 20, from_block=0, to_block=10
    )
    assert captured
    topics = captured[0].get("topics") or []
    assert len(topics) == 1
    topic_or = topics[0]
    assert isinstance(topic_or, list)
    assert "0x" + ROLE_GRANTED_TOPIC0.hex() in topic_or
    assert "0x" + ROLE_REVOKED_TOPIC0.hex() in topic_or
