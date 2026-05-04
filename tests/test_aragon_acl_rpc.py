"""Tests for ``RpcAragonACLLogFetcher``.

Monkeypatches ``utils.rpc.rpc_request`` and exercises:

  * decoding SetPermission(allowed=true) -> grant
  * decoding SetPermission(allowed=false) -> revoke (allowed=False)
  * malformed log entries skipped (defensive decode)
  * the fetcher chunks ranges by ``max_block_range``
  * the request specifies the SET_PERMISSION_TOPIC0 filter
  * non-bool data region returns None (skipped)
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution.repos.aragon_acl_rpc import RpcAragonACLLogFetcher  # noqa: E402
from workers.aragon_acl_indexer import SET_PERMISSION_TOPIC0  # noqa: E402


def _addr_topic(addr: str) -> str:
    return "0x" + "00" * 12 + addr[2:].zfill(40)


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
) -> dict:
    data_word = bytes(31) + bytes([1 if allowed else 0])
    return {
        "topics": [
            "0x" + SET_PERMISSION_TOPIC0.hex(),
            _addr_topic(entity),
            _addr_topic(app),
            "0x" + role.hex(),
        ],
        "blockNumber": hex(block_number),
        "blockHash": "0x" + block_hash.hex(),
        "transactionHash": "0x" + tx_hash.hex(),
        "logIndex": hex(log_index),
        "transactionIndex": hex(transaction_index),
        "address": app,
        "data": "0x" + data_word.hex(),
    }


def test_decode_set_permission_grant(monkeypatch):
    role = b"\x01" * 32
    entity = "0x" + "11" * 20
    app = "0x" + "22" * 20

    def fake_rpc(_url, method, params, retries=1):
        assert method == "eth_getLogs"
        return [
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

    monkeypatch.setattr("services.resolution.repos.aragon_acl_rpc.rpc_request", fake_rpc)
    fetcher = RpcAragonACLLogFetcher("http://node.invalid")
    logs = fetcher.fetch_logs(chain_id=1, contract_address="0x" + "ee" * 20, from_block=0, to_block=200)
    assert len(logs) == 1
    log = logs[0]
    assert log.allowed is True
    assert log.entity == entity
    assert log.app == app
    assert log.role == role
    assert log.block_number == 100
    assert log.log_index == 3
    assert log.transaction_index == 2


def test_decode_set_permission_revoke(monkeypatch):
    """allowed=false in the data region -> FetchedAragonLog.allowed=False."""
    role = b"\x02" * 32

    def fake_rpc(_url, method, params, retries=1):
        return [
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

    monkeypatch.setattr("services.resolution.repos.aragon_acl_rpc.rpc_request", fake_rpc)
    fetcher = RpcAragonACLLogFetcher("http://node.invalid")
    logs = fetcher.fetch_logs(chain_id=1, contract_address="0x" + "ee" * 20, from_block=0, to_block=300)
    assert len(logs) == 1
    assert logs[0].allowed is False


def test_malformed_logs_skipped(monkeypatch):
    """A bad-log entry doesn't bring down the batch — only the
    well-formed entry survives."""
    role = b"\x55" * 32
    good = _make_log(
        entity="0x" + "55" * 20,
        app="0x" + "66" * 20,
        role=role,
        allowed=True,
        block_number=50,
        block_hash=b"\x11" * 32,
        tx_hash=b"\x22" * 32,
        log_index=0,
    )

    def fake_rpc(_url, method, params, retries=1):
        return [
            # Empty topics — skipped.
            {"topics": []},
            # Wrong topic0 — skipped.
            dict(good, topics=["0x" + "ff" * 32, *good["topics"][1:]]),
            # Garbled data region — skipped (neither 0 nor 1).
            dict(good, data="0x" + "ff" * 32),
            # Good — kept.
            good,
        ]

    monkeypatch.setattr("services.resolution.repos.aragon_acl_rpc.rpc_request", fake_rpc)
    fetcher = RpcAragonACLLogFetcher("http://node.invalid")
    logs = fetcher.fetch_logs(chain_id=1, contract_address="0x" + "ee" * 20, from_block=0, to_block=200)
    assert len(logs) == 1
    assert logs[0].block_number == 50


def test_fetcher_chunks_large_range(monkeypatch):
    calls: list[tuple[int, int]] = []

    def fake_rpc(_url, method, params, retries=1):
        flt = params[0]
        calls.append((int(flt["fromBlock"], 16), int(flt["toBlock"], 16)))
        return []

    monkeypatch.setattr("services.resolution.repos.aragon_acl_rpc.rpc_request", fake_rpc)
    fetcher = RpcAragonACLLogFetcher("http://node.invalid", max_block_range=500)
    fetcher.fetch_logs(chain_id=1, contract_address="0x" + "ee" * 20, from_block=1, to_block=1500)
    assert calls == [(1, 500), (501, 1000), (1001, 1500)]


def test_filter_uses_set_permission_topic0(monkeypatch):
    captured: list[dict] = []

    def fake_rpc(_url, method, params, retries=1):
        captured.append(params[0])
        return []

    monkeypatch.setattr("services.resolution.repos.aragon_acl_rpc.rpc_request", fake_rpc)
    fetcher = RpcAragonACLLogFetcher("http://node.invalid")
    fetcher.fetch_logs(chain_id=1, contract_address="0x" + "ee" * 20, from_block=0, to_block=10)
    assert captured
    assert captured[0]["topics"] == ["0x" + SET_PERMISSION_TOPIC0.hex()]
