"""Regression tests for the eth_blockNumber TTL cache in utils/rpc.py.

Block height is finalized state; a few seconds of staleness is harmless for the
log-range upper bounds, snapshot block tagging, and binary-search-by-block
sites that read it. We pin:

1. Same rpc_url within TTL → one underlying RPC call, cache served on repeats.
2. RPC errors are NOT cached (transient failures shouldn't cement a stale value).
3. Different rpc_urls don't share the cache slot (cross-chain isolation).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest  # noqa: E402

from utils import rpc  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_cache():
    rpc.clear_block_number_cache()
    yield
    rpc.clear_block_number_cache()


def test_cache_serves_repeat_calls_within_ttl(monkeypatch):
    calls: list[tuple] = []

    def fake_rpc_request(rpc_url, method, params, retries=1):
        calls.append((rpc_url, method))
        return "0x" + format(12345678, "x")

    monkeypatch.setattr(rpc, "rpc_request", fake_rpc_request)

    a = rpc.current_block_number("https://rpc.example")
    b = rpc.current_block_number("https://rpc.example")
    c = rpc.current_block_number("https://rpc.example")

    assert a == b == c == 12345678
    # Only the first call hits the wire — subsequent calls reuse the cache.
    assert len(calls) == 1


def test_rpc_errors_are_not_cached(monkeypatch):
    state = {"raise_next": True}

    def fake_rpc_request(rpc_url, method, params, retries=1):
        if state["raise_next"]:
            state["raise_next"] = False
            raise RuntimeError("transient")
        return "0x" + format(99, "x")

    monkeypatch.setattr(rpc, "rpc_request", fake_rpc_request)

    with pytest.raises(RuntimeError):
        rpc.current_block_number("https://rpc.example")
    # Second call must re-fetch since the first errored — caching the error
    # would cement a wrong value indefinitely.
    assert rpc.current_block_number("https://rpc.example") == 99


def test_different_rpc_urls_do_not_share_cache(monkeypatch):
    block_for: dict[str, int] = {
        "https://rpc.eth": 100,
        "https://rpc.poly": 200,
    }
    calls: list[str] = []

    def fake_rpc_request(rpc_url, method, params, retries=1):
        calls.append(rpc_url)
        return "0x" + format(block_for[rpc_url], "x")

    monkeypatch.setattr(rpc, "rpc_request", fake_rpc_request)

    assert rpc.current_block_number("https://rpc.eth") == 100
    assert rpc.current_block_number("https://rpc.poly") == 200
    assert rpc.current_block_number("https://rpc.eth") == 100
    assert rpc.current_block_number("https://rpc.poly") == 200

    # Two cold misses (one per chain), two warm hits — no cross-chain pollution.
    assert calls == ["https://rpc.eth", "https://rpc.poly"]


def test_unexpected_response_shape_raises(monkeypatch):
    monkeypatch.setattr(rpc, "rpc_request", lambda *_a, **_kw: None)
    with pytest.raises(RuntimeError):
        rpc.current_block_number("https://rpc.example")


def test_ttl_expiry_re_fetches(monkeypatch):
    fake_clock = {"now": 1000.0}
    monkeypatch.setattr(rpc.time, "monotonic", lambda: fake_clock["now"])

    calls: list[str] = []

    def fake_rpc_request(rpc_url, method, params, retries=1):
        calls.append(rpc_url)
        return "0x" + format(7, "x")

    monkeypatch.setattr(rpc, "rpc_request", fake_rpc_request)

    rpc.current_block_number("https://rpc.example")
    rpc.current_block_number("https://rpc.example")  # cache hit
    assert len(calls) == 1

    # Advance past the TTL → next call must re-fetch.
    fake_clock["now"] += rpc._BLOCK_NUMBER_CACHE_TTL_S + 1
    rpc.current_block_number("https://rpc.example")
    assert len(calls) == 2
