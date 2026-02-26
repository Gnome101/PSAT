import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services import dynamic_dependencies as ddc

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


# Verifies representative tx selection prioritizes unique function selectors and successful calls to target.
def test_pick_representative_transactions_prefers_selector_coverage():
    target = "0x1111111111111111111111111111111111111111"
    txs = [
        {"hash": "0xtx1", "to": target, "isError": "0", "blockNumber": "100", "input": "0xaaaaaaaa1234"},
        {"hash": "0xtx2", "to": target, "isError": "0", "blockNumber": "99", "input": "0xbbbbbbbb1234"},
        {"hash": "0xtx3", "to": target, "isError": "0", "blockNumber": "98", "input": "0xaaaaaaaa9999"},
        {"hash": "0xtx4", "to": "0x2222222222222222222222222222222222222222", "isError": "0", "blockNumber": "97", "input": "0xcccccccc"},
        {"hash": "0xtx5", "to": target, "isError": "1", "blockNumber": "96", "input": "0xdddddddd"},
    ]

    selected = ddc.pick_representative_transactions(target, txs, max_txs=3)
    assert [tx["tx_hash"] for tx in selected] == ["0xtx1", "0xtx2", "0xtx3"]
    assert [tx["method_selector"] for tx in selected] == ["0xaaaaaaaa", "0xbbbbbbbb", "0xaaaaaaaa"]


# Verifies edge extraction handles all call op types (including CALLCODE) for both debug and parity trace formats.
def test_extract_edges_captures_all_op_types():
    addr = lambda n: f"0x{str(n) * 40}"

    # debug callTracer: nested CALL/DELEGATECALL/CREATE/CALLCODE
    debug_trace = {
        "type": "CALL",
        "from": addr(1), "to": addr(2),
        "calls": [
            {"type": "DELEGATECALL", "from": addr(2), "to": addr(3)},
            {"type": "CREATE",       "from": addr(2), "to": addr(4)},
            {"type": "CALLCODE",     "from": addr(2), "to": addr(5)},
        ],
    }
    debug_edges = ddc.extract_edges_from_trace("debug_traceTransaction", debug_trace, "0xtx", 1)
    assert {e["op"] for e in debug_edges} == {"CALL", "DELEGATECALL", "CREATE", "CALLCODE"}

    # parity style: all six types including STATICCALL and CREATE2
    parity_entries = [
        {"type": "call",   "action": {"from": addr(1), "to": addr(2), "callType": "call"}},
        {"type": "call",   "action": {"from": addr(1), "to": addr(3), "callType": "staticcall"}},
        {"type": "call",   "action": {"from": addr(1), "to": addr(4), "callType": "delegatecall"}},
        {"type": "call",   "action": {"from": addr(1), "to": addr(5), "callType": "callcode"}},
        {"type": "create", "action": {"from": addr(1), "creationMethod": "create"},  "result": {"address": addr(6)}},
        {"type": "create", "action": {"from": addr(1), "creationMethod": "create2"}, "result": {"address": addr(7)}},
    ]
    parity_edges = ddc.extract_edges_from_trace("trace_transaction", parity_entries, "0xtx", 1)
    assert {e["op"] for e in parity_edges} == {"CALL", "STATICCALL", "DELEGATECALL", "CALLCODE", "CREATE", "CREATE2"}


# Verifies tracer fallback behavior from debug_traceTransaction to trace_transaction.
def test_trace_transaction_falls_back_to_parity_style(monkeypatch):
    calls = []

    def fake_rpc_call(_rpc_url, method, _params, retries=0):
        calls.append(method)
        if method == "debug_traceTransaction":
            raise RuntimeError("method not found")
        return [{"type": "call", "action": {"from": "0x1", "to": "0x2", "callType": "call"}}]

    monkeypatch.setattr(ddc, "rpc_call", fake_rpc_call)

    method, result = ddc.trace_transaction("https://rpc.example", "0xtx")
    assert method == "trace_transaction"
    assert calls == ["debug_traceTransaction", "debug_traceTransaction", "trace_transaction"]


# Verifies dynamic discovery aggregates dependencies and provenance across traced transactions.
def test_find_dynamic_dependencies_aggregates_graph(monkeypatch):
    target = "0x1111111111111111111111111111111111111111"
    tx1 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    tx2 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://trace.example")
    monkeypatch.setattr(
        ddc, "fetch_contract_transactions",
        lambda _address, limit=0: [
            {"hash": tx1, "to": target, "isError": "0", "blockNumber": "11", "input": "0xaaaaaaaa"},
            {"hash": tx2, "to": target, "isError": "0", "blockNumber": "12", "input": "0xbbbbbbbb"},
        ],
    )

    def fake_trace(_rpc_url, tx_hash):
        if tx_hash == tx1:
            return "debug_traceTransaction", {
                "type": "CALL", "from": target, "to": "0x2222222222222222222222222222222222222222",
                "calls": [{"type": "DELEGATECALL", "from": "0x2222222222222222222222222222222222222222", "to": "0x3333333333333333333333333333333333333333"}],
            }
        return "debug_traceTransaction", {"type": "CALL", "from": target, "to": "0x2222222222222222222222222222222222222222"}

    monkeypatch.setattr(ddc, "trace_transaction", fake_trace)

    out = ddc.find_dynamic_dependencies(target, tx_limit=2)
    assert out["address"] == target
    assert out["dependencies"] == [
        "0x2222222222222222222222222222222222222222",
        "0x3333333333333333333333333333333333333333",
    ]
    assert len(out["transactions_analyzed"]) == 2
    assert len(out["dependency_graph"]) == 2
    assert isinstance(out["trace_errors"], list) and out["trace_errors"] == []


# Verifies a single failed trace is recorded in trace_errors while remaining transactions still produce results.
def test_find_dynamic_dependencies_continues_on_single_trace_failure(monkeypatch):
    target = "0x1111111111111111111111111111111111111111"
    tx1 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    tx2 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://trace.example")
    monkeypatch.setattr(
        ddc, "fetch_contract_transactions",
        lambda _address, limit=0: [
            {"hash": tx1, "to": target, "isError": "0", "blockNumber": "10", "input": "0xaaaaaaaa"},
            {"hash": tx2, "to": target, "isError": "0", "blockNumber": "11", "input": "0xbbbbbbbb"},
        ],
    )

    def fake_trace(_rpc_url, tx_hash):
        if tx_hash == tx1:
            raise RuntimeError("RPC error on tx1")
        return "debug_traceTransaction", {"type": "CALL", "from": target, "to": "0x2222222222222222222222222222222222222222"}

    monkeypatch.setattr(ddc, "trace_transaction", fake_trace)

    out = ddc.find_dynamic_dependencies(target, tx_limit=2)
    assert "0x2222222222222222222222222222222222222222" in out["dependencies"]
    assert len(out["trace_errors"]) == 1
    assert out["trace_errors"][0]["tx_hash"] == tx1


# Verifies all-trace-failure raises RuntimeError so callers know no results were produced.
def test_find_dynamic_dependencies_raises_if_all_traces_fail(monkeypatch):
    target = "0x1111111111111111111111111111111111111111"
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://trace.example")
    monkeypatch.setattr(
        ddc, "fetch_contract_transactions",
        lambda _address, limit=0: [
            {"hash": "0xtx1", "to": target, "isError": "0", "blockNumber": "10", "input": "0xaaaaaaaa"},
        ],
    )
    monkeypatch.setattr(ddc, "trace_transaction", lambda _rpc, _tx: (_ for _ in ()).throw(RuntimeError("all fail")))

    with pytest.raises(RuntimeError, match="All"):
        ddc.find_dynamic_dependencies(target, tx_limit=1)


# Verifies explicit tx_hashes skips Etherscan fetch and traces only the provided hashes.
def test_find_dynamic_dependencies_with_explicit_tx_hashes(monkeypatch):
    target = "0x1111111111111111111111111111111111111111"
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://trace.example")

    fetch_called = []
    monkeypatch.setattr(ddc, "fetch_contract_transactions", lambda *a, **kw: fetch_called.append(1) or [])
    monkeypatch.setattr(
        ddc, "_fetch_tx_metadata_from_rpc",
        lambda _rpc, tx_hash: {"tx_hash": tx_hash, "block_number": 1, "method_selector": "0xdeadbeef"},
    )
    monkeypatch.setattr(
        ddc, "trace_transaction",
        lambda _rpc, _tx: ("debug_traceTransaction", {"type": "CALL", "from": target, "to": "0x2222222222222222222222222222222222222222"}),
    )

    out = ddc.find_dynamic_dependencies(target, tx_hashes=["0xtxhash"])
    assert fetch_called == [], "fetch_contract_transactions should not be called when tx_hashes provided"
    assert out["transactions_analyzed"][0]["tx_hash"] == "0xtxhash"


# Verifies end-to-end dynamic dependency discovery against a live tracing RPC.
def test_live_dynamic_dependencies():
    rpc_url = os.environ.get("ETH_RPC")
    if not rpc_url:
        pytest.skip("Set ETH_RPC before running this test.")

    # Skip if RPC is unreachable or doesn't support tracing
    try:
        from services.dependent_contracts import rpc_call
        rpc_call(rpc_url, "eth_blockNumber", [], retries=0)
    except Exception as exc:
        pytest.skip(f"RPC unreachable: {exc}")

    # 1inch V5 router — same contract used in test_find_dependent_contracts.py
    address = "0x1111111254eeb25477b68fb85ed929f73a960582"
    result = ddc.find_dynamic_dependencies(address, rpc_url=rpc_url, tx_limit=2)

    assert isinstance(result["dependencies"], list) and result["dependencies"]
    assert result["transactions_analyzed"]
    assert result["dependency_graph"]
    assert "trace_errors" in result and isinstance(result["trace_errors"], list)
