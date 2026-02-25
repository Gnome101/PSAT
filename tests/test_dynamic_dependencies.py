import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services import dynamic_dependencies as ddc


# Verifies representative tx selection prioritizes unique function selectors and successful calls to target.
def test_pick_representative_transactions_prefers_selector_coverage():
    target = "0x1111111111111111111111111111111111111111"
    txs = [
        {
            "hash": "0xtx1",
            "to": target,
            "isError": "0",
            "blockNumber": "100",
            "input": "0xaaaaaaaa1234",
        },
        {
            "hash": "0xtx2",
            "to": target,
            "isError": "0",
            "blockNumber": "99",
            "input": "0xbbbbbbbb1234",
        },
        {
            "hash": "0xtx3",
            "to": target,
            "isError": "0",
            "blockNumber": "98",
            "input": "0xaaaaaaaa9999",
        },
        {
            "hash": "0xtx4",
            "to": "0x2222222222222222222222222222222222222222",
            "isError": "0",
            "blockNumber": "97",
            "input": "0xcccccccc",
        },
        {
            "hash": "0xtx5",
            "to": target,
            "isError": "1",
            "blockNumber": "96",
            "input": "0xdddddddd",
        },
    ]

    selected = ddc.pick_representative_transactions(target, txs, max_txs=3)
    assert [tx["tx_hash"] for tx in selected] == ["0xtx1", "0xtx2", "0xtx3"]
    assert [tx["method_selector"] for tx in selected] == ["0xaaaaaaaa", "0xbbbbbbbb", "0xaaaaaaaa"]


# Verifies debug callTracer parsing captures runtime call/delegatecall/create edges.
def test_extract_edges_from_debug_trace():
    trace = {
        "type": "CALL",
        "from": "0x1111111111111111111111111111111111111111",
        "to": "0x2222222222222222222222222222222222222222",
        "calls": [
            {
                "type": "DELEGATECALL",
                "from": "0x2222222222222222222222222222222222222222",
                "to": "0x3333333333333333333333333333333333333333",
            },
            {
                "type": "CREATE",
                "from": "0x2222222222222222222222222222222222222222",
                "to": "0x4444444444444444444444444444444444444444",
            },
        ],
    }

    edges = ddc.extract_edges_from_trace("debug_traceTransaction", trace, "0xtx", 123)
    assert {(e["op"], e["to"]) for e in edges} == {
        ("CALL", "0x2222222222222222222222222222222222222222"),
        ("DELEGATECALL", "0x3333333333333333333333333333333333333333"),
        ("CREATE", "0x4444444444444444444444444444444444444444"),
    }


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
    assert isinstance(result, list)
    assert calls == ["debug_traceTransaction", "debug_traceTransaction", "trace_transaction"]


# Verifies dynamic discovery aggregates dependencies and provenance across traced transactions.
def test_find_dynamic_dependencies_aggregates_graph(monkeypatch):
    target = "0x1111111111111111111111111111111111111111"
    tx1 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    tx2 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://trace.example")
    monkeypatch.setattr(
        ddc,
        "fetch_contract_transactions",
        lambda _address, limit=0: [
            {"hash": tx1, "to": target, "isError": "0", "blockNumber": "11", "input": "0xaaaaaaaa"},
            {"hash": tx2, "to": target, "isError": "0", "blockNumber": "12", "input": "0xbbbbbbbb"},
        ],
    )

    def fake_trace(_rpc_url, tx_hash):
        if tx_hash == tx1:
            return "debug_traceTransaction", {
                "type": "CALL",
                "from": target,
                "to": "0x2222222222222222222222222222222222222222",
                "calls": [
                    {
                        "type": "DELEGATECALL",
                        "from": "0x2222222222222222222222222222222222222222",
                        "to": "0x3333333333333333333333333333333333333333",
                    }
                ],
            }
        return "debug_traceTransaction", {
            "type": "CALL",
            "from": target,
            "to": "0x2222222222222222222222222222222222222222",
        }

    monkeypatch.setattr(ddc, "trace_transaction", fake_trace)

    out = ddc.find_dynamic_dependencies(target, tx_limit=2)
    assert out["address"] == target
    assert out["dependencies"] == [
        "0x2222222222222222222222222222222222222222",
        "0x3333333333333333333333333333333333333333",
    ]
    assert len(out["transactions_analyzed"]) == 2
    assert len(out["dependency_graph"]) == 2
    assert out["provenance"]["0x2222222222222222222222222222222222222222"]
    assert out["provenance"]["0x3333333333333333333333333333333333333333"]


def test_find_dynamic_dependencies_requires_trace_rpc(monkeypatch):
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.delenv("ETH_RPC", raising=False)

    with pytest.raises(RuntimeError, match="requires --dynamic-rpc or ETH_RPC"):
        ddc.find_dynamic_dependencies("0x1111111111111111111111111111111111111111")
