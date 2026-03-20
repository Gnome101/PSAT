import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services import dynamic_dependencies as ddc

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


# ---------------------------------------------------------------------------
# Core helpers: _normalize_maybe_address, _to_int, _tx_selector, _is_precompile
# ---------------------------------------------------------------------------


def test_is_precompile():
    assert ddc._is_precompile("0x0000000000000000000000000000000000000001") is True
    assert ddc._is_precompile("0x0000000000000000000000000000000000000009") is True
    assert ddc._is_precompile("0x000000000000000000000000000000000000000a") is False
    assert ddc._is_precompile("0x" + "11" * 20) is False


def test_parsing_helpers():
    # _normalize_maybe_address: valid addresses
    assert ddc._normalize_maybe_address("0xAbCd" + "0" * 36) == "0xabcd" + "0" * 36
    assert ddc._normalize_maybe_address("abcd" + "0" * 36) == "0xabcd" + "0" * 36

    # _normalize_maybe_address: invalid inputs
    assert ddc._normalize_maybe_address(None) is None
    assert ddc._normalize_maybe_address(123) is None
    assert ddc._normalize_maybe_address("0xshort") is None
    assert ddc._normalize_maybe_address("0xZZZZ" + "0" * 36) is None  # invalid hex

    # _to_int: various formats
    assert ddc._to_int("0xa") == 10
    assert ddc._to_int("100") == 100
    assert ddc._to_int(42) == 42
    assert ddc._to_int(None) is None
    with pytest.raises(RuntimeError, match="Unsupported"):
        ddc._to_int([1, 2])

    # _tx_selector: extracts first 4 bytes
    assert ddc._tx_selector("0xdeadbeef1234") == "0xdeadbeef"
    assert ddc._tx_selector("0x12345678") == "0x12345678"
    assert ddc._tx_selector("0x1234") == "0x"  # too short
    assert ddc._tx_selector("") == "0x"
    assert ddc._tx_selector(None) == "0x"


# ---------------------------------------------------------------------------
# Transaction selection
# ---------------------------------------------------------------------------


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
    assert [tx["method_selector"] for tx in selected] == [
        "0xaaaaaaaa",
        "0xbbbbbbbb",
        "0xaaaaaaaa",
    ]


# Verifies edge extraction handles all call op types (including CALLCODE) for both debug and parity trace formats.
def test_extract_edges_captures_all_op_types():
    def addr(n: int) -> str:
        return "0x" + hex(n)[2:].zfill(40)

    # debug callTracer: nested CALL/DELEGATECALL/CREATE/CALLCODE
    debug_trace = {
        "type": "CALL",
        "from": addr(1),
        "to": addr(2),
        "calls": [
            {"type": "DELEGATECALL", "from": addr(2), "to": addr(3)},
            {"type": "CREATE", "from": addr(2), "to": addr(4)},
            {"type": "CALLCODE", "from": addr(2), "to": addr(5)},
        ],
    }
    debug_edges = ddc.extract_edges_from_trace("debug_traceTransaction", debug_trace, "0xtx", 1)
    assert {e["op"] for e in debug_edges} == {
        "CALL",
        "DELEGATECALL",
        "CREATE",
        "CALLCODE",
    }

    # parity style: all six types including STATICCALL and CREATE2
    parity_entries = [
        {
            "type": "call",
            "action": {"from": addr(1), "to": addr(2), "callType": "call"},
        },
        {
            "type": "call",
            "action": {"from": addr(1), "to": addr(3), "callType": "staticcall"},
        },
        {
            "type": "call",
            "action": {"from": addr(1), "to": addr(4), "callType": "delegatecall"},
        },
        {
            "type": "call",
            "action": {"from": addr(1), "to": addr(5), "callType": "callcode"},
        },
        {
            "type": "create",
            "action": {"from": addr(1), "creationMethod": "create"},
            "result": {"address": addr(6)},
        },
        {
            "type": "create",
            "action": {"from": addr(1), "creationMethod": "create2"},
            "result": {"address": addr(7)},
        },
    ]
    parity_edges = ddc.extract_edges_from_trace("trace_transaction", parity_entries, "0xtx", 1)
    assert {e["op"] for e in parity_edges} == {
        "CALL",
        "STATICCALL",
        "DELEGATECALL",
        "CALLCODE",
        "CREATE",
        "CREATE2",
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
    assert calls == [
        "debug_traceTransaction",
        "debug_traceTransaction",
        "trace_transaction",
    ]


# Shared mock: all traced addresses are contracts
def _mock_code_checks(monkeypatch):
    monkeypatch.setattr(ddc, "get_code", lambda _rpc, _addr: "0x6000")
    monkeypatch.setattr(ddc, "has_deployed_code", lambda code: code not in ("0x", "0x0"))


# Verifies dynamic discovery aggregates dependencies and provenance across traced transactions.
def test_find_dynamic_dependencies_aggregates_graph(monkeypatch):
    target = "0x1111111111111111111111111111111111111111"
    tx1 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    tx2 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    _mock_code_checks(monkeypatch)
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://trace.example")
    monkeypatch.setattr(
        ddc,
        "fetch_contract_transactions",
        lambda _address, limit=0: [
            {
                "hash": tx1,
                "to": target,
                "isError": "0",
                "blockNumber": "11",
                "input": "0xaaaaaaaa",
            },
            {
                "hash": tx2,
                "to": target,
                "isError": "0",
                "blockNumber": "12",
                "input": "0xbbbbbbbb",
            },
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
    assert isinstance(out["trace_errors"], list) and out["trace_errors"] == []


# Verifies precompiles and EOAs are filtered from dynamic dependencies.
def test_find_dynamic_dependencies_filters_precompiles_and_eoas(monkeypatch):
    target = "0x1111111111111111111111111111111111111111"
    contract = "0x2222222222222222222222222222222222222222"
    precompile = "0x0000000000000000000000000000000000000001"
    eoa = "0x3333333333333333333333333333333333333333"
    tx1 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://trace.example")
    monkeypatch.setattr(ddc, "get_code", lambda _rpc, addr: "0x6000" if addr == contract else "0x")
    monkeypatch.setattr(ddc, "has_deployed_code", lambda code: code not in ("0x", "0x0"))
    monkeypatch.setattr(
        ddc,
        "fetch_contract_transactions",
        lambda _addr, limit=0: [
            {"hash": tx1, "to": target, "isError": "0", "blockNumber": "10", "input": "0xaa"},
        ],
    )
    monkeypatch.setattr(
        ddc,
        "trace_transaction",
        lambda _rpc, _tx: (
            "debug_traceTransaction",
            {
                "type": "CALL",
                "from": target,
                "to": contract,
                "calls": [
                    {"type": "STATICCALL", "from": contract, "to": precompile},
                    {"type": "CALL", "from": contract, "to": eoa},
                ],
            },
        ),
    )

    out = ddc.find_dynamic_dependencies(target, tx_limit=1)
    assert out["dependencies"] == [contract]
    assert all(e["to"] == contract for e in out["dependency_graph"])


# Verifies a single failed trace is recorded in trace_errors while remaining transactions still produce results.
def test_find_dynamic_dependencies_continues_on_single_trace_failure(monkeypatch):
    target = "0x1111111111111111111111111111111111111111"
    tx1 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    tx2 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    _mock_code_checks(monkeypatch)
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://trace.example")
    monkeypatch.setattr(
        ddc,
        "fetch_contract_transactions",
        lambda _address, limit=0: [
            {
                "hash": tx1,
                "to": target,
                "isError": "0",
                "blockNumber": "10",
                "input": "0xaaaaaaaa",
            },
            {
                "hash": tx2,
                "to": target,
                "isError": "0",
                "blockNumber": "11",
                "input": "0xbbbbbbbb",
            },
        ],
    )

    def fake_trace(_rpc_url, tx_hash):
        if tx_hash == tx1:
            raise RuntimeError("RPC error on tx1")
        return "debug_traceTransaction", {
            "type": "CALL",
            "from": target,
            "to": "0x2222222222222222222222222222222222222222",
        }

    monkeypatch.setattr(ddc, "trace_transaction", fake_trace)

    out = ddc.find_dynamic_dependencies(target, tx_limit=2)
    assert "0x2222222222222222222222222222222222222222" in out["dependencies"]
    assert len(out["trace_errors"]) == 1
    assert out["trace_errors"][0]["tx_hash"] == tx1


# Verifies all-trace-failure raises RuntimeError so callers know no results were produced.
def test_find_dynamic_dependencies_raises_if_all_traces_fail(monkeypatch):
    target = "0x1111111111111111111111111111111111111111"
    _mock_code_checks(monkeypatch)
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://trace.example")
    monkeypatch.setattr(
        ddc,
        "fetch_contract_transactions",
        lambda _address, limit=0: [
            {
                "hash": "0xtx1",
                "to": target,
                "isError": "0",
                "blockNumber": "10",
                "input": "0xaaaaaaaa",
            },
        ],
    )
    monkeypatch.setattr(
        ddc,
        "trace_transaction",
        lambda _rpc, _tx: (_ for _ in ()).throw(RuntimeError("all fail")),
    )

    with pytest.raises(RuntimeError, match="All"):
        ddc.find_dynamic_dependencies(target, tx_limit=1)


# Verifies explicit tx_hashes skips Etherscan fetch and traces only the provided hashes.
def test_find_dynamic_dependencies_with_explicit_tx_hashes(monkeypatch):
    target = "0x1111111111111111111111111111111111111111"
    _mock_code_checks(monkeypatch)
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://trace.example")

    fetch_called = []
    monkeypatch.setattr(
        ddc,
        "fetch_contract_transactions",
        lambda *a, **kw: fetch_called.append(1) or [],
    )
    monkeypatch.setattr(
        ddc,
        "_fetch_tx_metadata_from_rpc",
        lambda _rpc, tx_hash: {
            "tx_hash": tx_hash,
            "block_number": 1,
            "method_selector": "0xdeadbeef",
        },
    )
    monkeypatch.setattr(
        ddc,
        "trace_transaction",
        lambda _rpc, _tx: (
            "debug_traceTransaction",
            {
                "type": "CALL",
                "from": target,
                "to": "0x2222222222222222222222222222222222222222",
            },
        ),
    )

    out = ddc.find_dynamic_dependencies(target, tx_hashes=["0xtxhash"])
    assert fetch_called == [], "fetch_contract_transactions should not be called when tx_hashes provided"
    assert out["transactions_analyzed"][0]["tx_hash"] == "0xtxhash"


# ---------------------------------------------------------------------------
# resolve_trace_rpc
# ---------------------------------------------------------------------------


def test_resolve_trace_rpc_raises_without_rpc(monkeypatch):
    """resolve_trace_rpc raises RuntimeError when no RPC is available."""
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.delenv("ETH_RPC", raising=False)
    with pytest.raises(RuntimeError, match="requires --dynamic-rpc or ETH_RPC"):
        ddc.resolve_trace_rpc()


def test_resolve_trace_rpc_prefers_arg(monkeypatch):
    """resolve_trace_rpc returns the explicit argument over ETH_RPC."""
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://env.example")
    assert ddc.resolve_trace_rpc("https://arg.example") == "https://arg.example"


def test_resolve_trace_rpc_falls_back_to_env(monkeypatch):
    """resolve_trace_rpc falls back to ETH_RPC when no argument is given."""
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://env.example")
    assert ddc.resolve_trace_rpc() == "https://env.example"


# ---------------------------------------------------------------------------
# _build_graph
# ---------------------------------------------------------------------------


def test_build_graph_deduplicates_and_sorts_none_blocks():
    """_build_graph deduplicates provenance and handles None block_number."""
    a = "0x" + "aa" * 20
    b = "0x" + "bb" * 20
    edges = [
        {"from": a, "to": b, "op": "CALL", "tx_hash": "0xtx1", "block_number": None},
        {"from": a, "to": b, "op": "CALL", "tx_hash": "0xtx1", "block_number": None},
        {"from": a, "to": b, "op": "CALL", "tx_hash": "0xtx2", "block_number": 100},
    ]
    graph = ddc._build_graph(edges)
    assert len(graph) == 1
    prov = graph[0]["provenance"]
    assert len(prov) == 2  # deduplicated
    assert prov[0]["tx_hash"] == "0xtx1"
    assert prov[0]["block_number"] is None
    assert prov[1]["tx_hash"] == "0xtx2"
    assert prov[1]["block_number"] == 100


# ---------------------------------------------------------------------------
# fetch_contract_transactions dual fetch
# ---------------------------------------------------------------------------


def test_fetch_contract_transactions_merges_normal_and_internal(monkeypatch):
    """Both txlist and txlistinternal results are merged; failure of one doesn't block the other."""
    calls = []

    def fake_etherscan_get(_module, action, **_kw):
        calls.append(action)
        if action == "txlist":
            raise RuntimeError("No transactions found")
        return {"result": [{"hash": "0xaa", "to": "0x1", "isError": "0"}]}

    monkeypatch.setattr(ddc, "etherscan_get", fake_etherscan_get)
    txs = ddc.fetch_contract_transactions("0x1")
    assert calls == ["txlist", "txlistinternal"]
    assert len(txs) == 1
    assert txs[0]["hash"] == "0xaa"


# ---------------------------------------------------------------------------
# _fetch_tx_metadata_from_rpc error path
# ---------------------------------------------------------------------------


def test_fetch_tx_metadata_invalid_response(monkeypatch):
    """_fetch_tx_metadata_from_rpc raises on non-dict or missing hash."""
    monkeypatch.setattr(ddc, "rpc_call", lambda *a, **kw: "0x")
    with pytest.raises(RuntimeError, match="Could not fetch"):
        ddc._fetch_tx_metadata_from_rpc("https://rpc.example", "0xbad")


# ---------------------------------------------------------------------------
# find_dynamic_dependencies validation
# ---------------------------------------------------------------------------


def test_find_dynamic_dependencies_rejects_invalid_tx_limit(monkeypatch):
    monkeypatch.setattr(ddc, "load_dotenv", lambda _path: None)
    monkeypatch.setenv("ETH_RPC", "https://rpc.example")
    with pytest.raises(RuntimeError, match="tx_limit must be >= 1"):
        ddc.find_dynamic_dependencies("0x" + "11" * 20, tx_limit=0)


# ---------------------------------------------------------------------------
# Live RPC
# ---------------------------------------------------------------------------


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
