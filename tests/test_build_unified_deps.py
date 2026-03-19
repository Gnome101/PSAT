import importlib
import sys
import types


def _get_build_fn():
    """Import _build_unified_deps from main without triggering heavy deps."""
    if "requests" not in sys.modules:
        requests_stub = types.ModuleType("requests")
        requests_stub.get = lambda *a, **kw: None  # type: ignore[attr-defined]
        requests_stub.post = lambda *a, **kw: None  # type: ignore[attr-defined]
        sys.modules["requests"] = requests_stub
    if "dotenv" not in sys.modules:
        dotenv_stub = types.ModuleType("dotenv")
        dotenv_stub.load_dotenv = lambda *a, **kw: None  # type: ignore[attr-defined]
        sys.modules["dotenv"] = dotenv_stub
    mod = (
        importlib.import_module("main")
        if "main" not in sys.modules
        else sys.modules["main"]
    )
    return mod._build_unified_deps


TARGET = "0x1111111111111111111111111111111111111111"
DEP_A = "0x2222222222222222222222222222222222222222"
DEP_B = "0x3333333333333333333333333333333333333333"
DEP_C = "0x4444444444444444444444444444444444444444"
IMPL = "0x5555555555555555555555555555555555555555"


def _static(deps, network="ethereum"):
    return {
        "address": TARGET,
        "dependencies": deps,
        "rpc": "https://rpc.example",
        "network": network,
    }


def _dynamic(deps, provenance=None, graph=None):
    return {
        "address": TARGET,
        "rpc": "https://trace.example",
        "transactions_analyzed": [
            {"tx_hash": "0xaa", "block_number": 1, "method_selector": "0xdeadbeef"}
        ],
        "trace_methods": ["debug_traceTransaction"],
        "dependencies": deps,
        "provenance": provenance or {},
        "dependency_graph": graph or [],
        "trace_errors": [],
    }


def test_source_tracking():
    """Static-only, dynamic-only, and merged sources are all tracked correctly."""
    build = _get_build_fn()

    # Static only
    r = build(TARGET, _static([DEP_A]), None, None)
    assert r["dependencies"][DEP_A]["source"] == ["static"]
    assert r["network"] == "ethereum"
    assert "dependency_graph" not in r

    # Dynamic only (with provenance and graph)
    prov = {
        DEP_A: [{"tx_hash": "0xaa", "block_number": 1, "from": TARGET, "op": "CALL"}]
    }
    graph = [
        {
            "from": TARGET,
            "to": DEP_A,
            "op": "CALL",
            "provenance": [{"tx_hash": "0xaa", "block_number": 1}],
        }
    ]
    r = build(TARGET, None, _dynamic([DEP_A], provenance=prov, graph=graph), None)
    assert r["dependencies"][DEP_A]["source"] == ["dynamic"]
    assert r["dependencies"][DEP_A]["provenance"] == prov[DEP_A]
    assert r["dependency_graph"] == graph
    assert "network" not in r

    # Merged: DEP_A in both, DEP_B static-only, DEP_C dynamic-only
    r = build(TARGET, _static([DEP_A, DEP_B]), _dynamic([DEP_A, DEP_C]), None)
    assert r["dependencies"][DEP_A]["source"] == ["dynamic", "static"]
    assert r["dependencies"][DEP_B]["source"] == ["static"]
    assert r["dependencies"][DEP_C]["source"] == ["dynamic"]

    # Duplicates within a source are deduplicated
    r = build(TARGET, _static([DEP_A, DEP_A]), _dynamic([DEP_A]), None)
    assert r["dependencies"][DEP_A]["source"] == ["dynamic", "static"]


def test_classification_merging():
    """Classification types, target classification, and discovered addresses are merged."""
    build = _get_build_fn()
    cls = {
        "address": TARGET,
        "rpc": "https://rpc.example",
        "classifications": {
            TARGET: {
                "address": TARGET,
                "type": "proxy",
                "proxy_type": "eip1967",
                "implementation": IMPL,
            },
            DEP_A: {
                "address": DEP_A,
                "type": "proxy",
                "proxy_type": "eip1967",
                "implementation": IMPL,
            },
            IMPL: {"address": IMPL, "type": "implementation", "proxies": [DEP_A]},
        },
        "discovered_addresses": [IMPL],
    }
    r = build(TARGET, _static([DEP_A]), None, cls)

    # Dep classification
    assert r["dependencies"][DEP_A]["type"] == "proxy"
    assert r["dependencies"][DEP_A]["proxy_type"] == "eip1967"
    assert r["dependencies"][DEP_A]["implementation"] == IMPL

    # Discovered address included with source=["classification"]
    assert IMPL in r["dependencies"]
    assert r["dependencies"][IMPL]["type"] == "implementation"
    assert r["dependencies"][IMPL]["source"] == ["classification"]
    assert r["discovered_addresses"] == [IMPL]

    # Target classification included when non-regular
    assert r["target_classification"]["type"] == "proxy"
    assert r["target_classification"]["implementation"] == IMPL

    # Target classification omitted when regular
    cls["classifications"][TARGET]["type"] = "regular"
    r = build(TARGET, _static([DEP_A]), None, cls)
    assert "target_classification" not in r


def test_edge_cases():
    """Empty deps, no classification, and discovered-address deduplication."""
    build = _get_build_fn()

    # Empty deps
    r = build(TARGET, _static([]), None, None)
    assert r["dependencies"] == {}

    # No classification — all regular
    r = build(TARGET, _static([DEP_A, DEP_B]), None, None)
    assert all(d["type"] == "regular" for d in r["dependencies"].values())
    assert "discovered_addresses" not in r

    # Discovered address already in dep list — not duplicated, keeps original source
    cls = {
        "address": TARGET,
        "rpc": "https://rpc.example",
        "classifications": {
            TARGET: {"address": TARGET, "type": "regular"},
            DEP_A: {
                "address": DEP_A,
                "type": "proxy",
                "proxy_type": "eip1967",
                "implementation": DEP_B,
            },
            DEP_B: {"address": DEP_B, "type": "implementation", "proxies": [DEP_A]},
        },
        "discovered_addresses": [DEP_B],
    }
    r = build(TARGET, _static([DEP_A, DEP_B]), None, cls)
    assert r["dependencies"][DEP_B]["source"] == ["static"]
    assert r["dependencies"][DEP_B]["type"] == "implementation"
