"""Tests for services.discovery.dependency_graph_builder."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.discovery.dependency_graph_builder import (
    build_dependency_visualization,
    write_dependency_visualization,
)

TARGET = "0x1111111111111111111111111111111111111111"
DEP_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
DEP_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
IMPL = "0xcccccccccccccccccccccccccccccccccccccccc"


def _unified(
    deps=None,
    graph=None,
    target_cls=None,
    network="ethereum",
):
    out = {"address": TARGET, "dependencies": deps or {}}
    if graph is not None:
        out["dependency_graph"] = graph
        out["transactions_analyzed"] = []
        out["trace_methods"] = ["debug_traceTransaction"]
        out["trace_errors"] = []
    if target_cls:
        out["target_classification"] = target_cls
    if network:
        out["network"] = network
    return out


def _write(tmp_path, unified):
    (tmp_path / "dependencies.json").write_text(json.dumps(unified))
    return tmp_path


def test_basic_nodes_and_static_ref_edges(tmp_path):
    """Deps without dynamic edges get STATIC_REF edges from target."""
    deps = {
        DEP_A: {"type": "regular", "source": ["static"]},
        DEP_B: {"type": "regular", "source": ["static"]},
    }
    result = build_dependency_visualization(_write(tmp_path, _unified(deps=deps)))

    assert len(result["nodes"]) == 3  # target + 2 deps
    target_node = next(n for n in result["nodes"] if n["is_target"])
    assert target_node["address"] == TARGET

    assert len(result["edges"]) == 2
    for edge in result["edges"]:
        assert edge["from"] == f"addr:{TARGET}"
        assert edge["op"] == "STATIC_REF"


def test_dynamic_edges_suppress_static_ref(tmp_path):
    """Deps already in the dependency_graph don't get a STATIC_REF edge."""
    deps = {DEP_A: {"type": "regular", "source": ["dynamic"]}}
    graph = {
        f"{TARGET}|{DEP_A}": [
            {"op": "CALL", "provenance": [{"tx_hash": "0xaa", "block_number": 1}]},
        ],
    }
    result = build_dependency_visualization(_write(tmp_path, _unified(deps=deps, graph=graph)))

    assert len(result["edges"]) == 1
    assert result["edges"][0]["op"] == "CALL"


def test_proxy_delegates_to_edge(tmp_path):
    """Proxy deps with a nested implementation get a DELEGATES_TO edge."""
    deps = {
        DEP_A: {
            "type": "proxy",
            "proxy_type": "eip1967",
            "source": ["dynamic"],
            "implementation": {
                "address": IMPL,
                "type": "implementation",
                "source": ["classification"],
            },
        },
    }
    graph = {f"{TARGET}|{DEP_A}": [{"op": "CALL", "provenance": []}]}
    result = build_dependency_visualization(_write(tmp_path, _unified(deps=deps, graph=graph)))

    # 3 nodes: target + proxy + nested implementation
    assert len(result["nodes"]) == 3

    ops = {(e["from"], e["to"], e["op"]) for e in result["edges"]}
    assert (f"addr:{DEP_A}", f"addr:{IMPL}", "DELEGATES_TO") in ops
    assert (f"addr:{TARGET}", f"addr:{DEP_A}", "CALL") in ops
    # IMPL is classification-only (nested) — no STATIC_REF
    assert not any(e["op"] == "STATIC_REF" and e["to"] == f"addr:{IMPL}" for e in result["edges"])


def test_classification_only_no_static_ref(tmp_path):
    """Deps with source=['classification'] and no dynamic edge get no STATIC_REF."""
    deps = {
        DEP_A: {
            "type": "proxy",
            "source": ["dynamic"],
            "implementation": {
                "address": IMPL,
                "type": "implementation",
                "source": ["classification"],
            },
        },
    }
    graph = {f"{TARGET}|{DEP_A}": [{"op": "CALL", "provenance": []}]}
    result = build_dependency_visualization(_write(tmp_path, _unified(deps=deps, graph=graph)))

    impl_edges = [e for e in result["edges"] if e["to"] == f"addr:{IMPL}"]
    assert len(impl_edges) == 1
    assert impl_edges[0]["op"] == "DELEGATES_TO"


def test_contract_name_used_as_label(tmp_path):
    """When contract_name is present it becomes the node label."""
    deps = {DEP_A: {"type": "regular", "source": ["dynamic"], "contract_name": "WETH9"}}
    graph = {f"{TARGET}|{DEP_A}": [{"op": "CALL", "provenance": []}]}
    result = build_dependency_visualization(_write(tmp_path, _unified(deps=deps, graph=graph)))

    dep_node = next(n for n in result["nodes"] if n["address"] == DEP_A)
    assert dep_node["label"] == "WETH9"


def test_contract_name_fallback_to_short_address(tmp_path):
    """Without contract_name the label falls back to shortened address."""
    deps = {DEP_A: {"type": "regular", "source": ["static"]}}
    result = build_dependency_visualization(_write(tmp_path, _unified(deps=deps)))

    dep_node = next(n for n in result["nodes"] if n["address"] == DEP_A)
    assert dep_node["label"] == f"{DEP_A[:6]}...{DEP_A[-4:]}"


def test_target_classification_in_node(tmp_path):
    """Target classification type is reflected in the target node."""
    deps = {DEP_A: {"type": "regular", "source": ["static"]}}
    target_cls = {"type": "proxy", "proxy_type": "eip1967"}
    result = build_dependency_visualization(_write(tmp_path, _unified(deps=deps, target_cls=target_cls)))

    target_node = next(n for n in result["nodes"] if n["is_target"])
    assert target_node["type"] == "proxy"


def test_metadata_populated(tmp_path):
    """Metadata includes network, transactions, trace info, and derived discovered addresses."""
    deps = {
        DEP_A: {
            "type": "proxy",
            "source": ["dynamic"],
            "implementation": {
                "address": IMPL,
                "type": "implementation",
                "source": ["classification"],
                "contract_name": "ImplV2",
            },
        },
    }
    graph = {f"{TARGET}|{DEP_A}": [{"op": "CALL", "provenance": []}]}
    result = build_dependency_visualization(_write(tmp_path, _unified(deps=deps, graph=graph)))

    assert result["metadata"]["target"] == TARGET
    assert result["metadata"]["network"] == "ethereum"
    assert result["metadata"]["trace_methods"] == ["debug_traceTransaction"]
    # discovered_addresses derived from classification sources
    assert result["metadata"]["discovered_addresses"] == [IMPL]


def test_empty_dependencies(tmp_path):
    """No dependencies produces empty graph."""
    result = build_dependency_visualization(_write(tmp_path, _unified()))
    assert result["nodes"] == []
    assert result["edges"] == []


def test_missing_file_returns_empty():
    """Missing dependencies.json produces empty graph."""
    result = build_dependency_visualization(Path("/tmp/nonexistent_dir_12345"))
    assert result["nodes"] == []


def test_write_creates_file(tmp_path):
    """write_dependency_visualization writes a valid JSON file."""
    deps = {DEP_A: {"type": "regular", "source": ["static"]}}
    _write(tmp_path, _unified(deps=deps))

    out = write_dependency_visualization(tmp_path)
    assert out is not None
    assert out.name == "dependency_graph_viz.json"

    data = json.loads(out.read_text())
    assert len(data["nodes"]) == 2
    assert len(data["edges"]) == 1


def test_write_returns_none_for_empty(tmp_path):
    """write_dependency_visualization returns None when there's no data."""
    _write(tmp_path, _unified())

    out = write_dependency_visualization(tmp_path)
    assert out is None


def test_beacon_edge(tmp_path):
    """Proxy deps with a beacon get a BEACON edge."""
    beacon = "0xdddddddddddddddddddddddddddddddddddddd"
    deps = {
        DEP_A: {
            "type": "proxy",
            "proxy_type": "beacon_proxy",
            "source": ["dynamic"],
            "beacon": beacon,
        },
        beacon: {"type": "beacon", "source": ["classification"]},
    }
    graph = {f"{TARGET}|{DEP_A}": [{"op": "CALL", "provenance": []}]}
    result = build_dependency_visualization(_write(tmp_path, _unified(deps=deps, graph=graph)))

    ops = {(e["from"], e["to"], e["op"]) for e in result["edges"]}
    assert (f"addr:{DEP_A}", f"addr:{beacon}", "BEACON") in ops


def test_edges_skip_unknown_node_ids(tmp_path):
    """Edges referencing addresses not in nodes are dropped."""
    unknown = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    deps = {DEP_A: {"type": "regular", "source": ["dynamic"]}}
    graph = {
        f"{TARGET}|{DEP_A}": [{"op": "CALL", "provenance": []}],
        f"{TARGET}|{unknown}": [{"op": "CALL", "provenance": []}],
    }
    result = build_dependency_visualization(_write(tmp_path, _unified(deps=deps, graph=graph)))

    assert all(e["to"] != f"addr:{unknown}" for e in result["edges"])


def test_nested_implementation_label(tmp_path):
    """Nested implementation contract_name is used as its node label."""
    deps = {
        DEP_A: {
            "type": "proxy",
            "proxy_type": "eip1967",
            "source": ["dynamic"],
            "contract_name": "TransparentProxy",
            "implementation": {
                "address": IMPL,
                "type": "implementation",
                "source": ["classification"],
                "contract_name": "TokenV2",
            },
        },
    }
    graph = {f"{TARGET}|{DEP_A}": [{"op": "CALL", "provenance": []}]}
    result = build_dependency_visualization(_write(tmp_path, _unified(deps=deps, graph=graph)))

    proxy_node = next(n for n in result["nodes"] if n["address"] == DEP_A)
    impl_node = next(n for n in result["nodes"] if n["address"] == IMPL)
    assert proxy_node["label"] == "TransparentProxy"
    assert impl_node["label"] == "TokenV2"


# ---------------------------------------------------------------------------
# Regression: root node label must come from contract_meta.json, not dir name
# ---------------------------------------------------------------------------


def test_root_label_from_contract_meta_not_dir_name(tmp_path):
    """Regression: _contract_label must read contract_meta.json, not use the
    directory name.  Previously the worker temp dir name (e.g. ``psat_static_abc123``)
    leaked into the visualization."""
    # Create a dir whose name looks like a worker temp path
    bad_dir = tmp_path / "psat_static_abc123"
    bad_dir.mkdir()

    # Write contract metadata with the real name
    (bad_dir / "contract_meta.json").write_text(
        json.dumps(
            {
                "contract_name": "LiquidityPool",
                "display_name": "Liquidity Pool",
            }
        )
    )

    deps = {DEP_A: {"type": "regular", "source": ["static"]}}
    _write(bad_dir, _unified(deps=deps))

    result = build_dependency_visualization(bad_dir)

    target_node = next(n for n in result["nodes"] if n["is_target"])
    # Must be the real contract name, NOT the directory name
    assert target_node["label"] == "LiquidityPool"
    assert target_node["label"] != "psat_static_abc123"


def test_root_label_prefers_display_name_for_generic_proxy(tmp_path):
    """Regression: when contract_name is a generic proxy name like 'UUPSProxy',
    _contract_label should return the display_name (job name) instead."""
    contract_dir = tmp_path / "some_worker_dir"
    contract_dir.mkdir()

    (contract_dir / "contract_meta.json").write_text(
        json.dumps(
            {
                "contract_name": "UUPSProxy",
                "display_name": "Rewards Router",
            }
        )
    )

    deps = {DEP_A: {"type": "regular", "source": ["static"]}}
    _write(contract_dir, _unified(deps=deps))

    result = build_dependency_visualization(contract_dir)

    target_node = next(n for n in result["nodes"] if n["is_target"])
    assert target_node["label"] == "Rewards Router"
    assert target_node["label"] != "UUPSProxy"
