"""Integration tests for cross-module pipeline wiring.

Validates behavior that spans multiple modules:
  1. Static worker dependency phase wiring (proxy_address, upgrade_history storage)
  2. API merge + display name pipeline
  3. API detail endpoint artifact inlining
  4. Graph builder label resolution via contract_meta
  5. Full dependency data flow: unified deps -> graph viz -> upgrade history
  6. Discovery -> Static artifact handoff (contract_meta, build_settings, source files)
  7. Static -> Resolution artifact handoff (contract_analysis, control_tracking_plan)
  8. Resolution -> Policy artifact handoff (control_snapshot, resolved_control_graph)
  9. Policy final artifact storage (effective_permissions, principal_labels)
  10. API analyses list and detail endpoints serve worker-stored artifacts correctly
  11. Resolution worker proxy_address override for impl jobs
  12. Tracking plan construction from contract_analysis output
  13. Discovery company mode child job creation

All tests run without live services (no RPC, no Etherscan, no database).
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

TARGET = "0x1111111111111111111111111111111111111111"
PROXY = "0x2222222222222222222222222222222222222222"
IMPL = "0x3333333333333333333333333333333333333333"
DEP_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
DEP_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

# ---------------------------------------------------------------------------
# Shared helpers / fixtures
# ---------------------------------------------------------------------------


def _job(**overrides) -> Any:
    """Create a duck-typed Job stand-in for tests (avoids DB dependency)."""
    defaults: dict[str, Any] = {
        "id": "job-1",
        "address": TARGET,
        "name": "TestContract",
        "request": {"rpc_url": "https://rpc.example"},
        "company": None,
        "protocol_id": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _fake_api_job(
    address: str = "0xabc",
    name: str = "demo_run",
    request: dict | None = None,
    company: str | None = None,
) -> MagicMock:
    job = MagicMock()
    job.id = uuid.uuid4()
    job.address = address
    job.company = company
    job.name = name
    job.status = MagicMock(value="completed")
    job.stage = MagicMock(value="done")
    job.detail = "done"
    job.request = request or {"address": address}
    job.error = None
    job.worker_id = None
    job.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    job.updated_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    job.to_dict.return_value = {"job_id": str(job.id), "address": address, "name": name}
    return job


def _mock_session_ctx(mock_session_cls: MagicMock, mock_session: MagicMock) -> None:
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)


def _static_deps(address: str = TARGET, deps: list[str] | None = None) -> dict:
    return {
        "address": address,
        "dependencies": deps or [],
        "rpc": "https://rpc.example",
        "network": "ethereum",
    }


def _dynamic_deps(address: str = TARGET, deps: list[str] | None = None, graph: list | None = None) -> dict:
    return {
        "address": address,
        "dependencies": deps or [],
        "rpc": "https://rpc.example",
        "dependency_graph": graph or [],
        "transactions_analyzed": [],
        "trace_methods": ["debug_traceTransaction"],
        "trace_errors": [],
    }


def _classifications(target: str = TARGET, cls_map: dict | None = None, discovered: list | None = None) -> dict:
    return {
        "address": target,
        "classifications": cls_map or {},
        "discovered_addresses": discovered or [],
    }


def _patch_dep_phase(monkeypatch, worker, static=None, dynamic=None, classify=None):
    """Wire all external calls for _run_dependency_phase with sensible defaults."""
    store: dict[str, Any] = {}
    monkeypatch.setattr(
        "workers.static_worker.store_artifact",
        lambda _s, _j, name, data=None, text_data=None: store.update({name: data or text_data}),
    )
    monkeypatch.setattr("workers.static_worker.get_artifact", lambda _s, _j, _name: None)
    monkeypatch.setattr(worker, "update_detail", lambda *_a, **_kw: None)
    monkeypatch.setattr(
        "workers.static_worker.find_dependencies",
        lambda addr, rpc_url, code_cache=None: static or _static_deps(addr),
    )
    monkeypatch.setattr(
        "workers.static_worker.find_dynamic_dependencies",
        dynamic
        or (
            lambda addr, rpc_url=None, tx_limit=10, tx_hashes=None, proxy_address=None, code_cache=None, **kw: (
                _dynamic_deps(addr)
            )
        ),
    )
    monkeypatch.setattr(
        "workers.static_worker.classify_contracts",
        classify or (lambda tgt, deps, rpc, dynamic_edges=None, code_cache=None, **kw: _classifications(tgt)),
    )
    monkeypatch.setattr("workers.static_worker.enrich_dependency_metadata", lambda u, **kw: u)
    return store


# ===================================================================
# 1. Static worker: proxy_address wiring to find_dynamic_dependencies
# ===================================================================


def test_dep_phase_passes_proxy_address(monkeypatch, tmp_path):
    """For impl jobs, proxy_address from the job request propagates to
    find_dynamic_dependencies so transactions are fetched from the proxy."""
    from workers.static_worker import StaticWorker

    worker = StaticWorker()
    captured: list[dict] = []

    def capture_dynamic(address, rpc_url=None, tx_limit=10, tx_hashes=None, proxy_address=None, code_cache=None, **kw):
        captured.append({"address": address, "proxy_address": proxy_address})
        graph = [{"from": address, "to": DEP_A, "op": "CALL", "provenance": []}]
        return _dynamic_deps(address, deps=[DEP_A], graph=graph)

    job = _job(
        address=IMPL,
        name="Impl",
        request={"rpc_url": "https://rpc.example", "proxy_address": PROXY},
    )
    _patch_dep_phase(monkeypatch, worker, dynamic=capture_dynamic)
    # Mock upgrade history to avoid Etherscan calls
    monkeypatch.setattr(
        "services.discovery.upgrade_history.build_upgrade_history",
        lambda _p, enrich=True, from_block=0: {
            "schema_version": "0.1",
            "target_address": IMPL,
            "proxies": {},
            "total_upgrades": 0,
        },
    )

    project_dir = tmp_path / "p"
    project_dir.mkdir()
    worker._run_dependency_phase(MagicMock(), job, project_dir, "Impl", IMPL)

    assert len(captured) == 1
    assert captured[0]["address"] == IMPL
    assert captured[0]["proxy_address"] == PROXY


# ===================================================================
# 2. Static worker: upgrade_history stored when proxies exist
# ===================================================================


def test_dep_phase_stores_upgrade_history(monkeypatch, tmp_path):
    """When build_upgrade_history finds proxies, the artifact is stored."""
    from workers.static_worker import StaticWorker

    worker = StaticWorker()
    store = _patch_dep_phase(
        monkeypatch,
        worker,
        static=_static_deps(TARGET, [DEP_A]),
        dynamic=lambda addr, **_kw: _dynamic_deps(addr, [DEP_A]),
    )
    fake_uh = {
        "schema_version": "0.1",
        "target_address": TARGET,
        "proxies": {DEP_A: {}},
        "total_upgrades": 2,
    }
    monkeypatch.setattr(
        "services.discovery.upgrade_history.build_upgrade_history",
        lambda _p, enrich=True, from_block=0: fake_uh,
    )

    project_dir = tmp_path / "p"
    project_dir.mkdir()
    worker._run_dependency_phase(MagicMock(), _job(), project_dir, "TestContract", TARGET)

    assert "upgrade_history" in store
    assert store["upgrade_history"]["total_upgrades"] == 2


def test_dep_phase_skips_upgrade_history_when_no_proxies(monkeypatch, tmp_path):
    """When no proxies are found, upgrade_history artifact is not stored."""
    from workers.static_worker import StaticWorker

    worker = StaticWorker()
    store = _patch_dep_phase(
        monkeypatch,
        worker,
        static=_static_deps(TARGET, [DEP_A]),
        dynamic=lambda addr, **_kw: _dynamic_deps(addr, [DEP_A]),
    )
    monkeypatch.setattr(
        "services.discovery.upgrade_history.build_upgrade_history",
        lambda _p, enrich=True: {"schema_version": "0.1", "target_address": TARGET, "proxies": {}, "total_upgrades": 0},
    )

    project_dir = tmp_path / "p"
    project_dir.mkdir()
    worker._run_dependency_phase(MagicMock(), _job(), project_dir, "TestContract", TARGET)

    assert "upgrade_history" not in store
    assert "dependencies" in store


# ===================================================================
# 3. API: _merge_proxy_impl_entries + _display_name together
# ===================================================================


def test_merge_uses_impl_name_and_propagates_company():
    """Merged entry gets impl contract_name as display_name and inherits
    the proxy's company and rank_score."""
    import api

    proxy_entry = {
        "run_name": "MyProxy",
        "job_id": "j1",
        "address": PROXY,
        "chain": "ethereum",
        "company": "TestCo",
        "parent_job_id": None,
        "rank_score": 0.8,
        "is_proxy": True,
        "proxy_type": "eip1967",
        "implementation_address": IMPL,
        "proxy_address": None,
        "contract_name": "TransparentUpgradeableProxy",
    }
    impl_entry = {
        "run_name": "MyProxy: (impl)",
        "job_id": "j2",
        "address": IMPL,
        "chain": "ethereum",
        "company": None,
        "parent_job_id": "j1",
        "rank_score": None,
        "is_proxy": False,
        "proxy_type": None,
        "implementation_address": None,
        "proxy_address": PROXY,
        "contract_name": "LiquidityPool",
    }

    merged = api._merge_proxy_impl_entries([proxy_entry, impl_entry])
    assert len(merged) == 1
    # Merge sets display_name from impl's contract_name directly (no chain suffix)
    assert merged[0]["display_name"] == "LiquidityPool"
    assert merged[0]["company"] == "TestCo"
    assert merged[0]["rank_score"] == 0.8
    assert merged[0]["proxy_address_display"] == PROXY


def test_display_name_chain_suffix_and_generic_fallback():
    """_display_name appends chain, prefers display_name, and falls back
    to run_name for generic proxy contract names."""
    import api

    entry1 = {"contract_name": "Pool", "run_name": "x", "display_name": None, "chain": "base"}
    assert api._display_name(entry1) == "Pool (base)"
    entry2 = {"contract_name": "ERC1967Proxy", "run_name": "Router", "display_name": None, "chain": None}
    assert api._display_name(entry2) == "Router"
    entry3 = {"contract_name": "Proxy", "run_name": "r", "display_name": "Custom", "chain": None}
    assert api._display_name(entry3) == "Custom"


def test_proxy_with_completed_impl_visible_after_merge():
    """A proxy entry whose impl child has completed should appear as
    a merged entry in the list (this is the normal end state)."""
    import api

    proxy_entry = {
        "run_name": "eETH",
        "job_id": "j1",
        "address": "0x3333333333333333333333333333333333333333",
        "chain": "ethereum",
        "company": "etherfi",
        "parent_job_id": None,
        "rank_score": 0.9,
        "is_proxy": True,
        "proxy_type": "eip1967",
        "implementation_address": "0x4444444444444444444444444444444444444444",
        "proxy_address": None,
        "contract_name": "ERC1967Proxy",
    }
    impl_entry = {
        "run_name": "eETH: (impl)",
        "job_id": "j2",
        "address": "0x4444444444444444444444444444444444444444",
        "chain": "ethereum",
        "company": None,
        "parent_job_id": "j1",
        "rank_score": None,
        "is_proxy": False,
        "proxy_type": None,
        "implementation_address": None,
        "proxy_address": "0x3333333333333333333333333333333333333333",
        "contract_name": "EETH",
    }
    merged = api._merge_proxy_impl_entries([proxy_entry, impl_entry])
    assert len(merged) == 1
    assert merged[0]["display_name"] == "EETH"
    assert merged[0]["proxy_address_display"] == "0x3333333333333333333333333333333333333333"


def test_orphan_impl_appears_in_merged_list():
    """An impl whose proxy_address is not in the list still appears."""
    import api

    orphan = {
        "run_name": "Orphan",
        "job_id": "j1",
        "address": IMPL,
        "chain": None,
        "company": None,
        "parent_job_id": "jx",
        "rank_score": None,
        "is_proxy": False,
        "proxy_type": None,
        "implementation_address": None,
        "proxy_address": "0x9999999999999999999999999999999999999999",
        "contract_name": "Impl",
    }
    merged = api._merge_proxy_impl_entries([orphan])
    assert len(merged) == 1


# ===================================================================
# 4. API detail: inlines upgrade_history and dependency_graph_viz
# ===================================================================


@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_detail_inlines_upgrade_history_and_graph_viz(mock_session_cls, mock_get_all_artifacts):
    from fastapi.testclient import TestClient

    import api

    client = TestClient(api.app)
    fake_job = _fake_api_job(name="proxy_run", address=TARGET)

    mock_session = MagicMock()
    mock_session.execute.return_value.scalar_one_or_none.return_value = fake_job
    _mock_session_ctx(mock_session_cls, mock_session)

    mock_get_all_artifacts.return_value = {
        "contract_analysis": {"subject": {"name": "Pool"}, "summary": {"control_model": "proxy"}},
        "upgrade_history": {"schema_version": "0.1", "proxies": {PROXY: {}}, "total_upgrades": 3},
        "dependency_graph_viz": {"nodes": [{"id": "addr:" + TARGET}], "edges": []},
        "dependencies": {"address": TARGET, "dependencies": {}},
    }

    resp = client.get("/api/analyses/proxy_run")
    assert resp.status_code == 200
    body = resp.json()
    assert body["upgrade_history"]["total_upgrades"] == 3
    assert len(body["dependency_graph_viz"]["nodes"]) == 1
    assert body["contract_name"] == "Pool"


# ===================================================================
# 5. Graph builder: label uses display_name for generic proxy names
# ===================================================================


def test_graph_label_prefers_display_name_for_generic_proxy(tmp_path):
    """When contract_meta has a generic proxy name and a display_name,
    the graph root node label should use display_name."""
    from services.discovery.dependency_graph_builder import build_dependency_visualization

    (tmp_path / "contract_meta.json").write_text(
        json.dumps(
            {
                "contract_name": "ERC1967Proxy",
                "display_name": "Rewards Router",
            }
        )
    )
    unified = {"address": TARGET, "dependencies": {DEP_A: {"type": "regular", "source": ["static"]}}}
    (tmp_path / "dependencies.json").write_text(json.dumps(unified))

    viz = build_dependency_visualization(tmp_path)
    target_node = next(n for n in viz["nodes"] if n["is_target"])
    assert target_node["label"] == "Rewards Router"


# ===================================================================
# 6. Full data flow: unified -> graph viz -> upgrade history
# ===================================================================


def test_full_data_flow_unified_through_graph_and_upgrade_history(monkeypatch, tmp_path):
    """Exercise the real build_unified_dependencies, build_dependency_visualization,
    and build_upgrade_history functions with mocks only at the network boundary,
    verifying the entire data chain."""
    from services.discovery.dependency_graph_builder import build_dependency_visualization
    from services.discovery.unified_dependencies import build_unified_dependencies
    from services.discovery.upgrade_history import build_upgrade_history

    static = _static_deps(TARGET, [DEP_A, DEP_B])
    dynamic = {
        **_dynamic_deps(TARGET, [DEP_A]),
        "dependency_graph": [
            {"from": TARGET, "to": DEP_A, "op": "CALL", "provenance": [{"tx_hash": "0xaa", "block_number": 100}]},
        ],
        "transactions_analyzed": [{"tx_hash": "0xaa", "block_number": 100, "method_selector": "0xdeadbeef"}],
    }
    cls = _classifications(
        TARGET,
        cls_map={
            DEP_A: {"address": DEP_A, "type": "proxy", "proxy_type": "eip1967", "implementation": IMPL},
            DEP_B: {"address": DEP_B, "type": "regular"},
            IMPL: {"address": IMPL, "type": "implementation", "proxies": [DEP_A]},
        },
        discovered=[IMPL],
    )

    # -- unified deps --
    unified = build_unified_dependencies(TARGET, static, dynamic, cls)
    assert DEP_A in unified["dependencies"]
    assert IMPL not in unified["dependencies"]  # nested under DEP_A
    assert unified["dependencies"][DEP_A]["implementation"]["address"] == IMPL

    # -- graph viz --
    (tmp_path / "dependencies.json").write_text(json.dumps(unified))
    (tmp_path / "contract_meta.json").write_text(json.dumps({"contract_name": "TestContract"}))

    viz = build_dependency_visualization(tmp_path)
    node_addrs = {n["address"] for n in viz["nodes"]}
    assert {TARGET, DEP_A, DEP_B, IMPL} <= node_addrs

    edge_ops = {(e["from"], e["to"], e["op"]) for e in viz["edges"]}
    assert (f"addr:{DEP_A}", f"addr:{IMPL}", "DELEGATES_TO") in edge_ops
    assert (f"addr:{TARGET}", f"addr:{DEP_A}", "CALL") in edge_ops
    assert any(e["op"] == "STATIC_REF" and e["to"] == f"addr:{DEP_B}" for e in viz["edges"])

    # -- upgrade history (mock Etherscan boundary) --
    monkeypatch.setattr("services.discovery.upgrade_history._fetch_logs_etherscan", lambda _a, _t, from_block=0: [])
    from utils import etherscan

    monkeypatch.setattr(etherscan, "get_contract_info", lambda _a: (None, {}))

    deps_path = tmp_path / "dependencies.json"
    uh = build_upgrade_history(deps_path)
    assert DEP_A in uh["proxies"]
    assert uh["proxies"][DEP_A]["current_implementation"] == IMPL


# ===================================================================
# 7. Discovery -> Static: artifact name contract between workers
# ===================================================================


def test_discovery_artifact_names_match_static_worker_reads():
    """The data that discovery stores must match what static reads.

    Discovery writes to the Contract table and stores source files via
    store_source_files.  Static reads from the Contract table via
    session.execute(select(Contract)...) and source files via
    get_source_files.  This test verifies both modules reference the
    same DB model (Contract) and source-file helpers.
    """
    import inspect

    import workers.discovery as disc
    import workers.static_worker as sw

    # Discovery writes to Contract table and stores source files
    disc_source = inspect.getsource(disc.DiscoveryWorker._process_address)
    assert "Contract(" in disc_source
    assert "store_source_files" in disc_source

    # Static reads from Contract table and source files
    sw_source = inspect.getsource(sw.StaticWorker.process)
    assert "Contract" in sw_source
    assert "get_source_files" in sw_source


# ===================================================================
# 8. Static -> Resolution: artifact name contract between workers
# ===================================================================


def test_static_artifact_names_match_resolution_worker_reads():
    """Static stores 'contract_analysis' and 'control_tracking_plan';
    Resolution reads them back."""
    import inspect

    import workers.resolution_worker as rw
    import workers.static_worker as sw

    # Static stores
    slither_source = inspect.getsource(sw.StaticWorker._run_analysis_phase)
    plan_source = inspect.getsource(sw.StaticWorker._run_tracking_plan_phase)
    assert '"contract_analysis"' in slither_source
    assert '"control_tracking_plan"' in plan_source

    # Resolution reads
    rw_source = inspect.getsource(rw.ResolutionWorker.process)
    assert '"control_tracking_plan"' in rw_source
    assert '"contract_analysis"' in rw_source


# ===================================================================
# 9. Resolution -> Policy: artifact name contract between workers
# ===================================================================


def test_resolution_artifact_names_match_policy_worker_reads():
    """Resolution stores 'control_snapshot' and 'resolved_control_graph';
    Policy reads them back."""
    import inspect

    import workers.policy_worker as pw
    import workers.resolution_worker as rw

    # Resolution stores
    rw_source = inspect.getsource(rw.ResolutionWorker.process)
    assert '"control_snapshot"' in rw_source
    assert '"resolved_control_graph"' in rw_source

    # Policy reads
    pw_source = inspect.getsource(pw.PolicyWorker.process)
    assert '"contract_analysis"' in pw_source
    assert '"control_snapshot"' in pw_source
    assert '"resolved_control_graph"' in pw_source


# ===================================================================
# 10. Policy: stores final artifacts that API detail endpoint inlines
# ===================================================================


def test_policy_stores_all_artifacts_that_api_detail_inlines():
    """Policy worker stores effective_permissions, principal_labels, and
    resolved_control_graph. The API detail endpoint inlines each of these.
    Verify the names match between producer and consumer."""
    import inspect

    import api
    import workers.policy_worker as pw

    pw_source = inspect.getsource(pw.PolicyWorker.process)

    # Policy stores these
    assert '"effective_permissions"' in pw_source
    assert '"principal_labels"' in pw_source
    assert '"resolved_control_graph"' in pw_source

    # API detail inlines these
    api_source = inspect.getsource(api.analysis_detail)
    assert '"effective_permissions"' in api_source
    assert '"principal_labels"' in api_source
    assert '"resolved_control_graph"' in api_source
    assert '"contract_analysis"' in api_source
    assert '"control_snapshot"' in api_source


# ===================================================================
# 11. API detail endpoint inlines all expected artifacts
# ===================================================================


@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_detail_inlines_all_pipeline_artifacts(mock_session_cls, mock_get_all_artifacts):
    """The API detail endpoint must inline effective_permissions,
    principal_labels, control_snapshot, and resolved_control_graph
    alongside the already-tested contract_analysis and dependencies."""
    from fastapi.testclient import TestClient

    import api

    client = TestClient(api.app)
    fake_job = _fake_api_job(name="full_run", address=TARGET)

    mock_session = MagicMock()

    # Build mock relational objects for effective_permissions and principal_labels.
    # The API now reads these from the EffectiveFunction / PrincipalLabel tables,
    # not from artifacts.
    fake_contract_row = MagicMock()
    fake_contract_row.id = "contract-1"
    fake_contract_row.contract_name = "Vault"
    fake_contract_row.address = TARGET
    fake_contract_row.is_proxy = False
    fake_contract_row.implementation = None
    fake_contract_row.summary = None

    fake_ef = MagicMock()
    fake_ef.id = "ef-1"
    fake_ef.abi_signature = "pause()"
    fake_ef.function_name = "pause"
    fake_ef.selector = "0x12"
    fake_ef.effect_labels = []
    fake_ef.action_summary = None
    fake_ef.authority_public = False

    fake_fp = MagicMock()
    fake_fp.address = "0xaa"
    fake_fp.resolved_type = "admin"
    fake_fp.origin = None
    fake_fp.details = {}

    fake_pl = MagicMock()
    fake_pl.address = "0xaa"
    fake_pl.label = "admin"
    fake_pl.resolved_type = "eoa"

    # Route session.execute calls based on what the API queries
    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        stmt_str = str(stmt)
        if call_count["n"] == 1:
            # First call: select(Job) to find job by name
            result.scalar_one_or_none.return_value = fake_job
        elif "contract" in stmt_str.lower() and "job_id" in stmt_str.lower() and call_count["n"] == 2:
            # Second call: select(Contract) for contract_row
            result.scalar_one_or_none.return_value = fake_contract_row
        elif "effective" in stmt_str.lower():
            # EffectiveFunction query
            result.scalars.return_value.all.return_value = [fake_ef]
        elif "function_principal" in stmt_str.lower():
            # FunctionPrincipal query
            result.scalars.return_value.all.return_value = [fake_fp]
        elif "principal_label" in stmt_str.lower():
            # PrincipalLabel query
            result.scalars.return_value.all.return_value = [fake_pl]
        else:
            result.scalar_one_or_none.return_value = None
            result.scalars.return_value.all.return_value = []
        return result

    mock_session.execute.side_effect = route_execute
    mock_session.get.return_value = None
    _mock_session_ctx(mock_session_cls, mock_session)

    mock_get_all_artifacts.return_value = {
        "contract_analysis": {"subject": {"name": "Vault"}, "summary": {"control_model": "authority"}},
        "control_snapshot": {"schema_version": "0.1", "controller_values": {"state_variable:owner": {"value": "0xaa"}}},
        "resolved_control_graph": {"nodes": [{"id": "a", "address": TARGET}], "edges": []},
        "dependencies": {"address": TARGET, "dependencies": {}},
        "analysis_report": "High-level report text",
    }

    resp = client.get("/api/analyses/full_run")
    assert resp.status_code == 200
    body = resp.json()

    # JSON artifacts inlined from all_artifacts
    assert body["contract_analysis"]["summary"]["control_model"] == "authority"
    assert "state_variable:owner" in body["control_snapshot"]["controller_values"]
    assert len(body["resolved_control_graph"]["nodes"]) == 1
    assert body["dependencies"]["address"] == TARGET

    # effective_permissions built from EffectiveFunction + FunctionPrincipal tables
    assert body["effective_permissions"]["functions"][0]["function"] == "pause()"

    # principal_labels built from PrincipalLabel table
    assert body["principal_labels"]["principals"][0]["label"] == "admin"

    # Text artifact should be inlined
    assert body["analysis_report"] == "High-level report text"

    # Subject info should be extracted
    assert body["contract_name"] == "Vault"


# ===================================================================
# 12. API analyses list serves contract_flags stored by static worker
# ===================================================================


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_analyses_list_reads_contract_flags_from_static_worker(mock_session_cls, mock_get_artifact):
    """The analyses list endpoint reads 'contract_flags' artifact that
    static worker stores during _resolve_proxy. Verify the is_proxy
    and proxy_type fields propagate into the merged entry.

    _merge_proxy_impl_entries hides proxy entries whose impl child job
    hasn't completed, so we include both the proxy and impl jobs."""
    from fastapi.testclient import TestClient

    import api

    client = TestClient(api.app)
    proxy_job = _fake_api_job(name="proxy_test", address=PROXY)
    impl_job = _fake_api_job(
        name="proxy_test: (impl)",
        address=IMPL,
        request={"address": IMPL, "proxy_address": PROXY, "parent_job_id": str(proxy_job.id)},
    )

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    from db.models import JobStatus

    impl_job.status = JobStatus.completed

    # Route execute calls: first returns jobs, subsequent return artifact names
    # or impl job lookups
    call_count = 0

    def route_execute(stmt, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        if call_count == 1:
            result.scalars.return_value.all.return_value = [proxy_job, impl_job]
        else:
            result.scalar_one_or_none.return_value = impl_job
            result.scalars.return_value.all.return_value = ["contract_flags", "contract_analysis"]
        return result

    mock_session.execute.side_effect = route_execute

    def _get_artifact(_session, _job_id, name):
        if str(_job_id) == str(proxy_job.id):
            if name == "contract_analysis":
                return {"subject": {"name": "MyProxy"}, "summary": {"control_model": "proxy"}}
            if name == "contract_flags":
                return {"is_proxy": True, "proxy_type": "eip1967", "implementation": IMPL}
        if str(_job_id) == str(impl_job.id):
            if name == "contract_analysis":
                return {"subject": {"name": "VaultImpl"}, "summary": {"control_model": "authority"}}
        if name == "contract_inventory":
            return None
        if name == "contract_flags":
            return None
        return None

    mock_get_artifact.side_effect = _get_artifact

    resp = client.get("/api/analyses")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) >= 1
    merged = body[0]
    # The merged entry carries proxy info via proxy_address_display and
    # proxy_type_display (not is_proxy — that comes from the impl base).
    assert merged["proxy_address_display"] == PROXY
    assert merged["proxy_type_display"] == "eip1967"


# ===================================================================
# 13. Resolution worker: proxy_address overrides tracking plan address
# ===================================================================


def test_resolution_worker_rewrites_address_for_impl_jobs(monkeypatch):
    """When a job has proxy_address in its request, the resolution worker
    should override contract_address in the tracking plan and subject.address
    in the contract_analysis so state is read from the proxy, not the impl."""
    from workers.resolution_worker import ResolutionWorker

    worker = ResolutionWorker()
    session = MagicMock()

    job = _job(
        address=IMPL,
        name="Impl",
        request={"rpc_url": "https://rpc.example", "proxy_address": PROXY},
    )

    # What static worker stored
    tracking_plan = {
        "schema_version": "0.1",
        "contract_address": IMPL,
        "contract_name": "VaultImpl",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [],
        "tracked_policies": [],
    }
    contract_analysis = {
        "subject": {"address": IMPL, "name": "VaultImpl"},
        "access_control": {"privileged_functions": []},
    }

    artifacts = {
        "control_tracking_plan": tracking_plan,
        "contract_analysis": contract_analysis,
    }

    monkeypatch.setattr(
        "workers.resolution_worker.get_artifact",
        lambda _session, _job_id, name: artifacts.get(name),
    )

    # Capture what build_control_snapshot receives
    captured_plans: list[dict] = []
    captured_analyses: list[dict] = []

    def fake_build_snapshot(plan, _rpc_url):
        captured_plans.append(plan)
        return {
            "schema_version": "0.1",
            "contract_address": plan["contract_address"],
            "contract_name": "VaultImpl",
            "block_number": 100,
            "controller_values": {},
        }

    stored_artifacts: dict[str, Any] = {}
    monkeypatch.setattr("workers.resolution_worker.build_control_snapshot", fake_build_snapshot)
    monkeypatch.setattr(
        "workers.resolution_worker.store_artifact",
        lambda _s, _j, name, data=None, text_data=None: stored_artifacts.update({name: data or text_data}),
    )
    monkeypatch.setattr(worker, "update_detail", lambda *_a, **_kw: None)

    # Mock write_resolved_control_graph to capture the analysis it receives
    def fake_write_graph(analysis_path, rpc_url, output_path, max_depth, workspace_prefix, refresh_snapshots):
        analysis_data = json.loads(analysis_path.read_text())
        captured_analyses.append(analysis_data)
        output_path.write_text(json.dumps({"nodes": [], "edges": []}) + "\n")
        return output_path

    monkeypatch.setattr("workers.resolution_worker.write_resolved_control_graph", fake_write_graph)

    worker.process(session, job)  # type: ignore[arg-type]

    # The tracking plan should have proxy address, not impl
    assert captured_plans[0]["contract_address"] == PROXY

    # The analysis written to disk should have proxy address
    assert captured_analyses[0]["subject"]["address"] == PROXY

    # Artifacts should be stored
    assert "control_snapshot" in stored_artifacts
    assert "resolved_control_graph" in stored_artifacts


# ===================================================================
# 14. Tracking plan construction preserves controller_tracking fields
# ===================================================================


def test_tracking_plan_preserves_controller_ids_and_read_specs():
    """The tracking plan builder must carry forward controller_id, read_spec,
    and associated_events from contract_analysis.controller_tracking. These
    are consumed by build_control_snapshot in the resolution stage."""
    from services.resolution.tracking_plan import build_control_tracking_plan

    analysis = {
        "subject": {"address": "0x1111111111111111111111111111111111111111", "name": "Vault"},
        "controller_tracking": [
            {
                "controller_id": "state_variable:owner",
                "label": "owner",
                "source": "owner",
                "kind": "state_variable",
                "read_spec": {"strategy": "getter_call", "target": "owner"},
                "tracking_mode": "event_plus_state",
                "associated_events": [
                    {
                        "name": "OwnershipTransferred",
                        "signature": "OwnershipTransferred(address,address)",
                        "topic0": "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0",
                        "inputs": [
                            {"name": "user", "type": "address", "indexed": True},
                            {"name": "newOwner", "type": "address", "indexed": True},
                        ],
                    }
                ],
                "writer_functions": [{"function": "transferOwnership(address)"}],
                "polling_sources": ["owner"],
                "notes": [],
            },
            {
                "controller_id": "external_contract:authority",
                "label": "authority",
                "source": "authority",
                "kind": "external_contract",
                "read_spec": {"strategy": "getter_call", "target": "authority"},
                "tracking_mode": "event_plus_state",
                "associated_events": [
                    {
                        "name": "AuthorityUpdated",
                        "signature": "AuthorityUpdated(address,address)",
                        "topic0": "0xa3396fd7f6e0a21b50e5089d2da70d5ac0a3bbbd1f617a93f134b76389980198",
                        "inputs": [
                            {"name": "user", "type": "address", "indexed": True},
                            {"name": "newAuthority", "type": "address", "indexed": True},
                        ],
                    }
                ],
                "writer_functions": [{"function": "setAuthority(address)"}],
                "polling_sources": ["authority"],
                "notes": [],
            },
        ],
        "policy_tracking": [],
    }

    plan = build_control_tracking_plan(analysis)  # type: ignore[arg-type]

    assert plan["contract_address"] == "0x1111111111111111111111111111111111111111"
    assert plan["contract_name"] == "Vault"
    assert len(plan["tracked_controllers"]) == 2

    # Controller IDs carry through (resolution worker uses these as keys)
    ids = {tc["controller_id"] for tc in plan["tracked_controllers"]}
    assert ids == {"state_variable:owner", "external_contract:authority"}

    # Read specs carry through (build_control_snapshot uses these)
    for tc in plan["tracked_controllers"]:
        assert tc["read_spec"] is not None
        assert tc["read_spec"]["strategy"] == "getter_call"

    # Event watches carry through (event-first tracking)
    owner = next(tc for tc in plan["tracked_controllers"] if tc["label"] == "owner")
    assert owner["event_watch"] is not None
    assert len(owner["event_watch"]["events"]) == 1
    assert owner["event_watch"]["events"][0]["name"] == "OwnershipTransferred"


# ===================================================================
# 15. Tracking plan policy_tracking round-trip
# ===================================================================


def test_tracking_plan_preserves_policy_tracking_fields():
    """Policy tracking targets from contract_analysis must carry through
    to the tracking plan. The resolution and policy workers depend on
    the policy_id, tracked_state_targets, and event_watch fields."""
    from services.resolution.tracking_plan import build_control_tracking_plan

    analysis = {
        "subject": {"address": "0x1111111111111111111111111111111111111111", "name": "Auth"},
        "controller_tracking": [],
        "policy_tracking": [
            {
                "policy_id": "canCall_policy",
                "label": "canCall policy",
                "policy_function": "canCall(address,address,bytes4)",
                "tracked_state_targets": ["getUserRoles", "getRolesWithCapability"],
                "associated_events": [
                    {
                        "name": "UserRoleUpdated",
                        "signature": "UserRoleUpdated(address,uint8,bool)",
                        "topic0": "0xabcdef",
                        "inputs": [],
                    }
                ],
                "writer_functions": [{"function": "setUserRole(address,uint8,bool)"}],
                "notes": ["Track role mutations"],
            }
        ],
    }

    plan = build_control_tracking_plan(analysis)  # type: ignore[arg-type]

    assert len(plan["tracked_policies"]) == 1
    policy = plan["tracked_policies"][0]
    assert policy["policy_id"] == "canCall_policy"
    assert policy["policy_function"] == "canCall(address,address,bytes4)"
    assert policy["tracked_state_targets"] == ["getUserRoles", "getRolesWithCapability"]
    assert policy["event_watch"]["events"][0]["name"] == "UserRoleUpdated"
    assert policy["event_watch"]["writer_functions"] == ["setUserRole(address,uint8,bool)"]


# ===================================================================
# 16. Discovery company mode creates child jobs with correct request fields
# ===================================================================


def test_discovery_company_mode_creates_child_jobs(monkeypatch):
    """When DiscoveryWorker processes a company job, it creates child jobs
    with address, name, chain, rpc_url, and parent_job_id in the request.
    The static worker depends on these fields being present."""
    from workers.base import JobHandledDirectly
    from workers.discovery import DiscoveryWorker

    worker = DiscoveryWorker()
    session = MagicMock()

    job = SimpleNamespace(
        id="parent-1",
        address=None,
        company="TestProtocol",
        name=None,
        protocol_id=None,
        request={"company": "TestProtocol", "chain": "ethereum", "rpc_url": "https://rpc.example", "analyze_limit": 2},
    )
    # Mock session.commit and session.flush to avoid DB calls
    session.commit = MagicMock()
    session.flush = MagicMock()
    session.add = MagicMock()
    # Mock session.execute to return None for Protocol lookup and Contract lookups
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    session.execute = MagicMock(return_value=mock_result)

    stored_artifacts: dict[str, Any] = {}
    created_jobs: list[dict] = []

    monkeypatch.setattr(
        "workers.discovery.store_artifact",
        lambda _s, _j, name, data=None, text_data=None: stored_artifacts.update({name: data or text_data}),
    )
    monkeypatch.setattr(
        "workers.discovery.create_job",
        lambda _s, req, initial_stage=None: (
            created_jobs.append(req) or SimpleNamespace(id=f"child-{len(created_jobs)}", company=None, protocol_id=None)
        ),
    )
    monkeypatch.setattr(worker, "update_detail", lambda *_a, **_kw: None)

    # Mock count_analysis_children (now called before selecting contracts)
    monkeypatch.setattr(
        "workers.discovery.count_analysis_children",
        lambda _s, _root_id: 0,
    )

    # Mock get_artifact (no previous inventory)
    monkeypatch.setattr(
        "workers.discovery.get_artifact",
        lambda _s, _j, _name: None,
    )

    # Mock cache lookup and dedup functions (no previous inventory, no existing jobs)
    monkeypatch.setattr(
        "workers.discovery.find_previous_company_inventory",
        lambda _s, _company, exclude_job_id=None, chain=None: None,
    )
    monkeypatch.setattr(
        "workers.discovery.find_existing_job_for_address",
        lambda _s, _addr, chain=None: None,
    )


    # Mock inventory search to return 2 contracts
    monkeypatch.setattr(
        "workers.discovery.search_protocol_inventory",
        lambda company, chain=None, limit=25: {
            "contracts": [
                {"address": "0xaaaa" + "a" * 36, "name": "TokenA", "chains": ["ethereum"], "confidence": 0.9},
                {"address": "0xbbbb" + "b" * 36, "name": "TokenB", "chains": ["ethereum"], "confidence": 0.8},
            ],
            "official_domain": "testprotocol.io",
        },
    )

    # Mock complete_job
    monkeypatch.setattr("db.queue.complete_job", lambda _s, _j, detail="": None)

    # Mock _spawn_parallel_discovery (calls external services)
    monkeypatch.setattr(worker, "_spawn_parallel_discovery", lambda *_a, **_kw: None)

    try:
        worker.process(session, job)  # type: ignore[arg-type]
    except JobHandledDirectly:
        pass  # Expected for company jobs

    # Verify child jobs have required fields for static worker
    assert len(created_jobs) == 2
    for child_req in created_jobs:
        assert "address" in child_req
        assert "name" in child_req
        assert "parent_job_id" in child_req
        assert child_req["parent_job_id"] == "parent-1"
        assert child_req["rpc_url"] == "https://rpc.example"

    # Verify discovery_summary artifact was stored
    assert "discovery_summary" in stored_artifacts
    summary = stored_artifacts["discovery_summary"]
    assert summary["mode"] == "company"
    assert summary["company"] == "TestProtocol"
    assert summary["analyzed_count"] == 2
    assert len(summary["child_jobs"]) == 2

    # Verify contract_inventory artifact was stored
    assert "contract_inventory" in stored_artifacts


# ===================================================================
# 17. Static worker: process method reads discovery artifacts correctly
# ===================================================================


def test_static_worker_reads_discovery_artifacts(monkeypatch):
    """StaticWorker.process reads contract metadata from the Contract table
    and source files from get_source_files. Verify it handles the expected
    data shapes and passes them to _scaffold_project correctly."""
    from workers.static_worker import StaticWorker

    worker = StaticWorker()
    session = MagicMock()

    job = _job(name="TestContract")

    # Mock what discovery stored
    sources = {"src/Test.sol": "pragma solidity ^0.8.19;\ncontract Test {}"}

    monkeypatch.setattr("workers.static_worker.get_source_files", lambda _s, _j: sources)

    # Mock the Contract table row that discovery now writes
    contract_row = SimpleNamespace(
        address=TARGET,
        contract_name="Test",
        compiler_version="v0.8.19",
        language="solidity",
        evm_version="shanghai",
        optimization=True,
        optimization_runs=200,
        source_format="flat",
        source_file_count=1,
        remappings=[],
        is_proxy=False,
    )
    session.execute.return_value.scalar_one_or_none.return_value = contract_row
    session.refresh = MagicMock()

    # Capture calls to worker phases
    scaffold_args: list[tuple] = []
    monkeypatch.setattr(
        worker,
        "_scaffold_project",
        lambda project_dir, src, m, bs, rm: scaffold_args.append((src, m, bs, rm)),
    )
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *_a, **_kw: None)
    monkeypatch.setattr(worker, "_run_dependency_phase", lambda *_a, **_kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *_a, **_kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *_a, **_kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *_a, **_kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *_a, **_kw: None)

    worker.process(session, job)  # type: ignore[arg-type]

    assert len(scaffold_args) == 1
    passed_sources, passed_meta, passed_build, passed_remap = scaffold_args[0]
    assert passed_sources == sources
    assert passed_meta["contract_name"] == "Test"
    assert passed_build["evm_version"] == "shanghai"
    assert passed_remap == []


# ===================================================================
# 18. Static worker: _scaffold_project writes foundry.toml and sources
# ===================================================================


def test_scaffold_project_writes_expected_files(tmp_path):
    """_scaffold_project must write foundry.toml, source files, remappings,
    and contract_meta.json. These files must be present for Slither and
    contract analysis to function."""
    from workers.static_worker import StaticWorker

    worker = StaticWorker()
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    sources = {
        "src/Vault.sol": "pragma solidity ^0.8.24;\ncontract Vault { address owner; }",
        "lib/openzeppelin/Ownable.sol": "pragma solidity ^0.8.24;\ncontract Ownable {}",
    }
    meta = {"address": TARGET, "contract_name": "Vault"}
    build_settings = {"evm_version": "shanghai", "optimization_used": True, "runs": 200}
    remappings = ["@openzeppelin/=lib/openzeppelin/"]

    worker._scaffold_project(project_dir, sources, meta, build_settings, remappings)

    # foundry.toml must exist with src dir and solc version
    foundry_toml = (project_dir / "foundry.toml").read_text()
    assert 'src = "src"' in foundry_toml
    assert "solc_version" in foundry_toml

    # Source files must be written
    assert (project_dir / "src" / "Vault.sol").exists()
    assert (project_dir / "lib" / "openzeppelin" / "Ownable.sol").exists()

    # Remappings must be written (since lib/openzeppelin/ has files)
    assert (project_dir / "remappings.txt").exists()

    # contract_meta.json must be written
    meta_data = json.loads((project_dir / "contract_meta.json").read_text())
    assert meta_data["contract_name"] == "Vault"


# ===================================================================
# 19. API artifact endpoint strips .json extension for lookup
# ===================================================================


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_artifact_endpoint_strips_json_extension(mock_session_cls, mock_get_artifact):
    """The artifact endpoint should strip .json and .txt extensions when
    looking up artifacts, since workers store artifacts without extensions
    but the frontend requests them with extensions."""
    from fastapi.testclient import TestClient

    import api

    client = TestClient(api.app)
    fake_job = _fake_api_job(name="test_run")

    mock_session = MagicMock()
    mock_session.execute.return_value.scalar_one_or_none.return_value = fake_job
    _mock_session_ctx(mock_session_cls, mock_session)

    call_names: list[str] = []

    def _get_artifact(_session, _job_id, name):
        call_names.append(name)
        if name == "effective_permissions":
            return {"functions": []}
        return None

    mock_get_artifact.side_effect = _get_artifact

    resp = client.get("/api/analyses/test_run/artifact/effective_permissions.json")
    assert resp.status_code == 200
    # First call should be with stripped name
    assert call_names[0] == "effective_permissions"


# ===================================================================
# 20. Static worker: dependency_errors artifact stored on phase failures
# ===================================================================


def test_dep_phase_stores_dependency_errors_on_failure(monkeypatch, tmp_path):
    """When dependency discovery phases fail, the errors should be stored
    in a 'dependency_errors' artifact so downstream consumers know what
    failed."""
    from workers.static_worker import StaticWorker

    worker = StaticWorker()
    store: dict[str, Any] = {}

    monkeypatch.setattr(
        "workers.static_worker.store_artifact",
        lambda _s, _j, name, data=None, text_data=None: store.update({name: data or text_data}),
    )
    monkeypatch.setattr("workers.static_worker.get_artifact", lambda _s, _j, _name: None)
    monkeypatch.setattr(worker, "update_detail", lambda *_a, **_kw: None)
    monkeypatch.setattr(
        "workers.static_worker.find_dependencies",
        lambda addr, rpc_url, code_cache=None: (_ for _ in ()).throw(RuntimeError("static dep error")),
    )
    monkeypatch.setattr(
        "workers.static_worker.find_dynamic_dependencies",
        lambda addr, rpc_url=None, tx_limit=10, tx_hashes=None, proxy_address=None, code_cache=None, **kw: (
            _ for _ in ()
        ).throw(RuntimeError("dynamic dep error")),
    )

    project_dir = tmp_path / "p"
    project_dir.mkdir()
    worker._run_dependency_phase(MagicMock(), _job(), project_dir, "Test", TARGET)

    assert "dependency_errors" in store
    assert "static" in store["dependency_errors"]
    assert "dynamic" in store["dependency_errors"]
    assert "static dep error" in store["dependency_errors"]["static"]
    assert "dynamic dep error" in store["dependency_errors"]["dynamic"]


# ===================================================================
# 21. Static worker: proxy jobs skip analysis and complete directly
# ===================================================================


def test_static_worker_proxy_skips_analysis_and_completes(monkeypatch):
    """When the static worker detects a proxy contract, it should skip
    Slither/analysis/tracking_plan, call complete_job, and raise
    JobHandledDirectly.  This test exercises the actual process() method
    to catch import errors and wiring bugs."""
    from workers.base import JobHandledDirectly
    from workers.static_worker import StaticWorker

    worker = StaticWorker()
    session = MagicMock()

    job = _job(name="MyProxy")

    sources = {"src/Proxy.sol": "pragma solidity ^0.8.19;\ncontract Proxy {}"}

    # Mock DB reads
    monkeypatch.setattr("workers.static_worker.get_source_files", lambda _s, _j: sources)

    # Mock the Contract table row — after _resolve_proxy runs and session.refresh
    # is called, is_proxy should be True
    contract_row = SimpleNamespace(
        address=TARGET,
        contract_name="Proxy",
        compiler_version="v0.8.19",
        language="solidity",
        evm_version="shanghai",
        optimization=True,
        optimization_runs=200,
        source_format="flat",
        source_file_count=1,
        remappings=[],
        is_proxy=True,
    )
    session.execute.return_value.scalar_one_or_none.return_value = contract_row
    session.refresh = MagicMock()

    # Mock external calls
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *_a, **_kw: None)
    monkeypatch.setattr(worker, "_run_dependency_phase", lambda *_a, **_kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *_a, **_kw: None)

    completed = []
    monkeypatch.setattr("db.queue.complete_job", lambda _s, _j, detail="": completed.append(True))

    # Slither/analysis should NOT be called
    slither_called = []
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *_a, **_kw: slither_called.append(True) or True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *_a, **_kw: slither_called.append(True) or True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *_a, **_kw: slither_called.append(True))

    try:
        worker.process(session, job)  # type: ignore[arg-type]
        assert False, "Expected JobHandledDirectly"
    except JobHandledDirectly:
        pass

    assert len(completed) == 1, "complete_job should have been called"
    assert len(slither_called) == 0, "Slither/analysis should NOT run for proxy contracts"


# ===================================================================
# 22. Pipeline stage sequence: stage/next_stage chain is connected
# ===================================================================


def test_worker_stage_chain_is_complete():
    """The pipeline stage chain must be: discovery -> static -> resolution
    -> policy -> done. If any next_stage doesn't match the following
    worker's stage, jobs will get stuck."""
    from db.models import JobStage
    from workers.discovery import DiscoveryWorker
    from workers.policy_worker import PolicyWorker
    from workers.resolution_worker import ResolutionWorker
    from workers.static_worker import StaticWorker

    # Verify the chain
    assert DiscoveryWorker.stage == JobStage.discovery
    assert DiscoveryWorker.next_stage == JobStage.static

    assert StaticWorker.stage == JobStage.static
    assert StaticWorker.next_stage == JobStage.resolution

    assert ResolutionWorker.stage == JobStage.resolution
    assert ResolutionWorker.next_stage == JobStage.policy

    assert PolicyWorker.stage == JobStage.policy
    assert PolicyWorker.next_stage == JobStage.done

    # Verify the chain links: each worker's next_stage matches the next worker's stage
    chain = [DiscoveryWorker, StaticWorker, ResolutionWorker, PolicyWorker]
    for i in range(len(chain) - 1):
        assert chain[i].next_stage == chain[i + 1].stage, (
            f"{chain[i].__name__}.next_stage ({chain[i].next_stage}) != "
            f"{chain[i + 1].__name__}.stage ({chain[i + 1].stage})"
        )


# ===================================================================
# 22. Policy worker reads all three required artifacts
# ===================================================================


def test_policy_worker_fails_cleanly_on_missing_artifacts(monkeypatch):
    """Policy worker should raise RuntimeError if contract_analysis or
    control_snapshot are missing. These are produced by the static and
    resolution workers respectively."""
    from workers.policy_worker import PolicyWorker

    worker = PolicyWorker()
    session = MagicMock()
    job = _job(request={"rpc_url": "https://rpc.example"})

    # Missing contract_analysis
    monkeypatch.setattr(
        "workers.policy_worker.get_artifact",
        lambda _s, _j, name: None,
    )

    import pytest

    with pytest.raises(RuntimeError, match="contract_analysis"):
        worker.process(session, job)  # type: ignore[arg-type]

    # contract_analysis present but control_snapshot missing
    monkeypatch.setattr(
        "workers.policy_worker.get_artifact",
        lambda _s, _j, name: {"subject": {"address": TARGET, "name": "T"}} if name == "contract_analysis" else None,
    )

    with pytest.raises(RuntimeError, match="control_snapshot"):
        worker.process(session, job)  # type: ignore[arg-type]


# ===================================================================
# 23. Resolution worker fails cleanly on missing artifacts
# ===================================================================


def test_resolution_worker_fails_on_missing_artifacts(monkeypatch):
    """Resolution worker should raise RuntimeError if control_tracking_plan
    or contract_analysis are missing from the DB."""
    from workers.resolution_worker import ResolutionWorker

    worker = ResolutionWorker()
    session = MagicMock()
    job = _job(request={"rpc_url": "https://rpc.example"})

    import pytest

    # Missing tracking plan
    monkeypatch.setattr(
        "workers.resolution_worker.get_artifact",
        lambda _s, _j, name: None,
    )
    with pytest.raises(RuntimeError, match="control_tracking_plan"):
        worker.process(session, job)  # type: ignore[arg-type]

    # tracking plan present but contract_analysis missing
    monkeypatch.setattr(
        "workers.resolution_worker.get_artifact",
        lambda _s, _j, name: (
            {"schema_version": "0.1", "tracked_controllers": [], "tracked_policies": []}
            if name == "control_tracking_plan"
            else None
        ),
    )
    with pytest.raises(RuntimeError, match="contract_analysis"):
        worker.process(session, job)  # type: ignore[arg-type]
