"""Tests for ResolutionWorker — process(), _fetch_balances, _queue_discovered_contracts, _run_upgrade_history."""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from workers.resolution_worker import ResolutionWorker

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TARGET_ADDRESS = "0x1111111111111111111111111111111111111111"
PROXY_ADDRESS = "0x2222222222222222222222222222222222222222"
CHILD_ADDRESS = "0x3333333333333333333333333333333333333333"


def _job(**overrides: Any) -> SimpleNamespace:
    payload: dict[str, Any] = {
        "id": uuid.uuid4(),
        "address": TARGET_ADDRESS,
        "name": "TestContract",
        "company": None,
        "protocol_id": None,
        "request": {"rpc_url": "https://rpc.example"},
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _minimal_tracking_plan() -> dict:
    return {
        "contract_address": TARGET_ADDRESS,
        "controllers": [],
    }


def _minimal_contract_analysis() -> dict:
    return {
        "subject": {"address": TARGET_ADDRESS},
        "contract_name": "TestContract",
        "functions": [],
    }


def _minimal_snapshot() -> dict:
    return {
        "contract_address": TARGET_ADDRESS,
        "controller_values": {},
        "block_number": 12345,
    }


def _resolved_graph(nodes: list[dict] | None = None, edges: list[dict] | None = None) -> dict:
    return {
        "root_contract_address": TARGET_ADDRESS,
        "nodes": nodes or [],
        "edges": edges or [],
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _patch_all(monkeypatch: pytest.MonkeyPatch, **overrides: Any) -> dict[str, Any]:
    """Patch all external dependencies for ResolutionWorker and return tracking dicts."""
    tracking_plan = overrides.get("tracking_plan", _minimal_tracking_plan())
    contract_analysis = overrides.get("contract_analysis", _minimal_contract_analysis())
    snapshot = overrides.get("snapshot", _minimal_snapshot())
    resolved_graph = overrides.get("resolved_graph", _resolved_graph())
    dependencies = overrides.get("dependencies", None)  # None = no artifact

    artifact_store: dict[str, Any] = {}

    def fake_get_artifact(_session: Any, _job_id: Any, name: str) -> Any:
        lookup: dict[str, Any] = {
            "control_tracking_plan": tracking_plan,
            "contract_analysis": contract_analysis,
            "dependencies": dependencies,
        }
        return lookup.get(name)

    store_calls: list[tuple[str, Any]] = []

    def fake_store_artifact(_session: Any, _job_id: Any, name: str, data: Any = None, text_data: Any = None) -> None:
        store_calls.append((name, data))
        artifact_store[name] = data

    create_job_calls: list[dict] = []

    def fake_create_job(_session: Any, request_dict: dict, initial_stage: Any = None) -> Any:
        create_job_calls.append(request_dict)
        return SimpleNamespace(id=uuid.uuid4(), company=None)

    def fake_build_control_snapshot(plan: Any, rpc_url: str) -> dict:
        return snapshot

    def fake_write_resolved_control_graph(
        analysis_path: Path,
        *,
        rpc_url: str = "",
        output_path: Path,
        max_depth: int = 6,
        workspace_prefix: str = "",
        refresh_snapshots: bool = False,
    ) -> Path:
        output_path.write_text(json.dumps(resolved_graph, indent=2) + "\n")
        return output_path

    monkeypatch.setattr("workers.resolution_worker.get_artifact", fake_get_artifact)
    monkeypatch.setattr("workers.resolution_worker.store_artifact", fake_store_artifact)
    monkeypatch.setattr("workers.resolution_worker.create_job", fake_create_job)
    monkeypatch.setattr("workers.resolution_worker.build_control_snapshot", fake_build_control_snapshot)
    monkeypatch.setattr("workers.resolution_worker.write_resolved_control_graph", fake_write_resolved_control_graph)
    monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

    return {
        "store_calls": store_calls,
        "create_job_calls": create_job_calls,
        "artifact_store": artifact_store,
    }


# ---------------------------------------------------------------------------
# 1. Happy-path process()
# ---------------------------------------------------------------------------


class TestProcessHappyPath:
    """Snapshot built, graph resolved, artifacts stored."""

    def test_stores_snapshot_and_graph(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        # Make scalar_one_or_none return None (no contract row) so DB writes are skipped
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job()

        ctx = _patch_all(monkeypatch)

        worker.process(session, cast(Any, job))

        stored_names = [name for name, _ in ctx["store_calls"]]
        assert "control_snapshot" in stored_names
        assert "resolved_control_graph" in stored_names

    def test_writes_controller_values_to_db(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        fake_contract = SimpleNamespace(id=42)
        session.execute.return_value.scalar_one_or_none.return_value = fake_contract

        snapshot = {
            "contract_address": TARGET_ADDRESS,
            "controller_values": {
                "owner_slot": {"value": "0xabc", "resolved_type": "address", "source": "storage", "details": {}},
            },
            "block_number": 100,
        }

        _patch_all(monkeypatch, snapshot=snapshot)
        worker.process(session, cast(Any, _job()))

        # session.add should have been called for the controller value
        add_calls = session.add.call_args_list
        assert len(add_calls) > 0

    def test_writes_graph_nodes_and_edges(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        fake_contract = SimpleNamespace(id=42)
        session.execute.return_value.scalar_one_or_none.return_value = fake_contract

        graph = _resolved_graph(
            nodes=[
                {
                    "address": CHILD_ADDRESS,
                    "node_type": "contract",
                    "resolved_type": "eoa",
                    "label": "child",
                    "depth": 1,
                    "analyzed": True,
                }
            ],
            edges=[{"from_id": TARGET_ADDRESS, "to_id": CHILD_ADDRESS, "relation": "owner", "label": "owns"}],
        )

        _patch_all(monkeypatch, resolved_graph=graph)
        worker.process(session, cast(Any, _job()))

        # Verify session.add was called for nodes and edges
        add_calls = session.add.call_args_list
        assert len(add_calls) >= 2  # at least one node + one edge


# ---------------------------------------------------------------------------
# 2. Proxy address override
# ---------------------------------------------------------------------------


class TestProxyAddressOverride:
    """When proxy_address is in request, tracking plan and analysis use it."""

    def test_proxy_overrides_contract_address(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        captured_plan: list[Any] = []
        original_tracking = _minimal_tracking_plan()

        def fake_build(plan: Any, rpc_url: str) -> dict:
            captured_plan.append(plan)
            return _minimal_snapshot()

        _patch_all(monkeypatch, tracking_plan=original_tracking)
        monkeypatch.setattr("workers.resolution_worker.build_control_snapshot", fake_build)

        job = _job(request={"rpc_url": "https://rpc.example", "proxy_address": PROXY_ADDRESS})
        worker.process(session, cast(Any, job))

        # The plan passed to build_control_snapshot should have proxy address
        assert captured_plan[0]["contract_address"] == PROXY_ADDRESS


# ---------------------------------------------------------------------------
# 3. _fetch_balances — ETH + tokens stored, price failure handled
# ---------------------------------------------------------------------------


class TestFetchBalancesHappyPath:
    """ETH + token balances stored, price fetch failure handled gracefully."""

    def test_stores_eth_and_tokens(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        fake_contract = SimpleNamespace(id=42)
        job = _job()

        monkeypatch.setattr("utils.etherscan.get_eth_balance", lambda addr: 1_000_000_000_000_000_000)  # 1 ETH
        monkeypatch.setattr("utils.etherscan.get_eth_price", lambda: 2000.0)
        monkeypatch.setattr(
            "utils.etherscan.get_token_balances",
            lambda addr: [
                {
                    "token_address": "0xtoken1",
                    "token_name": "USDC",
                    "token_symbol": "USDC",
                    "decimals": 6,
                    "balance": 1000000,
                    "price_usd": 1.0,
                    "usd_value": 1.0,
                }
            ],
        )
        monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

        cast(Any, worker)._fetch_balances(session, job, fake_contract)

        # 2 add calls: 1 ETH + 1 token
        assert session.add.call_count == 2
        session.commit.assert_called()

    def test_price_failure_still_stores_eth(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        fake_contract = SimpleNamespace(id=42)
        job = _job()

        monkeypatch.setattr("utils.etherscan.get_eth_balance", lambda addr: 1_000_000_000_000_000_000)
        monkeypatch.setattr("utils.etherscan.get_eth_price", MagicMock(side_effect=Exception("API down")))
        monkeypatch.setattr("utils.etherscan.get_token_balances", lambda addr: [])
        monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

        cast(Any, worker)._fetch_balances(session, job, fake_contract)

        # Should still add ETH balance even if price failed
        assert session.add.call_count == 1
        session.commit.assert_called()

    def test_balance_fetch_exception_returns_early(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        fake_contract = SimpleNamespace(id=42)
        job = _job()

        monkeypatch.setattr("utils.etherscan.get_eth_balance", MagicMock(side_effect=Exception("Network error")))
        monkeypatch.setattr("utils.etherscan.get_token_balances", lambda addr: [])
        monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

        cast(Any, worker)._fetch_balances(session, job, fake_contract)

        # No balances stored on exception
        session.add.assert_not_called()


# ---------------------------------------------------------------------------
# 4. _fetch_balances — no address / no contract_row returns early
# ---------------------------------------------------------------------------


class TestFetchBalancesEarlyReturn:
    """Returns early when address or contract_row is missing."""

    def test_no_address_returns_early(self) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        job = _job(address=None)
        fake_contract = SimpleNamespace(id=42)

        cast(Any, worker)._fetch_balances(session, job, fake_contract)
        session.add.assert_not_called()

    def test_no_contract_row_returns_early(self) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        job = _job()

        cast(Any, worker)._fetch_balances(session, job, None)
        session.add.assert_not_called()


# ---------------------------------------------------------------------------
# 5. _queue_discovered_contracts — creates child jobs, skips invalid
# ---------------------------------------------------------------------------


class TestQueueDiscoveredContracts:
    """Creates child jobs for valid contract nodes, skips invalid ones."""

    def test_creates_child_job_for_analyzed_contract(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        # No existing job
        session.execute.return_value.scalar_one_or_none.return_value = None

        create_calls: list[dict] = []

        def fake_create_job(_session: Any, request_dict: dict, initial_stage: Any = None) -> Any:
            create_calls.append(request_dict)
            return SimpleNamespace(id=uuid.uuid4(), company=None)

        monkeypatch.setattr("workers.resolution_worker.create_job", fake_create_job)

        graph = _resolved_graph(
            nodes=[
                {
                    "address": CHILD_ADDRESS,
                    "node_type": "contract",
                    "analyzed": True,
                    "contract_name": "ChildContract",
                },
            ]
        )

        job = _job()
        worker._queue_discovered_contracts(session, cast(Any, job), graph, "https://rpc.example")

        assert len(create_calls) == 1
        assert create_calls[0]["address"] == CHILD_ADDRESS
        assert create_calls[0]["name"] == "ChildContract"
        assert create_calls[0]["parent_job_id"] == str(job.id)

    def test_skips_non_contract_node(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        create_calls: list[dict] = []
        monkeypatch.setattr(
            "workers.resolution_worker.create_job",
            lambda _s, req, **kw: create_calls.append(req) or SimpleNamespace(id=uuid.uuid4(), company=None),
        )

        graph = _resolved_graph(
            nodes=[
                {"address": CHILD_ADDRESS, "node_type": "eoa", "analyzed": True},
            ]
        )

        worker._queue_discovered_contracts(session, cast(Any, _job()), graph, "https://rpc.example")
        assert len(create_calls) == 0

    def test_skips_non_analyzed_node(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        create_calls: list[dict] = []
        monkeypatch.setattr(
            "workers.resolution_worker.create_job",
            lambda _s, req, **kw: create_calls.append(req) or SimpleNamespace(id=uuid.uuid4(), company=None),
        )

        graph = _resolved_graph(
            nodes=[
                {"address": CHILD_ADDRESS, "node_type": "contract", "analyzed": False},
            ]
        )

        worker._queue_discovered_contracts(session, cast(Any, _job()), graph, "https://rpc.example")
        assert len(create_calls) == 0

    def test_skips_existing_job(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        # Existing job found
        session.execute.return_value.scalar_one_or_none.return_value = SimpleNamespace(id=uuid.uuid4())

        create_calls: list[dict] = []
        monkeypatch.setattr(
            "workers.resolution_worker.create_job",
            lambda _s, req, **kw: create_calls.append(req) or SimpleNamespace(id=uuid.uuid4(), company=None),
        )

        graph = _resolved_graph(
            nodes=[
                {"address": CHILD_ADDRESS, "node_type": "contract", "analyzed": True},
            ]
        )

        worker._queue_discovered_contracts(session, cast(Any, _job()), graph, "https://rpc.example")
        assert len(create_calls) == 0

    def test_skips_root_address(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        create_calls: list[dict] = []
        monkeypatch.setattr(
            "workers.resolution_worker.create_job",
            lambda _s, req, **kw: create_calls.append(req) or SimpleNamespace(id=uuid.uuid4(), company=None),
        )

        # Node address matches root
        graph = _resolved_graph(
            nodes=[
                {"address": TARGET_ADDRESS, "node_type": "contract", "analyzed": True},
            ]
        )

        worker._queue_discovered_contracts(session, cast(Any, _job()), graph, "https://rpc.example")
        assert len(create_calls) == 0

    def test_skips_invalid_address(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        create_calls: list[dict] = []
        monkeypatch.setattr(
            "workers.resolution_worker.create_job",
            lambda _s, req, **kw: create_calls.append(req) or SimpleNamespace(id=uuid.uuid4(), company=None),
        )

        graph = _resolved_graph(
            nodes=[
                {"address": "not-an-address", "node_type": "contract", "analyzed": True},
                {"address": "", "node_type": "contract", "analyzed": True},
                {"address": None, "node_type": "contract", "analyzed": True},
            ]
        )

        worker._queue_discovered_contracts(session, cast(Any, _job()), graph, "https://rpc.example")
        assert len(create_calls) == 0

    def test_propagates_chain_from_request(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        create_calls: list[dict] = []
        monkeypatch.setattr(
            "workers.resolution_worker.create_job",
            lambda _s, req, **kw: create_calls.append(req) or SimpleNamespace(id=uuid.uuid4(), company=None),
        )

        graph = _resolved_graph(nodes=[{"address": CHILD_ADDRESS, "node_type": "contract", "analyzed": True}])

        job = _job(request={"rpc_url": "https://rpc.example", "chain": "ethereum"})
        worker._queue_discovered_contracts(session, cast(Any, job), graph, "https://rpc.example")

        assert len(create_calls) == 1
        assert create_calls[0]["chain"] == "ethereum"


# ---------------------------------------------------------------------------
# 6. _queue_discovered_contracts — company inheritance via parent chain
# ---------------------------------------------------------------------------


class TestQueueDiscoveredContractsCompanyInheritance:
    """Company is inherited from parent job chain when not set on current job."""

    def test_inherits_company_from_parent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()

        parent_id = str(uuid.uuid4())
        parent_job = SimpleNamespace(
            id=parent_id,
            company="Acme Corp",
            request={},
        )

        # First call (select Job where address=...) returns None (no existing job)
        # session.get(Job, parent_id) returns parent_job

        def fake_execute(stmt):
            result = MagicMock()
            result.scalar_one_or_none.return_value = None
            return result

        session.execute = fake_execute
        session.get = lambda model, pid: parent_job if pid == parent_id else None

        create_calls: list[dict] = []
        child_ns = SimpleNamespace(id=uuid.uuid4(), company=None)

        def fake_create_job(_session: Any, request_dict: dict, initial_stage: Any = None) -> Any:
            create_calls.append(request_dict)
            return child_ns

        monkeypatch.setattr("workers.resolution_worker.create_job", fake_create_job)

        graph = _resolved_graph(nodes=[{"address": CHILD_ADDRESS, "node_type": "contract", "analyzed": True}])

        job = _job(company=None, request={"rpc_url": "https://rpc.example", "parent_job_id": parent_id})
        worker._queue_discovered_contracts(session, cast(Any, job), graph, "https://rpc.example")

        assert len(create_calls) == 1
        # Company should be set on child job
        assert child_ns.company == "Acme Corp"

    def test_uses_job_company_directly(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        create_calls: list[dict] = []
        child_ns = SimpleNamespace(id=uuid.uuid4(), company=None)

        def fake_create_job(_session: Any, request_dict: dict, initial_stage: Any = None) -> Any:
            create_calls.append(request_dict)
            return child_ns

        monkeypatch.setattr("workers.resolution_worker.create_job", fake_create_job)

        graph = _resolved_graph(nodes=[{"address": CHILD_ADDRESS, "node_type": "contract", "analyzed": True}])

        job = _job(company="Direct Corp")
        worker._queue_discovered_contracts(session, cast(Any, job), graph, "https://rpc.example")

        assert len(create_calls) == 1
        assert child_ns.company == "Direct Corp"


# ---------------------------------------------------------------------------
# 7. _run_upgrade_history — no dependencies artifact skips
# ---------------------------------------------------------------------------


class TestRunUpgradeHistoryNoDeps:
    """Skips when dependencies artifact is missing."""

    def test_skips_without_dependencies(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        job = _job()

        monkeypatch.setattr("workers.resolution_worker.get_artifact", lambda _s, _j, name: None)
        monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

        # Should not raise
        worker._run_upgrade_history(session, cast(Any, job), tmp_path)

        # store_artifact should NOT be called (no error stored)
        session.add.assert_not_called()


# ---------------------------------------------------------------------------
# 8. _run_upgrade_history — exception is non-fatal
# ---------------------------------------------------------------------------


class TestRunUpgradeHistoryNonFatal:
    """Exception during upgrade history does not propagate."""

    def test_exception_is_caught_and_logged(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        # Make the DB query raise after the artifact is loaded
        session.execute.return_value.scalar_one_or_none.side_effect = RuntimeError("db error")
        job = _job()

        uh_data = {
            "proxies": {
                "0xproxy": {
                    "proxy_address": "0xproxy",
                    "events": [
                        {
                            "event_type": "upgraded",
                            "block_number": 100,
                            "tx_hash": "0xtx",
                            "implementation": "0xnew",
                        }
                    ],
                }
            }
        }
        monkeypatch.setattr(
            "workers.resolution_worker.get_artifact",
            lambda _s, _j, name: uh_data if name == "upgrade_history" else None,
        )
        monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

        # Should NOT raise
        worker._run_upgrade_history(session, cast(Any, job), tmp_path)

    def test_upgrade_history_writes_events(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        # Mock stands in for BOTH the subject lookup (first scalar_one_or_none
        # call) and the per-proxy lookup (second call) — the worker reads
        # .chain on the subject to build its proxy-chain filter.
        fake_contract = SimpleNamespace(id=99, address="0xproxy", chain=None)
        session.execute.return_value.scalar_one_or_none.return_value = fake_contract
        job = _job()

        uh_data = {
            "proxies": {
                "0xproxy": {
                    "proxy_address": "0xproxy",
                    "events": [
                        {
                            "event_type": "upgraded",
                            "block_number": 100,
                            "tx_hash": "0xtx",
                            "implementation": "0xnew",
                        }
                    ],
                }
            }
        }
        monkeypatch.setattr(
            "workers.resolution_worker.get_artifact",
            lambda _s, _j, name: uh_data if name == "upgrade_history" else None,
        )
        monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

        worker._run_upgrade_history(session, cast(Any, job), tmp_path)

        # Should have added an UpgradeEvent
        assert session.add.call_count == 1
        session.commit.assert_called()


# ---------------------------------------------------------------------------
# 9. Missing artifacts raise RuntimeError
# ---------------------------------------------------------------------------


class TestMissingArtifactsRaise:
    """process() raises RuntimeError when required artifacts are missing."""

    def test_missing_tracking_plan_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        job = _job()

        monkeypatch.setattr("workers.resolution_worker.get_artifact", lambda _s, _j, name: None)
        monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

        with pytest.raises(RuntimeError, match="control_tracking_plan artifact not found"):
            worker.process(session, cast(Any, job))

    def test_missing_contract_analysis_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        job = _job()

        def fake_get_artifact(_s: Any, _j: Any, name: str) -> Any:
            if name == "control_tracking_plan":
                return _minimal_tracking_plan()
            return None

        monkeypatch.setattr("workers.resolution_worker.get_artifact", fake_get_artifact)
        monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

        with pytest.raises(RuntimeError, match="contract_analysis artifact not found"):
            worker.process(session, cast(Any, job))

    def test_non_dict_tracking_plan_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        job = _job()

        monkeypatch.setattr(
            "workers.resolution_worker.get_artifact",
            lambda _s, _j, name: "not a dict" if name == "control_tracking_plan" else None,
        )
        monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

        with pytest.raises(RuntimeError, match="control_tracking_plan artifact not found"):
            worker.process(session, cast(Any, job))


# ---------------------------------------------------------------------------
# 10. Zero ETH balance skips ETH row
# ---------------------------------------------------------------------------


class TestFetchBalancesZeroEth:
    """Zero ETH balance does not add an ETH row."""

    def test_zero_eth_no_row(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        fake_contract = SimpleNamespace(id=42)
        job = _job()

        monkeypatch.setattr("utils.etherscan.get_eth_balance", lambda addr: 0)
        monkeypatch.setattr("utils.etherscan.get_token_balances", lambda addr: [])
        monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

        cast(Any, worker)._fetch_balances(session, job, fake_contract)

        session.add.assert_not_called()
        session.commit.assert_called()


# ---------------------------------------------------------------------------
# 11. Proxy address used in _fetch_balances
# ---------------------------------------------------------------------------


class TestFetchBalancesProxyAddress:
    """_fetch_balances uses proxy_address when present in request."""

    def test_uses_proxy_address(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        fake_contract = SimpleNamespace(id=42)

        captured_addrs: list[str] = []

        def fake_get_eth(addr: str) -> int:
            captured_addrs.append(addr)
            return 0

        monkeypatch.setattr("utils.etherscan.get_eth_balance", fake_get_eth)
        monkeypatch.setattr("utils.etherscan.get_token_balances", lambda addr: [])
        monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

        job = _job(request={"proxy_address": PROXY_ADDRESS})
        cast(Any, worker)._fetch_balances(session, job, fake_contract)

        assert captured_addrs[0] == PROXY_ADDRESS


# ---------------------------------------------------------------------------
# 12. _queue_discovered_contracts — parent chain walk edge cases
# ---------------------------------------------------------------------------


class TestQueueDiscoveredContractsParentChainEdgeCases:
    """Edge cases in the parent chain walk for company inheritance."""

    def test_parent_not_found_breaks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When parent_job_id references a nonexistent job, walk stops."""
        worker = ResolutionWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        session.get = lambda model, pid: None  # parent not found

        create_calls: list[dict] = []
        monkeypatch.setattr(
            "workers.resolution_worker.create_job",
            lambda _s, req, **kw: create_calls.append(req) or SimpleNamespace(id=uuid.uuid4(), company=None),
        )

        graph = _resolved_graph(nodes=[{"address": CHILD_ADDRESS, "node_type": "contract", "analyzed": True}])
        job = _job(company=None, request={"rpc_url": "https://rpc.example", "parent_job_id": str(uuid.uuid4())})
        worker._queue_discovered_contracts(session, cast(Any, job), graph, "https://rpc.example")

        # Should still create the job, just no company
        assert len(create_calls) == 1

    def test_multi_level_parent_walk(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Walk through grandparent to find company."""
        worker = ResolutionWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        grandparent_id = str(uuid.uuid4())
        parent_id = str(uuid.uuid4())

        grandparent_job = SimpleNamespace(id=grandparent_id, company="GrandCorp", request={})
        parent_job = SimpleNamespace(
            id=parent_id,
            company=None,
            request={"parent_job_id": grandparent_id},
        )

        def fake_get(model: Any, pid: str) -> Any:
            if pid == parent_id:
                return parent_job
            if pid == grandparent_id:
                return grandparent_job
            return None

        session.get = fake_get

        create_calls: list[dict] = []
        child_ns = SimpleNamespace(id=uuid.uuid4(), company=None)

        def fake_create_job(_session: Any, request_dict: dict, initial_stage: Any = None) -> Any:
            create_calls.append(request_dict)
            return child_ns

        monkeypatch.setattr("workers.resolution_worker.create_job", fake_create_job)

        graph = _resolved_graph(nodes=[{"address": CHILD_ADDRESS, "node_type": "contract", "analyzed": True}])
        job = _job(company=None, request={"rpc_url": "https://rpc.example", "parent_job_id": parent_id})
        worker._queue_discovered_contracts(session, cast(Any, job), graph, "https://rpc.example")

        assert len(create_calls) == 1
        assert child_ns.company == "GrandCorp"


# ---------------------------------------------------------------------------
# 13. resolved_graph_path does not exist
# ---------------------------------------------------------------------------


class TestResolvedGraphPathMissing:
    """When write_resolved_control_graph returns a path that doesn't exist."""

    def test_graph_not_written_skips_store(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job()

        def fake_write_resolved_graph(
            analysis_path: Path,
            *,
            rpc_url: str = "",
            output_path: Path,
            max_depth: int = 6,
            workspace_prefix: str = "",
            refresh_snapshots: bool = False,
        ) -> Path:
            # Do NOT write the file — simulate no graph produced
            return output_path

        ctx = _patch_all(monkeypatch)
        monkeypatch.setattr("workers.resolution_worker.write_resolved_control_graph", fake_write_resolved_graph)

        worker.process(session, cast(Any, job))

        stored_names = [name for name, _ in ctx["store_calls"]]
        assert "control_snapshot" in stored_names
        assert "resolved_control_graph" not in stored_names


# ---------------------------------------------------------------------------
# 14. _run_upgrade_history — path is None
# ---------------------------------------------------------------------------


class TestRunUpgradeHistoryPathNone:
    """No upgrade_history artifact — nothing to write."""

    def test_returns_none_gracefully(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        worker = ResolutionWorker()
        session = MagicMock()
        job = _job()

        monkeypatch.setattr(
            "workers.resolution_worker.get_artifact",
            lambda _s, _j, name: None,
        )
        monkeypatch.setattr("workers.base.update_job_detail", lambda *a, **kw: None)

        # Should not raise
        worker._run_upgrade_history(session, cast(Any, job), tmp_path)
        session.add.assert_not_called()
