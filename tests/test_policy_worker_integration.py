"""Integration tests for PolicyWorker — _resolve_authority() and process() flows."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from workers.policy_worker import PolicyWorker

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
AUTH_ADDRESS = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TARGET_ADDRESS = "0x1111111111111111111111111111111111111111"


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


def _minimal_snapshot(controller_values: dict | None = None) -> dict:
    """Return a minimal control_snapshot dict."""
    return {
        "contract_address": TARGET_ADDRESS,
        "controller_values": controller_values or {},
    }


def _graph_with_nodes(nodes: list[dict]) -> dict:
    return {"nodes": nodes, "edges": []}


def _minimal_contract_analysis() -> dict:
    return {
        "contract_address": TARGET_ADDRESS,
        "contract_name": "TestContract",
        "functions": [],
    }


# ---------------------------------------------------------------------------
# _resolve_authority tests (now takes dicts directly)
# ---------------------------------------------------------------------------


class TestResolveAuthorityNoAuthority:
    """controller_values has keys but none end with ':authority'."""

    def test_returns_no_authority(self) -> None:
        worker = PolicyWorker()

        snapshot = _minimal_snapshot({"owner_slot:admin": {"value": "0xbbb"}})
        graph = _graph_with_nodes([])

        result = worker._resolve_authority(graph, snapshot)

        assert result["principal_resolution"]["status"] == "no_authority"


class TestResolveAuthorityZeroAddress:
    """Authority exists but is the zero address."""

    def test_returns_no_authority(self) -> None:
        worker = PolicyWorker()

        snapshot = _minimal_snapshot({"state_variable:authority": {"value": ZERO_ADDRESS}})
        graph = _graph_with_nodes([])

        result = worker._resolve_authority(graph, snapshot)

        assert result["principal_resolution"]["status"] == "no_authority"
        assert "non-zero" in result["principal_resolution"]["reason"].lower()


class TestResolveAuthorityNoSnapshot:
    """Authority address is in the graph but its node has no snapshot artifact."""

    def test_returns_no_authority_snapshot(self) -> None:
        worker = PolicyWorker()

        snapshot = _minimal_snapshot({"state_variable:authority": {"value": AUTH_ADDRESS}})
        graph = _graph_with_nodes([{"address": AUTH_ADDRESS, "artifacts": {}}])

        result = worker._resolve_authority(graph, snapshot)

        assert result["principal_resolution"]["status"] == "no_authority_snapshot"


class TestResolveAuthorityWithSnapshot:
    """Authority found with a real snapshot file, no policy tracking."""

    def test_returns_authority_path_and_missing_policy(self, tmp_path: Path) -> None:
        worker = PolicyWorker()

        # Create the authority project directory with a snapshot file
        authority_dir = tmp_path / "authority_project"
        authority_dir.mkdir()
        authority_snapshot = authority_dir / "control_snapshot.json"
        authority_snapshot.write_text('{"contract_address": "' + AUTH_ADDRESS + '", "controller_values": {}}\n')

        snapshot = _minimal_snapshot({"state_variable:authority": {"value": AUTH_ADDRESS}})
        graph = _graph_with_nodes(
            [
                {
                    "address": AUTH_ADDRESS,
                    "artifacts": {"snapshot": str(authority_snapshot)},
                }
            ]
        )

        result = worker._resolve_authority(graph, snapshot)

        assert result["authority_snapshot_path"] == authority_snapshot
        assert result["principal_resolution"]["status"] == "missing_policy_state"


# ---------------------------------------------------------------------------
# process() integration tests
# ---------------------------------------------------------------------------


class TestProcessStoresAllArtifacts:
    """Full process() stores effective_permissions, resolved_control_graph, and principal_labels."""

    def test_all_three_artifacts_stored(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = PolicyWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job()

        contract_analysis = _minimal_contract_analysis()
        control_snapshot = _minimal_snapshot({"some_key:admin": {"value": "0xbbb"}})
        resolved_graph = _graph_with_nodes([])

        def fake_get_artifact(_session: Any, _job_id: Any, name: str) -> Any:
            return {
                "contract_analysis": contract_analysis,
                "control_snapshot": control_snapshot,
                "resolved_control_graph": resolved_graph,
            }.get(name)

        store_calls: list[tuple[str, Any]] = []

        def fake_store_artifact(
            _session: Any,
            _job_id: Any,
            name: str,
            data: Any = None,
            text_data: Any = None,
        ) -> None:
            store_calls.append((name, data))

        monkeypatch.setattr("workers.policy_worker.get_artifact", fake_get_artifact)
        monkeypatch.setattr("workers.policy_worker.store_artifact", fake_store_artifact)
        monkeypatch.setattr(
            "workers.policy_worker.build_effective_permissions",
            lambda *a, **kw: {"schema_version": "1", "functions": []},
        )
        monkeypatch.setattr(
            "workers.policy_worker.resolve_control_graph",
            lambda **kw: {"nodes": [], "edges": [], "refreshed": True},
        )
        monkeypatch.setattr(
            "workers.policy_worker.build_principal_labels",
            lambda *a, **kw: {"principals": []},
        )

        worker.process(session, cast(Any, job))

        stored_names = [name for name, _ in store_calls]
        assert "effective_permissions" in stored_names
        assert "resolved_control_graph" in stored_names
        assert "principal_labels" in stored_names


class TestGraphRefreshAfterEffectivePermissions:
    """resolve_control_graph refresh runs AFTER build_effective_permissions."""

    def test_refresh_runs_after_effective_permissions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = PolicyWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job()

        contract_analysis = _minimal_contract_analysis()
        control_snapshot = _minimal_snapshot()
        resolved_graph = _graph_with_nodes([])

        def fake_get_artifact(_session: Any, _job_id: Any, name: str) -> Any:
            return {
                "contract_analysis": contract_analysis,
                "control_snapshot": control_snapshot,
                "resolved_control_graph": resolved_graph,
            }.get(name)

        call_order: list[str] = []

        def fake_build_ep(*args: Any, **kwargs: Any) -> dict:
            call_order.append("effective_permissions")
            return {"schema_version": "1", "functions": []}

        def fake_resolve_graph(**kwargs: Any) -> dict:
            call_order.append("resolved_control_graph")
            return {"nodes": [], "edges": [], "refreshed": True}

        def fake_build_labels(*args: Any, **kwargs: Any) -> dict:
            call_order.append("principal_labels")
            return {"principals": []}

        monkeypatch.setattr("workers.policy_worker.get_artifact", fake_get_artifact)
        monkeypatch.setattr("workers.policy_worker.store_artifact", lambda *a, **kw: None)
        monkeypatch.setattr("workers.policy_worker.build_effective_permissions", fake_build_ep)
        monkeypatch.setattr("workers.policy_worker.resolve_control_graph", fake_resolve_graph)
        monkeypatch.setattr("workers.policy_worker.build_principal_labels", fake_build_labels)

        worker.process(session, cast(Any, job))

        ep_idx = call_order.index("effective_permissions")
        rg_idx = call_order.index("resolved_control_graph")
        assert ep_idx < rg_idx, (
            f"effective_permissions (index {ep_idx}) must be called "
            f"before resolved_control_graph (index {rg_idx}); "
            f"actual order: {call_order}"
        )


class TestCrossContractEnrichmentArtifactSync:
    """Cross-contract enrichment rewrites the effective_permissions artifact."""

    def test_enrichment_rewrites_effective_permissions_artifact(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = PolicyWorker()
        session = MagicMock()
        job = _job()

        contract_analysis = _minimal_contract_analysis()
        control_snapshot = _minimal_snapshot({"state_variable:token": {"value": AUTH_ADDRESS}})
        resolved_graph = _graph_with_nodes([])

        def fake_get_artifact(_session: Any, _job_id: Any, name: str) -> Any:
            return {
                "contract_analysis": contract_analysis,
                "control_snapshot": control_snapshot,
                "resolved_control_graph": resolved_graph,
            }.get(name)

        store_calls: list[tuple[str, Any]] = []

        def fake_store_artifact(
            _session: Any,
            _job_id: Any,
            name: str,
            data: Any = None,
            text_data: Any = None,
        ) -> None:
            import json as _json

            store_calls.append((name, _json.loads(_json.dumps(data)) if data is not None else text_data))

        contract_row = MagicMock()
        contract_row.id = 1
        session.execute.return_value.scalar_one_or_none.return_value = contract_row

        monkeypatch.setattr("workers.policy_worker.get_artifact", fake_get_artifact)
        monkeypatch.setattr("workers.policy_worker.store_artifact", fake_store_artifact)
        monkeypatch.setattr(
            "workers.policy_worker.build_effective_permissions",
            lambda *a, **kw: {
                "schema_version": "1",
                "functions": [
                    {
                        "function": "mintRewards()",
                        "effect_labels": ["role_management"],
                        "controllers": [],
                        "authority_roles": [],
                        "direct_owner": None,
                    }
                ],
            },
        )
        monkeypatch.setattr(
            "workers.policy_worker.resolve_control_graph",
            lambda **kw: {"nodes": [], "edges": []},
        )
        monkeypatch.setattr(
            "workers.policy_worker.build_principal_labels",
            lambda *a, **kw: {"principals": []},
        )
        monkeypatch.setattr(
            PolicyWorker,
            "_enrich_cross_contract",
            lambda self, session, job, contract_analysis, control_snapshot: {"mintRewards()": ["mint"]},
        )

        worker.process(session, cast(Any, job))

        effective_payloads = [data for name, data in store_calls if name == "effective_permissions"]
        assert len(effective_payloads) == 2
        assert effective_payloads[-1]["functions"][0]["effect_labels"] == ["mint", "role_management"]
