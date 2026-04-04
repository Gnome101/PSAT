"""Integration tests for PolicyWorker — _resolve_authority() and process() flows."""

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
        "request": {"rpc_url": "https://rpc.example"},
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _write_json(path: Path, data: Any) -> Path:
    path.write_text(json.dumps(data, indent=2) + "\n")
    return path


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
# _resolve_authority tests
# ---------------------------------------------------------------------------


class TestResolveAuthorityNoAuthority:
    """Case 1: controller_values has keys but none end with ':authority'."""

    def test_returns_no_authority(self, tmp_path: Path) -> None:
        worker = PolicyWorker()

        snapshot_path = _write_json(
            tmp_path / "control_snapshot.json",
            _minimal_snapshot({"owner_slot:admin": {"value": "0xbbb"}}),
        )
        graph_path = _write_json(
            tmp_path / "resolved_control_graph.json",
            _graph_with_nodes([]),
        )

        result = worker._resolve_authority(tmp_path, graph_path, snapshot_path, _minimal_contract_analysis())

        assert result["principal_resolution"]["status"] == "no_authority"


class TestResolveAuthorityZeroAddress:
    """Case 2: authority exists but is the zero address."""

    def test_returns_no_authority(self, tmp_path: Path) -> None:
        worker = PolicyWorker()

        snapshot_path = _write_json(
            tmp_path / "control_snapshot.json",
            _minimal_snapshot({"state_variable:authority": {"value": ZERO_ADDRESS}}),
        )
        graph_path = _write_json(
            tmp_path / "resolved_control_graph.json",
            _graph_with_nodes([]),
        )

        result = worker._resolve_authority(tmp_path, graph_path, snapshot_path, _minimal_contract_analysis())

        assert result["principal_resolution"]["status"] == "no_authority"
        assert "non-zero" in result["principal_resolution"]["reason"].lower()


class TestResolveAuthorityNoSnapshot:
    """Case 3: authority address found in graph but node has no snapshot artifact."""

    def test_returns_no_authority_snapshot(self, tmp_path: Path) -> None:
        worker = PolicyWorker()

        snapshot_path = _write_json(
            tmp_path / "control_snapshot.json",
            _minimal_snapshot({"state_variable:authority": {"value": AUTH_ADDRESS}}),
        )
        graph_path = _write_json(
            tmp_path / "resolved_control_graph.json",
            _graph_with_nodes([{"address": AUTH_ADDRESS, "artifacts": {}}]),
        )

        result = worker._resolve_authority(tmp_path, graph_path, snapshot_path, _minimal_contract_analysis())

        assert result["principal_resolution"]["status"] == "no_authority_snapshot"


class TestResolveAuthorityWithSnapshot:
    """Case 4: authority found with a real snapshot file, no policy tracking."""

    def test_returns_authority_path_and_missing_policy(self, tmp_path: Path) -> None:
        worker = PolicyWorker()

        # Create the authority project directory with a snapshot file
        authority_dir = tmp_path / "authority_project"
        authority_dir.mkdir()
        authority_snapshot = _write_json(
            authority_dir / "control_snapshot.json",
            {"contract_address": AUTH_ADDRESS, "controller_values": {}},
        )

        snapshot_path = _write_json(
            tmp_path / "control_snapshot.json",
            _minimal_snapshot({"state_variable:authority": {"value": AUTH_ADDRESS}}),
        )
        graph_path = _write_json(
            tmp_path / "resolved_control_graph.json",
            _graph_with_nodes(
                [
                    {
                        "address": AUTH_ADDRESS,
                        "artifacts": {"snapshot": str(authority_snapshot)},
                    }
                ]
            ),
        )

        result = worker._resolve_authority(tmp_path, graph_path, snapshot_path, _minimal_contract_analysis())

        assert result["authority_snapshot_path"] == authority_snapshot
        assert result["principal_resolution"]["status"] == "missing_policy_state"


# ---------------------------------------------------------------------------
# process() integration tests
# ---------------------------------------------------------------------------


class TestProcessStoresAllArtifacts:
    """Case 5: Full process() stores effective_permissions, resolved_control_graph,
    and principal_labels artifacts."""

    def test_all_three_artifacts_stored(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = PolicyWorker()
        session = MagicMock()
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

        # Mock the three writer functions so they create output files
        def fake_write_effective_permissions(
            analysis_path: Path,
            *,
            target_snapshot_path: Any = None,
            authority_snapshot_path: Any = None,
            policy_state_path: Any = None,
            output_path: Path,
            principal_resolution: Any = None,
        ) -> Path:
            _write_json(output_path, {"schema_version": "1", "functions": []})
            return output_path

        def fake_write_resolved_control_graph(
            analysis_path: Path,
            *,
            rpc_url: str = "",
            output_path: Path,
            max_depth: int = 6,
            workspace_prefix: str = "",
            refresh_snapshots: bool = False,
        ) -> Path:
            _write_json(output_path, {"nodes": [], "edges": [], "refreshed": True})
            return output_path

        def fake_write_principal_labels(
            effective_permissions_path: Path,
            *,
            resolved_control_graph_path: Any = None,
            rpc_url: str = "",
            output_path: Path,
        ) -> Path:
            _write_json(output_path, {"labels": []})
            return output_path

        monkeypatch.setattr(
            "services.policy.write_effective_permissions_from_files",
            fake_write_effective_permissions,
        )
        monkeypatch.setattr(
            "services.resolution.recursive.write_resolved_control_graph",
            fake_write_resolved_control_graph,
        )
        monkeypatch.setattr(
            "services.policy.write_principal_labels_from_files",
            fake_write_principal_labels,
        )

        worker.process(session, cast(Any, job))

        stored_names = [name for name, _ in store_calls]
        assert "effective_permissions" in stored_names
        assert "resolved_control_graph" in stored_names
        assert "principal_labels" in stored_names


class TestGraphRefreshAfterEffectivePermissions:
    """Case 6: write_resolved_control_graph is called AFTER
    write_effective_permissions_from_files, so the effective_permissions file
    exists on disk when the graph refresh runs."""

    def test_refresh_sees_effective_permissions_file(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = PolicyWorker()
        session = MagicMock()
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

        monkeypatch.setattr("workers.policy_worker.get_artifact", fake_get_artifact)
        monkeypatch.setattr(
            "workers.policy_worker.store_artifact",
            lambda *args, **kwargs: None,
        )

        call_order: list[str] = []
        effective_permissions_file_existed_during_refresh: list[bool] = []

        def fake_write_effective_permissions(
            analysis_path: Path,
            *,
            target_snapshot_path: Any = None,
            authority_snapshot_path: Any = None,
            policy_state_path: Any = None,
            output_path: Path,
            principal_resolution: Any = None,
        ) -> Path:
            call_order.append("effective_permissions")
            _write_json(output_path, {"schema_version": "1", "functions": []})
            return output_path

        def fake_write_resolved_control_graph(
            analysis_path: Path,
            *,
            rpc_url: str = "",
            output_path: Path,
            max_depth: int = 6,
            workspace_prefix: str = "",
            refresh_snapshots: bool = False,
        ) -> Path:
            call_order.append("resolved_control_graph")
            # Check that effective_permissions.json already exists in the same directory
            ep_path = output_path.parent / "effective_permissions.json"
            effective_permissions_file_existed_during_refresh.append(ep_path.exists())
            _write_json(output_path, {"nodes": [], "edges": [], "refreshed": True})
            return output_path

        def fake_write_principal_labels(
            effective_permissions_path: Path,
            *,
            resolved_control_graph_path: Any = None,
            rpc_url: str = "",
            output_path: Path,
        ) -> Path:
            call_order.append("principal_labels")
            _write_json(output_path, {"labels": []})
            return output_path

        monkeypatch.setattr(
            "services.policy.write_effective_permissions_from_files",
            fake_write_effective_permissions,
        )
        monkeypatch.setattr(
            "services.resolution.recursive.write_resolved_control_graph",
            fake_write_resolved_control_graph,
        )
        monkeypatch.setattr(
            "services.policy.write_principal_labels_from_files",
            fake_write_principal_labels,
        )

        worker.process(session, cast(Any, job))

        # Verify ordering: effective_permissions BEFORE resolved_control_graph
        ep_idx = call_order.index("effective_permissions")
        rg_idx = call_order.index("resolved_control_graph")
        assert ep_idx < rg_idx, (
            f"effective_permissions (index {ep_idx}) must be called "
            f"before resolved_control_graph (index {rg_idx}); "
            f"actual order: {call_order}"
        )

        # Verify the file was on disk when the refresh ran
        assert effective_permissions_file_existed_during_refresh == [True], (
            "effective_permissions.json must exist on disk when write_resolved_control_graph is invoked"
        )
