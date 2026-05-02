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


def _authority_bundle(snapshot: dict | None = None, policy_tracking: bool = False) -> dict:
    return {
        "analysis": {
            "subject": {"address": AUTH_ADDRESS, "name": "Authority"},
            "policy_tracking": policy_tracking,
        },
        "tracking_plan": {
            "schema_version": "0.1",
            "contract_address": AUTH_ADDRESS,
            "contract_name": "Authority",
            "tracking_strategy": "event_first_with_polling_fallback",
            "tracked_controllers": [],
            "tracked_policies": [],
        },
        "snapshot": snapshot or {"contract_address": AUTH_ADDRESS, "controller_values": {}},
    }


# ---------------------------------------------------------------------------
# _resolve_authority tests (now takes session, job, graph, snapshot, nested)
# ---------------------------------------------------------------------------


class TestResolveAuthorityNoAuthority:
    """controller_values has keys but none end with ':authority'."""

    def test_returns_no_authority(self) -> None:
        worker = PolicyWorker()
        session = MagicMock()
        job = _job()

        snapshot = _minimal_snapshot({"owner_slot:admin": {"value": "0xbbb"}})
        graph = _graph_with_nodes([])

        result = worker._resolve_authority(session, cast(Any, job), graph, snapshot, {})

        assert result["principal_resolution"]["status"] == "no_authority"


class TestResolveAuthorityZeroAddress:
    """Authority exists but is the zero address."""

    def test_returns_no_authority(self) -> None:
        worker = PolicyWorker()
        session = MagicMock()
        job = _job()

        snapshot = _minimal_snapshot({"state_variable:authority": {"value": ZERO_ADDRESS}})
        graph = _graph_with_nodes([])

        result = worker._resolve_authority(session, cast(Any, job), graph, snapshot, {})

        assert result["principal_resolution"]["status"] == "no_authority"
        assert "non-zero" in result["principal_resolution"]["reason"].lower()


class TestResolveAuthorityNoSnapshot:
    """Authority address is in the graph but its bundle is missing from nested artifacts."""

    def test_returns_no_authority_snapshot(self) -> None:
        worker = PolicyWorker()
        session = MagicMock()
        job = _job()

        snapshot = _minimal_snapshot({"state_variable:authority": {"value": AUTH_ADDRESS}})
        graph = _graph_with_nodes([{"address": AUTH_ADDRESS, "artifacts": {}}])

        result = worker._resolve_authority(session, cast(Any, job), graph, snapshot, {})

        assert result["principal_resolution"]["status"] == "no_authority_snapshot"


class TestResolveAuthorityWithSnapshot:
    """Authority found with a real snapshot bundle, no policy tracking."""

    def test_returns_authority_snapshot_and_missing_policy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = PolicyWorker()
        session = MagicMock()
        job = _job()

        snapshot = _minimal_snapshot({"state_variable:authority": {"value": AUTH_ADDRESS}})
        graph = _graph_with_nodes([{"address": AUTH_ADDRESS, "artifacts": {"data_key": f"recursive:{AUTH_ADDRESS}"}}])
        nested = cast(Any, {AUTH_ADDRESS: _authority_bundle(policy_tracking=False)})

        # No policy_state artifact stored for authority.
        monkeypatch.setattr("workers.policy_worker.get_artifact", lambda *_args, **_kwargs: None)

        result = worker._resolve_authority(session, cast(Any, job), graph, snapshot, nested)

        assert result["authority_snapshot"] == nested[AUTH_ADDRESS]["snapshot"]
        assert result["principal_resolution"]["status"] == "missing_policy_state"


class TestResolveAuthorityCachedPolicyState:
    """Authority bundle declares policy_tracking and a cached policy_state exists — reuse it."""

    def test_returns_cached_policy_state_without_backfill(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = PolicyWorker()
        session = MagicMock()
        job = _job()

        snapshot = _minimal_snapshot({"state_variable:authority": {"value": AUTH_ADDRESS}})
        graph = _graph_with_nodes([{"address": AUTH_ADDRESS, "artifacts": {"data_key": f"recursive:{AUTH_ADDRESS}"}}])
        nested = cast(Any, {AUTH_ADDRESS: _authority_bundle(policy_tracking=True)})

        cached_policy_state = {"schema_version": "0.1", "event_count": 7, "user_roles": []}

        monkeypatch.setattr(
            "workers.policy_worker.get_artifact",
            lambda _session, _job_id, _name: cached_policy_state,
        )
        # If the backfill path is (incorrectly) taken, force a loud failure.
        monkeypatch.setattr(
            "workers.policy_worker.run_hypersync_policy_backfill",
            lambda *_a, **_kw: pytest.fail("backfill should not run when cached policy_state exists"),
        )

        result = worker._resolve_authority(session, cast(Any, job), graph, snapshot, nested)

        assert result["policy_state"] is cached_policy_state
        assert result["authority_snapshot"] == nested[AUTH_ADDRESS]["snapshot"]
        assert result["principal_resolution"]["status"] == "complete"
        assert "Existing" in result["principal_resolution"]["reason"]


class TestResolveAuthorityHypersyncBackfill:
    """Authority has policy_tracking, no cached state, ENVIO_API_TOKEN set — run backfill + persist."""

    def test_runs_backfill_and_persists_events_and_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = PolicyWorker()
        session = MagicMock()
        job = _job()

        snapshot = _minimal_snapshot({"state_variable:authority": {"value": AUTH_ADDRESS}})
        graph = _graph_with_nodes([{"address": AUTH_ADDRESS, "artifacts": {"data_key": f"recursive:{AUTH_ADDRESS}"}}])
        nested = cast(Any, {AUTH_ADDRESS: _authority_bundle(policy_tracking=True)})

        # No cached policy_state — forces the backfill path.
        monkeypatch.setattr("workers.policy_worker.get_artifact", lambda *_a, **_kw: None)
        monkeypatch.setenv("ENVIO_API_TOKEN", "test-token")

        backfill_events = [{"schema_version": "0.1", "block_number": 42}]
        backfill_state = {"schema_version": "0.1", "event_count": 1, "source": "hypersync"}
        backfill_calls: list[Any] = []

        def fake_backfill(plan: Any, **_kw: Any) -> tuple[list, dict]:
            backfill_calls.append(plan)
            return backfill_events, backfill_state

        monkeypatch.setattr("workers.policy_worker.run_hypersync_policy_backfill", fake_backfill)

        store_calls: list[tuple[str, Any]] = []

        def fake_store(_session: Any, _job_id: Any, name: str, data: Any = None, text_data: Any = None) -> None:
            store_calls.append((name, data))

        monkeypatch.setattr("workers.policy_worker.store_artifact", fake_store)

        result = worker._resolve_authority(session, cast(Any, job), graph, snapshot, nested)

        # Backfill ran exactly once with the authority's tracking plan.
        assert len(backfill_calls) == 1
        assert backfill_calls[0] == nested[AUTH_ADDRESS]["tracking_plan"]

        # Both events and state were persisted under the authority's address-scoped keys.
        stored = dict(store_calls)
        assert stored[f"recursive.{AUTH_ADDRESS}.policy_event_history"] == backfill_events
        assert stored[f"recursive.{AUTH_ADDRESS}.policy_state"] == backfill_state

        assert result["policy_state"] is backfill_state
        assert result["authority_snapshot"] == nested[AUTH_ADDRESS]["snapshot"]
        assert result["principal_resolution"]["status"] == "complete"
        assert "backfill" in result["principal_resolution"]["reason"].lower()


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
        tracking_plan = {"schema_version": "0.1", "contract_address": TARGET_ADDRESS, "contract_name": "TestContract"}

        def fake_get_artifact(_session: Any, _job_id: Any, name: str) -> Any:
            return {
                "contract_analysis": contract_analysis,
                "control_snapshot": control_snapshot,
                "resolved_control_graph": resolved_graph,
                "control_tracking_plan": tracking_plan,
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
        monkeypatch.setattr("workers.policy_worker._load_nested_artifacts", lambda *_a, **_kw: {})
        monkeypatch.setattr(
            "workers.policy_worker.build_effective_permissions",
            lambda *a, **kw: {"schema_version": "1", "functions": []},
        )
        monkeypatch.setattr(
            "workers.policy_worker.resolve_control_graph",
            lambda **kw: ({"nodes": [], "edges": [], "refreshed": True}, {}),
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
        tracking_plan = {"schema_version": "0.1", "contract_address": TARGET_ADDRESS, "contract_name": "TestContract"}

        def fake_get_artifact(_session: Any, _job_id: Any, name: str) -> Any:
            return {
                "contract_analysis": contract_analysis,
                "control_snapshot": control_snapshot,
                "resolved_control_graph": resolved_graph,
                "control_tracking_plan": tracking_plan,
            }.get(name)

        call_order: list[str] = []

        def fake_build_ep(*args: Any, **kwargs: Any) -> dict:
            call_order.append("effective_permissions")
            return {"schema_version": "1", "functions": []}

        def fake_resolve_graph(**kwargs: Any) -> tuple[dict, dict]:
            call_order.append("resolved_control_graph")
            return {"nodes": [], "edges": [], "refreshed": True}, {}

        def fake_build_labels(*args: Any, **kwargs: Any) -> dict:
            call_order.append("principal_labels")
            return {"principals": []}

        monkeypatch.setattr("workers.policy_worker.get_artifact", fake_get_artifact)
        monkeypatch.setattr("workers.policy_worker.store_artifact", lambda *a, **kw: None)
        monkeypatch.setattr("workers.policy_worker._load_nested_artifacts", lambda *_a, **_kw: {})
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
        tracking_plan = {"schema_version": "0.1", "contract_address": TARGET_ADDRESS, "contract_name": "TestContract"}

        def fake_get_artifact(_session: Any, _job_id: Any, name: str) -> Any:
            return {
                "contract_analysis": contract_analysis,
                "control_snapshot": control_snapshot,
                "resolved_control_graph": resolved_graph,
                "control_tracking_plan": tracking_plan,
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
        monkeypatch.setattr("workers.policy_worker._load_nested_artifacts", lambda *_a, **_kw: {})
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
            lambda **kw: ({"nodes": [], "edges": []}, {}),
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


# ---------------------------------------------------------------------------
# Step 3 parallelism: full process() with 50+ principals must produce identical
# stored artifacts under PSAT_RPC_FANOUT=1 vs =8, and the per-job classify_cache
# must collapse repeated probes deterministically.
# ---------------------------------------------------------------------------


class TestProcessFanoutParity:
    """Drive ``PolicyWorker.process`` end-to-end (real ``build_principal_labels``)
    with a 50+ principal fixture and assert sequential vs parallel parity."""

    @staticmethod
    def _run(monkeypatch: pytest.MonkeyPatch, fanout: str) -> tuple[Any, dict[str, Any]]:
        from utils.concurrency import RpcExecutor

        monkeypatch.setenv("PSAT_RPC_FANOUT", fanout)
        RpcExecutor.reset_for_tests()

        worker = PolicyWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job()

        target = TARGET_ADDRESS
        principal_addrs = [f"0x{(i + 0x100):040x}" for i in range(60)]

        def role_principals(addrs: list[str]) -> list[dict]:
            return [{"address": a, "resolved_type": "unknown", "details": {}} for a in addrs]

        ep_data: dict = {
            "schema_version": "0.1",
            "contract_address": target,
            "contract_name": "VaultBig",
            "functions": [
                {
                    "function": "manage(address,bytes,uint256)",
                    "abi_signature": "manage(address,bytes,uint256)",
                    "selector": "0x12345678",
                    "direct_owner": None,
                    "authority_public": False,
                    "authority_roles": [{"role": 1, "principals": role_principals(principal_addrs[:30])}],
                    "controllers": [],
                    "effect_targets": [],
                    "effect_labels": ["arbitrary_external_call"],
                    "action_summary": "Manage",
                    "notes": [],
                },
                {
                    "function": "setAuthority(address)",
                    "abi_signature": "setAuthority(address)",
                    "selector": "0x12345679",
                    "direct_owner": None,
                    "authority_public": False,
                    "authority_roles": [{"role": 8, "principals": role_principals(principal_addrs[30:])}],
                    "controllers": [],
                    "effect_targets": [],
                    "effect_labels": ["authority_update"],
                    "action_summary": "Set authority",
                    "notes": [],
                },
            ],
        }
        contract_analysis = _minimal_contract_analysis()
        control_snapshot = _minimal_snapshot({})
        resolved_graph = _graph_with_nodes(
            [
                {
                    "id": "address:" + target,
                    "address": target,
                    "node_type": "contract",
                    "resolved_type": "contract",
                    "label": "VaultBig",
                    "contract_name": "VaultBig",
                    "depth": 0,
                    "analyzed": True,
                    "details": {"address": target},
                    "artifacts": {},
                }
            ]
        )
        tracking_plan = {
            "schema_version": "0.1",
            "contract_address": target,
            "contract_name": "VaultBig",
            "tracked_controllers": [],
            "tracked_policies": [],
        }

        def fake_get_artifact(_session: Any, _job_id: Any, name: str) -> Any:
            return {
                "contract_analysis": contract_analysis,
                "control_snapshot": control_snapshot,
                "resolved_control_graph": resolved_graph,
                "control_tracking_plan": tracking_plan,
                "semantic_guards": None,
                "classified_addresses": None,
            }.get(name)

        store_calls: list[tuple[str, Any]] = []

        def fake_store_artifact(
            _session: Any, _job_id: Any, name: str, data: Any = None, text_data: Any = None
        ) -> None:
            store_calls.append((name, data))

        # Track every classify call so we can assert no spurious re-probes
        # leak through the parallel path.
        classify_calls: list[str] = []

        def fake_classify(_rpc, address):
            classify_calls.append(address)
            return "eoa", {"address": address}, True

        monkeypatch.setattr("workers.policy_worker.get_artifact", fake_get_artifact)
        monkeypatch.setattr("workers.policy_worker.store_artifact", fake_store_artifact)
        monkeypatch.setattr("workers.policy_worker._load_nested_artifacts", lambda *_a, **_kw: {})
        monkeypatch.setattr(
            "workers.policy_worker.build_effective_permissions",
            lambda *a, **kw: ep_data,
        )
        monkeypatch.setattr(
            "workers.policy_worker.resolve_control_graph",
            lambda **kw: (resolved_graph, {}),
        )
        monkeypatch.setattr(
            "services.policy.principal_enrichment.classify_resolved_address_with_status",
            fake_classify,
        )
        monkeypatch.setattr(
            PolicyWorker,
            "_enrich_cross_contract",
            lambda self, session, job, contract_analysis, control_snapshot: {},
        )

        worker.process(session, cast(Any, job))

        labels_payload = next(data for name, data in store_calls if name == "principal_labels")
        return labels_payload, {"classify_calls": classify_calls}

    def test_process_fanout_parity_50_plus_principals(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Sequential and parallel runs must produce identical principal_labels."""
        seq_payload, seq_stats = self._run(monkeypatch, "1")
        par_payload, par_stats = self._run(monkeypatch, "8")

        assert seq_payload["contract_address"] == par_payload["contract_address"]
        assert seq_payload["contract_name"] == par_payload["contract_name"]
        assert len(seq_payload["principals"]) == len(par_payload["principals"])

        # Principals are emitted in sorted-address order — direct equality holds.
        for seq_p, par_p in zip(seq_payload["principals"], par_payload["principals"]):
            assert seq_p == par_p

        # Cache discipline: 60 unknown principals should each classify roughly
        # once. The parallel path tolerates a benign per-address race where
        # two threads miss before the first writes back, but the total must
        # remain bounded by 2× the sequential count — anything more means
        # the cache lock isn't collapsing concurrent misses.
        assert len(seq_stats["classify_calls"]) == 60
        assert len(par_stats["classify_calls"]) <= 60 * 2
