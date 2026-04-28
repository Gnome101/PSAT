from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from workers.static_worker import StaticWorker


def _job(**overrides):
    payload = {
        "id": "job-1",
        "address": "0x1111111111111111111111111111111111111111",
        "name": "HiddenThing",
        "request": {"rpc_url": "https://rpc.example"},
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def test_resolve_proxy_queues_hidden_proxy_impl(monkeypatch):
    worker = StaticWorker()
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    job = _job()

    store_calls = []
    created_jobs = []

    monkeypatch.setattr(
        "workers.static_worker.store_artifact",
        lambda _session, _job_id, name, data=None, text_data=None: store_calls.append((name, data, text_data)),
    )
    monkeypatch.setattr(
        "workers.static_worker.create_job",
        lambda _session, request: created_jobs.append(request) or SimpleNamespace(id="child-1"),
    )
    monkeypatch.setattr(
        "services.discovery.classifier.classify_single",
        lambda address, rpc_url: {
            "address": address,
            "type": "proxy",
            "proxy_type": "unknown",
            "implementation": "0x2222222222222222222222222222222222222222",
        },
    )

    worker._resolve_proxy(session, job, job.address, job.name)

    assert store_calls[0][0] == "contract_flags"
    assert store_calls[0][1]["is_proxy"] is True
    assert store_calls[0][1]["implementation"] == "0x2222222222222222222222222222222222222222"
    # root_job_id falls back to the parent job's id when no upstream
    # cascade was set on the request — preserves the within-cascade
    # dedup semantics for top-level calls too.
    assert created_jobs == [
        {
            "address": "0x2222222222222222222222222222222222222222",
            "name": "HiddenThing: (impl)",
            "rpc_url": "https://rpc.example",
            "parent_job_id": "job-1",
            "root_job_id": "job-1",
            "proxy_address": "0x1111111111111111111111111111111111111111",
            "proxy_type": "unknown",
        }
    ]


def test_resolve_proxy_marks_regular_contract_without_proxy_flag(monkeypatch):
    worker = StaticWorker()
    session = MagicMock()
    job = _job()
    store_calls = []

    monkeypatch.setattr(
        "workers.static_worker.store_artifact",
        lambda _session, _job_id, name, data=None, text_data=None: store_calls.append((name, data, text_data)),
    )
    monkeypatch.setattr(
        "services.discovery.classifier.classify_single",
        lambda address, rpc_url: {"address": address, "type": "regular"},
    )

    worker._resolve_proxy(session, job, job.address, job.name)

    assert store_calls == [
        (
            "contract_flags",
            {"is_proxy": False, "classification_type": "regular"},
            None,
        )
    ]


def test_process_attempts_semantic_proxy_classification_for_non_obvious_names(monkeypatch, tmp_path):
    worker = StaticWorker()
    session = MagicMock()
    # Provide a mock Contract row so the worker can read contract metadata
    mock_contract = MagicMock()
    mock_contract.contract_name = "OssifiableProxy"
    mock_contract.address = "0x1111111111111111111111111111111111111111"
    mock_contract.compiler_version = "v0.8.0"
    mock_contract.language = "solidity"
    mock_contract.evm_version = "shanghai"
    mock_contract.source_format = "flat"
    mock_contract.source_file_count = 1
    mock_contract.remappings = []
    mock_contract.optimization = False
    mock_contract.optimization_runs = 200
    mock_contract.is_proxy = False
    session.execute.return_value.scalar_one_or_none.return_value = mock_contract

    job = _job(name="OssifiableProxy")
    called = []

    monkeypatch.setattr(
        "workers.static_worker.get_source_files",
        lambda _session, _job_id: {"contracts/OssifiableProxy.sol": "contract OssifiableProxy {}"},
    )
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *_args: called.append("resolve"))
    monkeypatch.setattr(worker, "_scaffold_project", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, "_run_dependency_phase", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *args, **kwargs: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, "update_detail", lambda *args, **kwargs: None)

    worker.process(session, cast(Any, job))

    assert called == ["resolve"]


# ---------------------------------------------------------------------------
# Within-cascade impl-job dedupe under --force
# ---------------------------------------------------------------------------


def test_force_dedupes_impl_jobs_within_same_root_cascade(monkeypatch):
    """Two proxy paths to the same impl in one cascade: second submission dedupes; new cascades still get fresh jobs."""
    worker = StaticWorker()
    session = MagicMock()

    # Calls 1+2 = first proxy (contract-row + dedupe), 3+4 = second proxy; only call 4 hits an existing prior job.
    existing_in_cascade = SimpleNamespace(id="prior-impl-in-same-cascade")
    call_count = {"n": 0}

    def _fake_execute(query, *params):
        # Skip the advisory_lock SELECT — it's a no-op in the mock.
        if params:
            return MagicMock()
        call_count["n"] += 1
        result = MagicMock()
        result.scalar_one_or_none.return_value = existing_in_cascade if call_count["n"] >= 4 else None
        return result

    session.execute.side_effect = _fake_execute

    created_jobs: list[dict] = []
    monkeypatch.setattr(
        "workers.static_worker.create_job",
        lambda _session, request: created_jobs.append(request) or SimpleNamespace(id=f"child-{len(created_jobs)}"),
    )
    monkeypatch.setattr(
        "workers.static_worker.store_artifact",
        lambda *_a, **_kw: None,
    )
    monkeypatch.setattr(
        "services.discovery.classifier.classify_single",
        lambda address, rpc_url: {
            "address": address,
            "type": "proxy",
            "proxy_type": "uups",
            "implementation": "0x" + "33" * 20,
        },
    )

    job1 = _job(
        id="proxy-job-A",
        request={"rpc_url": "https://rpc", "force": True, "root_job_id": "root-1"},
    )
    job2 = _job(
        id="proxy-job-B",
        request={"rpc_url": "https://rpc", "force": True, "root_job_id": "root-1"},
    )

    worker._resolve_proxy(session, job1, job1.address, job1.name)
    worker._resolve_proxy(session, job2, job2.address, job2.name)

    # First proxy spawned its impl; second proxy in same cascade was deduped.
    assert len(created_jobs) == 1, f"second proxy in same cascade must dedupe its impl; got {len(created_jobs)} jobs"
    # The created job carries root_job_id so downstream static_worker calls
    # in the same cascade can dedupe against it too.
    assert created_jobs[0]["root_job_id"] == "root-1"


def test_force_does_not_dedupe_across_different_root_cascades(monkeypatch):
    """The (address, root_job_id) check must NOT block a fresh cascade.
    If root_job_id differs, the impl gets a fresh job per cascade —
    bench A/B runs need clean cold-path measurements per root."""
    worker = StaticWorker()
    session = MagicMock()
    # Both queries return None → no existing job in either cascade.
    session.execute.return_value.scalar_one_or_none.return_value = None

    created_jobs: list[dict] = []
    monkeypatch.setattr(
        "workers.static_worker.create_job",
        lambda _session, request: created_jobs.append(request) or SimpleNamespace(id=f"child-{len(created_jobs)}"),
    )
    monkeypatch.setattr(
        "workers.static_worker.store_artifact",
        lambda *_a, **_kw: None,
    )
    monkeypatch.setattr(
        "services.discovery.classifier.classify_single",
        lambda address, rpc_url: {
            "address": address,
            "type": "proxy",
            "proxy_type": "uups",
            "implementation": "0x" + "44" * 20,
        },
    )

    job_root_a = _job(
        id="proxy-A",
        request={"rpc_url": "https://rpc", "force": True, "root_job_id": "root-A"},
    )
    job_root_b = _job(
        id="proxy-B",
        request={"rpc_url": "https://rpc", "force": True, "root_job_id": "root-B"},
    )

    worker._resolve_proxy(session, job_root_a, job_root_a.address, job_root_a.name)
    worker._resolve_proxy(session, job_root_b, job_root_b.address, job_root_b.name)

    assert len(created_jobs) == 2, "fresh cascades must each get their own impl job"
    assert {j["root_job_id"] for j in created_jobs} == {"root-A", "root-B"}


def test_no_force_uses_global_dedupe(monkeypatch):
    """Without --force, the historical global dedupe is preserved: any
    prior job for the impl wins, even from an unrelated cascade. This
    is the production behavior and the new (address, root_job_id) check
    must NOT silently apply when force is off."""
    worker = StaticWorker()
    session = MagicMock()
    prior = SimpleNamespace(id="prior-impl-from-other-cascade")
    # The global query (no root_job_id filter) returns the prior job.
    session.execute.return_value.scalar_one_or_none.return_value = prior

    created_jobs: list[dict] = []
    monkeypatch.setattr(
        "workers.static_worker.create_job",
        lambda _session, request: created_jobs.append(request) or SimpleNamespace(id="should-not-be-created"),
    )
    monkeypatch.setattr(
        "workers.static_worker.store_artifact",
        lambda *_a, **_kw: None,
    )
    monkeypatch.setattr(
        "services.discovery.classifier.classify_single",
        lambda address, rpc_url: {
            "address": address,
            "type": "proxy",
            "proxy_type": "uups",
            "implementation": "0x" + "55" * 20,
        },
    )

    job = _job(request={"rpc_url": "https://rpc"})  # no force
    worker._resolve_proxy(session, job, job.address, job.name)

    assert created_jobs == [], "global dedupe must reject this impl when prior job exists"
