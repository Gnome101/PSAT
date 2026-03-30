from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
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
    assert created_jobs == [
        {
            "address": "0x2222222222222222222222222222222222222222",
            "name": "HiddenThing: (impl)",
            "rpc_url": "https://rpc.example",
            "parent_job_id": "job-1",
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
    job = _job(name="OssifiableProxy")
    called = []

    monkeypatch.setattr(
        "workers.static_worker.get_source_files",
        lambda _session, _job_id: {"contracts/OssifiableProxy.sol": "contract OssifiableProxy {}"},
    )
    artifacts = {
        "contract_meta": {"contract_name": "OssifiableProxy", "address": job.address, "remappings": []},
        "build_settings": {},
    }
    monkeypatch.setattr("workers.static_worker.get_artifact", lambda _session, _job_id, name: artifacts.get(name))
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *_args: called.append("resolve"))
    monkeypatch.setattr(worker, "_scaffold_project", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, "_run_dependency_phase", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *args, **kwargs: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *args, **kwargs: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, "update_detail", lambda *args, **kwargs: None)

    worker.process(session, job)

    assert called == ["resolve"]
