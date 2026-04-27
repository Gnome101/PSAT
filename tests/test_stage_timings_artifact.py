"""Regression tests for the per-job ``stage_timings`` artifact written by
``workers.base.BaseWorker._record_stage_timing``.

The bench harness (scripts/bench_workers.py) used to derive per-stage
durations by scraping Fly logs for the ``Worker X completed job Y in Zs``
line. That line is buffered and routinely missed under load, so
``worker_elapsed_seconds`` in bench JSON was empty about half the runs.
Persisting timings as a structured artifact gives the harness a
reliable source of truth that doesn't depend on log delivery.

Schema (v1):
  {
    "schema_version": "1",
    "stages": [
      {"stage": "...", "started_at": ISO, "ended_at": ISO, "elapsed_s": float,
       "worker_id": str, "status": "success"|"failed"|"handled_directly"},
      ...
    ]
  }
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.models import Job, JobStage
from workers.base import BaseWorker


class _FakeWorker(BaseWorker):
    """Minimal subclass for unit-testing the helper in isolation."""

    stage = JobStage.discovery
    next_stage = JobStage.static
    poll_interval = 0.0


def _job(job_id: str = "job-1") -> Job:
    """Build a duck-typed Job stub. The helper only reads ``id`` so a
    SimpleNamespace satisfies the runtime contract; ``cast`` quiets pyright."""
    return cast(Job, SimpleNamespace(id=job_id, address="0xabc", name="test"))


def test_record_stage_timing_creates_first_entry(monkeypatch):
    captured: dict = {}

    def _fake_get(*_a, **_kw):
        # No prior artifact.
        return None

    def _fake_store(*_a, **kw):
        captured["name"] = _a[2] if len(_a) > 2 else kw.get("name")
        captured["data"] = kw.get("data")

    monkeypatch.setattr("workers.base.get_artifact", _fake_get)
    monkeypatch.setattr("workers.base.store_artifact", _fake_store)

    w = _FakeWorker()
    w._record_stage_timing(
        MagicMock(),
        _job(),
        started_at="2026-04-27T03:00:00.000Z",
        ended_at="2026-04-27T03:00:02.500Z",
        elapsed_s=2.5,
        status="success",
    )

    assert captured["name"] == "stage_timings"
    payload = captured["data"]
    assert payload["schema_version"] == "1"
    assert len(payload["stages"]) == 1
    entry = payload["stages"][0]
    assert entry["stage"] == "discovery"
    assert entry["elapsed_s"] == 2.5
    assert entry["status"] == "success"
    assert entry["started_at"] == "2026-04-27T03:00:00.000Z"
    assert entry["ended_at"] == "2026-04-27T03:00:02.500Z"
    assert entry["worker_id"].startswith("_FakeWorker-")


def test_record_stage_timing_appends_to_existing(monkeypatch):
    """Multiple stages on the same job append rather than overwrite."""
    existing = {
        "schema_version": "1",
        "stages": [
            {
                "stage": "discovery",
                "started_at": "2026-04-27T03:00:00.000Z",
                "ended_at": "2026-04-27T03:00:02.000Z",
                "elapsed_s": 2.0,
                "worker_id": "DiscoveryWorker-1-aaa",
                "status": "success",
            }
        ],
    }
    captured: dict = {}

    def _fake_get(*_a, **_kw):
        return existing

    def _fake_store(*_a, **kw):
        captured["data"] = kw.get("data")

    monkeypatch.setattr("workers.base.get_artifact", _fake_get)
    monkeypatch.setattr("workers.base.store_artifact", _fake_store)

    class _StaticWorker(BaseWorker):
        stage = JobStage.static
        next_stage = JobStage.resolution

    w = _StaticWorker()
    w._record_stage_timing(
        MagicMock(),
        _job(),
        started_at="2026-04-27T03:00:02.000Z",
        ended_at="2026-04-27T03:00:42.000Z",
        elapsed_s=40.0,
        status="success",
    )

    payload = captured["data"]
    assert len(payload["stages"]) == 2
    assert [s["stage"] for s in payload["stages"]] == ["discovery", "static"]
    assert payload["stages"][1]["elapsed_s"] == 40.0
    # Existing entry untouched.
    assert payload["stages"][0]["elapsed_s"] == 2.0


def test_record_stage_timing_swallows_storage_errors(monkeypatch, caplog):
    """A failing artifact write must NOT crash the worker — losing a
    metrics record is much better than failing a real job."""

    def _boom(*_a, **_kw):
        raise RuntimeError("storage down")

    monkeypatch.setattr("workers.base.get_artifact", _boom)

    w = _FakeWorker()
    # Should not raise.
    w._record_stage_timing(
        MagicMock(),
        _job(),
        started_at="2026-04-27T03:00:00.000Z",
        ended_at="2026-04-27T03:00:01.000Z",
        elapsed_s=1.0,
        status="success",
    )


def test_record_stage_timing_failed_status_persists(monkeypatch):
    """The exception path in run_loop calls record with status='failed'.
    Ensures we capture timing for jobs that errored out — important for
    bench analysis ('which stage tends to fail and how long does it run
    before failing?')."""
    captured: dict = {}
    monkeypatch.setattr("workers.base.get_artifact", lambda *a, **kw: None)
    monkeypatch.setattr(
        "workers.base.store_artifact",
        lambda _s, _j, name, data=None, text_data=None: captured.update({"data": data}),
    )

    w = _FakeWorker()
    w._record_stage_timing(
        MagicMock(),
        _job(),
        started_at="2026-04-27T03:00:00.000Z",
        ended_at="2026-04-27T03:00:30.000Z",
        elapsed_s=30.0,
        status="failed",
    )
    assert captured["data"]["stages"][0]["status"] == "failed"


def test_record_stage_timing_rejects_corrupt_existing_payload(monkeypatch):
    """If the artifact exists but the schema is unrecognisable, start
    fresh rather than crashing or dropping the new entry."""
    captured: dict = {}
    monkeypatch.setattr(
        "workers.base.get_artifact",
        lambda *a, **kw: {"unrelated_key": "garbage"},  # missing 'stages'
    )
    monkeypatch.setattr(
        "workers.base.store_artifact",
        lambda _s, _j, name, data=None, text_data=None: captured.update({"data": data}),
    )

    w = _FakeWorker()
    w._record_stage_timing(
        MagicMock(),
        _job(),
        started_at="2026-04-27T03:00:00.000Z",
        ended_at="2026-04-27T03:00:01.000Z",
        elapsed_s=1.0,
        status="success",
    )
    # Wrote a fresh schema-v1 payload with just our entry.
    assert captured["data"]["schema_version"] == "1"
    assert len(captured["data"]["stages"]) == 1


# ---------------------------------------------------------------------------
# Codex-iter-1 finding: timing must be recorded before advance/complete
# ---------------------------------------------------------------------------


def test_record_timing_runs_before_advance_in_run_loop(monkeypatch):
    """Codex review finding: if ``advance_job`` commits the next stage as
    queued before ``_record_stage_timing`` writes, the next-stage worker
    can claim the row, finish, and write its own ``stage_timings`` entry
    concurrently. Both workers do read-modify-write on the same JSON
    artifact — last-writer-wins drops one stage's entry under multi-worker
    configs.

    Verify the call order: stage_timings recording happens BEFORE
    ``advance_job`` / ``complete_job`` so the artifact write is
    fully owned by this worker before the row is exposed to the next
    stage. Timing is best-effort (errors swallowed) so this re-ordering
    does not delay or block the advance even when the artifact write
    fails."""
    from db.models import JobStatus
    from workers import base
    from workers.base import BaseWorker, JobHandledDirectly  # noqa: F401

    call_order: list[str] = []

    class _OrderingWorker(BaseWorker):
        stage = JobStage.discovery
        next_stage = JobStage.static
        poll_interval = 0.0

        def process(self, _session, _job):
            call_order.append("process")

        def _record_stage_timing(self, *_a, **_kw):
            call_order.append("record_stage_timing")

    # Synthesize a job; never actually persisted.
    job = SimpleNamespace(
        id="job-ordering",
        address="0xabc",
        name="t",
        status=JobStatus.processing,
        worker_id="w",
        stage=JobStage.discovery,
    )

    # Patch claim/advance/complete: claim returns our fake job once, then
    # signals shutdown so run_loop exits cleanly.
    claims = iter([job, None])

    def _fake_claim(self_, _session):
        call_order.append("claim")
        try:
            j = next(claims)
        except StopIteration:
            return None
        if j is None:
            self_._running = False
        return j

    def _fake_advance(_session, _job_id, _next_stage, _detail):
        call_order.append("advance_job")

    def _fake_complete(_session, _job_id):
        call_order.append("complete_job")

    monkeypatch.setattr(base.BaseWorker, "_claim_job", _fake_claim)
    monkeypatch.setattr(base, "advance_job", _fake_advance)
    # Patch the deferred import in run_loop too.
    import db.queue as db_queue
    monkeypatch.setattr(db_queue, "complete_job", _fake_complete)
    monkeypatch.setattr(base, "SessionLocal", lambda: MagicMock())
    monkeypatch.setattr(base.time, "sleep", lambda *_: None)

    w = _OrderingWorker()
    w.run_loop()

    # Filter out claim/no-job iterations; we care about the per-job sequence.
    relevant = [c for c in call_order if c in {"process", "record_stage_timing", "advance_job", "complete_job"}]
    assert relevant == ["process", "record_stage_timing", "advance_job"], (
        f"timing must record BEFORE advance_job; saw {relevant}"
    )
