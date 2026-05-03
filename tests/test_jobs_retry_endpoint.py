"""Integration tests for ``POST /api/jobs/{id}/retry``.

Hits the FastAPI app with the test DB wired through ``api_client`` so
admin-key, status checks, and the artifact append are exercised end to end.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.models import Artifact, Job, JobStatus  # noqa: E402
from db.queue import create_job, fail_job_terminal  # noqa: E402
from tests.cache_helpers import requires_postgres  # noqa: E402


@pytest.fixture()
def clean_jobs(db_session):
    db_session.query(Artifact).delete()
    db_session.query(Job).delete()
    db_session.commit()
    yield db_session
    db_session.rollback()
    db_session.query(Artifact).delete()
    db_session.query(Job).delete()
    db_session.commit()


def _read_stage_errors(session, job_id):
    art = session.query(Artifact).filter(Artifact.job_id == job_id, Artifact.name == "stage_errors").one_or_none()
    if art is None or art.data is None:
        return None
    return art.data


# ---------------------------------------------------------------------------
# Happy path: failed_terminal → queued
# ---------------------------------------------------------------------------


@requires_postgres
def test_retry_endpoint_resets_failed_terminal_to_queued(api_client, clean_jobs):
    """A failed_terminal job is reset to queued with retry_count=0 and a
    manual_retry artifact entry appended."""
    db_session = clean_jobs
    job = create_job(db_session, {"address": "0xabc", "name": "manual-retry"})
    fail_job_terminal(db_session, job.id, "boom", kind="terminal")

    response = api_client.post(f"/api/jobs/{job.id}/retry")
    assert response.status_code == 200

    body = response.json()
    assert body["status"] == "queued"
    assert body["retry_count"] == 0
    assert body["next_attempt_at"] is None
    assert body["last_failure_kind"] is None

    db_session.expire_all()
    refreshed = db_session.get(Job, job.id)
    assert refreshed is not None
    assert refreshed.status == JobStatus.queued
    assert refreshed.retry_count == 0

    payload = _read_stage_errors(db_session, job.id)
    assert payload is not None
    errors = payload["errors"]
    assert len(errors) >= 1
    last = errors[-1]
    assert last["severity"] == "degraded"
    assert last["phase"] == "manual_retry"
    assert "perator-initiated" in last["message"]  # case-insensitive match


# ---------------------------------------------------------------------------
# 409 for the wrong status
# ---------------------------------------------------------------------------


@requires_postgres
def test_retry_endpoint_rejects_done_job(api_client, clean_jobs):
    """A done/completed job must not be retried — that would clobber a real outcome."""
    db_session = clean_jobs
    job = create_job(db_session, {"address": "0xabc", "name": "completed"})
    job.status = JobStatus.completed
    db_session.commit()

    response = api_client.post(f"/api/jobs/{job.id}/retry")
    assert response.status_code == 409
    assert "completed" in response.json()["detail"]


@requires_postgres
def test_retry_endpoint_rejects_queued_job(api_client, clean_jobs):
    """A queued job is already eligible to run — retrying it would be a no-op
    that resets its retry_count, masking earlier failures."""
    db_session = clean_jobs
    job = create_job(db_session, {"address": "0xabc", "name": "queued"})

    response = api_client.post(f"/api/jobs/{job.id}/retry")
    assert response.status_code == 409
    assert "queued" in response.json()["detail"]


@requires_postgres
def test_retry_endpoint_rejects_processing_job(api_client, clean_jobs):
    """A processing job is in flight — clobbering it could double-execute work."""
    db_session = clean_jobs
    job = create_job(db_session, {"address": "0xabc", "name": "processing"})
    job.status = JobStatus.processing
    db_session.commit()

    response = api_client.post(f"/api/jobs/{job.id}/retry")
    assert response.status_code == 409


@requires_postgres
def test_retry_endpoint_rejects_legacy_failed_job(api_client, clean_jobs):
    """``status='failed'`` (the legacy state) is not retryable via this
    endpoint — the operator must promote to ``failed_terminal`` first or
    use a different mechanism. Avoids accidental retries of pre-migration
    rows that may have been transient and stayed flapping."""
    db_session = clean_jobs
    job = create_job(db_session, {"address": "0xabc", "name": "legacy-failed"})
    job.status = JobStatus.failed
    db_session.commit()

    response = api_client.post(f"/api/jobs/{job.id}/retry")
    assert response.status_code == 409


# ---------------------------------------------------------------------------
# 404 for missing / malformed
# ---------------------------------------------------------------------------


@requires_postgres
def test_retry_endpoint_returns_404_for_missing_job(api_client, clean_jobs):
    response = api_client.post("/api/jobs/00000000-0000-0000-0000-000000000000/retry")
    assert response.status_code == 404


@requires_postgres
def test_retry_endpoint_returns_404_for_malformed_uuid(api_client, clean_jobs):
    response = api_client.post("/api/jobs/not-a-uuid/retry")
    assert response.status_code == 404
