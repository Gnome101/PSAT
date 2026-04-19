"""Unit tests for db/queue.py helpers."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.models import Protocol
from db.queue import get_or_create_protocol

DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")


def _can_connect() -> bool:
    if not DATABASE_URL:
        return False
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(not _can_connect(), reason="PostgreSQL not available")


@pytest.fixture()
def session():
    """PostgreSQL session scoped to one test, cleans Job rows on teardown."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from db.models import Artifact, Base, Job, SourceFile

    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    s = Session(engine, expire_on_commit=False)
    try:
        yield s
    finally:
        s.rollback()
        s.query(SourceFile).delete()
        s.query(Artifact).delete()
        s.query(Job).delete()
        s.commit()
        s.close()
        engine.dispose()


def _backdate_job(s, job_id, seconds_ago: int) -> None:
    """Force ``updated_at`` into the past — bypassing the onupdate trigger.

    Without this helper the ``updated_at`` column auto-stamps NOW() on every
    write, which would defeat any stuck-job assertion.
    """
    from sqlalchemy import update as sa_update

    from db.models import Job

    past = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    s.execute(sa_update(Job).where(Job.id == job_id).values(updated_at=past))
    s.commit()


class TestGetOrCreateProtocol:
    def test_creates_when_missing(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        row = get_or_create_protocol(session, "ether.fi", official_domain="ether.fi")

        assert isinstance(row, Protocol)
        assert row.name == "ether.fi"
        assert row.official_domain == "ether.fi"
        session.add.assert_called_once()
        session.flush.assert_called_once()

    def test_returns_existing_without_modifying(self):
        existing = Protocol(name="uniswap", official_domain="uniswap.org")
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = existing

        row = get_or_create_protocol(session, "uniswap", official_domain="uniswap.org")

        assert row is existing
        assert row.official_domain == "uniswap.org"
        session.add.assert_not_called()
        session.flush.assert_not_called()

    def test_backfills_official_domain_when_null(self):
        existing = Protocol(name="aave", official_domain=None)
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = existing

        row = get_or_create_protocol(session, "aave", official_domain="aave.com")

        assert row is existing
        assert row.official_domain == "aave.com"
        session.add.assert_not_called()
        session.flush.assert_called_once()

    def test_does_not_overwrite_existing_official_domain(self):
        existing = Protocol(name="aave", official_domain="aave-v3.com")
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = existing

        row = get_or_create_protocol(session, "aave", official_domain="different.com")

        assert row.official_domain == "aave-v3.com"
        session.flush.assert_not_called()

    def test_no_domain_provided_leaves_null_on_new_row(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        row = get_or_create_protocol(session, "some-slug")

        assert row.name == "some-slug"
        assert row.official_domain is None


# ---------------------------------------------------------------------------
# reclaim_stuck_jobs — cross-stage worker-crash recovery
# ---------------------------------------------------------------------------


@requires_postgres
def test_reclaim_stuck_jobs_resets_long_running_processing_to_queued(session):
    """A job that's been ``processing`` past the threshold is swept back to
    ``queued`` with ``worker_id`` cleared. The returned list contains its id."""
    from db.models import JobStage, JobStatus
    from db.queue import claim_job, create_job, reclaim_stuck_jobs

    job = create_job(session, {"address": "0x" + "1" * 40})
    claimed = claim_job(session, JobStage.discovery, "crashed-worker")
    assert claimed is not None
    assert claimed.id == job.id
    assert claimed.status == JobStatus.processing
    _backdate_job(session, job.id, seconds_ago=10)

    reclaimed_ids = reclaim_stuck_jobs(session, stale_timeout_seconds=1)

    assert str(job.id) in [str(i) for i in reclaimed_ids]
    session.expire_all()
    refreshed = session.get(type(job), job.id)
    assert refreshed.status == JobStatus.queued
    assert refreshed.worker_id is None


@requires_postgres
def test_reclaim_stuck_jobs_leaves_recent_processing_alone(session):
    """A freshly-claimed job whose ``updated_at`` is within the threshold
    must NOT be swept — that would steal work out from under a live worker."""
    from db.models import JobStage, JobStatus
    from db.queue import claim_job, create_job, reclaim_stuck_jobs

    job = create_job(session, {"address": "0x" + "2" * 40})
    claimed = claim_job(session, JobStage.discovery, "live-worker")
    assert claimed is not None

    reclaimed_ids = reclaim_stuck_jobs(session, stale_timeout_seconds=900)

    assert reclaimed_ids == []
    session.expire_all()
    refreshed = session.get(type(job), job.id)
    assert refreshed.status == JobStatus.processing
    assert refreshed.worker_id == "live-worker"


@requires_postgres
def test_reclaim_stuck_jobs_is_idempotent(session):
    """Re-running reclaim after a successful sweep returns an empty list —
    the first sweep flipped the job to queued, so the predicate no longer
    matches it. Running back-to-back must never double-reset anything."""
    from db.models import JobStage
    from db.queue import claim_job, create_job, reclaim_stuck_jobs

    job = create_job(session, {"address": "0x" + "3" * 40})
    claim_job(session, JobStage.discovery, "crashed-worker")
    _backdate_job(session, job.id, seconds_ago=10)

    first = reclaim_stuck_jobs(session, stale_timeout_seconds=1)
    assert len(first) == 1

    second = reclaim_stuck_jobs(session, stale_timeout_seconds=1)
    assert second == []


@requires_postgres
def test_reclaim_stuck_jobs_ignores_terminal_states(session):
    """Completed and failed jobs should never be touched — only those that
    are actually ``processing``. A worker's own completion updated_at can
    be arbitrarily old without inviting a reclaim."""
    from db.models import JobStage, JobStatus
    from db.queue import claim_job, complete_job, create_job, fail_job, reclaim_stuck_jobs

    completed = create_job(session, {"address": "0x" + "4" * 40})
    claim_job(session, JobStage.discovery, "w1")
    complete_job(session, completed.id)
    _backdate_job(session, completed.id, seconds_ago=10)

    failed = create_job(session, {"address": "0x" + "5" * 40})
    claim_job(session, JobStage.discovery, "w2")
    fail_job(session, failed.id, "boom")
    _backdate_job(session, failed.id, seconds_ago=10)

    reclaimed_ids = reclaim_stuck_jobs(session, stale_timeout_seconds=1)

    assert reclaimed_ids == []
    session.expire_all()
    assert session.get(type(completed), completed.id).status == JobStatus.completed
    assert session.get(type(failed), failed.id).status == JobStatus.failed
