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
    """Force ``updated_at`` *and* ``lease_expires_at`` into the past.

    Without this helper the ``updated_at`` column auto-stamps NOW() on every
    write and ``claim_job`` sets ``lease_expires_at`` to NOW()+ttl, both of
    which would defeat any stuck-job assertion. Backdating both pins the row
    as "stale by both predicates".
    """
    from sqlalchemy import update as sa_update

    from db.models import Job

    past = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    s.execute(sa_update(Job).where(Job.id == job_id).values(updated_at=past, lease_expires_at=past))
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


# ---------------------------------------------------------------------------
# Lease-based claim — duplicate-claim race POC
# ---------------------------------------------------------------------------
#
# claim_job today filters only on status='queued'; the only mid-run path
# that flips a processing job back to queued is reclaim_stuck_jobs, which
# fires when updated_at < NOW() - stale_timeout. The heartbeat that keeps
# updated_at fresh runs from inside parallel_map's per-task callback
# (utils/concurrency.py:82-86, 122-126). A single nested forge build
# longer than PSAT_JOB_STALE_TIMEOUT (900s in prod) silently expires the
# lease — and a sibling worker then claims the same job. From that point
# both workers process the same row in parallel.
#
# These tests pin the desired post-fix behaviour:
#   1. The original holder's mutating writes detect they no longer hold
#      the lease and refuse to commit.
#   2. The claim path takes the lease atomically with the status flip
#      so the reclaim never hands one row to two workers.
#
# They FAIL today (no lease enforcement) and PASS once the lease columns
# + LeaseLost exception land.


@requires_postgres
def test_reclaimed_job_cannot_be_silently_finished_by_original_holder(session):
    """Worker A claims, lags past stale_timeout, gets reclaimed; B claims;
    A finishes its long task and tries to complete the job. A's write
    must be rejected (lease lost).
    """
    from db.models import JobStage
    from db.queue import LeaseLost, claim_job, complete_job, create_job, reclaim_stuck_jobs

    job = create_job(session, {"address": "0x" + "a" * 40, "name": "long-running"})
    a = claim_job(session, JobStage.discovery, "worker-A")
    assert a is not None
    a_lease = getattr(a, "lease_id", None)
    assert a_lease is not None, "claim_job must mint a lease id for the holder"

    _backdate_job(session, job.id, seconds_ago=1000)
    reclaim_stuck_jobs(session, stale_timeout_seconds=1)

    b = claim_job(session, JobStage.discovery, "worker-B")
    assert b is not None
    assert b.id == job.id
    assert b.worker_id == "worker-B"
    assert getattr(b, "lease_id", None) != a_lease, "B's claim must produce a fresh lease id"

    with pytest.raises(LeaseLost):
        complete_job(session, a.id, lease_id=a_lease)  # type: ignore[call-arg]


@requires_postgres
def test_reclaimed_job_cannot_be_silently_advanced_by_original_holder(session):
    """Same race, advance variant: A's advance_job must be rejected after
    the row's lease has rolled to B."""
    from db.models import JobStage
    from db.queue import LeaseLost, advance_job, claim_job, create_job, reclaim_stuck_jobs

    job = create_job(session, {"address": "0x" + "b" * 40, "name": "long-running"})
    a = claim_job(session, JobStage.discovery, "worker-A")
    assert a is not None
    a_lease = getattr(a, "lease_id", None)

    _backdate_job(session, job.id, seconds_ago=1000)
    reclaim_stuck_jobs(session, stale_timeout_seconds=1)

    b = claim_job(session, JobStage.discovery, "worker-B")
    assert b is not None
    assert b.worker_id == "worker-B"

    with pytest.raises(LeaseLost):
        advance_job(session, a.id, JobStage.static, lease_id=a_lease)  # type: ignore[call-arg]


@requires_postgres
def test_heartbeat_extends_lease_and_blocks_reclaim(session):
    """A worker that heartbeats inside a long task must not be reclaimed.
    After the fix the sweep keys on lease_expires_at; the heartbeat
    extends it past now+ttl regardless of updated_at.
    """
    from db.models import JobStage, JobStatus
    from db.queue import claim_job, create_job, reclaim_stuck_jobs
    from workers.base import BaseWorker

    class _Probe(BaseWorker):
        stage = JobStage.discovery
        next_stage = JobStage.static
        poll_interval = 0.0

    job = create_job(session, {"address": "0x" + "c" * 40, "name": "heartbeating"})
    claimed = claim_job(session, JobStage.discovery, "worker-A")
    assert claimed is not None

    _backdate_job(session, job.id, seconds_ago=1000)

    probe = _Probe()
    probe._heartbeat(session, claimed)

    rescued = reclaim_stuck_jobs(session, stale_timeout_seconds=1)
    assert rescued == [], "heartbeat must keep the lease alive — sweep should leave the row"

    session.expire_all()
    refreshed = session.get(type(job), job.id)
    assert refreshed is not None
    assert refreshed.status == JobStatus.processing
    assert refreshed.worker_id == "worker-A"


@requires_postgres
def test_concurrent_claims_cannot_both_acquire_lease(session):
    """A live (non-expired) lease must block any sibling claim. After the
    fix the sweep keys on the explicit lease_expires_at column rather
    than updated_at, so this still holds when an unrelated write would
    otherwise have stamped updated_at.
    """
    from db.models import JobStage
    from db.queue import claim_job, create_job, reclaim_stuck_jobs

    create_job(session, {"address": "0x" + "d" * 40, "name": "live"})
    a = claim_job(session, JobStage.discovery, "worker-A")
    assert a is not None

    rescued = reclaim_stuck_jobs(session, stale_timeout_seconds=900)
    assert rescued == []

    other = claim_job(session, JobStage.discovery, "worker-B")
    assert other is None


# ---------------------------------------------------------------------------
# Contract row job_id race
#
# PoC for the static_worker terminal failure "Contract row not found for this
# job". Two jobs targeting the same (address, chain) — e.g. WETH discovered
# concurrently across protocols — collide on uq_contract_address_chain. The
# discovery writer at workers/discovery.py:402 unconditionally rebinds
# existing.job_id = job.id, orphaning whichever job ran first. A subsequent
# static_worker query keyed on Contract.job_id == job.id then turns up empty.
# ---------------------------------------------------------------------------


@requires_postgres
def test_contract_row_survives_concurrent_discovery_for_each_job(session):
    """PoC for the static_worker terminal failure 'Contract row not found
    for this job'. After two discovery writes for the same (address, chain),
    BOTH jobs' static_worker lookups must succeed — neither job's row may
    be orphaned. Today the second write at workers/discovery.py:402 does
    ``existing.job_id = job.id`` which clobbers the first job's binding.

    Goes green when discovery stops mutating job_id on the shared row
    (e.g. moves to a per-job pivot table or snapshots job_id elsewhere)."""
    from sqlalchemy import select

    from db.models import Contract
    from db.queue import create_job

    address = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"  # WETH
    chain = "ethereum"

    # Idempotent clean: session fixture only clears Job/Artifact/SourceFile;
    # Contract writes below are durable across runs and would collide on
    # uq_contract_address_chain.
    session.query(Contract).filter(Contract.address == address, Contract.chain == chain).delete()
    session.commit()

    try:
        job_a = create_job(session, {"address": address, "chain": chain, "name": "protocol-a"})
        job_b = create_job(session, {"address": address, "chain": chain, "name": "protocol-b"})

        # Job A's discovery writer — fresh insert with its job_id.
        contract = Contract(
            job_id=job_a.id,
            address=address,
            chain=chain,
            contract_name="WETH9",
            source_verified=True,
        )
        session.add(contract)
        session.commit()

        # Job B's discovery writer — exact mirror of workers/discovery.py:394-441.
        # The existing-row branch unconditionally rebinds job_id.
        existing = session.execute(
            select(Contract).where(Contract.address == address, Contract.chain == chain)
        ).scalar_one()
        existing.job_id = job_b.id
        existing.contract_name = "WETH9"
        session.commit()

        # Static worker query at workers/static_worker.py:677-681: must succeed
        # for BOTH jobs — each job needs to see ITS contract row.
        static_for_a = session.execute(
            select(Contract).where(Contract.job_id == job_a.id).limit(1)
        ).scalar_one_or_none()
        static_for_b = session.execute(
            select(Contract).where(Contract.job_id == job_b.id).limit(1)
        ).scalar_one_or_none()

        assert static_for_b is not None, "Job B should see its row (last writer)"
        assert static_for_a is not None, (
            "Job A should also see a Contract row — today it's orphaned by "
            "Job B's discovery writer rebinding existing.job_id, producing "
            "'Contract row not found for this job' in the static stage"
        )
    finally:
        session.query(Contract).filter(Contract.address == address, Contract.chain == chain).delete()
        session.commit()
