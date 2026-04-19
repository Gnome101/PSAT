"""Unit tests for the end-of-pipeline ``CoverageWorker``.

Exercises the readiness-gated claim, the stuck-job escape hatch, and the
source-equivalence-enabled refresh path. Network calls into
``services.audits.source_equivalence`` are stubbed at module scope so
the real coverage code runs end-to-end without GitHub / Etherscan
traffic — per the test-hygiene rule in the handoff prompt: never rely
on env-var-controlled divergence; stub the network helpers directly.
"""

from __future__ import annotations

import hashlib
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.conftest import requires_postgres  # noqa: E402

pytestmark = [requires_postgres]


# ---------------------------------------------------------------------------
# Network-stubbing fixture — every test in this module gets it.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _stub_source_equivalence_network(monkeypatch):
    """Replace GitHub + Etherscan helpers with deterministic no-ops.

    Tests that need a positive equivalence result override these with a
    local monkeypatch; by default both return None so no match is proven
    and the temporal matcher's answer stands.
    """
    from services.audits import source_equivalence

    monkeypatch.setattr(source_equivalence, "fetch_github_source_hash", lambda *a, **k: None)
    monkeypatch.setattr(source_equivalence, "fetch_etherscan_source_files", lambda *a, **k: None)


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def worker():
    """CoverageWorker with signals patched so pytest's handlers aren't touched.

    Tests call ``_claim_next_job``, ``_claim_stuck_job``, and ``process``
    directly against ``db_session``; the inherited ``run_loop`` (which
    opens its own ``SessionLocal``) is never exercised here.
    """
    from unittest.mock import patch

    from workers.coverage_worker import CoverageWorker

    with patch("signal.signal"):
        yield CoverageWorker()


@pytest.fixture()
def seed_protocol(db_session):
    """Bare protocol with cleanup of cascading rows + lingering Jobs."""
    from db.models import AuditContractCoverage, AuditReport, Contract, Job, Protocol, UpgradeEvent

    name = f"cov-worker-{uuid.uuid4().hex[:10]}"
    p = Protocol(name=name)
    db_session.add(p)
    db_session.commit()
    protocol_id = p.id
    try:
        yield protocol_id, name
    finally:
        db_session.rollback()
        db_session.query(AuditContractCoverage).filter_by(protocol_id=protocol_id).delete()
        contract_ids = [c.id for c in db_session.query(Contract).filter_by(protocol_id=protocol_id).all()]
        if contract_ids:
            db_session.query(UpgradeEvent).filter(UpgradeEvent.contract_id.in_(contract_ids)).delete(
                synchronize_session=False
            )
        # Jobs have ON DELETE SET NULL on protocol_id; clean them up by
        # (protocol_id + jobs whose contract we just deleted).
        job_ids = {c.job_id for c in db_session.query(Contract).filter_by(protocol_id=protocol_id).all() if c.job_id}
        db_session.query(Contract).filter_by(protocol_id=protocol_id).delete()
        db_session.query(AuditReport).filter_by(protocol_id=protocol_id).delete()
        if job_ids:
            db_session.query(Job).filter(Job.id.in_(job_ids)).delete(synchronize_session=False)
        db_session.query(Job).filter_by(protocol_id=protocol_id).delete()
        db_session.query(Protocol).filter_by(id=protocol_id).delete()
        db_session.commit()


def _add_contract(session, *, protocol_id: int, name: str, address: str, job_id=None):
    from db.models import Contract

    c = Contract(
        protocol_id=protocol_id,
        address=address.lower(),
        chain="ethereum",
        contract_name=name,
        job_id=job_id,
    )
    session.add(c)
    session.commit()
    return c


def _add_job(
    session,
    *,
    protocol_id: int | None,
    stage,
    status,
    address: str = "0x" + "e" * 40,
    updated_at: datetime | None = None,
):
    """Insert a Job at the given stage/status, optionally backdated."""
    from db.models import Job

    j = Job(
        address=address.lower(),
        protocol_id=protocol_id,
        stage=stage,
        status=status,
        request={"address": address.lower()},
    )
    session.add(j)
    session.commit()
    if updated_at is not None:
        # Force the updated_at column — server_default/onupdate would
        # otherwise stamp NOW(), which defeats the stuck-job test.
        from sqlalchemy import update as sa_update

        from db.models import Job as _Job

        session.execute(sa_update(_Job).where(_Job.id == j.id).values(updated_at=updated_at))
        session.commit()
        session.refresh(j)
    return j


def _add_audit(
    session,
    *,
    protocol_id: int,
    text_status: str | None,
    scope_status: str | None,
    scope: list[str] | None = None,
    date: str | None = "2024-06-01",
):
    """Create an AuditReport in a specific (text, scope) state pair."""
    from db.models import AuditReport

    ar = AuditReport(
        protocol_id=protocol_id,
        url=f"https://example.com/{uuid.uuid4().hex}.pdf",
        auditor="T",
        title="T",
        date=date,
        confidence=0.9,
        text_extraction_status=text_status,
        scope_extraction_status=scope_status,
        scope_contracts=scope,
    )
    session.add(ar)
    session.commit()
    return ar


# ---------------------------------------------------------------------------
# 1. Happy path — claim + process + write coverage rows
# ---------------------------------------------------------------------------


def test_coverage_worker_claims_and_writes_when_ready(db_session, seed_protocol, worker):
    """Audit is fully scoped, job is queued at stage=coverage → claim
    succeeds, process() writes one coverage row for the contract whose
    name matches scope, and advance_job routes it to done.
    """
    from db.models import AuditContractCoverage, JobStage, JobStatus

    protocol_id, _ = seed_protocol
    # Contract linked via job_id so the worker's Contract-by-job lookup succeeds.
    job = _add_job(
        db_session,
        protocol_id=protocol_id,
        stage=JobStage.coverage,
        status=JobStatus.queued,
    )
    contract = _add_contract(
        db_session,
        protocol_id=protocol_id,
        name="Pool",
        address="0x" + "a" * 40,
        job_id=job.id,
    )
    # Audit scoping "Pool" — settled via text+scope success.
    _add_audit(
        db_session,
        protocol_id=protocol_id,
        text_status="success",
        scope_status="success",
        scope=["Pool"],
    )

    claimed = worker._claim_next_job(db_session)
    assert claimed is not None
    assert claimed.id == job.id
    assert claimed.status == JobStatus.processing

    worker.process(db_session, claimed)
    db_session.commit()
    db_session.expire_all()

    rows = (
        db_session.execute(select(AuditContractCoverage).where(AuditContractCoverage.contract_id == contract.id))
        .scalars()
        .all()
    )
    assert len(rows) == 1
    assert rows[0].matched_name == "Pool"
    assert rows[0].match_type == "direct"


def test_coverage_worker_process_upgrades_via_source_equivalence(db_session, seed_protocol, worker, monkeypatch):
    """When reviewed_commits + source_repo + a file hash match an impl's
    Etherscan verified source, process() writes a ``reviewed_commit`` row
    — not just the temporal ``direct`` match. Exercises the full pipeline
    path ``CoverageWorker.process → upsert_coverage_for_contract(..., verify=True)
    → _reviewed_commit_upgrades → source_equivalence``.
    """
    from db.models import AuditContractCoverage, JobStage, JobStatus
    from services.audits import source_equivalence

    protocol_id, _ = seed_protocol
    job = _add_job(
        db_session,
        protocol_id=protocol_id,
        stage=JobStage.coverage,
        status=JobStatus.queued,
    )
    contract = _add_contract(
        db_session,
        protocol_id=protocol_id,
        name="Pool",
        address="0x" + "a" * 40,
        job_id=job.id,
    )
    audit = _add_audit(
        db_session,
        protocol_id=protocol_id,
        text_status="success",
        scope_status="success",
        scope=["Pool"],
    )
    audit.reviewed_commits = ["abc1234"]
    audit.source_repo = "some/repo"
    db_session.commit()

    content = "contract Pool {}"
    h = hashlib.sha256(content.encode()).hexdigest()
    monkeypatch.setattr(
        source_equivalence,
        "fetch_etherscan_source_files",
        lambda _addr: source_equivalence.VerifiedSource(
            contract_name="Pool",
            compiler_version="0.8",
            files={"src/Pool.sol": h},
        ),
    )
    monkeypatch.setattr(
        source_equivalence,
        "fetch_github_source_hash",
        lambda _repo, _commit, path, token=None: h if path == "src/Pool.sol" else None,
    )

    claimed = worker._claim_next_job(db_session)
    assert claimed is not None
    worker.process(db_session, claimed)
    db_session.commit()
    db_session.expire_all()

    rows = (
        db_session.execute(select(AuditContractCoverage).where(AuditContractCoverage.contract_id == contract.id))
        .scalars()
        .all()
    )
    assert len(rows) == 1
    assert rows[0].match_type == "reviewed_commit"
    assert rows[0].match_confidence == "high"


# ---------------------------------------------------------------------------
# 2. Readiness blocking — an unsettled audit prevents claim
# ---------------------------------------------------------------------------


def test_coverage_worker_waits_for_text_extraction(db_session, seed_protocol, worker):
    """An audit whose text_extraction_status='processing' keeps readiness
    false → claim returns None. Once it's flipped to success + scope
    success, the next claim picks the job up.
    """
    from db.models import AuditReport, JobStage, JobStatus

    protocol_id, _ = seed_protocol
    job = _add_job(
        db_session,
        protocol_id=protocol_id,
        stage=JobStage.coverage,
        status=JobStatus.queued,
    )
    _add_contract(
        db_session,
        protocol_id=protocol_id,
        name="Pool",
        address="0x" + "a" * 40,
        job_id=job.id,
    )
    audit = _add_audit(
        db_session,
        protocol_id=protocol_id,
        text_status="processing",
        scope_status=None,
    )

    # Readiness predicate is false — claim returns nothing.
    assert worker._claim_next_job(db_session) is None

    # Flip text → success but leave scope NULL. Still blocked (scope mid-flight).
    ar = db_session.get(AuditReport, audit.id)
    ar.text_extraction_status = "success"
    db_session.commit()
    assert worker._claim_next_job(db_session) is None

    # Now mark scope success with a non-matching scope. Settled → claim succeeds.
    ar = db_session.get(AuditReport, audit.id)
    ar.scope_extraction_status = "success"
    ar.scope_contracts = ["SomethingElse"]
    db_session.commit()

    claimed = worker._claim_next_job(db_session)
    assert claimed is not None
    assert claimed.id == job.id


def test_coverage_worker_unblocks_on_text_extraction_failure(db_session, seed_protocol, worker):
    """An audit whose text extraction ``failed`` leaves scope_extraction_status
    NULL forever. The readiness predicate must treat that as settled — NOT
    blocked — otherwise a single bad PDF would wedge every coverage job in
    the protocol until the stuck-job timeout kicked in.
    """
    from db.models import JobStage, JobStatus

    protocol_id, _ = seed_protocol
    job = _add_job(
        db_session,
        protocol_id=protocol_id,
        stage=JobStage.coverage,
        status=JobStatus.queued,
    )
    _add_contract(
        db_session,
        protocol_id=protocol_id,
        name="Pool",
        address="0x" + "a" * 40,
        job_id=job.id,
    )
    _add_audit(
        db_session,
        protocol_id=protocol_id,
        text_status="failed",
        scope_status=None,
    )

    claimed = worker._claim_next_job(db_session)
    assert claimed is not None
    assert claimed.id == job.id


# ---------------------------------------------------------------------------
# 3. Stuck-audit timeout — bypass readiness after cutoff
# ---------------------------------------------------------------------------


def test_coverage_worker_claims_stuck_job_past_timeout(db_session, seed_protocol, worker, monkeypatch):
    """Job has been at stage=coverage, status=queued for > timeout AND
    an audit is still mid-flight (readiness predicate false). The stuck
    claim path bypasses readiness so the job doesn't hang forever.
    """
    import workers.coverage_worker as worker_mod
    from db.models import JobStage, JobStatus

    # Collapse the timeout so we don't have to actually backdate by an hour.
    monkeypatch.setattr(worker_mod, "_STUCK_COVERAGE_TIMEOUT", 60)

    protocol_id, _ = seed_protocol
    _add_audit(
        db_session,
        protocol_id=protocol_id,
        text_status="processing",
        scope_status=None,
    )
    past = datetime.now(timezone.utc) - timedelta(seconds=600)
    job = _add_job(
        db_session,
        protocol_id=protocol_id,
        stage=JobStage.coverage,
        status=JobStatus.queued,
        updated_at=past,
    )
    _add_contract(
        db_session,
        protocol_id=protocol_id,
        name="Pool",
        address="0x" + "a" * 40,
        job_id=job.id,
    )

    # Readiness-gated claim still blocked by the processing audit.
    assert worker._claim_next_job(db_session) is None

    # Stuck path picks it up.
    claimed = worker._claim_stuck_job(db_session)
    assert claimed is not None
    assert claimed.id == job.id
    assert claimed.status == JobStatus.processing


# ---------------------------------------------------------------------------
# 4. Edge — job.protocol_id is NULL (direct address submission)
# ---------------------------------------------------------------------------


def test_coverage_worker_claims_job_with_null_protocol(db_session, worker):
    """A direct address job (no parent company → protocol_id NULL) has
    no audits to wait on. The NOT EXISTS subquery is vacuously true, so
    claim fires immediately and process() runs a no-op coverage refresh.
    """
    from db.models import AuditContractCoverage, Contract, JobStage, JobStatus

    job = _add_job(
        db_session,
        protocol_id=None,
        stage=JobStage.coverage,
        status=JobStatus.queued,
    )
    # Contract linked to the job but with protocol_id NULL — no scope
    # name can match (match_audits_for_contract short-circuits), so the
    # upsert is a zero-row no-op.
    contract = Contract(
        protocol_id=None,
        address="0x" + "c" * 40,
        chain="ethereum",
        contract_name="SomeContract",
        job_id=job.id,
    )
    db_session.add(contract)
    db_session.commit()

    try:
        claimed = worker._claim_next_job(db_session)
        assert claimed is not None
        assert claimed.id == job.id

        worker.process(db_session, claimed)
        db_session.commit()

        rows = (
            db_session.execute(select(AuditContractCoverage).where(AuditContractCoverage.contract_id == contract.id))
            .scalars()
            .all()
        )
        assert rows == []
    finally:
        db_session.query(Contract).filter_by(id=contract.id).delete()
        db_session.query(type(job)).filter_by(id=job.id).delete()
        db_session.commit()


def test_coverage_worker_handles_job_without_contract(db_session, seed_protocol, worker):
    """If discovery/static never produced a Contract row for the job (e.g.
    a cached-path reassignment edge case), process() should log and
    return without crashing — the outer run_loop will then advance to done.
    """
    from db.models import JobStage, JobStatus

    protocol_id, _ = seed_protocol
    _add_job(
        db_session,
        protocol_id=protocol_id,
        stage=JobStage.coverage,
        status=JobStatus.queued,
    )
    # No Contract row for this job_id.

    claimed = worker._claim_next_job(db_session)
    assert claimed is not None

    # Should not raise.
    worker.process(db_session, claimed)
    db_session.commit()


# ---------------------------------------------------------------------------
# 5. Perf: HTTP calls must not run inside an open DB transaction
# ---------------------------------------------------------------------------


def test_coverage_worker_does_not_hold_transaction_during_http(db_session, seed_protocol, worker, monkeypatch):
    """S5: the coverage worker must not keep a Postgres transaction open
    while running GitHub / Etherscan HTTP calls. A held row-lock during a
    15s HTTP timeout blocks every other writer touching the same rows.

    We track the "is a transaction currently open?" flag via SQLAlchemy
    engine events, and assert the flag is False every time the stubbed
    source-equivalence HTTP helper is invoked.
    """
    from sqlalchemy import event

    from db.models import JobStage, JobStatus
    from services.audits import source_equivalence

    protocol_id, _ = seed_protocol
    job = _add_job(
        db_session,
        protocol_id=protocol_id,
        stage=JobStage.coverage,
        status=JobStatus.queued,
    )
    _add_contract(
        db_session,
        protocol_id=protocol_id,
        name="Pool",
        address="0x" + "a" * 40,
        job_id=job.id,
    )
    audit = _add_audit(
        db_session,
        protocol_id=protocol_id,
        text_status="success",
        scope_status="success",
        scope=["Pool"],
    )
    audit.reviewed_commits = ["abc1234"]
    audit.source_repo = "some/repo"
    db_session.commit()

    # Flush the fixture's own transaction so the "begin" events below fire
    # for the worker's writes, not pre-existing test setup.
    db_session.close()

    # Track active transactions on the engine. begin/commit events fire for
    # every implicit-begin in SQLAlchemy 2.x.
    tx_depth = {"value": 0}

    def on_begin(conn):
        tx_depth["value"] += 1

    def on_commit(conn):
        tx_depth["value"] = max(0, tx_depth["value"] - 1)

    def on_rollback(conn):
        tx_depth["value"] = max(0, tx_depth["value"] - 1)

    engine = db_session.bind
    event.listen(engine, "begin", on_begin)
    event.listen(engine, "commit", on_commit)
    event.listen(engine, "rollback", on_rollback)

    # Record tx_depth at the moment each HTTP helper is called.
    http_tx_depths: list[tuple[str, int]] = []

    def record_github(repo, commit, path, *, token=None):
        http_tx_depths.append(("github", tx_depth["value"]))
        return None

    def record_etherscan(address):
        http_tx_depths.append(("etherscan", tx_depth["value"]))
        return source_equivalence.VerifiedSource(
            contract_name="Pool",
            compiler_version="0.8",
            files={"src/Pool.sol": "deadbeef"},
        )

    monkeypatch.setattr(source_equivalence, "fetch_github_source_hash", record_github)
    monkeypatch.setattr(source_equivalence, "fetch_etherscan_source_files", record_etherscan)

    # Use a fresh session (the worker does too) so fixture tx state is
    # isolated from the worker's transaction activity.
    from sqlalchemy.orm import Session as _Session

    session = _Session(engine, expire_on_commit=False)
    try:
        claimed = worker._claim_next_job(session)
        assert claimed is not None

        worker.process(session, claimed)
        session.commit()
    finally:
        session.close()
        event.remove(engine, "begin", on_begin)
        event.remove(engine, "commit", on_commit)
        event.remove(engine, "rollback", on_rollback)

    # At least one HTTP call must have been made (via scope-name "Pool" +
    # reviewed_commits populated).
    assert http_tx_depths, "expected source-equivalence HTTP helpers to be invoked"
    for label, depth in http_tx_depths:
        assert depth == 0, f"{label} HTTP call happened inside an open transaction (depth={depth})"
