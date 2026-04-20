"""Integration tests for ``GET /api/audits/pipeline``.

Exercises the monitor-shelf endpoint against real PostgreSQL. Workers
don't need to run — each test directly seeds ``audit_reports`` rows in
the states the endpoint slices on (``NULL`` / ``processing`` / ``success``
/ ``failed``) and asserts the correct bucket picks them up.

Gated by ``requires_postgres``; object storage isn't touched.
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.conftest import requires_postgres  # noqa: E402

pytestmark = [requires_postgres]


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def seed_protocol(db_session):
    from db.models import AuditReport, Protocol

    name = f"pipe-{uuid.uuid4().hex[:10]}"
    p = Protocol(name=name)
    db_session.add(p)
    db_session.commit()
    pid = p.id
    try:
        yield pid, name
    finally:
        db_session.query(AuditReport).filter_by(protocol_id=pid).delete()
        db_session.query(Protocol).filter_by(id=pid).delete()
        db_session.commit()


def _insert_audit(
    db_session,
    protocol_id: int,
    *,
    text_status: str | None = None,
    text_started_at: datetime | None = None,
    text_extracted_at: datetime | None = None,
    text_error: str | None = None,
    text_worker: str | None = None,
    scope_status: str | None = None,
    scope_started_at: datetime | None = None,
    scope_extracted_at: datetime | None = None,
    scope_error: str | None = None,
    scope_worker: str | None = None,
    auditor: str = "Spearbit",
    title: str = "Test Audit",
    discovered_at: datetime | None = None,
) -> int:
    from db.models import AuditReport

    ar = AuditReport(
        protocol_id=protocol_id,
        url=f"https://example.com/{uuid.uuid4().hex}.pdf",
        pdf_url=f"https://example.com/{uuid.uuid4().hex}.pdf",
        auditor=auditor,
        title=title,
        date="2025-01-01",
        confidence=0.9,
        text_extraction_status=text_status,
        text_extraction_started_at=text_started_at,
        text_extracted_at=text_extracted_at,
        text_extraction_error=text_error,
        text_extraction_worker=text_worker,
        text_storage_key=(f"audits/text/placeholder-{uuid.uuid4().hex[:8]}.txt" if text_status == "success" else None),
        scope_extraction_status=scope_status,
        scope_extraction_started_at=scope_started_at,
        scope_extracted_at=scope_extracted_at,
        scope_extraction_error=scope_error,
        scope_extraction_worker=scope_worker,
    )
    if discovered_at is not None:
        ar.discovered_at = discovered_at
    db_session.add(ar)
    db_session.commit()
    return ar.id


# ---------------------------------------------------------------------------
# 1. Empty pipeline
# ---------------------------------------------------------------------------


def test_pipeline_empty_when_no_audits(api_client):
    """No audit rows at all → both worker panels empty but still well-shaped."""
    r = api_client.get("/api/audits/pipeline")
    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) == {"text_extraction", "scope_extraction", "generated_at"}
    for worker in ("text_extraction", "scope_extraction"):
        assert body[worker] == {"processing": [], "pending": [], "failed": []}


# ---------------------------------------------------------------------------
# 2. Bucket routing — rows land in the right column
# ---------------------------------------------------------------------------


def test_pipeline_places_rows_in_correct_buckets(db_session, api_client, seed_protocol):
    """A row in each terminal + non-terminal state lands in exactly the
    bucket the frontend expects."""
    pid, _ = seed_protocol
    now = datetime.now(timezone.utc)

    # Text extraction: pending (NULL), processing, success (terminal, ignored),
    # failed (recent → appears in failed bucket).
    pending_tid = _insert_audit(db_session, pid, text_status=None, auditor="Pending")
    proc_tid = _insert_audit(
        db_session,
        pid,
        text_status="processing",
        text_started_at=now - timedelta(seconds=30),
        text_worker="worker-a",
        auditor="Processing",
    )
    _insert_audit(db_session, pid, text_status="success", text_extracted_at=now, auditor="Ignored")
    failed_tid = _insert_audit(
        db_session,
        pid,
        text_status="failed",
        text_extracted_at=now - timedelta(hours=2),
        text_error="HTTP 404",
        auditor="Failed",
    )

    r = api_client.get("/api/audits/pipeline")
    assert r.status_code == 200
    te = r.json()["text_extraction"]

    assert {a["audit_id"] for a in te["pending"]} == {pending_tid}
    assert {a["audit_id"] for a in te["processing"]} == {proc_tid}
    assert {a["audit_id"] for a in te["failed"]} == {failed_tid}

    # Shape check on a processing row — every field the frontend card reads.
    proc = next(a for a in te["processing"] if a["audit_id"] == proc_tid)
    assert proc["company"] == seed_protocol[1]
    assert proc["auditor"] == "Processing"
    assert proc["worker_id"] == "worker-a"
    assert proc["started_at"] is not None
    assert isinstance(proc["elapsed_seconds"], int) and proc["elapsed_seconds"] >= 30

    # Failed row carries its error string so the monitor can show "why".
    failed = next(a for a in te["failed"] if a["audit_id"] == failed_tid)
    assert failed["error"] == "HTTP 404"


# ---------------------------------------------------------------------------
# 3. Scope pending is gated on text success
# ---------------------------------------------------------------------------


def test_scope_pending_excludes_unclaimable_rows(db_session, api_client, seed_protocol):
    """A scope row is only ``pending`` when text has already succeeded —
    otherwise the worker can't do anything with it and showing it in the
    monitor would misrepresent the work the scope worker actually has."""
    pid, _ = seed_protocol
    now = datetime.now(timezone.utc)

    # Text failed → scope unreachable; must NOT show up in scope pending.
    text_failed_id = _insert_audit(
        db_session,
        pid,
        text_status="failed",
        text_extracted_at=now - timedelta(hours=1),
        text_error="HTTP 500",
    )
    assert text_failed_id  # row exists, just not claimable for scope

    # Text still pending → scope also unclaimable.
    _insert_audit(db_session, pid, text_status=None)

    # Text succeeded + scope NULL → legitimately claimable for scope.
    claimable_id = _insert_audit(
        db_session,
        pid,
        text_status="success",
        text_extracted_at=now - timedelta(minutes=5),
    )

    r = api_client.get("/api/audits/pipeline")
    scope = r.json()["scope_extraction"]
    assert [a["audit_id"] for a in scope["pending"]] == [claimable_id]


# ---------------------------------------------------------------------------
# 4. Failed lookback window — stale failures drop out
# ---------------------------------------------------------------------------


def test_pipeline_excludes_failures_older_than_lookback(db_session, api_client, seed_protocol):
    """Only failures within the last 24h appear — older ones fade so the
    panel doesn't grow unbounded across weeks of accumulated misses."""
    pid, _ = seed_protocol
    now = datetime.now(timezone.utc)

    recent_id = _insert_audit(
        db_session,
        pid,
        text_status="failed",
        text_extracted_at=now - timedelta(hours=3),
        text_error="recent",
    )
    _insert_audit(  # >24h old — must not appear
        db_session,
        pid,
        text_status="failed",
        text_extracted_at=now - timedelta(days=3),
        text_error="stale",
    )

    r = api_client.get("/api/audits/pipeline")
    failed_ids = {a["audit_id"] for a in r.json()["text_extraction"]["failed"]}
    assert failed_ids == {recent_id}


# ---------------------------------------------------------------------------
# 5. Scope-stage state machine — pending → processing → failed / success
# ---------------------------------------------------------------------------


def test_scope_bucket_routing(db_session, api_client, seed_protocol):
    """The scope-extraction worker's three non-terminal states each route
    to their own bucket the same way text extraction does."""
    pid, _ = seed_protocol
    now = datetime.now(timezone.utc)

    pending = _insert_audit(
        db_session,
        pid,
        text_status="success",
        text_extracted_at=now,
        scope_status=None,
    )
    processing = _insert_audit(
        db_session,
        pid,
        text_status="success",
        text_extracted_at=now,
        scope_status="processing",
        scope_started_at=now - timedelta(minutes=2),
        scope_worker="scope-worker-b",
    )
    failed = _insert_audit(
        db_session,
        pid,
        text_status="success",
        text_extracted_at=now,
        scope_status="failed",
        scope_extracted_at=now - timedelta(hours=4),
        scope_error="LLM timeout",
    )
    _insert_audit(  # success — terminal, excluded
        db_session,
        pid,
        text_status="success",
        text_extracted_at=now,
        scope_status="success",
        scope_extracted_at=now,
    )

    r = api_client.get("/api/audits/pipeline")
    scope = r.json()["scope_extraction"]

    assert {a["audit_id"] for a in scope["pending"]} == {pending}
    assert {a["audit_id"] for a in scope["processing"]} == {processing}
    assert {a["audit_id"] for a in scope["failed"]} == {failed}

    # Processing row's worker_id is the SCOPE worker, not text — the
    # frontend shows "who's working on this right now".
    proc = next(a for a in scope["processing"] if a["audit_id"] == processing)
    assert proc["worker_id"] == "scope-worker-b"
    assert proc["error"] is None

    # Failed row's error is the scope error, not the text error.
    fail = next(a for a in scope["failed"] if a["audit_id"] == failed)
    assert fail["error"] == "LLM timeout"


# ---------------------------------------------------------------------------
# 6. Bucket cap — the endpoint never returns more than _PIPELINE_BUCKET_LIMIT
#    entries, protecting the monitor page from pathological backlogs
# ---------------------------------------------------------------------------


def test_pipeline_caps_buckets_at_limit(db_session, api_client, seed_protocol):
    """Seeding more rows than the cap in a single bucket still yields a
    bounded response. Prevents one stuck worker from bricking the monitor."""
    import api as api_module

    cap = api_module._PIPELINE_BUCKET_LIMIT
    pid, _ = seed_protocol

    for _ in range(cap + 10):
        _insert_audit(db_session, pid, text_status=None)

    r = api_client.get("/api/audits/pipeline")
    pending = r.json()["text_extraction"]["pending"]
    assert len(pending) == cap


# ---------------------------------------------------------------------------
# 7. Multi-protocol — company name is joined correctly per row
# ---------------------------------------------------------------------------


def test_pipeline_joins_protocol_name_per_row(db_session, api_client):
    """The monitor needs ``company`` on each item so the click-through to
    the protocol audit tab works. Verify rows from two protocols carry
    the correct name each."""
    from db.models import AuditContractCoverage, AuditReport, Protocol

    name_a = f"pipe-a-{uuid.uuid4().hex[:8]}"
    name_b = f"pipe-b-{uuid.uuid4().hex[:8]}"
    pa = Protocol(name=name_a)
    pb = Protocol(name=name_b)
    db_session.add_all([pa, pb])
    db_session.commit()
    pa_id, pb_id = pa.id, pb.id

    try:
        a_id = _insert_audit(db_session, pa_id, text_status="processing", auditor="A")
        b_id = _insert_audit(db_session, pb_id, text_status="processing", auditor="B")

        r = api_client.get("/api/audits/pipeline")
        processing = {a["audit_id"]: a for a in r.json()["text_extraction"]["processing"]}

        assert processing[a_id]["company"] == name_a
        assert processing[b_id]["company"] == name_b
    finally:
        db_session.query(AuditContractCoverage).filter(AuditContractCoverage.protocol_id.in_([pa_id, pb_id])).delete(
            synchronize_session=False
        )
        db_session.query(AuditReport).filter(AuditReport.protocol_id.in_([pa_id, pb_id])).delete(
            synchronize_session=False
        )
        db_session.query(Protocol).filter(Protocol.id.in_([pa_id, pb_id])).delete(synchronize_session=False)
        db_session.commit()


# ---------------------------------------------------------------------------
# 8. Pending ordering — oldest discovered first so FIFO matches worker claim
# ---------------------------------------------------------------------------


def test_text_pending_ordered_oldest_first(db_session, api_client, seed_protocol):
    """The monitor's pending list should match the order the worker will
    actually claim rows in — ``discovered_at`` ascending. Without this the
    top-of-list entry might be the *newest* audit, which misleads anyone
    watching a stuck queue."""
    pid, _ = seed_protocol
    now = datetime.now(timezone.utc)

    # Insert newest first so we're sure ordering isn't just insertion order.
    newer = _insert_audit(db_session, pid, text_status=None, discovered_at=now - timedelta(hours=1))
    older = _insert_audit(db_session, pid, text_status=None, discovered_at=now - timedelta(hours=5))
    middle = _insert_audit(db_session, pid, text_status=None, discovered_at=now - timedelta(hours=3))

    r = api_client.get("/api/audits/pipeline")
    ids_in_order = [a["audit_id"] for a in r.json()["text_extraction"]["pending"]]
    assert ids_in_order == [older, middle, newer]
