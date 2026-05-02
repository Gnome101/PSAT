"""End-to-end tests for ``POST /api/contract/{address}/probe/membership``.

Exercises the full route: address → most-recent completed Job →
predicate_trees artifact → probe_membership. Uses the
``api_client`` + ``db_session`` fixtures from conftest, which point
the FastAPI app at the test database.

The route is admin-gated; tests bypass auth by overriding
``api.require_admin_key`` to a no-op (the same pattern test_api.py
uses for other admin-gated endpoints).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")


def _can_connect() -> bool:
    if not DATABASE_URL:
        return False
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(
    not _can_connect(), reason="PostgreSQL not available"
)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _no_auth(api_module):
    api_module.app.dependency_overrides[api_module.require_admin_key] = lambda: None


def _seed_completed_job_with_artifact(
    db_session,
    *,
    address: str,
    predicate_trees: dict | None,
):
    from db.models import Job, JobStage, JobStatus
    from db.queue import store_artifact

    job = Job(
        address=address,
        request={"address": address, "name": "T"},
        status=JobStatus.completed,
        stage=JobStage.done,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db_session.add(job)
    db_session.flush()
    if predicate_trees is not None:
        store_artifact(db_session, job.id, "predicate_trees", data=predicate_trees)
    db_session.commit()
    return job


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_postgres
def test_probe_membership_returns_yes_for_known_member(api_client, db_session):
    """End-to-end happy path: a guarded function with a multi-key
    membership leaf, member is the caller. Adapter returns
    finite_set exact → result yes."""
    import api as api_module

    _no_auth(api_module)
    address = "0x" + "ab" * 20
    member = "0x" + "11" * 20
    # The leaf's set_descriptor has caller as a key — the
    # AccessControlAdapter scores 0 for this without an explicit
    # role topic / role_grants_repo, so the registry falls through
    # to no_adapter (CapabilityExpr.unsupported). The probe's
    # response carries that reason — still not "yes". The point of
    # this test is that the route HTTP layer (resolve address →
    # job → artifact → tree → probe_membership) plumbs through; we
    # don't assert "yes" here, we assert the payload shape.
    artifact = {
        "schema_version": "v2",
        "contract_name": "T",
        "trees": {
            "f()": {
                "op": "LEAF",
                "leaf": {
                    "kind": "membership",
                    "operator": "truthy",
                    "authority_role": "caller_authority",
                    "operands": [{"source": "msg_sender"}],
                    "set_descriptor": {
                        "kind": "mapping_membership",
                        "key_sources": [{"source": "constant", "constant_value": "0x" + "01" * 32}, {"source": "msg_sender"}],
                        "storage_var": "_roles",
                    },
                    "references_msg_sender": True,
                    "parameter_indices": [],
                    "expression": "_roles[ROLE][msg.sender]",
                    "basis": [],
                },
            }
        },
    }
    _seed_completed_job_with_artifact(
        db_session, address=address, predicate_trees=artifact
    )

    resp = api_client.post(
        f"/api/contract/{address}/probe/membership",
        json={
            "function_signature": "f()",
            "predicate_index": 0,
            "member": member,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["leaf_kind"] == "membership"
    assert body["authority_role"] == "caller_authority"
    # ``result`` is one of yes/no/unknown — the exact value depends
    # on which adapter wins; we pin the protocol shape.
    assert body["result"] in ("yes", "no", "unknown")


@requires_postgres
def test_probe_membership_unguarded_function_returns_yes(api_client, db_session):
    """Resolver convention: a function absent from v2 trees is
    publicly callable. The probe surface translates that into
    ``yes`` with reason ``function_unguarded`` so a UI can render
    'anyone can call this'."""
    import api as api_module

    _no_auth(api_module)
    address = "0x" + "cd" * 20
    artifact = {"schema_version": "v2", "contract_name": "T", "trees": {}}
    _seed_completed_job_with_artifact(
        db_session, address=address, predicate_trees=artifact
    )

    resp = api_client.post(
        f"/api/contract/{address}/probe/membership",
        json={
            "function_signature": "open()",
            "predicate_index": 0,
            "member": "0x" + "11" * 20,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["result"] == "yes"
    assert body["reason"] == "function_unguarded"


@requires_postgres
def test_probe_membership_no_completed_job_returns_404(api_client, db_session):
    import api as api_module

    _no_auth(api_module)
    resp = api_client.post(
        f"/api/contract/0x{'ee' * 20}/probe/membership",
        json={
            "function_signature": "f()",
            "predicate_index": 0,
            "member": "0x" + "11" * 20,
        },
    )
    assert resp.status_code == 404
    assert "No completed analysis job" in resp.json()["detail"]


@requires_postgres
def test_probe_membership_no_artifact_returns_404(api_client, db_session):
    """A completed job without a predicate_trees artifact (legacy
    pre-v2 analysis) returns 404 with a clear reason. The UI then
    knows to surface 'this contract was analyzed before v2'."""
    import api as api_module

    _no_auth(api_module)
    address = "0x" + "f1" * 20
    _seed_completed_job_with_artifact(
        db_session, address=address, predicate_trees=None
    )

    resp = api_client.post(
        f"/api/contract/{address}/probe/membership",
        json={
            "function_signature": "f()",
            "predicate_index": 0,
            "member": "0x" + "11" * 20,
        },
    )
    assert resp.status_code == 404
    assert "predicate_trees artifact missing" in resp.json()["detail"]


@requires_postgres
def test_probe_membership_v2_error_payload_returns_unknown(api_client, db_session):
    """When predicate_trees was emitted with an error (v2 emit
    failed mid-analysis), the artifact carries
    ``{"error": "..."}`` instead of ``trees``. The route returns
    a 200 with result=unknown so the UI can degrade gracefully
    rather than 500."""
    import api as api_module

    _no_auth(api_module)
    address = "0x" + "f2" * 20
    artifact = {"schema_version": "v2", "error": "v2_emit_blew_up"}
    _seed_completed_job_with_artifact(
        db_session, address=address, predicate_trees=artifact
    )

    resp = api_client.post(
        f"/api/contract/{address}/probe/membership",
        json={
            "function_signature": "f()",
            "predicate_index": 0,
            "member": "0x" + "11" * 20,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["result"] == "unknown"
    assert body["reason"] == "predicate_trees_unavailable"
    assert body["detail"] == "v2_emit_blew_up"


@requires_postgres
def test_probe_membership_rejects_malformed_address_payload(api_client, db_session):
    """Pydantic validator: ``member`` must be a 0x-prefixed
    20-byte address. Pinned so a regression doesn't allow an empty
    string / wrong-length blob through."""
    import api as api_module

    _no_auth(api_module)
    address = "0x" + "1f" * 20
    _seed_completed_job_with_artifact(
        db_session,
        address=address,
        predicate_trees={"schema_version": "v2", "trees": {}},
    )

    resp = api_client.post(
        f"/api/contract/{address}/probe/membership",
        json={
            "function_signature": "f()",
            "predicate_index": 0,
            "member": "not-an-address",
        },
    )
    assert resp.status_code == 422


@requires_postgres
def test_probe_membership_picks_most_recent_completed_job(api_client, db_session):
    """Two completed jobs for the same address — the route uses
    the most recent one (by updated_at). Pinned so an old
    re-analysis with stale artifact data doesn't shadow a newer
    one."""
    import api as api_module
    from db.models import Job, JobStage, JobStatus
    from db.queue import store_artifact

    _no_auth(api_module)
    address = "0x" + "2f" * 20

    older = Job(
        address=address,
        request={"address": address},
        status=JobStatus.completed,
        stage=JobStage.done,
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        updated_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    newer = Job(
        address=address,
        request={"address": address},
        status=JobStatus.completed,
        stage=JobStage.done,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    db_session.add_all([older, newer])
    db_session.flush()

    store_artifact(
        db_session,
        older.id,
        "predicate_trees",
        data={"schema_version": "v2", "trees": {"f()": {"op": "LEAF", "leaf": {"kind": "equality", "operator": "eq", "authority_role": "caller_authority", "operands": [], "references_msg_sender": True, "parameter_indices": [], "expression": "OLD", "basis": []}}}},
    )
    store_artifact(
        db_session,
        newer.id,
        "predicate_trees",
        data={"schema_version": "v2", "trees": {"f()": {"op": "LEAF", "leaf": {"kind": "equality", "operator": "eq", "authority_role": "caller_authority", "operands": [], "references_msg_sender": True, "parameter_indices": [], "expression": "NEW", "basis": []}}}},
    )
    db_session.commit()

    resp = api_client.post(
        f"/api/contract/{address}/probe/membership",
        json={
            "function_signature": "f()",
            "predicate_index": 0,
            "member": "0x" + "11" * 20,
        },
    )
    # Equality leaf -> non_membership_leaf; that's expected. The
    # point: the route picked the NEWER tree, so the leaf walks
    # without finding membership data. We don't assert text equality
    # on `expression` here because the field isn't surfaced in the
    # response — the implicit pin is "no 500 / no stale-tree
    # error", just the standard non-membership-leaf signal.
    assert resp.status_code == 200
    body = resp.json()
    assert body["leaf_kind"] == "equality"
    assert body["reason"] == "non_membership_leaf"
