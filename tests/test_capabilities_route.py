"""End-to-end tests for ``GET /api/contract/{address}/capabilities``.

Read path for the schema-v2 cutover (#18). Read-only and not
admin-gated (idempotent, no side effects), so tests skip the
``require_admin_key`` override.
"""

from __future__ import annotations

import os
import sys
import uuid
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


def _seed_completed_job_with_artifact(db_session, *, address: str, predicate_trees):
    from db.models import Job, JobStage, JobStatus
    from db.queue import store_artifact

    job = Job(
        address=address,
        request={"address": address},
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


def _equality_leaf_artifact(contract_name: str = "T") -> dict:
    return {
        "schema_version": "v2",
        "contract_name": contract_name,
        "trees": {
            "f()": {
                "op": "LEAF",
                "leaf": {
                    "kind": "equality",
                    "operator": "eq",
                    "authority_role": "caller_authority",
                    "operands": [
                        {"source": "msg_sender"},
                        {"source": "state_variable", "state_variable_name": "owner"},
                    ],
                    "references_msg_sender": True,
                    "parameter_indices": [],
                    "expression": "msg.sender == owner",
                    "basis": [],
                },
            }
        },
    }


@requires_postgres
def test_capabilities_returns_per_function_dict(api_client, db_session):
    address = "0x" + uuid.uuid4().hex[:8] + "a1" * 16
    _seed_completed_job_with_artifact(
        db_session, address=address, predicate_trees=_equality_leaf_artifact()
    )

    resp = api_client.get(f"/api/contract/{address}/capabilities")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["contract_address"] == address.lower()
    assert body["chain_id"] == 1
    assert body["block"] is None
    assert "f()" in body["capabilities"]
    cap = body["capabilities"]["f()"]
    assert "kind" in cap
    assert "confidence" in cap
    assert "membership_quality" in cap


@requires_postgres
def test_capabilities_returns_404_for_unknown_address(api_client, db_session):
    resp = api_client.get(f"/api/contract/0x{'ee' * 20}/capabilities")
    assert resp.status_code == 404
    assert "No v2 capabilities" in resp.json()["detail"]


@requires_postgres
def test_capabilities_returns_404_for_legacy_pre_v2_contract(api_client, db_session):
    """A completed Job without a predicate_trees artifact (legacy
    pre-v2 analysis). The route returns 404 with the documented
    fallback note so a UI knows to query the v1 endpoints."""
    address = "0x" + uuid.uuid4().hex[:8] + "b2" * 16
    _seed_completed_job_with_artifact(
        db_session, address=address, predicate_trees=None
    )
    resp = api_client.get(f"/api/contract/{address}/capabilities")
    assert resp.status_code == 404
    assert "predates the schema-v2 emit" in resp.json()["detail"]


@requires_postgres
def test_capabilities_empty_dict_for_unguarded_only_contract(api_client, db_session):
    """A contract with no guarded functions returns 200 with
    capabilities={}; consumers know the contract IS analyzed but
    every function is implicitly public."""
    address = "0x" + uuid.uuid4().hex[:8] + "c3" * 16
    _seed_completed_job_with_artifact(
        db_session,
        address=address,
        predicate_trees={"schema_version": "v2", "contract_name": "T", "trees": {}},
    )
    resp = api_client.get(f"/api/contract/{address}/capabilities")
    assert resp.status_code == 200
    body = resp.json()
    assert body["capabilities"] == {}


@requires_postgres
def test_capabilities_block_query_param(api_client, db_session):
    """``block=N`` supports point-in-time queries — the response
    echoes the block back so a UI can display 'as of block N'."""
    address = "0x" + uuid.uuid4().hex[:8] + "d4" * 16
    _seed_completed_job_with_artifact(
        db_session, address=address, predicate_trees=_equality_leaf_artifact()
    )
    resp = api_client.get(
        f"/api/contract/{address}/capabilities", params={"block": 18_000_000}
    )
    assert resp.status_code == 200
    assert resp.json()["block"] == 18_000_000


@requires_postgres
def test_capabilities_chain_id_query_param(api_client, db_session):
    """``chain_id`` defaults to 1 (mainnet) but is overridable for
    multi-chain contracts."""
    address = "0x" + uuid.uuid4().hex[:8] + "e5" * 16
    _seed_completed_job_with_artifact(
        db_session, address=address, predicate_trees=_equality_leaf_artifact()
    )
    resp = api_client.get(
        f"/api/contract/{address}/capabilities", params={"chain_id": 137}
    )
    assert resp.status_code == 200
    assert resp.json()["chain_id"] == 137


@requires_postgres
def test_capabilities_route_is_not_admin_gated(api_client, db_session):
    """Verify no X-PSAT-Admin-Key header is required — the route
    is read-only / idempotent so anyone can hit it. Pinned because
    accidentally adding require_admin_key would lock external
    consumers out."""
    import api as api_module

    # No dependency override — let the real require_admin_key run
    # if it's wired (it shouldn't be, on this route).
    api_module.app.dependency_overrides.pop(api_module.require_admin_key, None)

    address = "0x" + uuid.uuid4().hex[:8] + "f6" * 16
    _seed_completed_job_with_artifact(
        db_session, address=address, predicate_trees=_equality_leaf_artifact()
    )
    # Send with no X-PSAT-Admin-Key header -> still 200.
    resp = api_client.get(f"/api/contract/{address}/capabilities")
    assert resp.status_code == 200
