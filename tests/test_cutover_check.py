"""Tests for the cutover-gate service + HTTP endpoint.

Two layers:
  - ``cutover_check_for_address`` against synthetic v1+v2 artifacts
    — pins each severity bucket and the None/prerequisites paths.
  - ``GET /api/contract/{addr}/v1_v2_diff`` — the admin-gated
    audit surface for #18.
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


requires_postgres = pytest.mark.skipif(not _can_connect(), reason="PostgreSQL not available")


def _seed_completed_job(db_session, *, address: str):
    from db.models import Job, JobStage, JobStatus

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
    db_session.commit()
    return job


def _v1_artifact(name: str, *fns_with_kinds: tuple[str, list[str]]) -> dict:
    return {
        "subject": {"name": name},
        "access_control": {
            "privileged_functions": [{"function": fn, "guard_kinds": list(kinds)} for fn, kinds in fns_with_kinds]
        },
    }


def _v2_artifact(name: str, *fns_with_roles: tuple[str, list[str]]) -> dict:
    def _tree(roles: list[str]) -> dict:
        leaf = {
            "kind": "equality",
            "operator": "eq",
            "authority_role": roles[0] if roles else "business",
            "operands": [],
            "references_msg_sender": True,
            "parameter_indices": [],
            "expression": "",
            "basis": [],
        }
        return {"op": "LEAF", "leaf": leaf}

    return {
        "schema_version": "v2",
        "contract_name": name,
        "trees": {fn: _tree(list(roles)) for fn, roles in fns_with_roles},
    }


# ---------------------------------------------------------------------------
# Service-layer
# ---------------------------------------------------------------------------


@requires_postgres
def test_returns_none_when_no_job(db_session):
    from services.static.contract_analysis_pipeline.cutover_check import (
        cutover_check_for_address,
    )

    out = cutover_check_for_address(db_session, address="0x" + "ee" * 20)
    assert out is None


@requires_postgres
def test_returns_none_when_v1_artifact_missing(db_session):
    from db.queue import store_artifact
    from services.static.contract_analysis_pipeline.cutover_check import (
        cutover_check_for_address,
    )

    address = "0x" + uuid.uuid4().hex[:8] + "01" * 16
    job = _seed_completed_job(db_session, address=address)
    # Only v2 stored.
    store_artifact(db_session, job.id, "predicate_trees", data=_v2_artifact("C", ("f()", ["caller_authority"])))
    db_session.commit()

    out = cutover_check_for_address(db_session, address=address)
    assert out is None


@requires_postgres
def test_returns_none_when_v2_artifact_missing(db_session):
    """Legacy pre-v2 contract — v1 artifact present, v2 not yet
    written. ``cutover_check_for_address`` returns None to signal
    'not eligible for cutover yet'."""
    from db.queue import store_artifact
    from services.static.contract_analysis_pipeline.cutover_check import (
        cutover_check_for_address,
    )

    address = "0x" + uuid.uuid4().hex[:8] + "02" * 16
    job = _seed_completed_job(db_session, address=address)
    store_artifact(
        db_session,
        job.id,
        "contract_analysis",
        data=_v1_artifact("C", ("f()", ["access_control"])),
    )
    db_session.commit()

    out = cutover_check_for_address(db_session, address=address)
    assert out is None


@requires_postgres
def test_clean_severity_when_artifacts_match(db_session):
    from db.queue import store_artifact
    from services.static.contract_analysis_pipeline.cutover_check import (
        cutover_check_for_address,
        is_safe_to_cut_over,
    )

    address = "0x" + uuid.uuid4().hex[:8] + "03" * 16
    job = _seed_completed_job(db_session, address=address)
    store_artifact(
        db_session,
        job.id,
        "contract_analysis",
        data=_v1_artifact("C", ("f()", ["access_control"])),
    )
    store_artifact(
        db_session,
        job.id,
        "predicate_trees",
        data=_v2_artifact("C", ("f()", ["caller_authority"])),
    )
    db_session.commit()

    out = cutover_check_for_address(db_session, address=address)
    assert out is not None
    assert out["severity"] == "clean"
    assert out["address"] == address.lower()
    assert out["agreed"] == ["f()"]
    assert out["v1_only"] == []
    assert out["v2_only"] == []
    assert is_safe_to_cut_over(out) is True


@requires_postgres
def test_regression_severity_blocks_cutover(db_session):
    from db.queue import store_artifact
    from services.static.contract_analysis_pipeline.cutover_check import (
        cutover_check_for_address,
        is_safe_to_cut_over,
    )

    address = "0x" + uuid.uuid4().hex[:8] + "04" * 16
    job = _seed_completed_job(db_session, address=address)
    # v1 sees 2 functions; v2 only sees 1 -> regression.
    store_artifact(
        db_session,
        job.id,
        "contract_analysis",
        data=_v1_artifact("C", ("f()", ["access_control"]), ("g()", ["pause"])),
    )
    store_artifact(
        db_session,
        job.id,
        "predicate_trees",
        data=_v2_artifact("C", ("f()", ["caller_authority"])),
    )
    db_session.commit()

    out = cutover_check_for_address(db_session, address=address)
    assert out["severity"] == "regression"
    assert out["v1_only"] == ["g()"]
    assert is_safe_to_cut_over(out) is False


@requires_postgres
def test_new_coverage_is_safe_to_cut(db_session):
    from db.queue import store_artifact
    from services.static.contract_analysis_pipeline.cutover_check import (
        cutover_check_for_address,
        is_safe_to_cut_over,
    )

    address = "0x" + uuid.uuid4().hex[:8] + "05" * 16
    job = _seed_completed_job(db_session, address=address)
    store_artifact(
        db_session,
        job.id,
        "contract_analysis",
        data=_v1_artifact("C", ("f()", ["access_control"])),
    )
    store_artifact(
        db_session,
        job.id,
        "predicate_trees",
        data=_v2_artifact("C", ("f()", ["caller_authority"]), ("g()", ["caller_authority"])),
    )
    db_session.commit()

    out = cutover_check_for_address(db_session, address=address)
    assert out["severity"] == "new_coverage"
    assert out["v2_only"] == ["g()"]
    assert is_safe_to_cut_over(out) is True


@requires_postgres
def test_role_drift_requires_review(db_session):
    from db.queue import store_artifact
    from services.static.contract_analysis_pipeline.cutover_check import (
        cutover_check_for_address,
        is_safe_to_cut_over,
    )

    address = "0x" + uuid.uuid4().hex[:8] + "06" * 16
    job = _seed_completed_job(db_session, address=address)
    # v1 says access_control, v2 says pause -> role_drift.
    store_artifact(
        db_session,
        job.id,
        "contract_analysis",
        data=_v1_artifact("C", ("f()", ["access_control"])),
    )
    store_artifact(
        db_session,
        job.id,
        "predicate_trees",
        data=_v2_artifact("C", ("f()", ["pause"])),
    )
    db_session.commit()

    out = cutover_check_for_address(db_session, address=address)
    assert out["severity"] == "role_drift"
    assert "f()" in out["role_disagreements"]
    assert is_safe_to_cut_over(out) is False


# ---------------------------------------------------------------------------
# HTTP route
# ---------------------------------------------------------------------------


def _no_auth(api_module):
    # Auth dependency moved to routers.deps after main's routers refactor.
    from routers.deps import require_admin_key

    api_module.app.dependency_overrides[require_admin_key] = lambda: None


@requires_postgres
def test_route_returns_diff_payload(api_client, db_session):
    import api as api_module
    from db.queue import store_artifact

    _no_auth(api_module)
    address = "0x" + uuid.uuid4().hex[:8] + "07" * 16
    job = _seed_completed_job(db_session, address=address)
    store_artifact(
        db_session,
        job.id,
        "contract_analysis",
        data=_v1_artifact("C", ("f()", ["access_control"])),
    )
    store_artifact(
        db_session,
        job.id,
        "predicate_trees",
        data=_v2_artifact("C", ("f()", ["caller_authority"])),
    )
    db_session.commit()

    resp = api_client.get(f"/api/contract/{address}/v1_v2_diff")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["severity"] == "clean"
    assert body["safe_to_cut_over"] is True
    assert body["address"] == address.lower()
    assert body["agreed"] == ["f()"]


@requires_postgres
def test_route_404_when_prerequisites_missing(api_client, db_session):
    import api as api_module

    _no_auth(api_module)
    resp = api_client.get(f"/api/contract/0x{'ee' * 20}/v1_v2_diff")
    assert resp.status_code == 404
    assert "Re-analyze before evaluating" in resp.json()["detail"]
