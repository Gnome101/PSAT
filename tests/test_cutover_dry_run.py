"""Tests for the fleet-wide cutover dry-run aggregator."""

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


def _v1(name: str, *fns_with_kinds):
    return {
        "subject": {"name": name},
        "access_control": {
            "privileged_functions": [{"function": fn, "guard_kinds": list(kinds)} for fn, kinds in fns_with_kinds]
        },
    }


def _v2(name: str, *fns_with_roles):
    def _tree(roles):
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
    return job


def _seed_with_artifacts(db_session, *, address, v1, v2):
    from db.queue import store_artifact

    job = _seed_completed_job(db_session, address=address)
    if v1 is not None:
        store_artifact(db_session, job.id, "contract_analysis", data=v1)
    if v2 is not None:
        store_artifact(db_session, job.id, "predicate_trees", data=v2)
    db_session.commit()
    return job


@requires_postgres
def test_dry_run_aggregates_severities(db_session):
    """Mixed fleet: clean + new_coverage + regression + role_drift +
    not_eligible. The dry-run aggregates each into per-severity
    counts and surfaces the offending addresses for the operator."""
    from scripts.cutover_dry_run import run_dry_run

    pfx = uuid.uuid4().hex[:6]
    clean = "0x" + pfx + "00" * 17
    cov = "0x" + pfx + "11" * 17
    reg = "0x" + pfx + "22" * 17
    drift = "0x" + pfx + "33" * 17
    not_elig = "0x" + pfx + "44" * 17

    _seed_with_artifacts(
        db_session,
        address=clean,
        v1=_v1("Clean", ("f()", ["access_control"])),
        v2=_v2("Clean", ("f()", ["caller_authority"])),
    )
    _seed_with_artifacts(
        db_session,
        address=cov,
        v1=_v1("Cov", ("f()", ["access_control"])),
        v2=_v2("Cov", ("f()", ["caller_authority"]), ("g()", ["caller_authority"])),
    )
    _seed_with_artifacts(
        db_session,
        address=reg,
        v1=_v1("Reg", ("f()", ["access_control"]), ("g()", ["pause"])),
        v2=_v2("Reg", ("f()", ["caller_authority"])),
    )
    _seed_with_artifacts(
        db_session,
        address=drift,
        v1=_v1("Drift", ("f()", ["access_control"])),
        v2=_v2("Drift", ("f()", ["pause"])),
    )
    _seed_with_artifacts(db_session, address=not_elig, v1=None, v2=None)

    out = run_dry_run(db_session, address_prefix="0x" + pfx)
    assert out["total_addresses"] == 5
    assert out["eligible"] == 4
    assert out["counts"]["clean"] == 1
    assert out["counts"]["new_coverage"] == 1
    assert out["counts"]["regression"] == 1
    assert out["counts"]["role_drift"] == 1
    assert out["counts"]["not_eligible"] == 1
    assert out["safe_to_cut_count"] == 2
    # safe_pct against eligible (not total).
    assert out["safe_pct"] == pytest.approx(50.0)
    assert reg in [r["address"] for r in out["regressions"]]
    assert drift in [r["address"] for r in out["role_drifts"]]
    assert not_elig in out["not_eligible_sample"]
    assert clean in out["safe_addresses"]
    assert cov in out["safe_addresses"]


@requires_postgres
def test_dry_run_address_prefix_filter(db_session):
    """``address_prefix`` narrows the scan; addresses outside the
    prefix don't show up in any bucket."""
    from scripts.cutover_dry_run import run_dry_run

    target_pfx = uuid.uuid4().hex[:6]
    other_pfx = uuid.uuid4().hex[:6]
    in_addr = "0x" + target_pfx + "11" * 17
    out_addr = "0x" + other_pfx + "11" * 17
    _seed_with_artifacts(
        db_session,
        address=in_addr,
        v1=_v1("A", ("f()", ["access_control"])),
        v2=_v2("A", ("f()", ["caller_authority"])),
    )
    _seed_with_artifacts(
        db_session,
        address=out_addr,
        v1=_v1("B", ("f()", ["access_control"])),
        v2=_v2("B", ("f()", ["caller_authority"])),
    )

    out = run_dry_run(db_session, address_prefix="0x" + target_pfx)
    assert out["total_addresses"] == 1
    assert in_addr in out["safe_addresses"]


@requires_postgres
def test_dry_run_max_regressions_caps_list(db_session):
    from scripts.cutover_dry_run import run_dry_run

    pfx = uuid.uuid4().hex[:6]
    for i in range(3):
        addr = "0x" + pfx + f"{i:02x}" + "11" * 16
        _seed_with_artifacts(
            db_session,
            address=addr,
            v1=_v1("R", ("f()", ["access_control"]), ("g()", ["pause"])),
            v2=_v2("R", ("f()", ["caller_authority"])),
        )

    out = run_dry_run(db_session, address_prefix="0x" + pfx, max_regressions=2)
    # All 3 contribute to the count, but the regression LIST is
    # capped at 2 for output volume.
    assert out["counts"]["regression"] == 3
    assert len(out["regressions"]) == 2


@requires_postgres
def test_dry_run_uses_most_recent_job_per_address(db_session):
    """Address with multiple completed Jobs — the dry-run uses the
    most-recent (same selection rule cutover_check_for_address
    itself uses)."""
    from db.queue import store_artifact
    from scripts.cutover_dry_run import run_dry_run

    pfx = uuid.uuid4().hex[:6]
    addr = "0x" + pfx + "ee" * 17
    older = _seed_completed_job(db_session, address=addr)
    older.created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    older.updated_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    newer = _seed_completed_job(db_session, address=addr)
    db_session.flush()
    # Older has v1 only -> would not be eligible. Newer has both
    # artifacts and matches cleanly. The dry-run must see the
    # newer one.
    store_artifact(
        db_session,
        older.id,
        "contract_analysis",
        data=_v1("Old", ("f()", ["access_control"])),
    )
    store_artifact(
        db_session,
        newer.id,
        "contract_analysis",
        data=_v1("New", ("f()", ["access_control"])),
    )
    store_artifact(
        db_session,
        newer.id,
        "predicate_trees",
        data=_v2("New", ("f()", ["caller_authority"])),
    )
    db_session.commit()

    out = run_dry_run(db_session, address_prefix="0x" + pfx)
    assert out["total_addresses"] == 1
    assert out["counts"].get("clean") == 1
    assert addr in out["safe_addresses"]
