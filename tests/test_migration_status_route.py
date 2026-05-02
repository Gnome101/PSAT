"""End-to-end tests for ``GET /api/v2/migration_status`` — the
HTTP face of ``scripts/cutover_dry_run.run_dry_run``."""

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


def _no_auth(api_module):
    api_module.app.dependency_overrides[api_module.require_admin_key] = lambda: None


def _v1(name, *fns_with_kinds):
    return {
        "subject": {"name": name},
        "access_control": {
            "privileged_functions": [
                {"function": fn, "guard_kinds": list(kinds)}
                for fn, kinds in fns_with_kinds
            ]
        },
    }


def _v2(name, *fns_with_roles):
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


def _seed_completed_job(db_session, *, address):
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
def test_migration_status_aggregates_fleet(api_client, db_session):
    import api as api_module

    _no_auth(api_module)
    pfx = uuid.uuid4().hex[:6]
    addr_clean = "0x" + pfx + "00" * 17
    addr_reg = "0x" + pfx + "11" * 17

    _seed_with_artifacts(
        db_session,
        address=addr_clean,
        v1=_v1("Clean", ("f()", ["access_control"])),
        v2=_v2("Clean", ("f()", ["caller_authority"])),
    )
    _seed_with_artifacts(
        db_session,
        address=addr_reg,
        v1=_v1("Reg", ("f()", ["access_control"]), ("g()", ["pause"])),
        v2=_v2("Reg", ("f()", ["caller_authority"])),
    )

    resp = api_client.get(
        f"/api/v2/migration_status?address_prefix=0x{pfx}"
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total_addresses"] == 2
    assert body["counts"]["clean"] == 1
    assert body["counts"]["regression"] == 1
    assert body["safe_to_cut_count"] == 1
    assert body["safe_pct"] == pytest.approx(50.0)
    assert addr_reg in [r["address"] for r in body["regressions"]]


@requires_postgres
def test_migration_status_address_prefix_filter(api_client, db_session):
    import api as api_module

    _no_auth(api_module)
    pfx_a = uuid.uuid4().hex[:6]
    pfx_b = uuid.uuid4().hex[:6]
    addr_a = "0x" + pfx_a + "11" * 17
    addr_b = "0x" + pfx_b + "11" * 17
    for addr in (addr_a, addr_b):
        _seed_with_artifacts(
            db_session,
            address=addr,
            v1=_v1("X", ("f()", ["access_control"])),
            v2=_v2("X", ("f()", ["caller_authority"])),
        )
    resp = api_client.get(f"/api/v2/migration_status?address_prefix=0x{pfx_a}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_addresses"] == 1
    assert addr_a in body["safe_addresses"]


@requires_postgres
def test_migration_status_max_regressions_caps_list(api_client, db_session):
    import api as api_module

    _no_auth(api_module)
    pfx = uuid.uuid4().hex[:6]
    for i in range(3):
        addr = "0x" + pfx + f"{i:02x}" + "11" * 16
        _seed_with_artifacts(
            db_session,
            address=addr,
            v1=_v1("R", ("f()", ["access_control"]), ("g()", ["pause"])),
            v2=_v2("R", ("f()", ["caller_authority"])),
        )
    resp = api_client.get(
        f"/api/v2/migration_status?address_prefix=0x{pfx}&max_regressions=2"
    )
    body = resp.json()
    assert body["counts"]["regression"] == 3
    assert len(body["regressions"]) == 2
