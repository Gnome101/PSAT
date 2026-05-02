"""Tests for ``_load_semantic_guards_with_v2_fallback`` — the
flag-controlled selection between native v1 ``semantic_guards``
and the v2-shim-derived synthetic.

Pinned because this is the lever that flips policy_worker from
v1 to v2 input. A regression in either direction (silently
running v2 when not flagged, or silently running v1 when flagged)
would invalidate the cutover.
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


_NATIVE_GUARDS_MARKER = {
    "schema_version": "0.1",
    "contract_address": "0xnative",
    "contract_name": "C",
    "functions": [
        {
            "function": "f()",
            "status": "resolved",
            "predicates": [{"kind": "caller_equals_controller", "controller_label": "owner"}],
            "guard_ids": ["g1"],
            "guard_kinds": ["access_control"],
            "controller_refs": ["ctl1"],
            "notes": [],
        }
    ],
}

_V2_PREDICATE_TREES = {
    "schema_version": "v2",
    "contract_name": "C",
    "trees": {
        "g()": {
            "op": "LEAF",
            "leaf": {
                "kind": "equality",
                "operator": "eq",
                "authority_role": "caller_authority",
                "operands": [
                    {"source": "msg_sender"},
                    {"source": "state_variable", "state_variable_name": "admin"},
                ],
                "references_msg_sender": True,
                "parameter_indices": [],
                "expression": "msg.sender == admin",
                "basis": [],
            },
        }
    },
}


@requires_postgres
def test_default_flag_returns_native_v1(db_session, monkeypatch):
    """No flag set — returns the native semantic_guards artifact
    unchanged, ignoring any v2 artifacts that exist."""
    from db.queue import store_artifact
    from workers.policy_worker import _load_semantic_guards_with_v2_fallback

    address = "0x" + uuid.uuid4().hex[:8] + "11" * 16
    job = _seed_completed_job(db_session, address=address)
    store_artifact(db_session, job.id, "semantic_guards", data=_NATIVE_GUARDS_MARKER)
    store_artifact(db_session, job.id, "predicate_trees", data=_V2_PREDICATE_TREES)
    db_session.commit()

    monkeypatch.delenv("PSAT_POLICY_USE_V2_SHIM", raising=False)
    out = _load_semantic_guards_with_v2_fallback(db_session, job)
    assert out is not None
    # Native marker is present; synthetic flag is NOT.
    assert out["contract_address"] == "0xnative"
    assert "_synthetic_from" not in out


@requires_postgres
def test_flag_on_returns_v2_synthetic(db_session, monkeypatch):
    """Flag on AND predicate_trees exists -> synthetic v2-derived
    guards. The native artifact is NOT consulted."""
    from db.queue import store_artifact
    from workers.policy_worker import _load_semantic_guards_with_v2_fallback

    address = "0x" + uuid.uuid4().hex[:8] + "22" * 16
    job = _seed_completed_job(db_session, address=address)
    # Both stored — flag should pick v2.
    store_artifact(db_session, job.id, "semantic_guards", data=_NATIVE_GUARDS_MARKER)
    store_artifact(db_session, job.id, "predicate_trees", data=_V2_PREDICATE_TREES)
    db_session.commit()

    monkeypatch.setenv("PSAT_POLICY_USE_V2_SHIM", "true")
    out = _load_semantic_guards_with_v2_fallback(db_session, job)
    assert out is not None
    assert out["_synthetic_from"] == "v2_predicate_trees"
    # Synthetic shape carries g() (from v2 trees), not f() (from native marker).
    fn_names = [f["function"] for f in out["functions"]]
    assert fn_names == ["g()"]


@requires_postgres
def test_flag_on_but_no_v2_artifact_falls_back_to_native(db_session, monkeypatch):
    """Flag on but no predicate_trees artifact (legacy contract)
    -> fall back to native v1, log warning."""
    from db.queue import store_artifact
    from workers.policy_worker import _load_semantic_guards_with_v2_fallback

    address = "0x" + uuid.uuid4().hex[:8] + "33" * 16
    job = _seed_completed_job(db_session, address=address)
    store_artifact(db_session, job.id, "semantic_guards", data=_NATIVE_GUARDS_MARKER)
    db_session.commit()

    monkeypatch.setenv("PSAT_POLICY_USE_V2_SHIM", "true")
    out = _load_semantic_guards_with_v2_fallback(db_session, job)
    assert out is not None
    assert out["contract_address"] == "0xnative"
    assert "_synthetic_from" not in out


@requires_postgres
def test_flag_on_shim_failure_falls_back_to_native(db_session, monkeypatch):
    """Flag on AND v2 artifact exists, but the shim itself
    raises mid-translation -> fall back to native, log
    exception. Strictly safer than failing the whole job."""
    from db.queue import store_artifact
    from workers.policy_worker import _load_semantic_guards_with_v2_fallback

    address = "0x" + uuid.uuid4().hex[:8] + "44" * 16
    job = _seed_completed_job(db_session, address=address)
    store_artifact(db_session, job.id, "semantic_guards", data=_NATIVE_GUARDS_MARKER)
    store_artifact(db_session, job.id, "predicate_trees", data=_V2_PREDICATE_TREES)
    db_session.commit()

    monkeypatch.setenv("PSAT_POLICY_USE_V2_SHIM", "true")

    def _boom(*a, **kw):
        raise RuntimeError("simulated shim failure")

    monkeypatch.setattr(
        "services.static.contract_analysis_pipeline.v2_to_v1_shim."
        "synthesize_semantic_guards_from_predicate_trees",
        _boom,
    )
    out = _load_semantic_guards_with_v2_fallback(db_session, job)
    assert out is not None
    assert out["contract_address"] == "0xnative"
    assert "_synthetic_from" not in out


@requires_postgres
def test_neither_artifact_present_returns_none(db_session, monkeypatch):
    """Bare job with neither artifact -> returns None (the
    existing contract for callers; effective_permissions handles
    None gracefully)."""
    from workers.policy_worker import _load_semantic_guards_with_v2_fallback

    address = "0x" + uuid.uuid4().hex[:8] + "55" * 16
    job = _seed_completed_job(db_session, address=address)

    monkeypatch.setenv("PSAT_POLICY_USE_V2_SHIM", "true")
    assert _load_semantic_guards_with_v2_fallback(db_session, job) is None

    monkeypatch.delenv("PSAT_POLICY_USE_V2_SHIM", raising=False)
    assert _load_semantic_guards_with_v2_fallback(db_session, job) is None


@requires_postgres
def test_flag_truthy_variants_all_enable_v2(db_session, monkeypatch):
    """``true`` / ``1`` / ``yes`` / ``on`` (case-insensitive) all
    enable the v2 path. Pinned so a typo in the env-var doesn't
    silently keep production on v1 when an operator thinks they
    flipped."""
    from db.queue import store_artifact
    from workers.policy_worker import _load_semantic_guards_with_v2_fallback

    address = "0x" + uuid.uuid4().hex[:8] + "66" * 16
    job = _seed_completed_job(db_session, address=address)
    store_artifact(db_session, job.id, "semantic_guards", data=_NATIVE_GUARDS_MARKER)
    store_artifact(db_session, job.id, "predicate_trees", data=_V2_PREDICATE_TREES)
    db_session.commit()

    for value in ("true", "TRUE", "1", "yes", "ON"):
        monkeypatch.setenv("PSAT_POLICY_USE_V2_SHIM", value)
        out = _load_semantic_guards_with_v2_fallback(db_session, job)
        assert out["_synthetic_from"] == "v2_predicate_trees", value


@requires_postgres
def test_flag_falsy_variants_all_use_native(db_session, monkeypatch):
    """Empty / ``0`` / ``false`` / arbitrary text all keep the
    native v1 path. Default off until explicitly opted in."""
    from db.queue import store_artifact
    from workers.policy_worker import _load_semantic_guards_with_v2_fallback

    address = "0x" + uuid.uuid4().hex[:8] + "77" * 16
    job = _seed_completed_job(db_session, address=address)
    store_artifact(db_session, job.id, "semantic_guards", data=_NATIVE_GUARDS_MARKER)
    store_artifact(db_session, job.id, "predicate_trees", data=_V2_PREDICATE_TREES)
    db_session.commit()

    for value in ("", "0", "false", "no", "off", "maybe"):
        monkeypatch.setenv("PSAT_POLICY_USE_V2_SHIM", value)
        out = _load_semantic_guards_with_v2_fallback(db_session, job)
        assert "_synthetic_from" not in out, value
        assert out["contract_address"] == "0xnative"
