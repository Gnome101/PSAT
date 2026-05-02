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
def test_capabilities_response_is_cached(api_client, db_session, monkeypatch):
    """Repeat hits within the TTL window short-circuit the
    resolver — proven by counting resolve_contract_capabilities
    invocations across two requests."""
    import api as api_module
    from services.resolution import capability_resolver as resolver_mod

    address = "0x" + uuid.uuid4().hex[:8] + "ca" * 16
    _seed_completed_job_with_artifact(
        db_session, address=address, predicate_trees=_equality_leaf_artifact()
    )

    # Empty the cache so the test starts clean (other tests may
    # have warmed it).
    api_module._capabilities_cache.clear()
    monkeypatch.setattr(api_module, "_CAPABILITIES_CACHE_TTL_S", 60.0)

    calls = {"n": 0}
    original = resolver_mod.resolve_contract_capabilities

    def _counting(*args, **kwargs):
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(resolver_mod, "resolve_contract_capabilities", _counting)
    # The api route imports at handler time, so patch its reference too.
    monkeypatch.setattr(
        "api.SessionLocal",
        api_module.SessionLocal,  # no-op; ensures import path stable
        raising=False,
    )

    r1 = api_client.get(f"/api/contract/{address}/capabilities")
    r2 = api_client.get(f"/api/contract/{address}/capabilities")
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json() == r2.json()
    # Resolver invoked once across the two requests (second was a
    # cache hit).
    assert calls["n"] == 1


@requires_postgres
def test_capabilities_cache_ttl_disabled_when_zero(api_client, db_session, monkeypatch):
    """``PSAT_CAPABILITIES_CACHE_TTL_S=0`` (or default-overridden
    to 0) disables caching entirely — every request runs the
    resolver fresh."""
    import api as api_module
    from services.resolution import capability_resolver as resolver_mod

    address = "0x" + uuid.uuid4().hex[:8] + "cb" * 16
    _seed_completed_job_with_artifact(
        db_session, address=address, predicate_trees=_equality_leaf_artifact()
    )

    api_module._capabilities_cache.clear()
    monkeypatch.setattr(api_module, "_CAPABILITIES_CACHE_TTL_S", 0.0)

    calls = {"n": 0}
    original = resolver_mod.resolve_contract_capabilities

    def _counting(*args, **kwargs):
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(resolver_mod, "resolve_contract_capabilities", _counting)

    api_client.get(f"/api/contract/{address}/capabilities")
    api_client.get(f"/api/contract/{address}/capabilities")
    assert calls["n"] == 2  # both requests hit the resolver


@requires_postgres
def test_capabilities_cache_keyed_on_block_and_chain(api_client, db_session, monkeypatch):
    """Different ``block`` or ``chain_id`` parameters cache
    independently — no cross-contamination between e.g. mainnet
    and polygon, or between point-in-time queries."""
    import api as api_module
    from services.resolution import capability_resolver as resolver_mod

    address = "0x" + uuid.uuid4().hex[:8] + "cc" * 16
    _seed_completed_job_with_artifact(
        db_session, address=address, predicate_trees=_equality_leaf_artifact()
    )

    api_module._capabilities_cache.clear()
    monkeypatch.setattr(api_module, "_CAPABILITIES_CACHE_TTL_S", 60.0)

    calls = {"n": 0}
    original = resolver_mod.resolve_contract_capabilities

    def _counting(*args, **kwargs):
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(resolver_mod, "resolve_contract_capabilities", _counting)

    api_client.get(f"/api/contract/{address}/capabilities")  # default chain=1, block=None
    api_client.get(f"/api/contract/{address}/capabilities?chain_id=137")
    api_client.get(f"/api/contract/{address}/capabilities?block=18000000")
    # Three distinct keys -> three resolver calls.
    assert calls["n"] == 3


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
