from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.cache_helpers import requires_postgres  # noqa: E402


def _make_fake_job(
    job_id: str | None = None,
    address: str | None = "0xabc",
    name: str = "demo_run",
    status: str = "completed",
    stage: str = "done",
):
    """Build a mock Job object that looks like db.models.Job."""
    job = MagicMock()
    job.id = uuid.UUID(job_id) if job_id else uuid.uuid4()
    job.address = address
    job.company = None
    job.name = name
    job.status = MagicMock(value=status)
    job.stage = MagicMock(value=stage)
    job.detail = "Test detail"
    job.request = {"address": address}
    job.error = None
    job.worker_id = None
    job.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    job.updated_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    job.to_dict.return_value = {
        "job_id": str(job.id),
        "address": address,
        "company": None,
        "name": name,
        "status": status,
        "stage": stage,
        "detail": "Test detail",
        "request": {"address": address},
        "error": None,
        "worker_id": None,
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    return job


def make_client() -> TestClient:
    import api

    return TestClient(api.app)


def test_build_company_function_entry_filters_generic_authority_contract_when_specific_principals_exist() -> None:
    import api

    ef = SimpleNamespace(
        abi_signature="pauseContract()",
        function_name="pauseContract",
        selector="0x439766ce",
        effect_labels=["pause_toggle"],
        effect_targets=["paused"],
        action_summary="Changes the contract pause state.",
        authority_public=False,
        authority_roles=[],
    )
    principals = [
        SimpleNamespace(
            address="0xrole",
            resolved_type="contract",
            origin="roleRegistry",
            principal_type="controller",
            details={"authority_kind": "access_control_like"},
        ),
        SimpleNamespace(
            address="0xsafe",
            resolved_type="safe",
            origin="PROTOCOL_PAUSER",
            principal_type="controller",
            details={"threshold": 4, "owners": ["0x1", "0x2", "0x3", "0x4"]},
        ),
        SimpleNamespace(
            address="0xeoa",
            resolved_type="eoa",
            origin="PROTOCOL_PAUSER",
            principal_type="controller",
            details={},
        ),
    ]

    result = api._build_company_function_entry(cast(Any, ef), cast(Any, principals))

    assert result["controllers"] == [
        {
            "label": "PROTOCOL_PAUSER",
            "controller_id": "PROTOCOL_PAUSER",
            "source": "PROTOCOL_PAUSER",
            "principals": [
                {
                    "address": "0xsafe",
                    "resolved_type": "safe",
                    "source_controller_id": "PROTOCOL_PAUSER",
                    "details": {"threshold": 4, "owners": ["0x1", "0x2", "0x3", "0x4"]},
                },
                {
                    "address": "0xeoa",
                    "resolved_type": "eoa",
                    "source_controller_id": "PROTOCOL_PAUSER",
                    "details": {},
                },
            ],
        }
    ]


def test_index_serves_html() -> None:
    client = make_client()
    response = client.get("/")
    assert response.status_code == 200
    assert "Run an address and inspect the control surface" in response.text


def test_spa_fallback_serves_html_for_deep_link() -> None:
    client = make_client()
    response = client.get("/address/0x1234567890123456789012345678901234567890/graph")
    assert response.status_code == 200
    assert "Run an address and inspect the control surface" in response.text


@requires_postgres
def test_health_and_config_endpoints(api_client) -> None:
    health = api_client.get("/api/health")
    config = api_client.get("/api/config")

    assert health.status_code == 200
    payload = health.json()
    assert payload["status"] == "ok"
    assert payload["db"] == "ok"
    # conftest scrubs ARTIFACT_STORAGE_*, so this client sees the inline path.
    assert payload["storage"] == "inline"
    assert config.status_code == 200
    assert "default_rpc_url" in config.json()


def test_version_endpoint_returns_git_sha(api_client, monkeypatch) -> None:
    monkeypatch.setenv("GIT_SHA", "deadbeef")
    r = api_client.get("/api/version")
    assert r.status_code == 200
    assert r.json() == {"sha": "deadbeef"}

    monkeypatch.delenv("GIT_SHA", raising=False)
    r = api_client.get("/api/version")
    assert r.json() == {"sha": "unknown"}


def test_admin_key_required_for_non_get(monkeypatch) -> None:
    """Without a valid admin key, write endpoints must return 401."""
    import api

    # Drop the conftest override for this test only and force a known key.
    api.app.dependency_overrides.pop(api.require_admin_key, None)
    monkeypatch.setattr(api, "ADMIN_KEY", "real-key")

    client = TestClient(api.app)

    no_header = client.post(
        "/api/analyze",
        json={"address": "0x1234567890123456789012345678901234567890", "name": "demo"},
    )
    bad_header = client.post(
        "/api/analyze",
        json={"address": "0x1234567890123456789012345678901234567890", "name": "demo"},
        headers={"X-PSAT-Admin-Key": "wrong"},
    )
    assert no_header.status_code == 401
    assert bad_header.status_code == 401


@requires_postgres
def test_cors_allows_configured_origin(monkeypatch, db_session) -> None:
    """Configured origins should be reflected in CORS responses."""
    from tests.conftest import SessionFactory

    monkeypatch.setenv("PSAT_SITE_ORIGIN", "https://psat.example.com")
    import importlib

    import api

    importlib.reload(api)
    try:
        # The reload reset SessionLocal to the prod-default engine; point it
        # back at the test DB so /api/health can reach Postgres.
        api.SessionLocal = SessionFactory(db_session)
        api.app.dependency_overrides[api.require_admin_key] = lambda: None
        client = TestClient(api.app)

        response = client.get("/api/health", headers={"Origin": "https://psat.example.com"})
        assert response.status_code == 200
        assert response.headers.get("access-control-allow-origin") == "https://psat.example.com"
    finally:
        monkeypatch.delenv("PSAT_SITE_ORIGIN", raising=False)
        importlib.reload(api)


@patch("api.SessionLocal")
@patch("api.create_job")
def test_analyze_endpoint_creates_job(mock_create_job, mock_session_cls) -> None:
    client = make_client()

    fake_job = _make_fake_job(status="queued", stage="discovery")
    fake_job.to_dict.return_value["job_id"] = "job-1"
    mock_create_job.return_value = fake_job

    mock_session = MagicMock()
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    response = client.post(
        "/api/analyze",
        json={"address": "0x1234567890123456789012345678901234567890", "name": "demo"},
    )

    assert response.status_code == 200
    assert response.json()["job_id"] == "job-1"


@patch("api.SessionLocal")
@patch("api.create_job")
def test_company_analyze_endpoint(mock_create_job, mock_session_cls) -> None:
    client = make_client()

    fake_job = _make_fake_job(status="queued", stage="discovery")
    fake_job.to_dict.return_value["job_id"] = "job-2"
    mock_create_job.return_value = fake_job

    mock_session = MagicMock()
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    response = client.post(
        "/api/analyze",
        json={"company": "etherfi", "chain": "ethereum", "analyze_limit": 3},
    )

    assert response.status_code == 200
    assert response.json()["job_id"] == "job-2"


def test_analyze_endpoint_rejects_bad_address() -> None:
    client = make_client()
    response = client.post("/api/analyze", json={"address": "123", "name": "demo"})
    assert response.status_code == 422


def test_analyze_endpoint_requires_at_least_one_target() -> None:
    client = make_client()

    missing = client.post("/api/analyze", json={"name": "demo"})

    assert missing.status_code == 422


@patch("api.SessionLocal")
def test_get_job_and_missing_job(mock_session_cls) -> None:
    client = make_client()
    fake_job = _make_fake_job()

    mock_session = MagicMock()
    mock_session.get.side_effect = lambda cls, id: fake_job if id == str(fake_job.id) else None
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    existing = client.get(f"/api/jobs/{fake_job.id}")
    missing = client.get(f"/api/jobs/{uuid.uuid4()}")

    assert existing.status_code == 200
    assert existing.json()["status"] == "completed"
    assert missing.status_code == 404


@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_analyses_detail(mock_session_cls, mock_get_all_artifacts) -> None:
    client = make_client()
    fake_job = _make_fake_job(name="demo_run", address="0xabc")

    mock_session = MagicMock()
    mock_execute = MagicMock()
    mock_execute.scalar_one_or_none.return_value = fake_job
    mock_session.execute.return_value = mock_execute
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    mock_get_all_artifacts.return_value = {
        "contract_analysis": {"subject": {"name": "Demo"}, "summary": {"control_model": "ownable"}},
    }

    detail = client.get("/api/analyses/demo_run")

    assert detail.status_code == 200
    assert detail.json()["run_name"] == "demo_run"


@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_missing_analysis_returns_404(mock_session_cls, mock_get_all_artifacts) -> None:
    client = make_client()

    mock_session = MagicMock()
    mock_execute = MagicMock()
    mock_execute.scalar_one_or_none.return_value = None
    mock_session.execute.return_value = mock_execute
    mock_session.get.return_value = None
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    response = client.get("/api/analyses/missing")
    assert response.status_code == 404


@patch("api.SessionLocal")
def test_artifact_endpoint_serves_json_and_text(mock_session_cls) -> None:
    """Inline-stored artifacts (no storage_key) are served directly."""
    client = make_client()
    fake_job = _make_fake_job(name="demo_run")

    fake_json_artifact = MagicMock()
    fake_json_artifact.storage_key = None
    fake_json_artifact.data = {"summary": {"control_model": "ownable"}}
    fake_json_artifact.text_data = None
    fake_json_artifact.content_type = "application/json"

    fake_text_artifact = MagicMock()
    fake_text_artifact.storage_key = None
    fake_text_artifact.data = None
    fake_text_artifact.text_data = "report body"
    fake_text_artifact.content_type = "text/plain"

    # Per-request the endpoint runs: Job lookup (1) → Artifact lookup (2).
    # Two endpoint calls back-to-back = four execute() invocations total.
    sequence = [fake_job, fake_json_artifact, fake_job, fake_text_artifact]

    def execute_side_effect(*_args, **_kwargs):
        result = MagicMock()
        result.scalar_one_or_none.return_value = sequence.pop(0) if sequence else None
        return result

    mock_session = MagicMock()
    mock_session.execute.side_effect = execute_side_effect
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    json_response = client.get("/api/analyses/demo_run/artifact/contract_analysis.json")
    txt_response = client.get("/api/analyses/demo_run/artifact/analysis_report.txt")

    assert json_response.status_code == 200, json_response.text
    assert json_response.json()["summary"]["control_model"] == "ownable"
    assert txt_response.status_code == 200, txt_response.text
    assert "report body" in txt_response.text


@patch("api.SessionLocal")
def test_protocol_tvl_caps_days(mock_session_cls) -> None:
    """days parameter should be capped to MAX_TVL_HISTORY_DAYS."""
    client = make_client()

    fake_protocol = MagicMock()
    fake_protocol.id = 1
    fake_protocol.name = "TestProto"

    mock_session = MagicMock()
    mock_session.get.return_value = fake_protocol

    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []
    mock_execute_result = MagicMock()
    mock_execute_result.scalars.return_value = mock_scalars
    mock_session.execute.return_value = mock_execute_result

    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    response = client.get("/api/protocols/1/tvl?days=9999")
    assert response.status_code == 200

    import api

    assert hasattr(api, "MAX_TVL_HISTORY_DAYS"), "api.py should define MAX_TVL_HISTORY_DAYS"
    assert api.MAX_TVL_HISTORY_DAYS <= 365


# ---------------------------------------------------------------------------
# /api/jobs/{job_id}/stage_timings — bench harness consumer
# ---------------------------------------------------------------------------


@patch("api.SessionLocal")
def test_stage_timings_endpoint_returns_per_stage_artifacts(mock_session_cls) -> None:
    """Bench harness needs a reliable per-stage timing source. The endpoint
    must collect every ``stage_timing_<stage>`` artifact for the job and
    return them keyed by stage name (the suffix after ``stage_timing_``).
    Mirrors what the worker writes via ``_record_stage_timing``."""
    import api

    client = make_client()
    fake_job = _make_fake_job()

    # Use the inline path (data set, storage_key NULL) — exercises the same
    # response-assembly code as the storage path without needing a fake
    # boto3 client. SimpleNamespace stands in for an ORM Artifact row.
    art_discovery = SimpleNamespace(
        name="stage_timing_discovery",
        storage_key=None,
        content_type=None,
        data={
            "schema_version": "2",
            "stage": "discovery",
            "elapsed_s": 4.2,
            "started_at": "t0",
            "ended_at": "t1",
            "worker_id": "DiscoveryWorker-1-aaa",
            "status": "success",
        },
        text_data=None,
    )
    art_static = SimpleNamespace(
        name="stage_timing_static",
        storage_key=None,
        content_type=None,
        data={
            "schema_version": "2",
            "stage": "static",
            "elapsed_s": 28.7,
            "started_at": "t1",
            "ended_at": "t2",
            "worker_id": "StaticWorker-1-bbb",
            "status": "success",
        },
        text_data=None,
    )

    mock_session = MagicMock()
    mock_session.get.return_value = fake_job
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = [art_discovery, art_static]
    mock_execute_result = MagicMock()
    mock_execute_result.scalars.return_value = mock_scalars
    mock_session.execute.return_value = mock_execute_result
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    api.ADMIN_KEY = "test-admin-key"
    resp = client.get(
        f"/api/jobs/{fake_job.id}/stage_timings",
        headers={"X-PSAT-Admin-Key": "test-admin-key"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["job_id"] == str(fake_job.id)
    assert set(body["stage_timings"].keys()) == {"discovery", "static"}
    assert body["stage_timings"]["discovery"]["elapsed_s"] == 4.2
    assert body["stage_timings"]["static"]["elapsed_s"] == 28.7
    assert body["stage_timings"]["discovery"]["status"] == "success"


@patch("api.SessionLocal")
def test_stage_timings_endpoint_404_for_unknown_job(mock_session_cls) -> None:
    import api

    client = make_client()
    mock_session = MagicMock()
    mock_session.get.return_value = None
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    api.ADMIN_KEY = "test-admin-key"
    resp = client.get(
        f"/api/jobs/{uuid.uuid4()}/stage_timings",
        headers={"X-PSAT-Admin-Key": "test-admin-key"},
    )
    assert resp.status_code == 404


def test_stage_timings_endpoint_is_admin_protected() -> None:
    """Per-job timings expose internal worker_id / runtime metadata —
    must be admin-protected so a public-facing analyzer doesn't leak
    the worker fleet shape. Verified at the route-definition level
    because conftest's ``_bypass_admin_key`` autouse fixture stubs
    the auth dependency for every test, so an HTTP 401 assertion
    can never fire."""
    import api

    target_path = "/api/jobs/{job_id}/stage_timings"
    matching = [r for r in api.app.routes if getattr(r, "path", None) == target_path]
    assert matching, f"route {target_path} is not registered"
    route = matching[0]
    deps = [dep.call for dep in route.dependant.dependencies]  # type: ignore[attr-defined]
    assert api.require_admin_key in deps, (
        "stage_timings endpoint must depend on require_admin_key — without "
        "it, per-job worker_id / runtime metadata would be public"
    )


# ---------------------------------------------------------------------------
# Storage-failure degradation. Both endpoints used to 500 on a transport
# error or a missing storage client; we degrade per-row so a flaky bucket
# can't take down the whole response.
# ---------------------------------------------------------------------------


@patch("api.get_storage_client")
@patch("api.SessionLocal")
def test_stage_timings_degrades_when_storage_unconfigured(mock_session_cls, mock_get_client) -> None:
    """If storage is not configured but rows reference ``storage_key``, the
    endpoint must return what it can (inline rows, empty if none) instead of
    raising — env drift on a redeploy shouldn't 500 the SPA."""

    fake_job = _make_fake_job()
    storage_row = SimpleNamespace(
        name="stage_timing_static",
        storage_key="some/key",
        content_type="application/json",
        data=None,
        text_data=None,
    )
    inline_row = SimpleNamespace(
        name="stage_timing_discovery",
        storage_key=None,
        content_type=None,
        data={"stage": "discovery", "elapsed_s": 1.0},
        text_data=None,
    )

    mock_session = MagicMock()
    mock_session.get.return_value = fake_job
    mock_session.execute.return_value.scalars.return_value.all.return_value = [inline_row, storage_row]
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_client.return_value = None  # storage env stripped

    resp = make_client().get(f"/api/jobs/{fake_job.id}/stage_timings")
    assert resp.status_code == 200
    body = resp.json()
    # Inline row survives; storage row is silently skipped.
    assert set(body["stage_timings"].keys()) == {"discovery"}


@patch("api.get_storage_client")
@patch("api.SessionLocal")
def test_stage_timings_degrades_on_storage_transport_error(mock_session_cls, mock_get_client) -> None:
    """Transport error from the bucket (Tigris hiccup, signing failure, etc.)
    surfaces as ``None`` for the affected key (per ``get_many``'s contract,
    pinned in test_db_storage_unit.py). The endpoint must drop the affected
    stage and keep healthy ones, not 500."""
    fake_job = _make_fake_job()
    inline_row = SimpleNamespace(
        name="stage_timing_discovery",
        storage_key=None,
        content_type=None,
        data={"stage": "discovery", "elapsed_s": 1.0},
        text_data=None,
    )
    storage_row = SimpleNamespace(
        name="stage_timing_static",
        storage_key="some/key",
        content_type="application/json",
        data=None,
        text_data=None,
    )

    fake_client = MagicMock()
    fake_client.get_many.return_value = {"some/key": None}

    mock_session = MagicMock()
    mock_session.get.return_value = fake_job
    mock_session.execute.return_value.scalars.return_value.all.return_value = [inline_row, storage_row]
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_client.return_value = fake_client

    resp = make_client().get(f"/api/jobs/{fake_job.id}/stage_timings")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "discovery" in body["stage_timings"]
    assert "static" not in body["stage_timings"]


@patch("api.get_storage_client")
@patch("api.SessionLocal")
def test_analyses_degrades_per_row_on_partial_storage_failure(mock_session_cls, mock_get_client) -> None:
    """A single bad key must not wipe every entry. Pre-fix, ``get_many``
    raised on the first transport error and the catch zeroed ``bodies`` for
    every row in the response. Now: per-key None means the surviving rows
    keep their summary/contract_name."""

    j_good = _make_fake_job(name="run_good", address="0xaaa")
    j_bad = _make_fake_job(name="run_bad", address="0xbbb")

    def _art(job_id, name, key):
        return SimpleNamespace(
            job_id=job_id,
            name=name,
            storage_key=key,
            content_type="application/json",
            data=None,
            text_data=None,
        )

    artifacts = [
        _art(j_good.id, "contract_analysis", "good_analysis"),
        _art(j_good.id, "contract_flags", "good_flags"),
        _art(j_bad.id, "contract_analysis", "bad_analysis"),
        _art(j_bad.id, "contract_flags", "bad_flags"),
    ]

    fake_client = MagicMock()
    # Good job's bodies arrive; bad job's keys come back as None (transport blip).
    fake_client.get_many.return_value = {
        "good_analysis": json.dumps({"subject": {"name": "Good"}, "summary": "ok"}).encode(),
        "good_flags": json.dumps({"is_proxy": False}).encode(),
        "bad_analysis": None,
        "bad_flags": None,
    }
    mock_get_client.return_value = fake_client

    jobs_result = MagicMock()
    jobs_result.scalars.return_value.all.return_value = [j_good, j_bad]
    contracts_result = MagicMock()
    contracts_result.all.return_value = []
    artifacts_result = MagicMock()
    artifacts_result.scalars.return_value = iter(artifacts)

    sess = MagicMock()
    sess.execute.side_effect = [jobs_result, contracts_result, artifacts_result]
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=sess)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    resp = make_client().get("/api/analyses")
    assert resp.status_code == 200, resp.text
    by_run = {e["run_name"]: e for e in resp.json()}
    assert by_run["run_good"].get("summary") == "ok"
    assert by_run["run_good"].get("contract_name") == "Good"
    # The bad row still appears in the listing (basic fields), just without
    # summary/contract_name. Pre-fix it would have appeared but ALSO the good
    # row would have lost its summary. Pin the per-row behavior.
    assert "run_bad" in by_run
    assert "summary" not in by_run["run_bad"]
