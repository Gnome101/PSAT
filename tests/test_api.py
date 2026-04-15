from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


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


def test_health_and_config_endpoints() -> None:
    client = make_client()

    health = client.get("/api/health")
    config = client.get("/api/config")

    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
    assert config.status_code == 200
    assert "default_rpc_url" in config.json()


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


def test_cors_allows_configured_origin(monkeypatch) -> None:
    """Configured origins should be reflected in CORS responses."""
    monkeypatch.setenv("PSAT_SITE_ORIGIN", "https://psat.example.com")
    import importlib

    import api

    importlib.reload(api)
    try:
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
