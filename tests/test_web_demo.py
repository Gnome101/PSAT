from __future__ import annotations

import json
import sys
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import web_demo


def make_client() -> TestClient:
    return TestClient(web_demo.app)


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


def test_analyze_endpoint_uses_start_demo_job(monkeypatch) -> None:
    client = make_client()

    def fake_start_demo_job(request: web_demo.AnalyzeRequest) -> dict:
        assert request.address == "0x1234567890123456789012345678901234567890"
        return {"job_id": "job-1", "status": "queued"}

    monkeypatch.setattr(web_demo, "start_demo_job", fake_start_demo_job)

    response = client.post(
        "/api/analyze",
        json={"address": "0x1234567890123456789012345678901234567890", "name": "demo"},
    )

    assert response.status_code == 200
    assert response.json()["job_id"] == "job-1"


def test_analyze_endpoint_rejects_bad_address() -> None:
    client = make_client()
    response = client.post("/api/analyze", json={"address": "123", "name": "demo"})

    assert response.status_code == 422


def test_get_job_and_missing_job() -> None:
    client = make_client()
    web_demo.JOBS.clear()
    web_demo.JOBS["job-1"] = {"job_id": "job-1", "status": "completed"}

    existing = client.get("/api/jobs/job-1")
    missing = client.get("/api/jobs/job-2")

    assert existing.status_code == 200
    assert existing.json()["status"] == "completed"
    assert missing.status_code == 404


def test_analyses_endpoints(monkeypatch) -> None:
    client = make_client()

    monkeypatch.setattr(
        web_demo,
        "list_analyses",
        lambda: [{"run_name": "demo_run", "contract_name": "Demo", "address": "0xabc"}],
    )
    monkeypatch.setattr(
        web_demo,
        "read_analysis",
        lambda run_name: {
            "run_name": run_name,
            "contract_name": "Demo",
            "address": "0xabc",
            "contract_analysis": {"summary": {"control_model": "ownable"}},
        },
    )

    listing = client.get("/api/analyses")
    detail = client.get("/api/analyses/demo_run")

    assert listing.status_code == 200
    assert listing.json()[0]["run_name"] == "demo_run"
    assert detail.status_code == 200
    assert detail.json()["run_name"] == "demo_run"


def test_missing_analysis_returns_404(monkeypatch) -> None:
    client = make_client()

    def fake_read_analysis(run_name: str) -> dict:
        raise FileNotFoundError(run_name)

    monkeypatch.setattr(web_demo, "read_analysis", fake_read_analysis)

    response = client.get("/api/analyses/missing")
    assert response.status_code == 404


def test_artifact_endpoint_serves_json_and_text(monkeypatch, tmp_path: Path) -> None:
    client = make_client()

    json_path = tmp_path / "contract_analysis.json"
    json_path.write_text(json.dumps({"summary": {"control_model": "ownable"}}))
    txt_path = tmp_path / "analysis_report.txt"
    txt_path.write_text("report body")

    monkeypatch.setattr(
        web_demo,
        "read_analysis",
        lambda run_name: {
            "contract_analysis": {"summary": {"control_model": "ownable"}},
        },
    )

    def fake_artifact_path(run_name: str, artifact_name: str) -> Path:
        if artifact_name == "contract_analysis.json":
            return json_path
        if artifact_name == "analysis_report.txt":
            return txt_path
        raise FileNotFoundError(artifact_name)

    monkeypatch.setattr(web_demo, "artifact_path", fake_artifact_path)

    json_response = client.get("/api/analyses/demo_run/artifact/contract_analysis.json")
    txt_response = client.get("/api/analyses/demo_run/artifact/analysis_report.txt")
    missing_response = client.get("/api/analyses/demo_run/artifact/missing.json")

    assert json_response.status_code == 200
    assert json_response.json()["summary"]["control_model"] == "ownable"
    assert txt_response.status_code == 200
    assert "report body" in txt_response.text
    assert missing_response.status_code == 404
