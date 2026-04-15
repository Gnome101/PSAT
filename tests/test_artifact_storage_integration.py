"""Integration tests for object-storage-backed artifacts.

Requires:
- TEST_DATABASE_URL pointing at a Postgres test DB.
- TEST_ARTIFACT_STORAGE_* pointing at a running S3-compatible service
  (minio in docker-compose, real Tigris bucket in CI).

These tests exercise the production storage code path end to end:
write → row metadata → read → presigned URL → API redirect.
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.cache_helpers import requires_postgres  # noqa: E402
from tests.conftest import requires_storage  # noqa: E402

pytestmark = [requires_postgres, requires_storage]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _SessionFactory:
    """Stand-in for sessionmaker that yields a single shared Session."""

    def __init__(self, session):
        self._session = session

    def __call__(self):
        return self

    def __enter__(self):
        return self._session

    def __exit__(self, *exc):
        return False


@pytest.fixture()
def api_with(monkeypatch, db_session, storage_bucket):
    """Wire the FastAPI app to the test DB session and storage bucket."""
    import api as api_module

    monkeypatch.setattr(api_module, "SessionLocal", _SessionFactory(db_session))
    api_module.app.dependency_overrides[api_module.require_admin_key] = lambda: None
    return api_module


def _completed_job(session, name: str, address: str = "0xabcdef0000000000000000000000000000000001"):
    from db.models import JobStage, JobStatus
    from db.queue import create_job

    job = create_job(session, {"address": address, "name": name})
    job.status = JobStatus.completed
    job.stage = JobStage.done
    session.commit()
    return job


# ---------------------------------------------------------------------------
# 1. Full lifecycle — small + large artifacts both go to storage
# ---------------------------------------------------------------------------


def test_full_lifecycle_artifacts_round_trip(db_session, storage_bucket):
    """Every store_artifact write goes to storage; rows hold only metadata."""
    from db.models import Artifact
    from db.queue import create_job, get_all_artifacts, get_artifact, store_artifact

    job = create_job(db_session, {"address": "0xab", "name": "lifecycle"})

    small = {"is_proxy": True, "proxy_type": "eip1967"}
    large = {"detectors": [{"id": i, "data": "x" * 100} for i in range(2_000)]}
    text = "report body " * 5_000

    store_artifact(db_session, job.id, "contract_flags", data=small)
    store_artifact(db_session, job.id, "slither_results", data=large)
    store_artifact(db_session, job.id, "analysis_report", text_data=text)

    rows = db_session.execute(select(Artifact).where(Artifact.job_id == job.id)).scalars().all()
    by_name = {r.name: r for r in rows}

    for name in ("contract_flags", "slither_results", "analysis_report"):
        row = by_name[name]
        assert row.storage_key, f"{name} should have a storage_key"
        assert row.data is None, f"{name} should not be inline JSONB"
        assert row.text_data is None, f"{name} should not be inline text"
        assert row.size_bytes and row.size_bytes > 0
        assert row.content_type

    assert by_name["contract_flags"].content_type == "application/json"
    assert by_name["analysis_report"].content_type.startswith("text/plain")
    assert by_name["slither_results"].size_bytes > 100_000

    assert get_artifact(db_session, job.id, "contract_flags") == small
    assert get_artifact(db_session, job.id, "slither_results") == large
    assert get_artifact(db_session, job.id, "analysis_report") == text

    all_arts = get_all_artifacts(db_session, job.id)
    assert all_arts["contract_flags"] == small
    assert all_arts["slither_results"] == large
    assert all_arts["analysis_report"] == text


# ---------------------------------------------------------------------------
# 2. Source files round-trip via storage
# ---------------------------------------------------------------------------


def test_source_files_round_trip_via_storage(db_session, storage_bucket):
    from db.models import SourceFile
    from db.queue import create_job, get_source_files, store_source_files

    job = create_job(db_session, {"address": "0xab", "name": "sf-test"})
    files = {
        "src/Big.sol": "pragma solidity ^0.8.24;\n" + ("// big line\n" * 10_000),
        "src/Small.sol": "pragma solidity ^0.8.24;\ncontract X {}",
        "src/sub/Nested.sol": "// nested",
    }
    store_source_files(db_session, job.id, files)

    rows = db_session.execute(select(SourceFile).where(SourceFile.job_id == job.id)).scalars().all()
    assert len(rows) == 3
    for r in rows:
        assert r.storage_key, f"{r.path} should have a storage_key"
        assert r.content is None

    assert get_source_files(db_session, job.id) == files


# ---------------------------------------------------------------------------
# 3. Legacy inline rows still read (no storage_key)
# ---------------------------------------------------------------------------


def test_legacy_inline_artifact_still_reads(db_session, storage_bucket):
    """An old row written before the migration (storage_key NULL) still works."""
    from db.models import Artifact
    from db.queue import create_job, get_artifact

    job = create_job(db_session, {"address": "0xab", "name": "legacy"})
    db_session.add(Artifact(job_id=job.id, name="legacy_blob", data={"v": 1}))
    db_session.commit()

    assert get_artifact(db_session, job.id, "legacy_blob") == {"v": 1}


# ---------------------------------------------------------------------------
# 4. Idempotent overwrite (deterministic key)
# ---------------------------------------------------------------------------


def test_repeat_store_overwrites_same_key(db_session, storage_bucket):
    from db.models import Artifact
    from db.queue import create_job, get_artifact, store_artifact

    job = create_job(db_session, {"address": "0xab", "name": "overwrite"})
    store_artifact(db_session, job.id, "x", data={"v": 1})
    first_row = db_session.execute(select(Artifact).where(Artifact.job_id == job.id, Artifact.name == "x")).scalar_one()
    first_key = first_row.storage_key

    store_artifact(db_session, job.id, "x", data={"v": 2})
    db_session.expire_all()
    second_row = db_session.execute(
        select(Artifact).where(Artifact.job_id == job.id, Artifact.name == "x")
    ).scalar_one()

    assert first_key == second_row.storage_key, "second write should reuse the deterministic key"
    assert get_artifact(db_session, job.id, "x") == {"v": 2}


# ---------------------------------------------------------------------------
# 5. /api/analyses/.../artifact endpoint serves storage-backed bodies
# ---------------------------------------------------------------------------


def test_artifact_endpoint_serves_storage_backed_json(api_with, db_session, storage_bucket):
    """Storage-backed JSON artifact is fetched from Tigris and served as JSON."""
    from db.queue import store_artifact

    job = _completed_job(db_session, "json-test")
    payload = {"summary": {"control_model": "ownable"}, "tag": "v1"}
    store_artifact(db_session, job.id, "contract_analysis", data=payload)

    client = TestClient(api_with.app)
    resp = client.get("/api/analyses/json-test/artifact/contract_analysis.json")
    assert resp.status_code == 200
    assert resp.json() == payload


def test_artifact_endpoint_serves_storage_backed_text(api_with, db_session, storage_bucket):
    """Storage-backed text artifact is fetched and served as text."""
    from db.queue import store_artifact

    job = _completed_job(db_session, "text-test")
    body = "analysis report line " * 50
    store_artifact(db_session, job.id, "analysis_report", text_data=body)

    client = TestClient(api_with.app)
    resp = client.get("/api/analyses/text-test/artifact/analysis_report.txt")
    assert resp.status_code == 200
    assert body in resp.text


def test_storage_client_can_presign(storage_bucket):
    """presign() returns a working URL — important for any future direct-from-storage download path."""
    storage_bucket.put("artifacts/test/presign.json", b'{"ok": true}', "application/json")
    url = storage_bucket.presign("artifacts/test/presign.json", expires_in=60)
    body = urllib.request.urlopen(url).read()
    assert json.loads(body.decode("utf-8")) == {"ok": True}


# ---------------------------------------------------------------------------
# 6. /api/jobs proxy detection works through storage
# ---------------------------------------------------------------------------


def test_list_jobs_detects_proxy_via_storage(api_with, db_session, storage_bucket):
    from db.queue import store_artifact

    proxy_job = _completed_job(db_session, "proxy-job")
    plain_job = _completed_job(db_session, "plain-job", address="0xabcdef0000000000000000000000000000000002")
    store_artifact(db_session, proxy_job.id, "contract_flags", data={"is_proxy": True})
    store_artifact(db_session, plain_job.id, "contract_flags", data={"is_proxy": False})

    client = TestClient(api_with.app)
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    by_id = {j["job_id"]: j for j in resp.json()}
    assert by_id[str(proxy_job.id)]["is_proxy"] is True
    assert by_id[str(plain_job.id)]["is_proxy"] is False


# ---------------------------------------------------------------------------
# 7. /api/health hits the DB
# ---------------------------------------------------------------------------


def test_health_endpoint_runs_db_select(api_with):
    client = TestClient(api_with.app)
    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# 8. End-to-end stub: simulated worker writes a full job, API reads it back
# ---------------------------------------------------------------------------


def test_end_to_end_stubbed_worker(api_with, db_session, storage_bucket):
    """Simulate a worker writing all the artifacts a real job would."""
    from db.queue import store_artifact, store_source_files

    job = _completed_job(db_session, "e2e-test", address="0xabcdef0000000000000000000000000000000003")

    store_source_files(
        db_session,
        job.id,
        {
            "src/Main.sol": "pragma solidity ^0.8.24;\ncontract Main { function f() public {} }",
            "src/Lib.sol": "pragma solidity ^0.8.24;\nlibrary L {}",
        },
    )
    store_artifact(db_session, job.id, "contract_flags", data={"is_proxy": False})
    store_artifact(
        db_session,
        job.id,
        "contract_analysis",
        data={"subject": {"name": "Main"}, "summary": {"control_model": "ownable"}},
    )
    store_artifact(db_session, job.id, "slither_results", data={"results": {"detectors": []}})
    store_artifact(db_session, job.id, "analysis_report", text_data="Test analysis report content")

    client = TestClient(api_with.app)

    detail = client.get("/api/analyses/e2e-test")
    assert detail.status_code == 200, detail.text
    payload = detail.json()
    assert payload["run_name"] == "e2e-test"
    assert "contract_analysis" in payload["available_artifacts"]
    assert payload["contract_analysis"]["subject"]["name"] == "Main"

    artifact = client.get("/api/analyses/e2e-test/artifact/slither_results.json", follow_redirects=True)
    assert artifact.status_code == 200
    assert artifact.json() == {"results": {"detectors": []}}


# ---------------------------------------------------------------------------
# 9. Object storage outage during read surfaces as a graceful skip in lists
# ---------------------------------------------------------------------------


def test_get_all_artifacts_skips_missing_storage_objects(db_session, storage_bucket):
    """If the storage object is gone, get_all_artifacts skips that entry rather than crashing."""
    from db.models import Artifact
    from db.queue import create_job, get_all_artifacts, store_artifact

    job = create_job(db_session, {"address": "0xab", "name": "missing-obj"})
    store_artifact(db_session, job.id, "good", data={"v": 1})
    store_artifact(db_session, job.id, "broken", data={"v": 2})

    broken_row = db_session.execute(
        select(Artifact).where(Artifact.job_id == job.id, Artifact.name == "broken")
    ).scalar_one()
    storage_bucket.delete(broken_row.storage_key)

    all_arts = get_all_artifacts(db_session, job.id)
    assert all_arts == {"good": {"v": 1}}


# ---------------------------------------------------------------------------
# 10. Inline-fallback path (no storage configured) still works
# ---------------------------------------------------------------------------


def test_inline_fallback_when_storage_unconfigured(db_session, monkeypatch):
    """With no storage env, store_artifact writes inline like the legacy code."""
    from db.models import Artifact
    from db.queue import create_job, get_artifact, store_artifact
    from db.storage import reset_client_cache

    for k in (
        "ARTIFACT_STORAGE_ENDPOINT",
        "ARTIFACT_STORAGE_BUCKET",
        "ARTIFACT_STORAGE_ACCESS_KEY",
        "ARTIFACT_STORAGE_SECRET_KEY",
    ):
        monkeypatch.delenv(k, raising=False)
    reset_client_cache()

    job = create_job(db_session, {"address": "0xab", "name": "inline-only"})
    store_artifact(db_session, job.id, "x", data={"v": 1})

    row = db_session.execute(select(Artifact).where(Artifact.job_id == job.id, Artifact.name == "x")).scalar_one()
    assert row.storage_key is None
    assert row.data == {"v": 1}
    assert get_artifact(db_session, job.id, "x") == {"v": 1}
