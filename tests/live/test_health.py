"""Live integration tests for unauth GET endpoints + SPA fallback.

Smoke-level: just enough to prove the deployed stack is up, serving JSON
from the expected routes, and falling back to the built frontend for
non-API paths. Anything deeper belongs in a dedicated test file.
"""

from __future__ import annotations

import requests

from tests.live.conftest import LiveClient


def test_health_reports_ok(live_client: LiveClient):
    r = live_client.health()
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("status") == "ok"
    assert body.get("db") == "ok"
    # storage is "ok" when object storage is configured, "inline" for legacy
    # Postgres-only stacks. Both are healthy — "unavailable" is the failure.
    assert body.get("storage") in ("ok", "inline")


def test_config_returns_default_rpc(live_client: LiveClient):
    body = live_client.config()
    assert isinstance(body, dict)
    assert isinstance(body.get("default_rpc_url"), str)
    assert body["default_rpc_url"].startswith(("http://", "https://"))


def test_stats_returns_counts(live_client: LiveClient):
    body = live_client.stats()
    for key in ("unique_addresses", "total_jobs", "completed_jobs", "failed_jobs"):
        assert key in body, f"Missing {key} in /api/stats response"
        assert isinstance(body[key], int)
        assert body[key] >= 0


def test_spa_fallback_serves_frontend(live_base_url: str):
    # SPA fallback should return HTML for any non-/api path; auth not required.
    r = requests.get(live_base_url + "/", timeout=15)
    assert r.status_code == 200, r.text
    ctype = r.headers.get("Content-Type", "")
    assert "html" in ctype.lower() or "text/plain" in ctype.lower(), (
        f"Root path should return HTML (or the build-not-found text fallback), got {ctype!r}"
    )
