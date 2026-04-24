"""Shared fixtures for the live test suite.

Live tests run against a deployed PSAT API (staging preview or prod) and
exercise the whole stack end-to-end: API → Postgres → worker pool →
Etherscan/RPC/Tavily/OpenRouter → object storage.

Configuration via env:
    PSAT_LIVE_URL    — base URL of the deployed API (required in CI)
    PSAT_ADMIN_KEY   — admin key header for POST endpoints (required)

Everything under ``tests/live/`` is tagged with ``@pytest.mark.live`` by the
autouse marker below, so the CI command ``pytest -m "not live"`` skips the
whole directory and ``pytest -m live`` selects it.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any

import pytest
import requests

# Known, fast-to-analyze contract used by the session-scoped WETH fixture.
# WETH on Ethereum mainnet: small, non-proxy, cached aggressively on Etherscan.
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5c4F27eAD9083C756Cc2"

DEFAULT_SINGLE_TIMEOUT = 600  # one contract: 2-5 min typical, 10 min ceiling
DEFAULT_COMPANY_TIMEOUT = 900  # company: 5-10 min typical, 15 min ceiling
DEFAULT_POLL_INTERVAL = 5


def _parse_dt(s: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(s)


class LiveClient:
    """Thin wrapper over a deployed PSAT API.

    Centralizes the base URL, admin-key header, timeouts, and the polling
    loops that every live test otherwise re-implements.
    """

    def __init__(self, base_url: str, admin_key: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update({"X-PSAT-Admin-Key": admin_key})

    # -- basic HTTP ----------------------------------------------------------

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def health(self, timeout: float = 5) -> requests.Response:
        # Health doesn't need auth — use bare requests so a missing/invalid
        # admin key doesn't mask a real reachability problem.
        return requests.get(self._url("/api/health"), timeout=timeout)

    def is_healthy(self) -> bool:
        try:
            return self.health().status_code == 200
        except requests.RequestException:
            return False

    # -- analyze -------------------------------------------------------------

    def analyze(self, address: str) -> dict[str, Any]:
        r = self._session.post(self._url("/api/analyze"), json={"address": address}, timeout=15)
        r.raise_for_status()
        return r.json()

    def analyze_company(self, company: str, limit: int = 2) -> dict[str, Any]:
        r = self._session.post(
            self._url("/api/analyze"),
            json={"company": company, "analyze_limit": limit},
            timeout=15,
        )
        r.raise_for_status()
        return r.json()

    # -- reads ---------------------------------------------------------------

    def job(self, job_id: str) -> dict[str, Any]:
        r = self._session.get(self._url(f"/api/jobs/{job_id}"), timeout=15)
        r.raise_for_status()
        return r.json()

    def jobs(self) -> list[dict[str, Any]]:
        r = self._session.get(self._url("/api/jobs"), timeout=15)
        r.raise_for_status()
        return r.json()

    def children_of(self, parent_job_id: str) -> list[dict[str, Any]]:
        return [j for j in self.jobs() if (j.get("request") or {}).get("parent_job_id") == parent_job_id]

    def artifact(self, run_name: str, artifact_name: str) -> dict | str | None:
        """Fetch an artifact. Returns dict for JSON, str for text, None on 404."""
        # The artifact endpoint routes on the extension, so we need to know
        # which artifacts are text vs JSON. analysis_report is the slither
        # output (plain text); everything else the pipeline emits is JSON.
        ext = ".txt" if artifact_name == "analysis_report" else ".json"
        r = self._session.get(
            self._url(f"/api/analyses/{run_name}/artifact/{artifact_name}{ext}"),
            timeout=15,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json() if ext == ".json" else r.text

    # -- polling -------------------------------------------------------------

    def poll_job_until_done(
        self,
        job_id: str,
        timeout: float = DEFAULT_SINGLE_TIMEOUT,
        interval: float = DEFAULT_POLL_INTERVAL,
    ) -> dict[str, Any]:
        """Poll ``/api/jobs/{id}`` until status is terminal or timeout fires."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self.job(job_id)
            if status["status"] in ("completed", "failed"):
                return status
            time.sleep(interval)
        raise TimeoutError(f"Job {job_id} did not reach a terminal status within {timeout}s")

    def poll_children_until_done(
        self,
        parent_job_id: str,
        timeout: float = DEFAULT_COMPANY_TIMEOUT,
        interval: float = DEFAULT_POLL_INTERVAL * 2,
    ) -> list[dict[str, Any]]:
        """Poll ``/api/jobs`` until every child of *parent_job_id* is terminal."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            children = self.children_of(parent_job_id)
            if children and all(c["status"] in ("completed", "failed") for c in children):
                return children
            time.sleep(interval)
        return self.children_of(parent_job_id)

    # -- utilities -----------------------------------------------------------

    @staticmethod
    def job_duration_seconds(job: dict[str, Any]) -> float:
        return (_parse_dt(job["updated_at"]) - _parse_dt(job["created_at"])).total_seconds()

    def submit_and_wait(
        self,
        address: str,
        timeout: float = DEFAULT_SINGLE_TIMEOUT,
    ) -> dict[str, Any]:
        return self.poll_job_until_done(self.analyze(address)["job_id"], timeout=timeout)

    def submit_company_and_wait(
        self,
        company: str,
        limit: int = 2,
        timeout: float = DEFAULT_COMPANY_TIMEOUT,
    ) -> dict[str, Any]:
        return self.poll_job_until_done(
            self.analyze_company(company, limit=limit)["job_id"],
            timeout=timeout,
        )


# ---------------------------------------------------------------------------
# Pytest hooks / fixtures
# ---------------------------------------------------------------------------


def pytest_collection_modifyitems(config, items):
    """Auto-tag every test under ``tests/live/`` with @pytest.mark.live.

    Saves callers from remembering to set ``pytestmark`` in each new file
    and keeps the ``-m live`` filter honest across the whole directory.
    """
    live_mark = pytest.mark.live
    for item in items:
        if "/tests/live/" in str(item.fspath) or "\\tests\\live\\" in str(item.fspath):
            item.add_marker(live_mark)


@pytest.fixture(scope="session")
def live_base_url() -> str:
    return os.environ.get("PSAT_LIVE_URL", "http://127.0.0.1:8000").rstrip("/")


@pytest.fixture(scope="session")
def live_admin_key() -> str:
    key = os.environ.get("PSAT_ADMIN_KEY", "")
    if not key:
        pytest.skip("PSAT_ADMIN_KEY not set (required for POST /api/analyze)")
    return key


@pytest.fixture(scope="session")
def live_client(live_base_url: str, live_admin_key: str) -> LiveClient:
    return LiveClient(live_base_url, live_admin_key)


@pytest.fixture(scope="session", autouse=True)
def _require_live_api(live_client: LiveClient):
    """Health-gate the entire live suite once per session."""
    if not live_client.is_healthy():
        pytest.skip(f"API not reachable at {live_client.base_url}")


# ---------------------------------------------------------------------------
# Shared "known-good completed run" fixtures
#
# These amortize the ~3-5 min WETH analysis across every downstream test
# that just needs a terminated job to inspect.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def analyzed_weth(live_client: LiveClient) -> dict[str, Any]:
    """Submit WETH once per session; every dependent test reuses the result.

    Fails the whole fixture (and therefore every dependent test) if WETH
    didn't reach ``completed`` — a hard signal that the pipeline is broken
    on staging, not just this one test.
    """
    job = live_client.submit_and_wait(WETH_ADDRESS)
    if job["status"] != "completed":
        pytest.fail(f"WETH analysis did not complete on {live_client.base_url}: {job.get('error')}")
    return job


@pytest.fixture
def analyze_and_wait(live_client: LiveClient):
    """Factory for tests that need their own fresh analysis of an address."""

    def _fn(address: str, timeout: float = DEFAULT_SINGLE_TIMEOUT) -> dict[str, Any]:
        return live_client.submit_and_wait(address, timeout=timeout)

    return _fn
