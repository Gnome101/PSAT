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

    def config(self) -> dict[str, Any]:
        r = self._session.get(self._url("/api/config"), timeout=15)
        r.raise_for_status()
        return r.json()

    def stats(self) -> dict[str, Any]:
        r = self._session.get(self._url("/api/stats"), timeout=15)
        r.raise_for_status()
        return r.json()

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

    def analyze_remaining(self, company: str) -> dict[str, Any]:
        r = self._session.post(
            self._url(f"/api/company/{company}/analyze-remaining"),
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def refresh_company_coverage(self, company: str, verify_source_equivalence: bool = False) -> dict[str, Any]:
        # ``verify_source_equivalence=False`` skips the per-file Etherscan
        # equivalence pass. Tests default to false because the network leg
        # is rate-limited and irrelevant to "did the row count update?".
        r = self._session.post(
            self._url(f"/api/company/{company}/refresh_coverage"),
            params={"verify_source_equivalence": str(verify_source_equivalence).lower()},
            timeout=120,
        )
        r.raise_for_status()
        return r.json()

    def reextract_audit_scope(self, audit_id: int) -> requests.Response:
        # Raw Response so callers can inspect 409 (text extraction not
        # complete) without the wrapper raising on it.
        return self._session.post(self._url(f"/api/audits/{audit_id}/reextract_scope"), timeout=15)

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

    def analyses(self) -> list[dict[str, Any]]:
        r = self._session.get(self._url("/api/analyses"), timeout=15)
        r.raise_for_status()
        return r.json()

    def analysis_detail(self, run_name: str) -> dict[str, Any]:
        r = self._session.get(self._url(f"/api/analyses/{run_name}"), timeout=15)
        r.raise_for_status()
        return r.json()

    # -- company -------------------------------------------------------------

    def company_overview(self, company: str) -> dict[str, Any]:
        r = self._session.get(self._url(f"/api/company/{company}"), timeout=30)
        r.raise_for_status()
        return r.json()

    def list_company_audits(self, company: str) -> dict[str, Any]:
        r = self._session.get(self._url(f"/api/company/{company}/audits"), timeout=15)
        r.raise_for_status()
        return r.json()

    # -- address labels ------------------------------------------------------

    def list_address_labels(self) -> dict[str, Any]:
        r = self._session.get(self._url("/api/address_labels"), timeout=15)
        r.raise_for_status()
        return r.json()

    def put_address_label(self, address: str, payload: dict[str, Any]) -> dict[str, Any]:
        r = self._session.put(self._url(f"/api/address_labels/{address}"), json=payload, timeout=15)
        r.raise_for_status()
        return r.json()

    def delete_address_label(self, address: str) -> dict[str, Any]:
        r = self._session.delete(self._url(f"/api/address_labels/{address}"), timeout=15)
        r.raise_for_status()
        return r.json()

    # -- monitoring ----------------------------------------------------------

    def list_monitored_events(self, limit: int = 50) -> list[dict[str, Any]]:
        r = self._session.get(self._url("/api/monitored-events"), params={"limit": limit}, timeout=15)
        r.raise_for_status()
        return r.json()

    def list_proxy_events(self) -> list[dict[str, Any]]:
        r = self._session.get(self._url("/api/proxy-events"), timeout=15)
        r.raise_for_status()
        return r.json()

    def contract_audit_timeline(self, contract_id: int) -> dict[str, Any]:
        r = self._session.get(self._url(f"/api/contracts/{contract_id}/audit_timeline"), timeout=30)
        r.raise_for_status()
        return r.json()

    def audit_text(self, audit_id: int) -> requests.Response:
        # Raw Response so tests can distinguish 200 (text ready) from 409
        # (extraction still in progress) from 503 (storage offline).
        return self._session.get(self._url(f"/api/audits/{audit_id}/text"), timeout=30)

    def audit_pdf(self, audit_id: int) -> requests.Response:
        return self._session.get(self._url(f"/api/audits/{audit_id}/pdf"), timeout=60)

    # -- audits --------------------------------------------------------------

    def add_audit(self, company: str, payload: dict[str, Any]) -> dict[str, Any]:
        r = self._session.post(
            self._url(f"/api/company/{company}/audits"),
            json=payload,
            timeout=15,
        )
        r.raise_for_status()
        return r.json()

    def get_audit(self, audit_id: int) -> dict[str, Any]:
        r = self._session.get(self._url(f"/api/audits/{audit_id}"), timeout=15)
        r.raise_for_status()
        return r.json()

    def audit_scope(self, audit_id: int) -> requests.Response:
        # Returns the raw Response so callers can inspect 409 ("not ready yet")
        # without the polling helper treating it as a hard error.
        return self._session.get(self._url(f"/api/audits/{audit_id}/scope"), timeout=15)

    def delete_audit(self, audit_id: int) -> dict[str, Any]:
        r = self._session.delete(self._url(f"/api/audits/{audit_id}"), timeout=15)
        r.raise_for_status()
        return r.json()

    def audits_pipeline(self) -> dict[str, Any]:
        r = self._session.get(self._url("/api/audits/pipeline"), timeout=15)
        r.raise_for_status()
        return r.json()

    def company_audit_coverage(self, company: str) -> dict[str, Any]:
        r = self._session.get(self._url(f"/api/company/{company}/audit_coverage"), timeout=15)
        r.raise_for_status()
        return r.json()

    def poll_audit_until_scope(
        self,
        audit_id: int,
        timeout: float = DEFAULT_COMPANY_TIMEOUT,
        interval: float = DEFAULT_POLL_INTERVAL * 2,
    ) -> dict[str, Any]:
        """Poll ``/api/audits/{id}`` until scope extraction reaches success/failed.

        Returns the final audit row. Raises ``TimeoutError`` if the workers
        didn't produce a terminal status within ``timeout`` seconds.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            row = self.get_audit(audit_id)
            if row.get("scope_extraction_status") in ("success", "failed"):
                return row
            time.sleep(interval)
        raise TimeoutError(f"Audit {audit_id} did not finish scope extraction within {timeout}s")

    # -- watched proxies -----------------------------------------------------

    def add_watched_proxy(self, payload: dict[str, Any]) -> dict[str, Any]:
        r = self._session.post(
            self._url("/api/watched-proxies"),
            json=payload,
            timeout=15,
        )
        r.raise_for_status()
        return r.json()

    def list_watched_proxies(self) -> list[dict[str, Any]]:
        r = self._session.get(self._url("/api/watched-proxies"), timeout=15)
        r.raise_for_status()
        return r.json()

    def delete_watched_proxy(self, proxy_id: str) -> dict[str, Any]:
        r = self._session.delete(self._url(f"/api/watched-proxies/{proxy_id}"), timeout=15)
        r.raise_for_status()
        return r.json()

    def list_subscriptions(self, proxy_id: str) -> list[dict[str, Any]]:
        r = self._session.get(self._url(f"/api/watched-proxies/{proxy_id}/subscriptions"), timeout=15)
        r.raise_for_status()
        return r.json()

    def add_subscription(self, proxy_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        r = self._session.post(
            self._url(f"/api/watched-proxies/{proxy_id}/subscriptions"),
            json=payload,
            timeout=15,
        )
        r.raise_for_status()
        return r.json()

    def delete_subscription(self, subscription_id: str) -> dict[str, Any]:
        r = self._session.delete(self._url(f"/api/subscriptions/{subscription_id}"), timeout=15)
        r.raise_for_status()
        return r.json()

    # -- monitored contracts (PATCH) ----------------------------------------

    def list_monitored_contracts(
        self,
        protocol_id: int | None = None,
        chain: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if protocol_id is not None:
            params["protocol_id"] = protocol_id
        if chain is not None:
            params["chain"] = chain
        r = self._session.get(self._url("/api/monitored-contracts"), params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    def patch_monitored_contract(self, contract_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        r = self._session.patch(
            self._url(f"/api/monitored-contracts/{contract_id}"),
            json=payload,
            timeout=15,
        )
        r.raise_for_status()
        return r.json()

    # -- protocol monitoring -------------------------------------------------

    def protocol_monitoring(self, protocol_id: int) -> list[dict[str, Any]]:
        r = self._session.get(self._url(f"/api/protocols/{protocol_id}/monitoring"), timeout=15)
        r.raise_for_status()
        return r.json()

    def protocol_subscriptions(self, protocol_id: int) -> list[dict[str, Any]]:
        r = self._session.get(self._url(f"/api/protocols/{protocol_id}/subscriptions"), timeout=15)
        r.raise_for_status()
        return r.json()

    def protocol_events(self, protocol_id: int, limit: int = 50) -> list[dict[str, Any]]:
        r = self._session.get(self._url(f"/api/protocols/{protocol_id}/events"), params={"limit": limit}, timeout=15)
        r.raise_for_status()
        return r.json()

    def protocol_tvl(self, protocol_id: int, days: int = 30) -> dict[str, Any]:
        r = self._session.get(self._url(f"/api/protocols/{protocol_id}/tvl"), params={"days": days}, timeout=30)
        r.raise_for_status()
        return r.json()

    def re_enroll_protocol(self, protocol_id: int, chain: str = "ethereum") -> dict[str, Any]:
        # Re-enrollment hits the live RPC + classifier and can take a while
        # on a cold preview, so the timeout is generous compared to other
        # mutation endpoints.
        r = self._session.post(
            self._url(f"/api/protocols/{protocol_id}/re-enroll"),
            params={"chain": chain},
            timeout=120,
        )
        r.raise_for_status()
        return r.json()

    def subscribe_protocol(self, protocol_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        r = self._session.post(
            self._url(f"/api/protocols/{protocol_id}/subscribe"),
            json=payload,
            timeout=15,
        )
        r.raise_for_status()
        return r.json()

    def delete_protocol_subscription(self, sub_id: str) -> dict[str, Any]:
        r = self._session.delete(self._url(f"/api/protocol-subscriptions/{sub_id}"), timeout=15)
        r.raise_for_status()
        return r.json()

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


# Company used by tests that need a Protocol row in the DB (e.g. audits).
# ``etherfi`` is the same company test_cache.py exercises, so the inventory
# is typically already warm; ``limit=1`` keeps this fixture cheap when the
# preview is cold.
DEFAULT_TEST_COMPANY = "etherfi"


@pytest.fixture(scope="session")
def analyzed_company(live_client: LiveClient) -> dict[str, Any]:
    """Ensure a Protocol row exists for ``DEFAULT_TEST_COMPANY``.

    Tests that POST to ``/api/company/{name}/...`` endpoints need a protocol
    in the DB or the endpoint 404s. This fixture guarantees one exists with
    minimal overhead — ``limit=1`` caps analysis to a single contract, and
    subsequent runs reuse cached static data from the first.
    """
    parent = live_client.submit_company_and_wait(DEFAULT_TEST_COMPANY, limit=1)
    if parent["status"] != "completed":
        pytest.fail(
            f"Company fixture for '{DEFAULT_TEST_COMPANY}' did not complete "
            f"on {live_client.base_url}: {parent.get('error')}"
        )
    return parent


@pytest.fixture(scope="session")
def company_protocol_id(analyzed_company, live_client: LiveClient) -> int:
    """Resolve the integer Protocol.id for the test company.

    The protocol cluster of endpoints (``/api/protocols/{id}/...``) all key
    on this id. The parent Job row ``analyzed_company`` returns includes a
    ``protocol_id`` after discovery commits it, but we deliberately re-read
    via ``GET /api/company/{name}`` so this fixture also doubles as a sanity
    check that the company was actually upserted as a Protocol row.
    """
    overview = live_client.company_overview(DEFAULT_TEST_COMPANY)
    pid = overview.get("protocol_id")
    if not isinstance(pid, int):
        pytest.fail(
            f"Company '{DEFAULT_TEST_COMPANY}' has no Protocol row after analysis "
            f"(overview.protocol_id={pid!r}); cannot exercise protocol endpoints."
        )
    return pid
