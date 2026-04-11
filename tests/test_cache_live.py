"""Live integration tests for caching behavior.

These tests hit a running API server and verify that re-analyzing the same
contract (or company) correctly reuses cached data from the first run.

Run with:
    uv run pytest tests/test_cache_live.py -v -s

Prerequisites:
    - API + workers running via ``bash start_local.sh``
    - Database freshly reset (or at least no prior job for the test address)
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE = "http://127.0.0.1:8000"

# WETH on Ethereum mainnet -- small, well-known, non-proxy, fast to analyze.
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5c4F27eAD9083C756Cc2"

# Company for the company-caching test.
COMPANY_NAME = "etherfi"
COMPANY_LIMIT = 2

# Generous timeouts (single contract ~2-5 min, company ~5-10 min)
SINGLE_TIMEOUT = 600
COMPANY_TIMEOUT = 900

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def api_healthy() -> bool:
    """Return True if the API health endpoint responds 200."""
    try:
        r = requests.get(f"{API_BASE}/api/health", timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def submit_and_wait(address: str, timeout: int = SINGLE_TIMEOUT) -> dict:
    """Submit a single-address analysis and poll until terminal status."""
    r = requests.post(
        f"{API_BASE}/api/analyze",
        json={"address": address},
        timeout=15,
    )
    r.raise_for_status()
    job = r.json()
    job_id = job["job_id"]

    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(f"{API_BASE}/api/jobs/{job_id}", timeout=15)
        r.raise_for_status()
        status = r.json()
        if status["status"] in ("completed", "failed"):
            return status
        time.sleep(5)

    raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")


def submit_company_and_wait(
    company: str,
    limit: int = COMPANY_LIMIT,
    timeout: int = COMPANY_TIMEOUT,
) -> dict:
    """Submit a company analysis and wait for the parent job to complete.

    Returns the parent job dict.  Child jobs can be discovered via
    ``get_child_jobs(parent_job_id)``.
    """
    r = requests.post(
        f"{API_BASE}/api/analyze",
        json={"company": company, "analyze_limit": limit},
        timeout=15,
    )
    r.raise_for_status()
    job = r.json()
    job_id = job["job_id"]

    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(f"{API_BASE}/api/jobs/{job_id}", timeout=15)
        r.raise_for_status()
        status = r.json()
        if status["status"] in ("completed", "failed"):
            return status
        time.sleep(10)

    raise TimeoutError(f"Company job {job_id} did not complete within {timeout}s")


def wait_for_children(parent_job_id: str, timeout: int = COMPANY_TIMEOUT) -> list[dict]:
    """Poll until all child jobs of *parent_job_id* reach a terminal status."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        children = get_child_jobs(parent_job_id)
        if children and all(c["status"] in ("completed", "failed") for c in children):
            return children
        time.sleep(10)

    # Return whatever we have, let the caller assert
    return get_child_jobs(parent_job_id)


def get_child_jobs(parent_job_id: str) -> list[dict]:
    """Return all jobs whose request.parent_job_id == parent_job_id."""
    r = requests.get(f"{API_BASE}/api/jobs", timeout=15)
    r.raise_for_status()
    all_jobs = r.json()
    children = []
    for j in all_jobs:
        req = j.get("request") or {}
        if req.get("parent_job_id") == parent_job_id:
            children.append(j)
    return children


def get_artifact(run_name: str, artifact_name: str) -> dict | str | None:
    """Fetch a single artifact by run_name and artifact_name."""
    ext = ".json"
    if artifact_name in ("analysis_report",):
        ext = ".txt"
    r = requests.get(
        f"{API_BASE}/api/analyses/{run_name}/artifact/{artifact_name}{ext}",
        timeout=15,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    if ext == ".json":
        return r.json()
    return r.text


def parse_duration_seconds(job: dict) -> float:
    """Compute wall-clock seconds between created_at and updated_at."""
    fmt_variants = ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"]
    for key in ("created_at", "updated_at"):
        val = job[key]
        # Already a datetime? Shouldn't happen from JSON, but be safe.
        if isinstance(val, datetime):
            continue
    created = _parse_dt(job["created_at"])
    updated = _parse_dt(job["updated_at"])
    return (updated - created).total_seconds()


def _parse_dt(s: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f+00:00"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    # Fallback: fromisoformat (Python 3.11+)
    return datetime.fromisoformat(s)


# ---------------------------------------------------------------------------
# Marks & skip conditions
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.live

skip_if_api_down = pytest.fixture(autouse=True)


@pytest.fixture(autouse=True)
def _require_api():
    """Skip the entire test if the API is not reachable."""
    if not api_healthy():
        pytest.skip("API not reachable at " + API_BASE)


# ---------------------------------------------------------------------------
# Test: single contract caching
# ---------------------------------------------------------------------------


class TestSingleContractCaching:
    """Submit the same address twice. The second run should use cached static data."""

    def test_first_run_completes(self):
        """Run 1: fresh analysis of WETH completes successfully."""
        job1 = submit_and_wait(WETH_ADDRESS)
        assert job1["status"] == "completed", f"First run failed: {job1.get('error')}"

        # Store job info for the next test via a class variable
        TestSingleContractCaching._job1 = job1

    def test_first_run_has_artifacts(self):
        """Run 1 produced the expected artifacts."""
        job1 = getattr(TestSingleContractCaching, "_job1", None)
        if job1 is None:
            pytest.skip("First run did not complete (test_first_run_completes must pass first)")

        run_name = job1["name"]
        assert run_name is not None, "Job has no run name"

        # Verify key artifacts exist
        analysis = get_artifact(run_name, "contract_analysis")
        assert analysis is not None, "contract_analysis artifact missing"
        assert isinstance(analysis, dict)
        assert "subject" in analysis or "summary" in analysis

    def test_second_run_uses_cache(self):
        """Run 2: re-analysis of WETH should leverage cached static data."""
        job1 = getattr(TestSingleContractCaching, "_job1", None)
        if job1 is None:
            pytest.skip("First run did not complete")

        t1 = parse_duration_seconds(job1)

        # Submit the same address again
        job2 = submit_and_wait(WETH_ADDRESS)
        assert job2["status"] == "completed", f"Second run failed: {job2.get('error')}"

        t2 = parse_duration_seconds(job2)

        # --- Cache flag assertions ---
        req2 = job2.get("request") or {}
        assert req2.get("static_cached") is True, (
            "Second run should have static_cached=True in its request"
        )
        assert req2.get("cache_source_job_id") is not None, (
            "Second run should reference the source job via cache_source_job_id"
        )
        assert req2["cache_source_job_id"] == job1["job_id"], (
            "cache_source_job_id should point to the first job"
        )

        # --- Timing assertion (informational) ---
        # We don't hard-assert speedup since timing is noisy, but log it.
        print(f"\n  Run 1 duration: {t1:.1f}s")
        print(f"  Run 2 duration: {t2:.1f}s")
        if t1 > 0:
            print(f"  Speedup ratio:  {t1 / max(t2, 0.1):.1f}x")

        # --- Artifact consistency ---
        run1_name = job1["name"]
        run2_name = job2["name"]

        analysis1 = get_artifact(run1_name, "contract_analysis")
        analysis2 = get_artifact(run2_name, "contract_analysis")

        assert analysis1 is not None, "Run 1 contract_analysis missing"
        assert analysis2 is not None, "Run 2 contract_analysis missing"

        # The cached analysis should be identical (copied from run 1)
        # Compare subject/summary sections which should be deterministic
        if isinstance(analysis1, dict) and isinstance(analysis2, dict):
            subj1 = analysis1.get("subject", {})
            subj2 = analysis2.get("subject", {})
            assert subj1.get("name") == subj2.get("name"), (
                f"Contract name mismatch between runs: {subj1.get('name')} vs {subj2.get('name')}"
            )

        # Store for potential further tests
        TestSingleContractCaching._job2 = job2

    def test_second_run_completed_faster(self):
        """Run 2 should be meaningfully faster since static stages were skipped."""
        job1 = getattr(TestSingleContractCaching, "_job1", None)
        job2 = getattr(TestSingleContractCaching, "_job2", None)
        if job1 is None or job2 is None:
            pytest.skip("Both runs must complete first")

        t1 = parse_duration_seconds(job1)
        t2 = parse_duration_seconds(job2)

        # Soft assertion: second run should be at least a bit faster.
        # If first run was under 30s, caching overhead might make this flaky,
        # so only assert when the first run took a meaningful amount of time.
        if t1 > 30:
            assert t2 < t1, (
                f"Expected second run ({t2:.1f}s) to be faster than first ({t1:.1f}s)"
            )


# ---------------------------------------------------------------------------
# Test: company caching
# ---------------------------------------------------------------------------


class TestCompanyCaching:
    """Submit the same company twice. The second run should merge inventory
    and skip addresses that were already analyzed."""

    def test_first_company_run_completes(self):
        """Run 1: company analysis for ether.fi with limit=2."""
        job1 = submit_company_and_wait(COMPANY_NAME, limit=COMPANY_LIMIT)
        assert job1["status"] == "completed", f"Company run 1 failed: {job1.get('error')}"
        TestCompanyCaching._parent1 = job1

    def test_first_company_run_has_children(self):
        """Run 1 should have spawned child address jobs."""
        parent1 = getattr(TestCompanyCaching, "_parent1", None)
        if parent1 is None:
            pytest.skip("First company run did not complete")

        children = wait_for_children(parent1["job_id"], timeout=COMPANY_TIMEOUT)
        assert len(children) > 0, "Expected at least one child job from company run 1"

        # All children should have completed (or at least attempted)
        completed = [c for c in children if c["status"] == "completed"]
        assert len(completed) > 0, (
            f"Expected at least one completed child job, got statuses: "
            f"{[c['status'] for c in children]}"
        )

        TestCompanyCaching._children1 = children
        TestCompanyCaching._child_addresses1 = {
            c["address"].lower() for c in children if c.get("address")
        }
        print(f"\n  Run 1 child addresses: {TestCompanyCaching._child_addresses1}")

    def test_first_company_run_has_inventory(self):
        """Run 1 should have a contract_inventory artifact."""
        parent1 = getattr(TestCompanyCaching, "_parent1", None)
        if parent1 is None:
            pytest.skip("First company run did not complete")

        run_name = parent1.get("name")
        if run_name:
            inventory = get_artifact(run_name, "contract_inventory")
            if inventory is not None:
                assert isinstance(inventory, dict)
                contracts = inventory.get("contracts", [])
                assert isinstance(contracts, list)
                assert len(contracts) > 0, "Inventory should have discovered contracts"
                print(f"\n  Run 1 inventory: {len(contracts)} contracts")
                TestCompanyCaching._inventory1 = inventory

    def test_second_company_run_deduplicates(self):
        """Run 2: same company. Should merge inventory and skip known addresses."""
        parent1 = getattr(TestCompanyCaching, "_parent1", None)
        child_addrs1 = getattr(TestCompanyCaching, "_child_addresses1", None)
        if parent1 is None or child_addrs1 is None:
            pytest.skip("First company run data not available")

        job2 = submit_company_and_wait(COMPANY_NAME, limit=COMPANY_LIMIT)
        assert job2["status"] == "completed", f"Company run 2 failed: {job2.get('error')}"
        TestCompanyCaching._parent2 = job2

        children2 = wait_for_children(job2["job_id"], timeout=COMPANY_TIMEOUT)
        child_addrs2 = {c["address"].lower() for c in children2 if c.get("address")}
        print(f"\n  Run 2 child addresses: {child_addrs2}")

        # Key assertion: addresses from run 1 should NOT be re-spawned as
        # child jobs in run 2 (dedup).  New addresses may appear if the
        # inventory discovered more contracts.
        duplicated = child_addrs1 & child_addrs2
        print(f"  Addresses in both runs (should be empty): {duplicated}")
        assert len(duplicated) == 0, (
            f"Run 2 re-spawned child jobs for already-analyzed addresses: {duplicated}"
        )

    def test_second_company_run_inventory_merged(self):
        """Run 2 inventory should contain contracts from both runs."""
        parent2 = getattr(TestCompanyCaching, "_parent2", None)
        inventory1 = getattr(TestCompanyCaching, "_inventory1", None)
        if parent2 is None:
            pytest.skip("Second company run did not complete")

        run_name2 = parent2.get("name")
        if not run_name2:
            pytest.skip("Second company job has no run name")

        inventory2 = get_artifact(run_name2, "contract_inventory")
        if inventory2 is None:
            # Inventory might be on the parent job -- try fetching via job_id
            pytest.skip("Could not fetch inventory artifact for run 2")

        contracts2 = inventory2.get("contracts", [])
        assert len(contracts2) > 0, "Run 2 inventory should have contracts"

        if inventory1 is not None:
            contracts1 = inventory1.get("contracts", [])
            addrs1 = {c.get("address", "").lower() for c in contracts1 if c.get("address")}
            addrs2 = {c.get("address", "").lower() for c in contracts2 if c.get("address")}
            # Run 2 inventory should be a superset of (or equal to) run 1
            missing = addrs1 - addrs2
            print(f"\n  Run 1 inventory: {len(addrs1)} addresses")
            print(f"  Run 2 inventory: {len(addrs2)} addresses")
            if missing:
                print(f"  WARNING: Run 2 lost {len(missing)} addresses from run 1: {missing}")
            # Merged inventory should have at least as many contracts
            assert len(addrs2) >= len(addrs1), (
                f"Expected run 2 inventory ({len(addrs2)}) >= run 1 ({len(addrs1)})"
            )
