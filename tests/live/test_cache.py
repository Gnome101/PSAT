"""Live integration tests for caching behavior.

Re-analyzing the same contract (or company) should reuse cached static data
from the first run, not re-scaffold / re-run slither from scratch.
"""

from __future__ import annotations

import pytest

from tests.live.conftest import WETH_ADDRESS, LiveClient

COMPANY_NAME = "etherfi"
COMPANY_LIMIT = 2


# ---------------------------------------------------------------------------
# Single contract
# ---------------------------------------------------------------------------


class TestSingleContractCaching:
    """Submit WETH twice. The second run should reuse cached static data."""

    def test_first_run_completes(self, analyzed_weth):
        # ``analyzed_weth`` is session-scoped and fails the fixture if the
        # first run didn't complete — so reaching this line already proves it.
        assert analyzed_weth["status"] == "completed"

    def test_first_run_has_artifacts(self, analyzed_weth, live_client: LiveClient):
        analysis = live_client.artifact(analyzed_weth["name"], "contract_analysis")
        assert isinstance(analysis, dict)
        assert "subject" in analysis or "summary" in analysis

    def test_second_run_uses_cache(self, analyzed_weth, live_client: LiveClient):
        job2 = live_client.submit_and_wait(WETH_ADDRESS)
        assert job2["status"] == "completed", f"Second run failed: {job2.get('error')}"

        req2 = job2.get("request") or {}
        assert req2.get("static_cached") is True
        assert req2.get("cache_source_job_id") == analyzed_weth["job_id"]

        a1 = live_client.artifact(analyzed_weth["name"], "contract_analysis")
        a2 = live_client.artifact(job2["name"], "contract_analysis")
        assert isinstance(a1, dict) and isinstance(a2, dict)
        assert a1.get("subject", {}).get("name") == a2.get("subject", {}).get("name")

        # Stash the second job on the class so the timing test can read it.
        TestSingleContractCaching._job2 = job2

    def test_second_run_completed_faster(self, analyzed_weth, live_client: LiveClient):
        job2 = getattr(TestSingleContractCaching, "_job2", None)
        if job2 is None:
            pytest.skip("test_second_run_uses_cache must run first")

        t1 = live_client.job_duration_seconds(analyzed_weth)
        t2 = live_client.job_duration_seconds(job2)
        # Soft check: timings below 30s are dominated by fixed overhead and
        # can flap either way. Only assert when the first run was substantial.
        if t1 > 30:
            assert t2 < t1, f"Second run ({t2:.1f}s) should be faster than first ({t1:.1f}s)"


# ---------------------------------------------------------------------------
# Company
# ---------------------------------------------------------------------------


class TestCompanyCaching:
    """Submit the same company twice. The second run should merge inventory
    and skip already-analyzed non-proxy addresses."""

    def test_first_company_run_completes(self, live_client: LiveClient):
        parent = live_client.submit_company_and_wait(COMPANY_NAME, limit=COMPANY_LIMIT)
        assert parent["status"] == "completed", f"Company run 1 failed: {parent.get('error')}"
        TestCompanyCaching._parent1 = parent

    def test_first_company_run_has_children(self, live_client: LiveClient):
        parent1 = getattr(TestCompanyCaching, "_parent1", None)
        if parent1 is None:
            pytest.skip("First company run did not complete")

        children = live_client.poll_children_until_done(parent1["job_id"])
        assert children, "Expected at least one child job"
        completed = [c for c in children if c["status"] == "completed"]
        assert completed, f"No completed children; statuses: {[c['status'] for c in children]}"

        TestCompanyCaching._child_addrs1 = {c["address"].lower() for c in children if c.get("address")}

    def test_first_company_run_has_inventory(self, live_client: LiveClient):
        parent1 = getattr(TestCompanyCaching, "_parent1", None)
        if parent1 is None:
            pytest.skip("First company run did not complete")

        inventory = live_client.artifact(parent1["name"], "contract_inventory")
        if inventory is None:
            pytest.skip("No contract_inventory artifact on parent job")

        assert isinstance(inventory, dict)
        assert inventory.get("contracts"), "Inventory should have discovered contracts"
        TestCompanyCaching._inventory1 = inventory

    def test_second_company_run_deduplicates(self, live_client: LiveClient):
        parent1 = getattr(TestCompanyCaching, "_parent1", None)
        child_addrs1 = getattr(TestCompanyCaching, "_child_addrs1", None)
        if parent1 is None or child_addrs1 is None:
            pytest.skip("First company run data not available")

        parent2 = live_client.submit_company_and_wait(COMPANY_NAME, limit=COMPANY_LIMIT)
        assert parent2["status"] == "completed", f"Company run 2 failed: {parent2.get('error')}"
        TestCompanyCaching._parent2 = parent2

        children2 = live_client.poll_children_until_done(parent2["job_id"])
        child_addrs2 = {c["address"].lower() for c in children2 if c.get("address")}

        # Proxies are intentionally re-queued on every run for upgrade checks,
        # so only non-proxy duplicates are a real cache miss.
        proxy_addrs = {(j.get("address") or "").lower() for j in live_client.jobs() if j.get("is_proxy")}
        non_proxy_dup = (child_addrs1 & child_addrs2) - proxy_addrs
        assert not non_proxy_dup, f"Run 2 re-spawned jobs for already-analyzed non-proxy addresses: {non_proxy_dup}"

    def test_second_company_run_inventory_merged(self, live_client: LiveClient):
        parent2 = getattr(TestCompanyCaching, "_parent2", None)
        inventory1 = getattr(TestCompanyCaching, "_inventory1", None)
        if parent2 is None:
            pytest.skip("Second company run did not complete")

        inventory2 = live_client.artifact(parent2["name"], "contract_inventory")
        if not isinstance(inventory2, dict):
            pytest.skip("Could not fetch inventory for run 2")

        contracts2 = inventory2.get("contracts", [])
        assert contracts2, "Run 2 inventory should have contracts"

        if inventory1 is not None:
            addrs1 = {c.get("address", "").lower() for c in inventory1.get("contracts", []) if c.get("address")}
            addrs2 = {c.get("address", "").lower() for c in contracts2 if c.get("address")}
            assert len(addrs2) >= len(addrs1), (
                f"Run 2 inventory ({len(addrs2)}) should be a superset of run 1 ({len(addrs1)})"
            )
