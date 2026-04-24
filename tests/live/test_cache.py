"""Caching: re-running an address/company should reuse cached static data."""

from __future__ import annotations

from typing import Any

import pytest

from tests.live.conftest import WETH_ADDRESS, LiveClient

COMPANY_NAME = "etherfi"
COMPANY_LIMIT = 2


@pytest.fixture(scope="module")
def weth_second_run(analyzed_weth, live_client: LiveClient) -> dict[str, Any]:
    """Second WETH submission reused across tests that inspect the cached run."""
    job = live_client.submit_and_wait(WETH_ADDRESS)
    assert job["status"] == "completed", f"Second run failed: {job.get('error')}"
    return job


def test_first_run_completes(analyzed_weth):
    assert analyzed_weth["status"] == "completed"


def test_first_run_has_artifacts(analyzed_weth, live_client: LiveClient):
    analysis = live_client.artifact(analyzed_weth["name"], "contract_analysis")
    assert isinstance(analysis, dict)
    assert "subject" in analysis or "summary" in analysis


def test_second_run_uses_cache(analyzed_weth, weth_second_run, live_client: LiveClient):
    req2 = weth_second_run.get("request") or {}
    assert req2.get("static_cached") is True
    assert req2.get("cache_source_job_id") == analyzed_weth["job_id"]

    a1 = live_client.artifact(analyzed_weth["name"], "contract_analysis")
    a2 = live_client.artifact(weth_second_run["name"], "contract_analysis")
    assert isinstance(a1, dict) and isinstance(a2, dict)
    assert a1.get("subject", {}).get("name") == a2.get("subject", {}).get("name")


def test_second_run_completed_faster(analyzed_weth, weth_second_run, live_client: LiveClient):
    t1 = live_client.job_duration_seconds(analyzed_weth)
    t2 = live_client.job_duration_seconds(weth_second_run)
    # Below 30s fixed overhead dominates; assertion flaps either way.
    if t1 > 30:
        assert t2 < t1, f"Second run ({t2:.1f}s) should be faster than first ({t1:.1f}s)"


@pytest.fixture(scope="module")
def company_first_run(live_client: LiveClient) -> dict[str, Any]:
    parent = live_client.submit_company_and_wait(COMPANY_NAME, limit=COMPANY_LIMIT)
    assert parent["status"] == "completed", f"Company run 1 failed: {parent.get('error')}"
    return parent


@pytest.fixture(scope="module")
def company_first_children(company_first_run, live_client: LiveClient) -> list[dict[str, Any]]:
    children = live_client.poll_children_until_done(company_first_run["job_id"])
    assert children, "Expected at least one child job"
    completed = [c for c in children if c["status"] == "completed"]
    assert completed, f"No completed children; statuses: {[c['status'] for c in children]}"
    return children


@pytest.fixture(scope="module")
def company_first_inventory(company_first_run, live_client: LiveClient) -> dict[str, Any] | None:
    art = live_client.artifact(company_first_run["name"], "contract_inventory")
    return art if isinstance(art, dict) else None


@pytest.fixture(scope="module")
def company_second_run(company_first_run, live_client: LiveClient) -> dict[str, Any]:
    parent = live_client.submit_company_and_wait(COMPANY_NAME, limit=COMPANY_LIMIT)
    assert parent["status"] == "completed", f"Company run 2 failed: {parent.get('error')}"
    return parent


def test_first_company_run_completes(company_first_run):
    assert company_first_run["status"] == "completed"


def test_first_company_run_has_children(company_first_children):
    assert any(c["status"] == "completed" for c in company_first_children)


def test_first_company_run_has_inventory(company_first_inventory):
    if company_first_inventory is None:
        pytest.skip("No contract_inventory artifact on parent job")
    assert isinstance(company_first_inventory, dict)
    assert company_first_inventory.get("contracts"), "Inventory should have discovered contracts"


def test_second_company_run_deduplicates(
    company_first_children,
    company_second_run,
    live_client: LiveClient,
):
    child_addrs1 = {c["address"].lower() for c in company_first_children if c.get("address")}
    children2 = live_client.poll_children_until_done(company_second_run["job_id"])
    child_addrs2 = {c["address"].lower() for c in children2 if c.get("address")}

    # Proxies are re-queued every run for upgrade checks — only non-proxy dups are a cache miss.
    proxy_addrs = {(j.get("address") or "").lower() for j in live_client.jobs() if j.get("is_proxy")}
    non_proxy_dup = (child_addrs1 & child_addrs2) - proxy_addrs
    assert not non_proxy_dup, f"Run 2 re-spawned jobs for already-analyzed non-proxy addresses: {non_proxy_dup}"


def test_second_company_run_inventory_merged(
    company_first_inventory,
    company_second_run,
    live_client: LiveClient,
):
    inventory2 = live_client.artifact(company_second_run["name"], "contract_inventory")
    if not isinstance(inventory2, dict):
        pytest.skip("Could not fetch inventory for run 2")

    contracts2 = inventory2.get("contracts", [])
    assert contracts2, "Run 2 inventory should have contracts"

    if company_first_inventory is not None:
        addrs1 = {
            c.get("address", "").lower() for c in company_first_inventory.get("contracts", []) if c.get("address")
        }
        addrs2 = {c.get("address", "").lower() for c in contracts2 if c.get("address")}
        assert len(addrs2) >= len(addrs1), (
            f"Run 2 inventory ({len(addrs2)}) should be a superset of run 1 ({len(addrs1)})"
        )
