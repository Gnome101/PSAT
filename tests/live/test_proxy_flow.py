"""EIP-1967 proxy flow: classify → resolve impl → emit classifications → spawn impl child job."""

from __future__ import annotations

import pytest

from tests.live.conftest import DEFAULT_SINGLE_TIMEOUT, LiveClient

USDC_PROXY = "0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48"  # USDC FiatTokenProxy, EIP-1967


@pytest.fixture(scope="module")
def usdc_job(live_client: LiveClient) -> dict:
    # Module-scoped so the USDC run is amortized across the four tests in this file.
    job = live_client.submit_and_wait(USDC_PROXY, timeout=DEFAULT_SINGLE_TIMEOUT)
    if job["status"] != "completed":
        pytest.fail(f"USDC proxy analysis did not complete: {job.get('error')}")
    return job


def test_contract_flags_marks_proxy(usdc_job, live_client: LiveClient):
    flags = live_client.artifact(usdc_job["name"], "contract_flags")
    assert isinstance(flags, dict)
    assert flags.get("is_proxy") is True, f"USDC should be detected as a proxy, got flags={flags}"


def test_implementation_is_resolved(usdc_job, live_client: LiveClient):
    flags = live_client.artifact(usdc_job["name"], "contract_flags")
    assert isinstance(flags, dict)
    impl = flags.get("implementation")
    assert isinstance(impl, str) and impl.startswith("0x") and len(impl) == 42, (
        f"implementation should be a 0x-prefixed address, got {impl!r}"
    )
    assert impl.lower() != USDC_PROXY.lower(), "implementation must differ from proxy (else resolution silently failed)"


def test_classifications_artifact_non_empty(usdc_job, live_client: LiveClient):
    cls = live_client.artifact(usdc_job["name"], "classifications")
    assert isinstance(cls, dict), "classifications artifact should exist and be JSON"
    entries = cls.get("classifications") or {}
    assert entries, "classifications map should not be empty for a proxy"


def test_implementation_job_completed(usdc_job, live_client: LiveClient):
    flags = live_client.artifact(usdc_job["name"], "contract_flags")
    assert isinstance(flags, dict)
    impl = (flags.get("implementation") or "").lower()
    assert impl

    # The invariant is "the impl has a completed analysis job somewhere" — not "this
    # specific parent spawned a new child for it". On a warm preview DB the static
    # worker logs ``impl <addr> already has job <id>, skipping`` and reuses the
    # existing impl analysis, so ``children_of(parent)`` is empty and polling it
    # times out. Look up the impl by address across all jobs instead.
    children = live_client.children_of(usdc_job["job_id"])
    child_match = [c for c in children if (c.get("address") or "").lower() == impl]
    if child_match:
        impl_job = child_match[0]
    else:
        all_jobs = live_client.jobs()
        candidates = [j for j in all_jobs if (j.get("address") or "").lower() == impl]
        assert candidates, f"No analysis job of any age found for implementation {impl}"
        # Prefer a completed job; fall back to the most recent so the error reports the real state.
        completed = [j for j in candidates if j["status"] == "completed"]
        impl_job = completed[0] if completed else candidates[0]

    assert impl_job["status"] == "completed", (
        f"Implementation {impl} analysis is {impl_job['status']!r}: error={impl_job.get('error')}"
    )
