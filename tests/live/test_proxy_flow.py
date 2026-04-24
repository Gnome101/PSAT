"""EIP-1967 proxy flow: classify → resolve impl → emit classifications → spawn impl child job."""

from __future__ import annotations

import pytest

from tests.live.conftest import DEFAULT_SINGLE_TIMEOUT, LiveClient

USDC_PROXY = "0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48"  # USDC FiatTokenProxy, EIP-1967


@pytest.fixture(scope="module")
def usdc_job(analyze_and_wait) -> dict:
    # Module-scoped wrapper around the function-scoped factory to amortize the USDC run.
    job = analyze_and_wait(USDC_PROXY, timeout=DEFAULT_SINGLE_TIMEOUT)
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

    # Parent can be "completed" before impl child finishes on cold previews — poll children.
    children = live_client.poll_children_until_done(usdc_job["job_id"])
    impl_jobs = [c for c in children if (c.get("address") or "").lower() == impl]
    assert impl_jobs, (
        f"No child job spawned for implementation {impl}. Children: {[c.get('address') for c in children]}"
    )
    impl_job = impl_jobs[0]
    assert impl_job["status"] == "completed", (
        f"Implementation job did not complete: status={impl_job['status']} error={impl_job.get('error')}"
    )
