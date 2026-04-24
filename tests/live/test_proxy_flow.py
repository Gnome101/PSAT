"""Live integration tests for EIP-1967 proxy handling.

Analyzes USDC's FiatTokenProxy (a canonical EIP-1967 proxy) and asserts
the classifier correctly identifies it as a proxy, resolves the
implementation, emits the ``classifications`` artifact, and spawns a
child job to analyze the implementation.
"""

from __future__ import annotations

import pytest

from tests.live.conftest import DEFAULT_SINGLE_TIMEOUT, LiveClient

# USDC FiatTokenProxy on Ethereum mainnet. EIP-1967 storage slot, widely
# known impl, verified on Etherscan, and the impl itself is small enough
# to finish in the same window as the proxy analysis.
USDC_PROXY = "0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48"


@pytest.fixture(scope="module")
def usdc_job(analyze_and_wait) -> dict:
    # ``analyze_and_wait`` is function-scoped, but the proxy fixture only
    # needs one USDC run for the whole module — module-scoped amortizes
    # the ~3-5 min analysis across every test in this file.
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
    # Distinct from the proxy itself — otherwise resolution silently failed.
    assert impl.lower() != USDC_PROXY.lower(), "implementation must differ from proxy address"


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

    # The pipeline spawns a child job for the impl. Poll children until the
    # impl job reaches a terminal state — the parent can be "completed"
    # before the impl child finishes, especially on a cold preview.
    children = live_client.poll_children_until_done(usdc_job["job_id"])
    impl_jobs = [c for c in children if (c.get("address") or "").lower() == impl]
    assert impl_jobs, (
        f"No child job spawned for implementation {impl}. Children: {[c.get('address') for c in children]}"
    )
    impl_job = impl_jobs[0]
    assert impl_job["status"] == "completed", (
        f"Implementation job did not complete: status={impl_job['status']} error={impl_job.get('error')}"
    )
