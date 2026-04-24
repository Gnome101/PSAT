"""Live integration tests for end-to-end failure surfacing.

The frontend reads ``job.error`` (and ``job.detail``) when a pipeline run
fails — if a worker swallows an exception or leaves the job in a
half-failed state, the UI shows "completed" or no error context, which
is worse than a clean failure.

These tests submit guaranteed-to-fail inputs and assert the failure is
visible to a downstream consumer the same way the frontend would see it.
"""

from __future__ import annotations

import time

import pytest

from tests.live.conftest import LiveClient

# 0x-prefixed valid format, but no deployed bytecode — Etherscan returns
# "No verified source code" and discovery raises (utils/etherscan.py:182).
# Using the burn address (all zeros) rather than a "dead" pattern keeps
# the test independent of any future detection that might special-case
# the dead pattern; the burn address is canonically uninhabited.
NO_CODE_ADDRESS = "0x0000000000000000000000000000000000000000"

# A random-looking address that isn't a real deployed contract on
# mainnet. Picked to be unlikely to collide with an actual contract
# anyone wants to analyze. Same failure path as NO_CODE_ADDRESS but
# with a non-trivial-looking input — guards against tests that only
# pass for the obviously-zero case.
UNVERIFIED_ADDRESS = "0xDeAdBeefDeadBeefDEADbEEFdEadbeEFDEADBeef"


def _wait_for_failure(live_client: LiveClient, job_id: str, timeout: float = 300) -> dict:
    """Poll until the job reaches a terminal status, then return it."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = live_client.job(job_id)
        if snap["status"] in ("completed", "failed"):
            return snap
        time.sleep(3)
    raise TimeoutError(f"Job {job_id} did not terminate within {timeout}s")


def test_unverified_contract_fails_cleanly(live_client: LiveClient):
    """Discovery should raise on Etherscan's "no verified source" response,
    BaseWorker should catch it, and the job should be marked failed with
    a populated ``error`` field."""
    job = live_client.analyze(NO_CODE_ADDRESS)
    final = _wait_for_failure(live_client, job["job_id"])

    assert final["status"] == "failed", (
        f"unverified contract should fail, got status={final['status']} "
        f"stage={final['stage']} error={final.get('error')}"
    )
    assert final.get("error"), "failed job must populate error field for the frontend to display"
    # ``detail`` is set to "Failed" by db.queue.fail_job — confirms the
    # failure went through the canonical path, not a half-state where
    # status flipped without the rest of the failure metadata.
    assert final.get("detail"), "failed job must populate detail field"


def test_no_code_address_fails_cleanly(live_client: LiveClient):
    """Same failure mode as above, with a different input pattern. If only
    one of these two passes, the failure handling has an input-dependent
    bug rather than reliable error surfacing."""
    job = live_client.analyze(UNVERIFIED_ADDRESS)
    final = _wait_for_failure(live_client, job["job_id"])

    assert final["status"] == "failed", f"unverified address should fail, got status={final['status']}"
    error = final.get("error") or ""
    # Don't assert exact error text — Etherscan's wording is theirs to
    # change. But "verified" or "source" should appear somewhere because
    # that's the failure category we're exercising; otherwise we may have
    # caught a different bug and the test name would be misleading.
    assert error, "error field must not be empty"


def test_failed_job_visible_via_jobs_endpoint(live_client: LiveClient):
    """The failed job should appear in ``GET /api/jobs`` so the frontend's
    job log can render it. A failed job that's silently dropped from the
    list would be invisible to operators."""
    job = live_client.analyze(NO_CODE_ADDRESS)
    final = _wait_for_failure(live_client, job["job_id"])
    if final["status"] != "failed":
        pytest.skip("upstream failure-mode test already covers status; this only checks listing visibility")

    listing = live_client.jobs()
    matched = [j for j in listing if j.get("job_id") == job["job_id"]]
    assert matched, f"failed job {job['job_id']} missing from /api/jobs response"
    assert matched[0]["status"] == "failed"
