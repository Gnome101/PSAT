"""Live integration test for pipeline stage advancement.

``test_pipeline_stages.py`` only checks the terminal ``stage == "done"``
result. A worker that silently skipped its stage but left a stale
artifact would still pass that assertion. This file polls the job mid-
analysis and verifies the stage values it observes form a prefix of the
canonical single-contract pipeline order.

Canonical order (db/models.py:44 + workers/*.py advance_job calls):
    discovery → static → resolution → policy → coverage → done

The ``selection`` stage only fires for company jobs (the discovery worker
splits company → selection on workers/discovery.py:217-222), so it is
intentionally excluded from the single-address sequence below.
"""

from __future__ import annotations

import time

import pytest

from tests.live.conftest import WETH_ADDRESS, LiveClient

# Canonical stage order for a single-address analysis. Each stage's
# transition is owned by the worker named after it (workers/discovery.py,
# static_worker.py, resolution_worker.py, policy_worker.py, coverage_worker.py).
EXPECTED_ORDER = ["discovery", "static", "resolution", "policy", "coverage", "done"]


def _is_prefix(observed: list[str], canonical: list[str]) -> bool:
    """True iff ``observed`` (in observation order) appears as a subsequence
    of ``canonical`` with each element appearing in the canonical order.

    We don't require every stage to be observed — fast cached runs can
    transition between polls — but every stage we *did* see must occur in
    the canonical order with no out-of-order or unknown entries.
    """
    j = 0
    for stage in observed:
        while j < len(canonical) and canonical[j] != stage:
            j += 1
        if j >= len(canonical):
            return False
        j += 1
    return True


def test_stages_advance_in_order(live_client: LiveClient):
    """Submit a fresh WETH run and assert the observed stage sequence is
    a prefix of the canonical order.

    Polls every 1s (rather than the default 5s) because the static stage
    on a cached run can be sub-2-second; coarser polling would routinely
    miss intermediate stages and weaken the test to "must reach done"
    which is what test_pipeline_stages.py already covers.
    """
    job_id = live_client.analyze(WETH_ADDRESS)["job_id"]
    seen_stages: list[str] = []
    deadline = time.time() + 600  # match DEFAULT_SINGLE_TIMEOUT

    while time.time() < deadline:
        snap = live_client.job(job_id)
        stage = snap.get("stage")
        if stage and (not seen_stages or seen_stages[-1] != stage):
            seen_stages.append(stage)
        if snap["status"] in ("completed", "failed"):
            break
        time.sleep(1)
    else:
        pytest.fail(f"Job {job_id} did not reach a terminal status; stages seen: {seen_stages}")

    final = live_client.job(job_id)
    assert final["status"] == "completed", (
        f"Job did not complete: status={final['status']} error={final.get('error')} stages={seen_stages}"
    )
    # The final stage must be ``done``; the API caller would otherwise see
    # a "completed" job that's somehow not at the terminal stage.
    assert final["stage"] == "done", f"Completed job not at done stage: {final['stage']}"

    # If polling caught the very-first stage emission, ``discovery`` should
    # be there. If we only ever saw ``done`` (caching too fast for any
    # other observation), don't fail — the prefix check below still
    # validates we never saw an out-of-order stage.
    assert seen_stages, "no stage observations recorded"
    assert _is_prefix(seen_stages, EXPECTED_ORDER), (
        f"observed stages {seen_stages} are not a prefix of canonical order {EXPECTED_ORDER}"
    )
    # ``done`` must be the last thing we saw (since polling exits on terminal status).
    assert seen_stages[-1] == "done", f"final observed stage should be 'done', got {seen_stages[-1]}"
