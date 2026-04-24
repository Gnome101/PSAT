"""Live integration tests for worker-pool health.

A worker that crashes or wedges mid-job leaves the job in
``status=processing`` indefinitely. The reclaim path
(``db.queue.reclaim_stuck_jobs``) is supposed to detect that and
re-queue. If it isn't running, jobs accumulate in processing forever and
new submissions never make it past discovery — the canonical wedge.

Detect this directly: scan the visible jobs list for anything stuck in
``processing`` for longer than the largest legitimate single-stage
runtime. If the assertion fires, the preview is unhealthy and every
other live test is at risk of false-failing on a depleted worker pool.
"""

from __future__ import annotations

from datetime import datetime, timezone

from tests.live.conftest import LiveClient, _parse_dt

# Generous ceiling — a real run hardly ever stays in processing past
# 10-15 min even on cold previews (cold-start + worst-case slither).
# 20 min keeps false positives near zero while still being well below
# "wedged forever".
WEDGE_THRESHOLD_SECONDS = 20 * 60


def test_no_wedged_jobs(live_client: LiveClient):
    """Nothing should be stuck in ``processing`` for more than ~20 min."""
    now = datetime.now(timezone.utc)
    stuck = []
    for job in live_client.jobs():
        if job.get("status") != "processing":
            continue
        updated_at = job.get("updated_at")
        if not updated_at:
            continue
        try:
            updated = _parse_dt(updated_at)
        except (ValueError, TypeError):
            # Malformed timestamp on a processing job is itself a signal
            # something's off — treat as suspicious so it surfaces here.
            stuck.append(job)
            continue
        age = (now - updated).total_seconds()
        if age > WEDGE_THRESHOLD_SECONDS:
            stuck.append(job)

    assert not stuck, (
        "jobs stuck in processing > "
        f"{WEDGE_THRESHOLD_SECONDS // 60} min: "
        f"{[(j.get('job_id'), j.get('stage'), j.get('updated_at')) for j in stuck]}"
    )
