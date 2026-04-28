"""Worker-pool health: catches wedged ``processing`` jobs that ``reclaim_stuck_jobs`` failed to re-queue."""

from __future__ import annotations

from datetime import datetime, timezone

from tests.live.conftest import LiveClient, _parse_dt

# Real runs rarely exceed 10-15 min even on cold previews; 20 keeps false positives near zero.
WEDGE_THRESHOLD_SECONDS = 20 * 60


def test_no_wedged_jobs(live_client: LiveClient):
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
            # Malformed timestamp on a live job is itself a signal.
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
