"""Queue re-analysis jobs when governance events indicate stale analysis data."""

from __future__ import annotations

import logging
import os

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from db.models import Job, JobStatus, MonitoredContract
from db.queue import create_job

logger = logging.getLogger(__name__)

# Event types that should trigger a full re-analysis job.
REANALYSIS_EVENT_TYPES = frozenset({
    # Proxy upgrades — implementation code changed, entire analysis is stale
    "upgraded",
    "new_implementation",
    "changed_master_copy",
    "target_updated",
    # Beacon upgrade — all proxies pointing at this beacon delegate to new code
    "beacon_upgraded",
    # Admin changed — control graph and effective permissions are stale
    "admin_changed",
    # Ownership transferred — control graph needs re-resolution
    "ownership_transferred",
})

# State-poll field names that map to the same triggers above.
REANALYSIS_POLL_FIELDS = frozenset({
    "implementation",  # equivalent to proxy upgrade
    "owner",           # equivalent to ownership_transferred
})


def should_trigger_reanalysis(event_type: str, data: dict | None = None) -> bool:
    """Return True if *event_type* (with optional *data*) warrants a re-analysis."""
    if event_type in REANALYSIS_EVENT_TYPES:
        return True
    if event_type == "state_changed_poll" and data:
        return data.get("field") in REANALYSIS_POLL_FIELDS
    return False


def maybe_queue_reanalysis(
    session: Session,
    mc: MonitoredContract,
    event_type: str,
    data: dict | None = None,
) -> Job | None:
    """Queue a re-analysis job if the event warrants it.

    Checks:
    1. Event type is in the trigger set.
    2. No queued or processing job already exists for this address+chain.

    The job starts at the ``discovery`` stage so the caching system can
    copy static artifacts (source files, contract_analysis, etc.) and the
    static worker can detect implementation changes via
    ``_check_proxy_cache``.

    Returns the created :class:`Job`, or ``None`` if skipped.
    """
    if not should_trigger_reanalysis(event_type, data):
        return None

    # Deduplicate: skip if a job is already in-flight for this address+chain.
    in_flight_candidates = (
        session.execute(
            select(Job).where(
                func.lower(Job.address) == mc.address.lower(),
                Job.status.in_([JobStatus.queued, JobStatus.processing]),
            )
        )
        .scalars()
        .all()
    )
    for candidate in in_flight_candidates:
        req = candidate.request if isinstance(candidate.request, dict) else {}
        if req.get("chain", "ethereum") == mc.chain:
            logger.info(
                "Skipping re-analysis for %s: job %s already in-flight (stage=%s)",
                mc.address,
                candidate.id,
                candidate.stage.value,
            )
            return None

    # Determine a human-readable trigger label
    if event_type == "state_changed_poll":
        trigger = f"poll:{(data or {}).get('field', 'unknown')}"
    else:
        trigger = event_type

    rpc_url = os.environ.get("ETH_RPC", "https://ethereum-rpc.publicnode.com")

    request_dict: dict = {
        "address": mc.address,
        "chain": mc.chain,
        "name": f"Re-analysis ({trigger})",
        "rpc_url": rpc_url,
        "reanalysis_trigger": trigger,
    }
    if mc.protocol_id:
        request_dict["protocol_id"] = mc.protocol_id

    job = create_job(session, request_dict)

    logger.info(
        "Queued re-analysis job %s for %s (trigger: %s)",
        job.id,
        mc.address,
        trigger,
    )
    return job
