"""Base worker loop with graceful SIGTERM handling."""

from __future__ import annotations

import logging
import os
import signal
import time
import traceback
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import Job, JobStage, JobStatus, SessionLocal
from db.queue import (
    advance_job,
    claim_job,
    fail_job,
    reclaim_stuck_jobs,
    store_artifact,
    update_job_detail,
)
from utils.memory import (
    cgroup_memory_current_bytes,
    cgroup_memory_max_bytes,
    count_sibling_python_procs,
    current_rss_bytes,
    mb,
)

logger = logging.getLogger(__name__)

DEBUG_TIMING = os.getenv("PSAT_DEBUG_TIMING", "").lower() in ("1", "true", "yes")
STALE_JOB_TIMEOUT = int(os.getenv("PSAT_STALE_JOB_TIMEOUT", "180"))  # seconds

# Per-worker throttle for the stuck-job sweep; default 30s keeps fleet sweeps well under the 900s stale_timeout while
# cutting per-poll DB load.
RECLAIM_INTERVAL_S = float(os.getenv("PSAT_RECLAIM_INTERVAL_S", "30"))


class JobHandledDirectly(Exception):
    """Raised by process() when it has already completed/failed the job itself."""

    pass


class BaseWorker:
    """Poll-based worker that claims jobs for a specific pipeline stage."""

    stage: JobStage
    next_stage: JobStage
    poll_interval: float = 2.0

    def __init__(self) -> None:
        self.worker_id = f"{self.__class__.__name__}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._running = True
        # -inf = "never swept; sweep now"; throttles _claim_job to RECLAIM_INTERVAL_S between sweeps.
        self._last_reclaim_at: float = float("-inf")
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

        # One-line boot banner per worker process so a fly-log scrape can
        # reconstruct the fleet shape that hit OOM. Captures stage + RSS at
        # boot + the cgroup memory limit + sibling python proc count.
        # `stage` is a class attribute set by subclasses; default to "?"
        # so bare BaseWorker() in unit tests doesn't trip AttributeError.
        stage_attr = getattr(self, "stage", None)
        stage_str = stage_attr.value if stage_attr is not None else "?"
        logger.info(
            "[BOOT] worker=%s pid=%d stage=%s rss_mb=%s cgroup_used_mb=%s/%s python_siblings=%d",
            self.worker_id,
            os.getpid(),
            stage_str,
            mb(current_rss_bytes()),
            mb(cgroup_memory_current_bytes()),
            mb(cgroup_memory_max_bytes()),
            count_sibling_python_procs(),
        )

    def _handle_sigterm(self, signum: int, frame: object) -> None:
        logger.info("Worker %s received signal %s, shutting down gracefully", self.worker_id, signum)
        self._running = False

    def process(self, session: Session, job: Job) -> None:
        """Subclasses implement this to run their pipeline stage."""
        raise NotImplementedError

    def _claim_job(self, session: Session) -> Job | None:
        """Throttled stuck-job sweep + claim; override for readiness-gated or multi-phase claim patterns."""
        now = time.monotonic()
        if now - self._last_reclaim_at >= RECLAIM_INTERVAL_S:
            reclaim_stuck_jobs(session)
            self._last_reclaim_at = now
        return claim_job(session, self.stage, self.worker_id)

    def _recover_stale_jobs(self, session: Session) -> None:
        """Requeue jobs stuck in 'processing' for longer than STALE_JOB_TIMEOUT."""
        from datetime import datetime, timedelta, timezone

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=STALE_JOB_TIMEOUT)
        stale = (
            session.execute(
                select(Job).where(
                    Job.stage == self.stage,
                    Job.status == JobStatus.processing,
                    Job.updated_at < cutoff,
                )
            )
            .scalars()
            .all()
        )
        for job in stale:
            logger.warning(
                "Worker %s: requeuing stale job %s (%s) — stuck since %s",
                self.worker_id,
                job.id,
                job.name or job.address,
                job.updated_at.isoformat(),
            )
            job.status = JobStatus.queued
            job.worker_id = None
            job.detail = "Re-queued after stale processing timeout"
        if stale:
            session.commit()

    def run_loop(self) -> None:
        logger.info("Worker %s starting (stage=%s)", self.worker_id, self.stage.value)
        recovery_counter = 0
        while self._running:
            session = SessionLocal()
            try:
                # Check for stale jobs every ~30 poll cycles (~60s at 2s interval)
                recovery_counter += 1
                if recovery_counter >= 30:
                    recovery_counter = 0
                    self._recover_stale_jobs(session)

                job = self._claim_job(session)
                if job is None:
                    session.close()
                    time.sleep(self.poll_interval)
                    continue

                logger.info("Worker %s claimed job %s", self.worker_id, job.id)
                t0 = time.monotonic()
                rss_before = current_rss_bytes()
                started_at_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                try:
                    self.process(session, job)
                    elapsed = time.monotonic() - t0
                    ended_at_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                    rss_after = current_rss_bytes()
                    rss_delta_mb = (rss_after - rss_before) / (1024 * 1024)
                    logger.info(
                        "[JOB] worker=%s job=%s stage=%s elapsed_s=%.1f rss_mb=%s delta_mb=%+.0f cgroup_used_mb=%s",
                        self.worker_id,
                        job.id,
                        self.stage.value,
                        elapsed,
                        mb(rss_after),
                        rss_delta_mb,
                        mb(cgroup_memory_current_bytes()),
                    )
                    # Record timing before advancing — otherwise the next-stage worker can race read-modify-write on the
                    # shared stage_timings artifact.
                    self._record_stage_timing(
                        session,
                        job,
                        started_at=started_at_iso,
                        ended_at=ended_at_iso,
                        elapsed_s=elapsed,
                        status="success",
                    )
                    if self.next_stage == JobStage.done:
                        from db.queue import complete_job

                        complete_job(session, job.id)
                    else:
                        advance_job(session, job.id, self.next_stage, f"Completed {self.stage.value}")
                    logger.info(
                        "Worker %s completed job %s in %.1fs",
                        self.worker_id,
                        job.id,
                        elapsed,
                    )
                except JobHandledDirectly:
                    elapsed = time.monotonic() - t0
                    ended_at_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                    # Fresh session — process() may have left the original in an inconsistent state.
                    try:
                        fresh_for_timing = SessionLocal()
                        self._record_stage_timing(
                            fresh_for_timing,
                            job,
                            started_at=started_at_iso,
                            ended_at=ended_at_iso,
                            elapsed_s=elapsed,
                            status="handled_directly",
                        )
                        fresh_for_timing.close()
                    except Exception:
                        logger.exception("Worker %s: failed to record handled_directly timing", self.worker_id)
                    logger.info("Worker %s: job %s handled directly by process()", self.worker_id, job.id)
                except Exception:
                    elapsed = time.monotonic() - t0
                    ended_at_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                    error = traceback.format_exc()
                    logger.error(
                        "\n=================== WORKER FAILURE ===================\n"
                        "Worker:   %s\n"
                        "Job:      %s\n"
                        "Address:  %s\n"
                        "Name:     %s\n"
                        "Stage:    %s\n"
                        "-------------------------------------------------------\n"
                        "%s"
                        "=======================================================",
                        self.worker_id,
                        job.id,
                        getattr(job, "address", "?"),
                        getattr(job, "name", "?"),
                        self.stage.value,
                        error,
                    )
                    try:
                        session.rollback()
                        fail_job(session, job.id, error)
                        self._record_stage_timing(
                            session,
                            job,
                            started_at=started_at_iso,
                            ended_at=ended_at_iso,
                            elapsed_s=elapsed,
                            status="failed",
                        )
                    except Exception:
                        logger.exception("Failed to mark job %s as failed, retrying with fresh session", job.id)
                        try:
                            fresh = SessionLocal()
                            fail_job(fresh, job.id, error[-4000:])
                            self._record_stage_timing(
                                fresh,
                                job,
                                started_at=started_at_iso,
                                ended_at=ended_at_iso,
                                elapsed_s=elapsed,
                                status="failed",
                            )
                            fresh.close()
                        except Exception:
                            logger.exception("Could not mark job %s as failed even with fresh session", job.id)
            except Exception:
                logger.exception("Worker %s encountered error in main loop", self.worker_id)
            finally:
                session.close()

        logger.info("Worker %s shut down", self.worker_id)

    def update_detail(self, session: Session, job: Job, detail: str) -> None:
        """Update the job's progress detail message."""
        update_job_detail(session, job.id, detail)

    def _record_stage_timing(
        self,
        session: Session,
        job: Job,
        *,
        started_at: str,
        ended_at: str,
        elapsed_s: float,
        status: str,
    ) -> None:
        """Write this stage's timing as a ``stage_timing_<stage>`` artifact (one slot per stage avoids cross-stage RMW
        races); best-effort with session rollback on failure."""
        artifact_name = f"stage_timing_{self.stage.value}"
        payload = {
            "schema_version": "2",
            "stage": self.stage.value,
            "started_at": started_at,
            "ended_at": ended_at,
            "elapsed_s": round(elapsed_s, 3),
            "worker_id": self.worker_id,
            "status": status,
        }
        try:
            store_artifact(session, job.id, artifact_name, data=payload)
        except Exception:
            logger.exception("Worker %s: failed to record %s (non-fatal)", self.worker_id, artifact_name)
            # Mid-transaction failure leaves the session needing rollback or the success path's advance_job will raise
            # PendingRollbackError.
            try:
                session.rollback()
            except Exception:
                logger.exception("Worker %s: failed to rollback after timing write failure", self.worker_id)
