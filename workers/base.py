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
    get_artifact,
    reclaim_stuck_jobs,
    store_artifact,
    update_job_detail,
)

logger = logging.getLogger(__name__)

DEBUG_TIMING = os.getenv("PSAT_DEBUG_TIMING", "").lower() in ("1", "true", "yes")
STALE_JOB_TIMEOUT = int(os.getenv("PSAT_STALE_JOB_TIMEOUT", "180"))  # seconds

# Per-worker throttle for the stuck-job sweep that runs inside _claim_job.
# Without it, every worker swept on every poll: 10 procs × poll_interval=2s
# = 5 cross-stage UPDATE-with-SKIP-LOCKED queries per second, 24/7. With
# this throttle each worker sweeps at most every N seconds, so the fleet
# does ~10 sweeps per N (default ~1 every 3s) — still well under the
# 900s stale_timeout, so recovery latency is unaffected.
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
        # Track when this worker last ran the cross-stage stuck-job sweep,
        # so _claim_job can throttle to RECLAIM_INTERVAL_S instead of
        # sweeping on every poll. Sentinel -inf = "never swept; sweep now".
        self._last_reclaim_at: float = float("-inf")
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    def _handle_sigterm(self, signum: int, frame: object) -> None:
        logger.info("Worker %s received signal %s, shutting down gracefully", self.worker_id, signum)
        self._running = False

    def process(self, session: Session, job: Job) -> None:
        """Subclasses implement this to run their pipeline stage."""
        raise NotImplementedError

    def _claim_job(self, session: Session) -> Job | None:
        """Claim the next job for this stage. Default: stage + status match.

        Sweeps cross-stage stuck ``processing`` rows back to ``queued``
        before claiming, but throttled per-worker to once every
        ``RECLAIM_INTERVAL_S`` seconds. With 10 worker procs this still
        gives the fleet a sweep every ~3s — well under the 900s
        stale_timeout — while cutting per-poll DB load 15×.

        Recovery stays global: every worker is still eligible to sweep,
        and ``SKIP LOCKED`` keeps concurrent sweeps from contending. A
        crashed worker's stuck job is rescued within at most one
        RECLAIM_INTERVAL_S window, then any pollable peer can claim it.

        Override in subclasses that need a readiness-gated claim (e.g.
        ``CoverageWorker`` waits for all audits in the protocol to settle)
        or a multi-phase claim pattern (primary claim OR a stuck-job
        escape hatch). Keeping this as a hook means the subclass never
        needs to copy-paste the run loop just to swap one line.
        """
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
                started_at_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                try:
                    self.process(session, job)
                    elapsed = time.monotonic() - t0
                    ended_at_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                    # Record timing BEFORE advancing/completing — otherwise
                    # the next-stage worker can claim the row, finish, and
                    # write its own stage_timings entry concurrently. Both
                    # workers do read-modify-write on the same JSON
                    # artifact, so the later commit drops the earlier
                    # stage's entry. Recording first keeps the artifact
                    # write inside this worker's exclusive ownership of
                    # the row. Timing is best-effort (swallows errors)
                    # so this does not delay or block the advance.
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
                    # Use a fresh session — the worker's process() may have left
                    # the original in an inconsistent state when raising.
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
        """Append this stage's timing to the per-job ``stage_timings`` artifact.

        Schema (v1):
          {
            "schema_version": "1",
            "stages": [
              {
                "stage":      "discovery" | "static" | "resolution" | ...,
                "started_at": ISO 8601 UTC,
                "ended_at":   ISO 8601 UTC,
                "elapsed_s":  float (monotonic),
                "worker_id":  str,
                "status":     "success" | "failed" | "handled_directly",
              },
              ...
            ]
          }

        Append-only. Each stage worker writes one entry on its own job
        when it advances/completes/fails the row. Lets the bench harness
        read durations reliably without scraping Fly logs (which routinely
        miss lines under buffering).

        Best-effort: a failure here logs and swallows — we'd rather lose
        a timing record than mark a job as failed for a metrics-only bug.
        """
        try:
            existing = get_artifact(session, job.id, "stage_timings")
            if isinstance(existing, dict) and isinstance(existing.get("stages"), list):
                payload = {"schema_version": existing.get("schema_version", "1"), "stages": list(existing["stages"])}
            else:
                payload = {"schema_version": "1", "stages": []}
            payload["stages"].append(
                {
                    "stage": self.stage.value,
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "elapsed_s": round(elapsed_s, 3),
                    "worker_id": self.worker_id,
                    "status": status,
                }
            )
            store_artifact(session, job.id, "stage_timings", data=payload)
        except Exception:
            logger.exception("Worker %s: failed to record stage_timings (non-fatal)", self.worker_id)
