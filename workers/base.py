"""Base worker loop with graceful SIGTERM handling."""

from __future__ import annotations

import logging
import os
import signal
import time
import traceback
import uuid

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from db.models import Job, JobStage, JobStatus, SessionLocal
from db.queue import advance_job, claim_job, fail_job, reclaim_stuck_jobs, update_job_detail
from utils.logging_setup import process_rss_mb

logger = logging.getLogger(__name__)

DEBUG_TIMING = os.getenv("PSAT_DEBUG_TIMING", "").lower() in ("1", "true", "yes")
STALE_JOB_TIMEOUT = int(os.getenv("PSAT_STALE_JOB_TIMEOUT", "180"))  # seconds
# How often to emit an idle heartbeat when the worker finds no work. One per
# minute is enough to prove liveness without drowning the log stream; any
# shorter and a fleet of 10 sub-workers per machine × N machines generates
# more noise than signal in Grafana.
IDLE_HEARTBEAT_SECONDS = int(os.getenv("PSAT_IDLE_HEARTBEAT_SECONDS", "60"))


class JobHandledDirectly(Exception):
    """Raised by process() when it has already completed/failed the job itself."""

    pass


class ProcessBudgetExceeded(Exception):
    """Raised by the SIGALRM watchdog when process() exceeds its stage budget.

    We learned the hard way (RW stuck 20min on a poll() syscall inside a
    third-party library with no socket timeout) that trusting every HTTP/
    subprocess call to self-terminate is not enough. This exception flows
    into the existing worker_id/job_id failure path, marks the job failed,
    and lets the loop move on.
    """


class BaseWorker:
    """Poll-based worker that claims jobs for a specific pipeline stage."""

    stage: JobStage
    next_stage: JobStage
    poll_interval: float = 2.0
    # Hard cap for a single `process()` invocation. If the default is wrong
    # for a slow stage (static analysis can legitimately run 20+ min on big
    # contracts), subclasses override via class attribute; env var is the
    # global floor for everyone else. Only set to 0 to disable.
    process_budget_seconds: int = int(os.getenv("PSAT_PROCESS_BUDGET_SECONDS", "900"))

    def __init__(self) -> None:
        self.worker_id = f"{self.__class__.__name__}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._running = True
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    def _handle_sigterm(self, signum: int, frame: object) -> None:
        logger.info("Worker %s received signal %s, shutting down gracefully", self.worker_id, signum)
        self._running = False

    def _on_process_timeout(self, signum: int, frame: object) -> None:
        """SIGALRM handler that trips the per-stage watchdog.

        Raising from a signal handler works because SIGALRM is delivered to
        the main (only) Python thread between opcodes. If process() is deep
        in a C call, the signal pending flag gets set and the exception
        fires as soon as Python regains control — which also interrupts
        blocking syscalls with EINTR, unsticking `poll()`/`recv()` hangs.
        """
        raise ProcessBudgetExceeded(
            f"process() exceeded {self.process_budget_seconds}s budget for stage={self.stage.value}"
        )

    def process(self, session: Session, job: Job) -> None:
        """Subclasses implement this to run their pipeline stage."""
        raise NotImplementedError

    def _claim_job(self, session: Session) -> Job | None:
        """Claim the next job for this stage. Default: stage + status match.

        Sweeps cross-stage stuck ``processing`` rows back to ``queued``
        before claiming. That keeps worker-crash recovery cheap (a single
        UPDATE per poll) and global — no single worker's stage is a
        bottleneck on recovery — so a crashed discovery worker's job can
        be picked up by any pollable peer on the very next tick.

        Override in subclasses that need a readiness-gated claim (e.g.
        ``CoverageWorker`` waits for all audits in the protocol to settle)
        or a multi-phase claim pattern (primary claim OR a stuck-job
        escape hatch). Keeping this as a hook means the subclass never
        needs to copy-paste the run loop just to swap one line.
        """
        reclaim_stuck_jobs(session)
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
            # Capturing the previous worker_id BEFORE clearing it is the
            # whole point of this log — operators correlate the id with the
            # dead worker's last heartbeat to identify which machine/sub-
            # worker crashed and why.
            prior_worker = job.worker_id
            logger.warning(
                "requeuing stale job",
                extra={
                    "reclaimed_by": self.worker_id,
                    "job_id": str(job.id),
                    "job_name": job.name or job.address,
                    "stage": self.stage.value,
                    "prior_worker_id": prior_worker,
                    "stuck_since": job.updated_at.isoformat(),
                    "stale_age_s": int((datetime.now(timezone.utc) - job.updated_at).total_seconds()),
                },
            )
            job.status = JobStatus.queued
            job.worker_id = None
            job.detail = "Re-queued after stale processing timeout"
        if stale:
            session.commit()

    def run_loop(self) -> None:
        logger.info(
            "worker starting",
            extra={
                "worker_id": self.worker_id,
                "stage": self.stage.value,
                "poll_interval": self.poll_interval,
                "rss_mb": process_rss_mb(),
            },
        )
        recovery_counter = 0
        last_idle_heartbeat = 0.0
        while self._running:
            session = SessionLocal()
            try:
                # Check for stale jobs every ~30 poll cycles (~60s at 2s interval)
                recovery_counter += 1
                if recovery_counter >= 30:
                    recovery_counter = 0
                    self._recover_stale_jobs(session)
                    # RSS heartbeat rides the same cadence — one log/minute/
                    # worker is plenty to attribute OOMs to the hot sub-
                    # worker without flooding Loki.
                    logger.info(
                        "worker heartbeat",
                        extra={
                            "worker_id": self.worker_id,
                            "stage": self.stage.value,
                            "rss_mb": process_rss_mb(),
                        },
                    )

                job = self._claim_job(session)
                if job is None:
                    now = time.monotonic()
                    # Prove liveness even when the worker has no work to do,
                    # but only emit at most once per IDLE_HEARTBEAT_SECONDS
                    # so an idle fleet doesn't spam the stream.
                    if now - last_idle_heartbeat >= IDLE_HEARTBEAT_SECONDS:
                        last_idle_heartbeat = now
                        queued_here = (
                            session.execute(
                                select(func.count(Job.id)).where(
                                    Job.stage == self.stage,
                                    Job.status == JobStatus.queued,
                                )
                            ).scalar()
                            or 0
                        )
                        logger.info(
                            "worker idle",
                            extra={
                                "worker_id": self.worker_id,
                                "stage": self.stage.value,
                                "queued_at_stage": int(queued_here),
                                "rss_mb": process_rss_mb(),
                            },
                        )
                    session.close()
                    time.sleep(self.poll_interval)
                    continue

                logger.info(
                    "worker claimed job",
                    extra={
                        "worker_id": self.worker_id,
                        "stage": self.stage.value,
                        "job_id": str(job.id),
                        "job_name": job.name or job.address,
                        "budget_s": self.process_budget_seconds,
                    },
                )
                t0 = time.monotonic()
                # Arm the watchdog before process() runs. signal.alarm(0)
                # in the finally clears it; signal.SIG_DFL reinstalls in
                # case a library swapped our handler during the call.
                budget = self.process_budget_seconds
                if budget > 0:
                    signal.signal(signal.SIGALRM, self._on_process_timeout)
                    signal.alarm(budget)
                try:
                    self.process(session, job)
                    if self.next_stage == JobStage.done:
                        from db.queue import complete_job

                        complete_job(session, job.id)
                    else:
                        advance_job(session, job.id, self.next_stage, f"Completed {self.stage.value}")
                    elapsed_ms = int((time.monotonic() - t0) * 1000)
                    logger.info(
                        "worker completed job",
                        extra={
                            "worker_id": self.worker_id,
                            "stage": self.stage.value,
                            "job_id": str(job.id),
                            "duration_ms": elapsed_ms,
                            "next_stage": self.next_stage.value,
                        },
                    )
                except JobHandledDirectly:
                    elapsed_ms = int((time.monotonic() - t0) * 1000)
                    logger.info(
                        "worker handled job directly",
                        extra={
                            "worker_id": self.worker_id,
                            "stage": self.stage.value,
                            "job_id": str(job.id),
                            "duration_ms": elapsed_ms,
                        },
                    )
                except ProcessBudgetExceeded:
                    # Distinct log from the generic failure path so ops can
                    # alert on budget trips specifically — they point at
                    # third-party hangs (network, subprocess) rather than
                    # logic errors in our code.
                    elapsed_ms = int((time.monotonic() - t0) * 1000)
                    logger.warning(
                        "worker process() budget exceeded",
                        extra={
                            "worker_id": self.worker_id,
                            "stage": self.stage.value,
                            "job_id": str(job.id),
                            "job_address": getattr(job, "address", None),
                            "job_name": getattr(job, "name", None),
                            "duration_ms": elapsed_ms,
                            "budget_s": self.process_budget_seconds,
                            "rss_mb": process_rss_mb(),
                        },
                    )
                    try:
                        session.rollback()
                        fail_job(
                            session,
                            job.id,
                            f"process() exceeded {self.process_budget_seconds}s budget",
                        )
                    except Exception:
                        logger.exception(
                            "fail_job after budget trip errored",
                            extra={"worker_id": self.worker_id, "job_id": str(job.id)},
                        )
                except Exception:
                    elapsed_ms = int((time.monotonic() - t0) * 1000)
                    error = traceback.format_exc()
                    # Single structured error record: Loki's `| json` surfaces
                    # every field for alerting/dashboards. The plain-text
                    # traceback rides along in `exc` for humans.
                    logger.error(
                        "worker job failed",
                        extra={
                            "worker_id": self.worker_id,
                            "stage": self.stage.value,
                            "job_id": str(job.id),
                            "job_address": getattr(job, "address", None),
                            "job_name": getattr(job, "name", None),
                            "duration_ms": elapsed_ms,
                            "rss_mb": process_rss_mb(),
                        },
                        exc_info=True,
                    )
                    try:
                        session.rollback()
                        fail_job(session, job.id, error)
                    except Exception:
                        logger.exception(
                            "fail_job rollback path errored",
                            extra={"worker_id": self.worker_id, "job_id": str(job.id)},
                        )
                        try:
                            fresh = SessionLocal()
                            fail_job(fresh, job.id, error[-4000:])
                            fresh.close()
                        except Exception:
                            logger.exception(
                                "fail_job fresh-session path errored — job stays stuck",
                                extra={"worker_id": self.worker_id, "job_id": str(job.id)},
                            )
            except Exception:
                logger.exception(
                    "worker main loop error",
                    extra={"worker_id": self.worker_id, "stage": self.stage.value},
                )
            finally:
                # Disarm the watchdog no matter how process() returned —
                # an alarm that outlives its intended window would fire
                # during the next idle-poll and crash the worker.
                signal.alarm(0)
                session.close()

        logger.info(
            "worker shut down",
            extra={"worker_id": self.worker_id, "stage": self.stage.value},
        )

    def update_detail(self, session: Session, job: Job, detail: str) -> None:
        """Update the job's progress detail message."""
        update_job_detail(session, job.id, detail)
