"""Base worker loop with graceful SIGTERM handling."""

from __future__ import annotations

import contextvars
import logging
import os
import signal
import time
import traceback
import uuid
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import Job, JobStage, JobStatus, SessionLocal
from db.queue import (
    advance_job,
    claim_job,
    fail_job_terminal,
    get_artifact,
    reclaim_stuck_jobs,
    requeue_job,
    store_artifact,
    update_job_detail,
)
from schemas.stage_errors import StageError, StageErrors
from utils.logging import bind_trace_context, configure_logging, degraded_errors_var
from utils.memory import (
    cgroup_memory_current_bytes,
    cgroup_memory_max_bytes,
    count_sibling_python_procs,
    current_rss_bytes,
    mb,
)
from workers.retry_policy import classify, compute_next_attempt, max_retries

logger = logging.getLogger(__name__)

# Bumped to 600 alongside the threaded fan-outs: any single fan-out section can now legitimately go
# minutes without a status/detail write, so the previous 180s default would requeue live jobs.
# Mid-fan-out heartbeats (``BaseWorker._heartbeat``) keep ``updated_at`` fresh inside the long sections.
STALE_JOB_TIMEOUT = int(os.getenv("PSAT_STALE_JOB_TIMEOUT", "600"))  # seconds

# Per-worker throttle for the stuck-job sweep; default 30s keeps fleet sweeps well under the 900s stale_timeout while
# cutting per-poll DB load.
RECLAIM_INTERVAL_S = float(os.getenv("PSAT_RECLAIM_INTERVAL_S", "30"))


def _resolve_job_concurrency(stage_value: str) -> int:
    """Resolve K (max concurrent jobs per worker process) for *stage_value*.

    Precedence: per-stage env (``PSAT_<STAGE>_JOB_CONCURRENCY``) → global
    ``PSAT_JOB_CONCURRENCY`` → 1. K=1 takes a fast path that's behaviourally
    identical to the pre-concurrency loop; K>1 opts into the futures-based
    dispatcher. Subclasses that override ``_claim_job`` (coverage,
    selection — readiness-gated) stay K=1 implicitly: the per-stage env is
    just never set for them in production.
    """

    def _read(name: str) -> int | None:
        raw = os.getenv(name)
        if not raw:
            return None
        try:
            return max(1, int(raw))
        except ValueError:
            return None

    per_stage = _read(f"PSAT_{stage_value.upper()}_JOB_CONCURRENCY")
    if per_stage is not None:
        return per_stage
    return _read("PSAT_JOB_CONCURRENCY") or 1


class JobHandledDirectly(Exception):
    """Raised by process() when it has already completed/failed the job itself."""

    pass


class BaseWorker:
    """Poll-based worker that claims jobs for a specific pipeline stage."""

    stage: JobStage
    next_stage: JobStage
    poll_interval: float = 2.0

    def __init__(self) -> None:
        # Idempotent — the per-worker ``main()`` may have already called
        # this, but a bare ``BaseWorker()`` constructed in tests still
        # gets the JSON formatter installed so emitted log lines parse.
        configure_logging()
        self.worker_id = f"{self.__class__.__name__}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._running = True
        # -inf = "never swept; sweep now"; throttles _claim_job to RECLAIM_INTERVAL_S between sweeps.
        self._last_reclaim_at: float = float("-inf")
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

        # In-process job concurrency: K=1 keeps the legacy single-job loop
        # byte-identical; K>1 spins up a per-worker thread pool so RPC waits
        # in one job overlap with another job's CPU work. Resolved once at
        # boot so per-stage env tuning (e.g. PSAT_RESOLUTION_JOB_CONCURRENCY=2)
        # is visible in the boot banner below.
        stage_attr = getattr(self, "stage", None)
        stage_str = stage_attr.value if stage_attr is not None else "?"
        self._job_concurrency = _resolve_job_concurrency(stage_str)
        self._job_pool: ThreadPoolExecutor | None = None
        self._inflight: set[Future[None]] = set()
        if self._job_concurrency > 1:
            self._job_pool = ThreadPoolExecutor(
                max_workers=self._job_concurrency,
                thread_name_prefix=f"{self.__class__.__name__}-job",
            )

        # One-line boot banner per worker process so a fly-log scrape can
        # reconstruct the fleet shape that hit OOM. Captures stage + RSS at
        # boot + the cgroup memory limit + sibling python proc count.
        # `stage` is a class attribute set by subclasses; default to "?"
        # so bare BaseWorker() in unit tests doesn't trip AttributeError.
        logger.info(
            "[BOOT] worker=%s pid=%d stage=%s rss_mb=%s cgroup_used_mb=%s/%s python_siblings=%d job_concurrency=%d",
            self.worker_id,
            os.getpid(),
            stage_str,
            mb(current_rss_bytes()),
            mb(cgroup_memory_current_bytes()),
            mb(cgroup_memory_max_bytes()),
            count_sibling_python_procs(),
            self._job_concurrency,
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

    def _execute_job(self, session: Session, job: Job) -> None:
        """Run a single claimed job to completion: process → record timing → advance/complete/fail.

        Owns the full lifecycle of one (session, job) pair. Caller is
        responsible for closing *session* afterwards. Both the legacy
        single-job loop and the K>1 dispatcher route through here so the
        success/JobHandledDirectly/exception branches stay in one place.

        The whole body runs inside a ``bind_trace_context`` so every
        log line emitted from any helper called by ``process()`` carries
        the same ``trace_id`` / ``job_id`` / ``stage`` / ``worker_id``
        without callers having to thread them through.
        """
        # ``getattr`` defaults guard the test stubs that pass a bare
        # ``SimpleNamespace`` job without a request/trace_id field.
        raw_request = getattr(job, "request", None)
        request = raw_request if isinstance(raw_request, dict) else {}
        with bind_trace_context(
            trace_id=getattr(job, "trace_id", None),
            job_id=str(job.id),
            stage=self.stage.value,
            worker_id=self.worker_id,
            address=getattr(job, "address", None),
            chain=request.get("chain"),
        ):
            # Per-job accumulator for ``record_degraded`` calls. Reset
            # alongside ``bind_trace_context`` so K>1 jobs running in
            # parallel pool threads don't share a list (each thread's
            # context is a copy from the dispatcher).
            degraded_accumulator: list[StageError] = []
            accumulator_token = degraded_errors_var.set(degraded_accumulator)
            try:
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
                        extra={
                            "duration_ms": int(elapsed * 1000),
                            "phase": "job",
                            "rss_mb": mb(rss_after),
                            "rss_delta_mb": round(rss_delta_mb, 1),
                        },
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
                    # Drain degraded entries before advancing so a stage_errors
                    # artifact is visible to the next-stage worker at its claim.
                    if degraded_accumulator:
                        self._persist_stage_errors(job, degraded_accumulator)
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
                        extra={"duration_ms": int(elapsed * 1000), "phase": "job", "outcome": "success"},
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
                    if degraded_accumulator:
                        self._persist_stage_errors(job, degraded_accumulator)
                    logger.info(
                        "Worker %s: job %s handled directly by process()",
                        self.worker_id,
                        job.id,
                        extra={"duration_ms": int(elapsed * 1000), "phase": "job", "outcome": "handled_directly"},
                    )
                except Exception as exc:
                    elapsed = time.monotonic() - t0
                    ended_at_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                    error = traceback.format_exc()
                    # Decide retry vs terminal up front. ``prior_retry_count``
                    # is the count of attempts that had ALREADY failed before
                    # the current one — i.e. what we tag the just-failed
                    # attempt with in the StageErrors history. ``new_retry_count``
                    # is what the row's ``retry_count`` becomes after this
                    # attempt is recorded.
                    kind = classify(exc)
                    prior_retry_count = getattr(job, "retry_count", 0) or 0
                    new_retry_count = prior_retry_count + 1
                    will_retry = kind == "transient" and new_retry_count < max_retries()
                    next_attempt_at = compute_next_attempt(prior_retry_count) if will_retry else None
                    outcome = "requeued" if will_retry else "failed_terminal"
                    exc_type_str = f"{type(exc).__module__}.{type(exc).__name__}"
                    # Banner: WARNING for retry (the job will run again, so
                    # it's not a failure); ERROR for terminal so ops alerts
                    # still fire on real failures.
                    log_fn = logger.warning if will_retry else logger.error
                    log_fn(
                        "\n=================== WORKER FAILURE ===================\n"
                        "Worker:   %s\n"
                        "Job:      %s\n"
                        "Address:  %s\n"
                        "Name:     %s\n"
                        "Stage:    %s\n"
                        "Outcome:  %s (retry_count=%d)\n"
                        "-------------------------------------------------------\n"
                        "%s"
                        "=======================================================",
                        self.worker_id,
                        job.id,
                        getattr(job, "address", "?"),
                        getattr(job, "name", "?"),
                        self.stage.value,
                        outcome,
                        new_retry_count if will_retry else prior_retry_count,
                        error,
                        extra={
                            "duration_ms": int(elapsed * 1000),
                            "phase": "job",
                            "outcome": outcome,
                            "exc_type": exc_type_str,
                            "retry_count": new_retry_count if will_retry else prior_retry_count,
                            "next_attempt_at": next_attempt_at.isoformat() if next_attempt_at else None,
                            "failure_kind": kind,
                        },
                    )
                    # Append the job-failing exception alongside any degraded
                    # entries the run produced before the crash, tagged with
                    # the just-failed attempt's ``retry_count`` so the
                    # accumulator history reads chronologically (0, 1, 2, …).
                    # Persist via a fresh session inside ``_persist_stage_errors``
                    # so a poisoned primary transaction can't take the
                    # artifact down with it.
                    degraded_accumulator.append(
                        StageError(
                            stage=self.stage.value,
                            severity="error",
                            exc_type=exc_type_str,
                            message=str(exc),
                            traceback=error,
                            phase=None,
                            trace_id=getattr(job, "trace_id", None),
                            job_id=str(job.id),
                            worker_id=self.worker_id,
                            failed_at=datetime.now(timezone.utc),
                            retry_count=prior_retry_count,
                        )
                    )
                    self._persist_stage_errors(job, degraded_accumulator)
                    # On retries-exhaustion (kind=transient but no more
                    # retries left), the row records the full attempt count;
                    # on deterministic terminal (kind=terminal), retry_count
                    # stays at its current value because no retry slot was
                    # consumed by this attempt — we never even scheduled one.
                    terminal_retry_count = new_retry_count if kind == "transient" else None
                    try:
                        session.rollback()
                        if will_retry:
                            assert next_attempt_at is not None  # type-narrow for pyright
                            requeue_job(
                                session,
                                job.id,
                                error,
                                retry_count=new_retry_count,
                                next_attempt_at=next_attempt_at,
                            )
                        else:
                            fail_job_terminal(session, job.id, error, kind=kind, retry_count=terminal_retry_count)
                        self._record_stage_timing(
                            session,
                            job,
                            started_at=started_at_iso,
                            ended_at=ended_at_iso,
                            elapsed_s=elapsed,
                            status="failed",
                        )
                    except Exception:
                        logger.exception("Failed to update job %s after exception, retrying with fresh session", job.id)
                        try:
                            fresh = SessionLocal()
                            truncated = error[-4000:]
                            if will_retry:
                                assert next_attempt_at is not None
                                requeue_job(
                                    fresh,
                                    job.id,
                                    truncated,
                                    retry_count=new_retry_count,
                                    next_attempt_at=next_attempt_at,
                                )
                            else:
                                fail_job_terminal(fresh, job.id, truncated, kind=kind, retry_count=terminal_retry_count)
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
                            logger.exception(
                                "Could not update job %s for retry/terminal even with fresh session", job.id
                            )
            finally:
                degraded_errors_var.reset(accumulator_token)

    def _run_one_job(self, job_id) -> None:
        """K>1 dispatcher entry point: open a per-job session, re-fetch the job
        ORM object inside that session, run it through ``_execute_job``, close.

        The claim happened on a different (claim-loop) session that's already
        been closed; using ``session.get`` here re-binds the row to *this*
        thread's session so all heartbeats and DB writes belong to one
        identity map. Errors inside the job are absorbed by ``_execute_job``;
        anything that escapes is logged and dropped to keep the dispatcher
        loop alive.
        """
        session = SessionLocal()
        try:
            job = session.get(Job, job_id)
            if job is None:
                logger.warning("Worker %s: claimed job %s vanished before processing", self.worker_id, job_id)
                return
            self._execute_job(session, job)
        except Exception:
            logger.exception("Worker %s: unexpected error in dispatched job %s", self.worker_id, job_id)
        finally:
            session.close()

    def run_loop(self) -> None:
        logger.info(
            "Worker %s starting (stage=%s, job_concurrency=%d)",
            self.worker_id,
            self.stage.value,
            self._job_concurrency,
        )
        if self._job_concurrency > 1:
            self._run_loop_concurrent()
        else:
            self._run_loop_single()
        logger.info("Worker %s shut down", self.worker_id)

    def _run_loop_single(self) -> None:
        """Legacy K=1 loop: one in-flight job per worker process. Path is
        byte-identical to the pre-concurrency implementation."""
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

                self._execute_job(session, job)
            except Exception:
                logger.exception("Worker %s encountered error in main loop", self.worker_id)
            finally:
                session.close()

    def _run_loop_concurrent(self) -> None:
        """K>1 loop: claim jobs on a short-lived claim session and dispatch
        each into a per-worker ``ThreadPoolExecutor``. The pool is bounded
        at ``self._job_concurrency``; when full, the loop waits on
        ``FIRST_COMPLETED`` for back-pressure instead of polling.

        SIGTERM (``self._running == False``) stops new claims and waits up
        to ``STALE_JOB_TIMEOUT`` for in-flight jobs to drain before
        returning. Survivors are logged; the cross-worker stale-job sweep
        recovers them on a sibling worker.
        """
        assert self._job_pool is not None
        recovery_counter = 0
        while self._running:
            # Drain finished futures so the slot count is accurate.
            self._reap_finished_futures()

            if len(self._inflight) >= self._job_concurrency:
                # Pool is full: block until any future finishes (with a
                # ceiling so we still notice SIGTERM in time).
                wait(self._inflight, timeout=self.poll_interval, return_when=FIRST_COMPLETED)
                continue

            claim_session = SessionLocal()
            job_to_dispatch: Job | None = None
            job_id_for_dispatch = None
            try:
                recovery_counter += 1
                if recovery_counter >= 30:
                    recovery_counter = 0
                    self._recover_stale_jobs(claim_session)

                job_to_dispatch = self._claim_job(claim_session)
                if job_to_dispatch is not None:
                    # Capture the id before the session closes — the ORM
                    # object will be expired/detached on the dispatcher
                    # thread and we rebuild it via session.get there.
                    job_id_for_dispatch = job_to_dispatch.id
            except Exception:
                logger.exception("Worker %s encountered error in claim loop", self.worker_id)
            finally:
                claim_session.close()

            if job_id_for_dispatch is None:
                # Nothing to claim — sleep just enough to avoid hammering
                # Postgres while still letting in-flight futures progress.
                if self._inflight:
                    wait(self._inflight, timeout=self.poll_interval, return_when=FIRST_COMPLETED)
                else:
                    time.sleep(self.poll_interval)
                continue

            # ``ThreadPoolExecutor.submit`` does not propagate contextvars
            # by default; wrap with ``copy_context().run`` so the dispatched
            # job inherits the claim-loop's contextvar state. The per-job
            # ``bind_trace_context`` inside ``_execute_job`` then layers on
            # the job-specific bind once the row is loaded.
            ctx = contextvars.copy_context()
            future = self._job_pool.submit(ctx.run, self._run_one_job, job_id_for_dispatch)
            self._inflight.add(future)

        # Drain on shutdown so in-flight jobs land cleanly. Anything still
        # running past the timeout is logged; the cross-worker sweep
        # (``reclaim_stuck_jobs``) recovers them after STALE_JOB_TIMEOUT.
        if self._inflight:
            logger.info(
                "Worker %s draining %d in-flight job(s) (timeout=%ds)",
                self.worker_id,
                len(self._inflight),
                STALE_JOB_TIMEOUT,
            )
            done, not_done = wait(self._inflight, timeout=STALE_JOB_TIMEOUT)
            if not_done:
                logger.warning(
                    "Worker %s: %d job(s) still running at shutdown; "
                    "abandoning so cross-worker stale sweep can recover",
                    self.worker_id,
                    len(not_done),
                )
            self._inflight.clear()
        if self._job_pool is not None:
            self._job_pool.shutdown(wait=False)

    def _reap_finished_futures(self) -> None:
        """Drop completed futures from ``self._inflight`` to free dispatch slots."""
        finished = {f for f in self._inflight if f.done()}
        if finished:
            self._inflight -= finished

    def update_detail(self, session: Session, job: Job, detail: str) -> None:
        """Update the job's progress detail message."""
        update_job_detail(session, job.id, detail)

    def _heartbeat(self, session: Session, job: Job) -> None:
        """Bump ``Job.updated_at`` without changing any other state.

        Used inside long parallel sections so the stale-job sweep doesn't
        requeue live work. Issues a stand-alone UPDATE rather than touching
        ``job.detail`` so concurrent ``update_detail`` writes from the same
        worker don't fight over the message.
        """
        from sqlalchemy import update as sa_update

        try:
            session.execute(sa_update(Job).where(Job.id == job.id).values(updated_at=datetime.now(timezone.utc)))
            session.commit()
        except Exception:
            # Best-effort: a heartbeat failure is never fatal — worst case we
            # eat a redundant requeue on the next sweep, which the idempotent
            # claim path tolerates.
            try:
                session.rollback()
            except Exception:
                logger.debug("heartbeat rollback failed", exc_info=True)

    def _persist_stage_errors(self, job: Job, errors: list[StageError]) -> None:
        """Write the ``stage_errors`` artifact via a fresh session, merging
        with any pre-existing artifact so retries accumulate per-attempt.

        Used on both the success and failure paths so the artifact survives a
        broken primary transaction. ``store_artifact`` does its own commit, so
        the only state to clean up is the fresh session itself. Best-effort —
        we never want a stage_errors write failure to mask the underlying job
        failure or block the success advance.

        Accumulation matters for retries: a job that fails transiently three
        times before succeeding ends up with three error entries plus any
        degraded entries from each attempt. Without the merge step every
        retry would overwrite the prior attempt's history.
        """
        if not errors:
            return
        fresh = SessionLocal()
        try:
            existing = get_artifact(fresh, job.id, "stage_errors")
            merged: list[StageError] = []
            if isinstance(existing, dict):
                try:
                    merged = list(StageErrors.model_validate(existing).errors)
                except Exception:
                    # Corrupt body shouldn't block the new write — best-effort
                    # replace it. The operator can correlate the lost entries
                    # via the trace_id in logs if it ever matters.
                    merged = []
            merged.extend(errors)
            store_artifact(
                fresh,
                job.id,
                "stage_errors",
                data=StageErrors(errors=merged).model_dump(mode="json"),
            )
        except Exception:
            logger.exception(
                "Worker %s: failed to write stage_errors artifact for job %s (non-fatal)",
                self.worker_id,
                job.id,
            )
        finally:
            try:
                fresh.close()
            except Exception:
                logger.debug("stage_errors session close failed", exc_info=True)

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
