"""Coverage worker — end-of-pipeline source-equivalence-aware coverage refresh.

Runs after ``PolicyWorker`` for every analyzed contract. Links each Contract
to its protocol's audits (via ``services.audits.coverage``) with the
expensive ``verify_source_equivalence=True`` pass enabled, so proof-grade
``reviewed_commit`` matches land automatically instead of requiring an
admin ``refresh_coverage`` call.

Two races are solved by the readiness predicate in ``_claim_next_job``:

    Timeline A: address pipeline (discovery → static → resolution → policy
                → coverage → done)
    Timeline B: audit pipeline (text_extraction → scope_extraction)

Coverage for a protocol's contracts must wait until every audit in that
protocol has either succeeded, failed, or been explicitly skipped. A
claim fires only when NO audit in the protocol is mid-flight. An audit
with ``text_extraction_status=NULL`` (never attempted) or ``'processing'``
counts as mid-flight; text-extraction failures (status='failed') don't
block because ``scope_extraction_status`` stays NULL forever for those
rows, which the predicate below explicitly handles by only blocking on
scope when text extraction ``succeeded``.

Stuck-audit escape hatch: once a job has sat at ``stage=coverage,
status=queued`` for longer than ``_STUCK_COVERAGE_TIMEOUT`` (default 1h),
we claim it anyway and log a warning. Better to produce coverage (even
just temporal) than to leave the job hanging forever because one audit's
PDF extraction wedged.

Jobs with ``protocol_id=NULL`` (direct address submissions without a
parent company) bypass the readiness wait naturally — ``NULL = NULL``
evaluates to UNKNOWN, so the NOT EXISTS subquery returns true and claim
succeeds immediately.
"""

from __future__ import annotations

import logging
import os
import time
import traceback

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from db.models import Contract, Job, JobStage, JobStatus, SessionLocal
from db.queue import advance_job, complete_job, fail_job
from workers.base import BaseWorker, JobHandledDirectly

logger = logging.getLogger("workers.coverage_worker")

# How long a coverage job can sit in 'queued' before we bypass the
# readiness predicate and run it anyway. An hour is long enough for a
# stuck audit PDF extraction to unstick on its own (or be manually
# reset) without leaving analysis users waiting indefinitely.
_STUCK_COVERAGE_TIMEOUT = int(os.getenv("PSAT_COVERAGE_STUCK_TIMEOUT", "3600"))


class CoverageWorker(BaseWorker):
    """Drains the ``coverage`` stage with a custom readiness-gated claim."""

    stage = JobStage.coverage
    next_stage = JobStage.done
    poll_interval = 5.0

    def _claim_next_job(self, session: Session) -> Job | None:
        """Claim a coverage job whose protocol's audit side has settled.

        Readiness predicate: NO audit in the same protocol is still
        moving through the text → scope pipeline. An audit whose text
        extraction failed (status='failed') leaves scope_extraction_status
        NULL forever — that's "settled" for our purposes, not "blocked",
        which is why the inner AND only guards on scope when text
        extraction ``succeeded``.
        """
        claim_id = session.execute(
            text(
                """
                SELECT j.id
                FROM jobs j
                WHERE j.stage = 'coverage' AND j.status = 'queued'
                  AND NOT EXISTS (
                    SELECT 1 FROM audit_reports ar
                    WHERE ar.protocol_id = j.protocol_id
                      AND (
                        ar.text_extraction_status IS NULL
                        OR ar.text_extraction_status = 'processing'
                        OR (ar.text_extraction_status = 'success'
                            AND (ar.scope_extraction_status IS NULL
                                 OR ar.scope_extraction_status = 'processing'))
                      )
                  )
                ORDER BY j.updated_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """
            )
        ).scalar_one_or_none()
        if claim_id is None:
            return None
        job = session.get(Job, claim_id)
        if job is None:
            return None
        job.status = JobStatus.processing
        job.worker_id = self.worker_id
        session.commit()
        session.refresh(job)
        return job

    def _claim_stuck_job(self, session: Session) -> Job | None:
        """Bypass readiness and claim a job that's been queued too long.

        The audit pipeline may be permanently wedged on one bad PDF;
        don't punish every contract in the protocol for it. Logs a
        warning so the wedge is visible in operational dashboards.
        """
        claim_id = session.execute(
            text(
                """
                SELECT j.id
                FROM jobs j
                WHERE j.stage = 'coverage' AND j.status = 'queued'
                  AND j.updated_at < (NOW() - (:timeout * INTERVAL '1 second'))
                ORDER BY j.updated_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """
            ),
            {"timeout": _STUCK_COVERAGE_TIMEOUT},
        ).scalar_one_or_none()
        if claim_id is None:
            return None
        job = session.get(Job, claim_id)
        if job is None:
            return None
        logger.warning(
            "Worker %s: claiming stuck coverage job %s (address=%s) past %ss timeout — "
            "protocol %s has unresolved audit(s)",
            self.worker_id,
            job.id,
            job.address or "?",
            _STUCK_COVERAGE_TIMEOUT,
            job.protocol_id,
        )
        job.status = JobStatus.processing
        job.worker_id = self.worker_id
        session.commit()
        session.refresh(job)
        return job

    def process(self, session: Session, job: Job) -> None:
        """Refresh coverage for this job's Contract with source-equivalence on.

        Finds the Contract via the job_id link the discovery worker set,
        then delegates to ``upsert_coverage_for_contract``. The match
        side is symmetric to the audit-side refresh triggered from the
        scope worker — we fetch all audits whose scope mentions the
        contract name and write one coverage row per match.
        """
        from services.audits.coverage import upsert_coverage_for_contract

        contract = session.execute(select(Contract).where(Contract.job_id == job.id).limit(1)).scalar_one_or_none()
        if contract is None:
            # Address-only jobs where discovery/static skipped the Contract
            # write (cached path reassigned it) can land here. Nothing to
            # refresh; let the next-stage advance carry the job to done.
            logger.info(
                "Coverage stage: job %s has no Contract row — skipping refresh, advancing to done",
                job.id,
            )
            return

        self.update_detail(session, job, "Refreshing audit coverage")
        inserted = upsert_coverage_for_contract(
            session,
            contract.id,
            verify_source_equivalence=True,
        )
        session.commit()
        logger.info(
            "Coverage stage complete for job %s (contract %s): %d coverage row(s)",
            job.id,
            contract.id,
            inserted,
        )

    # -- Loop --------------------------------------------------------------

    def run_loop(self) -> None:
        """Custom run loop — readiness-gated claim + stuck-job escape hatch.

        Mirrors ``BaseWorker.run_loop`` structure (stale recovery, error
        isolation, session-per-tick) but swaps the claim step for our
        two-phase claim. Order: readiness-gated first, then stuck-job
        fallback, so a normal-path claim always wins when available.
        """
        logger.info("Worker %s starting (stage=%s)", self.worker_id, self.stage.value)
        recovery_counter = 0
        while self._running:
            session = SessionLocal()
            try:
                recovery_counter += 1
                if recovery_counter >= 30:
                    recovery_counter = 0
                    self._recover_stale_jobs(session)

                job = self._claim_next_job(session) or self._claim_stuck_job(session)
                if job is None:
                    session.close()
                    time.sleep(self.poll_interval)
                    continue

                logger.info("Worker %s claimed job %s", self.worker_id, job.id)
                t0 = time.monotonic()
                try:
                    self.process(session, job)
                    if self.next_stage == JobStage.done:
                        complete_job(session, job.id)
                    else:
                        advance_job(session, job.id, self.next_stage, f"Completed {self.stage.value}")
                    logger.info(
                        "Worker %s completed job %s in %.1fs",
                        self.worker_id,
                        job.id,
                        time.monotonic() - t0,
                    )
                except JobHandledDirectly:
                    logger.info("Worker %s: job %s handled directly by process()", self.worker_id, job.id)
                except Exception:
                    error = traceback.format_exc()
                    logger.error(
                        "Coverage worker failed on job %s (address=%s):\n%s",
                        job.id,
                        getattr(job, "address", "?"),
                        error,
                    )
                    try:
                        session.rollback()
                        fail_job(session, job.id, error)
                    except Exception:
                        logger.exception("Failed to mark job %s as failed, retrying with fresh session", job.id)
                        try:
                            fresh = SessionLocal()
                            fail_job(fresh, job.id, error[-4000:])
                            fresh.close()
                        except Exception:
                            logger.exception("Could not mark job %s as failed even with fresh session", job.id)
            except Exception:
                logger.exception("Worker %s encountered error in main loop", self.worker_id)
            finally:
                session.close()

        logger.info("Worker %s shut down", self.worker_id)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        force=True,
    )
    CoverageWorker().run_loop()


if __name__ == "__main__":
    main()
