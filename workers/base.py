"""Base worker loop with graceful SIGTERM handling."""

from __future__ import annotations

import logging
import os
import signal
import time
import traceback
import uuid

from sqlalchemy.orm import Session

from db.models import Job, JobStage, SessionLocal
from db.queue import advance_job, claim_job, fail_job, update_job_detail

logger = logging.getLogger(__name__)


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
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    def _handle_sigterm(self, signum: int, frame: object) -> None:
        logger.info("Worker %s received signal %s, shutting down gracefully", self.worker_id, signum)
        self._running = False

    def process(self, session: Session, job: Job) -> None:
        """Subclasses implement this to run their pipeline stage."""
        raise NotImplementedError

    def run_loop(self) -> None:
        logger.info("Worker %s starting (stage=%s)", self.worker_id, self.stage.value)
        while self._running:
            session = SessionLocal()
            try:
                job = claim_job(session, self.stage, self.worker_id)
                if job is None:
                    session.close()
                    time.sleep(self.poll_interval)
                    continue

                logger.info("Worker %s claimed job %s", self.worker_id, job.id)
                try:
                    self.process(session, job)
                    if self.next_stage == JobStage.done:
                        from db.queue import complete_job

                        complete_job(session, job.id)
                    else:
                        advance_job(session, job.id, self.next_stage, f"Completed {self.stage.value}")
                    logger.info("Worker %s completed job %s", self.worker_id, job.id)
                except JobHandledDirectly:
                    logger.info("Worker %s: job %s handled directly by process()", self.worker_id, job.id)
                except Exception:
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
                        fail_job(session, job.id, error)
                    except Exception:
                        logger.exception("Failed to mark job %s as failed", job.id)
            except Exception:
                logger.exception("Worker %s encountered error in main loop", self.worker_id)
            finally:
                session.close()

        logger.info("Worker %s shut down", self.worker_id)

    def update_detail(self, session: Session, job: Job, detail: str) -> None:
        """Update the job's progress detail message."""
        update_job_detail(session, job.id, detail)
