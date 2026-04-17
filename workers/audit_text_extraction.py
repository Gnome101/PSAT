"""Worker that downloads audit PDFs, extracts text, and stores the result.

Scales horizontally via ``FOR UPDATE SKIP LOCKED`` on the claim query — run as
many processes as you like and none of them will double-process the same row.
Within a single process, a thread pool drives concurrent HTTP + parse work
(PDF extraction is I/O- and CPU-bound but pypdf releases the GIL often
enough that threads are measurably faster than serial).

Per-host throttling keeps any one audit firm's CDN from getting hammered by
the thread pool (Spearbit / Cantina portfolio pages rate-limit at a few
concurrent requests).

State model on ``audit_reports``:

    NULL            — never attempted. Eligible for claim.
    "processing"    — a worker holds this row. Gets recovered if stale.
    "success"       — text is in object storage at ``text_storage_key``.
    "failed"        — terminal failure; ``text_extraction_error`` has details.
                      (Manual DB op can reset to NULL to retry.)
    "skipped"       — image-only PDF, >50MB, or otherwise not extractable.
"""

from __future__ import annotations

import logging
import os
import signal
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import requests
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from db.models import AuditReport, SessionLocal
from services.audits import ExtractionOutcome, process_audit_report

logger = logging.getLogger("workers.audit_text_extraction")


# --- Tunables (env-overridable for ops) ----------------------------------

# Rows claimed per poll. The thread pool will process all of them in
# parallel, so keep this in proportion to MAX_CONCURRENT.
_BATCH_SIZE = int(os.getenv("PSAT_AUDIT_TEXT_BATCH_SIZE", "8"))

# Thread-pool size. pypdf parse is GIL-bounded, but downloads dominate —
# 8 threads cover ~4 concurrent downloads on average (rest are waiting on
# DB / storage I/O).
_MAX_CONCURRENT = int(os.getenv("PSAT_AUDIT_TEXT_CONCURRENCY", "8"))

# How often we poll when there's no work to do.
_IDLE_POLL_INTERVAL = float(os.getenv("PSAT_AUDIT_TEXT_POLL_INTERVAL", "10.0"))

# Max in-flight requests per host. Most auditor portfolios rate-limit
# aggressively; GitHub's raw CDN tolerates far more but 3 is plenty for the
# volume we're doing.
_PER_HOST_CONCURRENCY = int(os.getenv("PSAT_AUDIT_TEXT_HOST_CONCURRENCY", "3"))

# A row in "processing" longer than this is considered abandoned by a dead
# worker and gets reset to NULL on the next recovery pass.
_STALE_PROCESSING_SECONDS = int(os.getenv("PSAT_AUDIT_TEXT_STALE_TIMEOUT", "600"))

# How many poll cycles between stale-row recovery passes.
_STALE_RECOVERY_EVERY_N_POLLS = 20


# --- Worker --------------------------------------------------------------


class AuditTextExtractionWorker:
    """Long-running process that drains the ``audit_reports`` text-extraction queue."""

    def __init__(self) -> None:
        self.worker_id = f"AuditTextExtraction-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._running = True
        self._host_semaphores: dict[str, threading.Semaphore] = {}
        self._host_semaphores_lock = threading.Lock()
        # One shared session so keep-alive connections pay off across requests.
        self._http_session = requests.Session()

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: object) -> None:
        logger.info("Worker %s received signal %s, shutting down", self.worker_id, signum)
        self._running = False

    # --- Claim ----------------------------------------------------------

    def _claim_batch(self, session: Session) -> list[AuditReport]:
        """Atomically claim up to _BATCH_SIZE pending rows.

        Uses ``SELECT ... FOR UPDATE SKIP LOCKED`` so multiple workers can
        run safely: each one gets a distinct slice, and locked rows are
        simply passed over rather than blocking.
        """
        stmt = (
            select(AuditReport)
            .where(AuditReport.text_extraction_status.is_(None))
            .order_by(AuditReport.discovered_at.desc().nullslast(), AuditReport.id.asc())
            .limit(_BATCH_SIZE)
            .with_for_update(skip_locked=True)
        )
        rows = session.execute(stmt).scalars().all()
        if not rows:
            return []

        now = datetime.now(timezone.utc)
        for row in rows:
            row.text_extraction_status = "processing"
            row.text_extraction_worker = self.worker_id
            row.text_extraction_started_at = now
            row.text_extraction_error = None
        session.commit()
        # Detach so we can use the row data after closing this session —
        # each worker thread will reopen its own session for the update.
        for row in rows:
            session.expunge(row)
        return rows

    # --- Recover --------------------------------------------------------

    def _recover_stale_rows(self, session: Session) -> None:
        """Reset rows stuck in "processing" past the stale timeout.

        These are rows claimed by a worker that crashed or was hard-killed
        before it could persist a final status. Setting status back to NULL
        lets the regular claim query pick them up again.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=_STALE_PROCESSING_SECONDS)
        result = session.execute(
            update(AuditReport)
            .where(
                AuditReport.text_extraction_status == "processing",
                AuditReport.text_extraction_started_at < cutoff,
            )
            .values(
                text_extraction_status=None,
                text_extraction_worker=None,
                text_extraction_started_at=None,
            )
            .returning(AuditReport.id)
        )
        ids = [row.id for row in result]
        if ids:
            logger.warning(
                "Worker %s: reset %d stale row(s) back to pending: %s",
                self.worker_id,
                len(ids),
                ids,
            )
            session.commit()
        else:
            session.rollback()

    # --- Rate limiting --------------------------------------------------

    def _host_semaphore(self, url: str) -> threading.Semaphore:
        """Return (and lazily create) the semaphore gating concurrent calls
        to the URL's host. One semaphore per unique netloc."""
        host = urlparse(url).netloc.lower() or "_unknown"
        with self._host_semaphores_lock:
            sem = self._host_semaphores.get(host)
            if sem is None:
                sem = threading.Semaphore(_PER_HOST_CONCURRENCY)
                self._host_semaphores[host] = sem
        return sem

    # --- Per-row processing --------------------------------------------

    def _process_row(self, audit: AuditReport) -> tuple[int, ExtractionOutcome]:
        """Run the extraction pipeline for a single claimed row.

        Returns the row id + outcome. Blocks on the per-host semaphore
        during the HTTP fetch; never raises.
        """
        url = audit.pdf_url or audit.url
        if not url:
            return audit.id, ExtractionOutcome(
                status="failed", error="no URL on audit row"
            )

        host_sem = self._host_semaphore(url)
        with host_sem:
            outcome = process_audit_report(
                audit_report_id=audit.id,
                url=url,
                session=self._http_session,
            )
        return audit.id, outcome

    # --- Update ---------------------------------------------------------

    def _persist_outcome(self, audit_id: int, outcome: ExtractionOutcome) -> None:
        """Write the extraction outcome back to the row in its own session."""
        now = datetime.now(timezone.utc)
        session = SessionLocal()
        try:
            audit = session.get(AuditReport, audit_id)
            if audit is None:
                logger.warning("Audit %s disappeared before outcome could be saved", audit_id)
                return
            audit.text_extraction_status = outcome.status
            audit.text_extraction_error = outcome.error
            audit.text_extraction_worker = None
            if outcome.status == "success":
                audit.text_storage_key = outcome.storage_key
                audit.text_size_bytes = outcome.text_size_bytes
                audit.text_sha256 = outcome.text_sha256
                audit.text_extracted_at = now
            session.commit()
        except Exception:
            session.rollback()
            logger.exception("Failed to persist outcome for audit %s", audit_id)
        finally:
            session.close()

    # --- Main loop ------------------------------------------------------

    def run_loop(self) -> None:
        logger.info(
            "AuditTextExtraction worker %s starting "
            "(batch=%d, pool=%d, per-host=%d, idle=%ss, stale=%ss)",
            self.worker_id,
            _BATCH_SIZE,
            _MAX_CONCURRENT,
            _PER_HOST_CONCURRENCY,
            _IDLE_POLL_INTERVAL,
            _STALE_PROCESSING_SECONDS,
        )

        # Dedicated executor per process; torn down on shutdown.
        executor = ThreadPoolExecutor(
            max_workers=_MAX_CONCURRENT,
            thread_name_prefix="audit-text",
        )

        poll_counter = 0
        try:
            while self._running:
                poll_counter += 1

                session = SessionLocal()
                try:
                    if poll_counter % _STALE_RECOVERY_EVERY_N_POLLS == 0:
                        self._recover_stale_rows(session)

                    claimed = self._claim_batch(session)
                finally:
                    session.close()

                if not claimed:
                    time.sleep(_IDLE_POLL_INTERVAL)
                    continue

                logger.info(
                    "Worker %s claimed %d audit(s) for text extraction",
                    self.worker_id,
                    len(claimed),
                )

                futures = {executor.submit(self._process_row, row): row.id for row in claimed}
                for future in as_completed(futures):
                    try:
                        audit_id, outcome = future.result()
                    except Exception:
                        # Should not happen — _process_row catches everything.
                        # Log and move on so we don't leak "processing" rows.
                        logger.exception("Unexpected error in audit text thread")
                        continue
                    self._persist_outcome(audit_id, outcome)
                    logger.info(
                        "Audit %s → %s%s",
                        audit_id,
                        outcome.status,
                        f" ({outcome.error})" if outcome.error else "",
                    )
        finally:
            executor.shutdown(wait=True)
            logger.info("Worker %s shut down", self.worker_id)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        force=True,
    )
    AuditTextExtractionWorker().run_loop()


if __name__ == "__main__":
    main()
