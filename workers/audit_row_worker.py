"""Base class for workers that drain a column-state-machine on ``audit_reports``.

The text-extraction and scope-extraction workers share ~80% of their
scaffolding — signal handling, batch claim via ``SELECT ... FOR UPDATE
SKIP LOCKED``, stale-row recovery, the outer run loop with its thread
pool, and the "open your own session to persist" pattern. Before this
base class, that scaffolding lived twice with subtle drift risk: a bug
fix to stale recovery had to land in two places, and adding a third
row-worker meant copy-pasting ~100 LOC of boilerplate.

Subclasses declare only what actually differs:

  - ``_pending_rows_query``       which rows are eligible for this phase
  - ``_mark_processing``          how to flip a row to "processing"
  - ``_stale_recovery_query``     how to reset a stuck row back to pending
  - ``_process_row``              the phase's real work (runs on a thread)
  - ``_persist_outcome``          how to write the result back (own session)
  - ``_log_outcome``              optional per-row log line (default: minimal)

Tunables (batch size, concurrency, poll interval, stale timeout) are
class attributes so subclasses can override at class-definition time
from their own env-driven defaults. Nothing in the base class reads
env directly — scheduling decisions stay owned by the subclass.

This is NOT a ``workers.base.BaseWorker`` subclass. BaseWorker drives
the ``jobs`` queue via ``db.queue.claim_job``; this one drives the
``audit_reports`` queue via its own column state machine. Same shape,
different table, deliberately kept separate so the two queues can't
accidentally become each other's abstractions.
"""

from __future__ import annotations

import logging
import os
import signal
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.orm import Session
from sqlalchemy.sql import Select, Update

from db.models import AuditReport, SessionLocal

logger = logging.getLogger("workers.audit_row_worker")


class AuditRowWorker:
    """Poll-based worker drain for one phase of the ``audit_reports`` state machine."""

    # -- Subclass customization (class attributes) -------------------------

    # Prefix for worker_id and the starting log line. Override per-phase.
    worker_name: str = "AuditRow"

    # Per-tick rows to claim; thread pool size; idle sleep when no work.
    batch_size: int = 4
    max_concurrent: int = 4
    idle_poll_interval: float = 10.0

    # Rows stuck in "processing" past this many seconds are reset.
    # Per poll cycles between recovery passes: higher = less DB traffic.
    stale_processing_seconds: int = 600
    stale_recovery_every_n_polls: int = 20

    thread_name_prefix: str = "audit-row"

    # Subclass should assign its own module logger so log lines carry the
    # right source location; defaults to this module's logger.
    log: logging.Logger = logger

    # -- Init / signals ---------------------------------------------------

    def __init__(self) -> None:
        self.worker_id = f"{self.worker_name}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._running = True
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: object) -> None:
        self.log.info(
            "Worker %s received signal %s, shutting down",
            self.worker_id,
            signum,
        )
        self._running = False

    # -- Abstract hooks ---------------------------------------------------

    def _pending_rows_query(self) -> Select:
        """Return the eligibility SELECT (with LIMIT + FOR UPDATE SKIP LOCKED).

        Must SELECT ``AuditReport`` rows whose state column(s) match the
        phase's "ready to claim" condition. The base's ``_claim_batch``
        executes this and flips each returned row to processing via
        ``_mark_processing``.
        """
        raise NotImplementedError

    def _mark_processing(self, row: AuditReport, now: datetime) -> None:
        """Mutate ``row`` so its status is 'processing' with this worker's id.

        Called on every row returned from ``_pending_rows_query`` inside
        the claim transaction. Subclass writes to its own status /
        worker / started_at / error columns; the base commits.
        """
        raise NotImplementedError

    def _stale_recovery_query(self, cutoff: datetime) -> Update:
        """Return the UPDATE ... RETURNING id query that resets stale rows.

        Matches rows in the 'processing' state for this phase whose
        ``started_at`` is older than ``cutoff``, zeroes the status/
        worker/started_at columns, and RETURNS the ids so the base can
        log what it unstuck.
        """
        raise NotImplementedError

    def _process_row(self, audit: AuditReport) -> tuple[int, Any]:
        """Perform the phase's real work for one claimed row.

        Runs on a worker thread. Must NEVER raise — wrap any exceptions
        internally and return a failure-shaped outcome. The return value
        is handed verbatim to ``_persist_outcome`` (and ``_log_outcome``).
        The specific outcome type is phase-local.
        """
        raise NotImplementedError

    def _persist_outcome(self, audit_id: int, result: Any) -> None:
        """Write ``result`` back to the row. Uses its OWN session.

        Each row gets a fresh session so one row's failure never poisons
        another's commit. Subclass handles all row-state updates here,
        including any downstream refreshes (e.g. coverage) that should
        land in the same transaction as the state flip.
        """
        raise NotImplementedError

    def _log_outcome(self, audit_id: int, result: Any) -> None:
        """Default per-row log — one line with status and optional error.

        Subclasses override when the outcome type carries extra fields
        worth surfacing at INFO level (e.g. ``method``, ``contracts``
        count). The run loop logs AFTER persist so the log line reflects
        the write that actually happened.
        """
        status = getattr(result, "status", "?")
        error = getattr(result, "error", None)
        self.log.info(
            "Audit %s → %s%s",
            audit_id,
            status,
            f" ({error})" if error else "",
        )

    # -- Claim + stale recovery (shared) ----------------------------------

    def _claim_batch(self, session: Session) -> list[AuditReport]:
        """Atomically claim up to ``batch_size`` pending rows.

        SKIP LOCKED lets multiple workers run without coordinating:
        each one gets a distinct slice, locked rows pass silently, and
        no row is ever claimed twice. Rows are expunged so worker
        threads can read the fields off them without the session
        following the thread.
        """
        rows = list(session.execute(self._pending_rows_query()).scalars().all())
        if not rows:
            return []

        now = datetime.now(timezone.utc)
        for row in rows:
            self._mark_processing(row, now)
        session.commit()
        for row in rows:
            session.expunge(row)
        return rows

    def _recover_stale_rows(self, session: Session) -> None:
        """Reset rows stuck in 'processing' past ``stale_processing_seconds``.

        The prior claimer's process is assumed dead (crashed, hard-killed,
        or just lost connectivity past the timeout). Flipping their
        status back to pending lets the next claim pass re-run them.
        Rolls back when there's nothing to do so we don't leave an
        empty transaction open against Postgres.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.stale_processing_seconds)
        result = session.execute(self._stale_recovery_query(cutoff))
        ids = [row.id for row in result]
        if ids:
            self.log.warning(
                "Worker %s: reset %d stale row(s) back to pending: %s",
                self.worker_id,
                len(ids),
                ids,
            )
            session.commit()
        else:
            session.rollback()

    # -- Main loop --------------------------------------------------------

    def run_loop(self) -> None:
        """Poll → claim → dispatch → persist → log, with periodic stale recovery.

        A single thread pool lives for the worker's lifetime. Inside one
        tick we claim a batch, fan it out to the pool, and serialize
        persist calls in whatever order the futures complete — each
        persist opens its own DB session so ordering is safe. Sleeps
        ``idle_poll_interval`` seconds when there's no work.
        """
        self.log.info(
            "%s worker %s starting (batch=%d, pool=%d, idle=%ss, stale=%ss)",
            self.worker_name,
            self.worker_id,
            self.batch_size,
            self.max_concurrent,
            self.idle_poll_interval,
            self.stale_processing_seconds,
        )

        executor = ThreadPoolExecutor(
            max_workers=self.max_concurrent,
            thread_name_prefix=self.thread_name_prefix,
        )

        poll_counter = 0
        try:
            while self._running:
                poll_counter += 1

                session = SessionLocal()
                try:
                    if poll_counter % self.stale_recovery_every_n_polls == 0:
                        self._recover_stale_rows(session)
                    claimed = self._claim_batch(session)
                finally:
                    session.close()

                if not claimed:
                    time.sleep(self.idle_poll_interval)
                    continue

                self.log.info(
                    "Worker %s claimed %d audit(s)",
                    self.worker_id,
                    len(claimed),
                )

                futures = {executor.submit(self._process_row, row): row.id for row in claimed}
                for future in as_completed(futures):
                    try:
                        audit_id, result = future.result()
                    except Exception:
                        # _process_row contract says "never raise"; if
                        # one does, log and move on rather than leaking
                        # a 'processing' row until stale recovery runs.
                        self.log.exception("Unexpected error in %s thread", self.worker_name)
                        continue
                    self._persist_outcome(audit_id, result)
                    self._log_outcome(audit_id, result)
        finally:
            executor.shutdown(wait=True)
            self.log.info("Worker %s shut down", self.worker_id)
