"""Worker that extracts the list of in-scope contracts from audit PDFs.

Runs after ``workers.audit_text_extraction`` has stored the extracted PDF
text in object storage. Eligible rows satisfy
``text_extraction_status='success' AND scope_extraction_status IS NULL``.

Scales horizontally via ``FOR UPDATE SKIP LOCKED`` like its text-extraction
sibling — many processes safe, each thread runs one LLM call at a time.

State model on ``audit_reports``:

    scope_extraction_status:
        NULL          — eligible for claim
        "processing"  — held by a worker; stale-recovered after 15 min
        "success"     — scope_contracts[] + scope_storage_key populated
        "failed"      — storage / decode error; scope_extraction_error has details
        "skipped"     — no scope-section header found, or validation emptied the list

Content-hash cache: before calling the LLM we look up any sibling
AuditReport with the same ``text_sha256`` that has already been scoped —
clone its ``scope_contracts`` and ``scope_storage_key`` instead of paying
for the LLM call again. Covers the common "Solodit copy + GitHub copy of
the same PDF" case at zero cost.
"""

from __future__ import annotations

import logging
import os
import signal
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text, update
from sqlalchemy.orm import Session

from db.models import AuditReport, SessionLocal
from services.audits import ScopeExtractionOutcome, process_audit_scope

logger = logging.getLogger("workers.audit_scope_extraction")


# --- Tunables (env-overridable) ------------------------------------------

# LLM calls dominate latency; keep batches small so individual workers
# don't sit on many rows when the pool is slow.
_BATCH_SIZE = int(os.getenv("PSAT_AUDIT_SCOPE_BATCH_SIZE", "4"))

_MAX_CONCURRENT = int(os.getenv("PSAT_AUDIT_SCOPE_CONCURRENCY", "4"))

_IDLE_POLL_INTERVAL = float(os.getenv("PSAT_AUDIT_SCOPE_POLL_INTERVAL", "15.0"))

# Generous — an LLM call can take 60s+ on a slow day, and the worker
# reads a large-ish object from storage before calling. 15 min leaves
# margin for retries inside one process.
_STALE_PROCESSING_SECONDS = int(os.getenv("PSAT_AUDIT_SCOPE_STALE_TIMEOUT", "900"))

_STALE_RECOVERY_EVERY_N_POLLS = 20


# --- Cache-copy sentinel --------------------------------------------------


class _CacheCopyOutcome:
    """Lightweight sentinel returned by ``_process_row`` when the content-hash
    cache hits. Carries just enough state for ``_persist_outcome`` to clone
    the sibling row without re-running extraction.
    """

    __slots__ = ("sibling_id",)

    def __init__(self, sibling_id: int) -> None:
        self.sibling_id = sibling_id


_ProcessResult = ScopeExtractionOutcome | _CacheCopyOutcome


# --- Worker --------------------------------------------------------------


class AuditScopeExtractionWorker:
    """Drains the ``audit_reports`` scope-extraction queue."""

    def __init__(self) -> None:
        self.worker_id = f"AuditScopeExtraction-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._running = True
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: object) -> None:
        logger.info(
            "Worker %s received signal %s, shutting down",
            self.worker_id,
            signum,
        )
        self._running = False

    # --- Claim ----------------------------------------------------------

    def _claim_batch(self, session: Session) -> list[AuditReport]:
        """Atomically claim up to ``_BATCH_SIZE`` pending rows.

        Eligibility: text extraction has already succeeded AND scope
        extraction hasn't been attempted. Newest-first ordering so a
        freshly-discovered audit isn't blocked behind a big backlog.
        """
        stmt = (
            select(AuditReport)
            .where(
                AuditReport.text_extraction_status == "success",
                AuditReport.scope_extraction_status.is_(None),
            )
            .order_by(
                AuditReport.text_extracted_at.desc().nullslast(),
                AuditReport.id.asc(),
            )
            .limit(_BATCH_SIZE)
            .with_for_update(skip_locked=True)
        )
        rows = list(session.execute(stmt).scalars().all())
        if not rows:
            return []

        now = datetime.now(timezone.utc)
        for row in rows:
            row.scope_extraction_status = "processing"
            row.scope_extraction_worker = self.worker_id
            row.scope_extraction_started_at = now
            row.scope_extraction_error = None
        session.commit()
        for row in rows:
            session.expunge(row)
        return rows

    # --- Recover --------------------------------------------------------

    def _recover_stale_rows(self, session: Session) -> None:
        """Reset rows stuck in ``processing`` past the stale timeout."""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=_STALE_PROCESSING_SECONDS)
        result = session.execute(
            update(AuditReport)
            .where(
                AuditReport.scope_extraction_status == "processing",
                AuditReport.scope_extraction_started_at < cutoff,
            )
            .values(
                scope_extraction_status=None,
                scope_extraction_worker=None,
                scope_extraction_started_at=None,
            )
            .returning(AuditReport.id)
        )
        ids = [row.id for row in result]
        if ids:
            logger.warning(
                "Worker %s: reset %d stale scope row(s) back to pending: %s",
                self.worker_id,
                len(ids),
                ids,
            )
            session.commit()
        else:
            session.rollback()

    # --- Cache lookup ---------------------------------------------------

    def _find_cache_sibling(self, session: Session, audit_id: int, text_sha256: str | None) -> int | None:
        """Return the id of an already-scoped audit with matching text_sha256.

        Returns None when no match (forcing a fresh LLM call) or when
        ``text_sha256`` is None (pre-extraction rows don't have a hash).
        """
        if not text_sha256:
            return None
        row = session.execute(
            text(
                "SELECT id FROM audit_reports "
                "WHERE text_sha256 = :sha AND id != :self_id "
                "AND scope_extraction_status = 'success' "
                "AND scope_contracts IS NOT NULL "
                "ORDER BY scope_extracted_at DESC NULLS LAST, id ASC "
                "LIMIT 1"
            ),
            {"sha": text_sha256, "self_id": audit_id},
        ).scalar_one_or_none()
        return int(row) if row is not None else None

    # --- Per-row processing --------------------------------------------

    def _process_row(self, audit: AuditReport) -> tuple[int, _ProcessResult]:
        """Run the scope pipeline for a single claimed row.

        First tries the content-hash cache; falls through to
        ``process_audit_scope`` on a miss. Never raises.
        """
        session = SessionLocal()
        try:
            sibling_id = self._find_cache_sibling(session, audit.id, audit.text_sha256)
        finally:
            session.close()

        if sibling_id is not None:
            logger.info(
                "Worker %s: audit %s — cache hit via sibling %s (sha=%s)",
                self.worker_id,
                audit.id,
                sibling_id,
                (audit.text_sha256 or "")[:16],
            )
            return audit.id, _CacheCopyOutcome(sibling_id)

        if not audit.text_storage_key:
            return audit.id, ScopeExtractionOutcome(
                status="failed",
                error="audit has text_extraction_status=success but no text_storage_key",
            )

        outcome = process_audit_scope(
            audit_report_id=audit.id,
            text_storage_key=audit.text_storage_key,
            text_sha256=audit.text_sha256,
            audit_title=audit.title or "",
            auditor=audit.auditor or "",
        )
        return audit.id, outcome

    # --- Persistence ---------------------------------------------------

    def _persist_outcome(self, audit_id: int, result: _ProcessResult) -> None:
        """Write the outcome back to the row in a dedicated session."""
        now = datetime.now(timezone.utc)
        session = SessionLocal()
        try:
            audit = session.get(AuditReport, audit_id)
            if audit is None:
                logger.warning("Scope audit %s disappeared before persist", audit_id)
                return

            if isinstance(result, _CacheCopyOutcome):
                sibling = session.get(AuditReport, result.sibling_id)
                if sibling is None:
                    # Sibling was deleted between the lookup and persist —
                    # fall back to marking this row as pending so the next
                    # pass does a fresh extraction.
                    logger.warning(
                        "Cache sibling %s gone; resetting audit %s to NULL",
                        result.sibling_id,
                        audit_id,
                    )
                    audit.scope_extraction_status = None
                    audit.scope_extraction_worker = None
                    audit.scope_extraction_started_at = None
                    audit.scope_extraction_error = None
                    session.commit()
                    return
                audit.scope_extraction_status = "success"
                audit.scope_extraction_error = None
                audit.scope_extraction_worker = None
                audit.scope_extracted_at = now
                audit.scope_storage_key = sibling.scope_storage_key
                audit.scope_contracts = list(sibling.scope_contracts or [])
                self._maybe_backfill_date(audit, sibling.date)
                self._refresh_coverage(session, audit_id)
                session.commit()
                logger.info(
                    "Audit %s → cache-copy from %s (%d contracts)",
                    audit_id,
                    result.sibling_id,
                    len(audit.scope_contracts or []),
                )
                return

            outcome = result
            audit.scope_extraction_status = outcome.status
            audit.scope_extraction_error = outcome.error
            audit.scope_extraction_worker = None
            if outcome.status == "success":
                audit.scope_extracted_at = now
                audit.scope_storage_key = outcome.storage_key
                audit.scope_contracts = list(outcome.contracts)
                self._maybe_backfill_date(audit, outcome.extracted_date)
                self._refresh_coverage(session, audit_id)
            session.commit()
        except Exception:
            session.rollback()
            logger.exception("Failed to persist scope outcome for audit %s", audit_id)
        finally:
            session.close()

    @staticmethod
    def _refresh_coverage(session: Session, audit_id: int) -> None:
        """Rebuild ``audit_contract_coverage`` rows for this audit.

        Runs inside the caller's transaction so a coverage failure rolls
        the scope persist back too — but we also guard with try/except so
        an unexpected coverage bug never blocks a successful extraction
        from being recorded. Import is local to avoid a circular at
        worker-module import time.
        """
        from services.audits.coverage import upsert_coverage_for_audit

        try:
            inserted = upsert_coverage_for_audit(session, audit_id)
            logger.info(
                "Audit %s → coverage refreshed (%d row(s))",
                audit_id,
                inserted,
            )
        except Exception:
            logger.exception(
                "Failed to refresh coverage for audit %s — scope persist still proceeds",
                audit_id,
            )

    @staticmethod
    def _maybe_backfill_date(audit: AuditReport, candidate: str | None) -> None:
        """Overwrite ``audit.date`` when the existing value is null or partial.

        Discovery-time dates are best-effort (filename parsing), so nulls
        and ``YYYY-MM-00`` placeholders are common. When the extractor
        pulled a real date off the title page, prefer it.
        """
        if not candidate:
            return
        existing = audit.date or ""
        if not existing or existing.endswith("-00") or len(existing) < 10:
            audit.date = candidate

    # --- Main loop ------------------------------------------------------

    def run_loop(self) -> None:
        logger.info(
            "AuditScopeExtraction worker %s starting (batch=%d, pool=%d, idle=%ss, stale=%ss)",
            self.worker_id,
            _BATCH_SIZE,
            _MAX_CONCURRENT,
            _IDLE_POLL_INTERVAL,
            _STALE_PROCESSING_SECONDS,
        )

        executor = ThreadPoolExecutor(
            max_workers=_MAX_CONCURRENT,
            thread_name_prefix="audit-scope",
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
                    "Worker %s claimed %d audit(s) for scope extraction",
                    self.worker_id,
                    len(claimed),
                )

                futures = {executor.submit(self._process_row, row): row.id for row in claimed}
                for future in as_completed(futures):
                    try:
                        audit_id, result = future.result()
                    except Exception:
                        logger.exception("Unexpected error in audit scope thread")
                        continue
                    self._persist_outcome(audit_id, result)
                    if isinstance(result, _CacheCopyOutcome):
                        continue
                    logger.info(
                        "Audit %s → %s (method=%s, contracts=%d)%s",
                        audit_id,
                        result.status,
                        result.method,
                        len(result.contracts),
                        f" [{result.error}]" if result.error else "",
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
    AuditScopeExtractionWorker().run_loop()


if __name__ == "__main__":
    main()
