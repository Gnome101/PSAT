"""Match audit reports to the contracts they actually reviewed.

Populates the ``audit_contract_coverage`` join table so "has this impl been
audited?" is a cheap join, not a query-time scan of ``scope_contracts[]``.
Proxy-aware: a coverage row links the *implementation* contract the audit
reviewed, not the proxy — using ``UpgradeEvent`` history to pin the audit
to the right era.

Two symmetric entry points call the same core matcher:

    match_contracts_for_audit(session, audit_id)     # scope_contracts → Contracts
    match_audits_for_contract(session, contract_id)  # Contract name → AuditReports

Both produce ``CoverageMatch`` records. ``upsert_coverage_for_audit`` (and
the ``upsert_coverage_for_protocol`` wrapper) writes them to the DB with
idempotent delete-then-insert semantics so re-running is safe and scope
re-extractions pick up cleanly.

Matching signals are layered, cheapest first:

  - ``'direct'`` — scope name matches a Contract.contract_name. No proxy
    history; confidence=high if audit has a date, medium if not.
  - ``'impl_era'`` — scope name matches AND the impl was active at the
    audit's date per ``UpgradeEvent`` history. Confidence=high inside
    window, medium in a 14-day grace zone on either boundary, low when
    clearly outside.
  - ``'reviewed_commit'`` — source-equivalence proof (see
    ``services/audits/source_equivalence.py``). The audit PDF references
    a commit whose source file is byte-identical to Etherscan's verified
    source for the matched impl. Overrides temporal confidence with
    high — this is definitive evidence the audit reviewed the deployed
    code. Opt-in via ``verify_source_equivalence=True`` because it
    costs ~2 HTTP requests per (scope-contract × reviewed-commit) pair.

The ``AuditReport.source_commit`` field is a *discovery-time* SHA (where
the PDF was found in an org's repo), not a review-commit anchor. The
real reviewed commits live in ``AuditReport.reviewed_commits``, extracted
from PDF text by scope extraction and then consumed by the source-
equivalence pass.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Final

from sqlalchemy import delete as sql_delete
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from db.models import (
    AuditContractCoverage,
    AuditReport,
    Contract,
    UpgradeEvent,
)

logger = logging.getLogger(__name__)


# --- Tunables -----------------------------------------------------------

# Grace window around an impl's active range. An audit published a few days
# after an upgrade usually reviewed the older impl — the engagement predates
# the publish date. 14 days is loose enough to catch common cases without
# letting the audit match an impl that was active weeks later.
GRACE_DAYS: Final[int] = 14


# --- Result types -------------------------------------------------------


@dataclass(frozen=True)
class ImplWindow:
    """A contiguous span during which an address was an active impl."""

    proxy_contract_id: int  # Contract.id of the proxy this window is on
    proxy_address: str
    from_block: int
    to_block: int | None  # None = still current on this proxy
    from_ts: datetime | None
    to_ts: datetime | None


@dataclass(frozen=True)
class CoverageMatch:
    """One contract ↔ audit link that ``upsert_coverage_for_audit`` will persist."""

    audit_report_id: int
    contract_id: int
    protocol_id: int
    matched_name: str
    match_type: str  # 'direct' | 'impl_era'
    match_confidence: str  # 'high' | 'medium' | 'low'
    covered_from_block: int | None = None
    covered_to_block: int | None = None


# --- Date parsing -------------------------------------------------------


def _audit_effective_ts(audit_date: str | None) -> datetime | None:
    """Parse ``AuditReport.date`` into a tz-aware datetime.

    Accepted forms (produced by the discovery + scope-extraction pipeline):
      - YYYY-MM-DD → that day at 23:59:59 UTC (end-of-day)
      - YYYY-MM or YYYY-MM-00 → last day of month at 23:59:59 UTC
      - YYYY → December 31 at 23:59:59 UTC
      - None / unparseable → None

    Returns None rather than raising so a malformed discovery-time date
    downgrades to ``'low'`` confidence instead of blocking coverage.
    End-of-period semantics are intentional: audits finalize late in the
    stated period, so picking the latest plausible moment is the most
    generous interpretation of "was this impl active at audit time?".
    """
    if not audit_date:
        return None
    s = audit_date.strip()
    if not s:
        return None

    # YYYY-MM-DD
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        y, m, d = s[0:4], s[5:7], s[8:10]
        try:
            year, month, day = int(y), int(m), int(d)
            if day == 0:
                # YYYY-MM-00 placeholder → end of month
                return _end_of_month(year, month)
            return datetime(year, month, day, 23, 59, 59, tzinfo=timezone.utc)
        except ValueError:
            return None

    # YYYY-MM
    if len(s) == 7 and s[4] == "-":
        try:
            return _end_of_month(int(s[0:4]), int(s[5:7]))
        except ValueError:
            return None

    # YYYY
    if len(s) == 4 and s.isdigit():
        try:
            return datetime(int(s), 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        except ValueError:
            return None

    return None


def _end_of_month(year: int, month: int) -> datetime:
    """Return the last second of the given month as tz-aware UTC."""
    if month == 12:
        first_of_next = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        first_of_next = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return first_of_next - timedelta(seconds=1)


# --- Impl window computation --------------------------------------------


def _compute_impl_windows_for_contract(session: Session, contract: Contract) -> list[ImplWindow]:
    """Return windows during which ``contract.address`` was an active impl.

    Walks ``UpgradeEvent`` rows where ``new_impl`` matches the contract's
    address. The upper bound of each window is the *next* upgrade on the
    same proxy (same ``UpgradeEvent.contract_id``) with a strictly greater
    block number — NULL means the impl is still current on that proxy.

    Address comparison is case-insensitive via a lowered match because
    Etherscan and EVM tooling disagree about checksum casing.

    A contract used as the impl of multiple proxies gets multiple windows;
    the caller unions them when deciding "was the audit consistent with
    any active window?". Kept explicit so the UI can later distinguish
    "audit covers impl X on proxy P1" from "impl X on proxy P2".
    """
    if not contract.address:
        return []
    addr_lower = contract.address.lower()

    # Pull every proxy (via its UpgradeEvent.contract_id) that ever pointed
    # at this address. We need the FULL event history for those proxies
    # below — not just the ones naming this address — because the window's
    # end is defined by the NEXT event on the same proxy, whatever its
    # new_impl was.
    proxy_ids = set(
        session.execute(
            select(UpgradeEvent.contract_id).where(func.lower(UpgradeEvent.new_impl) == addr_lower).distinct()
        )
        .scalars()
        .all()
    )
    if not proxy_ids:
        return []

    windows: list[ImplWindow] = []

    for proxy_id in proxy_ids:
        proxy = session.get(Contract, proxy_id)
        if proxy is None:
            continue

        # Ordered full history for this proxy. Events with NULL block_number
        # sink to the bottom so a hand-crafted (test) event without blocks
        # doesn't corrupt the ordering.
        events = (
            session.execute(
                select(UpgradeEvent)
                .where(UpgradeEvent.contract_id == proxy_id)
                .order_by(
                    UpgradeEvent.block_number.asc().nullslast(),
                    UpgradeEvent.id.asc(),
                )
            )
            .scalars()
            .all()
        )
        if not events:
            continue

        # For each occurrence where this address was introduced as the new
        # impl, the window ends at the next event on the same proxy.
        for i, ev in enumerate(events):
            if not ev.new_impl or ev.new_impl.lower() != addr_lower:
                continue
            from_block = ev.block_number or 0
            from_ts = ev.timestamp
            to_block: int | None = None
            to_ts: datetime | None = None
            if i + 1 < len(events):
                nxt = events[i + 1]
                to_block = nxt.block_number
                to_ts = nxt.timestamp
            windows.append(
                ImplWindow(
                    proxy_contract_id=proxy_id,
                    proxy_address=proxy.address or "",
                    from_block=from_block,
                    to_block=to_block,
                    from_ts=from_ts,
                    to_ts=to_ts,
                )
            )

    # Deterministic order so callers (and tests) can rely on it.
    windows.sort(key=lambda w: (w.from_block, w.proxy_contract_id))
    return windows


# --- Confidence scoring -------------------------------------------------


def _confidence_for_impl_era(audit_ts: datetime | None, windows: list[ImplWindow]) -> tuple[str, ImplWindow | None]:
    """Pick the best-matching window for the audit timestamp + grade it.

    - ``high``: the audit landed inside one of the contract's active windows.
      Open-ended windows (``to_ts=None``, still current) count too.
    - ``medium``: audit is within ``GRACE_DAYS`` of a window boundary on
      either side. Covers the common "audit published shortly after an
      upgrade happened" case where the reviewed code is actually the
      just-replaced impl.
    - ``low``: audit timestamp is None (nothing to place against) or falls
      clearly outside every window. The row still gets emitted so the UI
      can show "we found a name match but the timing doesn't line up".

    Returns the chosen window for range-column population. ``None`` when
    every window is a bad fit — the row gets NULL range bounds.
    """
    if not windows:
        # No impl history at all — shouldn't be called in impl_era mode.
        return "low", None

    if audit_ts is None:
        # No audit date. Best effort: attach to the most recent (open-ended)
        # window if any — the naive assumption that absent-date audits are
        # modern. Low confidence; UI can filter these out.
        open_windows = [w for w in windows if w.to_block is None]
        if open_windows:
            return "low", open_windows[0]
        return "low", windows[-1]

    # In a strictly-inside window → high.
    for w in windows:
        if w.from_ts is None:
            # Block-only windows can't be temporally placed — fall through
            # to the grace check, which will skip them too.
            continue
        if audit_ts >= w.from_ts and (w.to_ts is None or audit_ts < w.to_ts):
            return "high", w

    # Within grace of some boundary → medium. Pick the closest window.
    grace = timedelta(days=GRACE_DAYS)
    best_distance: timedelta | None = None
    best_window: ImplWindow | None = None
    for w in windows:
        if w.from_ts is None:
            continue
        # Distance to this window (0 if inside, otherwise to the nearest edge).
        if audit_ts < w.from_ts:
            dist = w.from_ts - audit_ts
        elif w.to_ts is not None and audit_ts >= w.to_ts:
            dist = audit_ts - w.to_ts
        else:
            # Inside, but we would have matched above — timestamp weirdness.
            dist = timedelta(0)
        if dist <= grace:
            if best_distance is None or dist < best_distance:
                best_distance = dist
                best_window = w
    if best_window is not None:
        return "medium", best_window

    # Neither inside nor within grace. Pick the nearest window anyway so
    # covered_from_block isn't NULL when we have some plausible anchor.
    # This is a 'low' — the UI / caller decides what to do with it.
    for w in windows:
        if w.from_ts is None:
            continue
        if audit_ts < w.from_ts:
            dist = w.from_ts - audit_ts
        elif w.to_ts is not None and audit_ts >= w.to_ts:
            dist = audit_ts - w.to_ts
        else:
            dist = timedelta(0)
        if best_distance is None or dist < best_distance:
            best_distance = dist
            best_window = w
    return "low", best_window


def _confidence_for_direct(audit_ts: datetime | None, contract: Contract) -> str:
    """Confidence for a direct (no-impl-history) match.

    - ``high`` when we have an audit date AND the contract deployment /
      verification plausibly predates it. We don't have a deployment-time
      column on Contract today, so "plausibly" is just "audit has a date".
    - ``medium`` when the audit has no date (name-only match).
    - (No ``low`` from this path — a direct match with nothing to verify
      against is still a clean name match; falling to low would under-sell
      it relative to a distant impl_era match.)
    """
    return "high" if audit_ts is not None else "medium"


# --- Core matchers ------------------------------------------------------


def _normalize_name(name: str | None) -> str:
    return (name or "").strip().lower()


def match_contracts_for_audit(session: Session, audit_id: int) -> list[CoverageMatch]:
    """Find every Contract in the audit's protocol that matches a scope name.

    Returns at most one CoverageMatch per ``(contract_id, audit_id)`` pair
    — when multiple scope names collapse onto the same Contract (e.g.
    auditor shipped both ``EtherFiNodesManager`` and ``EtherFiNodeManager``),
    the highest-confidence match wins.
    """
    audit = session.get(AuditReport, audit_id)
    if audit is None:
        return []
    scope_names = audit.scope_contracts or []
    if not scope_names:
        return []

    scope_lookup: dict[str, str] = {}
    for name in scope_names:
        key = _normalize_name(name)
        if not key:
            continue
        # Preserve the first raw spelling for the matched_name column.
        scope_lookup.setdefault(key, name)

    if not scope_lookup:
        return []

    # Protocol contracts whose contract_name matches a scope entry, case-
    # insensitively. Parameter binding makes LLM-sourced strings safe to
    # pass through IN_.
    candidates = (
        session.execute(
            select(Contract).where(
                Contract.protocol_id == audit.protocol_id,
                func.lower(Contract.contract_name).in_(list(scope_lookup.keys())),
            )
        )
        .scalars()
        .all()
    )
    if not candidates:
        return []

    audit_ts = _audit_effective_ts(audit.date)

    # Per-contract: pick the best match. When a contract has impl windows,
    # we emit impl_era; otherwise direct.
    by_contract: dict[int, CoverageMatch] = {}
    for c in candidates:
        matched_name = scope_lookup.get(_normalize_name(c.contract_name))
        if not matched_name:
            continue
        windows = _compute_impl_windows_for_contract(session, c)
        if windows:
            confidence, window = _confidence_for_impl_era(audit_ts, windows)
            match = CoverageMatch(
                audit_report_id=audit.id,
                contract_id=c.id,
                protocol_id=audit.protocol_id,
                matched_name=matched_name,
                match_type="impl_era",
                match_confidence=confidence,
                covered_from_block=window.from_block if window else None,
                covered_to_block=window.to_block if window else None,
            )
        else:
            confidence = _confidence_for_direct(audit_ts, c)
            match = CoverageMatch(
                audit_report_id=audit.id,
                contract_id=c.id,
                protocol_id=audit.protocol_id,
                matched_name=matched_name,
                match_type="direct",
                match_confidence=confidence,
            )
        prev = by_contract.get(c.id)
        if prev is None or _confidence_rank(match.match_confidence) > _confidence_rank(prev.match_confidence):
            by_contract[c.id] = match

    return list(by_contract.values())


_CONFIDENCE_ORDER: Final[dict[str, int]] = {"low": 0, "medium": 1, "high": 2}


def _confidence_rank(conf: str) -> int:
    return _CONFIDENCE_ORDER.get(conf, 0)


def match_audits_for_contract(session: Session, contract_id: int) -> list[CoverageMatch]:
    """Dual entry. For each audit in the contract's protocol whose scope
    mentions the contract's name, emit a match. Same rules as
    ``match_contracts_for_audit``.
    """
    contract = session.get(Contract, contract_id)
    if contract is None or contract.protocol_id is None:
        return []
    name_key = _normalize_name(contract.contract_name)
    if not name_key:
        return []

    audits = (
        session.execute(
            select(AuditReport).where(
                AuditReport.protocol_id == contract.protocol_id,
                AuditReport.scope_extraction_status == "success",
                AuditReport.scope_contracts.isnot(None),
            )
        )
        .scalars()
        .all()
    )
    windows = _compute_impl_windows_for_contract(session, contract)

    out: list[CoverageMatch] = []
    for audit in audits:
        scope_names = audit.scope_contracts or []
        matched_name = next((n for n in scope_names if _normalize_name(n) == name_key), None)
        if not matched_name:
            continue
        audit_ts = _audit_effective_ts(audit.date)
        if windows:
            confidence, window = _confidence_for_impl_era(audit_ts, windows)
            out.append(
                CoverageMatch(
                    audit_report_id=audit.id,
                    contract_id=contract.id,
                    protocol_id=contract.protocol_id,
                    matched_name=matched_name,
                    match_type="impl_era",
                    match_confidence=confidence,
                    covered_from_block=window.from_block if window else None,
                    covered_to_block=window.to_block if window else None,
                )
            )
        else:
            confidence = _confidence_for_direct(audit_ts, contract)
            out.append(
                CoverageMatch(
                    audit_report_id=audit.id,
                    contract_id=contract.id,
                    protocol_id=contract.protocol_id,
                    matched_name=matched_name,
                    match_type="direct",
                    match_confidence=confidence,
                )
            )
    return out


# --- Upsert helpers -----------------------------------------------------


def _match_to_row_kwargs(match: CoverageMatch) -> dict:
    return {
        "contract_id": match.contract_id,
        "audit_report_id": match.audit_report_id,
        "protocol_id": match.protocol_id,
        "matched_name": match.matched_name,
        "match_type": match.match_type,
        "match_confidence": match.match_confidence,
        "covered_from_block": match.covered_from_block,
        "covered_to_block": match.covered_to_block,
    }


def _reviewed_commit_upgrades(
    session: Session,
    audit_id: int,
    matches: list[CoverageMatch],
) -> list[CoverageMatch]:
    """For each match, try to prove it via source-equivalence.

    When the audit has ``reviewed_commits`` + ``source_repo``, runs a
    GitHub + Etherscan cross-check against each candidate contract. A
    positive hit upgrades the match to ``match_type='reviewed_commit'``
    / ``match_confidence='high'`` — the audit is proven to have reviewed
    the byte-exact source now deployed at that impl. Temporal bounds
    (``covered_from_block`` / ``covered_to_block``) pass through unchanged;
    the proof is per-file, not per-window.

    Non-destructive: returns a new list; never mutates caller's input.
    Swallows per-match exceptions — if proof fails to resolve, the
    existing temporal match stands.
    """
    import os

    from services.audits.source_equivalence import check_audit_row_covers_contract

    audit = session.get(AuditReport, audit_id)
    if audit is None:
        return matches
    if not (audit.reviewed_commits and audit.source_repo):
        return matches

    gh_token = os.environ.get("GITHUB_TOKEN") or None
    upgraded: list[CoverageMatch] = []
    for m in matches:
        try:
            proofs = check_audit_row_covers_contract(session, audit_id, m.contract_id, github_token=gh_token)
        except Exception:
            logger.exception(
                "source-equivalence check crashed for audit %s / contract %s",
                audit_id,
                m.contract_id,
            )
            upgraded.append(m)
            continue
        if proofs:
            proof = proofs[0]
            upgraded.append(
                CoverageMatch(
                    audit_report_id=m.audit_report_id,
                    contract_id=m.contract_id,
                    protocol_id=m.protocol_id,
                    matched_name=m.matched_name,
                    match_type="reviewed_commit",
                    match_confidence="high",
                    covered_from_block=m.covered_from_block,
                    covered_to_block=m.covered_to_block,
                )
            )
            logger.info(
                "coverage: audit %s proven to cover contract %s via source-equivalence at commit %s / path %s",
                audit_id,
                m.contract_id,
                proof.commit,
                proof.etherscan_path,
            )
        else:
            upgraded.append(m)
    return upgraded


def upsert_coverage_for_audit(
    session: Session,
    audit_id: int,
    *,
    verify_source_equivalence: bool = False,
) -> int:
    """Replace all coverage rows for ``audit_id`` with a fresh match.

    Delete-then-insert (inside the caller's transaction) gives free
    invalidation when scope re-extraction changes ``scope_contracts[]`` —
    stale rows are gone, new rows reflect the current scope. Returns the
    number of rows inserted. The caller is responsible for ``session.commit()``.

    ``verify_source_equivalence`` (default False) runs a GitHub + Etherscan
    cross-check on each match: when the audit has ``reviewed_commits`` +
    ``source_repo`` set and a candidate impl's Etherscan-verified source
    is byte-identical to the file at the reviewed commit, the match is
    upgraded to ``'reviewed_commit'`` / ``'high'``. Opt-in because it
    costs ~2 HTTP requests per scope-contract × reviewed-commit pair.
    """
    audit = session.get(AuditReport, audit_id)
    if audit is None:
        return 0

    session.execute(sql_delete(AuditContractCoverage).where(AuditContractCoverage.audit_report_id == audit_id))

    if audit.scope_extraction_status != "success":
        # Nothing to insert — either not yet extracted or explicitly skipped.
        return 0

    matches = match_contracts_for_audit(session, audit_id)
    if not matches:
        logger.info(
            "coverage: audit %s has scope but no Contract rows matched in protocol %s",
            audit_id,
            audit.protocol_id,
        )
        return 0

    if verify_source_equivalence:
        matches = _reviewed_commit_upgrades(session, audit_id, matches)

    for match in matches:
        session.add(AuditContractCoverage(**_match_to_row_kwargs(match)))
    return len(matches)


def upsert_coverage_for_contract(session: Session, contract_id: int) -> int:
    """Refresh coverage rows whose ``contract_id`` points at this contract.

    Used when a live upgrade changes the contract's impl windows — any
    existing impl_era rows may have stale block ranges. Delete-then-insert
    keyed on ``contract_id`` keeps the write side simple.
    """
    session.execute(sql_delete(AuditContractCoverage).where(AuditContractCoverage.contract_id == contract_id))
    matches = match_audits_for_contract(session, contract_id)
    for match in matches:
        session.add(AuditContractCoverage(**_match_to_row_kwargs(match)))
    return len(matches)


def upsert_coverage_for_protocol(
    session: Session,
    protocol_id: int,
    *,
    verify_source_equivalence: bool = False,
) -> int:
    """Rebuild coverage for every scoped audit in a protocol.

    Idempotent. Used as the batch entry point — call from CLI flows,
    admin-triggered refresh endpoints, and integration tests. Returns
    the total inserted row count. ``verify_source_equivalence`` forwards
    to ``upsert_coverage_for_audit``; set True only when the caller is OK
    with the network cost of per-audit GitHub + Etherscan lookups.
    """
    audit_ids = (
        session.execute(
            select(AuditReport.id).where(
                AuditReport.protocol_id == protocol_id,
                AuditReport.scope_extraction_status == "success",
            )
        )
        .scalars()
        .all()
    )
    total = 0
    for aid in audit_ids:
        total += upsert_coverage_for_audit(session, aid, verify_source_equivalence=verify_source_equivalence)
    return total
