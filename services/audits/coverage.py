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


# Grace zone around an impl's active range — audits published shortly
# after an upgrade usually reviewed the older impl (engagement predates
# publication). 14d catches the common case without overreaching.
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
    """Parse ``AuditReport.date`` into a tz-aware datetime (end-of-period).

    Accepts ``YYYY-MM-DD`` / ``YYYY-MM`` / ``YYYY-MM-00`` / ``YYYY``. Returns
    ``None`` on malformed input so coverage downgrades to ``low`` instead
    of crashing. End-of-period semantics are deliberate — audits finalize
    late in the stated period.
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


def _compute_impl_windows_batch(session: Session, contracts: list[Contract]) -> dict[int, list[ImplWindow]]:
    """Return impl windows for each input contract, keyed by Contract.id.

    Batched replacement for per-contract ``_compute_impl_windows_for_contract``:
    fires ONE proxy-lookup and ONE event-history query regardless of how
    many candidate contracts are passed. Callers iterating over large
    candidate lists (e.g. ``match_contracts_for_audit`` on a protocol
    with many scope-name matches) see O(1) queries instead of O(N*K).
    """
    addr_to_ids: dict[str, list[int]] = {}
    for c in contracts:
        if c.address:
            addr_to_ids.setdefault(c.address.lower(), []).append(c.id)
    result: dict[int, list[ImplWindow]] = {c.id: [] for c in contracts}
    if not addr_to_ids:
        return result

    # One query: every (proxy_contract_id, new_impl) where new_impl is in
    # the candidate address set. ``new_impl`` may be mixed-case, so we
    # lowercase on both sides and project it back for the addr→impl map.
    proxy_rows = session.execute(
        select(
            UpgradeEvent.contract_id,
            func.lower(UpgradeEvent.new_impl).label("new_impl_lc"),
        )
        .where(func.lower(UpgradeEvent.new_impl).in_(list(addr_to_ids.keys())))
        .distinct()
    ).all()
    if not proxy_rows:
        return result

    proxy_ids = {row[0] for row in proxy_rows}
    # addr → set of proxy contract_ids that ever pointed at this address.
    addr_to_proxies: dict[str, set[int]] = {}
    for pid, impl_addr in proxy_rows:
        addr_to_proxies.setdefault(impl_addr, set()).add(pid)

    # One query: full event history for all relevant proxies, ordered
    # canonically. Events with NULL block_number sink so a hand-crafted
    # (test) event without blocks doesn't corrupt ordering.
    events_by_proxy: dict[int, list[UpgradeEvent]] = {pid: [] for pid in proxy_ids}
    events = (
        session.execute(
            select(UpgradeEvent)
            .where(UpgradeEvent.contract_id.in_(proxy_ids))
            .order_by(
                UpgradeEvent.contract_id.asc(),
                UpgradeEvent.block_number.asc().nullslast(),
                UpgradeEvent.id.asc(),
            )
        )
        .scalars()
        .all()
    )
    for ev in events:
        events_by_proxy.setdefault(ev.contract_id, []).append(ev)

    # One query: Contract rows for every relevant proxy, for the
    # ``proxy_address`` display field on ImplWindow.
    proxy_addr_by_id: dict[int, str] = {}
    if proxy_ids:
        rows = session.execute(select(Contract.id, Contract.address).where(Contract.id.in_(proxy_ids))).all()
        for pid, addr in rows:
            proxy_addr_by_id[pid] = addr or ""

    # Build a window list per (lowercase address) once; then distribute to
    # every Contract.id whose address matches (identical impl addresses on
    # different rows is rare, but the invariant costs nothing).
    windows_by_addr: dict[str, list[ImplWindow]] = {}
    for addr_lower, proxy_id_set in addr_to_proxies.items():
        windows: list[ImplWindow] = []
        for pid in proxy_id_set:
            proxy_events = events_by_proxy.get(pid) or []
            for i, ev in enumerate(proxy_events):
                if not ev.new_impl or ev.new_impl.lower() != addr_lower:
                    continue
                from_block = ev.block_number or 0
                from_ts = ev.timestamp
                to_block: int | None = None
                to_ts: datetime | None = None
                if i + 1 < len(proxy_events):
                    nxt = proxy_events[i + 1]
                    to_block = nxt.block_number
                    to_ts = nxt.timestamp
                windows.append(
                    ImplWindow(
                        proxy_contract_id=pid,
                        proxy_address=proxy_addr_by_id.get(pid, ""),
                        from_block=from_block,
                        to_block=to_block,
                        from_ts=from_ts,
                        to_ts=to_ts,
                    )
                )
        # Deterministic order so callers (and tests) can rely on it.
        windows.sort(key=lambda w: (w.from_block, w.proxy_contract_id))
        windows_by_addr[addr_lower] = windows

    for addr_lower, contract_ids in addr_to_ids.items():
        ws = windows_by_addr.get(addr_lower, [])
        for cid in contract_ids:
            # Copy so distinct list identity per contract_id, cheap for small lists.
            result[cid] = list(ws)
    return result


def _compute_impl_windows_for_contract(session: Session, contract: Contract) -> list[ImplWindow]:
    """Return windows during which ``contract.address`` was an active impl.

    Thin wrapper around :func:`_compute_impl_windows_batch` for the
    single-contract ``match_audits_for_contract`` entry point. Kept for
    callers that only need one contract's windows.
    """
    return _compute_impl_windows_batch(session, [contract]).get(contract.id, [])


# --- Confidence scoring -------------------------------------------------


def _confidence_for_impl_era(audit_ts: datetime | None, windows: list[ImplWindow]) -> tuple[str, ImplWindow | None]:
    """Pick the best-matching window for the audit timestamp.

    Returns ``(confidence, window)``:
        high   — audit lands inside an active window
        medium — audit within ``GRACE_DAYS`` of a window boundary
        low    — no timestamp, or clearly outside every window

    Low-confidence rows still emit so the UI can flag name-matches with
    bad timing. Window is ``None`` when nothing is a plausible anchor.
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
    """High when audit has a date, medium when name-only (no date).

    Direct matches never go low: a clean name match without history to
    falsify against beats a distant impl_era match.
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

    # Batch impl-window computation: one query for the proxy lookup and
    # one for the event history across every candidate, instead of N+K per
    # candidate. For protocols with a wide scope, this is the difference
    # between a single rebuild lasting seconds vs. minutes.
    windows_by_id = _compute_impl_windows_batch(session, list(candidates))

    # Per-contract: pick the best match. When a contract has impl windows,
    # we emit impl_era; otherwise direct.
    by_contract: dict[int, CoverageMatch] = {}
    for c in candidates:
        matched_name = scope_lookup.get(_normalize_name(c.contract_name))
        if not matched_name:
            continue
        windows = windows_by_id.get(c.id, [])
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
            # is_proxy=True direct matches are almost always false positives:
            # scope names a generic library (UUPSProxy, ERC1967Proxy, etc.),
            # the proxy's contract_name happens to be that string, but the
            # audit didn't actually review what sits behind this proxy.
            # Real coverage flows via the impl's own Contract row.
            if c.is_proxy:
                continue
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
            # See match_contracts_for_audit: is_proxy direct-name matches
            # are the false-positive class we drop.
            if contract.is_proxy:
                continue
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


@dataclass(frozen=True)
class _EquivalenceInputs:
    """Inputs to run ``check_audit_covers_impl`` without holding a session.

    Materialized in the DB phase so the HTTP phase can run with no open
    transaction: the GitHub fetches inside ``check_audit_covers_impl`` are
    pure HTTP and need no session.
    """

    audit_report_id: int
    contract_id: int
    contract_address: str | None
    reviewed_commits: tuple[str, ...]
    scope_contracts: tuple[str, ...]
    source_repo: str | None
    # DB-resolved impl source, if Contract.job_id had SourceFile rows.
    # None means the HTTP phase should call Etherscan as a fallback.
    db_impl_source: "object | None"


def _preload_equivalence_inputs(
    session: Session, matches: list[CoverageMatch]
) -> dict[tuple[int, int], _EquivalenceInputs]:
    """Pull the data needed for source-equivalence into plain Python objects.

    Runs inside the caller's DB phase so the subsequent HTTP phase can
    execute with the transaction released. Each match gets one entry keyed
    by ``(audit_id, contract_id)``. Matches whose audit lacks
    ``reviewed_commits``/``source_repo`` are still recorded (with empty
    tuples) so the HTTP phase can cheaply skip them.
    """
    from services.audits.source_equivalence import fetch_db_source_files

    out: dict[tuple[int, int], _EquivalenceInputs] = {}
    audit_cache: dict[int, AuditReport | None] = {}
    contract_cache: dict[int, Contract | None] = {}
    for m in matches:
        audit = audit_cache.get(m.audit_report_id)
        if m.audit_report_id not in audit_cache:
            audit = session.get(AuditReport, m.audit_report_id)
            audit_cache[m.audit_report_id] = audit
        contract = contract_cache.get(m.contract_id)
        if m.contract_id not in contract_cache:
            contract = session.get(Contract, m.contract_id)
            contract_cache[m.contract_id] = contract
        if audit is None or contract is None:
            continue
        db_source = fetch_db_source_files(session, m.contract_id)
        out[(m.audit_report_id, m.contract_id)] = _EquivalenceInputs(
            audit_report_id=m.audit_report_id,
            contract_id=m.contract_id,
            contract_address=contract.address,
            reviewed_commits=tuple(audit.reviewed_commits or ()),
            scope_contracts=tuple(audit.scope_contracts or ()),
            source_repo=audit.source_repo,
            db_impl_source=db_source,
        )
    return out


def _apply_equivalence_http(
    matches: list[CoverageMatch],
    inputs: dict[tuple[int, int], _EquivalenceInputs],
) -> list[CoverageMatch]:
    """HTTP phase: upgrade matches to ``reviewed_commit`` via source-equivalence.

    No session involvement. Per-match: if DB impl source was preloaded we
    use it; otherwise we fall back to Etherscan (pure HTTP). Then GitHub
    raw fetches decide equivalence. Failures are swallowed and the
    temporal match stands.
    """
    import os

    from services.audits.source_equivalence import (
        check_audit_covers_impl,
        fetch_etherscan_source_files,
    )

    gh_token = os.environ.get("GITHUB_TOKEN") or None
    etherscan_cache: dict[str, object | None] = {}

    upgraded: list[CoverageMatch] = []
    for m in matches:
        key = (m.audit_report_id, m.contract_id)
        data = inputs.get(key)
        if data is None or not (data.reviewed_commits and data.source_repo and data.scope_contracts):
            upgraded.append(m)
            continue
        impl_source = data.db_impl_source
        if impl_source is None and data.contract_address:
            addr_key = data.contract_address.lower()
            if addr_key not in etherscan_cache:
                try:
                    etherscan_cache[addr_key] = fetch_etherscan_source_files(data.contract_address)
                except Exception:
                    logger.exception("source-equivalence Etherscan fetch crashed for contract %s", m.contract_id)
                    etherscan_cache[addr_key] = None
            impl_source = etherscan_cache[addr_key]
        if impl_source is None:
            upgraded.append(m)
            continue
        try:
            proofs = check_audit_covers_impl(
                reviewed_commits=list(data.reviewed_commits),
                scope_contracts=list(data.scope_contracts),
                impl_source=impl_source,  # type: ignore[arg-type]
                source_repo=data.source_repo,
                github_token=gh_token,
            )
        except Exception:
            logger.exception(
                "source-equivalence check crashed for audit %s / contract %s",
                m.audit_report_id,
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
                m.audit_report_id,
                m.contract_id,
                proof.commit,
                proof.etherscan_path,
            )
        else:
            upgraded.append(m)
    return upgraded


def _persist_coverage_for_audit(session: Session, audit_id: int, matches: list[CoverageMatch]) -> int:
    """Delete-then-insert coverage rows for one audit in a short tx."""
    session.execute(sql_delete(AuditContractCoverage).where(AuditContractCoverage.audit_report_id == audit_id))
    for match in matches:
        session.add(AuditContractCoverage(**_match_to_row_kwargs(match)))
    return len(matches)


def _persist_coverage_for_contract(session: Session, contract_id: int, matches: list[CoverageMatch]) -> int:
    """Delete-then-insert coverage rows for one contract in a short tx."""
    session.execute(sql_delete(AuditContractCoverage).where(AuditContractCoverage.contract_id == contract_id))
    for match in matches:
        session.add(AuditContractCoverage(**_match_to_row_kwargs(match)))
    return len(matches)


def upsert_coverage_for_audit(
    session: Session,
    audit_id: int,
    *,
    verify_source_equivalence: bool = False,
) -> int:
    """Replace all coverage rows for ``audit_id`` with a fresh match.

    Two-phase when ``verify_source_equivalence`` is true: a DB phase
    computes the base temporal matches and pre-loads the inputs for
    source-equivalence, commits, runs GitHub + Etherscan HTTP with the
    transaction released, and finally opens a fresh transaction for the
    delete-then-insert. This keeps long-running HTTP (hundreds of seconds
    on a wide audit) out of the Postgres row locks that would otherwise
    wedge concurrent writers. Returns inserted row count; caller commits.
    """
    audit = session.get(AuditReport, audit_id)
    if audit is None:
        # Nothing to rebuild — ensure no stale coverage rows linger.
        _persist_coverage_for_audit(session, audit_id, [])
        return 0

    if audit.scope_extraction_status != "success":
        # Nothing to insert — either not yet extracted or explicitly skipped.
        _persist_coverage_for_audit(session, audit_id, [])
        return 0

    matches = match_contracts_for_audit(session, audit_id)
    if not matches:
        logger.info(
            "coverage: audit %s has scope but no Contract rows matched in protocol %s",
            audit_id,
            audit.protocol_id,
        )
        _persist_coverage_for_audit(session, audit_id, [])
        return 0

    if verify_source_equivalence:
        # Phase A2: pre-load source-equivalence inputs while still holding
        # the session, then commit so HTTP runs with no tx open.
        equiv_inputs = _preload_equivalence_inputs(session, matches)
        session.commit()
        matches = _apply_equivalence_http(matches, equiv_inputs)

    # Phase B: fresh transaction for the delete-then-insert.
    return _persist_coverage_for_audit(session, audit_id, matches)


def upsert_coverage_for_contract(
    session: Session,
    contract_id: int,
    *,
    verify_source_equivalence: bool = False,
) -> int:
    """Refresh coverage rows for one contract (delete-then-insert by contract_id).

    Called after a live upgrade changes the contract's impl windows.
    ``verify_source_equivalence`` forwards to the source-equivalence pass
    with the same two-phase tx-release-during-HTTP pattern as
    :func:`upsert_coverage_for_audit`.
    """
    matches = match_audits_for_contract(session, contract_id)
    if verify_source_equivalence and matches:
        equiv_inputs = _preload_equivalence_inputs(session, matches)
        session.commit()
        matches = _apply_equivalence_http(matches, equiv_inputs)
    return _persist_coverage_for_contract(session, contract_id, matches)


def upsert_coverage_for_protocol(
    session: Session,
    protocol_id: int,
    *,
    verify_source_equivalence: bool = False,
) -> int:
    """Rebuild coverage for every scoped audit in a protocol. Idempotent.

    Batch entry point used by CLI flows, admin refresh endpoints, and
    integration tests. ``verify_source_equivalence`` adds ~2 HTTP/pair.
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
