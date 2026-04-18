#!/usr/bin/env python3
"""End-to-end verification of the audit-coverage pipeline against real ether.fi data.

Runs against ``TEST_DATABASE_URL`` (from ``.env`` — the ``psat_test`` DB).
Live — calls Etherscan to fetch upgrade history for every seeded ether.fi
proxy and to resolve contract names for each historical implementation.
Not part of the CI suite; run manually to demo the pipeline or debug.

What it demonstrates:

  1. Seeds ``Protocol(name='ether.fi')`` + ``AuditReport`` rows enriched
     with ``scope_contracts`` from ``scope_extraction_v8.json``.
  2. Seeds ``Contract`` rows for a hand-curated set of known ether.fi
     proxies, resolving each proxy's Etherscan-verified name.
  3. Backfills ``UpgradeEvent`` history for every seeded proxy and creates
     ``Contract`` rows for every historical implementation, with its
     Etherscan-verified name (so temporal matching has real names to
     compare against scope_contracts).
  4. Runs ``upsert_coverage_for_protocol`` end-to-end.
  5. Hits the new API endpoints (in-process TestClient) and prints:
     - Per-proxy ``current_status`` (to see 'audited' vs
       'unaudited_since_upgrade' distribute across the real fleet)
     - A spot-check that the Solidified 2023-10-26 audit landed in the
       correct block window given its date
     - A non-proxy impl contract's timeline to exercise that code path

Exit code: 0 when at least one proxy resolves as ``audited`` AND at
least one resolves as ``unaudited_since_upgrade`` — showing both ends
of the status spectrum on real data. 1 otherwise.

Re-run safe: wipes prior ether.fi rows at the top.
"""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env")

# Route the API's SessionLocal at the test DB so TestClient reads what we
# write. Must happen BEFORE importing api. Falls back to DATABASE_URL.
TEST_DB = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
if not TEST_DB:
    sys.exit("Set TEST_DATABASE_URL or DATABASE_URL in .env")
os.environ["DATABASE_URL"] = TEST_DB
os.environ.setdefault("PSAT_ADMIN_KEY", "verification-key")


from sqlalchemy import create_engine, select  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from db.models import (  # noqa: E402
    AuditContractCoverage,
    AuditReport,
    Base,
    Contract,
    Protocol,
    UpgradeEvent,
    apply_storage_migrations,
)
from services.audits.coverage import upsert_coverage_for_protocol  # noqa: E402
from services.discovery.upgrade_history import fetch_upgrade_events  # noqa: E402

# Hand-curated ether.fi mainnet proxies. Each fetched from Etherscan for
# its actual on-chain contract_name — a wrong address surfaces loudly as
# a different-than-expected name.
KNOWN_ETHER_FI_PROXIES: list[str] = [
    "0x308861A430be4cce5502d0A12724771Fc6DaF216",  # LiquidityPool
    "0x35fA164735182de50811E8e2E824cFb9B6118ac2",  # EETH
    "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",  # WeETH
    "0x8b71140AD2e5d1E7018d2a7f8a288BD3CD38916F",  # EtherFiNodesManager
    "0x25e821b7197B146F7713C3b89B6A4D83516B912d",  # StakingManager
    "0x00C452aFFee3a17d9Cecc1Bcd2B8d5C7635C4CB9",  # AuctionManager
    "0x3d320286E014C3e1ce99Af6d6B00f0C1D63E3000",  # MembershipManager
]

LIQUIDITY_POOL = "0x308861A430be4cce5502d0A12724771Fc6DaF216"


def _banner(label: str) -> None:
    print(f"\n{'=' * 72}\n{label}\n{'=' * 72}")


@dataclass
class SeedStats:
    audits_seeded: int = 0
    audits_with_scope: int = 0
    contracts_seeded: int = 0
    upgrade_events_seeded: int = 0
    impls_resolved: int = 0
    coverage_rows: int = 0
    proxies_backfilled: int = 0
    proxy_status: dict[str, str] = field(default_factory=dict)


def _session_factory():
    # TEST_DB is validated non-None at import time (sys.exit above).
    assert TEST_DB is not None
    engine = create_engine(TEST_DB)
    Base.metadata.create_all(engine)
    apply_storage_migrations(engine)
    return sessionmaker(bind=engine, expire_on_commit=False), engine


def _wipe_prior_ether_fi(session) -> None:
    """Delete any prior ether.fi rows so the script is re-run-safe.

    ``Contract.protocol_id`` has ``ON DELETE SET NULL``, so a prior run
    that deleted the Protocol without cleaning Contracts leaves orphan
    rows that block re-insert via the ``(address, chain)`` unique
    constraint. We clean BOTH by-protocol and by-known-address to cover
    both the happy-path and the crashed-prior-run case.
    """
    protocol = session.execute(select(Protocol).where(Protocol.name == "ether.fi")).scalar_one_or_none()

    # Collect every address the script touches — known proxies + any
    # existing Contract rows attributed to ether.fi via protocol_id.
    known_addrs = {a.lower() for a in KNOWN_ETHER_FI_PROXIES}
    orphan_query = select(Contract).where(Contract.address.in_(known_addrs))
    orphans = session.execute(orphan_query).scalars().all()

    contract_ids_to_wipe: set[int] = {c.id for c in orphans}
    if protocol is not None:
        session.query(AuditContractCoverage).filter_by(protocol_id=protocol.id).delete()
        contract_ids_to_wipe.update(c.id for c in session.query(Contract).filter_by(protocol_id=protocol.id).all())

    if contract_ids_to_wipe:
        session.query(AuditContractCoverage).filter(AuditContractCoverage.contract_id.in_(contract_ids_to_wipe)).delete(
            synchronize_session=False
        )
        session.query(UpgradeEvent).filter(UpgradeEvent.contract_id.in_(contract_ids_to_wipe)).delete(
            synchronize_session=False
        )
        session.query(Contract).filter(Contract.id.in_(contract_ids_to_wipe)).delete(synchronize_session=False)
    if protocol is not None:
        session.query(AuditReport).filter_by(protocol_id=protocol.id).delete()
        session.query(Protocol).filter_by(id=protocol.id).delete()
    session.commit()


_GH_URL_RE = re.compile(r"^https?://(?:raw\.githubusercontent\.com|github\.com)/([^/]+/[^/]+)")


def _infer_source_repo(report: dict) -> str | None:
    """Prefer the already-populated source_repo; otherwise infer from URL."""
    explicit = report.get("source_repo")
    if explicit:
        return str(explicit)
    for url_key in ("pdf_url", "url", "source_url"):
        val = report.get(url_key)
        if not val:
            continue
        m = _GH_URL_RE.match(str(val))
        if m:
            return m.group(1)
    return None


def _seed_audits(session, protocol_id: int) -> tuple[int, int]:
    """Read audit_reports.json + scope_extraction_v8.json → AuditReport rows.

    Also populates ``reviewed_commits`` by regex-extracting commit SHAs from
    each audit's scope_section_text, and ``source_repo`` from the discovery
    URL when available. Both feed the source-equivalence matcher.
    """
    from services.audits.source_equivalence import extract_reviewed_commits

    ar_path = ROOT / "protocols" / "ether.fi" / "audit_reports.json"
    v8_path = ROOT / "protocols" / "ether.fi" / "scope_extraction_v8.json"
    audit_data = json.loads(ar_path.read_text())
    v8_by_url = {e["url"]: e for e in json.loads(v8_path.read_text())}

    seeded = 0
    with_scope = 0
    commits_populated = 0
    now = datetime.now(timezone.utc)
    for report in audit_data.get("reports", []):
        url = report.get("url")
        if not url:
            continue
        auditor = (report.get("auditor") or "Unknown").strip() or "Unknown"
        title = (report.get("title") or "Untitled").strip() or "Untitled"
        ar = AuditReport(
            protocol_id=protocol_id,
            url=url,
            pdf_url=report.get("pdf_url"),
            auditor=auditor,
            title=title,
            date=report.get("date"),
            confidence=report.get("confidence"),
            source_url=report.get("source_url"),
            source_repo=_infer_source_repo(report),
            text_extraction_status="success",
            text_extracted_at=now,
        )
        v8_entry = v8_by_url.get(url)
        if v8_entry and v8_entry.get("status") == "success":
            contracts = v8_entry.get("contracts") or []
            ar.scope_extraction_status = "success"
            ar.scope_extracted_at = now
            ar.scope_contracts = list(contracts)
            extracted_date = v8_entry.get("extracted_date")
            if extracted_date and (not ar.date or len(ar.date) < len(extracted_date)):
                ar.date = extracted_date
            if contracts:
                with_scope += 1
            # Pull reviewed-commit hashes from the preserved scope section
            # text — this is what the new scope-extraction pass would also
            # do during PDF processing. Both fields + scope_section_text
            # and first_chars are worth scanning — Solidified embeds its
            # commit in the title page prose, not in scope.
            signal_text = (v8_entry.get("scope_section_text") or "") + "\n" + (v8_entry.get("first_chars") or "")
            commits = extract_reviewed_commits(signal_text)
            if commits:
                ar.reviewed_commits = commits
                commits_populated += 1
        elif v8_entry and v8_entry.get("status") == "skipped":
            ar.scope_extraction_status = "skipped"
        session.add(ar)
        seeded += 1
    session.commit()
    print(f"  reviewed_commits populated on {commits_populated} audit(s)")
    return seeded, with_scope


def _resolve_contract_name(address: str) -> str | None:
    from utils.etherscan import get_contract_info

    name, _ = get_contract_info(address)
    return name


def _seed_known_proxies(session, protocol_id: int) -> int:
    """Idempotent — adopts orphan rows from prior crashed runs."""
    seeded = 0
    for addr in KNOWN_ETHER_FI_PROXIES:
        name = _resolve_contract_name(addr) or "UnknownProxy"
        lower_addr = addr.lower()
        existing = session.execute(
            select(Contract).where(Contract.address == lower_addr, Contract.chain == "ethereum")
        ).scalar_one_or_none()
        if existing is None:
            session.add(
                Contract(
                    protocol_id=protocol_id,
                    address=lower_addr,
                    chain="ethereum",
                    contract_name=name,
                    is_proxy=True,
                )
            )
        else:
            existing.protocol_id = protocol_id
            existing.contract_name = name
            existing.is_proxy = True
        print(f"  seeded proxy  {addr[:10]}… → {name}")
        seeded += 1
    session.commit()
    return seeded


def _seed_upgrade_history(
    session, protocol_id: int, proxy_address: str
) -> tuple[int, int, list[tuple[str, int, datetime | None]]]:
    """Backfill UpgradeEvent rows + impl Contract rows for one proxy.

    Returns (events_written, impls_resolved, impl_tuples) where impl_tuples
    is [(address, block_number, timestamp), ...] in chronological order —
    used for block/date spot-check assertions at the end.
    """
    proxy_address = proxy_address.lower()
    proxy_row = session.execute(
        select(Contract).where(
            Contract.protocol_id == protocol_id,
            Contract.address == proxy_address,
        )
    ).scalar_one_or_none()
    if proxy_row is None:
        raise RuntimeError(f"Proxy not seeded: {proxy_address}")

    events = fetch_upgrade_events([proxy_address])
    upgrade_events = [e for e in events if e["event_type"] == "upgraded" and e.get("implementation")]

    impl_addresses: list[str] = []
    seen: set[str] = set()
    for ev in upgrade_events:
        addr = ev["implementation"].lower()
        if addr not in seen:
            seen.add(addr)
            impl_addresses.append(addr)

    impls_resolved = 0
    latest_impl: str | None = None
    for addr in impl_addresses:
        name = _resolve_contract_name(addr)
        if name:
            impls_resolved += 1
        # Look up by (address, chain) — the unique constraint — not by
        # protocol. This is idempotent across crashed prior runs that
        # left orphan rows (protocol_id NULL after the protocol row was
        # deleted). We ADOPT such orphans into the current protocol
        # rather than try to insert a duplicate.
        existing = session.execute(
            select(Contract).where(
                Contract.address == addr,
                Contract.chain == "ethereum",
            )
        ).scalar_one_or_none()
        if existing is None:
            session.add(
                Contract(
                    protocol_id=protocol_id,
                    address=addr,
                    chain="ethereum",
                    contract_name=name or "UnknownImpl",
                    is_proxy=False,
                )
            )
        else:
            existing.protocol_id = protocol_id
            if name and not existing.contract_name:
                existing.contract_name = name
            existing.is_proxy = False
        latest_impl = addr

    impl_tuples: list[tuple[str, int, datetime | None]] = []
    written = 0
    for ev in sorted(upgrade_events, key=lambda e: e.get("block_number", 0)):
        ts_raw = ev.get("timestamp")
        ts = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc) if ts_raw else None
        session.add(
            UpgradeEvent(
                contract_id=proxy_row.id,
                proxy_address=proxy_address,
                old_impl=None,
                new_impl=ev["implementation"].lower(),
                block_number=ev.get("block_number"),
                tx_hash=ev.get("tx_hash"),
                timestamp=ts,
            )
        )
        impl_tuples.append((ev["implementation"].lower(), ev.get("block_number", 0), ts))
        written += 1

    if latest_impl:
        proxy_row.implementation = latest_impl

    session.commit()
    return written, impls_resolved, impl_tuples


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _proxy_timelines(client, session, protocol_id: int) -> dict[str, dict]:
    """Query /audit_timeline for every seeded proxy. Returns per-proxy
    summaries keyed by contract name for human reading."""
    proxies = (
        session.execute(
            select(Contract).where(
                Contract.protocol_id == protocol_id,
                Contract.is_proxy == True,  # noqa: E712
            )
        )
        .scalars()
        .all()
    )
    summaries: dict[str, dict] = {}
    for p in proxies:
        r = client.get(f"/api/contracts/{p.id}/audit_timeline").json()
        label = f"{p.contract_name}({p.address[:10]}…)"
        coverage = r.get("coverage") or []
        high_count = sum(1 for e in coverage if e["match_confidence"] == "high")
        med_count = sum(1 for e in coverage if e["match_confidence"] == "medium")
        low_count = sum(1 for e in coverage if e["match_confidence"] == "low")
        summaries[label] = {
            "contract_id": p.id,
            "status": r.get("current_status"),
            "impl_windows": len(r.get("impl_windows") or []),
            "coverage_total": len(coverage),
            "coverage_high": high_count,
            "coverage_medium": med_count,
            "coverage_low": low_count,
            "latest_audit": coverage[0] if coverage else None,
        }
    return summaries


def _spot_check_solidified_window(session, protocol_id: int) -> bool:
    """#4 — confirm the Solidified 2023-10-26 audit lands in the correct
    on-chain window on at least one LiquidityPool impl.

    Expected: the audit hits an impl whose covered_from_block has a
    timestamp <= 2023-10-26 and whose covered_to_block has a timestamp
    > 2023-10-26 (i.e. the audit is temporally inside the window).
    """
    # Solidified's audit is mirrored (Gitbook + the GitHub repo copy), so
    # the protocol has two AuditReport rows for this date. Either is fine
    # for the spot-check; they reference the same scope.
    audits = (
        session.execute(
            select(AuditReport).where(
                AuditReport.protocol_id == protocol_id,
                AuditReport.auditor == "Solidified",
                AuditReport.date == "2023-10-26",
            )
        )
        .scalars()
        .all()
    )
    if not audits:
        print("  SKIP — no Solidified 2023-10-26 audit row found")
        return False

    audit_ids = [a.id for a in audits]
    high_rows = (
        session.execute(
            select(AuditContractCoverage).where(
                AuditContractCoverage.audit_report_id.in_(audit_ids),
                AuditContractCoverage.match_confidence == "high",
            )
        )
        .scalars()
        .all()
    )
    if not high_rows:
        print("  FAIL — Solidified 2023-10-26 has no HIGH-confidence match")
        return False

    audit_ts = datetime(2023, 10, 26, tzinfo=timezone.utc)
    all_ok = True
    for r in high_rows:
        # Look up the timestamps on the UpgradeEvent rows at those blocks.
        start_ev = (
            session.execute(
                select(UpgradeEvent).where(
                    UpgradeEvent.block_number == r.covered_from_block,
                )
            )
            .scalars()
            .first()
        )
        end_ev = (
            session.execute(
                select(UpgradeEvent).where(
                    UpgradeEvent.block_number == r.covered_to_block,
                )
            )
            .scalars()
            .first()
            if r.covered_to_block
            else None
        )
        start_ts = start_ev.timestamp if start_ev else None
        end_ts = end_ev.timestamp if end_ev else None
        start_ok = start_ts is not None and start_ts <= audit_ts
        end_ok = end_ts is None or end_ts > audit_ts
        verdict = "OK" if (start_ok and end_ok) else "FAIL"
        if not (start_ok and end_ok):
            all_ok = False
        print(f"  {verdict}  impl covered_from_block={r.covered_from_block}  covered_to_block={r.covered_to_block}")
        print(
            f"        window timestamps: [{start_ts}, {end_ts or 'None'})  "
            f"audit=2023-10-26 → in-window={start_ok and end_ok}"
        )
    return all_ok


def _non_proxy_impl_timeline(client, session, protocol_id: int) -> dict | None:
    """#3 — hit the timeline endpoint on one of the LiquidityPool historical
    impl Contract rows (is_proxy=False) to exercise the non-proxy code path.
    """
    impl = (
        session.execute(
            select(Contract).where(
                Contract.protocol_id == protocol_id,
                Contract.contract_name == "LiquidityPool",
                Contract.is_proxy == False,  # noqa: E712
            )
        )
        .scalars()
        .first()
    )
    if impl is None:
        return None
    r = client.get(f"/api/contracts/{impl.id}/audit_timeline").json()
    return {
        "address": impl.address,
        "contract_id": impl.id,
        "status": r.get("current_status"),
        "impl_windows": len(r.get("impl_windows") or []),
        "coverage": len(r.get("coverage") or []),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    stats = SeedStats()
    SessionLocal, engine = _session_factory()
    session = SessionLocal()
    try:
        _banner("1. Wiping prior ether.fi rows")
        _wipe_prior_ether_fi(session)
        print("  clean slate.")

        _banner("2. Seeding Protocol + AuditReport rows")
        protocol = Protocol(name="ether.fi")
        session.add(protocol)
        session.commit()
        stats.audits_seeded, stats.audits_with_scope = _seed_audits(session, protocol.id)
        print(f"  {stats.audits_seeded} audit row(s); {stats.audits_with_scope} with scope_contracts")

        _banner("3. Seeding known ether.fi proxies")
        stats.contracts_seeded = _seed_known_proxies(session, protocol.id)

        _banner("4. Backfilling upgrade history for every seeded proxy")
        for addr in KNOWN_ETHER_FI_PROXIES:
            print(f"\n  [{addr[:10]}…]")
            try:
                written, impls_resolved, _ = _seed_upgrade_history(session, protocol.id, addr)
            except Exception as exc:
                print(f"    FAILED to backfill: {exc}")
                continue
            stats.upgrade_events_seeded += written
            stats.impls_resolved += impls_resolved
            stats.proxies_backfilled += 1
            print(f"    {written} upgrade event(s), {impls_resolved} impl(s) named")

        _banner("5. Running coverage matcher")
        stats.coverage_rows = upsert_coverage_for_protocol(session, protocol.id)
        session.commit()
        print(f"  {stats.coverage_rows} coverage row(s) inserted.")

        # In-process TestClient so the API reads our test-DB session.
        from fastapi.testclient import TestClient

        import api as api_module

        # Rebind SessionLocal to a proper factory so endpoints get a
        # fresh session that reads committed data.
        api_module.SessionLocal = SessionLocal
        client = TestClient(api_module.app)

        _banner("6. Per-proxy current_status — temporal matcher only")
        before = _proxy_timelines(client, session, protocol.id)
        for name, s in sorted(before.items(), key=lambda kv: kv[1]["status"] or ""):
            print(
                f"  {name:<46}  {s['status']:<26}  "
                f"impls={s['impl_windows']}  "
                f"cov={s['coverage_total']} (high={s['coverage_high']},"
                f"med={s['coverage_medium']},low={s['coverage_low']})"
            )
        statuses = {v["status"] for v in before.values()}
        print(f"\n  Distinct statuses observed: {sorted(x for x in statuses if x)}")

        _banner("7. Re-running matcher with verify_source_equivalence=True")
        print("  (cross-references audit reviewed_commits × GitHub ↔ Etherscan)")
        print("  ...this takes ~30-60s over GitHub + Etherscan")
        rc_total = upsert_coverage_for_protocol(session, protocol.id, verify_source_equivalence=True)
        session.commit()
        print(f"  {rc_total} coverage row(s) re-inserted.")

        _banner("8. Per-proxy current_status — after source-equivalence pass")
        after = _proxy_timelines(client, session, protocol.id)
        flipped = 0
        for name, s in sorted(after.items(), key=lambda kv: kv[1]["status"] or ""):
            prior = before.get(name, {}).get("status")
            marker = "  (FLIPPED)" if prior != s["status"] else ""
            if prior != s["status"]:
                flipped += 1
            print(f"  {name:<46}  {s['status']:<26}  impls={s['impl_windows']}  cov={s['coverage_total']}{marker}")
        statuses_after = {v["status"] for v in after.values()}
        print(f"\n  Distinct statuses observed: {sorted(x for x in statuses_after if x)}")
        print(f"  Proxies whose status flipped: {flipped}")
        stats.proxy_status = {k: v["status"] or "" for k, v in after.items()}

        # Dump the reviewed_commit rows for the headline cases.
        from db.models import AuditContractCoverage

        for name, proxy_addr in [
            ("LiquidityPool", "0x308861a430be4cce5502d0a12724771fc6daf216"),
            ("EtherFiNodesManager", "0x8b71140ad2e5d1e7018d2a7f8a288bd3cd38916f"),
        ]:
            cur_impl_row = session.execute(
                select(Contract).where(
                    Contract.protocol_id == protocol.id,
                    Contract.address.in_(
                        select(Contract.implementation).where(
                            Contract.protocol_id == protocol.id,
                            Contract.address == proxy_addr,
                        )
                    ),
                )
            ).scalar_one_or_none()
            if cur_impl_row is None:
                continue
            rc_rows = (
                session.execute(
                    select(AuditContractCoverage).where(
                        AuditContractCoverage.contract_id == cur_impl_row.id,
                        AuditContractCoverage.match_type == "reviewed_commit",
                    )
                )
                .scalars()
                .all()
            )
            print(f"\n  {name} current impl has {len(rc_rows)} reviewed_commit coverage row(s):")
            for r in rc_rows:
                a = session.get(AuditReport, r.audit_report_id)
                if a is None:
                    continue
                print(f"    → {a.date}  {a.auditor}  :: {a.title}  (matched '{r.matched_name}')")

        _banner("9. Spot-check: Solidified 2023-10-26 window boundaries")
        spot_ok = _spot_check_solidified_window(session, protocol.id)

        _banner("10. Non-proxy impl timeline (LiquidityPool impl contract)")
        np = _non_proxy_impl_timeline(client, session, protocol.id)
        if np is None:
            print("  FAIL — no LiquidityPool impl Contract row found")
            non_proxy_ok = False
        else:
            print(
                f"  address={np['address']}  contract_id={np['contract_id']}  "
                f"status={np['status']}  impl_windows={np['impl_windows']}  "
                f"coverage={np['coverage']}"
            )
            non_proxy_ok = np["status"] in {"non_proxy_audited", "non_proxy_unaudited"} and np["impl_windows"] == 0
            if not non_proxy_ok:
                print("  FAIL — unexpected shape for non-proxy timeline")

        _banner("11. Verdict")
        saw_audited = "audited" in statuses
        saw_unaudited = "unaudited_since_upgrade" in statuses
        ok = (
            stats.coverage_rows > 0
            and stats.proxies_backfilled >= 1
            and saw_audited
            and saw_unaudited
            and spot_ok
            and non_proxy_ok
        )
        print(f"  coverage rows written:              {stats.coverage_rows}")
        print(f"  proxies backfilled:                 {stats.proxies_backfilled}")
        print(f"  saw 'audited' status?               {saw_audited}")
        print(f"  saw 'unaudited_since_upgrade'?      {saw_unaudited}")
        print(f"  Solidified window spot-check:       {'PASS' if spot_ok else 'FAIL'}")
        print(f"  non-proxy timeline path:            {'PASS' if non_proxy_ok else 'FAIL'}")
        print(f"\n  {'PASS' if ok else 'FAIL'} — all 4 verification asks exercised end-to-end.")
        return 0 if ok else 1
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
