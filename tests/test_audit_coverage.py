"""Unit tests for the audit-coverage matcher.

Focus: the *rules* — direct vs. impl_era matching, temporal window
selection, confidence downgrade on boundary misses, proxy resolution,
date parsing quirks (partial months, null dates). Integration with the
scope worker + live API is covered separately in
``test_audit_coverage_integration.py``.

Requires a real test Postgres (matcher queries UpgradeEvent / Contract /
AuditReport) — skipped cleanly on a dev box without TEST_DATABASE_URL.
Object storage is NOT required here; we never touch the scope artifact
bucket.
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.conftest import requires_postgres  # noqa: E402

pytestmark = [requires_postgres]


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def seed_protocol(db_session):
    """Fresh, unique-named Protocol + cascading cleanup."""
    from db.models import AuditContractCoverage, AuditReport, Contract, Protocol, UpgradeEvent

    name = f"cov-test-{uuid.uuid4().hex[:12]}"
    p = Protocol(name=name)
    db_session.add(p)
    db_session.commit()
    protocol_id = p.id
    try:
        yield protocol_id, name
    finally:
        # Cascade order matters: coverage refs contract+audit, upgrade
        # refs contract, so delete children first.
        db_session.query(AuditContractCoverage).filter_by(protocol_id=protocol_id).delete()
        contract_ids = [c.id for c in db_session.query(Contract).filter_by(protocol_id=protocol_id).all()]
        if contract_ids:
            db_session.query(UpgradeEvent).filter(UpgradeEvent.contract_id.in_(contract_ids)).delete(
                synchronize_session=False
            )
        db_session.query(Contract).filter_by(protocol_id=protocol_id).delete()
        db_session.query(AuditReport).filter_by(protocol_id=protocol_id).delete()
        db_session.query(Protocol).filter_by(id=protocol_id).delete()
        db_session.commit()


def _add_contract(
    session,
    protocol_id: int,
    *,
    address: str,
    name: str,
    is_proxy: bool = False,
    implementation: str | None = None,
    chain: str = "ethereum",
):
    """Create a Contract row and return it (already committed)."""
    from db.models import Contract

    c = Contract(
        protocol_id=protocol_id,
        address=address.lower(),
        contract_name=name,
        is_proxy=is_proxy,
        implementation=implementation.lower() if implementation else None,
        chain=chain,
    )
    session.add(c)
    session.commit()
    return c


def _add_audit(
    session,
    protocol_id: int,
    *,
    auditor: str = "TestFirm",
    title: str = "Audit",
    date: str | None = None,
    scope: list[str] | None = None,
    status: str | None = "success",
):
    """Create an AuditReport with scope_contracts + status='success' by default."""
    from db.models import AuditReport

    ar = AuditReport(
        protocol_id=protocol_id,
        url=f"https://example.com/{uuid.uuid4().hex}.pdf",
        auditor=auditor,
        title=title,
        date=date,
        confidence=0.9,
        scope_extraction_status=status,
        scope_contracts=scope or [],
    )
    session.add(ar)
    session.commit()
    return ar


def _add_upgrade_event(
    session,
    *,
    contract_id: int,
    proxy_address: str,
    new_impl: str,
    old_impl: str | None = None,
    block_number: int,
    timestamp: datetime | None = None,
    tx_hash: str | None = None,
):
    """Append an UpgradeEvent row on the proxy's contract_id."""
    from db.models import UpgradeEvent

    ev = UpgradeEvent(
        contract_id=contract_id,
        proxy_address=proxy_address.lower(),
        old_impl=old_impl.lower() if old_impl else None,
        new_impl=new_impl.lower(),
        block_number=block_number,
        timestamp=timestamp,
        tx_hash=tx_hash or f"0x{uuid.uuid4().hex[:64]}",
    )
    session.add(ev)
    session.commit()
    return ev


def _ts(year: int, month: int = 1, day: int = 1) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# 1. Date parser — the foundation every confidence call depends on
# ---------------------------------------------------------------------------


def test_audit_effective_ts_full_date():
    from services.audits.coverage import _audit_effective_ts

    got = _audit_effective_ts("2024-06-15")
    assert got is not None
    # End-of-day semantics so "impl replaced on 2024-06-15" matches.
    assert got.year == 2024 and got.month == 6 and got.day == 15
    assert got.hour == 23 and got.minute == 59


def test_audit_effective_ts_month_placeholder():
    from services.audits.coverage import _audit_effective_ts

    # Both the scope-extraction "YYYY-MM-00" placeholder and "YYYY-MM"
    # resolve to end of month.
    a = _audit_effective_ts("2024-06-00")
    b = _audit_effective_ts("2024-06")
    assert a == b
    assert a is not None and a.month == 6 and a.day == 30


def test_audit_effective_ts_year_only():
    from services.audits.coverage import _audit_effective_ts

    got = _audit_effective_ts("2023")
    assert got is not None
    assert got.month == 12 and got.day == 31


def test_audit_effective_ts_none_and_garbage():
    from services.audits.coverage import _audit_effective_ts

    assert _audit_effective_ts(None) is None
    assert _audit_effective_ts("") is None
    assert _audit_effective_ts("nonsense") is None


# ---------------------------------------------------------------------------
# 2. Direct match — no proxy history
# ---------------------------------------------------------------------------


def test_direct_match_high_when_audit_has_date(db_session, seed_protocol):
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    contract = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="Pool")
    audit = _add_audit(db_session, protocol_id, scope=["Pool"], date="2024-06-15")

    matches = match_contracts_for_audit(db_session, audit.id)
    assert len(matches) == 1
    m = matches[0]
    assert m.contract_id == contract.id
    assert m.match_type == "direct"
    assert m.match_confidence == "high"
    assert m.covered_from_block is None
    assert m.covered_to_block is None
    assert m.matched_name == "Pool"


def test_direct_match_medium_without_audit_date(db_session, seed_protocol):
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="Pool")
    audit = _add_audit(db_session, protocol_id, scope=["Pool"], date=None)

    matches = match_contracts_for_audit(db_session, audit.id)
    assert len(matches) == 1
    assert matches[0].match_confidence == "medium"


def test_direct_match_is_case_insensitive(db_session, seed_protocol):
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="EtherFiNodesManager")
    audit = _add_audit(db_session, protocol_id, scope=["etherfinodesmanager"], date="2024-01-01")
    assert len(match_contracts_for_audit(db_session, audit.id)) == 1


def test_scope_name_not_in_protocol_yields_zero_matches(db_session, seed_protocol):
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="Pool")
    # Name in scope doesn't exist in this protocol's contracts.
    audit = _add_audit(db_session, protocol_id, scope=["UnrelatedThing"], date="2024-01-01")
    assert match_contracts_for_audit(db_session, audit.id) == []


def test_duplicate_scope_names_collapse_to_single_match(db_session, seed_protocol):
    """Extraction glitches sometimes ship the same contract twice under
    near-identical names (e.g. 'EtherFiNodesManager' + 'EtherFiNodeManager').
    Only one coverage row should emerge for the single Contract row.
    """
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="Pool")
    audit = _add_audit(db_session, protocol_id, scope=["Pool", "pool", "POOL"], date="2024-01-01")

    matches = match_contracts_for_audit(db_session, audit.id)
    assert len(matches) == 1


# ---------------------------------------------------------------------------
# 3. impl_era match — the proxy-aware path
# ---------------------------------------------------------------------------


def test_impl_era_match_inside_window_is_high(db_session, seed_protocol):
    """Proxy was X (block 100) → Y (block 200). Audit on X dated between
    those blocks lands in X's window → high confidence, range set."""
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    proxy = _add_contract(
        db_session,
        protocol_id,
        address="0x" + "1" * 40,
        name="Proxy",
        is_proxy=True,
        implementation="0x" + "b" * 40,  # currently pointing at Y
    )
    impl_x = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="MorphoBlue")
    _add_contract(db_session, protocol_id, address="0x" + "b" * 40, name="MorphoBlueV2")

    _add_upgrade_event(
        db_session,
        contract_id=proxy.id,
        proxy_address=proxy.address,
        new_impl=impl_x.address,
        block_number=100,
        timestamp=_ts(2024, 1, 1),
    )
    _add_upgrade_event(
        db_session,
        contract_id=proxy.id,
        proxy_address=proxy.address,
        new_impl="0x" + "b" * 40,
        old_impl=impl_x.address,
        block_number=200,
        timestamp=_ts(2024, 6, 1),
    )

    audit = _add_audit(db_session, protocol_id, scope=["MorphoBlue"], date="2024-03-15")
    matches = match_contracts_for_audit(db_session, audit.id)
    assert len(matches) == 1
    m = matches[0]
    assert m.contract_id == impl_x.id
    assert m.match_type == "impl_era"
    assert m.match_confidence == "high"
    assert m.covered_from_block == 100
    assert m.covered_to_block == 200


def test_impl_era_match_open_ended_window_for_current_impl(db_session, seed_protocol):
    """An audit dated AFTER the latest upgrade covers the still-current
    impl — covered_to_block is NULL because the impl hasn't been replaced."""
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    proxy = _add_contract(
        db_session,
        protocol_id,
        address="0x" + "1" * 40,
        name="Proxy",
        is_proxy=True,
        implementation="0x" + "a" * 40,
    )
    impl_x = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="MorphoBlue")

    _add_upgrade_event(
        db_session,
        contract_id=proxy.id,
        proxy_address=proxy.address,
        new_impl=impl_x.address,
        block_number=100,
        timestamp=_ts(2024, 1, 1),
    )

    audit = _add_audit(db_session, protocol_id, scope=["MorphoBlue"], date="2024-06-01")
    [m] = match_contracts_for_audit(db_session, audit.id)
    assert m.covered_from_block == 100
    assert m.covered_to_block is None
    assert m.match_confidence == "high"


def test_impl_era_grace_window_gives_medium_confidence(db_session, seed_protocol):
    """Audit published 10 days AFTER the impl was replaced should still
    attach to that impl at 'medium' — typical 'audit finalized after
    remediation upgrade shipped' pattern."""
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    proxy = _add_contract(db_session, protocol_id, address="0x" + "1" * 40, name="Proxy", is_proxy=True)
    impl_x = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="MorphoBlue")

    _add_upgrade_event(
        db_session,
        contract_id=proxy.id,
        proxy_address=proxy.address,
        new_impl=impl_x.address,
        block_number=100,
        timestamp=_ts(2024, 1, 1),
    )
    _add_upgrade_event(
        db_session,
        contract_id=proxy.id,
        proxy_address=proxy.address,
        new_impl="0x" + "b" * 40,
        old_impl=impl_x.address,
        block_number=200,
        timestamp=_ts(2024, 6, 1),
    )

    # 10 days after replacement — within the 14-day grace.
    audit = _add_audit(db_session, protocol_id, scope=["MorphoBlue"], date="2024-06-11")
    [m] = match_contracts_for_audit(db_session, audit.id)
    assert m.match_confidence == "medium"
    assert m.contract_id == impl_x.id


def test_impl_era_far_outside_window_is_low(db_session, seed_protocol):
    """Audit dated years after the impl was replaced — name matches but
    timing is clearly off. Still emits a row (don't silently drop) but
    marks it low so a UI can hide or badge it."""
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    proxy = _add_contract(db_session, protocol_id, address="0x" + "1" * 40, name="Proxy", is_proxy=True)
    impl_x = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="MorphoBlue")
    _add_upgrade_event(
        db_session,
        contract_id=proxy.id,
        proxy_address=proxy.address,
        new_impl=impl_x.address,
        block_number=100,
        timestamp=_ts(2023, 1, 1),
    )
    _add_upgrade_event(
        db_session,
        contract_id=proxy.id,
        proxy_address=proxy.address,
        new_impl="0x" + "b" * 40,
        old_impl=impl_x.address,
        block_number=200,
        timestamp=_ts(2023, 6, 1),
    )

    audit = _add_audit(db_session, protocol_id, scope=["MorphoBlue"], date="2025-01-01")
    [m] = match_contracts_for_audit(db_session, audit.id)
    assert m.match_confidence == "low"
    assert m.contract_id == impl_x.id


def test_impl_era_with_no_audit_date_falls_to_low(db_session, seed_protocol):
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    proxy = _add_contract(db_session, protocol_id, address="0x" + "1" * 40, name="Proxy", is_proxy=True)
    impl_x = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="MorphoBlue")
    _add_upgrade_event(
        db_session,
        contract_id=proxy.id,
        proxy_address=proxy.address,
        new_impl=impl_x.address,
        block_number=100,
        timestamp=_ts(2024, 1, 1),
    )

    audit = _add_audit(db_session, protocol_id, scope=["MorphoBlue"], date=None)
    [m] = match_contracts_for_audit(db_session, audit.id)
    assert m.match_confidence == "low"
    assert m.match_type == "impl_era"


def test_impl_era_picks_correct_window_across_multiple_upgrades(db_session, seed_protocol):
    """Proxy: A (block 100) → B (200) → A (300) → C (400). Audit dated
    in the [300,400) window matches A again with THAT window, not the
    earlier [100,200) one."""
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    proxy = _add_contract(db_session, protocol_id, address="0x" + "1" * 40, name="Proxy", is_proxy=True)
    impl_a = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="ImplA")

    # A active [100, 200), then B [200, 300), then A again [300, 400), then C.
    for ts, block, new_impl, old_impl in [
        (_ts(2024, 1, 1), 100, impl_a.address, None),
        (_ts(2024, 2, 1), 200, "0x" + "b" * 40, impl_a.address),
        (_ts(2024, 3, 1), 300, impl_a.address, "0x" + "b" * 40),
        (_ts(2024, 4, 1), 400, "0x" + "c" * 40, impl_a.address),
    ]:
        _add_upgrade_event(
            db_session,
            contract_id=proxy.id,
            proxy_address=proxy.address,
            new_impl=new_impl,
            old_impl=old_impl,
            block_number=block,
            timestamp=ts,
        )

    audit = _add_audit(db_session, protocol_id, scope=["ImplA"], date="2024-03-15")
    [m] = match_contracts_for_audit(db_session, audit.id)
    # Audit lands in the SECOND active window of A: [300, 400).
    assert m.covered_from_block == 300
    assert m.covered_to_block == 400
    assert m.match_confidence == "high"


# ---------------------------------------------------------------------------
# 4. Proxy that shares a name with its impl
# ---------------------------------------------------------------------------


def test_proxy_and_impl_share_name_only_impl_gets_row(db_session, seed_protocol):
    """Proxy and impl share a scope name. Under is_proxy-aware matching,
    only the impl gets a coverage row — the proxy's direct match on its
    own name is dropped regardless of the spelling. The proxy view still
    shows coverage through ``audit_timeline``'s union over historical
    impls.
    """
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    proxy = _add_contract(
        db_session,
        protocol_id,
        address="0x" + "1" * 40,
        name="SharedName",
        is_proxy=True,
    )
    impl = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="SharedName")
    _add_upgrade_event(
        db_session,
        contract_id=proxy.id,
        proxy_address=proxy.address,
        new_impl=impl.address,
        block_number=100,
        timestamp=_ts(2024, 1, 1),
    )
    audit = _add_audit(db_session, protocol_id, scope=["SharedName"], date="2024-03-01")
    matches = match_contracts_for_audit(db_session, audit.id)
    by_id = {m.contract_id: m for m in matches}
    assert set(by_id) == {impl.id}
    assert by_id[impl.id].match_type == "impl_era"


# ---------------------------------------------------------------------------
# 4b. Proxy-aware matching — generic proxy names shouldn't create coverage
# ---------------------------------------------------------------------------


def test_proxy_direct_match_on_own_name_skipped(db_session, seed_protocol):
    """A Contract whose ``is_proxy`` is True shouldn't get a direct
    coverage row on its own scope-name match. Scope like "UUPSProxy"
    matches generic proxy Contract rows verbatim, and attributing audit
    coverage on that alone is the false-positive class we want gone.
    Coverage for the proxy, when legitimate, flows via the impl's own
    Contract row + audit_timeline's union, not via a direct match on the
    proxy name.
    """
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    # Proxy whose DB name is a generic OZ pattern. Impl name is NOT in
    # the audit scope — mirrors the KING Distributor / CumulativeMerkleDrop
    # shape where the audit reviewed core etherfi contracts but not the
    # distributor's impl.
    _add_contract(
        db_session,
        protocol_id,
        address="0x" + "a" * 40,
        name="UUPSProxy",
        is_proxy=True,
        implementation="0x" + "b" * 40,
    )
    _add_contract(
        db_session,
        protocol_id,
        address="0x" + "b" * 40,
        name="Distributor",
    )
    audit = _add_audit(
        db_session,
        protocol_id,
        scope=["UUPSProxy", "SomeOtherContract"],
        date="2024-06-01",
    )

    matches = match_contracts_for_audit(db_session, audit.id)
    # Proxy row must not emit a coverage row — its ``is_proxy`` tells us
    # the meaningful name is the impl's, and the impl's name isn't in
    # scope here. Impl row's own name ("Distributor") also isn't in
    # scope, so no coverage anywhere. Before the fix this returned a
    # direct/high row for the proxy.
    assert matches == []


def test_proxy_direct_match_skipped_but_impl_still_matches(db_session, seed_protocol):
    """The etherfi-style case: proxy named generically, impl has the
    protocol-specific name the audit actually scopes. The impl gets a
    direct coverage row on its own name; the proxy still appears
    covered in audit_timeline through the impl_era union, but the
    matcher doesn't emit a redundant direct row on the proxy's generic
    name.
    """
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    proxy = _add_contract(
        db_session,
        protocol_id,
        address="0x" + "1" * 40,
        name="UUPSProxy",
        is_proxy=True,
        implementation="0x" + "2" * 40,
    )
    impl = _add_contract(
        db_session,
        protocol_id,
        address="0x" + "2" * 40,
        name="LiquidityPool",
    )
    _add_upgrade_event(
        db_session,
        contract_id=proxy.id,
        proxy_address=proxy.address,
        new_impl=impl.address,
        block_number=100,
        timestamp=_ts(2024, 1, 1),
    )
    audit = _add_audit(
        db_session,
        protocol_id,
        scope=["UUPSProxy", "LiquidityPool"],
        date="2024-06-01",
    )

    matches = match_contracts_for_audit(db_session, audit.id)
    by_id = {m.contract_id: m for m in matches}
    # Only the impl gets a coverage row; the proxy's generic-name match
    # is dropped in favor of the impl-sourced signal.
    assert set(by_id) == {impl.id}
    assert by_id[impl.id].match_type == "impl_era"
    assert by_id[impl.id].matched_name == "LiquidityPool"


def test_non_proxy_contract_named_proxy_still_matches_directly(db_session, seed_protocol):
    """Defensive: skipping only triggers on ``is_proxy=True``. A
    protocol could have a Contract legitimately named "Proxy" or
    "UUPSProxy" that isn't actually a delegator (``is_proxy=False`` per
    the static analyzer). Those must still get a direct match — the
    rule is about the behavior flag, not the string.
    """
    from services.audits.coverage import match_contracts_for_audit

    protocol_id, _ = seed_protocol
    # Named "UUPSProxy" but classifier said is_proxy=False — treat as a
    # regular contract.
    c = _add_contract(
        db_session,
        protocol_id,
        address="0x" + "c" * 40,
        name="UUPSProxy",
        is_proxy=False,
    )
    audit = _add_audit(db_session, protocol_id, scope=["UUPSProxy"], date="2024-06-01")
    matches = match_contracts_for_audit(db_session, audit.id)
    assert len(matches) == 1
    assert matches[0].contract_id == c.id
    assert matches[0].match_type == "direct"


def test_match_audits_for_contract_skips_proxies_own_name(db_session, seed_protocol):
    """Symmetric: querying by proxy_id must not surface audits that only
    matched on the proxy's own generic name. The audit_timeline endpoint
    will still show coverage for the proxy through impl-era rows on
    historical impls — that path is separate.
    """
    from services.audits.coverage import match_audits_for_contract

    protocol_id, _ = seed_protocol
    proxy = _add_contract(
        db_session,
        protocol_id,
        address="0x" + "a" * 40,
        name="UUPSProxy",
        is_proxy=True,
        implementation="0x" + "b" * 40,
    )
    _add_audit(
        db_session,
        protocol_id,
        scope=["UUPSProxy"],
        date="2024-06-01",
    )
    assert match_audits_for_contract(db_session, proxy.id) == []


# ---------------------------------------------------------------------------
# 5. Symmetry — match_audits_for_contract returns the same thing
# ---------------------------------------------------------------------------


def test_match_audits_for_contract_is_symmetric(db_session, seed_protocol):
    from services.audits.coverage import match_audits_for_contract, match_contracts_for_audit

    protocol_id, _ = seed_protocol
    impl = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="Pool")
    audit = _add_audit(db_session, protocol_id, scope=["Pool"], date="2024-06-01")

    m_from_audit = match_contracts_for_audit(db_session, audit.id)
    m_from_contract = match_audits_for_contract(db_session, impl.id)
    assert len(m_from_audit) == 1
    assert len(m_from_contract) == 1
    assert m_from_audit[0].contract_id == m_from_contract[0].contract_id == impl.id
    assert m_from_audit[0].audit_report_id == m_from_contract[0].audit_report_id == audit.id
    assert m_from_audit[0].match_type == m_from_contract[0].match_type


def test_match_audits_for_contract_ignores_non_success_scope(db_session, seed_protocol):
    from services.audits.coverage import match_audits_for_contract

    protocol_id, _ = seed_protocol
    contract = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="Pool")
    # A failed/skipped/pending extraction shouldn't contribute coverage.
    _add_audit(db_session, protocol_id, scope=["Pool"], date="2024-06-01", status="skipped")
    _add_audit(db_session, protocol_id, scope=["Pool"], date="2024-06-01", status="failed")
    _add_audit(db_session, protocol_id, scope=None, date="2024-06-01", status=None)

    assert match_audits_for_contract(db_session, contract.id) == []


# ---------------------------------------------------------------------------
# 6. Upsert helpers — idempotency + scope re-extraction invalidation
# ---------------------------------------------------------------------------


def test_upsert_coverage_for_audit_is_idempotent(db_session, seed_protocol):
    from db.models import AuditContractCoverage
    from services.audits.coverage import upsert_coverage_for_audit

    protocol_id, _ = seed_protocol
    _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="Pool")
    audit = _add_audit(db_session, protocol_id, scope=["Pool"], date="2024-06-01")

    n1 = upsert_coverage_for_audit(db_session, audit.id)
    db_session.commit()
    n2 = upsert_coverage_for_audit(db_session, audit.id)
    db_session.commit()
    assert n1 == n2 == 1
    rows = (
        db_session.execute(select(AuditContractCoverage).where(AuditContractCoverage.audit_report_id == audit.id))
        .scalars()
        .all()
    )
    assert len(rows) == 1


def test_upsert_drops_stale_rows_after_scope_change(db_session, seed_protocol):
    """Simulates a re-extraction where scope_contracts changes from
    ['Pool'] to ['Vault']. The prior Pool coverage row must disappear;
    a fresh Vault row must appear."""
    from db.models import AuditContractCoverage, AuditReport
    from services.audits.coverage import upsert_coverage_for_audit

    protocol_id, _ = seed_protocol
    pool = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="Pool")
    vault = _add_contract(db_session, protocol_id, address="0x" + "b" * 40, name="Vault")
    audit = _add_audit(db_session, protocol_id, scope=["Pool"], date="2024-06-01")

    upsert_coverage_for_audit(db_session, audit.id)
    db_session.commit()
    rows = db_session.query(AuditContractCoverage).filter_by(audit_report_id=audit.id).all()
    assert {r.contract_id for r in rows} == {pool.id}

    # Re-extraction result: scope now says Vault instead of Pool.
    ar = db_session.get(AuditReport, audit.id)
    ar.scope_contracts = ["Vault"]
    db_session.commit()

    upsert_coverage_for_audit(db_session, audit.id)
    db_session.commit()
    rows = db_session.query(AuditContractCoverage).filter_by(audit_report_id=audit.id).all()
    assert {r.contract_id for r in rows} == {vault.id}


def test_upsert_skipped_audit_wipes_rows(db_session, seed_protocol):
    """If an audit transitions from success → skipped (e.g. a
    reextract_scope that later failed), coverage rows for it must
    clear. Otherwise stale data outlives the extraction."""
    from db.models import AuditContractCoverage, AuditReport
    from services.audits.coverage import upsert_coverage_for_audit

    protocol_id, _ = seed_protocol
    _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="Pool")
    audit = _add_audit(db_session, protocol_id, scope=["Pool"], date="2024-06-01")
    upsert_coverage_for_audit(db_session, audit.id)
    db_session.commit()
    assert db_session.query(AuditContractCoverage).filter_by(audit_report_id=audit.id).count() == 1

    ar = db_session.get(AuditReport, audit.id)
    ar.scope_extraction_status = "skipped"
    db_session.commit()

    upsert_coverage_for_audit(db_session, audit.id)
    db_session.commit()
    assert db_session.query(AuditContractCoverage).filter_by(audit_report_id=audit.id).count() == 0


def test_upsert_coverage_for_protocol_batches(db_session, seed_protocol):
    from db.models import AuditContractCoverage
    from services.audits.coverage import upsert_coverage_for_protocol

    protocol_id, _ = seed_protocol
    _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="Pool")
    _add_contract(db_session, protocol_id, address="0x" + "b" * 40, name="Vault")
    _add_audit(db_session, protocol_id, scope=["Pool"], date="2024-06-01")
    _add_audit(db_session, protocol_id, scope=["Vault"], date="2024-07-01")
    _add_audit(db_session, protocol_id, scope=["Pool", "Vault"], date="2024-08-01")

    inserted = upsert_coverage_for_protocol(db_session, protocol_id)
    db_session.commit()
    assert inserted == 4  # 1 + 1 + 2
    assert db_session.query(AuditContractCoverage).filter_by(protocol_id=protocol_id).count() == 4


def test_upsert_on_audit_with_no_matches_inserts_nothing(db_session, seed_protocol):
    from db.models import AuditContractCoverage
    from services.audits.coverage import upsert_coverage_for_audit

    protocol_id, _ = seed_protocol
    audit = _add_audit(db_session, protocol_id, scope=["NoSuchContract"], date="2024-06-01")
    assert upsert_coverage_for_audit(db_session, audit.id) == 0
    db_session.commit()
    assert db_session.query(AuditContractCoverage).filter_by(audit_report_id=audit.id).count() == 0


# ---------------------------------------------------------------------------
# 7. Reviewed-commit extraction — regex over PDF text
# ---------------------------------------------------------------------------


def test_extract_reviewed_commits_pulls_git_shas():
    from services.audits.source_equivalence import extract_reviewed_commits

    text = "Initial Commit Hash: 3b6b81b a643d24f2 7fc5100\nThe audit reviewed src/LiquidityPool.sol at commit abc1234."
    got = extract_reviewed_commits(text)
    # All four plus the 7-char abc1234 — deduped, lowercase, order preserved.
    assert got == ["3b6b81b", "a643d24f2", "7fc5100", "abc1234"]


def test_extract_reviewed_commits_filters_all_digit_and_palette_tokens():
    from services.audits.source_equivalence import extract_reviewed_commits

    # 7 digits with no hex-letters → rejected (block number / issue ID).
    # Repeated-char token (0x000000..0) → rejected.
    # Real commit → kept.
    text = "issue 1234567 placeholder 0000000 real commit deadbeefcafe01"
    assert extract_reviewed_commits(text) == ["deadbeefcafe01"]


def test_extract_reviewed_commits_empty_input_safe():
    from services.audits.source_equivalence import extract_reviewed_commits

    assert extract_reviewed_commits("") == []
    assert extract_reviewed_commits(None) == []  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 8. Source-equivalence matcher — synthetic GitHub / Etherscan stubs
# ---------------------------------------------------------------------------


def test_source_equivalence_proves_coverage_when_hashes_match(db_session, seed_protocol, monkeypatch):
    """When audit.reviewed_commits[0] has a src file whose sha matches the
    impl's Etherscan source, the upsert path upgrades the match to
    ``reviewed_commit`` / ``high`` — regardless of temporal fit.
    """
    import hashlib

    from db.models import AuditContractCoverage
    from services.audits import source_equivalence
    from services.audits.coverage import upsert_coverage_for_audit

    protocol_id, _ = seed_protocol

    # Seed an impl + an audit whose date falls OUTSIDE all temporal windows.
    # If only temporal matching applied, we'd get 'direct'/'medium' at best.
    impl = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="MyPool")
    audit = _add_audit(db_session, protocol_id, scope=["MyPool"], date="2099-01-01")
    # Manually set the source-equivalence inputs (would be populated by
    # scope extraction in production).
    audit.reviewed_commits = ["abc1234"]
    audit.source_repo = "etherfi-protocol/smart-contracts"
    db_session.commit()

    content = "contract MyPool {}"
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

    # Stub Etherscan: return a single source file whose sha matches.
    def fake_etherscan(address):
        return source_equivalence.VerifiedSource(
            contract_name="MyPool",
            compiler_version="0.8.27",
            files={"src/MyPool.sol": content_hash},
        )

    # Stub GitHub: same hash → equivalence proven.
    def fake_github(repo, commit, path, *, token=None):
        return content_hash if path == "src/MyPool.sol" else None

    monkeypatch.setattr(source_equivalence, "fetch_etherscan_source_files", fake_etherscan)
    monkeypatch.setattr(source_equivalence, "fetch_github_source_hash", fake_github)

    n = upsert_coverage_for_audit(db_session, audit.id, verify_source_equivalence=True)
    db_session.commit()
    assert n == 1

    row = db_session.query(AuditContractCoverage).filter_by(audit_report_id=audit.id).one()
    assert row.contract_id == impl.id
    assert row.match_type == "reviewed_commit"
    assert row.match_confidence == "high"


def test_source_equivalence_leaves_temporal_match_when_hashes_differ(db_session, seed_protocol, monkeypatch):
    """Source hashes don't match → match stays at its original type/confidence.
    Proof failure must never DOWNGRADE a match the temporal matcher already
    emitted.
    """
    from db.models import AuditContractCoverage
    from services.audits import source_equivalence
    from services.audits.coverage import upsert_coverage_for_audit

    protocol_id, _ = seed_protocol
    _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="MyPool")
    audit = _add_audit(db_session, protocol_id, scope=["MyPool"], date="2024-06-01")
    audit.reviewed_commits = ["abc1234"]
    audit.source_repo = "etherfi-protocol/smart-contracts"
    db_session.commit()

    # Stubs return mismatching hashes.
    def fake_etherscan(address):
        return source_equivalence.VerifiedSource(
            contract_name="MyPool", compiler_version="0.8", files={"src/MyPool.sol": "aaa"}
        )

    def fake_github(repo, commit, path, *, token=None):
        return "bbb"  # different sha

    monkeypatch.setattr(source_equivalence, "fetch_etherscan_source_files", fake_etherscan)
    monkeypatch.setattr(source_equivalence, "fetch_github_source_hash", fake_github)

    upsert_coverage_for_audit(db_session, audit.id, verify_source_equivalence=True)
    db_session.commit()
    row = db_session.query(AuditContractCoverage).filter_by(audit_report_id=audit.id).one()
    # Original temporal match stands — 'direct' because no proxy history.
    assert row.match_type == "direct"


def test_source_equivalence_prefers_db_source_files(db_session, seed_protocol, monkeypatch):
    """When a Contract's Job has SourceFile rows, source-equivalence reads
    them instead of hitting Etherscan. Saves one HTTP call per impl and
    keeps the matcher usable when Etherscan is rate-limited.
    """
    import hashlib
    import uuid as _uuid

    from db.models import AuditContractCoverage, Job, JobStage, JobStatus, SourceFile
    from services.audits import source_equivalence
    from services.audits.coverage import upsert_coverage_for_audit

    protocol_id, _ = seed_protocol
    impl = _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="MyPool")
    audit = _add_audit(db_session, protocol_id, scope=["MyPool"], date="2024-06-01")
    audit.reviewed_commits = ["abc1234"]
    audit.source_repo = "etherfi-protocol/smart-contracts"

    # Attach a Job + SourceFile so fetch_db_source_files finds content
    # without needing Etherscan.
    job = Job(id=_uuid.uuid4(), status=JobStatus.completed, stage=JobStage.done)
    db_session.add(job)
    db_session.flush()
    impl.job_id = job.id

    content = "contract MyPool {}"
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    db_session.add(SourceFile(job_id=job.id, path="src/MyPool.sol", content=content))
    db_session.commit()

    # Etherscan stub blows up loudly — if the DB path fails, we'll know.
    etherscan_calls = {"count": 0}

    def boom_etherscan(address):
        etherscan_calls["count"] += 1
        raise AssertionError("Etherscan should not be called when DB source is available")

    # GitHub stub returns the matching hash.
    def fake_github(repo, commit, path, *, token=None):
        return content_hash if path == "src/MyPool.sol" else None

    monkeypatch.setattr(source_equivalence, "fetch_etherscan_source_files", boom_etherscan)
    monkeypatch.setattr(source_equivalence, "fetch_github_source_hash", fake_github)

    upsert_coverage_for_audit(db_session, audit.id, verify_source_equivalence=True)
    db_session.commit()

    assert etherscan_calls["count"] == 0, "DB path must short-circuit Etherscan"
    row = db_session.query(AuditContractCoverage).filter_by(audit_report_id=audit.id).one()
    assert row.match_type == "reviewed_commit"
    assert row.match_confidence == "high"

    # Cleanup — the autouse teardown doesn't know about the Job we created.
    db_session.query(SourceFile).filter_by(job_id=job.id).delete()
    # Detach the job_id from the contract so the contract teardown doesn't
    # cascade-delete a job that other tests may depend on (Job has no FK
    # back here but keeping clean is nice).
    impl.job_id = None
    db_session.commit()
    db_session.query(Job).filter_by(id=job.id).delete()
    db_session.commit()


def test_source_equivalence_falls_back_to_etherscan_when_no_db_source(db_session, seed_protocol, monkeypatch):
    """Contract has no Job → no SourceFile rows → DB path returns None →
    matcher falls through to Etherscan. Proves the fallback is wired,
    not just a happy-path optimization.
    """
    import hashlib

    from db.models import AuditContractCoverage
    from services.audits import source_equivalence
    from services.audits.coverage import upsert_coverage_for_audit

    protocol_id, _ = seed_protocol
    _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="MyPool")
    audit = _add_audit(db_session, protocol_id, scope=["MyPool"], date="2024-06-01")
    audit.reviewed_commits = ["abc1234"]
    audit.source_repo = "etherfi-protocol/smart-contracts"
    db_session.commit()

    content = "contract MyPool {}"
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

    etherscan_calls = {"count": 0}

    def fake_etherscan(address):
        etherscan_calls["count"] += 1
        return source_equivalence.VerifiedSource(
            contract_name="MyPool",
            compiler_version="0.8",
            files={"src/MyPool.sol": content_hash},
        )

    def fake_github(repo, commit, path, *, token=None):
        return content_hash if path == "src/MyPool.sol" else None

    monkeypatch.setattr(source_equivalence, "fetch_etherscan_source_files", fake_etherscan)
    monkeypatch.setattr(source_equivalence, "fetch_github_source_hash", fake_github)

    upsert_coverage_for_audit(db_session, audit.id, verify_source_equivalence=True)
    db_session.commit()

    assert etherscan_calls["count"] == 1, "Etherscan must be the fallback when DB is empty"
    row = db_session.query(AuditContractCoverage).filter_by(audit_report_id=audit.id).one()
    assert row.match_type == "reviewed_commit"


def test_source_equivalence_skipped_when_audit_missing_commits(db_session, seed_protocol, monkeypatch):
    """No reviewed_commits populated → equivalence short-circuits without
    any HTTP calls. Protects us from a config error pounding GitHub."""
    from services.audits import source_equivalence
    from services.audits.coverage import upsert_coverage_for_audit

    protocol_id, _ = seed_protocol
    _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="MyPool")
    audit = _add_audit(db_session, protocol_id, scope=["MyPool"], date="2024-06-01")
    # reviewed_commits is None (not set).
    db_session.commit()

    called = {"etherscan": 0, "github": 0}

    def boom_etherscan(address):
        called["etherscan"] += 1
        return None

    def boom_github(*args, **kwargs):
        called["github"] += 1
        return None

    monkeypatch.setattr(source_equivalence, "fetch_etherscan_source_files", boom_etherscan)
    monkeypatch.setattr(source_equivalence, "fetch_github_source_hash", boom_github)

    upsert_coverage_for_audit(db_session, audit.id, verify_source_equivalence=True)
    db_session.commit()
    assert called == {"etherscan": 0, "github": 0}


def test_verify_source_equivalence_off_by_default(db_session, seed_protocol, monkeypatch):
    """verify_source_equivalence=False (the default) must not reach GitHub /
    Etherscan at all, even when reviewed_commits is populated."""
    from services.audits import source_equivalence
    from services.audits.coverage import upsert_coverage_for_audit

    protocol_id, _ = seed_protocol
    _add_contract(db_session, protocol_id, address="0x" + "a" * 40, name="MyPool")
    audit = _add_audit(db_session, protocol_id, scope=["MyPool"], date="2024-06-01")
    audit.reviewed_commits = ["abc1234"]
    audit.source_repo = "etherfi-protocol/smart-contracts"
    db_session.commit()

    called = {"etherscan": 0, "github": 0}

    def boom_etherscan(address):
        called["etherscan"] += 1
        return None

    def boom_github(*args, **kwargs):
        called["github"] += 1
        return None

    monkeypatch.setattr(source_equivalence, "fetch_etherscan_source_files", boom_etherscan)
    monkeypatch.setattr(source_equivalence, "fetch_github_source_hash", boom_github)

    upsert_coverage_for_audit(db_session, audit.id)
    db_session.commit()
    assert called == {"etherscan": 0, "github": 0}
