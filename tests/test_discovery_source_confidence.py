"""Regression tests for the discovery source-confidence gate.

Two leak paths motivated this gate, and both are exercised here:

  1. ``dapp_crawl`` scrapes every ``0x...`` on a DApp page — including
     widely-held tokens (WETH, stETH) and shared infrastructure
     (OptimismPortal, EigenLayer cores) that the protocol *integrates
     with* but does not *own*. Pre-fix, those landed in the protocol's
     ``Contract`` rows tagged with its ``protocol_id``, polluting the
     surface page.

  2. ``upgrade_history`` materializes every historical implementation
     of every proxy a protocol's job analyzes. When the analyzed proxy
     is itself foreign (snuck in via path 1), the backfill multiplied
     the leak — one EigenPodManager proxy → 7 EigenPodManager impls all
     tagged etherfi.

The fix funnels ownership through ``services.discovery.source_confidence``:
only HIGH_CONFIDENCE sources may stamp ``Contract.protocol_id``. Both
the central ``bulk_upsert`` writer and the historical-impl backfill
consult ``asserts_ownership`` before assigning ownership.

These tests:
  * unit-cover the helper (boundary cases),
  * verify the writer gate at the persistence boundary,
  * verify the backfill gate against a real Postgres so the
    cross-protocol / orphan paths are exercised end-to-end,
  * include an end-to-end shape test that mirrors the EigenLayer leak.

A pre-fix build fails the writer tests (rows get ``protocol_id`` set
from low-confidence sources) and the backfill tests (orphan impls get
adopted into the protocol).
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.discovery.source_confidence import (  # noqa: E402
    HIGH_CONFIDENCE_SOURCES,
    LOW_CONFIDENCE_SOURCES,
    asserts_ownership,
)
from tests.conftest import requires_postgres  # noqa: E402

# ---------------------------------------------------------------------------
# 1. The helper itself
# ---------------------------------------------------------------------------


class TestAssertsOwnership:
    """Boundary cases for the helper that the gate consults. Cheap unit
    tests — if these regress, every downstream gate test will get noisier
    failures than they need to."""

    def test_empty_or_none_does_not_assert(self):
        assert asserts_ownership(None) is False
        assert asserts_ownership([]) is False

    def test_single_high_confidence_source_asserts(self):
        assert asserts_ownership(["deployer_expansion"]) is True
        assert asserts_ownership(["defillama"]) is True
        assert asserts_ownership(["ai_inventory"]) is True

    def test_single_low_confidence_source_does_not_assert(self):
        assert asserts_ownership(["dapp_crawl"]) is False
        assert asserts_ownership(["upgrade_history"]) is False

    def test_mixed_promotes_to_assertion(self):
        # The whole point of unioning sources — corroboration from any
        # high-confidence source promotes the row out of orphan status.
        assert asserts_ownership(["dapp_crawl", "deployer_expansion"]) is True

    def test_unknown_source_does_not_assert(self):
        # Safety default: an unknown source is treated as low-confidence
        # so newly-added sources can't accidentally start stamping
        # ``protocol_id`` before they're vetted.
        assert asserts_ownership(["some_brand_new_source"]) is False

    def test_tier_sets_are_disjoint(self):
        # Defensive: if a source ends up in both tiers, the helper's
        # behavior is ambiguous and the gate's contract starts leaking.
        assert HIGH_CONFIDENCE_SOURCES.isdisjoint(LOW_CONFIDENCE_SOURCES)


# ---------------------------------------------------------------------------
# 2. Writer-side gate — db/queue.py
# ---------------------------------------------------------------------------


pytestmark_db = [requires_postgres]


def _addr(n: int) -> str:
    return "0x" + hex(n)[2:].zfill(40)


@pytest.fixture()
def seed_protocol(db_session):
    """Fresh protocol whose contracts get cleaned up by db_session teardown."""
    from db.models import Protocol

    p = Protocol(name=f"src-conf-{uuid.uuid4().hex[:10]}")
    db_session.add(p)
    db_session.commit()
    return p.id


@requires_postgres
class TestBulkUpsertOwnershipGate:
    """Path-1 leak: dapp_crawl + similar low-confidence writers must
    create Contract rows but leave ``protocol_id`` NULL until a high-
    confidence source corroborates."""

    def test_dapp_crawl_only_entry_stays_orphan(self, db_session, seed_protocol):
        """Pre-fix: this row landed with ``protocol_id=etherfi`` — that's
        how WETH and Lido ended up "owned by" ether.fi."""
        from db.models import Contract
        from db.queue import bulk_upsert_discovered_contracts

        addr = _addr(0xDA00)
        bulk_upsert_discovered_contracts(
            db_session,
            protocol_id=seed_protocol,
            entries=[
                {
                    "address": addr,
                    "chain": "ethereum",
                    "new_sources": ["dapp_crawl"],
                    "discovery_url": "https://example.com/cash",
                }
            ],
        )
        db_session.commit()
        row = db_session.query(Contract).filter_by(address=addr, chain="ethereum").one()
        assert row.protocol_id is None, (
            "low-confidence-only entry was stamped with protocol_id — "
            "this is the dapp_crawl leak that pulled WETH/Lido into etherfi"
        )
        # Discovery trail is still preserved — the row exists, just not
        # attributed to the protocol.
        assert "dapp_crawl" in (row.discovery_sources or [])
        assert row.discovery_url == "https://example.com/cash"

    def test_high_confidence_source_does_stamp(self, db_session, seed_protocol):
        """Sanity counterpart: high-confidence sources still own the row.
        Without this assertion the gate could be a stuck-open valve and
        the other tests would still pass."""
        from db.models import Contract
        from db.queue import bulk_upsert_discovered_contracts

        addr = _addr(0xDE01)
        bulk_upsert_discovered_contracts(
            db_session,
            protocol_id=seed_protocol,
            entries=[{"address": addr, "chain": "ethereum", "new_sources": ["deployer_expansion"]}],
        )
        db_session.commit()
        row = db_session.query(Contract).filter_by(address=addr, chain="ethereum").one()
        assert row.protocol_id == seed_protocol

    def test_mixed_sources_promote_to_owned(self, db_session, seed_protocol):
        """A single high-confidence tag in a mixed source list is enough
        to assert ownership — corroboration is the whole point."""
        from db.models import Contract
        from db.queue import bulk_upsert_discovered_contracts

        addr = _addr(0xDE02)
        bulk_upsert_discovered_contracts(
            db_session,
            protocol_id=seed_protocol,
            entries=[
                {
                    "address": addr,
                    "chain": "ethereum",
                    "new_sources": ["dapp_crawl", "ai_inventory"],
                }
            ],
        )
        db_session.commit()
        row = db_session.query(Contract).filter_by(address=addr, chain="ethereum").one()
        assert row.protocol_id == seed_protocol

    def test_later_high_confidence_source_promotes_orphan(self, db_session, seed_protocol):
        """Models the real-world cycle: dapp_crawl finds an address first
        (no ownership), then a deployer_expansion run corroborates →
        the existing row gets adopted into the protocol."""
        from db.models import Contract
        from db.queue import bulk_upsert_discovered_contracts

        addr = _addr(0xDE03)
        # First pass: dapp_crawl only → orphan.
        bulk_upsert_discovered_contracts(
            db_session,
            protocol_id=seed_protocol,
            entries=[{"address": addr, "chain": "ethereum", "new_sources": ["dapp_crawl"]}],
        )
        db_session.commit()
        assert db_session.query(Contract).filter_by(address=addr).one().protocol_id is None
        # Second pass: deployer_expansion → adopted.
        bulk_upsert_discovered_contracts(
            db_session,
            protocol_id=seed_protocol,
            entries=[{"address": addr, "chain": "ethereum", "new_sources": ["deployer_expansion"]}],
        )
        db_session.commit()
        row = db_session.query(Contract).filter_by(address=addr).one()
        assert row.protocol_id == seed_protocol
        # Both sources retained — discovery history is union, not overwrite.
        assert set(row.discovery_sources or []) >= {"dapp_crawl", "deployer_expansion"}

    def test_low_confidence_update_does_not_adopt_existing_orphan(self, db_session, seed_protocol):
        """An orphan row stays orphan when only a low-confidence source
        shows up — otherwise the gate is just a slower leak."""
        from db.models import Contract
        from db.queue import bulk_upsert_discovered_contracts

        addr = _addr(0xDE04)
        # Seed an orphan directly so we know the row pre-exists.
        db_session.add(Contract(address=addr, chain="ethereum", protocol_id=None, discovery_sources=["dapp_crawl"]))
        db_session.commit()
        bulk_upsert_discovered_contracts(
            db_session,
            protocol_id=seed_protocol,
            entries=[{"address": addr, "chain": "ethereum", "new_sources": ["dapp_crawl"]}],
        )
        db_session.commit()
        row = db_session.query(Contract).filter_by(address=addr).one()
        assert row.protocol_id is None

    def test_singular_upsert_helper_is_also_gated(self, db_session, seed_protocol):
        """``upsert_discovered_contract`` (single-row variant) goes through
        the same path; gate this one too so single-shot writers don't
        bypass it."""
        from db.models import Contract
        from db.queue import upsert_discovered_contract

        addr = _addr(0xDE05)
        upsert_discovered_contract(
            db_session,
            address=addr,
            chain="ethereum",
            protocol_id=seed_protocol,
            new_sources=["dapp_crawl"],
        )
        db_session.commit()
        row = db_session.query(Contract).filter_by(address=addr).one()
        assert row.protocol_id is None


# ---------------------------------------------------------------------------
# 3. Backfill-side gate — services/discovery/upgrade_history.py
# ---------------------------------------------------------------------------


@pytest.fixture()
def stub_etherscan(monkeypatch):
    """Stub etherscan name lookup so the backfill works offline.

    Matches the helper in ``test_upgrade_history_backfill.py`` —
    duplicated here to keep this file self-contained and runnable in
    isolation.
    """
    import utils.etherscan as etherscan_mod

    def fake(address: str):
        return (f"StubImpl-{address[2:6]}", {})

    monkeypatch.setattr(etherscan_mod, "get_contract_info", fake)


@requires_postgres
class TestBackfillOwnershipGate:
    """Path-2 leak: when a low-confidence parent proxy's upgrade history
    is walked, the historical impls must not be stamped with the parent's
    protocol_id. That's the multiplier behind the EigenPodManager × 7,
    RewardsCoordinator × 6 chains in production."""

    def test_low_confidence_parent_produces_orphan_impls(self, db_session, seed_protocol, stub_etherscan):
        """Mirrors the production leak: an EigenPodManager proxy got into
        etherfi's inventory via dapp_crawl, then upgrade-history materialized
        7 historical EigenPodManager impls all tagged etherfi. With the
        gate, the impl rows still exist (so the coverage matcher has names
        to resolve) but their ``protocol_id`` is NULL."""
        from db.models import Contract
        from services.discovery.upgrade_history import backfill_historical_impl_contracts

        impl_addrs = {_addr(0xE100 + i) for i in range(3)}
        backfill_historical_impl_contracts(
            db_session,
            protocol_id=seed_protocol,
            chain="ethereum",
            impl_addrs=impl_addrs,
            parent_proxy_sources=["dapp_crawl"],  # the leaky path
        )
        db_session.commit()

        rows = db_session.query(Contract).filter(Contract.address.in_(impl_addrs)).all()
        assert len(rows) == 3, "rows should still be created — only protocol_id is gated"
        for r in rows:
            assert r.protocol_id is None, (
                f"impl {r.address} got protocol_id stamped from a low-confidence parent "
                "— this is the EigenLayer multiplier leak"
            )
            assert "upgrade_history" in (r.discovery_sources or [])

    def test_high_confidence_parent_stamps_impls(self, db_session, seed_protocol, stub_etherscan):
        """Counterpart: a proxy genuinely owned by the protocol (e.g. via
        deployer_expansion) still has its impls materialized AND owned."""
        from db.models import Contract
        from services.discovery.upgrade_history import backfill_historical_impl_contracts

        impl_addrs = {_addr(0xE200), _addr(0xE201)}
        backfill_historical_impl_contracts(
            db_session,
            protocol_id=seed_protocol,
            chain="ethereum",
            impl_addrs=impl_addrs,
            parent_proxy_sources=["deployer_expansion"],
        )
        db_session.commit()

        rows = db_session.query(Contract).filter(Contract.address.in_(impl_addrs)).all()
        assert len(rows) == 2
        for r in rows:
            assert r.protocol_id == seed_protocol

    def test_low_confidence_parent_does_not_adopt_existing_orphan(self, db_session, seed_protocol, stub_etherscan):
        """Orphan impl already exists (e.g. from another protocol's
        analysis). A low-confidence parent in OUR protocol must not pull
        it in — that would be the adoption branch of the same leak."""
        from db.models import Contract
        from services.discovery.upgrade_history import backfill_historical_impl_contracts

        addr = _addr(0xE300)
        db_session.add(
            Contract(address=addr, chain="ethereum", protocol_id=None, contract_name="ExistingOrphan", is_proxy=False)
        )
        db_session.commit()

        backfill_historical_impl_contracts(
            db_session,
            protocol_id=seed_protocol,
            chain="ethereum",
            impl_addrs={addr},
            parent_proxy_sources=["dapp_crawl"],
        )
        db_session.commit()

        row = db_session.query(Contract).filter_by(address=addr).one()
        assert row.protocol_id is None, (
            "orphan was adopted by a low-confidence parent — the upgrade-history "
            "adoption branch must respect the same ownership gate as the create branch"
        )
        # The tag still gets appended — discovery trail is preserved
        # even when adoption is blocked.
        assert "upgrade_history" in (row.discovery_sources or [])

    def test_empty_parent_sources_treated_as_low_confidence(self, db_session, seed_protocol, stub_etherscan):
        """A proxy with no discovery_sources at all is unconfirmed by
        definition. Belt-and-braces — the helper already returns False
        for None/[], this just pins the behavior at the backfill boundary."""
        from db.models import Contract
        from services.discovery.upgrade_history import backfill_historical_impl_contracts

        addrs = {_addr(0xE400)}
        backfill_historical_impl_contracts(
            db_session,
            protocol_id=seed_protocol,
            chain="ethereum",
            impl_addrs=addrs,
            parent_proxy_sources=None,
        )
        db_session.commit()
        row = db_session.query(Contract).filter_by(address=_addr(0xE400)).one()
        assert row.protocol_id is None


# ---------------------------------------------------------------------------
# 4. Analysis-job adoption gate — workers/discovery.py:_process_address
# ---------------------------------------------------------------------------


@requires_postgres
class TestAnalysisJobAdoptionGate:
    """Third leak path, caught only by live-DB observation: the static-
    stage dependency expander (and resolution) spawn discovery jobs for
    dependencies of a confirmed contract, with the parent's
    ``protocol_id`` on the request. The discovery worker's
    ``_process_address`` then stamps that ``protocol_id`` onto the
    existing Contract row — bypassing the writer gate. Real example:
    BoringGovernance (etherfi-confirmed) → calls WETH9 → static spawns a
    WETH9 analysis job → discovery._process_address adopts the orphan
    WETH9 row into etherfi. WETH9 is not an etherfi contract."""

    def test_orphan_with_only_low_confidence_sources_is_not_adopted(self, db_session, seed_protocol):
        """A WETH-shaped row exists as orphan from ``dapp_crawl``. An
        analysis job runs with ``protocol_id=etherfi``. The adoption
        path must check the orphan's discovery_sources before stamping
        ownership — only ``dapp_crawl`` → leave orphan."""
        from db.models import Contract
        from services.discovery.source_confidence import asserts_ownership

        # Seed an orphan from a low-confidence source.
        addr = _addr(0xDA51)
        db_session.add(Contract(address=addr, chain="ethereum", protocol_id=None, discovery_sources=["dapp_crawl"]))
        db_session.commit()

        # Inline the adoption check that lives in workers/discovery.py
        # — the pure logic the gate now enforces. Pinning the check at
        # the helper level keeps the test focused on the gate's intent
        # without spinning up a worker process.
        existing = db_session.query(Contract).filter_by(address=addr).one()
        should_adopt = asserts_ownership(existing.discovery_sources)
        assert should_adopt is False, (
            "asserts_ownership returned True for a dapp_crawl-only row — "
            "the analysis-job adoption gate would re-leak orphan rows"
        )

    def test_orphan_with_high_confidence_source_is_adopted(self, db_session, seed_protocol):
        """Counterpart: a contract found via deployer_expansion that
        ended up orphan for some reason should be adopted by a matching
        analysis job."""
        from db.models import Contract
        from services.discovery.source_confidence import asserts_ownership

        addr = _addr(0xDA52)
        db_session.add(
            Contract(
                address=addr,
                chain="ethereum",
                protocol_id=None,
                discovery_sources=["deployer_expansion"],
            )
        )
        db_session.commit()

        existing = db_session.query(Contract).filter_by(address=addr).one()
        assert asserts_ownership(existing.discovery_sources) is True


# ---------------------------------------------------------------------------
# 5. End-to-end shape — the EigenLayer leak in miniature
# ---------------------------------------------------------------------------


@requires_postgres
class TestEigenLayerLeakShape:
    """Reproduces the real-world shape that triggered this fix: a foreign
    proxy enters via dapp_crawl and its upgrade history adds N impls.
    Before the fix, the protocol's inventory grew by 1+N foreign rows.
    After the fix, the proxy lands orphan and so do the impls — zero
    pollution in the protocol's company-page rollup."""

    def test_dapp_crawl_proxy_plus_upgrade_history_does_not_pollute(self, db_session, seed_protocol, stub_etherscan):
        from db.models import Contract
        from db.queue import bulk_upsert_discovered_contracts
        from services.discovery.upgrade_history import backfill_historical_impl_contracts

        # Step 1: dapp_crawl pulls in an EigenLayer-shaped proxy address.
        proxy_addr = _addr(0xEEEE)
        bulk_upsert_discovered_contracts(
            db_session,
            protocol_id=seed_protocol,
            entries=[
                {
                    "address": proxy_addr,
                    "chain": "ethereum",
                    "new_sources": ["dapp_crawl"],
                    "discovery_url": "https://www.ether.fi/app/cash/referral",
                }
            ],
        )
        db_session.commit()
        proxy = db_session.query(Contract).filter_by(address=proxy_addr).one()
        assert proxy.protocol_id is None, "writer gate failed at step 1"

        # Step 2: the static worker analyzes the proxy and surfaces its
        # historical impls. With ``parent_proxy_sources`` carrying only
        # the low-confidence ``dapp_crawl`` tag, the impl backfill must
        # produce orphan rows.
        impl_addrs = {_addr(0xE001), _addr(0xE002), _addr(0xE003)}
        backfill_historical_impl_contracts(
            db_session,
            protocol_id=seed_protocol,
            chain="ethereum",
            impl_addrs=impl_addrs,
            parent_proxy_sources=proxy.discovery_sources,
        )
        db_session.commit()

        # The whole point of the fix: a company-page query keyed on
        # ``protocol_id`` returns ZERO of these rows.
        owned = db_session.query(Contract).filter_by(protocol_id=seed_protocol).all()
        leaked = [c for c in owned if c.address == proxy_addr or c.address in impl_addrs]
        assert leaked == [], (
            f"{len(leaked)} foreign row(s) stamped with protocol_id — "
            "this is the EigenLayer leak: foreign proxy via dapp_crawl + "
            "its historical impls all attributed to the protocol"
        )

        # Discovery records still exist — the data is preserved, just
        # not attributed. (Future corroboration can promote them.)
        assert db_session.query(Contract).filter_by(address=proxy_addr).count() == 1
        assert db_session.query(Contract).filter(Contract.address.in_(impl_addrs)).count() == 3
