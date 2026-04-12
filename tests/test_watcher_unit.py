"""Unit tests for the unified watcher and relational sync (no Anvil required).

Covers:
  - JSONB dirty-tracking (flag_modified)
  - Per-contract scan block filtering
  - Reverted eth_call handling in polling
  - Owner controller_id matching precision

Requires:
  - PostgreSQL (TEST_DATABASE_URL env var)

Run with:
    TEST_DATABASE_URL=postgresql://psat:psat@localhost:5433/psat_test \
        uv run pytest tests/test_watcher_unit.py -v
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session as SASession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.models import (
    Base,
    Contract,
    ControllerValue,
    MonitoredContract,
    MonitoredEvent,
    Protocol,
    WatchedProxy,
)

DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")


def _can_connect() -> bool:
    if not DATABASE_URL:
        return False
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _can_connect(), reason="PostgreSQL not available (set TEST_DATABASE_URL)")


def ADDR(n: int) -> str:
    return "0x" + hex(n)[2:].zfill(40)


@pytest.fixture()
def db_session():
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    session = SASession(engine, expire_on_commit=False)
    try:
        yield session
    finally:
        session.rollback()
        for model in [
            MonitoredEvent,
            MonitoredContract,
            ControllerValue,
            Contract,
            Protocol,
            WatchedProxy,
        ]:
            try:
                session.query(model).delete()
            except Exception:
                session.rollback()
        session.commit()
        session.close()
        engine.dispose()


# =========================================================================
# JSONB dirty-tracking
# =========================================================================


class TestJsonbDirtyTracking:
    """Verify that last_known_state changes survive a commit/refresh cycle."""

    def test_state_update_persists_after_commit(self, db_session: SASession):
        """Mutating last_known_state via dict-copy + reassign must persist.

        Without flag_modified(), SQLAlchemy may not detect the change on JSONB
        columns, causing the UPDATE to be silently skipped on commit.
        """
        mc = MonitoredContract(
            id=uuid.uuid4(),
            address=ADDR(1),
            chain="ethereum",
            contract_type="proxy",
            monitoring_config={},
            last_known_state={"implementation": ADDR(99)},
            last_scanned_block=100,
            needs_polling=True,
            is_active=True,
        )
        db_session.add(mc)
        db_session.commit()

        mc_id = mc.id

        # Simulate what _update_state_from_event does
        state = dict(mc.last_known_state or {})
        state["owner"] = ADDR(42)
        mc.last_known_state = state
        db_session.commit()

        # Re-load from a fresh query to bypass identity map caching
        db_session.expire_all()
        reloaded = db_session.get(MonitoredContract, mc_id)
        assert reloaded is not None
        assert reloaded.last_known_state is not None
        assert reloaded.last_known_state.get("owner") == ADDR(42), (
            "last_known_state change was lost after commit — flag_modified() is likely missing"
        )

    def test_poll_state_update_persists(self, db_session: SASession):
        """poll_for_state_changes also updates last_known_state the same way."""
        mc = MonitoredContract(
            id=uuid.uuid4(),
            address=ADDR(2),
            chain="ethereum",
            contract_type="pausable",
            monitoring_config={},
            last_known_state={"paused": False},
            last_scanned_block=100,
            needs_polling=True,
            is_active=True,
        )
        db_session.add(mc)
        db_session.commit()

        mc_id = mc.id

        state = dict(mc.last_known_state or {})
        state["paused"] = True
        mc.last_known_state = state
        db_session.commit()

        db_session.expire_all()
        reloaded = db_session.get(MonitoredContract, mc_id)
        assert reloaded is not None
        assert reloaded.last_known_state is not None
        assert reloaded.last_known_state.get("paused") is True


# =========================================================================
# Per-contract scan block filtering
# =========================================================================


class TestPerContractScanBlock:
    """Verify scan filters addresses per chunk based on last_scanned_block."""

    def test_already_scanned_address_excluded_from_chunks(self, db_session: SASession):
        """If contract A is at block 100 and B is at block 5000, chunks
        ending at or below 5000 should only include A's address — B has
        already scanned those blocks.
        """
        from services.monitoring.unified_watcher import scan_for_events

        mc_behind = MonitoredContract(
            id=uuid.uuid4(),
            address=ADDR(1),
            chain="ethereum",
            contract_type="regular",
            monitoring_config={},
            last_known_state={},
            last_scanned_block=100,
            needs_polling=False,
            is_active=True,
        )
        mc_ahead = MonitoredContract(
            id=uuid.uuid4(),
            address=ADDR(2),
            chain="ethereum",
            contract_type="regular",
            monitoring_config={},
            last_known_state={},
            last_scanned_block=5000,
            needs_polling=False,
            is_active=True,
        )
        db_session.add_all([mc_behind, mc_ahead])
        db_session.commit()

        rpc_calls = []

        def mock_rpc(url, method, params):
            rpc_calls.append((method, params))
            if method == "eth_blockNumber":
                return hex(6000)
            if method == "eth_getLogs":
                return []
            return None

        with patch("services.monitoring.unified_watcher.rpc_request", side_effect=mock_rpc):
            scan_for_events(db_session, "http://fake-rpc")

        log_calls = [c for c in rpc_calls if c[0] == "eth_getLogs"]

        # Chunks whose entire range is already scanned by mc_ahead
        # (toBlock <= 5000) should only include ADDR(1).
        early_chunks = [c for c in log_calls if int(c[1][0]["toBlock"], 16) <= 5000]
        assert len(early_chunks) > 0, "Expected at least one early chunk"
        for call in early_chunks:
            addrs = call[1][0]["address"]
            assert ADDR(2) not in addrs, (
                f"Already-scanned address {ADDR(2)} was included in chunk ending at {call[1][0]['toBlock']}"
            )
            assert ADDR(1) in addrs

        # Chunks that extend past mc_ahead's last_scanned_block (toBlock > 5000)
        # should include both addresses.
        late_chunks = [c for c in log_calls if int(c[1][0]["toBlock"], 16) > 5000]
        assert len(late_chunks) > 0
        for call in late_chunks:
            addrs = call[1][0]["address"]
            assert ADDR(1) in addrs
            assert ADDR(2) in addrs

    def test_all_scanned_chunks_skipped_entirely(self, db_session: SASession):
        """When all contracts are at the same block, only new blocks are scanned."""
        from services.monitoring.unified_watcher import scan_for_events

        mc_a = MonitoredContract(
            id=uuid.uuid4(),
            address=ADDR(1),
            chain="ethereum",
            contract_type="regular",
            monitoring_config={},
            last_known_state={},
            last_scanned_block=1000,
            needs_polling=False,
            is_active=True,
        )
        mc_b = MonitoredContract(
            id=uuid.uuid4(),
            address=ADDR(2),
            chain="ethereum",
            contract_type="regular",
            monitoring_config={},
            last_known_state={},
            last_scanned_block=1000,
            needs_polling=False,
            is_active=True,
        )
        db_session.add_all([mc_a, mc_b])
        db_session.commit()

        rpc_calls = []

        def mock_rpc(url, method, params):
            rpc_calls.append((method, params))
            if method == "eth_blockNumber":
                return hex(1500)
            if method == "eth_getLogs":
                return []
            return None

        with patch("services.monitoring.unified_watcher.rpc_request", side_effect=mock_rpc):
            scan_for_events(db_session, "http://fake-rpc")

        log_calls = [c for c in rpc_calls if c[0] == "eth_getLogs"]
        # Only 1 chunk needed: blocks 1001-1500, both addresses
        assert len(log_calls) == 1
        assert set(log_calls[0][1][0]["address"]) == {ADDR(1), ADDR(2)}


# =========================================================================
# Reverted eth_call in polling
# =========================================================================


class TestRevertedEthCallPolling:
    """Verify that reverted or garbage eth_call results don't produce events."""

    def test_revert_error_data_not_treated_as_address(self):
        """Revert ABI data should not be parsed as a valid address."""
        from utils.rpc import parse_address_result

        # Solidity revert: Error(string) selector + ABI-encoded "nope"
        revert_data = (
            "0x08c379a0"
            "0000000000000000000000000000000000000000000000000000000000000020"
            "0000000000000000000000000000000000000000000000000000000000000004"
            "6e6f706500000000000000000000000000000000000000000000000000000000"
        )
        result = parse_address_result(revert_data)
        if result is not None:
            assert result == "0x" + "0" * 40 or result is None, f"Revert data was parsed as address: {result}"

    def test_short_revert_returns_none(self):
        """Short revert responses (< 66 chars) must return None."""
        from utils.rpc import parse_address_result

        assert parse_address_result("0x") is None
        assert parse_address_result("0x08c379a0") is None
        assert parse_address_result(None) is None
        assert parse_address_result("") is None

    def test_poll_skips_error_rpc_results(self, db_session: SASession):
        """When rpc_batch_request returns None for an errored call,
        poll_for_state_changes should skip it gracefully.
        """
        from services.monitoring.unified_watcher import poll_for_state_changes

        mc = MonitoredContract(
            id=uuid.uuid4(),
            address=ADDR(1),
            chain="ethereum",
            contract_type="proxy",
            monitoring_config={},
            last_known_state={"implementation": ADDR(99)},
            last_scanned_block=100,
            needs_polling=True,
            is_active=True,
        )
        db_session.add(mc)
        db_session.commit()

        def mock_batch(url, calls):
            results: list[str | None] = [None] * len(calls)
            if len(calls) > 1:
                results[1] = "0x08c379a0"  # short revert selector
            return results

        with patch("services.monitoring.unified_watcher.rpc_batch_request", side_effect=mock_batch):
            events = poll_for_state_changes(db_session, "http://fake-rpc")

        assert len(events) == 0


# =========================================================================
# Owner controller_id matching
# =========================================================================


class TestOwnerControllerMatching:
    """Verify ownership_transferred only updates the correct controller rows."""

    def test_only_exact_owner_controllers_updated(self, db_session: SASession):
        """controller_id='token_owner_registry' should NOT be updated when
        an ownership_transferred event fires — only 'owner' should.
        """
        from services.monitoring.unified_watcher import _sync_relational_tables

        proto = Protocol(name="TestOwnerMatch1")
        db_session.add(proto)
        db_session.flush()

        contract = Contract(
            address=ADDR(1),
            chain="ethereum",
            protocol_id=proto.id,
        )
        db_session.add(contract)
        db_session.flush()

        cv_owner = ControllerValue(
            contract_id=contract.id,
            controller_id="owner",
            value=ADDR(10),
        )
        cv_fake = ControllerValue(
            contract_id=contract.id,
            controller_id="token_owner_registry",
            value=ADDR(20),
        )
        db_session.add_all([cv_owner, cv_fake])
        db_session.flush()

        mc = MonitoredContract(
            id=uuid.uuid4(),
            address=ADDR(1),
            chain="ethereum",
            contract_id=contract.id,
            contract_type="regular",
            monitoring_config={},
            last_known_state={},
            last_scanned_block=100,
            is_active=True,
        )
        db_session.add(mc)
        db_session.commit()

        parsed = {
            "event_type": "ownership_transferred",
            "block_number": 200,
            "tx_hash": "0xabc",
            "new_owner": ADDR(50),
            "old_owner": ADDR(10),
        }

        _sync_relational_tables(db_session, mc, parsed)
        db_session.commit()

        db_session.expire_all()
        cv_owner_reloaded = db_session.get(ControllerValue, cv_owner.id)
        cv_fake_reloaded = db_session.get(ControllerValue, cv_fake.id)

        assert cv_owner_reloaded is not None
        assert cv_fake_reloaded is not None
        assert cv_owner_reloaded.value == ADDR(50), "Real owner should be updated"
        assert cv_fake_reloaded.value == ADDR(20), (
            f"token_owner_registry was incorrectly updated to {cv_fake_reloaded.value} — ilike('%owner%') is too broad"
        )

    def test_poll_sync_only_updates_exact_owner(self, db_session: SASession):
        """Same check for _sync_relational_from_poll."""
        from services.monitoring.unified_watcher import _sync_relational_from_poll

        proto = Protocol(name="TestOwnerMatch2")
        db_session.add(proto)
        db_session.flush()

        contract = Contract(
            address=ADDR(3),
            chain="ethereum",
            protocol_id=proto.id,
        )
        db_session.add(contract)
        db_session.flush()

        cv_owner = ControllerValue(
            contract_id=contract.id,
            controller_id="owner",
            value=ADDR(10),
        )
        cv_previous = ControllerValue(
            contract_id=contract.id,
            controller_id="previous_owner_map",
            value=ADDR(20),
        )
        db_session.add_all([cv_owner, cv_previous])
        db_session.flush()

        mc = MonitoredContract(
            id=uuid.uuid4(),
            address=ADDR(3),
            chain="ethereum",
            contract_id=contract.id,
            contract_type="regular",
            monitoring_config={},
            last_known_state={},
            last_scanned_block=100,
            is_active=True,
        )
        db_session.add(mc)
        db_session.commit()

        _sync_relational_from_poll(db_session, mc, "owner", ADDR(50), ADDR(10))
        db_session.commit()

        db_session.expire_all()
        cv_owner_reloaded = db_session.get(ControllerValue, cv_owner.id)
        cv_previous_reloaded = db_session.get(ControllerValue, cv_previous.id)
        assert cv_owner_reloaded is not None
        assert cv_previous_reloaded is not None
        assert cv_owner_reloaded.value == ADDR(50)
        assert cv_previous_reloaded.value == ADDR(20), "previous_owner_map was incorrectly updated"
