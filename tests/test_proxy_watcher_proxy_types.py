"""Test proxy upgrade detection across all known proxy patterns.

Each test simulates a realistic eth_getLogs response from a specific proxy
type and runs it through the full scan_for_upgrades pipeline to verify
detection. Tests that are expected to fail (unsupported proxy types) are
marked so the results document the scanner's coverage gaps.

Proxy types tested:
  SUPPORTED:
    1. EIP-1967 TransparentProxy — Upgraded(address indexed impl)
    2. EIP-1967 UUPS — same Upgraded event
    3. EIP-1967 BeaconProxy — BeaconUpgraded(address indexed beacon)
    4. OZ Legacy (USDC-style) — Upgraded(address) with impl in data, not topics
    5. AdminChanged — addresses in data (standard)
    6. AdminChanged — addresses in indexed topics (variant)

  UNSUPPORTED (expected to produce 0 events):
    7. Diamond proxy (EIP-2535) — DiamondCut event, different topic
    8. GnosisSafe MasterCopy — ChangedMasterCopy event
    9. Custom proxy — proprietary ImplementationUpdated event
   10. Silent upgrade — SSTORE to impl slot with no event emission
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SASession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.models import ProxyUpgradeEvent, WatchedProxy
from services.discovery.upgrade_history import (
    ADMIN_CHANGED_TOPIC0,
    BEACON_UPGRADED_TOPIC0,
    UPGRADED_TOPIC0,
)
from services.monitoring.proxy_watcher import scan_for_upgrades

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PROXY_ADDR = "0x1111111111111111111111111111111111111111"
IMPL_OLD = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
IMPL_NEW = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
BEACON_ADDR = "0xcccccccccccccccccccccccccccccccccccccccc"
ADMIN_OLD = "0xdddddddddddddddddddddddddddddddddddddddd"
ADMIN_NEW = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
BLOCK_HEX = "0x100"
BLOCK_INT = 256
TX_HASH = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"


def _addr_to_topic(addr: str) -> str:
    """Pad a 20-byte address to a 32-byte topic."""
    return "0x" + "0" * 24 + addr[2:]


def _addr_to_data(addr: str) -> str:
    """ABI-encode a single address into log data."""
    return "0x" + "0" * 24 + addr[2:]


def _two_addrs_to_data(addr1: str, addr2: str) -> str:
    """ABI-encode two addresses into log data."""
    return "0x" + "0" * 24 + addr1[2:] + "0" * 24 + addr2[2:]


def _make_log(address: str, topics: list[str], data: str = "0x") -> dict:
    return {
        "address": address,
        "topics": topics,
        "data": data,
        "blockNumber": BLOCK_HEX,
        "transactionHash": TX_HASH,
        "logIndex": "0x0",
    }


@pytest.fixture
def db_session():
    """Create an in-memory SQLite DB with monitoring tables and a watched proxy."""
    engine = create_engine("sqlite:///:memory:")
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)

    session = SASession(engine, expire_on_commit=False)
    proxy = WatchedProxy(
        id=uuid.uuid4(),
        proxy_address=PROXY_ADDR,
        chain="ethereum",
        label="test",
        last_known_implementation=IMPL_OLD,
        last_scanned_block=BLOCK_INT - 10,
    )
    session.add(proxy)
    session.commit()

    yield session, proxy

    session.close()
    engine.dispose()


def _run_scan_with_logs(session, logs: list[dict]) -> list[ProxyUpgradeEvent]:
    """Run scan_for_upgrades with mocked RPC returning the given logs."""

    def mock_rpc(rpc_url, method, params):
        if method == "eth_blockNumber":
            return hex(BLOCK_INT + 5)
        if method == "eth_getLogs":
            return logs
        return None

    with patch("services.monitoring.proxy_watcher.rpc_request", side_effect=mock_rpc):
        return scan_for_upgrades(session, "http://fake-rpc")


# ===========================================================================
# SUPPORTED PROXY TYPES — these should all be detected
# ===========================================================================


class TestSupportedProxyTypes:
    """Proxy types the scanner correctly detects."""

    def test_eip1967_transparent_proxy(self, db_session):
        """EIP-1967 TransparentUpgradeableProxy: Upgraded(address indexed implementation).

        The most common modern proxy pattern. Implementation address is in topics[1].
        """
        session, proxy = db_session
        logs = [_make_log(PROXY_ADDR, [UPGRADED_TOPIC0, _addr_to_topic(IMPL_NEW)])]

        events = _run_scan_with_logs(session, logs)

        assert len(events) == 1
        assert events[0].event_type == "upgraded"
        assert events[0].new_implementation.lower() == IMPL_NEW
        assert events[0].old_implementation.lower() == IMPL_OLD
        assert events[0].block_number == BLOCK_INT

    def test_eip1967_uups_proxy(self, db_session):
        """EIP-1967 UUPS proxy: emits the same Upgraded(address indexed implementation).

        Identical event format to TransparentProxy — the upgrade logic lives
        in the implementation contract rather than the proxy, but the event
        is the same.
        """
        session, proxy = db_session
        logs = [_make_log(PROXY_ADDR, [UPGRADED_TOPIC0, _addr_to_topic(IMPL_NEW)])]

        events = _run_scan_with_logs(session, logs)

        assert len(events) == 1
        assert events[0].event_type == "upgraded"
        assert events[0].new_implementation.lower() == IMPL_NEW

    def test_eip1967_beacon_proxy(self, db_session):
        """EIP-1967 BeaconProxy: BeaconUpgraded(address indexed beacon).

        The beacon address is in topics[1]. The scanner records the beacon
        address as the new_implementation.
        """
        session, proxy = db_session
        logs = [_make_log(PROXY_ADDR, [BEACON_UPGRADED_TOPIC0, _addr_to_topic(BEACON_ADDR)])]

        events = _run_scan_with_logs(session, logs)

        assert len(events) == 1
        assert events[0].event_type == "beacon_upgraded"
        assert events[0].new_implementation.lower() == BEACON_ADDR

    def test_oz_legacy_upgraded_impl_in_data(self, db_session):
        """OZ legacy proxy (e.g. USDC FiatTokenProxy): Upgraded(address) with
        the implementation as a non-indexed parameter in log data.

        Same topic0 as EIP-1967 Upgraded, but only 1 topic (no indexed param).
        The implementation address is ABI-encoded in the data field.
        """
        session, proxy = db_session
        logs = [_make_log(PROXY_ADDR, [UPGRADED_TOPIC0], data=_addr_to_data(IMPL_NEW))]

        events = _run_scan_with_logs(session, logs)

        assert len(events) == 1
        assert events[0].event_type == "upgraded"
        assert events[0].new_implementation.lower() == IMPL_NEW

    def test_admin_changed_data(self, db_session):
        """AdminChanged(address previousAdmin, address newAdmin) — standard format.

        Both addresses are non-indexed, stored in log data.
        """
        session, proxy = db_session
        logs = [_make_log(
            PROXY_ADDR,
            [ADMIN_CHANGED_TOPIC0],
            data=_two_addrs_to_data(ADMIN_OLD, ADMIN_NEW),
        )]

        events = _run_scan_with_logs(session, logs)

        assert len(events) == 1
        assert events[0].event_type == "admin_changed"
        assert events[0].new_implementation.lower() == ADMIN_NEW

    def test_admin_changed_indexed_topics(self, db_session):
        """AdminChanged with addresses as indexed topics (variant encoding).

        Some proxy implementations index the admin addresses instead of
        putting them in data.
        """
        session, proxy = db_session
        logs = [_make_log(
            PROXY_ADDR,
            [ADMIN_CHANGED_TOPIC0, _addr_to_topic(ADMIN_OLD), _addr_to_topic(ADMIN_NEW)],
        )]

        events = _run_scan_with_logs(session, logs)

        assert len(events) == 1
        assert events[0].event_type == "admin_changed"
        assert events[0].new_implementation.lower() == ADMIN_NEW

    def test_beacon_upgraded_in_data(self, db_session):
        """BeaconUpgraded with beacon address in data instead of topics.

        Fallback encoding where the beacon is a non-indexed parameter.
        """
        session, proxy = db_session
        logs = [_make_log(
            PROXY_ADDR,
            [BEACON_UPGRADED_TOPIC0],
            data=_addr_to_data(BEACON_ADDR),
        )]

        events = _run_scan_with_logs(session, logs)

        assert len(events) == 1
        assert events[0].event_type == "beacon_upgraded"
        assert events[0].new_implementation.lower() == BEACON_ADDR


# ===========================================================================
# UNSUPPORTED PROXY TYPES — these document the scanner's coverage gaps
# ===========================================================================

# keccak256("DiamondCut((address,uint8,bytes4[])[],address,bytes)")
DIAMOND_CUT_TOPIC0 = "0x8faa70878671ccd212d20771b795c50af8fd3ff6cf27f4bde57e5d4de0aeb673"

# keccak256("ChangedMasterCopy(address)")
GNOSIS_MASTER_COPY_TOPIC0 = "0x5765cd750ece20bfa1de865bca0eebc5e72a3e9b6ee7cc9b8deed0d70253ef71"

# A made-up custom event: ImplementationUpdated(address indexed oldImpl, address indexed newImpl)
CUSTOM_IMPL_UPDATED_TOPIC0 = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"


class TestUnsupportedProxyTypes:
    """Proxy types the scanner does NOT yet detect.

    These tests assert that detection SHOULD work (len >= 1). They will
    fail until we add support. Each failure is a coverage gap to fix.
    """

    def test_diamond_proxy_eip2535(self, db_session):
        """EIP-2535 Diamond proxy: emits DiamondCut, not Upgraded.

        Diamond proxies use facets instead of a single implementation.
        The DiamondCut event has a completely different topic0.

        FIX: Add DIAMOND_CUT_TOPIC0 to the scanner's topic filter
        and a parser for the DiamondCut event structure.
        """
        session, proxy = db_session
        logs = [_make_log(
            PROXY_ADDR,
            [DIAMOND_CUT_TOPIC0],
            # DiamondCut data is complex ABI-encoded struct array — for now
            # just encode a single facet address in the first slot
            data=_addr_to_data(IMPL_NEW),
        )]

        events = _run_scan_with_logs(session, logs)

        assert len(events) >= 1
        assert events[0].new_implementation.lower() == IMPL_NEW

    def test_gnosis_safe_master_copy(self, db_session):
        """GnosisSafe: emits ChangedMasterCopy(address), not Upgraded.

        Old GnosisSafe proxies use a different event signature.

        FIX: Add GNOSIS_MASTER_COPY_TOPIC0 to the topic filter and
        parse the implementation from topics[1] or data.
        """
        session, proxy = db_session
        logs = [_make_log(
            PROXY_ADDR,
            [GNOSIS_MASTER_COPY_TOPIC0, _addr_to_topic(IMPL_NEW)],
        )]

        events = _run_scan_with_logs(session, logs)

        assert len(events) >= 1
        assert events[0].new_implementation.lower() == IMPL_NEW

    def test_custom_proprietary_event(self, db_session):
        """Custom proxy with a proprietary upgrade event name.

        Some projects define their own events like ImplementationUpdated
        or ContractUpgraded. Unrecognized topic0 is filtered out.

        FIX: No single fix — requires enumerating known custom signatures
        or falling back to storage slot polling.
        """
        session, proxy = db_session
        logs = [_make_log(
            PROXY_ADDR,
            [CUSTOM_IMPL_UPDATED_TOPIC0, _addr_to_topic(IMPL_OLD), _addr_to_topic(IMPL_NEW)],
        )]

        events = _run_scan_with_logs(session, logs)

        assert len(events) >= 1
        assert events[0].new_implementation.lower() == IMPL_NEW

    def test_silent_upgrade_no_event(self, db_session):
        """Proxy that upgrades via raw SSTORE with no event emission.

        If a proxy writes directly to the implementation slot without
        emitting any event, there is nothing for eth_getLogs to find.

        FIX: Only detectable via storage slot polling (eth_getStorageAt)
        which is O(n) per proxy per scan cycle.
        """
        session, proxy = db_session
        events = _run_scan_with_logs(session, [])

        assert len(events) >= 1

    def test_eip1167_minimal_proxy(self, db_session):
        """EIP-1167 minimal proxy (clone): immutable, never upgrades.

        These are bytecode-level clones that delegate to a fixed address.
        No upgrade mechanism exists, so there's nothing to detect.
        Not a gap — working as intended.
        """
        session, proxy = db_session
        events = _run_scan_with_logs(session, [])
        assert len(events) == 0, "EIP-1167 clones don't upgrade (expected)"
