"""Live integration test for proxy upgrade monitoring against Ethereum mainnet.

Tests scan_for_upgrades and resolve_current_implementation against real proxy
contracts using a real RPC endpoint. Requires ETH_RPC environment variable.

Uses the Aave V3 Pool proxy (0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2)
which is a standard EIP-1967 proxy with multiple known upgrades on mainnet.

Run with:
    uv run pytest tests/test_proxy_watcher_live.py -v
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytestmark = pytest.mark.live

# ---------------------------------------------------------------------------
# Known mainnet proxy data
# ---------------------------------------------------------------------------

# Aave V3 Pool — EIP-1967 upgradeable proxy
AAVE_V3_POOL = "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2"

# Block 17214196: first known Upgraded event on this proxy
# tx: 0x012b6d5e9be9ae815f6d4af51adc11aed782a6979eca66f9afbf4d122245f342
KNOWN_UPGRADE_BLOCK = 17214196
KNOWN_NEW_IMPL = "0xf1cd4193bbc1ad4a23e833170f49d60f3d35a621"

# Scan a small window: 5 blocks before to 5 blocks after
SCAN_FROM = KNOWN_UPGRADE_BLOCK - 5
SCAN_TO = KNOWN_UPGRADE_BLOCK + 5


def _get_rpc_url() -> str | None:
    return os.environ.get("ETH_RPC")


_has_rpc = _get_rpc_url() is not None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_rpc, reason="ETH_RPC not set")
def test_resolve_current_implementation_mainnet():
    """resolve_current_implementation returns a valid address for Aave V3 Pool proxy."""
    from services.monitoring.proxy_watcher import resolve_current_implementation

    rpc_url = _get_rpc_url()
    impl = resolve_current_implementation(AAVE_V3_POOL, rpc_url)

    assert impl is not None, "Aave V3 Pool proxy should have an EIP-1967 implementation"
    assert impl.startswith("0x")
    assert len(impl) == 42
    assert impl != "0x" + "0" * 40


@pytest.mark.skipif(not _has_rpc, reason="ETH_RPC not set")
def test_scan_detects_known_aave_upgrade():
    """scan_for_upgrades detects a known Aave V3 Pool upgrade at block 17214196.

    Sets last_scanned_block just before the upgrade and caps get_latest_block
    to just after, so we scan only ~10 blocks via a real eth_getLogs call.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from db.models import ProxyUpgradeEvent, WatchedProxy
    from services.monitoring.proxy_watcher import scan_for_upgrades

    rpc_url = _get_rpc_url()

    engine = create_engine("sqlite:///:memory:")
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)

    session = SASession(engine, expire_on_commit=False)
    try:
        proxy = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address=AAVE_V3_POOL,
            chain="ethereum",
            label="Aave V3 Pool",
            last_known_implementation=None,
            last_scanned_block=SCAN_FROM,
        )
        session.add(proxy)
        session.commit()

        with patch(
            "services.monitoring.proxy_watcher.get_latest_block",
            return_value=SCAN_TO,
        ):
            events = scan_for_upgrades(session, rpc_url)

        assert len(events) >= 1, f"Expected at least 1 upgrade event, got {len(events)}"

        upgrade = next(
            (e for e in events if e.event_type == "upgraded"),
            None,
        )
        assert upgrade is not None, "Expected an 'upgraded' event"
        assert upgrade.block_number == KNOWN_UPGRADE_BLOCK
        assert upgrade.new_implementation.lower() == KNOWN_NEW_IMPL.lower()

        # Verify DB state was updated
        session.refresh(proxy)
        assert proxy.last_scanned_block == SCAN_TO
        assert proxy.last_known_implementation is not None
        assert proxy.last_known_implementation.lower() == KNOWN_NEW_IMPL.lower()
    finally:
        session.close()
        engine.dispose()


@pytest.mark.skipif(not _has_rpc, reason="ETH_RPC not set")
def test_scan_no_events_in_quiet_range():
    """Scanning a block range with no upgrades returns empty."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from db.models import ProxyUpgradeEvent, WatchedProxy
    from services.monitoring.proxy_watcher import scan_for_upgrades

    rpc_url = _get_rpc_url()

    engine = create_engine("sqlite:///:memory:")
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)

    session = SASession(engine, expire_on_commit=False)
    try:
        quiet_from = KNOWN_UPGRADE_BLOCK - 1010
        quiet_to = KNOWN_UPGRADE_BLOCK - 1000

        proxy = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address=AAVE_V3_POOL,
            chain="ethereum",
            label="Aave V3 Pool",
            last_known_implementation=None,
            last_scanned_block=quiet_from,
        )
        session.add(proxy)
        session.commit()

        with patch(
            "services.monitoring.proxy_watcher.get_latest_block",
            return_value=quiet_to,
        ):
            events = scan_for_upgrades(session, rpc_url)

        assert events == []

        session.refresh(proxy)
        assert proxy.last_scanned_block == quiet_to
    finally:
        session.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# USDC FiatTokenProxy — OZ legacy proxy with Upgraded(address) in data
# ---------------------------------------------------------------------------

# USDC FiatTokenProxy on Ethereum mainnet
USDC_PROXY = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

# Block 10743414: USDC upgrade to FiatTokenV2
# tx: 0xe6f0f754398d89583da8e4229c5d7aaa00739a3ae334ecfc2839ac396b4836e3
# OZ legacy format: Upgraded(address) with impl in data, not topics[1]
USDC_UPGRADE_BLOCK = 10743414
USDC_NEW_IMPL = "0xb7277a6e95992041568d9391d09d0122023778a2"

USDC_SCAN_FROM = USDC_UPGRADE_BLOCK - 5
USDC_SCAN_TO = USDC_UPGRADE_BLOCK + 5


@pytest.mark.skipif(not _has_rpc, reason="ETH_RPC not set")
def test_scan_detects_usdc_upgrade():
    """Detect a known USDC FiatTokenProxy upgrade at block 10800677.

    USDC uses an OZ legacy proxy pattern that emits Upgraded(address)
    with the implementation address in the log data field (non-indexed),
    not in topics[1]. The scanner must handle this fallback encoding.

    Sets last_scanned_block just before the upgrade and caps get_latest_block
    to just after, so we scan only ~10 blocks via a real eth_getLogs call.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from db.models import ProxyUpgradeEvent, WatchedProxy
    from services.monitoring.proxy_watcher import scan_for_upgrades

    rpc_url = _get_rpc_url()

    engine = create_engine("sqlite:///:memory:")
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)

    session = SASession(engine, expire_on_commit=False)
    try:
        proxy = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address=USDC_PROXY,
            chain="ethereum",
            label="USDC FiatTokenProxy",
            last_known_implementation=None,
            last_scanned_block=USDC_SCAN_FROM,
        )
        session.add(proxy)
        session.commit()

        with patch(
            "services.monitoring.proxy_watcher.get_latest_block",
            return_value=USDC_SCAN_TO,
        ):
            events = scan_for_upgrades(session, rpc_url)

        assert len(events) >= 1, f"Expected at least 1 upgrade event for USDC, got {len(events)}"

        upgrade = next(
            (e for e in events if e.event_type == "upgraded"),
            None,
        )
        assert upgrade is not None, "Expected an 'upgraded' event for USDC"
        assert upgrade.block_number == USDC_UPGRADE_BLOCK
        assert upgrade.new_implementation.lower() == USDC_NEW_IMPL.lower()

        session.refresh(proxy)
        assert proxy.last_scanned_block == USDC_SCAN_TO
        assert proxy.last_known_implementation is not None
    finally:
        session.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# Beacon proxy — BeaconUpgraded(address indexed beacon) at block 17001069
# ---------------------------------------------------------------------------

# Contract 0xe662bb403214a62294351514ece015efd191f632 emitted BeaconUpgraded
# at block 17001069 with beacon address 0xcb387c45185738c490c17ee6f0babfc197525d16.
# tx: 0x11116036d88e226940aa1c1eec47cccef73ea5a756c9fe91460e01a9f1e3314b
BEACON_PROXY = "0xe662bb403214a62294351514ece015efd191f632"
BEACON_UPGRADE_BLOCK = 17001069
BEACON_TX = "0x11116036d88e226940aa1c1eec47cccef73ea5a756c9fe91460e01a9f1e3314b"
BEACON_NEW_BEACON = "0xcb387c45185738c490c17ee6f0babfc197525d16"

BEACON_SCAN_FROM = BEACON_UPGRADE_BLOCK - 5
BEACON_SCAN_TO = BEACON_UPGRADE_BLOCK + 5


@pytest.mark.skipif(not _has_rpc, reason="ETH_RPC not set")
def test_scan_detects_beacon_upgrade():
    """Detect a known BeaconUpgraded event at block 17001069.

    Contract 0xe662bb403214a62294351514ece015efd191f632 emitted a
    BeaconUpgraded(address indexed beacon) event with the beacon address
    in topics[1]. The scanner should detect this as a 'beacon_upgraded'
    event and record the beacon address as new_implementation.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from db.models import ProxyUpgradeEvent, WatchedProxy
    from services.monitoring.proxy_watcher import scan_for_upgrades

    rpc_url = _get_rpc_url()

    engine = create_engine("sqlite:///:memory:")
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)

    session = SASession(engine, expire_on_commit=False)
    try:
        proxy = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address=BEACON_PROXY,
            chain="ethereum",
            label="Beacon proxy test",
            last_known_implementation=None,
            last_scanned_block=BEACON_SCAN_FROM,
        )
        session.add(proxy)
        session.commit()

        with patch(
            "services.monitoring.proxy_watcher.get_latest_block",
            return_value=BEACON_SCAN_TO,
        ):
            events = scan_for_upgrades(session, rpc_url)

        assert len(events) >= 1, f"Expected at least 1 beacon_upgraded event, got {len(events)}"

        beacon_event = next(
            (e for e in events if e.event_type == "beacon_upgraded"),
            None,
        )
        assert beacon_event is not None, "Expected a 'beacon_upgraded' event"
        assert beacon_event.block_number == BEACON_UPGRADE_BLOCK
        assert beacon_event.new_implementation.lower() == BEACON_NEW_BEACON.lower()

        session.refresh(proxy)
        assert proxy.last_scanned_block == BEACON_SCAN_TO
        assert proxy.last_known_implementation is not None
        assert proxy.last_known_implementation.lower() == BEACON_NEW_BEACON.lower()
    finally:
        session.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# AdminChanged — standard EIP-1967 event at block 17000262
# ---------------------------------------------------------------------------

# Contract 0xb4370cfed6a13874a84935aed81d19618b89145f emitted AdminChanged
# at block 17000262 with previous admin 0x0 and new admin
# 0xf0a0c2e85f4f09e021037bd68c03432a48210dda.
# tx: 0x344b978d53d4f07888ea30fb93bf7b928e2ebfa28c53e7cb63e203205f88dbbe
ADMIN_PROXY = "0xb4370cfed6a13874a84935aed81d19618b89145f"
ADMIN_CHANGE_BLOCK = 17000262
ADMIN_CHANGE_TX = "0x344b978d53d4f07888ea30fb93bf7b928e2ebfa28c53e7cb63e203205f88dbbe"
ADMIN_NEW_ADMIN = "0xf0a0c2e85f4f09e021037bd68c03432a48210dda"

ADMIN_SCAN_FROM = ADMIN_CHANGE_BLOCK - 5
ADMIN_SCAN_TO = ADMIN_CHANGE_BLOCK + 5


@pytest.mark.skipif(not _has_rpc, reason="ETH_RPC not set")
def test_scan_detects_admin_changed():
    """Detect a known AdminChanged event at block 17000262.

    Contract 0xb4370cfed6a13874a84935aed81d19618b89145f emitted an
    AdminChanged(address previousAdmin, address newAdmin) event with both
    addresses ABI-encoded in the log data field (non-indexed). The scanner
    should detect this as an 'admin_changed' event and record the new admin
    address as new_implementation.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from db.models import ProxyUpgradeEvent, WatchedProxy
    from services.monitoring.proxy_watcher import scan_for_upgrades

    rpc_url = _get_rpc_url()

    engine = create_engine("sqlite:///:memory:")
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)

    session = SASession(engine, expire_on_commit=False)
    try:
        proxy = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address=ADMIN_PROXY,
            chain="ethereum",
            label="AdminChanged proxy test",
            last_known_implementation=None,
            last_scanned_block=ADMIN_SCAN_FROM,
        )
        session.add(proxy)
        session.commit()

        with patch(
            "services.monitoring.proxy_watcher.get_latest_block",
            return_value=ADMIN_SCAN_TO,
        ):
            events = scan_for_upgrades(session, rpc_url)

        assert len(events) >= 1, f"Expected at least 1 admin_changed event, got {len(events)}"

        admin_event = next(
            (e for e in events if e.event_type == "admin_changed"),
            None,
        )
        assert admin_event is not None, "Expected an 'admin_changed' event"
        assert admin_event.block_number == ADMIN_CHANGE_BLOCK
        assert admin_event.new_implementation.lower() == ADMIN_NEW_ADMIN.lower()

        session.refresh(proxy)
        assert proxy.last_scanned_block == ADMIN_SCAN_TO
        assert proxy.last_known_implementation is not None
        assert proxy.last_known_implementation.lower() == ADMIN_NEW_ADMIN.lower()
    finally:
        session.close()
        engine.dispose()


# ===========================================================================
# UNSUPPORTED PROXY TYPES — live mainnet tests that document coverage gaps
#
# These tests point the scanner at real contracts that emitted non-EIP-1967
# upgrade events. The scanner only monitors for Upgraded, AdminChanged, and
# BeaconUpgraded topics, so it will NOT detect these events. Each test
# asserts len(events) >= 1 and is EXPECTED TO FAIL, documenting the gap.
# ===========================================================================


# ---------------------------------------------------------------------------
# Diamond proxy (EIP-2535) — DiamondCut event at block 14004881
# ---------------------------------------------------------------------------

# Contract 0x3caca7b48d0573d793d3b0279b5f0029180e83b6 emitted DiamondCut
# at block 14004881.
# topic0: 0x8faa70878671ccd212d20771b795c50af8fd3ff6cf27f4bde57e5d4de0aeb673
# tx: 0x915094b1a190e242eeb15ba6c569370bcf4d40eb9caf95a4558d54e2f59c08fc
DIAMOND_PROXY = "0x3caca7b48d0573d793d3b0279b5f0029180e83b6"
DIAMOND_CUT_BLOCK = 14004881
DIAMOND_CUT_TX = "0x915094b1a190e242eeb15ba6c569370bcf4d40eb9caf95a4558d54e2f59c08fc"

DIAMOND_SCAN_FROM = DIAMOND_CUT_BLOCK - 5
DIAMOND_SCAN_TO = DIAMOND_CUT_BLOCK + 5


@pytest.mark.skipif(not _has_rpc, reason="ETH_RPC not set")
def test_scan_detects_diamond_cut():
    """UNSUPPORTED: Diamond proxy (EIP-2535) DiamondCut event at block 14004881.

    Contract 0x3caca7b48d0573d793d3b0279b5f0029180e83b6 emitted a DiamondCut
    event (topic0 0x8faa7087...) which uses a completely different event
    signature than EIP-1967. The scanner only monitors for EIP-1967 topics
    (Upgraded, AdminChanged, BeaconUpgraded) so this event is invisible.

    This test is EXPECTED TO FAIL because the scanner does not support
    Diamond proxy events. The failure documents the coverage gap.

    To fix: add the DiamondCut topic0 to EVENT_TOPICS in upgrade_history.py
    and implement a parser for the DiamondCut ABI structure.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from db.models import ProxyUpgradeEvent, WatchedProxy
    from services.monitoring.proxy_watcher import scan_for_upgrades

    rpc_url = _get_rpc_url()

    engine = create_engine("sqlite:///:memory:")
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)

    session = SASession(engine, expire_on_commit=False)
    try:
        proxy = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address=DIAMOND_PROXY,
            chain="ethereum",
            label="Diamond proxy (EIP-2535)",
            last_known_implementation=None,
            last_scanned_block=DIAMOND_SCAN_FROM,
        )
        session.add(proxy)
        session.commit()

        with patch(
            "services.monitoring.proxy_watcher.get_latest_block",
            return_value=DIAMOND_SCAN_TO,
        ):
            events = scan_for_upgrades(session, rpc_url)

        # This SHOULD detect the DiamondCut event but currently does not
        # because the scanner does not include the DiamondCut topic0 in
        # its eth_getLogs filter. The RPC call never returns this log.
        assert len(events) >= 1, (
            f"Diamond DiamondCut event at block {DIAMOND_CUT_BLOCK} was not detected. "
            f"The scanner does not support EIP-2535 DiamondCut events."
        )
    finally:
        session.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# GnosisSafe — ChangedMasterCopy event at block 9304839
# ---------------------------------------------------------------------------

# GnosisSafe proxy 0x78ecc4ad66c9ea16821df5ef762fe021cac3fd4c emitted
# ChangedMasterCopy(address) at block 9304839.
# topic0: 0x75e41bc35ff1bf14d81d1d2f649c0084a0f974f9289c803ec9898eeec4c8d0b8
# tx: 0x472e2fed6badf9a28d522c0836c4d5cc03a7cc0ab474f469001a3adb8e531bb3
# New master copy: 0x34cfac646f301356faa8b21e94227e3583fe3f5f
GNOSIS_PROXY = "0x78ecc4ad66c9ea16821df5ef762fe021cac3fd4c"
GNOSIS_CHANGE_BLOCK = 9304839
GNOSIS_CHANGE_TX = "0x472e2fed6badf9a28d522c0836c4d5cc03a7cc0ab474f469001a3adb8e531bb3"
GNOSIS_NEW_MASTER = "0x34cfac646f301356faa8b21e94227e3583fe3f5f"

GNOSIS_SCAN_FROM = GNOSIS_CHANGE_BLOCK - 5
GNOSIS_SCAN_TO = GNOSIS_CHANGE_BLOCK + 5


@pytest.mark.skipif(not _has_rpc, reason="ETH_RPC not set")
def test_scan_detects_gnosis_master_copy_change():
    """UNSUPPORTED: GnosisSafe ChangedMasterCopy event at block 9304839.

    GnosisSafe proxy 0x78ecc4ad66c9ea16821df5ef762fe021cac3fd4c emitted
    ChangedMasterCopy(address) (topic0 0x75e41bc3...) when switching its
    singleton implementation. This is a legacy event from GnosisSafe v1.0-1.1
    that predates the EIP-1967 standard.

    This test is EXPECTED TO FAIL because the scanner only monitors EIP-1967
    event topics and does not recognize the ChangedMasterCopy signature.

    To fix: add the ChangedMasterCopy topic0 to EVENT_TOPICS and parse
    the new master copy address from the log data field.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from db.models import ProxyUpgradeEvent, WatchedProxy
    from services.monitoring.proxy_watcher import scan_for_upgrades

    rpc_url = _get_rpc_url()

    engine = create_engine("sqlite:///:memory:")
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)

    session = SASession(engine, expire_on_commit=False)
    try:
        proxy = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address=GNOSIS_PROXY,
            chain="ethereum",
            label="GnosisSafe proxy",
            last_known_implementation=None,
            last_scanned_block=GNOSIS_SCAN_FROM,
        )
        session.add(proxy)
        session.commit()

        with patch(
            "services.monitoring.proxy_watcher.get_latest_block",
            return_value=GNOSIS_SCAN_TO,
        ):
            events = scan_for_upgrades(session, rpc_url)

        # This SHOULD detect the ChangedMasterCopy event but currently does
        # not because the scanner does not include the ChangedMasterCopy
        # topic0 in its eth_getLogs filter.
        assert len(events) >= 1, (
            f"GnosisSafe ChangedMasterCopy event at block {GNOSIS_CHANGE_BLOCK} was not "
            f"detected. The scanner does not support GnosisSafe-style upgrade events."
        )
    finally:
        session.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# Compound — NewImplementation event at block 7710677
# ---------------------------------------------------------------------------

# Compound Unitroller 0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b emitted
# NewImplementation(address,address) at block 7710677.
# topic0: 0xd604de94d45953f9138079ec1b82d533cb2160c906d1076d1f7ed54befbca97a
# tx: 0xa468f4f9a9054f49bcbff1293878c5ac34ca8321c92695cace6afe264b3c36d0
# New implementation: 0x62f18c451af964197341d3c86d27e98c41bb8fcc
COMPOUND_PROXY = "0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b"
COMPOUND_UPGRADE_BLOCK = 7710677
COMPOUND_UPGRADE_TX = "0xa468f4f9a9054f49bcbff1293878c5ac34ca8321c92695cace6afe264b3c36d0"
COMPOUND_NEW_IMPL = "0x62f18c451af964197341d3c86d27e98c41bb8fcc"

COMPOUND_SCAN_FROM = COMPOUND_UPGRADE_BLOCK - 5
COMPOUND_SCAN_TO = COMPOUND_UPGRADE_BLOCK + 5


@pytest.mark.skipif(not _has_rpc, reason="ETH_RPC not set")
def test_scan_detects_compound_new_implementation():
    """UNSUPPORTED: Compound NewImplementation event at block 7710677.

    Compound Unitroller 0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b emitted
    NewImplementation(address oldImpl, address newImpl) (topic0 0xd604de94...)
    when upgrading its comptroller logic. This is Compound's proprietary
    upgrade event that predates EIP-1967.

    This test is EXPECTED TO FAIL because the scanner only monitors EIP-1967
    event topics and does not recognize the NewImplementation signature.

    To fix: add the NewImplementation topic0 to EVENT_TOPICS and parse the
    two addresses from the ABI-encoded log data (old impl, new impl).
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from db.models import ProxyUpgradeEvent, WatchedProxy
    from services.monitoring.proxy_watcher import scan_for_upgrades

    rpc_url = _get_rpc_url()

    engine = create_engine("sqlite:///:memory:")
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)

    session = SASession(engine, expire_on_commit=False)
    try:
        proxy = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address=COMPOUND_PROXY,
            chain="ethereum",
            label="Compound Unitroller",
            last_known_implementation=None,
            last_scanned_block=COMPOUND_SCAN_FROM,
        )
        session.add(proxy)
        session.commit()

        with patch(
            "services.monitoring.proxy_watcher.get_latest_block",
            return_value=COMPOUND_SCAN_TO,
        ):
            events = scan_for_upgrades(session, rpc_url)

        # This SHOULD detect the NewImplementation event but currently does
        # not because the scanner does not include the NewImplementation
        # topic0 in its eth_getLogs filter.
        assert len(events) >= 1, (
            f"Compound NewImplementation event at block {COMPOUND_UPGRADE_BLOCK} was not "
            f"detected. The scanner does not support Compound-style upgrade events."
        )
    finally:
        session.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# Synthetix — TargetUpdated(address) event at block 10203309
# ---------------------------------------------------------------------------

# SNX token proxy 0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f emitted
# TargetUpdated(address) at block 10203309.
# topic0: 0x814250a3b8c79fcbe2ead2c131c952a278491c8f4322a79fe84b5040a810373e
# tx: 0xf286b1dd8d6163e866601cf85ba1646f76a11bae86f8f323ea4a8f0ca46db4c1
# New target: 0xc6738ed1eb79fa23941c75b4f437fc65893b5476 (in data, non-indexed)
SYNTHETIX_PROXY = "0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f"
SYNTHETIX_UPGRADE_BLOCK = 10203309
SYNTHETIX_UPGRADE_TX = "0xf286b1dd8d6163e866601cf85ba1646f76a11bae86f8f323ea4a8f0ca46db4c1"
SYNTHETIX_NEW_TARGET = "0xc6738ed1eb79fa23941c75b4f437fc65893b5476"

SYNTHETIX_SCAN_FROM = SYNTHETIX_UPGRADE_BLOCK - 5
SYNTHETIX_SCAN_TO = SYNTHETIX_UPGRADE_BLOCK + 5


@pytest.mark.skipif(not _has_rpc, reason="ETH_RPC not set")
def test_scan_detects_synthetix_target_updated():
    """UNSUPPORTED: Synthetix TargetUpdated event at block 10203309.

    SNX token proxy 0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f emitted
    TargetUpdated(address) (topic0 0x81425...) when switching its target
    implementation contract. Synthetix uses a custom proxy→target pattern
    across its entire ecosystem (SNX, sUSD, sBTC, sETH, etc.).

    This test is EXPECTED TO FAIL because the scanner only monitors EIP-1967
    event topics and does not recognize the TargetUpdated signature.

    To fix: add the TargetUpdated topic0 to EVENT_TOPICS and parse the
    new target address from the ABI-encoded log data field.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from db.models import ProxyUpgradeEvent, WatchedProxy
    from services.monitoring.proxy_watcher import scan_for_upgrades

    rpc_url = _get_rpc_url()

    engine = create_engine("sqlite:///:memory:")
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)

    session = SASession(engine, expire_on_commit=False)
    try:
        proxy = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address=SYNTHETIX_PROXY,
            chain="ethereum",
            label="SNX Token Proxy",
            last_known_implementation=None,
            last_scanned_block=SYNTHETIX_SCAN_FROM,
        )
        session.add(proxy)
        session.commit()

        with patch(
            "services.monitoring.proxy_watcher.get_latest_block",
            return_value=SYNTHETIX_SCAN_TO,
        ):
            events = scan_for_upgrades(session, rpc_url)

        assert len(events) >= 1, (
            f"Synthetix TargetUpdated event at block {SYNTHETIX_UPGRADE_BLOCK} was not "
            f"detected. The scanner does not support Synthetix-style proxy events."
        )
    finally:
        session.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# Compound — NewPendingImplementation(address,address) at block 7710675
# ---------------------------------------------------------------------------

# Compound Unitroller 0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b emitted
# NewPendingImplementation(address,address) at block 7710675 — the first step
# of Compound's two-step upgrade process (_setPendingImplementation, then _acceptImplementation).
# topic0: 0xe945ccee5d701fc83f9b8aa8ca94ea4219ec1fcbd4f4cab4f0ea57c5c3e1d815
# tx: 0x7466c9dabee14b97e3600a13ade4b43b6c4b04fd660fd28453da23a522c7d7f3
COMPOUND_PENDING_BLOCK = 7710675
COMPOUND_PENDING_TX = "0x7466c9dabee14b97e3600a13ade4b43b6c4b04fd660fd28453da23a522c7d7f3"

COMPOUND_PENDING_SCAN_FROM = COMPOUND_PENDING_BLOCK - 5
COMPOUND_PENDING_SCAN_TO = COMPOUND_PENDING_BLOCK + 5


@pytest.mark.skipif(not _has_rpc, reason="ETH_RPC not set")
def test_scan_detects_compound_pending_implementation():
    """UNSUPPORTED: Compound NewPendingImplementation at block 7710675.

    Compound Unitroller 0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b emitted
    NewPendingImplementation(address oldPending, address newPending)
    (topic0 0xe945ccee...) as the first step of its two-step upgrade process.
    Two blocks later, NewImplementation is emitted to finalize the upgrade.

    This test is EXPECTED TO FAIL because the scanner does not recognize
    either Compound event signature.

    To fix: add the NewPendingImplementation topic0 to EVENT_TOPICS and parse
    the two addresses from the ABI-encoded log data.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from db.models import ProxyUpgradeEvent, WatchedProxy
    from services.monitoring.proxy_watcher import scan_for_upgrades

    rpc_url = _get_rpc_url()

    engine = create_engine("sqlite:///:memory:")
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)

    session = SASession(engine, expire_on_commit=False)
    try:
        proxy = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address=COMPOUND_PROXY,
            chain="ethereum",
            label="Compound Unitroller",
            last_known_implementation=None,
            last_scanned_block=COMPOUND_PENDING_SCAN_FROM,
        )
        session.add(proxy)
        session.commit()

        with patch(
            "services.monitoring.proxy_watcher.get_latest_block",
            return_value=COMPOUND_PENDING_SCAN_TO,
        ):
            events = scan_for_upgrades(session, rpc_url)

        assert len(events) >= 1, (
            f"Compound NewPendingImplementation event at block {COMPOUND_PENDING_BLOCK} "
            f"was not detected. The scanner does not support Compound-style events."
        )
    finally:
        session.close()
        engine.dispose()
