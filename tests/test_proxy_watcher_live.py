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
