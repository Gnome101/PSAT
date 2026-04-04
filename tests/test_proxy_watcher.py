"""Unit and integration tests for the proxy upgrade monitoring scanner.

Tests cover:
  - scan_for_upgrades with empty DB, no new blocks, event detection
  - Single eth_getLogs call for multiple proxies
  - Block range chunking for large ranges
  - Filtering of unrelated events
  - Idempotent scans (last_scanned_block advances)
  - resolve_current_implementation with real and empty slots

All tests run without live services (no RPC, no database).
Uses an in-memory SQLite database for WatchedProxy / ProxyUpgradeEvent rows.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from unittest.mock import patch, call

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.models import Base, ProxyUpgradeEvent, WatchedProxy
from services.discovery.upgrade_history import (
    ADMIN_CHANGED_TOPIC0,
    BEACON_UPGRADED_TOPIC0,
    UPGRADED_TOPIC0,
)
from services.monitoring.proxy_watcher import (
    MAX_BLOCK_RANGE,
    resolve_current_implementation,
    scan_for_upgrades,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EIP1967_IMPL_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"


def ADDR(n: int) -> str:
    """Generate a deterministic 0x-prefixed address from an integer."""
    return "0x" + hex(n)[2:].zfill(40)


def _topic_for(addr: str) -> str:
    """Pad a 20-byte address to a 32-byte topic."""
    return "0x" + "0" * 24 + addr[2:]


def _admin_data(old: str, new: str) -> str:
    """ABI-encode two addresses as data for AdminChanged events."""
    return "0x" + "0" * 24 + old[2:] + "0" * 24 + new[2:]


def _make_log(
    address: str,
    topic0: str,
    topic1: str | None = None,
    data: str = "0x",
    block: str = "0x64",
    tx: str = "0xaaa",
    log_index: str = "0x0",
    timestamp: str = "0x65a00000",
) -> dict:
    """Build a mock eth_getLogs result entry."""
    return {
        "address": address,
        "topics": [topic0] + ([topic1] if topic1 else []),
        "data": data,
        "blockNumber": block,
        "transactionHash": tx,
        "logIndex": log_index,
        "timeStamp": timestamp,
    }


# ---------------------------------------------------------------------------
# Fixtures — in-memory SQLite database
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_session():
    """Create an in-memory SQLite database with only the monitoring tables,
    yield a session, then tear down.

    We only create the WatchedProxy and ProxyUpgradeEvent tables (not Job,
    Artifact, etc.) because the Job model uses PostgreSQL-specific JSONB
    columns that SQLite cannot handle.
    """
    engine = create_engine("sqlite:///:memory:")

    # SQLite needs foreign key enforcement turned on explicitly
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    # Only create the tables we need for proxy monitoring tests
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)

    session = Session(engine, expire_on_commit=False)
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def _add_proxy(
    session: Session,
    address: str,
    chain: str = "ethereum",
    label: str | None = None,
    last_known_impl: str | None = None,
    last_scanned_block: int = 0,
) -> WatchedProxy:
    """Insert a WatchedProxy row and return it."""
    proxy = WatchedProxy(
        id=uuid.uuid4(),
        proxy_address=address,
        chain=chain,
        label=label,
        last_known_implementation=last_known_impl,
        last_scanned_block=last_scanned_block,
    )
    session.add(proxy)
    session.commit()
    return proxy


# ---------------------------------------------------------------------------
# 1. test_scan_no_proxies
# ---------------------------------------------------------------------------


@patch("services.monitoring.proxy_watcher.rpc_request")
def test_scan_no_proxies(mock_rpc, db_session):
    """scan_for_upgrades with empty DB returns empty list, no RPC calls."""
    result = scan_for_upgrades(db_session, "http://localhost:8545")
    assert result == []
    mock_rpc.assert_not_called()


# ---------------------------------------------------------------------------
# 2. test_scan_no_new_blocks
# ---------------------------------------------------------------------------


@patch("services.monitoring.proxy_watcher.rpc_request")
def test_scan_no_new_blocks(mock_rpc, db_session):
    """When last_scanned_block >= latest block, returns empty (no getLogs calls)."""
    _add_proxy(db_session, ADDR(1), last_scanned_block=100)

    # get_latest_block returns 100 (same as last_scanned_block)
    mock_rpc.return_value = hex(100)

    result = scan_for_upgrades(db_session, "http://localhost:8545")
    assert result == []
    # Only one RPC call: eth_blockNumber
    assert mock_rpc.call_count == 1
    mock_rpc.assert_called_once_with("http://localhost:8545", "eth_blockNumber", [])


# ---------------------------------------------------------------------------
# 3. test_scan_detects_upgrade
# ---------------------------------------------------------------------------


@patch("services.monitoring.proxy_watcher.rpc_request")
def test_scan_detects_upgrade(mock_rpc, db_session):
    """Mock eth_getLogs returning an Upgraded event; verify ProxyUpgradeEvent
    is created, WatchedProxy.last_known_implementation is updated, and
    last_scanned_block advances."""
    proxy_addr = ADDR(1)
    old_impl = ADDR(10)
    new_impl = ADDR(11)
    proxy = _add_proxy(db_session, proxy_addr, last_known_impl=old_impl, last_scanned_block=90)

    latest_block = 100
    log = _make_log(
        proxy_addr,
        UPGRADED_TOPIC0,
        _topic_for(new_impl),
        block=hex(95),
        tx="0xdeadbeef" + "0" * 58,
    )

    def rpc_side_effect(url, method, params):
        if method == "eth_blockNumber":
            return hex(latest_block)
        if method == "eth_getLogs":
            return [log]
        return None

    mock_rpc.side_effect = rpc_side_effect

    events = scan_for_upgrades(db_session, "http://localhost:8545")

    assert len(events) == 1
    evt = events[0]
    assert evt.block_number == 95
    assert evt.old_implementation == old_impl
    assert evt.new_implementation == new_impl
    assert evt.event_type == "upgraded"
    assert evt.watched_proxy_id == proxy.id

    # WatchedProxy should be updated
    db_session.refresh(proxy)
    assert proxy.last_known_implementation == new_impl
    assert proxy.last_scanned_block == latest_block


# ---------------------------------------------------------------------------
# 4. test_scan_detects_admin_changed
# ---------------------------------------------------------------------------


@patch("services.monitoring.proxy_watcher.rpc_request")
def test_scan_detects_admin_changed(mock_rpc, db_session):
    """AdminChanged event is detected and recorded."""
    proxy_addr = ADDR(2)
    old_admin = ADDR(50)
    new_admin = ADDR(51)
    proxy = _add_proxy(db_session, proxy_addr, last_known_impl=old_admin, last_scanned_block=90)

    log = _make_log(
        proxy_addr,
        ADMIN_CHANGED_TOPIC0,
        data=_admin_data(old_admin, new_admin),
        block=hex(95),
        tx="0xadmin" + "0" * 58,
    )

    def rpc_side_effect(url, method, params):
        if method == "eth_blockNumber":
            return hex(100)
        if method == "eth_getLogs":
            return [log]
        return None

    mock_rpc.side_effect = rpc_side_effect

    events = scan_for_upgrades(db_session, "http://localhost:8545")

    assert len(events) == 1
    evt = events[0]
    assert evt.event_type == "admin_changed"
    assert evt.new_implementation == new_admin
    assert evt.old_implementation == old_admin
    assert evt.watched_proxy_id == proxy.id


# ---------------------------------------------------------------------------
# 5. test_scan_detects_beacon_upgraded
# ---------------------------------------------------------------------------


@patch("services.monitoring.proxy_watcher.rpc_request")
def test_scan_detects_beacon_upgraded(mock_rpc, db_session):
    """BeaconUpgraded event is detected and recorded."""
    proxy_addr = ADDR(3)
    old_beacon = ADDR(90)
    new_beacon = ADDR(99)
    proxy = _add_proxy(db_session, proxy_addr, last_known_impl=old_beacon, last_scanned_block=90)

    log = _make_log(
        proxy_addr,
        BEACON_UPGRADED_TOPIC0,
        _topic_for(new_beacon),
        block=hex(95),
        tx="0xbeacon" + "0" * 56,
    )

    def rpc_side_effect(url, method, params):
        if method == "eth_blockNumber":
            return hex(100)
        if method == "eth_getLogs":
            return [log]
        return None

    mock_rpc.side_effect = rpc_side_effect

    events = scan_for_upgrades(db_session, "http://localhost:8545")

    assert len(events) == 1
    evt = events[0]
    assert evt.event_type == "beacon_upgraded"
    assert evt.new_implementation == new_beacon
    assert evt.old_implementation == old_beacon
    assert evt.watched_proxy_id == proxy.id


# ---------------------------------------------------------------------------
# 6. test_scan_multiple_proxies_single_call
# ---------------------------------------------------------------------------


@patch("services.monitoring.proxy_watcher.rpc_request")
def test_scan_multiple_proxies_single_call(mock_rpc, db_session):
    """Two proxies, verify a single eth_getLogs call covers both
    (check mock was called once for eth_getLogs, not once per proxy)."""
    proxy_a = _add_proxy(db_session, ADDR(1), last_scanned_block=90)
    proxy_b = _add_proxy(db_session, ADDR(2), last_scanned_block=90)
    latest_block = 100

    log_a = _make_log(ADDR(1), UPGRADED_TOPIC0, _topic_for(ADDR(11)), block=hex(95), tx="0xa" + "0" * 63)
    log_b = _make_log(ADDR(2), UPGRADED_TOPIC0, _topic_for(ADDR(22)), block=hex(97), tx="0xb" + "0" * 63)

    get_logs_call_count = 0

    def rpc_side_effect(url, method, params):
        nonlocal get_logs_call_count
        if method == "eth_blockNumber":
            return hex(latest_block)
        if method == "eth_getLogs":
            get_logs_call_count += 1
            return [log_a, log_b]
        return None

    mock_rpc.side_effect = rpc_side_effect

    events = scan_for_upgrades(db_session, "http://localhost:8545")

    assert len(events) == 2
    # Only one eth_getLogs call for the whole block range (range is < MAX_BLOCK_RANGE)
    assert get_logs_call_count == 1

    # Both proxies updated
    db_session.refresh(proxy_a)
    db_session.refresh(proxy_b)
    assert proxy_a.last_known_implementation == ADDR(11)
    assert proxy_b.last_known_implementation == ADDR(22)


# ---------------------------------------------------------------------------
# 7. test_scan_chunks_large_ranges
# ---------------------------------------------------------------------------


@patch("services.monitoring.proxy_watcher.rpc_request")
def test_scan_chunks_large_ranges(mock_rpc, db_session):
    """Set last_scanned_block far behind latest, verify multiple eth_getLogs
    calls in MAX_BLOCK_RANGE chunks."""
    _add_proxy(db_session, ADDR(1), last_scanned_block=0)
    latest_block = MAX_BLOCK_RANGE * 3 + 500  # spans 4 chunks

    get_logs_calls = []

    def rpc_side_effect(url, method, params):
        if method == "eth_blockNumber":
            return hex(latest_block)
        if method == "eth_getLogs":
            get_logs_calls.append(params)
            return []
        return None

    mock_rpc.side_effect = rpc_side_effect

    events = scan_for_upgrades(db_session, "http://localhost:8545")

    assert events == []
    # Should be ceil((latest_block) / MAX_BLOCK_RANGE) = 4 calls
    assert len(get_logs_calls) == 4

    # Verify the chunk boundaries are correct
    from_blocks = [int(c[0]["fromBlock"], 16) for c in get_logs_calls]
    to_blocks = [int(c[0]["toBlock"], 16) for c in get_logs_calls]

    # First chunk starts at 1 (last_scanned_block=0, cursor = 0+1)
    assert from_blocks[0] == 1
    # Last chunk ends at latest_block
    assert to_blocks[-1] == latest_block
    # Chunks are contiguous
    for i in range(1, len(from_blocks)):
        assert from_blocks[i] == to_blocks[i - 1] + 1


# ---------------------------------------------------------------------------
# 8. test_scan_ignores_unrelated_events
# ---------------------------------------------------------------------------


@patch("services.monitoring.proxy_watcher.rpc_request")
def test_scan_ignores_unrelated_events(mock_rpc, db_session):
    """Logs from addresses not in the watched list are ignored."""
    _add_proxy(db_session, ADDR(1), last_scanned_block=90)

    # Log from ADDR(99) which is NOT watched
    unrelated_log = _make_log(
        ADDR(99),
        UPGRADED_TOPIC0,
        _topic_for(ADDR(42)),
        block=hex(95),
        tx="0xunrelated" + "0" * 52,
    )

    def rpc_side_effect(url, method, params):
        if method == "eth_blockNumber":
            return hex(100)
        if method == "eth_getLogs":
            return [unrelated_log]
        return None

    mock_rpc.side_effect = rpc_side_effect

    events = scan_for_upgrades(db_session, "http://localhost:8545")

    # No events created since the emitter is not watched
    assert events == []


# ---------------------------------------------------------------------------
# 9. test_scan_idempotent_on_restart
# ---------------------------------------------------------------------------


@patch("services.monitoring.proxy_watcher.rpc_request")
def test_scan_idempotent_on_restart(mock_rpc, db_session):
    """After a scan, last_scanned_block is updated; a second scan with
    no new blocks returns empty."""
    proxy = _add_proxy(db_session, ADDR(1), last_scanned_block=90)
    latest_block = 100

    log = _make_log(ADDR(1), UPGRADED_TOPIC0, _topic_for(ADDR(11)), block=hex(95), tx="0xfirst" + "0" * 58)

    call_count = {"block_number": 0, "get_logs": 0}

    def rpc_side_effect(url, method, params):
        if method == "eth_blockNumber":
            call_count["block_number"] += 1
            return hex(latest_block)
        if method == "eth_getLogs":
            call_count["get_logs"] += 1
            # Only return the log on the first call
            if call_count["get_logs"] == 1:
                return [log]
            return []
        return None

    mock_rpc.side_effect = rpc_side_effect

    # First scan: should find the event
    events_1 = scan_for_upgrades(db_session, "http://localhost:8545")
    assert len(events_1) == 1

    db_session.refresh(proxy)
    assert proxy.last_scanned_block == latest_block

    # Second scan: same latest_block, no new blocks -> empty
    events_2 = scan_for_upgrades(db_session, "http://localhost:8545")
    assert events_2 == []

    # eth_getLogs should NOT have been called a second time
    # (from_block >= latest_block, so the scanner short-circuits)
    assert call_count["get_logs"] == 1


# ---------------------------------------------------------------------------
# 10. test_resolve_current_implementation
# ---------------------------------------------------------------------------


@patch("services.monitoring.proxy_watcher.rpc_request")
def test_resolve_current_implementation(mock_rpc):
    """Mock eth_getStorageAt, verify correct slot is queried and address is parsed."""
    impl_addr = ADDR(42)
    storage_value = "0x" + "0" * 24 + impl_addr[2:]

    mock_rpc.return_value = storage_value

    result = resolve_current_implementation(ADDR(1), "http://localhost:8545")

    assert result is not None
    assert result == impl_addr.lower()

    # Verify the correct slot was queried
    mock_rpc.assert_called_once_with(
        "http://localhost:8545",
        "eth_getStorageAt",
        [ADDR(1), EIP1967_IMPL_SLOT, "latest"],
    )


# ---------------------------------------------------------------------------
# 11. test_resolve_implementation_empty_slot
# ---------------------------------------------------------------------------


@patch("services.monitoring.proxy_watcher.rpc_request")
def test_resolve_implementation_empty_slot(mock_rpc):
    """Returns None for zero-filled slot (no implementation set)."""
    mock_rpc.return_value = "0x" + "0" * 64

    result = resolve_current_implementation(ADDR(1), "http://localhost:8545")
    assert result is None
