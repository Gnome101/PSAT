"""scan_for_upgrades against synthesized eth_getLogs. Mirrors real mainnet layouts; no RPC needed."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Iterable

from sqlalchemy.orm import Session as SASession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.models import WatchedProxy
from services.discovery.upgrade_history import (
    ADMIN_CHANGED_TOPIC0,
    BEACON_UPGRADED_TOPIC0,
    CHANGED_MASTER_COPY_TOPIC0,
    DIAMOND_CUT_TOPIC0,
    NEW_IMPLEMENTATION_TOPIC0,
    NEW_PENDING_IMPLEMENTATION_TOPIC0,
    TARGET_UPDATED_TOPIC0,
    UPGRADED_TOPIC0,
)
from tests.conftest import requires_postgres

ZERO_WORD = "0x" + "0" * 64


def _addr_topic(addr: str) -> str:
    return "0x" + "0" * 24 + addr.lower().replace("0x", "")


def _addr_word(addr: str) -> str:
    return "0" * 24 + addr.lower().replace("0x", "")


def _make_log(
    proxy_addr: str,
    topics: list[str],
    data: str,
    block_number: int,
    tx_hash: str,
    log_index: int = 0,
) -> dict:
    return {
        "address": proxy_addr,
        "topics": topics,
        "data": data,
        "blockNumber": hex(block_number),
        "transactionHash": tx_hash,
        "logIndex": hex(log_index),
    }


def _install_rpc_stub(
    monkeypatch,
    logs: Iterable[dict],
    latest_block: int,
    storage: dict[tuple[str, str], str] | None = None,
) -> None:
    """Canned rpc_request stub; filters logs by range/address/topic0 like a real node."""
    import services.monitoring.proxy_watcher as pw

    all_logs = list(logs)
    storage = storage or {}

    def fake_rpc(rpc_url, method, params, retries=1):
        if method == "eth_blockNumber":
            return hex(latest_block)
        if method == "eth_getLogs":
            f = params[0]
            from_b = int(f["fromBlock"], 16)
            to_b = int(f["toBlock"], 16)
            wanted_addrs = {a.lower() for a in f.get("address", [])}
            wanted_topics = set(t.lower() for t in f.get("topics", [[]])[0])
            matched = []
            for log in all_logs:
                lb = int(log["blockNumber"], 16)
                if not (from_b <= lb <= to_b):
                    continue
                if log["address"].lower() not in wanted_addrs:
                    continue
                if log["topics"][0].lower() not in wanted_topics:
                    continue
                matched.append(log)
            return matched
        if method == "eth_getStorageAt":
            return storage.get((params[0].lower(), params[1].lower()), ZERO_WORD)
        if method == "eth_call":
            return "0x"
        raise RuntimeError(f"unexpected RPC call in test stub: {method} {params}")

    monkeypatch.setattr(pw, "rpc_request", fake_rpc)


def _add_proxy(
    session: SASession,
    address: str,
    last_scanned_block: int,
    label: str = "test",
) -> WatchedProxy:
    proxy = WatchedProxy(
        id=uuid.uuid4(),
        proxy_address=address,
        chain="ethereum",
        label=label,
        last_known_implementation=None,
        last_scanned_block=last_scanned_block,
    )
    session.add(proxy)
    session.commit()
    return proxy


# Mainnet-derived addrs + tx hashes. Block numbers, topic0s, and impls are real; only payloads synthesized.

# Aave V3 Pool — standard EIP-1967 Upgraded(address indexed implementation)
AAVE_V3_POOL = "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2"
AAVE_UPGRADE_BLOCK = 17214196
AAVE_NEW_IMPL = "0xf1cd4193bbc1ad4a23e833170f49d60f3d35a621"
AAVE_TX = "0x012b6d5e9be9ae815f6d4af51adc11aed782a6979eca66f9afbf4d122245f342"

# USDC FiatTokenProxy — OZ legacy Upgraded(address) with impl in data
USDC_PROXY = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDC_UPGRADE_BLOCK = 10743414
USDC_NEW_IMPL = "0xb7277a6e95992041568d9391d09d0122023778a2"
USDC_TX = "0xe6f0f754398d89583da8e4229c5d7aaa00739a3ae334ecfc2839ac396b4836e3"

# BeaconUpgraded — beacon in topics[1]
BEACON_PROXY = "0xe662bb403214a62294351514ece015efd191f632"
BEACON_UPGRADE_BLOCK = 17001069
BEACON_NEW_BEACON = "0xcb387c45185738c490c17ee6f0babfc197525d16"
BEACON_TX = "0x11116036d88e226940aa1c1eec47cccef73ea5a756c9fe91460e01a9f1e3314b"

# AdminChanged — previous + new admin in data
ADMIN_PROXY = "0xb4370cfed6a13874a84935aed81d19618b89145f"
ADMIN_CHANGE_BLOCK = 17000262
ADMIN_NEW_ADMIN = "0xf0a0c2e85f4f09e021037bd68c03432a48210dda"
ADMIN_TX = "0x344b978d53d4f07888ea30fb93bf7b928e2ebfa28c53e7cb63e203205f88dbbe"

# GnosisSafe ChangedMasterCopy(address)
GNOSIS_PROXY = "0x78ecc4ad66c9ea16821df5ef762fe021cac3fd4c"
GNOSIS_CHANGE_BLOCK = 9304839
GNOSIS_NEW_MASTER = "0x34cfac646f301356faa8b21e94227e3583fe3f5f"
GNOSIS_TX = "0x472e2fed6badf9a28d522c0836c4d5cc03a7cc0ab474f469001a3adb8e531bb3"

# Compound NewImplementation(address,address)
COMPOUND_PROXY = "0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b"
COMPOUND_UPGRADE_BLOCK = 7710677
COMPOUND_NEW_IMPL = "0x62f18c451af964197341d3c86d27e98c41bb8fcc"
COMPOUND_TX = "0xa468f4f9a9054f49bcbff1293878c5ac34ca8321c92695cace6afe264b3c36d0"

# Compound NewPendingImplementation(address,address)
COMPOUND_PENDING_BLOCK = 7710675
COMPOUND_PENDING_IMPL = "0x1234567890abcdef1234567890abcdef12345678"
COMPOUND_PENDING_TX = "0x7466c9dabee14b97e3600a13ade4b43b6c4b04fd660fd28453da23a522c7d7f3"

# Synthetix TargetUpdated(address)
SYNTHETIX_PROXY = "0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f"
SYNTHETIX_UPGRADE_BLOCK = 10203309
SYNTHETIX_NEW_TARGET = "0xc6738ed1eb79fa23941c75b4f437fc65893b5476"
SYNTHETIX_TX = "0xf286b1dd8d6163e866601cf85ba1646f76a11bae86f8f323ea4a8f0ca46db4c1"

# EIP-2535 DiamondCut
DIAMOND_PROXY = "0x3caca7b48d0573d793d3b0279b5f0029180e83b6"
DIAMOND_CUT_BLOCK = 14004881
DIAMOND_CUT_TX = "0x915094b1a190e242eeb15ba6c569370bcf4d40eb9caf95a4558d54e2f59c08fc"
# Pre-encoded DiamondCut(FacetCut[], _init=0, _calldata=0x) with one facet at 0x0606...06.
DIAMOND_CUT_DATA = (
    "0x"
    "0000000000000000000000000000000000000000000000000000000000000060"  # offset to FacetCut[]
    "0000000000000000000000000000000000000000000000000000000000000000"  # _init = 0
    "0000000000000000000000000000000000000000000000000000000000000120"  # offset to calldata
    "0000000000000000000000000000000000000000000000000000000000000001"  # array length
    "0000000000000000000000000000000000000000000000000000000000000020"  # offset to entry[0]
    "0000000000000000000000000606060606060606060606060606060606060606"  # entry[0].address
    "0000000000000000000000000000000000000000000000000000000000000000"  # entry[0].action = Add
    "0000000000000000000000000000000000000000000000000000000000000060"  # offset to selectors[]
    "0000000000000000000000000000000000000000000000000000000000000000"  # selectors[].length = 0
    "0000000000000000000000000000000000000000000000000000000000000000"  # calldata.length = 0
)


@requires_postgres
def test_scan_detects_eip1967_upgrade(monkeypatch, db_session: SASession):
    """Upgraded(address indexed implementation) with impl in topics[1]."""
    from services.monitoring.proxy_watcher import scan_for_upgrades

    _add_proxy(db_session, AAVE_V3_POOL, last_scanned_block=AAVE_UPGRADE_BLOCK - 5)
    log = _make_log(
        AAVE_V3_POOL,
        [UPGRADED_TOPIC0, _addr_topic(AAVE_NEW_IMPL)],
        "0x",
        AAVE_UPGRADE_BLOCK,
        AAVE_TX,
    )
    _install_rpc_stub(monkeypatch, [log], latest_block=AAVE_UPGRADE_BLOCK + 5)

    events = scan_for_upgrades(db_session, "http://test")

    assert len(events) == 1
    upgrade = events[0]
    assert upgrade.event_type == "upgraded"
    assert upgrade.block_number == AAVE_UPGRADE_BLOCK
    assert upgrade.new_implementation.lower() == AAVE_NEW_IMPL.lower()
    assert upgrade.tx_hash == AAVE_TX


@requires_postgres
def test_scan_detects_oz_legacy_upgrade(monkeypatch, db_session: SASession):
    """Upgraded(address implementation) with impl in data (OZ legacy variant)."""
    from services.monitoring.proxy_watcher import scan_for_upgrades

    _add_proxy(db_session, USDC_PROXY, last_scanned_block=USDC_UPGRADE_BLOCK - 5)
    log = _make_log(
        USDC_PROXY,
        [UPGRADED_TOPIC0],
        "0x" + _addr_word(USDC_NEW_IMPL),
        USDC_UPGRADE_BLOCK,
        USDC_TX,
    )
    _install_rpc_stub(monkeypatch, [log], latest_block=USDC_UPGRADE_BLOCK + 5)

    events = scan_for_upgrades(db_session, "http://test")

    assert len(events) == 1
    assert events[0].event_type == "upgraded"
    assert events[0].new_implementation.lower() == USDC_NEW_IMPL.lower()


@requires_postgres
def test_scan_detects_beacon_upgrade(monkeypatch, db_session: SASession):
    """BeaconUpgraded(address indexed beacon) — beacon in topics[1]."""
    from services.monitoring.proxy_watcher import scan_for_upgrades

    _add_proxy(db_session, BEACON_PROXY, last_scanned_block=BEACON_UPGRADE_BLOCK - 5)
    log = _make_log(
        BEACON_PROXY,
        [BEACON_UPGRADED_TOPIC0, _addr_topic(BEACON_NEW_BEACON)],
        "0x",
        BEACON_UPGRADE_BLOCK,
        BEACON_TX,
    )
    _install_rpc_stub(monkeypatch, [log], latest_block=BEACON_UPGRADE_BLOCK + 5)

    events = scan_for_upgrades(db_session, "http://test")

    assert len(events) == 1
    assert events[0].event_type == "beacon_upgraded"
    assert events[0].new_implementation.lower() == BEACON_NEW_BEACON.lower()


@requires_postgres
def test_scan_detects_admin_changed(monkeypatch, db_session: SASession):
    """AdminChanged — both admins in data (64 bytes)."""
    from services.monitoring.proxy_watcher import scan_for_upgrades

    _add_proxy(db_session, ADMIN_PROXY, last_scanned_block=ADMIN_CHANGE_BLOCK - 5)
    log = _make_log(
        ADMIN_PROXY,
        [ADMIN_CHANGED_TOPIC0],
        "0x" + "0" * 64 + _addr_word(ADMIN_NEW_ADMIN),
        ADMIN_CHANGE_BLOCK,
        ADMIN_TX,
    )
    _install_rpc_stub(monkeypatch, [log], latest_block=ADMIN_CHANGE_BLOCK + 5)

    events = scan_for_upgrades(db_session, "http://test")

    assert len(events) == 1
    assert events[0].event_type == "admin_changed"
    assert events[0].new_implementation.lower() == ADMIN_NEW_ADMIN.lower()


@requires_postgres
def test_scan_detects_gnosis_master_copy_change(monkeypatch, db_session: SASession):
    """GnosisSafe ChangedMasterCopy(address) — singleton in data."""
    from services.monitoring.proxy_watcher import scan_for_upgrades

    _add_proxy(db_session, GNOSIS_PROXY, last_scanned_block=GNOSIS_CHANGE_BLOCK - 5)
    log = _make_log(
        GNOSIS_PROXY,
        [CHANGED_MASTER_COPY_TOPIC0],
        "0x" + _addr_word(GNOSIS_NEW_MASTER),
        GNOSIS_CHANGE_BLOCK,
        GNOSIS_TX,
    )
    _install_rpc_stub(monkeypatch, [log], latest_block=GNOSIS_CHANGE_BLOCK + 5)

    events = scan_for_upgrades(db_session, "http://test")

    assert len(events) == 1
    assert events[0].event_type == "changed_master_copy"
    assert events[0].new_implementation.lower() == GNOSIS_NEW_MASTER.lower()


@requires_postgres
def test_scan_detects_compound_new_implementation(monkeypatch, db_session: SASession):
    """Compound NewImplementation(address,address) — new impl is 2nd address."""
    from services.monitoring.proxy_watcher import scan_for_upgrades

    _add_proxy(db_session, COMPOUND_PROXY, last_scanned_block=COMPOUND_UPGRADE_BLOCK - 5)
    log = _make_log(
        COMPOUND_PROXY,
        [NEW_IMPLEMENTATION_TOPIC0],
        "0x" + "0" * 64 + _addr_word(COMPOUND_NEW_IMPL),
        COMPOUND_UPGRADE_BLOCK,
        COMPOUND_TX,
    )
    _install_rpc_stub(monkeypatch, [log], latest_block=COMPOUND_UPGRADE_BLOCK + 5)

    events = scan_for_upgrades(db_session, "http://test")

    assert len(events) == 1
    assert events[0].event_type == "new_implementation"
    assert events[0].new_implementation.lower() == COMPOUND_NEW_IMPL.lower()


@requires_postgres
def test_scan_detects_compound_pending_implementation(monkeypatch, db_session: SASession):
    """Compound NewPendingImplementation(address,address) — first half of 2-step upgrade."""
    from services.monitoring.proxy_watcher import scan_for_upgrades

    _add_proxy(db_session, COMPOUND_PROXY, last_scanned_block=COMPOUND_PENDING_BLOCK - 5)
    log = _make_log(
        COMPOUND_PROXY,
        [NEW_PENDING_IMPLEMENTATION_TOPIC0],
        "0x" + "0" * 64 + _addr_word(COMPOUND_PENDING_IMPL),
        COMPOUND_PENDING_BLOCK,
        COMPOUND_PENDING_TX,
    )
    _install_rpc_stub(monkeypatch, [log], latest_block=COMPOUND_PENDING_BLOCK + 5)

    events = scan_for_upgrades(db_session, "http://test")

    assert len(events) == 1
    assert events[0].event_type == "new_pending_implementation"
    assert events[0].new_implementation.lower() == COMPOUND_PENDING_IMPL.lower()


@requires_postgres
def test_scan_detects_synthetix_target_updated(monkeypatch, db_session: SASession):
    """Synthetix TargetUpdated(address) — new target in data."""
    from services.monitoring.proxy_watcher import scan_for_upgrades

    _add_proxy(db_session, SYNTHETIX_PROXY, last_scanned_block=SYNTHETIX_UPGRADE_BLOCK - 5)
    log = _make_log(
        SYNTHETIX_PROXY,
        [TARGET_UPDATED_TOPIC0],
        "0x" + _addr_word(SYNTHETIX_NEW_TARGET),
        SYNTHETIX_UPGRADE_BLOCK,
        SYNTHETIX_TX,
    )
    _install_rpc_stub(monkeypatch, [log], latest_block=SYNTHETIX_UPGRADE_BLOCK + 5)

    events = scan_for_upgrades(db_session, "http://test")

    assert len(events) == 1
    assert events[0].event_type == "target_updated"
    assert events[0].new_implementation.lower() == SYNTHETIX_NEW_TARGET.lower()


@requires_postgres
def test_scan_detects_diamond_cut(monkeypatch, db_session: SASession):
    """EIP-2535 DiamondCut — ABI-encoded FacetCut[] in data."""
    from services.monitoring.proxy_watcher import scan_for_upgrades

    _add_proxy(db_session, DIAMOND_PROXY, last_scanned_block=DIAMOND_CUT_BLOCK - 5)
    log = _make_log(
        DIAMOND_PROXY,
        [DIAMOND_CUT_TOPIC0],
        DIAMOND_CUT_DATA,
        DIAMOND_CUT_BLOCK,
        DIAMOND_CUT_TX,
    )
    _install_rpc_stub(monkeypatch, [log], latest_block=DIAMOND_CUT_BLOCK + 5)

    events = scan_for_upgrades(db_session, "http://test")

    assert len(events) == 1
    assert events[0].event_type == "diamond_cut"
    assert events[0].block_number == DIAMOND_CUT_BLOCK


@requires_postgres
def test_scan_no_events_in_quiet_range(monkeypatch, db_session: SASession):
    """Scanning a block range with no matching logs returns empty and advances cursor."""
    from services.monitoring.proxy_watcher import scan_for_upgrades

    proxy = _add_proxy(db_session, AAVE_V3_POOL, last_scanned_block=1000)
    _install_rpc_stub(monkeypatch, [], latest_block=1100)

    events = scan_for_upgrades(db_session, "http://test")

    assert events == []
    db_session.refresh(proxy)
    assert proxy.last_scanned_block == 1100


@requires_postgres
def test_scan_updates_last_known_implementation(monkeypatch, db_session: SASession):
    """A successful scan updates the watched proxy's last_known_implementation."""
    from services.monitoring.proxy_watcher import scan_for_upgrades

    proxy = _add_proxy(db_session, AAVE_V3_POOL, last_scanned_block=AAVE_UPGRADE_BLOCK - 5)
    log = _make_log(
        AAVE_V3_POOL,
        [UPGRADED_TOPIC0, _addr_topic(AAVE_NEW_IMPL)],
        "0x",
        AAVE_UPGRADE_BLOCK,
        AAVE_TX,
    )
    _install_rpc_stub(monkeypatch, [log], latest_block=AAVE_UPGRADE_BLOCK + 5)

    scan_for_upgrades(db_session, "http://test")

    db_session.refresh(proxy)
    assert proxy.last_known_implementation is not None
    assert proxy.last_known_implementation.lower() == AAVE_NEW_IMPL.lower()
    assert proxy.last_scanned_block == AAVE_UPGRADE_BLOCK + 5
