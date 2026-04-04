"""Proxy upgrade monitor — scans blocks for EIP-1967 Upgraded events."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import ProxyUpgradeEvent, SessionLocal, WatchedProxy
from services.discovery.upgrade_history import (
    ADMIN_CHANGED_TOPIC0,
    BEACON_UPGRADED_TOPIC0,
    EVENT_TOPICS,
    UPGRADED_TOPIC0,
    parse_upgrade_log,
)
from utils.rpc import normalize_hex, rpc_request

logger = logging.getLogger(__name__)

# Maximum block range per eth_getLogs call (stay under node limits)
MAX_BLOCK_RANGE = 2000
# Default scan interval in seconds
DEFAULT_SCAN_INTERVAL = 15


def get_latest_block(rpc_url: str) -> int:
    result = rpc_request(rpc_url, "eth_blockNumber", [])
    return int(result, 16)


def scan_for_upgrades(session: Session, rpc_url: str) -> list[ProxyUpgradeEvent]:
    """Scan new blocks for Upgraded events across all watched proxies.

    Uses a single eth_getLogs call per block range with all watched proxy
    addresses, making this O(1) RPC calls regardless of proxy count.

    Returns list of new ProxyUpgradeEvent records created.
    """
    # Get all watched proxies for this chain
    proxies = session.execute(select(WatchedProxy)).scalars().all()
    if not proxies:
        return []

    proxy_by_address: dict[str, WatchedProxy] = {p.proxy_address.lower(): p for p in proxies}
    addresses = list(proxy_by_address.keys())

    # Determine block range: from the minimum last_scanned_block across all proxies
    from_block = min(p.last_scanned_block for p in proxies)
    latest_block = get_latest_block(rpc_url)

    if from_block >= latest_block:
        return []

    new_events: list[ProxyUpgradeEvent] = []
    topics = [list(EVENT_TOPICS.keys())]  # topic0 = any of the 3 event types

    # Pre-load existing events for deduplication
    existing_events: set[tuple] = set()
    existing_rows = (
        session.execute(
            select(
                ProxyUpgradeEvent.watched_proxy_id,
                ProxyUpgradeEvent.tx_hash,
                ProxyUpgradeEvent.block_number,
                ProxyUpgradeEvent.new_implementation,
            ).where(ProxyUpgradeEvent.block_number > from_block)
        )
        .all()
    )
    for row in existing_rows:
        existing_events.add((str(row[0]), row[1], row[2], row[3]))

    # Scan in chunks to stay under node limits
    last_successful_block = from_block
    cursor = from_block + 1
    while cursor <= latest_block:
        to_block = min(cursor + MAX_BLOCK_RANGE - 1, latest_block)

        filter_params = {
            "fromBlock": hex(cursor),
            "toBlock": hex(to_block),
            "address": addresses,
            "topics": topics,
        }

        try:
            logs = rpc_request(rpc_url, "eth_getLogs", [filter_params])
        except Exception:
            logger.exception("eth_getLogs failed for blocks %d-%d", cursor, to_block)
            break

        if not isinstance(logs, list):
            logs = []

        for log in logs:
            event = parse_upgrade_log(log)
            if not event:
                continue

            emitter = normalize_hex(log.get("address", "")).lower()
            proxy = proxy_by_address.get(emitter)
            if not proxy:
                continue

            new_impl = event.get("implementation") or event.get("beacon") or event.get("new_admin")
            if not new_impl:
                continue

            # Skip duplicates: same proxy, tx, block, and target already recorded
            dedup_key = (str(proxy.id), event.get("tx_hash", ""), event["block_number"], new_impl)
            if dedup_key in existing_events:
                continue
            existing_events.add(dedup_key)

            upgrade_event = ProxyUpgradeEvent(
                watched_proxy_id=proxy.id,
                block_number=event["block_number"],
                tx_hash=event.get("tx_hash", ""),
                old_implementation=proxy.last_known_implementation,
                new_implementation=new_impl,
                event_type=event["event_type"],
            )
            session.add(upgrade_event)
            new_events.append(upgrade_event)

            proxy.last_known_implementation = new_impl
            logger.info(
                "Detected %s on %s: %s -> %s (block %d)",
                event["event_type"],
                proxy.proxy_address,
                proxy.last_known_implementation,
                new_impl,
                event["block_number"],
            )

        last_successful_block = to_block
        cursor = to_block + 1

    # Only advance last_scanned_block to the last successfully scanned block
    for proxy in proxies:
        proxy.last_scanned_block = last_successful_block

    session.commit()
    return new_events


def resolve_current_implementation(proxy_address: str, rpc_url: str) -> str | None:
    """Read the current EIP-1967 implementation slot for a proxy."""
    # EIP-1967 implementation slot:
    # bytes32(uint256(keccak256('eip1967.proxy.implementation')) - 1)
    slot = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
    try:
        result = rpc_request(rpc_url, "eth_getStorageAt", [proxy_address, slot, "latest"])
        if result and result != "0x" + "0" * 64:
            addr = "0x" + result[-40:]
            return normalize_hex(addr)
    except Exception:
        logger.debug("Could not read implementation slot for %s", proxy_address)
    return None


def run_scan_loop(rpc_url: str, interval: float = DEFAULT_SCAN_INTERVAL) -> None:
    """Run the proxy upgrade scanner in a blocking loop."""
    logger.info("Starting proxy upgrade monitor (interval=%ss)", interval)
    while True:
        try:
            with SessionLocal() as session:
                new_events = scan_for_upgrades(session, rpc_url)
                if new_events:
                    logger.info("Detected %d new upgrade event(s)", len(new_events))
        except Exception:
            logger.exception("Scan cycle failed")
        time.sleep(interval)
