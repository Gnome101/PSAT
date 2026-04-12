"""Unified protocol monitoring — scans blocks for all governance + proxy events."""

from __future__ import annotations

import logging
import os
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import (
    Contract,
    ControllerValue,
    MonitoredContract,
    MonitoredEvent,
    ProxyUpgradeEvent,
    SessionLocal,
    UpgradeEvent,
    WatchedProxy,
)
from services.monitoring.event_topics import (
    ALL_EVENT_TOPICS,
    PROXY_EVENT_TOPICS,
    parse_any_log,
)
from services.monitoring.reanalysis import maybe_queue_reanalysis
from utils.rpc import (
    normalize_hex,
    parse_address_result,
    rpc_batch_request,
    rpc_request,
)

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

logger = logging.getLogger(__name__)

MAX_BLOCK_RANGE = 2000
DEFAULT_SCAN_INTERVAL = int(os.getenv("PROTOCOL_SCAN_INTERVAL", "15"))
DEFAULT_POLL_INTERVAL = int(os.getenv("PROTOCOL_POLL_INTERVAL", "60"))

# Storage slots for proxy resolution
_EIP1967_IMPL_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"

# Selectors for state polling
_OWNER_SEL = "0x8da5cb5b"  # owner()
_PAUSED_SEL = "0x5c975abb"  # paused()
_GET_THRESHOLD_SEL = "0xe75235b8"  # getThreshold()
_GET_MIN_DELAY_SEL = "0xf27a0c92"  # getMinDelay()


def get_latest_block(rpc_url: str) -> int:
    result = rpc_request(rpc_url, "eth_blockNumber", [])
    return int(result, 16)


def scan_for_events(session: Session, rpc_url: str) -> list[MonitoredEvent]:
    """Scan new blocks for all governance and proxy events.

    Uses a single eth_getLogs call per block chunk with all monitored
    addresses and all event topic0s. Returns list of new MonitoredEvent
    records created.
    """
    contracts = (
        session.execute(
            select(MonitoredContract).where(MonitoredContract.is_active == True)  # noqa: E712
        )
        .scalars()
        .all()
    )
    if not contracts:
        return []

    contract_by_address: dict[str, MonitoredContract] = {
        c.address.lower(): c for c in contracts
    }
    addresses = list(contract_by_address.keys())

    from_block = min(c.last_scanned_block for c in contracts)
    latest_block = get_latest_block(rpc_url)

    if from_block >= latest_block:
        logger.debug("Scan: no new blocks (from=%d, latest=%d)", from_block, latest_block)
        return []

    logger.debug(
        "Scan: %d contracts, block range %d->%d (%d blocks)",
        len(contracts),
        from_block + 1,
        latest_block,
        latest_block - from_block,
    )

    new_events: list[MonitoredEvent] = []
    topics = [list(ALL_EVENT_TOPICS.keys())]

    # Dedup set: (monitored_contract_id, tx_hash, block_number, event_type)
    existing_events: set[tuple] = set()
    max_scanned = max(c.last_scanned_block for c in contracts)
    if from_block < max_scanned:
        existing_rows = session.execute(
            select(
                MonitoredEvent.monitored_contract_id,
                MonitoredEvent.tx_hash,
                MonitoredEvent.block_number,
                MonitoredEvent.event_type,
            ).where(
                MonitoredEvent.block_number > from_block,
                MonitoredEvent.block_number <= max_scanned,
            )
        ).all()
        for row in existing_rows:
            existing_events.add((str(row[0]), row[1], row[2], row[3]))

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
            parsed = parse_any_log(log)
            if not parsed:
                continue

            emitter = normalize_hex(log.get("address", "")).lower()
            mc = contract_by_address.get(emitter)
            if not mc:
                continue

            event_type = parsed["event_type"]

            # Check monitoring config
            if mc.monitoring_config and not _should_watch(mc, event_type):
                continue

            # Dedup check
            dedup_key = (
                str(mc.id),
                parsed.get("tx_hash", ""),
                parsed["block_number"],
                event_type,
            )
            if dedup_key in existing_events:
                continue
            existing_events.add(dedup_key)

            # Build event data (everything except standard fields)
            event_data = {
                k: v
                for k, v in parsed.items()
                if k not in ("event_type", "block_number", "tx_hash", "log_index", "_emitter")
            }

            monitored_event = MonitoredEvent(
                id=uuid.uuid4(),
                monitored_contract_id=mc.id,
                event_type=event_type,
                block_number=parsed["block_number"],
                tx_hash=parsed.get("tx_hash", ""),
                data=event_data if event_data else None,
            )
            session.add(monitored_event)
            new_events.append(monitored_event)

            logger.info(
                "Detected %s on %s (block %d)",
                event_type,
                mc.address,
                parsed["block_number"],
            )

            # Write-through to ProxyUpgradeEvent for proxy events
            topic0 = log.get("topics", [""])[0].lower()
            if topic0 in PROXY_EVENT_TOPICS and mc.watched_proxy_id:
                _write_through_proxy_event(session, mc, parsed)

            # Update last_known_state
            _update_state_from_event(mc, parsed)

            # Propagate to relational tables (Contract, ControllerValue, UpgradeEvent)
            _sync_relational_tables(session, mc, parsed)

            # Queue a re-analysis job if the event warrants it
            try:
                maybe_queue_reanalysis(session, mc, event_type, event_data)
            except Exception:
                logger.exception("Failed to queue re-analysis for %s", mc.address)

        last_successful_block = to_block
        cursor = to_block + 1

    # Advance last_scanned_block
    for mc in contracts:
        if last_successful_block > mc.last_scanned_block:
            mc.last_scanned_block = last_successful_block

    session.commit()
    return new_events


def _should_watch(mc: MonitoredContract, event_type: str) -> bool:
    """Check if the monitoring config allows this event type."""
    config = mc.monitoring_config or {}

    type_to_config = {
        "upgraded": "watch_upgrades",
        "admin_changed": "watch_upgrades",
        "beacon_upgraded": "watch_upgrades",
        "changed_master_copy": "watch_upgrades",
        "new_implementation": "watch_upgrades",
        "new_pending_implementation": "watch_upgrades",
        "target_updated": "watch_upgrades",
        "upgraded_revision": "watch_upgrades",
        "diamond_cut": "watch_upgrades",
        "ownership_transferred": "watch_ownership",
        "paused": "watch_pause",
        "unpaused": "watch_pause",
        "role_granted": "watch_roles",
        "role_revoked": "watch_roles",
        "signer_added": "watch_safe_signers",
        "signer_removed": "watch_safe_signers",
        "threshold_changed": "watch_safe_signers",
        "timelock_scheduled": "watch_timelock",
        "timelock_executed": "watch_timelock",
        "delay_changed": "watch_timelock",
    }

    config_key = type_to_config.get(event_type)
    if config_key is None:
        return True  # Unknown event type — allow
    return config.get(config_key, True)


def _write_through_proxy_event(
    session: Session,
    mc: MonitoredContract,
    parsed: dict,
) -> None:
    """Write a ProxyUpgradeEvent for backward compatibility."""
    new_impl = (
        parsed.get("implementation")
        or parsed.get("beacon")
        or parsed.get("new_admin")
    )
    if not new_impl:
        return

    # Load the WatchedProxy to get old implementation
    wp = session.get(WatchedProxy, mc.watched_proxy_id)
    if not wp:
        return

    upgrade_event = ProxyUpgradeEvent(
        watched_proxy_id=wp.id,
        block_number=parsed["block_number"],
        tx_hash=parsed.get("tx_hash", ""),
        old_implementation=wp.last_known_implementation,
        new_implementation=new_impl,
        event_type=parsed["event_type"],
    )
    session.add(upgrade_event)

    wp.last_known_implementation = new_impl
    if parsed["block_number"] > wp.last_scanned_block:
        wp.last_scanned_block = parsed["block_number"]


def _update_state_from_event(mc: MonitoredContract, parsed: dict) -> None:
    """Update last_known_state based on a detected event."""
    state = dict(mc.last_known_state or {})
    event_type = parsed["event_type"]

    if event_type == "ownership_transferred" and parsed.get("new_owner"):
        state["owner"] = parsed["new_owner"]
    elif event_type in ("paused", "unpaused"):
        state["paused"] = event_type == "paused"
    elif event_type == "threshold_changed" and parsed.get("threshold"):
        state["threshold"] = parsed["threshold"]
    elif event_type in ("upgraded", "new_implementation", "changed_master_copy", "target_updated"):
        impl = parsed.get("implementation")
        if impl:
            state["implementation"] = impl
    elif event_type == "admin_changed" and parsed.get("new_admin"):
        state["admin"] = parsed["new_admin"]
    elif event_type == "beacon_upgraded" and parsed.get("beacon"):
        state["beacon"] = parsed["beacon"]
    elif event_type == "delay_changed" and parsed.get("new_delay") is not None:
        state["min_delay"] = parsed["new_delay"]

    mc.last_known_state = state


def _sync_relational_tables(
    session: Session,
    mc: MonitoredContract,
    parsed: dict,
) -> None:
    """Propagate a detected event to the relational Contract / ControllerValue /
    UpgradeEvent tables so the API serves up-to-date data.

    Only updates rows when the MonitoredContract has a linked contract_id.
    """
    if not mc.contract_id:
        return

    event_type = parsed["event_type"]

    # --- Proxy upgrade events → Contract.implementation + UpgradeEvent row ---
    if event_type in (
        "upgraded", "new_implementation", "changed_master_copy", "target_updated",
    ):
        new_impl = parsed.get("implementation")
        if not new_impl:
            return
        contract = session.get(Contract, mc.contract_id)
        if not contract:
            return

        old_impl = contract.implementation
        contract.implementation = new_impl

        session.add(UpgradeEvent(
            contract_id=contract.id,
            proxy_address=mc.address,
            old_impl=old_impl,
            new_impl=new_impl,
            block_number=parsed.get("block_number"),
            tx_hash=parsed.get("tx_hash"),
        ))

    # --- AdminChanged → Contract.admin ---
    elif event_type == "admin_changed":
        new_admin = parsed.get("new_admin")
        if not new_admin:
            return
        contract = session.get(Contract, mc.contract_id)
        if contract:
            contract.admin = new_admin

    # --- OwnershipTransferred → ControllerValue where controller_id contains 'owner' ---
    elif event_type == "ownership_transferred":
        new_owner = parsed.get("new_owner")
        if not new_owner:
            return
        cv_rows = (
            session.execute(
                select(ControllerValue).where(
                    ControllerValue.contract_id == mc.contract_id,
                    ControllerValue.controller_id.ilike("%owner%"),
                )
            )
            .scalars()
            .all()
        )
        for cv in cv_rows:
            cv.value = new_owner


def _sync_relational_from_poll(
    session: Session,
    mc: MonitoredContract,
    field_name: str,
    new_value: object,
    old_value: object,
) -> None:
    """Propagate a polling-detected state change to relational tables."""
    if not mc.contract_id:
        return

    if field_name == "implementation":
        contract = session.get(Contract, mc.contract_id)
        if contract:
            contract.implementation = str(new_value)
            session.add(UpgradeEvent(
                contract_id=contract.id,
                proxy_address=mc.address,
                old_impl=str(old_value) if old_value else None,
                new_impl=str(new_value),
                block_number=0,
                tx_hash="",
            ))

    elif field_name == "owner":
        cv_rows = (
            session.execute(
                select(ControllerValue).where(
                    ControllerValue.contract_id == mc.contract_id,
                    ControllerValue.controller_id.ilike("%owner%"),
                )
            )
            .scalars()
            .all()
        )
        for cv in cv_rows:
            cv.value = str(new_value)


# ---------------------------------------------------------------------------
# State polling
# ---------------------------------------------------------------------------


def poll_for_state_changes(session: Session, rpc_url: str) -> list[MonitoredEvent]:
    """Poll for state changes on contracts that need polling.

    Batches RPC calls based on contract_type and compares against
    last_known_state. Returns list of new MonitoredEvent records for changes.
    """
    contracts = (
        session.execute(
            select(MonitoredContract).where(
                MonitoredContract.is_active == True,  # noqa: E712
                MonitoredContract.needs_polling == True,  # noqa: E712
            )
        )
        .scalars()
        .all()
    )
    if not contracts:
        return []

    # Build batch calls
    batch_calls: list[tuple[str, list]] = []
    # (contract, start_idx, field_name)
    poll_plan: list[tuple[MonitoredContract, int, str]] = []

    for mc in contracts:
        ct = mc.contract_type
        if ct == "proxy":
            start = len(batch_calls)
            batch_calls.append(("eth_getStorageAt", [mc.address, _EIP1967_IMPL_SLOT, "latest"]))
            poll_plan.append((mc, start, "implementation"))
        if ct in ("proxy", "regular", "pausable", "access_control"):
            start = len(batch_calls)
            batch_calls.append(("eth_call", [{"to": mc.address, "data": _OWNER_SEL}, "latest"]))
            poll_plan.append((mc, start, "owner"))
        if ct in ("pausable",):
            start = len(batch_calls)
            batch_calls.append(("eth_call", [{"to": mc.address, "data": _PAUSED_SEL}, "latest"]))
            poll_plan.append((mc, start, "paused"))
        if ct == "safe":
            start = len(batch_calls)
            batch_calls.append(("eth_call", [{"to": mc.address, "data": _GET_THRESHOLD_SEL}, "latest"]))
            poll_plan.append((mc, start, "threshold"))
        if ct == "timelock":
            start = len(batch_calls)
            batch_calls.append(("eth_call", [{"to": mc.address, "data": _GET_MIN_DELAY_SEL}, "latest"]))
            poll_plan.append((mc, start, "min_delay"))

    if not batch_calls:
        return []

    try:
        results = rpc_batch_request(rpc_url, batch_calls)
    except Exception:
        logger.exception("Batch RPC failed during poll")
        return []

    new_events: list[MonitoredEvent] = []

    for mc, idx, field_name in poll_plan:
        raw = results[idx]
        state = dict(mc.last_known_state or {})
        old_value = state.get(field_name)

        if field_name == "implementation":
            new_value = parse_address_result(raw)
        elif field_name == "owner":
            new_value = parse_address_result(raw)
        elif field_name == "paused":
            if raw and raw != "0x" + "0" * 64:
                new_value = True
            else:
                new_value = False
        elif field_name in ("threshold", "min_delay"):
            if raw and raw != "0x":
                try:
                    new_value = int(raw, 16)
                except (ValueError, TypeError):
                    new_value = None
            else:
                new_value = None
        else:
            new_value = None

        if new_value is None:
            continue

        if new_value != old_value:
            # Always record the new value in last_known_state
            state[field_name] = new_value
            mc.last_known_state = state

            # Skip emitting an event when old_value is None — that's the
            # first observation after enrollment, not an actual state change.
            if old_value is None:
                logger.debug(
                    "Initial %s observation on %s: %s (no event emitted)",
                    field_name,
                    mc.address,
                    new_value,
                )
                continue

            event = MonitoredEvent(
                id=uuid.uuid4(),
                monitored_contract_id=mc.id,
                event_type="state_changed_poll",
                block_number=0,
                tx_hash="",
                data={
                    "field": field_name,
                    "old_value": str(old_value),
                    "new_value": str(new_value),
                },
            )
            session.add(event)
            new_events.append(event)

            logger.info(
                "Poll detected %s change on %s: %s -> %s",
                field_name,
                mc.address,
                old_value,
                new_value,
            )

            # Write-through for proxy implementation changes
            if field_name == "implementation" and mc.watched_proxy_id:
                wp = session.get(WatchedProxy, mc.watched_proxy_id)
                if wp:
                    upgrade_event = ProxyUpgradeEvent(
                        watched_proxy_id=wp.id,
                        block_number=0,
                        tx_hash="",
                        old_implementation=str(old_value) if old_value else None,
                        new_implementation=str(new_value),
                        event_type="storage_poll",
                    )
                    session.add(upgrade_event)
                    wp.last_known_implementation = str(new_value)

            # Propagate to relational tables
            _sync_relational_from_poll(session, mc, field_name, new_value, old_value)

            # Queue a re-analysis job if the state change warrants it
            try:
                poll_data = {
                    "field": field_name,
                    "old_value": str(old_value),
                    "new_value": str(new_value),
                }
                maybe_queue_reanalysis(session, mc, "state_changed_poll", poll_data)
            except Exception:
                logger.exception("Failed to queue re-analysis for %s", mc.address)

    session.commit()
    return new_events


# ---------------------------------------------------------------------------
# Blocking loops
# ---------------------------------------------------------------------------


def run_scan_loop(rpc_url: str, interval: float = DEFAULT_SCAN_INTERVAL) -> None:
    """Run the unified event scanner in a blocking loop."""
    logger.info("Starting unified protocol monitor (interval=%ss)", interval)
    while True:
        try:
            with SessionLocal() as session:
                new_events = scan_for_events(session, rpc_url)
                if new_events:
                    logger.info("Detected %d new event(s)", len(new_events))
                    try:
                        from services.monitoring.notifier import notify_protocol_events

                        notify_protocol_events(session, new_events)
                    except Exception:
                        logger.exception("Protocol notification failed")
        except Exception:
            logger.exception("Scan cycle failed")
        time.sleep(interval)


def run_poll_loop(rpc_url: str, interval: float = DEFAULT_POLL_INTERVAL) -> None:
    """Run the unified state polling loop."""
    logger.info("Starting unified protocol poller (interval=%ss)", interval)
    while True:
        try:
            with SessionLocal() as session:
                new_events = poll_for_state_changes(session, rpc_url)
                if new_events:
                    logger.info("Poll detected %d state change(s)", len(new_events))
                    try:
                        from services.monitoring.notifier import notify_protocol_events

                        notify_protocol_events(session, new_events)
                    except Exception:
                        logger.exception("Protocol notification failed")
        except Exception:
            logger.exception("Poll cycle failed")
        time.sleep(interval)
