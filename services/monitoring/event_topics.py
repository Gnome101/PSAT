"""Unified event topic constants and parsers for governance + proxy events."""

from __future__ import annotations

from eth_utils.crypto import keccak

from services.discovery.upgrade_history import (
    EVENT_TOPICS as PROXY_EVENT_TOPICS,
    _data_to_addresses,
    _hex_to_int,
    _topic_to_address,
    parse_upgrade_log,
)

# ---------------------------------------------------------------------------
# Governance event topic0 hashes
# ---------------------------------------------------------------------------

# OwnershipTransferred(address indexed previousOwner, address indexed newOwner)
OWNERSHIP_TRANSFERRED_TOPIC0 = (
    "0x" + keccak(text="OwnershipTransferred(address,address)").hex()
)

# Paused(address account)
PAUSED_TOPIC0 = "0x" + keccak(text="Paused(address)").hex()

# Unpaused(address account)
UNPAUSED_TOPIC0 = "0x" + keccak(text="Unpaused(address)").hex()

# RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)
ROLE_GRANTED_TOPIC0 = "0x" + keccak(text="RoleGranted(bytes32,address,address)").hex()

# RoleRevoked(bytes32 indexed role, address indexed account, address indexed sender)
ROLE_REVOKED_TOPIC0 = "0x" + keccak(text="RoleRevoked(bytes32,address,address)").hex()

# GnosisSafe AddedOwner(address owner)
ADDED_OWNER_TOPIC0 = "0x" + keccak(text="AddedOwner(address)").hex()

# GnosisSafe RemovedOwner(address owner)
REMOVED_OWNER_TOPIC0 = "0x" + keccak(text="RemovedOwner(address)").hex()

# GnosisSafe ChangedThreshold(uint256 threshold)
CHANGED_THRESHOLD_TOPIC0 = "0x" + keccak(text="ChangedThreshold(uint256)").hex()

# OZ TimelockController CallScheduled — exact v5 signature (7 params)
CALL_SCHEDULED_TOPIC0 = (
    "0x"
    + keccak(
        text="CallScheduled(bytes32,uint256,address,uint256,bytes,bytes32,uint256)"
    ).hex()
)

# OZ TimelockController CallExecuted — exact v5 signature (5 params)
CALL_EXECUTED_TOPIC0 = (
    "0x"
    + keccak(
        text="CallExecuted(bytes32,uint256,address,uint256,bytes)"
    ).hex()
)

# MinDelayChange(uint256 oldDuration, uint256 newDuration)
MIN_DELAY_CHANGE_TOPIC0 = (
    "0x" + keccak(text="MinDelayChange(uint256,uint256)").hex()
)

# ---------------------------------------------------------------------------
# Topic -> event_type mapping
# ---------------------------------------------------------------------------

GOVERNANCE_EVENT_TOPICS: dict[str, str] = {
    OWNERSHIP_TRANSFERRED_TOPIC0: "ownership_transferred",
    PAUSED_TOPIC0: "paused",
    UNPAUSED_TOPIC0: "unpaused",
    ROLE_GRANTED_TOPIC0: "role_granted",
    ROLE_REVOKED_TOPIC0: "role_revoked",
    ADDED_OWNER_TOPIC0: "signer_added",
    REMOVED_OWNER_TOPIC0: "signer_removed",
    CHANGED_THRESHOLD_TOPIC0: "threshold_changed",
    CALL_SCHEDULED_TOPIC0: "timelock_scheduled",
    CALL_EXECUTED_TOPIC0: "timelock_executed",
    MIN_DELAY_CHANGE_TOPIC0: "delay_changed",
}

ALL_EVENT_TOPICS: dict[str, str] = {**PROXY_EVENT_TOPICS, **GOVERNANCE_EVENT_TOPICS}

# ---------------------------------------------------------------------------
# Governance log parser
# ---------------------------------------------------------------------------


def parse_governance_log(log: dict) -> dict | None:
    """Parse a governance event log entry.

    Returns a dict with event_type, block_number, tx_hash, and parsed fields,
    or None if the log is not a recognised governance event.
    """
    topics = log.get("topics", [])
    if not topics:
        return None

    topic0 = topics[0].lower()
    event_type = GOVERNANCE_EVENT_TOPICS.get(topic0)
    if not event_type:
        return None

    event: dict = {
        "event_type": event_type,
        "block_number": _hex_to_int(log.get("blockNumber", "0x0")),
        "tx_hash": log.get("transactionHash"),
    }

    data = log.get("data", "0x")

    if event_type == "ownership_transferred":
        # topics[1] = old owner, topics[2] = new owner (both indexed)
        if len(topics) >= 3:
            event["old_owner"] = _topic_to_address(topics[1])
            event["new_owner"] = _topic_to_address(topics[2])

    elif event_type == "paused":
        # data = address account (non-indexed)
        if data and data != "0x" and len(data.replace("0x", "")) >= 40:
            addrs = _data_to_addresses(data, 1)
            event["account"] = addrs[0]

    elif event_type == "unpaused":
        # data = address account (non-indexed)
        if data and data != "0x" and len(data.replace("0x", "")) >= 40:
            addrs = _data_to_addresses(data, 1)
            event["account"] = addrs[0]

    elif event_type == "role_granted":
        # topics[1] = role (bytes32), topics[2] = account, topics[3] = sender
        if len(topics) >= 4:
            event["role"] = topics[1]
            event["account"] = _topic_to_address(topics[2])
            event["sender"] = _topic_to_address(topics[3])

    elif event_type == "role_revoked":
        # topics[1] = role (bytes32), topics[2] = account, topics[3] = sender
        if len(topics) >= 4:
            event["role"] = topics[1]
            event["account"] = _topic_to_address(topics[2])
            event["sender"] = _topic_to_address(topics[3])

    elif event_type == "signer_added":
        # data = address owner (non-indexed)
        if data and data != "0x" and len(data.replace("0x", "")) >= 40:
            addrs = _data_to_addresses(data, 1)
            event["owner"] = addrs[0]

    elif event_type == "signer_removed":
        # data = address owner (non-indexed)
        if data and data != "0x" and len(data.replace("0x", "")) >= 40:
            addrs = _data_to_addresses(data, 1)
            event["owner"] = addrs[0]

    elif event_type == "threshold_changed":
        # data = uint256 threshold (non-indexed)
        if data and data != "0x":
            event["threshold"] = _hex_to_int(data)

    elif event_type == "timelock_scheduled":
        # topics[1] = id (bytes32), topics[2] = index (uint256)
        # data = (address target, uint256 value, bytes data, bytes32 predecessor, uint256 delay)
        if len(topics) >= 3:
            event["operation_id"] = topics[1]
            event["index"] = _hex_to_int(topics[2])

    elif event_type == "timelock_executed":
        # topics[1] = id (bytes32), topics[2] = index (uint256)
        # data = (address target, uint256 value, bytes data)
        if len(topics) >= 3:
            event["operation_id"] = topics[1]
            event["index"] = _hex_to_int(topics[2])

    elif event_type == "delay_changed":
        # data = (uint256 oldDuration, uint256 newDuration) — both non-indexed
        if data and data != "0x" and len(data.replace("0x", "")) >= 128:
            raw = data.replace("0x", "").zfill(128)
            event["old_delay"] = int(raw[:64], 16)
            event["new_delay"] = int(raw[64:128], 16)

    return event


def parse_any_log(log: dict) -> dict | None:
    """Try to parse a log as a proxy upgrade event first, then governance.

    Returns the parsed event dict or None.
    """
    result = parse_upgrade_log(log)
    if result is not None:
        return result
    return parse_governance_log(log)
