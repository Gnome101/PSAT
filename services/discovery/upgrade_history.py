#!/usr/bin/env python3
"""Fetch upgrade history for proxy contracts via Etherscan event logs.

For each proxy in dependencies.json, queries Upgraded(address),
AdminChanged(address,address), and BeaconUpgraded(address) events across
the contract's lifetime.  Produces a timeline of implementation changes.

Designed to run *after* dependencies.json is written so that proxy
metadata (type, current implementation) is already available.

Uses Etherscan's getLogs endpoint which is indexed by address+topic
and returns results in <1s regardless of chain history length.
"""

from __future__ import annotations

import json
from pathlib import Path

from services.discovery.static_dependencies import normalize_address

# ---------------------------------------------------------------------------
# EIP-1967 event topic0 hashes (keccak256 of signature)
# ---------------------------------------------------------------------------

# Upgraded(address indexed implementation)
UPGRADED_TOPIC0 = "0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b"

# AdminChanged(address previousAdmin, address newAdmin)
ADMIN_CHANGED_TOPIC0 = "0x7e644d79422f17c01e4894b5f4f588d331ebfa28653d42ae832dc59e38c9798f"

# BeaconUpgraded(address indexed beacon)
BEACON_UPGRADED_TOPIC0 = "0x1cf3b03a6cf19fa2baba4df148e9dcabedea7f8a5c07840e207e5c089be95d3e"

EVENT_TOPICS = {
    UPGRADED_TOPIC0: "upgraded",
    ADMIN_CHANGED_TOPIC0: "admin_changed",
    BEACON_UPGRADED_TOPIC0: "beacon_upgraded",
}

# ---------------------------------------------------------------------------
# Log parsing helpers
# ---------------------------------------------------------------------------


def _hex_to_int(value: str | int) -> int:
    if isinstance(value, int):
        return value
    return int(value, 16) if value.startswith("0x") else int(value)


def _topic_to_address(topic: str) -> str:
    """Extract a 20-byte address from a 32-byte log topic."""
    raw = topic.replace("0x", "").zfill(64)
    return normalize_address("0x" + raw[-40:])


def _data_to_addresses(data: str, count: int) -> list[str]:
    """Decode *count* consecutive ABI-encoded addresses from log data."""
    raw = data.replace("0x", "").zfill(64 * count)
    addresses = []
    for i in range(count):
        chunk = raw[i * 64 : (i + 1) * 64]
        addresses.append(normalize_address("0x" + chunk[-40:]))
    return addresses


def parse_upgrade_log(log: dict) -> dict | None:
    """Parse an Etherscan log entry into an UpgradeEvent dict."""
    topics = log.get("topics", [])
    if not topics:
        return None

    topic0 = topics[0].lower()
    event_type = EVENT_TOPICS.get(topic0)
    if not event_type:
        return None

    event: dict = {
        "event_type": event_type,
        "block_number": _hex_to_int(log.get("blockNumber", "0x0")),
        "tx_hash": log.get("transactionHash"),
        "log_index": _hex_to_int(log.get("logIndex", "0x0")),
    }

    # Etherscan getLogs returns timeStamp as hex
    ts = log.get("timeStamp")
    if ts:
        event["timestamp"] = _hex_to_int(ts)

    # Emitting contract address for grouping multi-proxy queries
    emitter = log.get("address")
    if emitter:
        event["_emitter"] = normalize_address(emitter)

    if event_type == "upgraded":
        if len(topics) >= 2:
            event["implementation"] = _topic_to_address(topics[1])

    elif event_type == "admin_changed":
        data = log.get("data", "0x")
        if data and data != "0x" and len(data.replace("0x", "")) >= 128:
            addrs = _data_to_addresses(data, 2)
            event["previous_admin"] = addrs[0]
            event["new_admin"] = addrs[1]

    elif event_type == "beacon_upgraded":
        if len(topics) >= 2:
            event["beacon"] = _topic_to_address(topics[1])

    return event


# ---------------------------------------------------------------------------
# Etherscan getLogs fetching
# ---------------------------------------------------------------------------


def _fetch_logs_etherscan(proxy_address: str, topic0: str) -> list[dict]:
    """Fetch all logs for a given address and topic0 via Etherscan getLogs."""
    from utils.etherscan import get

    try:
        data = get(
            "logs",
            "getLogs",
            address=proxy_address,
            topic0=topic0,
            fromBlock="0",
            toBlock="99999999",
        )
        result = data.get("result", [])
        return result if isinstance(result, list) else []
    except RuntimeError:
        return []


def fetch_upgrade_events(proxy_addresses: list[str]) -> list[dict]:
    """Fetch all EIP-1967 upgrade events for proxy addresses via Etherscan.

    Queries each proxy for all three event types (Upgraded, AdminChanged,
    BeaconUpgraded). Returns a chronologically sorted list of parsed events.
    Respects Etherscan's free-tier rate limit (5 calls/sec).
    """
    import time

    all_events: list[dict] = []

    for addr in proxy_addresses:
        addr = normalize_address(addr)
        for topic0 in EVENT_TOPICS:
            time.sleep(0.22)  # ~4.5 calls/sec, stays under 5/sec free-tier limit
            raw_logs = _fetch_logs_etherscan(addr, topic0)
            for log in raw_logs:
                event = parse_upgrade_log(log)
                if event:
                    all_events.append(event)

    all_events.sort(key=lambda e: (e.get("block_number", 0), e.get("log_index", 0)))
    return all_events


# ---------------------------------------------------------------------------
# Building the implementation timeline
# ---------------------------------------------------------------------------


def _build_implementation_timeline(
    events: list[dict],
    current_impl: str | None,
) -> list[dict]:
    """Build an ordered list of ImplementationRecords from upgrade events."""
    upgrade_events = [e for e in events if e["event_type"] == "upgraded" and e.get("implementation")]

    if not upgrade_events:
        if current_impl:
            return [{"address": current_impl}]
        return []

    records: list[dict] = []
    for i, event in enumerate(upgrade_events):
        record: dict = {
            "address": event["implementation"],
            "block_introduced": event["block_number"],
            "tx_hash": event["tx_hash"],
        }
        if "timestamp" in event:
            record["timestamp_introduced"] = event["timestamp"]
        if i + 1 < len(upgrade_events):
            record["block_replaced"] = upgrade_events[i + 1]["block_number"]
            if "timestamp" in upgrade_events[i + 1]:
                record["timestamp_replaced"] = upgrade_events[i + 1]["timestamp"]
        records.append(record)

    return records


# ---------------------------------------------------------------------------
# Reading proxy metadata from dependencies.json
# ---------------------------------------------------------------------------


def _enrich_implementations(implementations: list[dict], known_names: dict[str, str]) -> None:
    """Add contract names to historical implementations not already named in dependencies.json."""
    from utils.etherscan import get_contract_info

    for impl in implementations:
        addr = impl["address"]
        if addr in known_names:
            impl["contract_name"] = known_names[addr]
            continue
        name, _ = get_contract_info(addr)
        if name:
            impl["contract_name"] = name


def _extract_proxies_from_dependencies(
    dependencies_path: Path,
) -> tuple[str, dict[str, tuple[str, str | None]], dict[str, str]]:
    """Read dependencies.json and extract proxy addresses with their metadata.

    Returns (target_address, {proxy_addr: (proxy_type, current_impl)}, {addr: name}).
    """
    deps = json.loads(dependencies_path.read_text())
    target = normalize_address(deps["address"])

    proxy_meta: dict[str, tuple[str, str | None]] = {}
    known_names: dict[str, str] = {}

    for addr, info in deps.get("dependencies", {}).items():
        # Collect all known names for reuse
        if info.get("contract_name"):
            known_names[normalize_address(addr)] = info["contract_name"]
        impl = info.get("implementation")
        if isinstance(impl, dict):
            if impl.get("contract_name"):
                known_names[normalize_address(impl["address"])] = impl["contract_name"]

        if info.get("type") != "proxy":
            continue
        proxy_type = info.get("proxy_type", "unknown")
        if isinstance(impl, dict):
            current_impl = impl.get("address")
        elif isinstance(impl, str):
            current_impl = impl
        else:
            current_impl = None
        proxy_meta[normalize_address(addr)] = (proxy_type, current_impl)

    return target, proxy_meta, known_names


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _strip_internal(event: dict) -> dict:
    """Remove internal keys (prefixed with _) before serialization."""
    return {k: v for k, v in event.items() if not k.startswith("_")}


def build_upgrade_history(dependencies_path: Path) -> dict:
    """Build upgrade history for all proxy contracts found in dependencies.json.

    Args:
        dependencies_path: Path to the dependencies.json file written by
            the dependency pipeline.

    Returns:
        UpgradeHistoryOutput dict with per-proxy upgrade timelines.
    """
    target_address, proxy_meta, known_names = _extract_proxies_from_dependencies(dependencies_path)

    if not proxy_meta:
        return {
            "schema_version": "0.1",
            "target_address": target_address,
            "proxies": {},
            "total_upgrades": 0,
        }

    # Etherscan getLogs — indexed by address+topic, <1s per query
    all_events = fetch_upgrade_events(list(proxy_meta.keys()))

    # Group events by emitting proxy address
    events_by_proxy: dict[str, list[dict]] = {addr: [] for addr in proxy_meta}
    for event in all_events:
        emitter = event.get("_emitter")
        if emitter and emitter in events_by_proxy:
            events_by_proxy[emitter].append(event)

    proxies: dict[str, dict] = {}
    total_upgrades = 0
    all_implementations: list[dict] = []

    for addr, (proxy_type, current_impl) in proxy_meta.items():
        proxy_events = events_by_proxy.get(addr, [])
        implementations = _build_implementation_timeline(proxy_events, current_impl)
        upgrade_events = [e for e in proxy_events if e["event_type"] == "upgraded"]

        proxies[addr] = {
            "proxy_address": addr,
            "proxy_type": proxy_type,
            "current_implementation": current_impl,
            "upgrade_count": len(upgrade_events),
            "first_upgrade_block": upgrade_events[0]["block_number"] if upgrade_events else None,
            "last_upgrade_block": upgrade_events[-1]["block_number"] if upgrade_events else None,
            "implementations": implementations,
            "events": [_strip_internal(e) for e in proxy_events],
        }
        total_upgrades += len(upgrade_events)
        all_implementations.extend(implementations)

    # Resolve names: reuse names from dependencies.json, only call Etherscan
    # for historical implementations not already known
    _enrich_implementations(all_implementations, known_names)

    return {
        "schema_version": "0.1",
        "target_address": target_address,
        "proxies": proxies,
        "total_upgrades": total_upgrades,
    }


def write_upgrade_history(
    dependencies_path: Path,
    output_path: Path | None = None,
) -> Path | None:
    """Build and write upgrade history JSON.

    Args:
        dependencies_path: Path to dependencies.json.
        output_path: Where to write. Defaults to upgrade_history.json
            in the same directory as dependencies_path.

    Returns the output path if any proxies were found, or None.
    """
    if output_path is None:
        output_path = dependencies_path.parent / "upgrade_history.json"

    result = build_upgrade_history(dependencies_path)
    if not result["proxies"]:
        return None
    output_path.write_text(json.dumps(result, indent=2) + "\n")
    return output_path


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------


def main():
    import argparse

    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    parser = argparse.ArgumentParser(description="Fetch upgrade history for proxy contracts")
    parser.add_argument("dependencies", help="Path to dependencies.json")
    args = parser.parse_args()

    result = build_upgrade_history(Path(args.dependencies))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
