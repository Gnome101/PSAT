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

# GnosisSafe — ChangedMasterCopy(address)
CHANGED_MASTER_COPY_TOPIC0 = "0x75e41bc35ff1bf14d81d1d2f649c0084a0f974f9289c803ec9898eeec4c8d0b8"

# Compound — NewImplementation(address oldImplementation, address newImplementation)
NEW_IMPLEMENTATION_TOPIC0 = "0xd604de94d45953f9138079ec1b82d533cb2160c906d1076d1f7ed54befbca97a"

# Compound — NewPendingImplementation(address oldPendingImplementation, address newPendingImplementation)
NEW_PENDING_IMPLEMENTATION_TOPIC0 = "0xe945ccee5d701fc83f9b8aa8ca94ea4219ec1fcbd4f4cab4f0ea57c5c3e1d815"

# Synthetix — TargetUpdated(address newTarget)
TARGET_UPDATED_TOPIC0 = "0x814250a3b8c79fcbe2ead2c131c952a278491c8f4322a79fe84b5040a810373e"

# Aave V2 — Upgraded(uint256 revision)
UPGRADED_REVISION_TOPIC0 = "0x65a5e70879738a94a00f00947edae8111ae0aed9175ce342db680bf1e0fb87fc"

# Diamond (EIP-2535) — DiamondCut((address,uint8,bytes4[])[],address,bytes)
DIAMOND_CUT_TOPIC0 = "0x8faa70878671ccd212d20771b795c50af8fd3ff6cf27f4bde57e5d4de0aeb673"

EVENT_TOPICS = {
    UPGRADED_TOPIC0: "upgraded",
    ADMIN_CHANGED_TOPIC0: "admin_changed",
    BEACON_UPGRADED_TOPIC0: "beacon_upgraded",
    CHANGED_MASTER_COPY_TOPIC0: "changed_master_copy",
    NEW_IMPLEMENTATION_TOPIC0: "new_implementation",
    NEW_PENDING_IMPLEMENTATION_TOPIC0: "new_pending_implementation",
    TARGET_UPDATED_TOPIC0: "target_updated",
    UPGRADED_REVISION_TOPIC0: "upgraded_revision",
    DIAMOND_CUT_TOPIC0: "diamond_cut",
}

# ---------------------------------------------------------------------------
# Log parsing helpers
# ---------------------------------------------------------------------------


def _hex_to_int(value: str | int) -> int:
    if isinstance(value, int):
        return value
    if value in ("0x", "0x0", ""):
        return 0
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
        if len(topics) >= 2 and topics[1]:
            event["implementation"] = _topic_to_address(topics[1])
        else:
            # Some proxies (e.g. OZ legacy) emit Upgraded(address) with the
            # implementation as a non-indexed parameter, stored in data.
            data = log.get("data", "0x")
            if data and data != "0x" and len(data.replace("0x", "")) >= 40:
                addrs = _data_to_addresses(data, 1)
                event["implementation"] = addrs[0]

    elif event_type == "admin_changed":
        # Standard: both addresses in data (non-indexed)
        data = log.get("data", "0x")
        if data and data != "0x" and len(data.replace("0x", "")) >= 128:
            addrs = _data_to_addresses(data, 2)
            event["previous_admin"] = addrs[0]
            event["new_admin"] = addrs[1]
        elif len(topics) >= 3 and topics[1] and topics[2]:
            # Variant: indexed parameters in topics
            event["previous_admin"] = _topic_to_address(topics[1])
            event["new_admin"] = _topic_to_address(topics[2])

    elif event_type == "beacon_upgraded":
        if len(topics) >= 2 and topics[1]:
            event["beacon"] = _topic_to_address(topics[1])
        else:
            # Fallback: non-indexed parameter in data
            data = log.get("data", "0x")
            if data and data != "0x" and len(data.replace("0x", "")) >= 40:
                addrs = _data_to_addresses(data, 1)
                event["beacon"] = addrs[0]

    elif event_type == "changed_master_copy":
        # GnosisSafe: single non-indexed address in data
        data = log.get("data", "0x")
        if data and data != "0x" and len(data.replace("0x", "")) >= 40:
            addrs = _data_to_addresses(data, 1)
            event["implementation"] = addrs[0]

    elif event_type == "new_implementation":
        # Compound: two ABI-encoded addresses in data (old impl, new impl)
        data = log.get("data", "0x")
        if data and data != "0x" and len(data.replace("0x", "")) >= 128:
            addrs = _data_to_addresses(data, 2)
            event["old_implementation"] = addrs[0]
            event["implementation"] = addrs[1]

    elif event_type == "new_pending_implementation":
        # Compound: two ABI-encoded addresses in data (old pending impl, new pending impl)
        data = log.get("data", "0x")
        if data and data != "0x" and len(data.replace("0x", "")) >= 128:
            addrs = _data_to_addresses(data, 2)
            event["implementation"] = addrs[1]

    elif event_type == "target_updated":
        # Synthetix: single non-indexed address in data
        data = log.get("data", "0x")
        if data and data != "0x" and len(data.replace("0x", "")) >= 40:
            addrs = _data_to_addresses(data, 1)
            event["implementation"] = addrs[0]

    elif event_type == "upgraded_revision":
        # Aave V2: uint256 revision number in data — NOT an implementation address
        data = log.get("data", "0x")
        if data and data != "0x" and len(data.replace("0x", "")) >= 2:
            event["revision"] = _hex_to_int(data)

    elif event_type == "diamond_cut":
        # EIP-2535 DiamondCut: ABI-encoded FacetCut[] + _init address + _calldata
        # Extract facet addresses from the FacetCut[] array, filtering out Remove actions.
        try:
            data = log.get("data", "0x")
            raw = data.replace("0x", "")
            if len(raw) >= 192:  # minimum: 3 words (offsets) + at least array length
                # bytes 0-63: offset to FacetCut[] array
                array_offset = int(raw[0:64], 16) * 2  # convert byte offset to hex-char offset
                # At array_offset: uint256 count of FacetCut entries
                count_start = array_offset
                if len(raw) >= count_start + 64:
                    count = int(raw[count_start : count_start + 64], 16)
                    if count > 1000:  # cap to prevent DoS from crafted events
                        count = 0
                    # After count: `count` uint256 offsets (relative to array_offset)
                    entry_offsets_start = count_start + 64
                    facets: list[str] = []
                    for i in range(count):
                        off_pos = entry_offsets_start + i * 64
                        if len(raw) < off_pos + 64:
                            break
                        entry_offset = int(raw[off_pos : off_pos + 64], 16) * 2
                        # Entry is relative to array_offset
                        entry_start = array_offset + entry_offset
                        # Each FacetCut entry: address (32 bytes) + action (32 bytes) + ...
                        if len(raw) < entry_start + 128:
                            break
                        facet_addr = normalize_address("0x" + raw[entry_start + 24 : entry_start + 64])
                        action = int(raw[entry_start + 64 : entry_start + 128], 16)
                        # action: 0=Add, 1=Replace, 2=Remove — skip Remove
                        if action != 2 and facet_addr != normalize_address("0x" + "0" * 40):
                            facets.append(facet_addr)
                    if facets:
                        event["implementation"] = facets[0]
                        event["facets"] = facets
        except (ValueError, IndexError):
            pass  # malformed data — return event without implementation

    return event


# ---------------------------------------------------------------------------
# Etherscan getLogs fetching
# ---------------------------------------------------------------------------


def _fetch_logs_etherscan(proxy_address: str, topic0: str, from_block: int = 0) -> list[dict]:
    """Fetch all logs for a given address and topic0 via Etherscan getLogs."""
    from utils.etherscan import get

    try:
        data = get(
            "logs",
            "getLogs",
            address=proxy_address,
            topic0=topic0,
            fromBlock=str(from_block),
            toBlock="99999999",
        )
        result = data.get("result", [])
        return result if isinstance(result, list) else []
    except RuntimeError:
        return []


def fetch_upgrade_events(proxy_addresses: list[str], from_block: int = 0) -> list[dict]:
    """Fetch all EIP-1967 upgrade events for proxy addresses via Etherscan.

    Queries each proxy for all three event types (Upgraded, AdminChanged,
    BeaconUpgraded). Returns a chronologically sorted list of parsed events.
    Rate-limited centrally by ``utils.etherscan``.

    Args:
        proxy_addresses: List of proxy contract addresses to query.
        from_block: Only fetch events from this block number onwards.
            Defaults to 0 (fetch all history).
    """
    all_events: list[dict] = []

    # Flatten the address × topic matrix into one task list. Each
    # ``_fetch_logs_etherscan`` call goes through the global Etherscan rate
    # lock so threading only stacks RTTs — the limiter still serialises wire
    # calls.
    tasks: list[tuple[str, str]] = []
    for addr in proxy_addresses:
        addr = normalize_address(addr)
        for topic0 in EVENT_TOPICS:
            tasks.append((addr, topic0))

    if tasks:
        from utils.etherscan import parallel_get

        calls = {
            f"{addr}|{topic0}": (lambda a=addr, t=topic0: _fetch_logs_etherscan(a, t, from_block=from_block))
            for addr, topic0 in tasks
        }
        results = parallel_get(calls)

        # Iterate tasks in their original (addr, topic) order so the parsed
        # events list is reconstructed deterministically before sorting.
        for addr, topic0 in tasks:
            raw_logs = results.get(f"{addr}|{topic0}", [])
            if isinstance(raw_logs, BaseException) or not isinstance(raw_logs, list):
                continue
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
    from utils.etherscan import get_contract_info, parallel_get

    addrs_to_fetch = sorted({impl["address"] for impl in implementations if impl["address"] not in known_names})
    fetched: dict[str, str | None] = {}
    if addrs_to_fetch:
        calls = {addr: (lambda a=addr: get_contract_info(a)) for addr in addrs_to_fetch}
        results = parallel_get(calls)
        for addr in addrs_to_fetch:
            value = results.get(addr)
            if isinstance(value, tuple) and len(value) == 2:
                fetched[addr] = value[0]
            else:
                fetched[addr] = None

    for impl in implementations:
        addr = impl["address"]
        if addr in known_names:
            impl["contract_name"] = known_names[addr]
            continue
        if fetched.get(addr):
            impl["contract_name"] = fetched[addr]


def _extract_proxies_from_dependencies(
    deps: dict,
) -> tuple[str, dict[str, tuple[str, str | None]], dict[str, str]]:
    """Extract proxy metadata for the TARGET only from a unified deps dict.

    Dependency proxies are intentionally ignored — each dependency gets its
    own analysis job later, and the upgrade history for that dependency is
    built when it's the target of its own run. Processing dependency proxies
    here would duplicate work and conflate unrelated contracts' histories.

    Returns (target_address, {proxy_addr: (proxy_type, current_impl)}, {addr: name}).
    The proxy_meta dict contains at most one entry — the target itself, if
    it's classified as a proxy.
    """
    target = normalize_address(deps["address"])

    proxy_meta: dict[str, tuple[str, str | None]] = {}
    known_names: dict[str, str] = {}

    # Only the target contract's upgrade history is built here.
    target_cls = deps.get("target_classification", {})
    if target_cls.get("type") == "proxy":
        proxy_type = target_cls.get("proxy_type", "unknown")
        impl = target_cls.get("implementation")
        if isinstance(impl, dict):
            current_impl = impl.get("address")
        elif isinstance(impl, str):
            current_impl = impl
        else:
            current_impl = None
        proxy_meta[target] = (proxy_type, current_impl)

    # Still harvest known names from dependencies so historical impl
    # enrichment can reuse them without extra Etherscan calls.
    for addr, info in deps.get("dependencies", {}).items():
        if info.get("contract_name"):
            known_names[normalize_address(addr)] = info["contract_name"]
        impl = info.get("implementation")
        if isinstance(impl, dict) and impl.get("contract_name"):
            known_names[normalize_address(impl["address"])] = impl["contract_name"]

    return target, proxy_meta, known_names


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _strip_internal(event: dict) -> dict:
    """Remove internal keys (prefixed with _) before serialization."""
    return {k: v for k, v in event.items() if not k.startswith("_")}


def build_upgrade_history(dependencies: dict, *, enrich: bool = True, from_block: int = 0) -> dict:
    """Build upgrade history for all proxy contracts in a unified deps dict.

    Args:
        dependencies: Unified dependency payload as produced by
            ``services.discovery.unified_dependencies.build_unified_dependencies``.
        enrich: If True (default), resolve contract names for historical
            implementations via Etherscan.  Set to False for faster runs
            when names are not needed.
        from_block: Only fetch events from this block number onwards.
            Defaults to 0 (fetch all history).  Used for incremental
            fetching when previous upgrade history is available.

    Returns:
        UpgradeHistoryOutput dict with per-proxy upgrade timelines.
    """
    target_address, proxy_meta, known_names = _extract_proxies_from_dependencies(dependencies)

    if not proxy_meta:
        return {
            "schema_version": "0.1",
            "target_address": target_address,
            "proxies": {},
            "total_upgrades": 0,
        }

    # Etherscan getLogs — indexed by address+topic, <1s per query
    all_events = fetch_upgrade_events(list(proxy_meta.keys()), from_block=from_block)

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

    # Resolve names: always apply already-known names from dependencies.json.
    # When enrich=True, also call Etherscan for historical unknowns.
    if enrich:
        _enrich_implementations(all_implementations, known_names)
    else:
        # Still apply names we already have — zero extra API calls
        for impl in all_implementations:
            if impl["address"] in known_names:
                impl["contract_name"] = known_names[impl["address"]]

    return {
        "schema_version": "0.1",
        "target_address": target_address,
        "proxies": proxies,
        "total_upgrades": total_upgrades,
    }
