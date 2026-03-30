#!/usr/bin/env python3
"""Event-first controller tracker with polling fallback and reconciliation."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any, TypeVar

import requests
import websockets
from eth_abi.abi import decode
from eth_utils.crypto import keccak

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from schemas.contract_analysis import AssociatedEvent
from schemas.control_tracking import (
    ControlChangeEvent,
    ControlSnapshot,
    ControlTrackingPlan,
    EventWatch,
    TrackedController,
    TrackedPolicy,
)
from .controller_adapters import expand_role_identifier_principals, type_authority_contract

JSON_RPC_TIMEOUT_SECONDS = 10
TrackedItem = TypeVar("TrackedItem", TrackedController, TrackedPolicy)


def load_control_tracking_plan(path: Path) -> ControlTrackingPlan:
    return json.loads(path.read_text())


def _rpc_request(rpc_url: str, method: str, params: list[Any]) -> Any:
    response = requests.post(
        rpc_url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=JSON_RPC_TIMEOUT_SECONDS,
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError(str(payload["error"]))
    return payload.get("result")


def _selector(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()[:8]


def _normalize_hex(value: str | None) -> str:
    if not isinstance(value, str) or not value.startswith("0x"):
        return "0x"
    return value.lower()


def _decode_controller_value(raw_value: Any, controller_kind: str) -> str:
    value = _normalize_hex(raw_value if isinstance(raw_value, str) else "0x")
    if controller_kind in {"state_variable", "external_contract"} and len(value) == 66:
        return "0x" + value[-40:]
    return value


def _call_selector(signature: str) -> str:
    return _selector(signature)


def _eth_call_raw(rpc_url: str, contract_address: str, signature: str, block_tag: str = "latest") -> str:
    call = {"to": contract_address, "data": _call_selector(signature)}
    raw = _rpc_request(rpc_url, "eth_call", [call, block_tag])
    if not isinstance(raw, str) or not raw.startswith("0x"):
        raise RuntimeError(f"Unexpected eth_call result for {signature}: {raw!r}")
    return raw


def _decode_abi_value(raw_value: str, abi_type: str):
    data = bytes.fromhex(_normalize_hex(raw_value)[2:])
    if not data:
        raise RuntimeError("Empty ABI data")
    value = decode([abi_type], data)[0]
    if abi_type == "address":
        return str(value).lower()
    if abi_type == "address[]":
        return [str(item).lower() for item in value]
    return value


def _decode_topic_value(raw_value: str, abi_type: str):
    normalized = _normalize_hex(raw_value)
    if abi_type == "address" and len(normalized) == 66:
        return "0x" + normalized[-40:]
    if abi_type == "bytes4" and len(normalized) == 66:
        return "0x" + normalized[2:10]
    if abi_type.startswith("uint"):
        return int(normalized, 16)
    if abi_type == "bool":
        return bool(int(normalized, 16))
    return normalized


def _try_eth_call_decoded(
    rpc_url: str, contract_address: str, signature: str, abi_type: str, block_tag: str = "latest"
) -> object | None:
    try:
        raw = _eth_call_raw(rpc_url, contract_address, signature, block_tag)
        if _normalize_hex(raw) in {"0x", "0x0"}:
            return None
        return _decode_abi_value(raw, abi_type)
    except Exception:
        return None


def _get_code(rpc_url: str, address: str, block_tag: str = "latest") -> str:
    raw = _rpc_request(rpc_url, "eth_getCode", [address, block_tag])
    return _normalize_hex(raw if isinstance(raw, str) else "0x")


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    raise RuntimeError(f"Unsupported integer value: {value!r}")


def classify_resolved_address(rpc_url: str, address: str, block_tag: str = "latest") -> tuple[str, dict[str, object]]:
    normalized = _normalize_hex(address)
    if normalized == "0x0000000000000000000000000000000000000000":
        return "zero", {"address": normalized}

    code = _get_code(rpc_url, normalized, block_tag)
    if code in {"0x", "0x0"}:
        return "eoa", {"address": normalized}

    safe_owners = _try_eth_call_decoded(rpc_url, normalized, "getOwners()", "address[]", block_tag)
    safe_threshold = _try_eth_call_decoded(rpc_url, normalized, "getThreshold()", "uint256", block_tag)
    if safe_owners is not None and safe_threshold is not None:
        return "safe", {
            "address": normalized,
            "owners": [str(item).lower() for item in safe_owners] if isinstance(safe_owners, list) else [],
            "threshold": _coerce_int(safe_threshold),
        }

    min_delay = _try_eth_call_decoded(rpc_url, normalized, "getMinDelay()", "uint256", block_tag)
    if min_delay is None:
        min_delay = _try_eth_call_decoded(rpc_url, normalized, "delay()", "uint256", block_tag)
    if min_delay is not None:
        owner = _try_eth_call_decoded(rpc_url, normalized, "owner()", "address", block_tag)
        details: dict[str, object] = {"address": normalized, "delay": _coerce_int(min_delay)}
        if owner is not None:
            details["owner"] = owner
        return "timelock", details

    upgrade_interface_version = _try_eth_call_decoded(
        rpc_url, normalized, "UPGRADE_INTERFACE_VERSION()", "string", block_tag
    )
    if upgrade_interface_version is not None:
        owner = _try_eth_call_decoded(rpc_url, normalized, "owner()", "address", block_tag)
        details = {
            "address": normalized,
            "upgrade_interface_version": str(upgrade_interface_version),
        }
        if owner is not None:
            details["owner"] = owner
        return "proxy_admin", details

    details = {"address": normalized}
    details.update(type_authority_contract(rpc_url, normalized, block_tag))
    return "contract", details


_classify_resolved_address = classify_resolved_address


def _current_block_number(rpc_url: str) -> int:
    raw = _rpc_request(rpc_url, "eth_blockNumber", [])
    if not isinstance(raw, str) or not raw.startswith("0x"):
        raise RuntimeError(f"Unexpected eth_blockNumber result: {raw!r}")
    return int(raw, 16)


def _read_polling_source(
    rpc_url: str,
    contract_address: str,
    source: str,
    controller_kind: str,
    block_tag: str = "latest",
    read_spec: dict[str, object] | None = None,
) -> str:
    target = source
    if isinstance(read_spec, dict) and read_spec.get("strategy") == "getter_call":
        read_target = read_spec.get("target")
        if isinstance(read_target, str) and read_target:
            target = read_target
    raw = _eth_call_raw(rpc_url, contract_address, f"{target}()", block_tag)
    return _decode_controller_value(raw, controller_kind)


def build_control_snapshot(plan: ControlTrackingPlan, rpc_url: str, block_tag: str = "latest") -> ControlSnapshot:
    block_number = _current_block_number(rpc_url) if block_tag == "latest" else int(block_tag, 16)
    controller_values = {}
    for controller in plan["tracked_controllers"]:
        source = controller["source"]
        read_spec = controller.get("read_spec")
        try:
            value = _read_polling_source(
                rpc_url,
                plan["contract_address"],
                source,
                controller["kind"],
                block_tag,
                read_spec=read_spec if isinstance(read_spec, dict) else None,
            )
            if controller["kind"] == "role_identifier":
                member_addresses, adapter_meta = expand_role_identifier_principals(
                    rpc_url,
                    plan["contract_address"],
                    value,
                    block_tag,
                )
                resolved_principals = []
                for member_address in member_addresses:
                    resolved_type, details = classify_resolved_address(
                        rpc_url,
                        member_address,
                        block_tag,
                    )
                    resolved_principals.append(
                        {
                            "address": member_address,
                            "resolved_type": resolved_type,
                            "details": details,
                        }
                    )
                controller_values[controller["controller_id"]] = {
                    "source": source,
                    "value": value,
                    "block_number": block_number,
                    "observed_via": f"eth_call+{adapter_meta.get('adapter', 'none')}",
                    "resolved_type": "unknown",
                    "details": {
                        "source": source,
                        "role_id": value,
                        **adapter_meta,
                        "resolved_principals": resolved_principals,
                    },
                }
                continue
            controller_values[controller["controller_id"]] = {
                "source": source,
                "value": value,
                "block_number": block_number,
                "observed_via": "eth_call",
            }
            resolved_type, details = classify_resolved_address(
                rpc_url,
                value,
                block_tag,
            )
            controller_values[controller["controller_id"]]["resolved_type"] = resolved_type
            controller_values[controller["controller_id"]]["details"] = details
        except Exception as exc:
            controller_values[controller["controller_id"]] = {
                "source": source,
                "value": None,
                "block_number": block_number,
                "observed_via": "eth_call_error",
                "resolved_type": "unknown",
                "details": {
                    "source": source,
                    "error": str(exc),
                },
            }
    return {
        "schema_version": "0.1",
        "contract_address": plan["contract_address"],
        "contract_name": plan["contract_name"],
        "block_number": block_number,
        "controller_values": controller_values,
    }


def diff_control_snapshots(previous: ControlSnapshot | None, current: ControlSnapshot) -> list[ControlChangeEvent]:
    if previous is None:
        return []

    changes: list[ControlChangeEvent] = []
    previous_values = previous.get("controller_values", {})
    current_values = current.get("controller_values", {})

    for controller_id, current_value in current_values.items():
        prior_value = previous_values.get(controller_id, {})
        old = prior_value.get("value")
        new = current_value.get("value")
        if old == new:
            continue
        changes.append(
            {
                "schema_version": "0.1",
                "contract_address": current["contract_address"],
                "contract_name": current["contract_name"],
                "change_kind": "controller_value_changed",
                "controller_id": controller_id,
                "block_number": current["block_number"],
                "tx_hash": None,
                "old_value": old,
                "new_value": new,
                "observed_via": current_value.get("observed_via", "eth_call"),
                "notes": [],
                "event_signature": None,
            }
        )

    return changes


def grouped_event_filters(plan: ControlTrackingPlan) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    tracked_items = list(plan.get("tracked_controllers", [])) + list(plan.get("tracked_policies", []))
    for item in tracked_items:
        event_watch = item.get("event_watch")
        if not event_watch:
            continue
        key = event_watch["contract_address"].lower()
        entry = grouped.setdefault(
            key,
            {
                "address": event_watch["contract_address"],
                "topics0": set(),
                "controller_ids": set(),
            },
        )
        entry["topics0"].update(event["topic0"] for event in event_watch["events"])
        tracked_id = item.get("controller_id") or item.get("policy_id")
        if tracked_id:
            entry["controller_ids"].add(tracked_id)

    return [
        {
            "address": item["address"],
            "topics": [sorted(item["topics0"])],
            "controller_ids": sorted(item["controller_ids"]),
        }
        for item in grouped.values()
    ]


def _matching_event_watch_items(items: list[TrackedItem], log_entry: dict[str, Any]) -> list[TrackedItem]:
    log_address = _normalize_hex(log_entry.get("address"))
    topics = list(log_entry.get("topics") or [])
    topic0 = _normalize_hex(topics[0]) if topics else "0x"

    matches = []
    for item in items:
        event_watch = item.get("event_watch")
        if not event_watch:
            continue
        if _normalize_hex(event_watch["contract_address"]) != log_address:
            continue
        if any(_normalize_hex(event["topic0"]) == topic0 for event in event_watch["events"]):
            matches.append(item)
    return matches


def matching_controllers_for_log(plan: ControlTrackingPlan, log_entry: dict[str, Any]) -> list[TrackedController]:
    return _matching_event_watch_items(list(plan.get("tracked_controllers", [])), log_entry)


def matching_policies_for_log(plan: ControlTrackingPlan, log_entry: dict[str, Any]) -> list[TrackedPolicy]:
    return _matching_event_watch_items(list(plan.get("tracked_policies", [])), log_entry)


def _event_from_log(event_watch: EventWatch, log_entry: dict[str, Any]) -> AssociatedEvent | None:
    topics = list(log_entry.get("topics") or [])
    topic0 = _normalize_hex(topics[0]) if topics else "0x"
    for event in event_watch.get("events", []):
        if _normalize_hex(event.get("topic0")) == topic0:
            return event
    return None


def _decode_event_log_fields(event_ref: AssociatedEvent, log_entry: dict[str, Any]) -> dict[str, Any]:
    topics = list(log_entry.get("topics") or [])
    topic_index = 1
    non_indexed_types = [item["type"] for item in event_ref.get("inputs", []) if not item.get("indexed")]
    non_indexed_values = []
    data = _normalize_hex(log_entry.get("data"))
    if non_indexed_types and data not in {"0x", "0x0"}:
        non_indexed_values = list(decode(non_indexed_types, bytes.fromhex(data[2:])))

    decoded: dict[str, Any] = {}
    non_indexed_index = 0
    for item in event_ref.get("inputs", []):
        name = item.get("name") or item["type"]
        abi_type = item["type"]
        if item.get("indexed"):
            if topic_index >= len(topics):
                break
            decoded[name] = _decode_topic_value(topics[topic_index], abi_type)
            topic_index += 1
            continue

        if non_indexed_index >= len(non_indexed_values):
            break
        value = non_indexed_values[non_indexed_index]
        non_indexed_index += 1
        if abi_type == "address":
            decoded[name] = str(value).lower()
        elif abi_type == "bytes4":
            decoded[name] = "0x" + bytes(value).hex()
        elif abi_type == "bool":
            decoded[name] = bool(value)
        elif abi_type.startswith("uint"):
            decoded[name] = int(value)
        else:
            decoded[name] = value
    return decoded


def policy_change_events(
    plan: ControlTrackingPlan, policy_matches: list[TrackedPolicy], log_entry: dict[str, Any]
) -> list[ControlChangeEvent]:
    if not policy_matches:
        return []

    raw_block_number = log_entry.get("blockNumber")
    if isinstance(raw_block_number, str) and raw_block_number.startswith("0x"):
        block_number = int(raw_block_number, 16)
    else:
        block_number = 0

    tx_hash = log_entry.get("transactionHash")
    changes: list[ControlChangeEvent] = []
    for policy in policy_matches:
        event_watch = policy["event_watch"]
        event_ref = _event_from_log(event_watch, log_entry)
        decoded_fields = _decode_event_log_fields(event_ref, log_entry) if event_ref else {}
        notes = list(policy.get("notes", []))
        if decoded_fields:
            notes.append(
                "decoded_fields=" + ", ".join(f"{key}={value}" for key, value in sorted(decoded_fields.items()))
            )
        changes.append(
            {
                "schema_version": "0.1",
                "contract_address": plan["contract_address"],
                "contract_name": plan["contract_name"],
                "change_kind": "policy_event_observed",
                "controller_id": policy["policy_id"],
                "block_number": block_number,
                "tx_hash": tx_hash if isinstance(tx_hash, str) else None,
                "old_value": None,
                "new_value": None,
                "observed_via": "wss_logs",
                "notes": notes,
                "event_signature": event_ref.get("signature") if event_ref else None,
            }
        )
    return changes


def write_control_snapshot(snapshot: ControlSnapshot, output_path: Path) -> None:
    output_path.write_text(json.dumps(snapshot, indent=2) + "\n")


def append_control_change_events(changes: list[ControlChangeEvent], output_path: Path) -> None:
    if not changes:
        return
    with output_path.open("a") as handle:
        for change in changes:
            handle.write(json.dumps(change) + "\n")


async def subscribe_control_logs(ws_url: str, plan: ControlTrackingPlan):
    """Yield matching log notifications from a websocket provider."""
    filters = grouped_event_filters(plan)
    if not filters:
        return

    async with websockets.connect(ws_url) as websocket:
        for index, log_filter in enumerate(filters, start=1):
            await websocket.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": index,
                        "method": "eth_subscribe",
                        "params": ["logs", {"address": log_filter["address"], "topics": log_filter["topics"]}],
                    }
                )
            )
            await websocket.recv()

        while True:
            message = json.loads(await websocket.recv())
            if message.get("method") != "eth_subscription":
                continue
            result = (message.get("params") or {}).get("result")
            if isinstance(result, dict):
                yield result


async def run_control_tracker(
    plan_path: Path,
    rpc_url: str,
    ws_url: str | None = None,
    snapshot_path: Path | None = None,
    change_events_path: Path | None = None,
    reconcile_interval_seconds: int = 900,
    once: bool = False,
) -> None:
    """Run an event-first controller tracker with periodic reconciliation."""
    plan = load_control_tracking_plan(plan_path)
    snapshot_path = snapshot_path or plan_path.with_name("control_snapshot.json")
    change_events_path = change_events_path or plan_path.with_name("control_change_events.jsonl")

    previous_snapshot = json.loads(snapshot_path.read_text()) if snapshot_path.exists() else None
    current_snapshot = build_control_snapshot(plan, rpc_url)
    write_control_snapshot(current_snapshot, snapshot_path)
    append_control_change_events(diff_control_snapshots(previous_snapshot, current_snapshot), change_events_path)
    previous_snapshot = current_snapshot

    if once:
        return

    last_reconcile = time.monotonic()

    if not ws_url:
        while True:
            await asyncio.sleep(reconcile_interval_seconds)
            current_snapshot = build_control_snapshot(plan, rpc_url)
            append_control_change_events(
                diff_control_snapshots(previous_snapshot, current_snapshot), change_events_path
            )
            write_control_snapshot(current_snapshot, snapshot_path)
            previous_snapshot = current_snapshot
        return

    async for log_entry in subscribe_control_logs(ws_url, plan):
        controller_matches = matching_controllers_for_log(plan, log_entry)
        policy_matches = matching_policies_for_log(plan, log_entry)
        if not controller_matches and not policy_matches:
            continue
        append_control_change_events(policy_change_events(plan, policy_matches, log_entry), change_events_path)
        current_snapshot = build_control_snapshot(plan, rpc_url)
        changes = diff_control_snapshots(previous_snapshot, current_snapshot)
        append_control_change_events(changes, change_events_path)
        write_control_snapshot(current_snapshot, snapshot_path)
        previous_snapshot = current_snapshot

        now = time.monotonic()
        if now - last_reconcile >= reconcile_interval_seconds:
            current_snapshot = build_control_snapshot(plan, rpc_url)
            changes = diff_control_snapshots(previous_snapshot, current_snapshot)
            append_control_change_events(changes, change_events_path)
            write_control_snapshot(current_snapshot, snapshot_path)
            previous_snapshot = current_snapshot
            last_reconcile = now


def main() -> None:
    parser = argparse.ArgumentParser(description="Track controller state with websocket logs and polling fallback.")
    parser.add_argument("plan", help="Path to control_tracking_plan.json")
    parser.add_argument("--rpc", required=True, help="HTTP RPC URL for state reads")
    parser.add_argument("--ws", help="Optional websocket RPC URL for log subscriptions")
    parser.add_argument("--snapshot-out", help="Optional path to control_snapshot.json")
    parser.add_argument("--changes-out", help="Optional path to control_change_events.jsonl")
    parser.add_argument(
        "--reconcile-interval-seconds",
        type=int,
        default=900,
        help="Polling reconciliation interval in seconds (default: 900)",
    )
    parser.add_argument("--once", action="store_true", help="Perform a single snapshot/diff cycle and exit")
    args = parser.parse_args()

    asyncio.run(
        run_control_tracker(
            Path(args.plan),
            rpc_url=args.rpc,
            ws_url=args.ws,
            snapshot_path=Path(args.snapshot_out) if args.snapshot_out else None,
            change_events_path=Path(args.changes_out) if args.changes_out else None,
            reconcile_interval_seconds=args.reconcile_interval_seconds,
            once=args.once,
        )
    )


if __name__ == "__main__":
    main()
