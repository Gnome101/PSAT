#!/usr/bin/env python3
"""Historical HyperSync backfill for policy-tracking events."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from schemas.contract_analysis import AssociatedEvent
from schemas.control_tracking import ControlTrackingPlan, TrackedPolicy
from schemas.hypersync_backfill import (
    PolicyEventRecord,
    PolicyStateSnapshot,
    PublicCapabilityStateEntry,
    RoleCapabilityStateEntry,
    UserRoleStateEntry,
)
from services.resolution.tracking import _decode_event_log_fields, load_control_tracking_plan
from utils.rpc import normalize_hex as _normalize_hex

DEFAULT_HYPERSYNC_URL = "https://eth.hypersync.xyz"


def _create_hypersync_client(hypersync_module, url: str, bearer_token: str):
    return hypersync_module.HypersyncClient(
        hypersync_module.ClientConfig(
            url=url,
            bearer_token=bearer_token,
        )
    )


def _build_query(hypersync_module, plan: ControlTrackingPlan, from_block: int, to_block: int | None):
    topic0s = sorted(
        {
            event["topic0"]
            for policy in plan.get("tracked_policies", [])
            for event in (policy.get("event_watch") or {}).get("events", [])
        }
    )
    return hypersync_module.Query(
        from_block=from_block,
        to_block=to_block,
        logs=[
            hypersync_module.LogSelection(
                address=[plan["contract_address"]],
                topics=[topic0s],
            )
        ],
        field_selection=hypersync_module.FieldSelection(
            log=[field.value for field in hypersync_module.LogField],
        ),
    )


def _topics_from_log(log) -> list[str]:
    topics = getattr(log, "topics", None)
    if isinstance(topics, (list, tuple)):
        return [_normalize_hex(topic) for topic in topics if isinstance(topic, str) and topic.startswith("0x")]

    extracted = []
    for attr in ("topic0", "topic1", "topic2", "topic3"):
        value = getattr(log, attr, None)
        if isinstance(value, str) and value.startswith("0x") and value not in {"0x", "0x0"}:
            extracted.append(_normalize_hex(value))
    return extracted


def _log_entry_from_hypersync_log(log) -> dict[str, Any]:
    return {
        "address": getattr(log, "address", None),
        "topics": _topics_from_log(log),
        "data": getattr(log, "data", "0x") or "0x",
        "blockNumber": getattr(log, "block_number", None),
        "transactionHash": getattr(log, "transaction_hash", None),
        "logIndex": getattr(log, "log_index", None),
    }


def _policy_event_lookup(plan: ControlTrackingPlan) -> dict[str, list[tuple[TrackedPolicy, AssociatedEvent]]]:
    by_topic0: dict[str, list[tuple[TrackedPolicy, AssociatedEvent]]] = {}
    for policy in plan.get("tracked_policies", []):
        for event in (policy.get("event_watch") or {}).get("events", []):
            by_topic0.setdefault(_normalize_hex(event["topic0"]), []).append((policy, event))
    return by_topic0


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    raise RuntimeError(f"Unsupported integer value: {value!r}")


async def fetch_policy_event_history(
    plan: ControlTrackingPlan,
    url: str = DEFAULT_HYPERSYNC_URL,
    bearer_token: str | None = None,
    from_block: int = 0,
    to_block: int | None = None,
    *,
    client=None,
    hypersync_module=None,
) -> list[PolicyEventRecord]:
    if not plan.get("tracked_policies"):
        return []

    if hypersync_module is None:
        import hypersync as hypersync_module  # type: ignore

    if client is None:
        if not bearer_token:
            raise RuntimeError("HyperSync requires an API token. Set ENVIO_API_TOKEN or pass --token.")
        client = _create_hypersync_client(hypersync_module, url, bearer_token)

    event_lookup = _policy_event_lookup(plan)
    current_from = from_block
    records: list[PolicyEventRecord] = []

    while True:
        query = _build_query(hypersync_module, plan, current_from, to_block)
        response = await client.get(query)
        logs = list(getattr(getattr(response, "data", None), "logs", []) or [])

        for log in logs:
            log_entry = _log_entry_from_hypersync_log(log)
            topics = list(log_entry.get("topics") or [])
            topic0 = _normalize_hex(topics[0]) if topics else "0x"
            matches = event_lookup.get(topic0, [])
            for policy, event_ref in matches:
                decoded_fields = _decode_event_log_fields(event_ref, log_entry)
                block_number = log_entry.get("blockNumber")
                if isinstance(block_number, str) and block_number.startswith("0x"):
                    block_number = int(block_number, 16)
                records.append(
                    {
                        "schema_version": "0.1",
                        "contract_address": plan["contract_address"],
                        "contract_name": plan["contract_name"],
                        "policy_id": policy["policy_id"],
                        "policy_label": policy["label"],
                        "event_signature": event_ref["signature"],
                        "block_number": int(block_number or 0),
                        "tx_hash": log_entry.get("transactionHash"),
                        "log_index": log_entry.get("logIndex"),
                        "decoded_fields": decoded_fields,
                    }
                )

        next_block = getattr(response, "next_block", None)
        if next_block is None or next_block <= current_from:
            break
        if to_block is not None and next_block >= to_block:
            break
        current_from = next_block

    return sorted(records, key=lambda item: (item["block_number"], item["log_index"] or -1))


def reconstruct_policy_state(plan: ControlTrackingPlan, records: list[PolicyEventRecord]) -> PolicyStateSnapshot:
    public_capabilities: dict[tuple[str, str], PublicCapabilityStateEntry] = {}
    role_capabilities: dict[tuple[int, str, str], RoleCapabilityStateEntry] = {}
    user_roles: dict[tuple[str, int], UserRoleStateEntry] = {}

    for record in sorted(records, key=lambda item: (item["block_number"], item["log_index"] or -1)):
        fields = record["decoded_fields"]
        signature = record["event_signature"]

        if signature == "PublicCapabilityUpdated(address,bytes4,bool)":
            target = str(fields["target"]).lower()
            function_sig = str(fields["functionSig"]).lower()
            public_capabilities[(target, function_sig)] = {
                "target": target,
                "function_sig": function_sig,
                "enabled": bool(fields["enabled"]),
                "last_updated_block": record["block_number"],
                "tx_hash": record["tx_hash"],
            }
            continue

        if signature == "RoleCapabilityUpdated(uint8,address,bytes4,bool)":
            role = _coerce_int(fields["role"])
            target = str(fields["target"]).lower()
            function_sig = str(fields["functionSig"]).lower()
            role_capabilities[(role, target, function_sig)] = {
                "role": role,
                "target": target,
                "function_sig": function_sig,
                "enabled": bool(fields["enabled"]),
                "last_updated_block": record["block_number"],
                "tx_hash": record["tx_hash"],
            }
            continue

        if signature == "UserRoleUpdated(address,uint8,bool)":
            user = str(fields["user"]).lower()
            role = _coerce_int(fields["role"])
            user_roles[(user, role)] = {
                "user": user,
                "role": role,
                "enabled": bool(fields["enabled"]),
                "last_updated_block": record["block_number"],
                "tx_hash": record["tx_hash"],
            }

    return {
        "schema_version": "0.1",
        "contract_address": plan["contract_address"],
        "contract_name": plan["contract_name"],
        "source": "hypersync",
        "event_count": len(records),
        "public_capabilities": sorted(
            public_capabilities.values(), key=lambda item: (item["target"], item["function_sig"])
        ),
        "role_capabilities": sorted(
            role_capabilities.values(),
            key=lambda item: (item["role"], item["target"], item["function_sig"]),
        ),
        "user_roles": sorted(user_roles.values(), key=lambda item: (item["user"], item["role"])),
    }


def write_policy_event_history(records: list[PolicyEventRecord], output_path: Path) -> None:
    with output_path.open("w") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")


def write_policy_state_snapshot(snapshot: PolicyStateSnapshot, output_path: Path) -> None:
    output_path.write_text(json.dumps(snapshot, indent=2) + "\n")


def run_hypersync_policy_backfill(
    plan_path: Path,
    *,
    url: str = DEFAULT_HYPERSYNC_URL,
    bearer_token: str | None = None,
    from_block: int = 0,
    to_block: int | None = None,
    events_out: Path | None = None,
    state_out: Path | None = None,
) -> tuple[Path, Path]:
    plan = load_control_tracking_plan(plan_path)
    bearer_token = bearer_token or os.getenv("ENVIO_API_TOKEN")
    records = asyncio.run(
        fetch_policy_event_history(
            plan,
            url=url,
            bearer_token=bearer_token,
            from_block=from_block,
            to_block=to_block,
        )
    )
    snapshot = reconstruct_policy_state(plan, records)

    events_out = events_out or plan_path.with_name("policy_event_history.jsonl")
    state_out = state_out or plan_path.with_name("policy_state.json")

    write_policy_event_history(records, events_out)
    write_policy_state_snapshot(snapshot, state_out)
    return events_out, state_out


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill tracked policy events with HyperSync.")
    parser.add_argument("plan", help="Path to control_tracking_plan.json")
    parser.add_argument("--url", default=DEFAULT_HYPERSYNC_URL, help="HyperSync URL (default: Ethereum mainnet)")
    parser.add_argument("--token", help="Envio API token. Falls back to ENVIO_API_TOKEN.")
    parser.add_argument("--from-block", type=int, default=0, help="Starting block for historical backfill (default: 0)")
    parser.add_argument("--to-block", type=int, help="Optional exclusive ending block")
    parser.add_argument("--events-out", help="Optional path to policy_event_history.jsonl")
    parser.add_argument("--state-out", help="Optional path to policy_state.json")
    args = parser.parse_args()

    events_path, state_path = run_hypersync_policy_backfill(
        Path(args.plan),
        url=args.url,
        bearer_token=args.token,
        from_block=args.from_block,
        to_block=args.to_block,
        events_out=Path(args.events_out) if args.events_out else None,
        state_out=Path(args.state_out) if args.state_out else None,
    )
    print(f"Policy event history: {events_path}")
    print(f"Policy state snapshot: {state_path}")


if __name__ == "__main__":
    main()
