"""Ingest contract addresses discovered from audit scope extraction.

Merges JSON output from:
- ``/tmp/agent1_addresses.json`` — etherfi deployment registry (GitHub + docs)
- ``/tmp/agent2_addresses.json`` — addresses extracted from audit PDF text

For each ``(address, chain)`` pair:

- If a ``contracts`` row exists:
  - Set ``protocol_id`` to 1 (etherfi) when NULL. Audit coverage's name
    matcher filters by protocol_id — a NULL-protocol row was silently
    invisible.
  - Replace ``contract_name`` when it was NULL or a generic proxy
    placeholder (``UUPSProxy``, ``UpgradeableBeacon``, ``BeaconProxy``),
    since the scope-discovered name is more useful downstream.
- Else insert a new row with ``discovery_source='audit_scope'``.

Idempotent. Pass ``--protocol-id`` to target a different protocol.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import select

from db.models import Contract, SessionLocal

_GENERIC_PROXY_NAMES: frozenset[str] = frozenset(
    {"uupsproxy", "upgradeableproxy", "upgradeablebeacon", "beaconproxy", "transparentupgradeableproxy"}
)


def _load(paths: list[Path]) -> list[dict]:
    merged: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for p in paths:
        if not p.exists():
            print(f"warn: {p} not found, skipping", file=sys.stderr)
            continue
        with p.open() as f:
            entries = json.load(f)
        for e in entries:
            addr = (e.get("address") or "").lower().strip()
            chain = (e.get("chain") or "ethereum").lower().strip()
            name = (e.get("name") or "").strip()
            if not addr or not addr.startswith("0x") or len(addr) != 42:
                continue
            if not name:
                continue
            key = (addr, chain)
            if key in seen:
                continue
            seen.add(key)
            merged.append({"address": addr, "chain": chain, "name": name})
    return merged


def ingest(session, entries: list[dict], *, protocol_id: int) -> dict[str, int]:
    inserted = 0
    adopted = 0  # existing row with NULL protocol_id, now set
    renamed = 0  # replaced generic proxy placeholder with real name
    unchanged = 0

    for entry in entries:
        existing = session.execute(
            select(Contract).where(
                Contract.address == entry["address"],
                Contract.chain == entry["chain"],
            )
        ).scalar_one_or_none()

        if existing is None:
            session.add(
                Contract(
                    address=entry["address"],
                    chain=entry["chain"],
                    contract_name=entry["name"],
                    protocol_id=protocol_id,
                    discovery_source="audit_scope",
                )
            )
            inserted += 1
            continue

        changed = False
        if existing.protocol_id is None:
            existing.protocol_id = protocol_id
            adopted += 1
            changed = True
        current_name = (existing.contract_name or "").lower()
        if current_name in _GENERIC_PROXY_NAMES or not current_name:
            if existing.contract_name != entry["name"]:
                existing.contract_name = entry["name"]
                renamed += 1
                changed = True
        if not changed:
            unchanged += 1

    session.commit()
    return {
        "inserted": inserted,
        "adopted_null_protocol": adopted,
        "renamed_generic_proxy": renamed,
        "unchanged": unchanged,
        "total_entries": len(entries),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol-id", type=int, default=1, help="Target protocol id (default: 1 = etherfi)")
    parser.add_argument(
        "--input",
        action="append",
        default=None,
        help="JSON file path (repeatable). Defaults to /tmp/agent1_addresses.json + /tmp/agent2_addresses.json",
    )
    args = parser.parse_args()

    paths = [Path(p) for p in args.input] if args.input else [
        Path("/tmp/agent1_addresses.json"),
        Path("/tmp/agent2_addresses.json"),
    ]
    entries = _load(paths)

    with SessionLocal() as session:
        stats = ingest(session, entries, protocol_id=args.protocol_id)

    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
