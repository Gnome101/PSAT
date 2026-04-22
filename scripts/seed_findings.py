"""Seed AuditReport.findings with mock data so the surface-view UI has
something to render before Phase 3b wires real LLM extraction.

Target audit ids are ones that already have ``audit_contract_coverage``
rows for etherfi so the UI has a contract to focus on and see the
findings surface through the timeline endpoint.

Each seeded finding:
    {"title": str,
     "severity": "info" | "low" | "medium" | "high" | "critical",
     "status":   "fixed" | "partially_fixed" | "acknowledged"
                 | "mitigated" | "wont_fix",
     "contract_hint": str | None}

``status != "fixed"`` ones show up on the UI as "live findings" on the
current-covered impl. We seed a mix of statuses so the filter logic
can be visibly exercised.

Usage::

    uv run python -m scripts.seed_findings
    # or to clear seeded data:
    uv run python -m scripts.seed_findings --clear
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from sqlalchemy import select

from db.models import AuditReport, SessionLocal

# Mapped to audit ids that have coverage in the etherfi DB after the
# 2026-04-20 discovery run. If an id isn't present (fresh DB, different
# protocol), the seeding just skips it.
_SEEDS: dict[int, list[dict[str, Any]]] = {
    # Certora — Reaudit Core Contracts (2026-01-29)
    3: [
        {
            "title": "Reentrancy guard missing on user-facing withdraw path",
            "severity": "high",
            "status": "fixed",
            "contract_hint": "Treasury",
        },
        {
            "title": "Rounding direction favors user on stETH conversion",
            "severity": "medium",
            "status": "acknowledged",
            "contract_hint": "Treasury",
        },
        {
            "title": "Slashing oracle can be front-run within same block",
            "severity": "medium",
            "status": "mitigated",
            "contract_hint": "StakingCore",
        },
    ],
    # Nethermind — EtherFi Audit (2023-07-05)
    28: [
        {
            "title": "Validator registration allows duplicate pubkeys",
            "severity": "high",
            "status": "fixed",
            "contract_hint": "StakingCore",
        },
        {
            "title": "Fee-on-transfer tokens not explicitly supported",
            "severity": "low",
            "status": "wont_fix",
            "contract_hint": "Treasury",
        },
    ],
    # Omniscia — ETH2.0 (2023-05-16)
    29: [
        {
            "title": "Withdrawal queue may desynchronize under partial exits",
            "severity": "medium",
            "status": "partially_fixed",
            "contract_hint": "WithdrawManager",
        },
        {
            "title": "Initializer gap on proxy reserved storage too small",
            "severity": "low",
            "status": "acknowledged",
            "contract_hint": "WithdrawManager",
        },
    ],
    # Hats Finance — EtherFi Audit (2023-12-20)
    26: [
        {
            "title": "Oracle price can drift during single-block MEV sandwich",
            "severity": "critical",
            "status": "mitigated",
            "contract_hint": "BeHYPE",
        },
        {
            "title": "Gas griefing via unbounded loop in distributor",
            "severity": "medium",
            "status": "fixed",
            "contract_hint": "BeHYPE",
        },
        {
            "title": "Owner can pause only, unpause requires governance",
            "severity": "info",
            "status": "acknowledged",
            "contract_hint": "BeHYPE",
        },
    ],
    # PeckShield — EtherFi Bundle 9 (id 35)
    35: [
        {
            "title": "Missing zero-address check on role assignment",
            "severity": "low",
            "status": "fixed",
            "contract_hint": "DepositAdapter",
        },
        {
            "title": "Timelock delay below industry-standard minimum",
            "severity": "medium",
            "status": "wont_fix",
            "contract_hint": "BeHYPETimelock",
        },
    ],
}


def seed(session, *, clear: bool) -> None:
    audit_ids = list(_SEEDS.keys())
    rows = session.execute(select(AuditReport).where(AuditReport.id.in_(audit_ids))).scalars().all()
    found_ids = {r.id for r in rows}
    missing = [a for a in audit_ids if a not in found_ids]
    if missing:
        print(f"warn: audit ids not found (skipping): {missing}", file=sys.stderr)

    updated = 0
    for row in rows:
        if clear:
            row.findings = None
        else:
            row.findings = _SEEDS[row.id]
        updated += 1
    session.commit()

    action = "cleared" if clear else "seeded"
    print(f"{action} findings on {updated} audit row(s): {sorted(found_ids)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clear", action="store_true", help="Clear seeded findings instead of writing them")
    args = parser.parse_args()

    with SessionLocal() as session:
        seed(session, clear=args.clear)


if __name__ == "__main__":
    main()
