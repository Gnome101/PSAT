"""Schema-v2 cutover dry-run: fleet-wide aggregator.

Runs ``cutover_check_for_address`` on every completed Job in the
database (or a filtered set) and produces a structured report:

  * per-severity counts (clean / new_coverage / role_drift /
    regression / not_eligible)
  * sorted list of regressions (the addresses that BLOCK cutover)
  * sorted list of role_drifts (need human review)
  * total fleet size + safe-to-cut percentage

Exit code:
  0 — every eligible address is safe-to-cut (clean OR new_coverage)
  1 — at least one regression — cutover MUST NOT proceed for that subset
  2 — no eligible addresses (run static analyses with v2 emit first)

Usage::

    DATABASE_URL=postgresql://psat:psat@localhost:5433/psat \\
      uv run python scripts/cutover_dry_run.py

    # JSON output for CI consumption
    uv run python scripts/cutover_dry_run.py --json > cutover.json

    # Filter by address prefix
    uv run python scripts/cutover_dry_run.py --address-prefix 0xabc

    # Top-N regressions only
    uv run python scripts/cutover_dry_run.py --max-regressions 20

No admin key needed — direct DB access bypasses the API layer.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

# Allow running as `python scripts/cutover_dry_run.py` from the repo root.
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))

from sqlalchemy import select  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from db.models import Job, JobStatus, SessionLocal  # noqa: E402
from services.static.contract_analysis_pipeline.cutover_check import (  # noqa: E402
    cutover_check_for_address,
    is_safe_to_cut_over,
)


def _iter_completed_jobs(session: Session, *, address_prefix: str | None = None) -> list[Job]:
    """Distinct addresses with at least one completed Job. Returns
    one Job per address (the most-recent one — same selection rule
    cutover_check_for_address itself uses)."""
    stmt = (
        select(Job)
        .where(Job.status == JobStatus.completed)
        .order_by(Job.address, Job.updated_at.desc(), Job.created_at.desc())
    )
    if address_prefix:
        stmt = stmt.where(Job.address.ilike(f"{address_prefix.lower()}%"))
    seen: set[str] = set()
    out: list[Job] = []
    for job in session.execute(stmt).scalars():
        addr = (job.address or "").lower()
        if addr in seen:
            continue
        seen.add(addr)
        out.append(job)
    return out


def run_dry_run(
    session: Session,
    *,
    address_prefix: str | None = None,
    max_regressions: int | None = None,
) -> dict[str, Any]:
    """Execute the cutover check across every distinct address with
    a completed Job. Returns an aggregated report."""
    counts: Counter[str] = Counter()
    regressions: list[dict[str, Any]] = []
    role_drifts: list[dict[str, Any]] = []
    not_eligible: list[str] = []
    safe_addresses: list[str] = []

    jobs = _iter_completed_jobs(session, address_prefix=address_prefix)
    for job in jobs:
        addr = (job.address or "").lower()
        report = cutover_check_for_address(session, address=addr)
        if report is None:
            counts["not_eligible"] += 1
            not_eligible.append(addr)
            continue
        severity = report["severity"]
        counts[severity] += 1
        if severity == "regression":
            regressions.append(_compact(report))
        elif severity == "role_drift":
            role_drifts.append(_compact(report))
        elif is_safe_to_cut_over(report):
            safe_addresses.append(addr)

    if max_regressions is not None:
        regressions = regressions[:max_regressions]

    total = len(jobs)
    safe = counts.get("clean", 0) + counts.get("new_coverage", 0)
    eligible = total - counts.get("not_eligible", 0)
    return {
        "total_addresses": total,
        "eligible": eligible,
        "not_eligible_count": counts.get("not_eligible", 0),
        "counts": dict(counts),
        "safe_to_cut_count": safe,
        "safe_pct": (safe / eligible * 100.0) if eligible else 0.0,
        "regressions": regressions,
        "role_drifts": role_drifts,
        "not_eligible_sample": not_eligible[:20],
        "safe_addresses": safe_addresses,
    }


def _compact(report: dict[str, Any]) -> dict[str, Any]:
    """Drop fields that bloat the dry-run output without adding
    value at the fleet-aggregate layer."""
    return {
        "address": report["address"],
        "contract_name": report["contract_name"],
        "severity": report["severity"],
        "agreed": report["agreed"],
        "v1_only": report["v1_only"],
        "v2_only": report["v2_only"],
        "role_disagreements": report["role_disagreements"],
    }


def _format_text(report: dict[str, Any]) -> str:
    """Human-readable summary. CI-greppable enough but still
    scannable on a terminal."""
    lines: list[str] = []
    lines.append(f"Cutover dry-run — {report['total_addresses']} addresses ({report['eligible']} eligible)")
    lines.append("=" * 72)
    counts = report["counts"]
    for key in ("clean", "new_coverage", "role_drift", "regression", "not_eligible"):
        n = counts.get(key, 0)
        lines.append(f"  {key:<14}  {n}")
    safe_pct = report["safe_pct"]
    lines.append("")
    lines.append(f"Safe to cut over: {report['safe_to_cut_count']} / {report['eligible']} eligible ({safe_pct:.1f}%)")
    if report["regressions"]:
        lines.append("")
        lines.append(f"Regressions ({len(report['regressions'])}):")
        for entry in report["regressions"]:
            lines.append(f"  {entry['address']}  {entry['contract_name'] or '<unnamed>'}  v1_only={entry['v1_only']}")
    if report["role_drifts"]:
        lines.append("")
        lines.append(f"Role drifts ({len(report['role_drifts'])}):")
        for entry in report["role_drifts"]:
            lines.append(
                f"  {entry['address']}  {entry['contract_name'] or '<unnamed>'}  "
                f"disagreements={list(entry['role_disagreements'].keys())}"
            )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address-prefix", default=None, help="Filter to addresses with this 0x-prefix")
    parser.add_argument("--max-regressions", type=int, default=None, help="Cap regression list size")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of human-readable text")
    args = parser.parse_args()

    with SessionLocal() as session:
        report = run_dry_run(
            session,
            address_prefix=args.address_prefix,
            max_regressions=args.max_regressions,
        )

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(_format_text(report))

    if report["eligible"] == 0:
        return 2
    if report["counts"].get("regression", 0) > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
