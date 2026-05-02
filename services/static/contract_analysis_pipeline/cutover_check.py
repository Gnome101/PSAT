"""Per-contract cutover gate-keeper for #18.

Loads BOTH the v1 ``contract_analysis`` artifact and the v2
``predicate_trees`` artifact stored by the static worker for the
most recent completed analysis of an address, then runs the
existing ``diff_artifacts`` harness and returns a structured
report the operator (or a CI gate) can consume to decide whether
flipping v2 readers is safe for that contract.

The decision matrix the operator runs against the report:

  * ``severity == "regression"`` — v1 saw guards v2 missed.
    BLOCK the cutover; extend the v2 pipeline or pin the v1 path
    on this contract until the gap is closed.
  * ``severity == "new_coverage"`` — v2 catches gates v1 missed.
    Safe to cut over; v1 was loose, v2 is strictly more thorough.
  * ``severity == "role_drift"`` — same set of guarded functions
    but classification disagrees on at least one. Review case by
    case; the typed v2 role is usually the more accurate label.
  * ``severity == "clean"`` — exact agreement. Flip away.

Returns ``None`` when the contract isn't ready for diffing
(no completed Job, no v1 artifact, or no v2 artifact). Callers
should treat ``None`` as "not eligible for cutover yet" rather
than "safe."
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import Job, JobStatus
from db.queue import get_artifact

from .v1_v2_diff import classify_diff_severity, diff_artifacts


def cutover_check_for_address(
    session: Session, *, address: str
) -> dict[str, Any] | None:
    """Run the v1 + v2 diff for the latest completed analysis of
    ``address``. Returns a JSON-ready dict::

        {
          "address": "0x...",
          "job_id": "<uuid>",
          "severity": "regression" | "new_coverage" | "role_drift" | "clean",
          "agreed": [...],
          "v1_only": [...],
          "v2_only": [...],
          "role_disagreements": {fn: [v1_kinds, v2_roles]},
          "contract_name": "..."
        }

    Or ``None`` when prerequisites aren't met.
    """
    addr = address.lower()
    job = session.execute(
        select(Job)
        .where(Job.address == addr)
        .where(Job.status == JobStatus.completed)
        .order_by(Job.updated_at.desc(), Job.created_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    if job is None:
        return None

    v1 = get_artifact(session, job.id, "contract_analysis")
    if not isinstance(v1, dict):
        return None
    v2 = get_artifact(session, job.id, "predicate_trees")
    if not isinstance(v2, dict):
        return None

    report = diff_artifacts(v1, v2)
    severity = classify_diff_severity(report)

    return {
        "address": addr,
        "job_id": str(job.id),
        "severity": severity,
        "contract_name": report.contract_name,
        "agreed": sorted(report.agreed),
        "v1_only": sorted(report.v1_only),
        "v2_only": sorted(report.v2_only),
        "role_disagreements": {
            fn: {"v1_guard_kinds": kinds, "v2_authority_roles": roles}
            for fn, (kinds, roles) in report.role_disagreements.items()
        },
    }


def is_safe_to_cut_over(report: dict[str, Any] | None) -> bool:
    """Cutover policy: clean OR new_coverage are green-lights;
    regression and role_drift require human review. Use this as
    the boolean gate in CI / cutover scripts."""
    if report is None:
        return False
    return report["severity"] in ("clean", "new_coverage")


__all__ = [
    "cutover_check_for_address",
    "is_safe_to_cut_over",
]
