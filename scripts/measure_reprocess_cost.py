"""Week-0 measurement: project reprocess time for the schema-v2 cutover.

Queries the artifacts table for ``stage_timing_static`` rows, decodes
each payload (inline JSONB or via object storage indirection), and
reports percentile latencies plus projected wall-clock at varying
worker concurrency.

Background: the predicate-pipeline rewrite ships as a single schema-v2
cutover. Every existing contract is reprocessed through the new static
stage. Before scoping the cutover window we need real numbers from the
existing static stage; the plan's earlier "~hour" guess was a finger
in the air.

Usage::

    DATABASE_URL=postgresql://psat:psat@localhost:5433/psat \\
      uv run python scripts/measure_reprocess_cost.py

No admin key needed — direct DB access bypasses the API layer.
"""

from __future__ import annotations

import argparse
import statistics
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from db.models import Artifact, Contract, SessionLocal  # noqa: E402
from db.queue import get_artifact  # noqa: E402

CONCURRENCIES = (1, 4, 8, 16)


def _stage_timing_rows(session: Session) -> Iterable[Artifact]:
    return session.execute(select(Artifact).where(Artifact.name == "stage_timing_static")).scalars()


def _elapsed_seconds(payload: Any) -> float | None:
    if not isinstance(payload, dict):
        return None
    val = payload.get("elapsed_s")
    if isinstance(val, (int, float)):
        return float(val)
    return None


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    k = (len(sorted_vals) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def _format_seconds(s: float) -> str:
    if s < 60:
        return f"{s:.1f}s"
    if s < 3600:
        return f"{s / 60:.1f}m"
    if s < 86400:
        return f"{s / 3600:.2f}h"
    return f"{s / 86400:.2f}d"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--concurrencies",
        type=lambda s: [int(x) for x in s.split(",")],
        default=list(CONCURRENCIES),
        help="comma-separated worker counts to project",
    )
    args = parser.parse_args()

    session = SessionLocal()
    try:
        elapsed: list[float] = []
        artifact_size_bytes: list[int] = []
        success_count = 0
        failure_count = 0
        empty_count = 0

        for art in _stage_timing_rows(session):
            payload: Any = art.data
            if payload is None and art.storage_key is not None:
                payload = get_artifact(session, art.job_id, art.name)
            if not isinstance(payload, dict):
                empty_count += 1
                continue
            t = _elapsed_seconds(payload)
            if t is None:
                empty_count += 1
                continue
            elapsed.append(t)
            if isinstance(art.size_bytes, int):
                artifact_size_bytes.append(art.size_bytes)
            if payload.get("status") == "success":
                success_count += 1
            else:
                failure_count += 1

        contract_total = session.execute(select(Contract.id)).scalars().all()
        contract_count = len(contract_total)

        if not elapsed:
            print("No stage_timing_static rows found — nothing to measure.")
            return 1

        p50 = _percentile(elapsed, 0.50)
        p95 = _percentile(elapsed, 0.95)
        max_t = max(elapsed)
        mean = statistics.fmean(elapsed)
        total_serial = sum(elapsed)

        print("Static stage measurements")
        print("-" * 50)
        print(f"  contracts in DB             : {contract_count:>10}")
        print(f"  stage_timing_static rows    : {len(elapsed):>10}")
        print(f"    success                   : {success_count:>10}")
        print(f"    failure                   : {failure_count:>10}")
        print(f"    empty/decode-failed       : {empty_count:>10}")
        print()
        print(f"  p50 stage time              : {_format_seconds(p50)}")
        print(f"  p95 stage time              : {_format_seconds(p95)}")
        print(f"  max stage time              : {_format_seconds(max_t)}")
        print(f"  mean stage time             : {_format_seconds(mean)}")
        print(f"  total serial time           : {_format_seconds(total_serial)}")
        print()

        if artifact_size_bytes:
            total_bytes = sum(artifact_size_bytes)
            print(
                f"  artifact bytes (sample)     : {total_bytes:>10} bytes "
                f"({total_bytes / 1024 / 1024:.1f} MiB across {len(artifact_size_bytes)} rows)"
            )
            print()

        print("Projected reprocess wall-clock (assuming all rerun, perfect parallelism):")
        for n in args.concurrencies:
            proj = total_serial / max(n, 1)
            print(f"  {n:>3} workers : {_format_seconds(proj)}")

        print()
        print("Notes:")
        print("  - Mean × contract_count is the upper bound for a fresh reprocess")
        print(f"    of every contract: {_format_seconds(mean * contract_count)} serial.")
        print("  - Real concurrency is bounded by Slither CPU + RPC. Use this as a")
        print("    floor, expect 1.3-2x in practice from contention/retries.")

        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
