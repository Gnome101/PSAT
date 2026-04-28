"""Run bench_workers.py against a sequence of addresses to validate warm-cache amortization.

Submits a small fleet of diverse contracts back-to-back (no --force so caches
warm naturally), then prints a per-cascade table and first-half-vs-second-half
comparison.

Usage:
    set -a; source .env; set +a
    uv run python scripts/bench_sequence.py \\
        --url https://psat-bench.fly.dev \\
        --label seq-warm-cache \\
        --fly-app psat-bench
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Diverse set: simple ERC20s + proxies + the cascade-heavy etherfi LP.
# Order is deliberate — first few are cold, later ones should hit the
# bytecode/Etherscan/classify caches warmed by earlier cascades.
DEFAULT_ADDRESSES = [
    ("WETH", "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
    ("USDC", "0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48"),
    ("UNI", "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984"),
    ("etherfi-LP", "0x308861A430be4cce5502d0A12724771Fc6DaF216"),
    ("DAI", "0x6B175474E89094C44Da98b954EedeAC495271d0F"),
    ("LINK", "0x514910771AF9Ca656af840dff83E8264EcF986CA"),
    ("MKR", "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2"),
    ("WBTC", "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"),
]


def run_one(args: argparse.Namespace, name: str, address: str, idx: int, total: int) -> dict | None:
    label = f"{args.label}-{idx:02d}-{name}"
    print(f"\n[{idx}/{total}] {name} ({address}) → label={label}", flush=True)
    cmd = [
        "uv",
        "run",
        "python",
        "scripts/bench_workers.py",
        "--url",
        args.url,
        "--address",
        address,
        "--label",
        label,
        "--admin-key",
        args.admin_key,
        "--follow-all-jobs",
        "--timeout",
        str(args.timeout),
    ]
    if args.fly_app:
        cmd += ["--fly-app", args.fly_app]
    if args.force:
        cmd += ["--force"]

    start = time.monotonic()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    wall = time.monotonic() - start
    if proc.returncode != 0:
        print(f"  ! bench_workers failed (rc={proc.returncode}); stdout/stderr tail:", flush=True)
        print(proc.stdout[-1000:], flush=True)
        print(proc.stderr[-1000:], flush=True)
        return None

    # bench_workers writes to bench_results/runs/<ts>_<label>_<jobid>.json.
    # Find the latest matching file by label.
    runs_dir = Path("bench_results/runs")
    candidates = sorted(runs_dir.glob(f"*_{label}_*.json"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        print(f"  ! no result file found for label={label}", flush=True)
        return None
    result = json.loads(candidates[-1].read_text())
    cascade_total = result.get("cascade_total_seconds")
    job_count = result.get("cascade_job_count", 0)
    print(
        f"  ✓ wall={wall:.1f}s  cascade_total={cascade_total}s  jobs={job_count}",
        flush=True,
    )
    return {
        "name": name,
        "address": address,
        "label": label,
        "wall_s": round(wall, 2),
        "cascade_total_s": cascade_total,
        "cascade_job_count": job_count,
        "result_file": str(candidates[-1]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--label", default="seq")
    parser.add_argument("--fly-app", default=None)
    parser.add_argument("--admin-key", default=os.getenv("PSAT_ADMIN_KEY", ""))
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Pass --force to each cascade (defeats warm-cache; use only for cold-cascade comparison).",
    )
    parser.add_argument(
        "--addresses-file",
        default=None,
        help="Path to a JSON list of [name, address] pairs; defaults to the built-in fleet.",
    )
    args = parser.parse_args()
    if not args.admin_key:
        print("PSAT_ADMIN_KEY not set; pass --admin-key or source .env first.", file=sys.stderr)
        return 2

    fleet = DEFAULT_ADDRESSES
    if args.addresses_file:
        fleet = [(item[0], item[1]) for item in json.loads(Path(args.addresses_file).read_text())]

    summaries: list[dict] = []
    for i, (name, addr) in enumerate(fleet, start=1):
        s = run_one(args, name, addr, i, len(fleet))
        if s is not None:
            summaries.append(s)

    # Aggregate.
    out_dir = Path("bench_results/sequences")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"{ts}_{args.label}.json"

    halves_split = max(1, len(summaries) // 2)
    first_half = summaries[:halves_split]
    second_half = summaries[halves_split:]

    def avg(rows: list[dict], key: str) -> float | None:
        vals = [r[key] for r in rows if isinstance(r.get(key), (int, float))]
        return round(sum(vals) / len(vals), 2) if vals else None

    summary = {
        "label": args.label,
        "url": args.url,
        "force": args.force,
        "fleet_size": len(fleet),
        "completed": len(summaries),
        "results": summaries,
        "first_half_avg_cascade_s": avg(first_half, "cascade_total_s"),
        "second_half_avg_cascade_s": avg(second_half, "cascade_total_s"),
        "first_half_avg_jobs": avg(first_half, "cascade_job_count"),
        "second_half_avg_jobs": avg(second_half, "cascade_job_count"),
        "total_wall_s": round(sum(r["wall_s"] for r in summaries), 2),
    }
    out_path.write_text(json.dumps(summary, indent=2))

    print("\n" + "=" * 80)
    print(f"Wrote {out_path}")
    print("Per-cascade results:")
    print(f"{'#':>2} {'name':12} {'cascade_s':>10} {'jobs':>5} {'wall_s':>8}")
    for i, r in enumerate(summaries, start=1):
        ct = r.get("cascade_total_s")
        ct_s = f"{ct:.1f}" if isinstance(ct, (int, float)) else "?"
        print(f"{i:>2} {r['name']:12} {ct_s:>10} {r['cascade_job_count']:>5} {r['wall_s']:>8.1f}")
    print(f"\nFirst {halves_split} avg cascade_total: {summary['first_half_avg_cascade_s']}s")
    print(f"Last  {len(summaries) - halves_split} avg cascade_total: {summary['second_half_avg_cascade_s']}s")
    if summary["first_half_avg_cascade_s"] and summary["second_half_avg_cascade_s"]:
        delta = summary["second_half_avg_cascade_s"] - summary["first_half_avg_cascade_s"]
        pct = 100 * delta / summary["first_half_avg_cascade_s"]
        sign = "FASTER" if delta < 0 else "SLOWER"
        print(f"Warm-cache delta: {delta:+.1f}s ({pct:+.1f}%) — second half {sign}")
    print(f"Total wall time: {summary['total_wall_s']:.1f}s ({summary['total_wall_s'] / 60:.1f} min)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
