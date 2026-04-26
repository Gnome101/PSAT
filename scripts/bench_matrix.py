"""Run the worker bench across a matrix of Fly VM configurations.

Reads a JSON config describing N runs (each with vm size, memory, count
overrides for the workers process group), applies each config to a Fly
app, waits for the workers machines to come back up, then invokes
scripts/bench_workers.py to time the pipeline.

The expected target is `psat-bench` — a dedicated app provisioned for
this purpose so we never reshape prod or PR-preview VMs. Pointing this
at any other app is allowed but the script will warn unless --i-know.

Usage:

    PSAT_ADMIN_KEY=... uv run python scripts/bench_matrix.py \\
        --config bench_configs/etherfi_lp.json

Config schema (see bench_configs/etherfi_lp.json):

    {
      "app": "psat-bench",
      "url": "https://psat-bench.fly.dev",
      "address": "0x308861A430be4cce5502d0A12724771Fc6DaF216",
      "label_prefix": "etherfi-lp",
      "warm_wait_s": 20,
      "samples_per_run": 1,
      "runs": [
        {
          "label": "shared-2x-2gb",
          "vm_size": "shared-cpu-2x",
          "memory_mb": 2048,
          "count": 1
        },
        ...
      ]
    }

Any of `vm_size`, `memory_mb`, `count` may be omitted to leave that
dimension untouched. The workers process group is the only one scaled
— web/browser/monitor are left alone.
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

ALLOWED_APP_DEFAULT = "psat-bench"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def run(cmd: list[str], *, check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess:
    """Run a subprocess and stream its output for visibility."""
    print(f"[matrix] $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.stdout.strip():
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
    if result.stderr.strip():
        print(result.stderr, end="" if result.stderr.endswith("\n") else "\n", file=sys.stderr)
    if check and result.returncode != 0:
        raise RuntimeError(f"command failed (exit={result.returncode}): {' '.join(cmd)}")
    return result


def list_machines_in_group(app: str, group: str) -> list[dict]:
    """Return [{id, state}, ...] for all machines in the given process group."""
    out = run(["fly", "machines", "list", "-a", app, "--json"], check=True, timeout=30).stdout
    machines = json.loads(out)
    return [
        {"id": m["id"], "state": m["state"]}
        for m in machines
        if m.get("config", {}).get("metadata", {}).get("fly_process_group") == group
    ]


def apply_scale(
    app: str,
    *,
    vm_size: str | None,
    memory_mb: int | None,
    count: int | None,
    process_group: str = "workers",
) -> None:
    """Apply scale changes to a process group. Each dimension is independent.

    Defaults to "workers" (the consolidated group from start_workers.sh). For
    split-worker experiments using start_workers_heavy.sh + start_workers_light.sh,
    the matrix config supplies a list of per-group scale specs and the runner
    calls this once per group.
    """
    if vm_size is not None:
        run(["fly", "scale", "vm", vm_size, "--process-group", process_group, "-a", app])
    if memory_mb is not None:
        run(["fly", "scale", "memory", str(memory_mb), "--process-group", process_group, "-a", app])
    if count is not None:
        run(["fly", "scale", "count", f"{process_group}={count}", "-a", app, "--yes"])


def wait_for_group_started(app: str, group: str, *, timeout_s: int = 180) -> None:
    """Block until every machine in the given process group reports state=started.

    `fly scale vm/memory` recreates each machine — they go through
    stopped/starting before reaching started. With multiple machines this
    is a rolling restart.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        machines = list_machines_in_group(app, group)
        if not machines:
            print(f"[matrix] no {group} machines yet, waiting...")
        else:
            states = {m["state"] for m in machines}
            print(f"[matrix] {group} states: {sorted(states)} ({len(machines)} machines)")
            if states == {"started"}:
                return
            # Kick any stragglers — `fly scale` doesn't always auto-start what it created.
            for m in machines:
                if m["state"] == "stopped":
                    run(["fly", "machine", "start", m["id"], "-a", app], check=False, timeout=30)
        time.sleep(5)
    raise TimeoutError(f"workers not all started after {timeout_s}s")


def run_one_bench(
    *,
    url: str,
    address: str,
    admin_key: str,
    label: str,
    fly_app: str | None,
    out_path: Path,
    poll_interval: float,
    force: bool = False,
    follow_all_jobs: bool = False,
    subprocess_timeout_s: int = 2400,
) -> dict:
    """Invoke bench_workers.py as a subprocess. Returns its parsed JSON output."""
    here = Path(__file__).parent
    cmd = [
        sys.executable,
        str(here / "bench_workers.py"),
        "--url",
        url,
        "--address",
        address,
        "--admin-key",
        admin_key,
        "--label",
        label,
        "--out",
        str(out_path),
        "--poll-interval",
        str(poll_interval),
    ]
    if fly_app:
        cmd.extend(["--fly-app", fly_app])
    if force:
        cmd.append("--force")
    if follow_all_jobs:
        cmd.append("--follow-all-jobs")
    # Stream child output directly so the user sees progress.
    proc = subprocess.run(cmd, text=True, timeout=subprocess_timeout_s)
    if proc.returncode != 0:
        raise RuntimeError(f"bench_workers.py exited with {proc.returncode}")
    return json.loads(out_path.read_text())


def wipe_bench_db(database_url: str) -> None:
    """DELETE FROM jobs on the bench database. Idempotent.

    Required between cascade matrix runs so each VM config starts from
    an empty queue — otherwise inherited stragglers from the prior run
    pollute the comparison.
    """
    print("[matrix] wiping bench DB jobs table for clean cascade start...")
    proc = subprocess.run(
        ["psql", database_url, "-c", "DELETE FROM jobs;"],
        env={**os.environ, "PGCONNECT_TIMEOUT": "15"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"DB wipe failed: {proc.stderr.strip()}")
    print(f"[matrix] {proc.stdout.strip() or 'DELETE issued'}")


def render_table(results: list[dict]) -> str:
    """Markdown table for the comparison summary.

    If any result has cascade_total_seconds, the table grows a `cascade` column
    that shows the wall-clock to reach all-jobs-terminal (etherfi-LP-style runs).
    """
    has_cascade = any(r.get("bench", {}).get("cascade_total_seconds") is not None for r in results)
    header = ["label", "vm", "mem", "count", "parent total", "discovery", "static", "resolution", "policy", "coverage"]
    if has_cascade:
        header.extend(["cascade total", "cascade jobs"])
    lines = [
        "| " + " | ".join(header) + " |",
        "|" + "|".join(["---"] * len(header)) + "|",
    ]
    for r in results:
        cfg = r.get("config", {})
        bench = r.get("bench", {})
        polled = bench.get("stage_elapsed_polled_seconds", {})
        worker = bench.get("worker_elapsed_seconds", {})

        def fmt(stage: str) -> str:
            p = polled.get(stage)
            w = worker.get(stage)
            if p is None and w is None:
                return "—"
            if w is not None and p is not None:
                return f"{p:.1f}s ({w:.1f}w)"
            return f"{p or w:.1f}s"

        row = [
            r["label"],
            str(cfg.get("vm_size", "—")),
            str(cfg.get("memory_mb", "—")),
            str(cfg.get("count", "—")),
            f"{bench.get('total_seconds', '—')}s",
            fmt("discovery"),
            fmt("static"),
            fmt("resolution"),
            fmt("policy"),
            fmt("coverage"),
        ]
        if has_cascade:
            cascade = bench.get("cascade_total_seconds")
            cascade_n = bench.get("cascade_job_count")
            row.append(f"{cascade}s" if cascade is not None else "—")
            row.append(str(cascade_n) if cascade_n is not None else "—")
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", required=True, help="Path to JSON matrix config")
    parser.add_argument("--admin-key", default=os.environ.get("PSAT_ADMIN_KEY"))
    parser.add_argument("--out-dir", default=None, help="Override output directory")
    parser.add_argument(
        "--i-know",
        action="store_true",
        help="Bypass the safety check that requires app=psat-bench (for prod/preview targeting)",
    )
    parser.add_argument("--skip-scale", action="store_true", help="Skip Fly scaling steps (just rerun bench)")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("PSAT_BENCH_DATABASE_URL"),
        help=(
            "Postgres URL for the bench DB. Used to wipe jobs between runs when "
            "the config sets wipe_db_between_runs=true."
        ),
    )
    args = parser.parse_args()

    if not args.admin_key:
        print("error: --admin-key or PSAT_ADMIN_KEY required", file=sys.stderr)
        return 2

    cfg = json.loads(Path(args.config).read_text())
    app = cfg.get("app")
    url = cfg.get("url")
    address = cfg.get("address")
    label_prefix = cfg.get("label_prefix", "matrix")
    warm_wait_s = int(cfg.get("warm_wait_s", 20))
    samples = int(cfg.get("samples_per_run", 1))
    runs = cfg.get("runs", [])

    if not (url and address and runs):
        print("error: config must have url, address, and runs[]", file=sys.stderr)
        return 2
    if app and app != ALLOWED_APP_DEFAULT and not args.i_know:
        print(
            f"error: refusing to scale app '{app}' without --i-know "
            f"(safety: only '{ALLOWED_APP_DEFAULT}' is allowed by default)",
            file=sys.stderr,
        )
        return 2

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else Path("bench_results/runs") / f"matrix_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[matrix] writing results to {out_dir}")

    results: list[dict] = []
    matrix_started = now_iso()

    wipe_db = bool(cfg.get("wipe_db_between_runs", False))
    if wipe_db and not args.database_url:
        print(
            "error: config has wipe_db_between_runs=true but --database-url / PSAT_BENCH_DATABASE_URL not set",
            file=sys.stderr,
        )
        return 2
    subprocess_timeout_s = int(cfg.get("subprocess_timeout_s", 2400))

    for run_idx, rc in enumerate(runs, start=1):
        label = f"{label_prefix}_{rc['label']}"
        print(f"\n========== run {run_idx}/{len(runs)}: {label} ==========")

        if wipe_db:
            wipe_bench_db(args.database_url)

        if app and not args.skip_scale:
            # Two shapes for `rc`:
            #   - flat: vm_size/memory_mb/count target the default "workers" group
            #   - groups: dict of {group_name: {vm_size, memory_mb, count}} for split-worker tests
            groups_spec = rc.get("groups")
            if groups_spec:
                for group_name, spec in groups_spec.items():
                    apply_scale(
                        app,
                        vm_size=spec.get("vm_size"),
                        memory_mb=spec.get("memory_mb"),
                        count=spec.get("count"),
                        process_group=group_name,
                    )
                for group_name in groups_spec:
                    wait_for_group_started(app, group_name)
            else:
                apply_scale(
                    app,
                    vm_size=rc.get("vm_size"),
                    memory_mb=rc.get("memory_mb"),
                    count=rc.get("count"),
                )
                wait_for_group_started(app, "workers")
            print(f"[matrix] warm-wait {warm_wait_s}s for workers to fully boot...")
            time.sleep(warm_wait_s)

        # When force=true (set per-config or top-level), every sample bypasses cache.
        force_per_run = bool(rc.get("force", cfg.get("force", False)))
        for sample_i in range(1, samples + 1):
            sample_label = label if samples == 1 else f"{label}_s{sample_i}"
            out_path = out_dir / f"{sample_label}.json"
            try:
                bench = run_one_bench(
                    url=url,
                    address=address,
                    admin_key=args.admin_key,
                    label=sample_label,
                    fly_app=app,
                    out_path=out_path,
                    poll_interval=float(cfg.get("poll_interval", 0.5)),
                    force=force_per_run,
                    follow_all_jobs=bool(cfg.get("follow_all_jobs", False)),
                    subprocess_timeout_s=subprocess_timeout_s,
                )
            except Exception as e:
                print(f"[matrix] run failed: {e}", file=sys.stderr)
                bench = {"error": str(e)}
            results.append({"label": sample_label, "config": rc, "sample": sample_i, "bench": bench})

    # Save aggregate summary
    summary = {
        "config_file": str(args.config),
        "app": app,
        "url": url,
        "address": address,
        "matrix_started_at": matrix_started,
        "matrix_completed_at": now_iso(),
        "results": results,
    }
    (out_dir / "_summary.json").write_text(json.dumps(summary, indent=2))
    table = render_table(results)
    (out_dir / "_summary.md").write_text(f"# Matrix bench: {label_prefix}\n\n{table}\n")

    print("\n========== matrix done ==========")
    print(table)
    print(f"\n[matrix] artifacts: {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
