#!/usr/bin/env python3
"""Compare address-discovery benchmark results across backends.

Reads bench_results_addresses/<protocol>/<backend>__<mode>.json and
computes per-protocol + overall recall against the union of found
0x addresses across all configs.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
RESULTS_DIR = ROOT / "bench_results_addresses"


def _config_label(backend: str, mode: str) -> str:
    return f"{backend}__{mode}"


def _addresses_from(record: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for c in record.get("contracts", []):
        addr = str(c.get("address") or "").strip().lower()
        if addr.startswith("0x") and len(addr) == 42:
            out.add(addr)
    return out


def load_runs(out_dir: Path) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for proto_dir in sorted(out_dir.iterdir()):
        if not proto_dir.is_dir():
            continue
        for json_path in sorted(proto_dir.glob("*.json")):
            runs.append(json.loads(json_path.read_text()))
    return runs


def compare(runs: list[dict[str, Any]]) -> dict[str, Any]:
    by_protocol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in runs:
        if "protocol" not in r:
            continue
        by_protocol[r["protocol"]].append(r)

    per_protocol: dict[str, dict[str, Any]] = {}
    for protocol, proto_runs in by_protocol.items():
        union: set[str] = set()
        per_config: dict[str, dict[str, Any]] = {}
        for r in proto_runs:
            cfg = _config_label(r["backend"], r["mode"])
            found = _addresses_from(r)
            per_config[cfg] = {
                "address_count": len(found),
                "errors": len(r.get("errors", [])),
                "elapsed_ms": r.get("elapsed_ms"),
                "addresses": sorted(found),
            }
            union |= found
        for cfg, stats in per_config.items():
            addrs = set(stats["addresses"])
            stats["recall"] = round(len(addrs) / len(union), 4) if union else 0.0
            stats["missed"] = sorted(union - addrs)
        per_protocol[protocol] = {
            "union_size": len(union),
            "union": sorted(union),
            "per_config": per_config,
        }

    return {"protocols": per_protocol}


def print_summary(report: dict[str, Any]) -> None:
    configs = sorted({c for p in report["protocols"].values() for c in p["per_config"]})
    recalls: dict[str, list[float]] = {c: [] for c in configs}
    wins: dict[str, float] = {c: 0.0 for c in configs}

    for protocol, info in report["protocols"].items():
        for c in configs:
            recalls[c].append(info["per_config"][c]["recall"])
        best = max(info["per_config"][c]["recall"] for c in configs)
        winners = [c for c in configs if info["per_config"][c]["recall"] == best and best > 0]
        for w in winners:
            wins[w] += 1 / len(winners)

    print(f"\n{'config':<20} {'mean':>7} {'median':>8} {'stdev':>7} {'min':>6} {'max':>6} {'wins':>6}")
    print("-" * 68)
    for c in sorted(configs, key=lambda x: -statistics.mean(recalls[x])):
        r = recalls[c]
        print(
            f"{c:<20} {statistics.mean(r):>6.1%} {statistics.median(r):>7.1%} "
            f"{statistics.stdev(r) if len(r) > 1 else 0:>6.1%} "
            f"{min(r):>5.1%} {max(r):>5.1%} {wins[c]:>5.1f}"
        )

    print(f"\n{'protocol':<14} {'union':>6} " + " ".join(f"{c.replace('__', '/'):>18}" for c in configs))
    for protocol, info in sorted(report["protocols"].items(), key=lambda kv: -kv[1]["union_size"]):
        union = info["union_size"]
        row = f"{protocol:<14} {union:>6}"
        best_recall = max(info["per_config"][c]["recall"] for c in configs)
        for c in configs:
            stats = info["per_config"][c]
            cell = f"{stats['address_count']}/{union} {stats['recall']:.0%}"
            if stats["recall"] == best_recall and best_recall > 0:
                cell = "*" + cell
            row += f"{cell:>19}"
        print(row)

    print("\n=== COMBOS (top 3 by mean per-protocol recall) ===")
    for k in range(1, 4):
        scored: list[tuple[float, tuple[str, ...]]] = []
        for combo in combinations(configs, k):
            per_proto = []
            for protocol, info in report["protocols"].items():
                union = set(info["union"])
                if not union:
                    continue
                found: set[str] = set()
                for c in combo:
                    found |= set(info["per_config"][c]["addresses"])
                per_proto.append(len(found & union) / len(union))
            scored.append((statistics.mean(per_proto) if per_proto else 0.0, combo))
        scored.sort(reverse=True)
        print(f"K={k}:")
        for rank, (score, combo) in enumerate(scored[:3], 1):
            print(f"  {rank}. {score:>6.1%}  {' + '.join(combo)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(RESULTS_DIR))
    args = parser.parse_args()
    out_dir = Path(args.out)
    runs = load_runs(out_dir)
    if not runs:
        print(f"no runs found in {out_dir}", file=sys.stderr)
        return 1
    report = compare(runs)
    (out_dir / "comparison.json").write_text(json.dumps(report, indent=2))
    print_summary(report)
    print(f"\nwrote {out_dir / 'comparison.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
