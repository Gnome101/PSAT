#!/usr/bin/env python3
"""Compare full-pipeline AI-search benchmark results across backends.

Reads bench_results_full/<protocol>/<backend>__<mode>.json and computes
per-protocol recall/precision against the union of audit URLs across all
6 configs (raw 5 + Exa Deep Research).

Outputs bench_results_full/comparison.json and a terminal summary.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "bench_results_full"


def _normalize_url(url: str) -> str:
    u = urlsplit(url.strip())
    netloc = u.netloc.lower().removeprefix("www.")
    path = u.path.rstrip("/")
    return f"{u.scheme.lower()}://{netloc}{path}"


def _config_label(backend: str, mode: str) -> str:
    return f"{backend}__{mode}"


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
        by_protocol[r["protocol"]].append(r)

    per_protocol: dict[str, dict[str, Any]] = {}
    overall_found: dict[str, set[str]] = defaultdict(set)

    for protocol, proto_runs in by_protocol.items():
        union: set[str] = set()
        per_config: dict[str, dict[str, Any]] = {}
        for r in proto_runs:
            cfg = _config_label(r["backend"], r["mode"])
            found = {_normalize_url(rep["url"]) for rep in r.get("reports", []) if rep.get("url")}
            per_config[cfg] = {
                "report_count": len(r.get("reports", [])),
                "unique_urls": len(found),
                "errors": len(r.get("errors", [])),
                "elapsed_ms": r.get("elapsed_ms"),
                "urls": sorted(found),
            }
            union |= found
            overall_found[cfg] |= {f"{protocol}::{u}" for u in found}

        for cfg, stats in per_config.items():
            urls_found = set(stats["urls"])
            missed = sorted(union - urls_found)
            stats["recall"] = round(len(urls_found) / len(union), 4) if union else 0.0
            stats["missed_vs_union"] = missed
        per_protocol[protocol] = {
            "union_size": len(union),
            "union_urls": sorted(union),
            "per_config": per_config,
        }

    full_union: set[str] = set()
    for urls in overall_found.values():
        full_union |= urls

    overall = {}
    for cfg, urls in overall_found.items():
        overall[cfg] = {
            "total_classified_urls": len(urls),
            "recall_vs_union": round(len(urls) / len(full_union), 4) if full_union else 0.0,
        }

    return {
        "protocols": per_protocol,
        "overall": overall,
        "global_union_size": len(full_union),
    }


def print_summary(report: dict[str, Any]) -> None:
    print("\n=== OVERALL (full pipeline, all 12 protocols) ===")
    overall = report["overall"]
    ranked = sorted(overall.items(), key=lambda kv: -kv[1]["total_classified_urls"])
    union_size = report["global_union_size"]
    print(f"global union of audit URLs: {union_size}")
    print(f"{'config':<22} {'unique_found':>14} {'recall':>8}")
    for cfg, stats in ranked:
        print(f"{cfg:<22} {stats['total_classified_urls']:>14} {stats['recall_vs_union']:>7.1%}")

    print("\n=== PER-PROTOCOL RECALL ===")
    configs = sorted({cfg for p in report["protocols"].values() for cfg in p["per_config"]})
    header = f"{'protocol':<22}" + "".join(f"{cfg:>22}" for cfg in configs)
    print(header)
    for protocol, data in report["protocols"].items():
        row = f"{protocol:<22}"
        for cfg in configs:
            stats = data["per_config"].get(cfg)
            if stats is None:
                row += f"{'--':>22}"
            else:
                row += f"{stats['unique_urls']}/{data['union_size']} ({stats['recall']:>4.0%})".rjust(22)
        print(row)


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
