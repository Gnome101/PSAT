#!/usr/bin/env python3
"""Find the best K-backend combination by coverage of the per-protocol union.

For K in [1, 2, 3], enumerates every combination of backends and reports
which combo maximizes mean-recall (each protocol equal-weighted).
"""

from __future__ import annotations

import argparse
import json
import statistics
from itertools import combinations
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
RESULTS_DIR = ROOT / "bench_results_full"


def load_comparison(out_dir: Path) -> dict[str, Any]:
    return json.loads((out_dir / "comparison.json").read_text())


def protocol_union_urls(protocol_info: dict[str, Any]) -> set[str]:
    return set(protocol_info["union_urls"])


def combo_urls(protocol_info: dict[str, Any], configs: tuple[str, ...]) -> set[str]:
    urls: set[str] = set()
    for cfg in configs:
        stats = protocol_info["per_config"].get(cfg)
        if stats is None:
            continue
        urls |= set(stats["urls"])
    return urls


def combo_mean_recall(comparison: dict[str, Any], configs: tuple[str, ...]) -> tuple[float, dict[str, float]]:
    recalls: list[float] = []
    per_proto: dict[str, float] = {}
    for protocol, info in comparison["protocols"].items():
        union = protocol_union_urls(info)
        if not union:
            continue
        found = combo_urls(info, configs)
        recall = len(found & union) / len(union)
        recalls.append(recall)
        per_proto[protocol] = recall
    return (statistics.mean(recalls) if recalls else 0.0, per_proto)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(RESULTS_DIR))
    parser.add_argument("--max-k", type=int, default=3)
    args = parser.parse_args()

    comparison = load_comparison(Path(args.out))
    all_configs = sorted({cfg for p in comparison["protocols"].values() for cfg in p["per_config"]})

    print(f"available configs: {all_configs}")
    print()

    for k in range(1, args.max_k + 1):
        scored: list[tuple[float, tuple[str, ...]]] = []
        for combo in combinations(all_configs, k):
            mean_recall, _ = combo_mean_recall(comparison, combo)
            scored.append((mean_recall, combo))
        scored.sort(reverse=True)

        print(f"=== K={k} ({'single' if k == 1 else 'combo'}) — top 5 by mean per-protocol recall ===")
        for rank, (score, combo) in enumerate(scored[:5], start=1):
            print(f"  {rank}. {score:>6.1%}  {' + '.join(combo)}")
        print()

    # Also report the ceiling
    all_k = tuple(all_configs)
    ceiling, _ = combo_mean_recall(comparison, all_k)
    print(f"ceiling (union of all {len(all_k)} configs): {ceiling:.1%}")

    # Per-protocol best-combo breakdown for the top-2 combo
    top_2 = combinations(all_configs, 2)
    best_2 = max(top_2, key=lambda c: combo_mean_recall(comparison, c)[0])
    _, per_proto_2 = combo_mean_recall(comparison, best_2)
    print(f"\nbest-pair ({' + '.join(best_2)}) per-protocol recall:")
    for protocol, recall in sorted(per_proto_2.items(), key=lambda kv: -kv[1]):
        print(f"  {protocol:<18} {recall:>5.1%}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
