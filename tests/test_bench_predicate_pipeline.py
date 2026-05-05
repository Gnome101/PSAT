"""Unit tests for the pure helpers in scripts/bench_predicate_pipeline.py.

Mirrors test_measure_reprocess_cost.py — the bench script's percentile
math + summary aggregation drive the v9-plan PERF go/no-go reading.
The Slither-touching paths (``_bench_one_fixture``, ``run_benchmark``)
exercise the full pipeline at operational time; the helpers below are
testable in isolation and worth pinning so a refactor doesn't silently
change the percentile algorithm or the build_meets_target boolean.

Note the bench uses **nearest-rank** percentile (not the linear-
interpolation variant in measure_reprocess_cost). Both are valid;
pinning the bench's specific behavior locks the published numbers.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.bench_predicate_pipeline import (  # noqa: E402
    TARGET_BUILD_P99_MS,
    TARGET_EVAL_P99_MS,
    _pct,
    _percentiles,
)

# ---------------------------------------------------------------------------
# _pct — nearest-rank single-percentile
# ---------------------------------------------------------------------------


def test_pct_empty_returns_zero():
    assert _pct([], 50) == 0.0
    assert _pct([], 99) == 0.0


def test_pct_single_element():
    assert _pct([42.0], 50) == 42.0
    assert _pct([42.0], 99) == 42.0


def test_pct_picks_correct_rank_on_100_samples():
    sorted_samples = [float(i) for i in range(100)]
    # Nearest-rank: rank = round(p/100 * n). For n=100, p=99:
    # rank = max(1, round(99.0)) = 99 → 1-indexed → sorted[98] = 98.0
    assert _pct(sorted_samples, 99) == 98.0
    # p=50, n=100 → rank=50, sorted[49] = 49.0
    assert _pct(sorted_samples, 50) == 49.0


def test_pct_clamps_to_last_element_at_p100():
    # Defensive: even if someone passes p=100, we don't index out of
    # bounds. The min(rank-1, n-1) clause handles it.
    sorted_samples = [1.0, 2.0, 3.0]
    assert _pct(sorted_samples, 100) == 3.0


def test_pct_minimum_rank_is_1():
    # The max(1, ...) clause ensures p=0 doesn't yield rank=0 (which
    # would be sorted[-1] = last). Instead it yields sorted[0].
    assert _pct([10.0, 20.0, 30.0], 0) == 10.0


# ---------------------------------------------------------------------------
# _percentiles — full percentile bundle
# ---------------------------------------------------------------------------


def test_percentiles_empty_returns_zeros():
    out = _percentiles([])
    assert out == {"p50": 0.0, "p95": 0.0, "p99": 0.0, "n": 0}


def test_percentiles_includes_all_required_fields_on_non_empty():
    out = _percentiles([1.0, 2.0, 3.0, 4.0, 5.0])
    assert {"p50", "p95", "p99", "n", "min", "max", "mean"} <= set(out.keys())
    assert out["n"] == 5
    assert out["min"] == 1.0
    assert out["max"] == 5.0


def test_percentiles_p50_matches_statistics_median():
    # 5 samples → median is the middle one
    out = _percentiles([10.0, 20.0, 30.0, 40.0, 50.0])
    assert out["p50"] == 30.0
    # Even-length: statistics.median averages the two middle values
    out = _percentiles([10.0, 20.0, 30.0, 40.0])
    assert out["p50"] == 25.0


def test_percentiles_handles_unsorted_input():
    out = _percentiles([3.0, 1.0, 4.0, 1.0, 5.0])
    assert out["min"] == 1.0
    assert out["max"] == 5.0
    assert out["p50"] == 3.0  # median


def test_percentiles_mean_is_arithmetic():
    out = _percentiles([1.0, 2.0, 3.0, 4.0, 5.0])
    assert out["mean"] == 3.0


# ---------------------------------------------------------------------------
# Targets — pin the canonical thresholds the v9 plan refers to
# ---------------------------------------------------------------------------


def test_target_thresholds_pinned():
    """The v9 plan targets are: predicate-build < 5ms p99 per function
    and resolver evaluator < 50ms p99 per function. These constants
    drive build_meets_target / eval_meets_target in the summary; a
    refactor that ratchets them up silently would let a real regression
    pass CI."""
    assert TARGET_BUILD_P99_MS == 5.0
    assert TARGET_EVAL_P99_MS == 50.0
