"""Unit tests for the pure helpers in scripts/measure_reprocess_cost.py.

The script is the operator's pre-flip evidence: it reads
``stage_timing_static`` artifacts and projects fleet-wide reprocess
wall-clock at varying concurrencies. The DB-touching paths
(``_stage_timing_rows`` and ``main``) are exercised end-to-end at
operational time, but the math + payload decoding is testable
without a DB and worth pinning so a refactor doesn't silently change
the percentile or unit conversions the operator's go/no-go decision
depends on.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.measure_reprocess_cost import (  # noqa: E402
    _elapsed_seconds,
    _format_seconds,
    _percentile,
)

# ---------------------------------------------------------------------------
# _elapsed_seconds — payload decoding
# ---------------------------------------------------------------------------


def test_elapsed_seconds_extracts_float_field():
    assert _elapsed_seconds({"elapsed_s": 1.5}) == 1.5


def test_elapsed_seconds_coerces_int_to_float():
    out = _elapsed_seconds({"elapsed_s": 3})
    assert out == 3.0
    assert isinstance(out, float)


def test_elapsed_seconds_returns_none_for_non_dict():
    assert _elapsed_seconds(None) is None
    assert _elapsed_seconds([1, 2, 3]) is None
    assert _elapsed_seconds("not a dict") is None
    # Defensive: protect against a future schema where elapsed_s
    # accidentally lands as a string ("1.5") — the script must skip
    # rather than crash, so the percentile math doesn't get a sentinel
    # value.
    assert _elapsed_seconds({"elapsed_s": "1.5"}) is None


def test_elapsed_seconds_returns_none_when_field_missing():
    assert _elapsed_seconds({}) is None
    assert _elapsed_seconds({"other_key": 1.5}) is None


# ---------------------------------------------------------------------------
# _percentile — interpolated nearest-rank
# ---------------------------------------------------------------------------


def test_percentile_empty_returns_zero():
    assert _percentile([], 0.5) == 0.0
    assert _percentile([], 0.99) == 0.0


def test_percentile_single_element():
    assert _percentile([42.0], 0.5) == 42.0
    assert _percentile([42.0], 0.99) == 42.0


def test_percentile_p50_matches_median_on_odd_length():
    # Odd length: median is the middle element; with linear
    # interpolation, p50 lands on it cleanly.
    assert _percentile([1.0, 2.0, 3.0, 4.0, 5.0], 0.5) == 3.0


def test_percentile_p50_interpolates_on_even_length():
    # Even length: linear interpolation between rank-2 and rank-3 (1-indexed).
    # k = (4 - 1) * 0.5 = 1.5 → between sorted[1]=2 and sorted[2]=3 → 2.5
    assert _percentile([1.0, 2.0, 3.0, 4.0], 0.5) == pytest.approx(2.5)


def test_percentile_p99_extreme():
    # 100 samples: p99 is between sorted[98] and sorted[99].
    samples = [float(i) for i in range(100)]
    p99 = _percentile(samples, 0.99)
    assert 98.0 <= p99 <= 99.0


def test_percentile_handles_unsorted_input():
    # Inputs not pre-sorted — the helper sorts.
    assert _percentile([3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0], 0.5) == pytest.approx(3.5)


# ---------------------------------------------------------------------------
# _format_seconds — unit conversions for human-readable projections
# ---------------------------------------------------------------------------


def test_format_seconds_under_minute_rounds_to_decisecond():
    assert _format_seconds(0.5) == "0.5s"
    assert _format_seconds(45.123) == "45.1s"


def test_format_seconds_minute_boundary():
    assert _format_seconds(60.0) == "1.0m"
    assert _format_seconds(150.0) == "2.5m"


def test_format_seconds_hour_boundary():
    assert _format_seconds(3600.0) == "1.00h"
    assert _format_seconds(7200.0) == "2.00h"


def test_format_seconds_day_boundary():
    assert _format_seconds(86400.0) == "1.00d"
    assert _format_seconds(86400.0 * 7) == "7.00d"


def test_format_seconds_units_switch_at_thresholds():
    # Documents that boundaries flip at exact 60s / 3600s / 86400s.
    # If a refactor moves the threshold, this test catches it
    # immediately so the operator's reading of e.g. "120s" vs "2m"
    # doesn't silently change.
    assert _format_seconds(59.99) == "60.0s"
    assert _format_seconds(60.0) == "1.0m"
    assert _format_seconds(3599.0) == "60.0m"
    assert _format_seconds(3600.0) == "1.00h"
    assert _format_seconds(86399.0) == "24.00h"
    assert _format_seconds(86400.0) == "1.00d"
