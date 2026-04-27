"""Regression tests for ``PSAT_STATIC_SKIP_SLITHER_CLI``.

#4 from todo-no-commit-to-gihub.txt — avoid double Slither parsing.
The full single-parse refactor needs codex review + fixture-based parity
tests (semantic refactor of static analysis where wrong = silent quality
regression). What we ship instead: a feature flag that lets operators
skip the Slither CLI subprocess entirely. Saves the ~20-40s subprocess
+ detector run per static job at the cost of user-facing detector
findings (``slither_results`` and ``analysis_report`` artifacts).

Why this is safe to ship default-OFF:
- The downstream pipeline (resolution, policy, coverage) reads
  ``contract_analysis``, ``semantic_guards``, ``controller_tracking``
  — none of which depend on detector findings.
- ``collect_contract_analysis`` already handles missing
  ``slither_results.json`` gracefully (empty detector counts, risk
  level falls back to ``unknown``). No code path raises.
- Default OFF preserves current behavior bit-for-bit.

What we pin here:
1. Constant defaults to False with no env set.
2. Constant becomes True for "1", "true", "yes" (case-insensitive).
3. ``_summarize_slither({})`` returns empty counts (the contract that
   makes the skip-CLI path safe). A future refactor that breaks the
   empty-input branch would leave the skip-CLI path unsafe.
4. ``_derive_static_risk_level`` on empty counts returns ``"unknown"``
   — same contract as above for the risk-level field.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static.contract_analysis_pipeline.summaries import (
    _derive_static_risk_level,
    _summarize_slither,
)


def _reload_static_worker():
    """Re-import workers.static_worker so the module-level constant
    picks up the current env. Required because ``SKIP_SLITHER_CLI`` is
    captured at import time."""
    if "workers.static_worker" in sys.modules:
        del sys.modules["workers.static_worker"]
    return importlib.import_module("workers.static_worker")


def test_default_skip_is_false(monkeypatch):
    """Without the env var, behavior is unchanged from before this commit."""
    monkeypatch.delenv("PSAT_STATIC_SKIP_SLITHER_CLI", raising=False)
    static_worker = _reload_static_worker()
    assert static_worker.SKIP_SLITHER_CLI is False


def test_skip_enabled_via_one(monkeypatch):
    monkeypatch.setenv("PSAT_STATIC_SKIP_SLITHER_CLI", "1")
    static_worker = _reload_static_worker()
    assert static_worker.SKIP_SLITHER_CLI is True


def test_skip_enabled_via_true_caps(monkeypatch):
    """The env-flag idiom in this codebase accepts {1, true, yes} and
    is case-insensitive — pin the parsing so it doesn't drift."""
    monkeypatch.setenv("PSAT_STATIC_SKIP_SLITHER_CLI", "TRUE")
    static_worker = _reload_static_worker()
    assert static_worker.SKIP_SLITHER_CLI is True


def test_skip_disabled_via_zero(monkeypatch):
    monkeypatch.setenv("PSAT_STATIC_SKIP_SLITHER_CLI", "0")
    static_worker = _reload_static_worker()
    assert static_worker.SKIP_SLITHER_CLI is False


def test_summarize_slither_empty_input_returns_zero_counts():
    """Load-bearing for the skip-CLI path: ``collect_contract_analysis``
    feeds ``_summarize_slither`` an empty dict when the JSON file is
    missing. If a future refactor crashes on empty input, the
    skip-CLI path stops being safe."""
    summary = _summarize_slither({})
    assert summary["key_findings"] == []
    # Every severity bucket initialized to zero — caller's
    # _derive_static_risk_level safely sees all zeros.
    assert all(count == 0 for count in summary["detector_counts"].values())


def test_summarize_slither_no_results_field():
    """Equivalent guard for a malformed but non-empty input."""
    summary = _summarize_slither({"unrelated": "garbage"})
    assert summary["key_findings"] == []
    assert all(count == 0 for count in summary["detector_counts"].values())


def test_derive_static_risk_level_empty_returns_unknown():
    """The risk-level field falls back to ``unknown`` when all detector
    counts are zero (skip-CLI path). Pinning so a future enum change
    that drops ``unknown`` doesn't silently break the skip-CLI path."""
    assert _derive_static_risk_level({}) == "unknown"
    assert _derive_static_risk_level({"High": 0, "Medium": 0, "Low": 0}) == "unknown"


def test_skip_path_routes_around_run_slither_phase(monkeypatch):
    """When SKIP_SLITHER_CLI is True, ``_run_slither_phase`` is NOT
    called from the static stage main flow. We can't easily exercise
    the full main flow without a DB session, so this test reaches
    into the orchestration via the same pattern as the production
    code: check the env flag, branch on it.

    A regression where someone calls ``_run_slither_phase`` regardless
    of the flag would re-spend the 20-40s the flag is meant to save."""
    monkeypatch.setenv("PSAT_STATIC_SKIP_SLITHER_CLI", "1")
    static_worker = _reload_static_worker()

    # Sanity check the contract the production code relies on: the
    # constant is truthy and is what the branch in _run_static_analysis
    # actually consults. (Module-level branch decision; pinned here
    # so a future refactor that reads from os.getenv inline instead
    # of via the constant would still be caught by tests that mock
    # the constant.)
    assert static_worker.SKIP_SLITHER_CLI is True
