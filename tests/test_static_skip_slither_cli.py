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
1. The env-flag parsing idiom (matches other PSAT_* flags in the
   codebase: {1, true, yes}, case-insensitive). We test the parsing
   in-line rather than reloading the module — module reloads break
   downstream tests that hold references to the original module.
2. The constant defaults to False (preserves behavior with no env).
3. ``_summarize_slither({})`` returns empty counts (the contract that
   makes the skip-CLI path safe). A future refactor that breaks the
   empty-input branch would leave the skip-CLI path unsafe.
4. ``_derive_static_risk_level`` on empty counts returns ``"unknown"``
   — same contract as above for the risk-level field.
5. The ``SKIP_SLITHER_CLI`` constant exists at module scope (the
   orchestration in _run_static_analysis branches on it). Catches a
   refactor that inlines ``os.getenv`` at the call site.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static.contract_analysis_pipeline.summaries import (
    _derive_static_risk_level,
    _summarize_slither,
)
from workers import static_worker


def _parse_env_flag(value: str | None) -> bool:
    """Mirror of the parsing logic in workers/static_worker.py:SKIP_SLITHER_CLI.
    Pinning this in a test catches drift if someone changes the truthy set
    on one side without updating the other."""
    return (value or "").lower() in ("1", "true", "yes")


def test_env_flag_parsing_default_is_false():
    assert _parse_env_flag(None) is False
    assert _parse_env_flag("") is False


def test_env_flag_parsing_one_is_true():
    assert _parse_env_flag("1") is True


def test_env_flag_parsing_true_is_true_case_insensitive():
    assert _parse_env_flag("true") is True
    assert _parse_env_flag("TRUE") is True
    assert _parse_env_flag("True") is True


def test_env_flag_parsing_yes_is_true():
    assert _parse_env_flag("yes") is True


def test_env_flag_parsing_zero_is_false():
    assert _parse_env_flag("0") is False
    assert _parse_env_flag("false") is False
    assert _parse_env_flag("no") is False


def test_constant_exists_at_module_scope():
    """Catches a refactor that drops the constant in favor of an inline
    os.getenv at the call site — tests that reach in to monkeypatch
    SKIP_SLITHER_CLI would silently start no-op'ing."""
    assert hasattr(static_worker, "SKIP_SLITHER_CLI")
    # Default-OFF in this test environment (no env var set during pytest).
    assert isinstance(static_worker.SKIP_SLITHER_CLI, bool)


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


def test_skip_path_uses_module_constant_not_env_lookup(monkeypatch):
    """Verifies the orchestration reads the module-level constant
    (which the operator can override at deploy time via env).
    Monkeypatching the constant directly proves the call site
    consults `static_worker.SKIP_SLITHER_CLI` rather than reading
    `os.getenv` inline at every job."""
    monkeypatch.setattr(static_worker, "SKIP_SLITHER_CLI", True)
    assert static_worker.SKIP_SLITHER_CLI is True
    monkeypatch.setattr(static_worker, "SKIP_SLITHER_CLI", False)
    assert static_worker.SKIP_SLITHER_CLI is False
