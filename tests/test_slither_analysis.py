"""Tests for services/static/slither.py — run_slither, format_report, analyze."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from services.static.slither import analyze, format_report, run_slither


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _completed(stdout: str = "", stderr: str = "", returncode: int = 0):
    """Return a ``subprocess.CompletedProcess`` with the given fields."""
    return subprocess.CompletedProcess(
        args=["slither"], returncode=returncode, stdout=stdout, stderr=stderr
    )


SAMPLE_DETECTORS = [
    {
        "check": "reentrancy-eth",
        "impact": "High",
        "confidence": "Medium",
        "description": "Reentrancy in Contract.withdraw()",
    },
    {
        "check": "unused-return",
        "impact": "Medium",
        "confidence": "Medium",
        "description": "Return value of transfer not checked",
    },
    {
        "check": "naming-convention",
        "impact": "Informational",
        "confidence": "High",
        "description": "Variable name does not follow convention",
    },
]


def _slither_json(detectors: list[dict] | None = None) -> dict:
    """Build a minimal Slither JSON output."""
    return {"results": {"detectors": detectors or []}}


# ---------------------------------------------------------------------------
# run_slither
# ---------------------------------------------------------------------------


class TestRunSlither:
    """Tests for the ``run_slither`` helper."""

    def test_first_command_succeeds(self, tmp_path, monkeypatch):
        """When the first command returns valid JSON, return parsed output."""
        expected = _slither_json(SAMPLE_DETECTORS)
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda *a, **kw: _completed(stdout=json.dumps(expected)),
        )
        result = run_slither(tmp_path)
        assert result == expected

    def test_fallback_to_second_command(self, tmp_path, monkeypatch):
        """First command produces no output; second succeeds."""
        expected = _slither_json(SAMPLE_DETECTORS)
        calls: list[list[str]] = []

        def _fake_run(cmd, *, capture_output, text, cwd):
            calls.append(cmd)
            if len(calls) == 1:
                return _completed(stdout="", stderr="compilation error")
            return _completed(stdout=json.dumps(expected))

        monkeypatch.setattr(subprocess, "run", _fake_run)
        result = run_slither(tmp_path)
        assert result == expected
        assert len(calls) == 2

    def test_both_commands_fail_raises(self, tmp_path, monkeypatch):
        """Both commands produce no output → RuntimeError."""
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda *a, **kw: _completed(stdout="", stderr="fail", returncode=1),
        )
        with pytest.raises(RuntimeError, match="no JSON output"):
            run_slither(tmp_path)

    def test_invalid_json_raises(self, tmp_path, monkeypatch):
        """Both commands return non-JSON stdout → RuntimeError."""
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda *a, **kw: _completed(stdout="NOT-JSON{{{"),
        )
        with pytest.raises(RuntimeError, match="Failed to parse"):
            run_slither(tmp_path)


# ---------------------------------------------------------------------------
# format_report
# ---------------------------------------------------------------------------


class TestFormatReport:
    """Tests for ``format_report``."""

    def test_no_detectors_no_issues(self):
        """Empty detector list → 'No issues found.'."""
        report = format_report(_slither_json([]), "MyToken", "0xABC")
        assert "No issues found" in report
        assert "MyToken" in report
        assert "0xABC" in report

    def test_multiple_findings_sorted_by_severity(self):
        """Findings should appear in High → Medium → Informational order."""
        report = format_report(_slither_json(SAMPLE_DETECTORS), "Vault", "0x123")

        high_pos = report.index("[HIGH]")
        medium_pos = report.index("[MEDIUM]")
        info_pos = report.index("[INFORMATIONAL]")

        assert high_pos < medium_pos < info_pos
        assert "Summary: 3 finding(s)" in report

    def test_single_finding_all_fields(self):
        """A single detector populates check, confidence, and description."""
        detector = {
            "check": "arbitrary-send-eth",
            "impact": "High",
            "confidence": "High",
            "description": "Contract sends ETH to arbitrary user",
        }
        report = format_report(_slither_json([detector]), "Token", "0xDEF")
        assert "arbitrary-send-eth" in report
        assert "confidence: High" in report
        assert "Contract sends ETH to arbitrary user" in report
        assert "Summary: 1 finding(s)" in report


# ---------------------------------------------------------------------------
# analyze (full integration)
# ---------------------------------------------------------------------------


class TestAnalyze:
    """Tests for ``analyze`` which orchestrates run_slither + format_report."""

    def test_full_flow(self, tmp_path, monkeypatch):
        """analyze runs slither, writes report + JSON, returns report path."""
        slither_data = _slither_json(SAMPLE_DETECTORS)
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda *a, **kw: _completed(stdout=json.dumps(slither_data)),
        )

        report_path = analyze(tmp_path, "Vault", "0xBEEF")

        # returned path is correct
        assert report_path == tmp_path / "analysis_report.txt"
        assert report_path.exists()

        # text report has expected content
        text = report_path.read_text()
        assert "Vault" in text
        assert "0xBEEF" in text
        assert "reentrancy-eth" in text

        # JSON file written with correct content
        json_path = tmp_path / "slither_results.json"
        assert json_path.exists()
        assert json.loads(json_path.read_text()) == slither_data
