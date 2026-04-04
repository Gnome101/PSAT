"""Integration tests for StaticWorker._run_slither_phase and _run_analysis_phase.

These exercise the real phase methods with mocked external tools (Slither CLI,
analyze_contract) so they can run in CI without any Solidity toolchain.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from workers.static_worker import StaticWorker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _job(**overrides):
    payload = {
        "id": "job-1",
        "address": "0xABCDABCDABCDABCDABCDABCDABCDABCDABCDABCD",
        "name": "TestContract",
        "request": {"rpc_url": "https://rpc.example"},
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _capture_store_artifact(monkeypatch):
    """Patch store_artifact and return a list that collects all calls."""
    calls: list[dict] = []

    def _fake_store(_session, _job_id, name, data=None, text_data=None):
        calls.append({"name": name, "data": data, "text_data": text_data})

    monkeypatch.setattr("workers.static_worker.store_artifact", _fake_store)
    return calls


# ---------------------------------------------------------------------------
# _run_slither_phase
# ---------------------------------------------------------------------------


class TestSlitherPhaseSuccess:
    """Mock analyze() to succeed; verify artifacts are read and stored."""

    def test_stores_slither_results_and_analysis_report(self, monkeypatch, tmp_path):
        worker = StaticWorker()
        monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)
        session = MagicMock()
        job = _job()

        # Pre-write the files that Slither would produce
        slither_data = {"success": True, "results": {"detectors": []}}
        (tmp_path / "slither_results.json").write_text(json.dumps(slither_data))
        report_text = "No major issues found."
        (tmp_path / "analysis_report.txt").write_text(report_text)

        # analyze() does nothing (files already on disk)
        monkeypatch.setattr("workers.static_worker.analyze", lambda *a, **kw: None)
        monkeypatch.setattr("workers.static_worker.is_vyper_project", lambda *a, **kw: False)

        calls = _capture_store_artifact(monkeypatch)

        result = worker._run_slither_phase(session, job, tmp_path, "TestContract", job.address)

        assert result is True

        stored_names = [c["name"] for c in calls]
        assert "slither_results" in stored_names
        assert "analysis_report" in stored_names

        sr = next(c for c in calls if c["name"] == "slither_results")
        assert sr["data"] == slither_data

        ar = next(c for c in calls if c["name"] == "analysis_report")
        assert ar["text_data"] == report_text

    def test_succeeds_without_report_file(self, monkeypatch, tmp_path):
        """If analyze() succeeds but only slither_results.json exists (no report), that's fine."""
        worker = StaticWorker()
        monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)
        session = MagicMock()
        job = _job()

        slither_data = {"success": True, "results": {"detectors": [{"check": "reentrancy"}]}}
        (tmp_path / "slither_results.json").write_text(json.dumps(slither_data))
        # No analysis_report.txt on disk

        monkeypatch.setattr("workers.static_worker.analyze", lambda *a, **kw: None)
        monkeypatch.setattr("workers.static_worker.is_vyper_project", lambda *a, **kw: False)
        calls = _capture_store_artifact(monkeypatch)

        result = worker._run_slither_phase(session, job, tmp_path, "TestContract", job.address)

        assert result is True
        stored_names = [c["name"] for c in calls]
        assert "slither_results" in stored_names
        assert "analysis_report" not in stored_names


class TestSlitherPhaseFailure:
    """Mock analyze() to raise; verify error artifact and return value."""

    def test_stores_slither_error_on_runtime_error(self, monkeypatch, tmp_path):
        worker = StaticWorker()
        monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)
        session = MagicMock()
        job = _job()

        def _raise(*a, **kw):
            raise RuntimeError("solc version mismatch")

        monkeypatch.setattr("workers.static_worker.analyze", _raise)
        monkeypatch.setattr("workers.static_worker.is_vyper_project", lambda *a, **kw: False)
        calls = _capture_store_artifact(monkeypatch)

        result = worker._run_slither_phase(session, job, tmp_path, "TestContract", job.address)

        assert result is False

        assert len(calls) == 1
        assert calls[0]["name"] == "slither_error"
        assert "solc version mismatch" in calls[0]["data"]["error"]

    def test_stores_slither_error_on_generic_exception(self, monkeypatch, tmp_path):
        worker = StaticWorker()
        monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)
        session = MagicMock()
        job = _job()

        def _raise(*a, **kw):
            raise Exception("unexpected failure")

        monkeypatch.setattr("workers.static_worker.analyze", _raise)
        monkeypatch.setattr("workers.static_worker.is_vyper_project", lambda *a, **kw: False)
        calls = _capture_store_artifact(monkeypatch)

        result = worker._run_slither_phase(session, job, tmp_path, "TestContract", job.address)

        assert result is False
        assert calls[0]["name"] == "slither_error"
        assert "unexpected failure" in calls[0]["data"]["error"]


class TestSlitherPhaseVyperSkip:
    """Vyper projects should skip Slither entirely."""

    def test_skips_slither_for_vyper_via_vy_file(self, monkeypatch, tmp_path):
        worker = StaticWorker()
        monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)
        session = MagicMock()
        job = _job()

        # Create a .vy file so is_vyper_project() returns True
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "Vault.vy").write_text("# @version ^0.3.7\n")

        analyze_called = []
        monkeypatch.setattr(
            "workers.static_worker.analyze",
            lambda *a, **kw: analyze_called.append(True),
        )
        calls = _capture_store_artifact(monkeypatch)

        result = worker._run_slither_phase(session, job, tmp_path, "Vault", job.address)

        assert result is False
        assert analyze_called == [], "analyze() must NOT be called for Vyper projects"

        assert len(calls) == 1
        assert calls[0]["name"] == "slither_error"
        assert "Skipped Slither for Vyper source" in calls[0]["data"]["error"]

    def test_skips_slither_for_vyper_via_version_pragma(self, monkeypatch, tmp_path):
        """is_vyper_project also detects files starting with '# @version'."""
        worker = StaticWorker()
        monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)
        session = MagicMock()
        job = _job()

        # Write a file without .vy extension but with Vyper version pragma
        (tmp_path / "Contract.txt").write_text("ignore me")
        (tmp_path / "Curve.sol").write_text("# @version ^0.3.7\n@external\ndef foo():\n    pass\n")

        analyze_called = []
        monkeypatch.setattr(
            "workers.static_worker.analyze",
            lambda *a, **kw: analyze_called.append(True),
        )
        calls = _capture_store_artifact(monkeypatch)

        result = worker._run_slither_phase(session, job, tmp_path, "Curve", job.address)

        assert result is False
        assert analyze_called == []
        assert calls[0]["name"] == "slither_error"


# ---------------------------------------------------------------------------
# _run_analysis_phase
# ---------------------------------------------------------------------------


class TestAnalysisPhaseSuccess:
    """Mock analyze_contract() to return a path; verify artifact is stored."""

    def test_stores_contract_analysis_artifact(self, monkeypatch, tmp_path):
        worker = StaticWorker()
        monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)
        session = MagicMock()
        job = _job()

        analysis_data = {
            "schema_version": "0.1",
            "subject": {"name": "TestContract"},
            "summary": {"control_model": "ownable"},
        }
        analysis_path = tmp_path / "contract_analysis.json"
        analysis_path.write_text(json.dumps(analysis_data))

        monkeypatch.setattr(
            "workers.static_worker.analyze_contract",
            lambda project_dir: analysis_path,
        )
        calls = _capture_store_artifact(monkeypatch)

        result = worker._run_analysis_phase(session, job, tmp_path, "TestContract", job.address)

        assert result is True
        assert len(calls) == 1
        assert calls[0]["name"] == "contract_analysis"
        assert calls[0]["data"] == analysis_data

    def test_succeeds_even_when_returned_path_missing(self, monkeypatch, tmp_path):
        """If analyze_contract returns a path that doesn't exist, phase still returns True
        (no artifact stored, but no error either)."""
        worker = StaticWorker()
        monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)
        session = MagicMock()
        job = _job()

        missing_path = tmp_path / "nonexistent.json"
        monkeypatch.setattr(
            "workers.static_worker.analyze_contract",
            lambda project_dir: missing_path,
        )
        calls = _capture_store_artifact(monkeypatch)

        result = worker._run_analysis_phase(session, job, tmp_path, "TestContract", job.address)

        assert result is True
        assert len(calls) == 0, "No artifact should be stored when file does not exist"


class TestAnalysisPhaseFailure:
    """Mock analyze_contract() to raise; verify error artifact and return value."""

    def test_stores_analysis_error_on_exception(self, monkeypatch, tmp_path):
        worker = StaticWorker()
        monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)
        session = MagicMock()
        job = _job()

        def _raise(project_dir):
            raise RuntimeError("LLM analysis timed out")

        monkeypatch.setattr("workers.static_worker.analyze_contract", _raise)
        calls = _capture_store_artifact(monkeypatch)

        result = worker._run_analysis_phase(session, job, tmp_path, "TestContract", job.address)

        assert result is False
        assert len(calls) == 1
        assert calls[0]["name"] == "analysis_error"
        assert "LLM analysis timed out" in calls[0]["data"]["error"]

    def test_stores_analysis_error_on_generic_exception(self, monkeypatch, tmp_path):
        worker = StaticWorker()
        monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)
        session = MagicMock()
        job = _job()

        def _raise(project_dir):
            raise ValueError("bad json from model")

        monkeypatch.setattr("workers.static_worker.analyze_contract", _raise)
        calls = _capture_store_artifact(monkeypatch)

        result = worker._run_analysis_phase(session, job, tmp_path, "TestContract", job.address)

        assert result is False
        assert calls[0]["name"] == "analysis_error"
        assert "bad json from model" in calls[0]["data"]["error"]
