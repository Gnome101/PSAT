"""Tests for DefiLlamaWorker — process() paths and edge cases."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from workers.base import JobHandledDirectly
from workers.defillama_worker import DefiLlamaWorker

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ADDR_1 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDR_2 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
ADDR_3 = "0xcccccccccccccccccccccccccccccccccccccccc"
PROTOCOL = "aave-v3"


def _job(**overrides: Any) -> SimpleNamespace:
    payload: dict[str, Any] = {
        "id": uuid.uuid4(),
        "address": None,
        "name": None,
        "company": None,
        "request": {"defillama_protocol": PROTOCOL},
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _scan_result(
    addresses: list[str] | None = None,
    address_details: list[dict] | None = None,
) -> dict:
    """Return a minimal scan_protocol result."""
    addrs = addresses or []
    return {
        "addresses": addrs,
        "scan_time": 1.23,
        "address_details": address_details or [],
    }


def _patch_worker_deps(monkeypatch: pytest.MonkeyPatch) -> dict[str, list]:
    """Patch all external dependencies of DefiLlamaWorker.process() and return trackers."""
    store_calls: list[tuple[str, Any]] = []
    create_calls: list[dict] = []
    complete_calls: list[tuple] = []

    def fake_store(session, job_id, name, data=None, text_data=None):
        store_calls.append((name, data))

    def fake_create(session, request_dict, initial_stage=None):
        child = SimpleNamespace(id=uuid.uuid4(), request=request_dict)
        create_calls.append(request_dict)
        return child

    def fake_complete(session, job_id, detail=""):
        complete_calls.append((job_id, detail))

    def fake_count(session, root_job_id):
        return 0

    def fake_update_detail(session, job_id, detail):
        pass

    monkeypatch.setattr("workers.defillama_worker.store_artifact", fake_store)
    monkeypatch.setattr("workers.defillama_worker.create_job", fake_create)
    monkeypatch.setattr("workers.defillama_worker.complete_job", fake_complete)
    monkeypatch.setattr("workers.defillama_worker.count_analysis_children", fake_count)
    monkeypatch.setattr("workers.base.update_job_detail", fake_update_detail)

    return {
        "store_calls": store_calls,
        "create_calls": create_calls,
        "complete_calls": complete_calls,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMissingProtocol:
    """process() raises ValueError when defillama_protocol is missing."""

    def test_missing_protocol_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        job = _job(request={})
        _patch_worker_deps(monkeypatch)

        with pytest.raises(ValueError, match="defillama_protocol"):
            worker.process(session, cast(Any, job))

    def test_none_request_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        job = _job(request=None)
        _patch_worker_deps(monkeypatch)

        with pytest.raises(ValueError, match="defillama_protocol"):
            worker.process(session, cast(Any, job))


class TestHappyPath:
    """Full successful scan with addresses, chain details, and child jobs."""

    def test_creates_children_and_stores_artifacts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        # session.execute(...).scalar_one_or_none() -> None  (no existing jobs)
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job()

        trackers = _patch_worker_deps(monkeypatch)

        details = [
            {"address": ADDR_1, "chain": "ethereum"},
            {"address": ADDR_2, "chain": "polygon"},
        ]
        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(
                addresses=[ADDR_1, ADDR_2],
                address_details=details,
            ),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # Three artifacts stored: defillama_full_scan, defillama_scan_results, discovery_summary
        stored_names = [name for name, _ in trackers["store_calls"]]
        assert "defillama_full_scan" in stored_names
        assert "defillama_scan_results" in stored_names
        assert "discovery_summary" in stored_names

        # Two child jobs created
        assert len(trackers["create_calls"]) == 2

        # complete_job called
        assert len(trackers["complete_calls"]) == 1


class TestChainFromAddressDetails:
    """Child jobs get chain from address_details when available."""

    def test_chain_from_details(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job(request={"defillama_protocol": PROTOCOL, "chain": "fallback-chain"})

        trackers = _patch_worker_deps(monkeypatch)

        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(
                addresses=[ADDR_1],
                address_details=[{"address": ADDR_1, "chain": "ethereum"}],
            ),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # Child should have chain from address_details, not the fallback
        assert trackers["create_calls"][0]["chain"] == "ethereum"


class TestChainFallback:
    """Child jobs use request chain when address_details has no chain."""

    def test_falls_back_to_request_chain(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job(request={"defillama_protocol": PROTOCOL, "chain": "arbitrum"})

        trackers = _patch_worker_deps(monkeypatch)

        # address_details has no chain for the address
        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(
                addresses=[ADDR_1],
                address_details=[{"address": ADDR_1}],  # no chain key
            ),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert trackers["create_calls"][0]["chain"] == "arbitrum"

    def test_no_chain_at_all_omits_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When neither address_details nor request has chain, child_request has no chain key."""
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job(request={"defillama_protocol": PROTOCOL})  # no chain in request

        trackers = _patch_worker_deps(monkeypatch)

        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(
                addresses=[ADDR_1],
                address_details=[],
            ),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert "chain" not in trackers["create_calls"][0]


class TestDeduplication:
    """Existing addresses are skipped when creating child jobs."""

    def test_existing_address_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()

        # First call returns an existing job, second returns None
        existing_job = SimpleNamespace(id=uuid.uuid4())
        session.execute.return_value.scalar_one_or_none.side_effect = [
            existing_job,  # ADDR_1 already exists
            None,  # ADDR_2 is new
        ]
        job = _job()

        trackers = _patch_worker_deps(monkeypatch)

        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(addresses=[ADDR_1, ADDR_2]),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # Only one child job created (ADDR_2); ADDR_1 was skipped
        assert len(trackers["create_calls"]) == 1
        assert trackers["create_calls"][0]["address"] == ADDR_2


class TestAnalyzeLimitCap:
    """analyze_limit caps the number of addresses processed."""

    def test_limit_respected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job(request={"defillama_protocol": PROTOCOL, "analyze_limit": 1})

        trackers = _patch_worker_deps(monkeypatch)

        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(addresses=[ADDR_1, ADDR_2, ADDR_3]),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # Only 1 child job despite 3 addresses
        assert len(trackers["create_calls"]) == 1

    def test_already_used_reduces_remaining(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job(request={"defillama_protocol": PROTOCOL, "analyze_limit": 3})

        trackers = _patch_worker_deps(monkeypatch)

        # Override count_analysis_children to return 2 already used
        monkeypatch.setattr(
            "workers.defillama_worker.count_analysis_children",
            lambda session, root_job_id: 2,
        )

        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(addresses=[ADDR_1, ADDR_2, ADDR_3]),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # remaining = max(0, 3 - 2) = 1, so only 1 child
        assert len(trackers["create_calls"]) == 1


class TestJobName:
    """Job name is set when missing, not overwritten when present."""

    def test_sets_name_when_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job(name=None)

        _patch_worker_deps(monkeypatch)
        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(addresses=[]),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert job.name == f"DefiLlama: {PROTOCOL}"

    def test_preserves_existing_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        original_name = "My Custom Name"
        job = _job(name=original_name)

        _patch_worker_deps(monkeypatch)
        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(addresses=[]),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert job.name == original_name


class TestNoCloneEnvVar:
    """DEFILLAMA_NO_CLONE env var is correctly passed through to scan_protocol."""

    def test_no_clone_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job()

        _patch_worker_deps(monkeypatch)
        monkeypatch.setenv("DEFILLAMA_NO_CLONE", "true")

        captured_kwargs: list[dict] = []

        def spy_scan(**kwargs):
            captured_kwargs.append(kwargs)
            return _scan_result(addresses=[])

        monkeypatch.setattr("workers.defillama_worker.scan_protocol", spy_scan)

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert captured_kwargs[0]["no_clone"] is True

    def test_no_clone_false_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job()

        _patch_worker_deps(monkeypatch)
        monkeypatch.delenv("DEFILLAMA_NO_CLONE", raising=False)

        captured_kwargs: list[dict] = []

        def spy_scan(**kwargs):
            captured_kwargs.append(kwargs)
            return _scan_result(addresses=[])

        monkeypatch.setattr("workers.defillama_worker.scan_protocol", spy_scan)

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert captured_kwargs[0]["no_clone"] is False


class TestScanResultArtifactContent:
    """Verify artifact data payloads contain correct information."""

    def test_full_scan_artifact_contents(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job()

        trackers = _patch_worker_deps(monkeypatch)

        details = [{"address": ADDR_1, "chain": "ethereum"}]
        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(
                addresses=[ADDR_1],
                address_details=details,
            ),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # Check defillama_full_scan artifact data
        full_scan = next(d for name, d in trackers["store_calls"] if name == "defillama_full_scan")
        assert full_scan["protocol"] == PROTOCOL
        assert full_scan["scan_time"] == 1.23
        assert full_scan["address_details"] == details

        # Check defillama_scan_results artifact data
        scan_results = next(d for name, d in trackers["store_calls"] if name == "defillama_scan_results")
        assert scan_results["addresses_found"] == 1
        assert scan_results["addresses"] == [ADDR_1]

    def test_discovery_summary_artifact(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job = _job()

        trackers = _patch_worker_deps(monkeypatch)

        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(
                addresses=[ADDR_1, ADDR_2],
                address_details=[
                    {"address": ADDR_1, "chain": "ethereum"},
                    {"address": ADDR_2, "chain": "polygon"},
                ],
            ),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        summary = next(d for name, d in trackers["store_calls"] if name == "discovery_summary")
        assert summary["mode"] == "defillama_scan"
        assert summary["protocol"] == PROTOCOL
        assert summary["discovered_count"] == 2
        assert summary["analyzed_count"] == 2
        assert len(summary["child_jobs"]) == 2


class TestZeroAddressesFound:
    """Scan finds zero addresses — no child jobs created, still completes."""

    def test_no_addresses(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        job = _job()

        trackers = _patch_worker_deps(monkeypatch)
        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(addresses=[]),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert len(trackers["create_calls"]) == 0
        assert len(trackers["complete_calls"]) == 1

        summary = next(d for name, d in trackers["store_calls"] if name == "discovery_summary")
        assert summary["discovered_count"] == 0
        assert summary["analyzed_count"] == 0


class TestChildJobFieldPropagation:
    """Child job requests contain correct parent_job_id, root_job_id, rpc_url, and name."""

    def test_child_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job_id = uuid.uuid4()
        job = _job(
            id=job_id,
            request={
                "defillama_protocol": PROTOCOL,
                "rpc_url": "https://rpc.example",
                "root_job_id": "root-123",
            },
        )

        trackers = _patch_worker_deps(monkeypatch)
        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(addresses=[ADDR_1]),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        child_req = trackers["create_calls"][0]
        assert child_req["address"] == ADDR_1
        assert child_req["parent_job_id"] == str(job_id)
        assert child_req["root_job_id"] == "root-123"
        assert child_req["rpc_url"] == "https://rpc.example"
        # name format: {protocol}_{addr[2:10]}
        assert child_req["name"] == f"{PROTOCOL}_{ADDR_1[2:10]}"

    def test_root_job_id_defaults_to_own_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When root_job_id is not in request, it defaults to str(job.id)."""
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        job_id = uuid.uuid4()
        job = _job(id=job_id, request={"defillama_protocol": PROTOCOL})

        trackers = _patch_worker_deps(monkeypatch)
        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(addresses=[ADDR_1]),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert trackers["create_calls"][0]["root_job_id"] == str(job_id)


class TestMixedCaseAddressChainLookup:
    """Chain lookup is case-insensitive — addresses in details may differ in case."""

    def test_mixed_case_matches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        # address_details has upper-case address, addresses list has lower-case
        upper_addr = "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        lower_addr = upper_addr.lower()
        job = _job(request={"defillama_protocol": PROTOCOL})

        trackers = _patch_worker_deps(monkeypatch)
        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: _scan_result(
                addresses=[lower_addr],
                address_details=[{"address": upper_addr, "chain": "optimism"}],
            ),
        )

        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # Chain should be found despite case difference
        assert trackers["create_calls"][0]["chain"] == "optimism"


class TestScanProtocolRaises:
    """If scan_protocol raises, the exception propagates (not caught by worker)."""

    def test_exception_propagates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = DefiLlamaWorker()
        session = MagicMock()
        job = _job()

        _patch_worker_deps(monkeypatch)
        monkeypatch.setattr(
            "workers.defillama_worker.scan_protocol",
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError("clone failed")),
        )

        with pytest.raises(RuntimeError, match="clone failed"):
            worker.process(session, cast(Any, job))
