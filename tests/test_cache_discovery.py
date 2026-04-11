"""Tests for discovery worker cache hit/miss and company mode."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cache_helpers import (
    ADDR_A,
    ADDR_B,
    _create_completed_job_with_static_data,
    db_session,
)


# ---------------------------------------------------------------------------
# Discovery worker cache hit
# ---------------------------------------------------------------------------


def test_discovery_worker_cache_hit_skips_fetch(db_session, monkeypatch):
    """When cache exists, discovery skips fetch() and copies data instead."""
    from db.models import Contract
    from db.queue import create_job, get_source_files
    from sqlalchemy import select
    from workers.discovery import DiscoveryWorker

    # Create completed job as cache source
    _create_completed_job_with_static_data(db_session)

    # Create new job for the same address
    new_job = create_job(db_session, {"address": ADDR_A})

    fetch_called = []
    monkeypatch.setattr(
        "workers.discovery.fetch",
        lambda addr: fetch_called.append(addr) or (_ for _ in ()).throw(AssertionError("fetch should not be called")),
    )

    worker = DiscoveryWorker()
    worker.update_detail = MagicMock()
    worker._process_address(db_session, new_job)

    # fetch() was NOT called
    assert fetch_called == []

    # Data was copied
    contract = db_session.execute(
        select(Contract).where(Contract.job_id == new_job.id)
    ).scalar_one_or_none()
    assert contract is not None
    assert contract.contract_name == "TestContract"

    # Explicit cache flag was set on the job request
    db_session.refresh(new_job)
    assert new_job.request.get("static_cached") is True
    assert new_job.request.get("cache_source_job_id") is not None

    sources = get_source_files(db_session, new_job.id)
    assert len(sources) == 2


# ---------------------------------------------------------------------------
# Discovery worker cache miss
# ---------------------------------------------------------------------------


def test_discovery_worker_cache_miss_runs_fetch(db_session, monkeypatch):
    """New address with no cached job runs fetch() normally."""
    from db.queue import create_job
    from workers.discovery import DiscoveryWorker

    new_job = create_job(db_session, {"address": ADDR_B})

    fetch_called = []

    def mock_fetch(addr):
        fetch_called.append(addr)
        return {
            "ContractName": "NewContract",
            "SourceCode": "contract NewContract {}",
            "CompilerVersion": "v0.8.24",
            "OptimizationUsed": "1",
            "Runs": "200",
            "EVMVersion": "shanghai",
            "LicenseType": "MIT",
        }

    monkeypatch.setattr("workers.discovery.fetch", mock_fetch)
    monkeypatch.setattr("workers.discovery.parse_sources", lambda r: {"src/New.sol": "contract NewContract {}"})
    monkeypatch.setattr("workers.discovery.parse_remappings", lambda r: [])
    monkeypatch.setattr("workers.discovery.is_vyper_result", lambda r: False)
    monkeypatch.setattr("workers.discovery._batch_get_creators", lambda addrs: {})

    worker = DiscoveryWorker()
    worker.update_detail = MagicMock()
    worker._process_address(db_session, new_job)

    assert fetch_called == [ADDR_B]


# ---------------------------------------------------------------------------
# Company-mode jobs are unaffected
# ---------------------------------------------------------------------------


def test_company_mode_unaffected(db_session, monkeypatch):
    """Company-mode jobs (no address) go through _process_company, not cache."""
    from db.queue import create_job
    from workers.discovery import DiscoveryWorker

    job = create_job(db_session, {"company": "TestProtocol"})
    job.company = "TestProtocol"
    db_session.commit()

    company_called = []
    monkeypatch.setattr(
        DiscoveryWorker,
        "_process_company",
        lambda self, session, job: company_called.append(True),
    )

    worker = DiscoveryWorker()
    worker.process(db_session, job)

    assert company_called == [True]
