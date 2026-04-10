"""Tests for static data caching — reuses immutable analysis across jobs.

These tests are integration tests that require PostgreSQL. Run with:
    docker compose up postgres -d
    DATABASE_URL=postgresql://psat:psat@localhost:5432/psat uv run pytest tests/test_static_cache.py -v

Tests are skipped if no PostgreSQL connection is available.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def _can_connect() -> bool:
    if not DATABASE_URL:
        return False
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(not _can_connect(), reason="PostgreSQL not available")

# Address constants
ADDR_A = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
ADDR_B = "0x0000000000000000000000000000000000000099"


@pytest.fixture()
def db_session():
    """Create tables, yield a session, then clean up test data."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from db.models import (
        Artifact,
        Base,
        Contract,
        ContractSummary,
        Job,
        PrivilegedFunction,
        RoleDefinition,
        SlitherFinding,
        SourceFile,
    )

    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    session = Session(engine, expire_on_commit=False)
    try:
        yield session
    finally:
        session.rollback()
        # Clean in dependency order
        for model in [
            SlitherFinding,
            RoleDefinition,
            PrivilegedFunction,
            ContractSummary,
            Contract,
            SourceFile,
            Artifact,
            Job,
        ]:
            session.query(model).delete()
        session.commit()
        session.close()
        engine.dispose()


def _create_completed_job_with_static_data(session, address=ADDR_A):
    """Helper: create a completed job with all static data populated."""
    from db.models import (
        Contract,
        ContractSummary,
        JobStage,
        JobStatus,
        PrivilegedFunction,
        RoleDefinition,
        SlitherFinding,
    )
    from db.queue import create_job, store_artifact, store_source_files

    job = create_job(session, {"address": address, "name": "TestContract"})
    job.status = JobStatus.completed
    job.stage = JobStage.done
    session.commit()

    # Contract row
    contract = Contract(
        job_id=job.id,
        address=address,
        contract_name="TestContract",
        compiler_version="v0.8.24",
        language="solidity",
        evm_version="shanghai",
        optimization=True,
        optimization_runs=200,
        source_format="flat",
        source_file_count=2,
        license="MIT",
        deployer="0x0000000000000000000000000000000000000001",
        remappings=[],
    )
    session.add(contract)
    session.flush()

    # Contract summary
    session.add(
        ContractSummary(
            contract_id=contract.id,
            control_model="ownable",
            is_upgradeable=False,
            is_pausable=True,
            has_timelock=False,
            risk_level="medium",
        )
    )

    # Privileged functions
    session.add(
        PrivilegedFunction(
            contract_id=contract.id,
            function_name="pause",
            selector="0x8456cb59",
            effect_labels=["pause"],
            authority_public=False,
        )
    )

    # Role definitions
    session.add(
        RoleDefinition(
            contract_id=contract.id,
            role_name="ADMIN_ROLE",
            declared_in="TestContract.sol",
        )
    )

    # Slither findings
    session.add(
        SlitherFinding(
            contract_id=contract.id,
            detector="reentrancy-eth",
            severity="High",
            description="Reentrancy in TestContract.withdraw()",
        )
    )

    session.commit()

    # Source files
    store_source_files(
        session,
        job.id,
        {
            "src/TestContract.sol": "pragma solidity ^0.8.24;\ncontract TestContract {}",
            "src/Utils.sol": "pragma solidity ^0.8.24;\nlibrary Utils {}",
        },
    )

    # Artifacts
    store_artifact(session, job.id, "contract_analysis", data={"summary": {"control_model": "ownable"}})
    store_artifact(session, job.id, "slither_results", data={"results": {"detectors": []}})
    store_artifact(session, job.id, "analysis_report", text_data="Test analysis report")
    store_artifact(session, job.id, "control_tracking_plan", data={"controllers": []})
    store_artifact(session, job.id, "contract_flags", data={"is_proxy": False})

    return job


# ---------------------------------------------------------------------------
# 1. Cache lookup tests
# ---------------------------------------------------------------------------


@requires_postgres
def test_find_completed_static_cache_hit(db_session):
    """Completed job with all static data is found."""
    from db.queue import find_completed_static_cache

    completed = _create_completed_job_with_static_data(db_session)
    found = find_completed_static_cache(db_session, ADDR_A)
    assert found is not None
    assert found.id == completed.id


@requires_postgres
def test_find_completed_static_cache_case_insensitive(db_session):
    """Cache lookup matches regardless of address casing."""
    from db.queue import find_completed_static_cache

    checksummed = ADDR_A  # 0xdAC17F958D2ee523a2206206994597C13D831ec7
    completed = _create_completed_job_with_static_data(db_session, address=checksummed)
    found = find_completed_static_cache(db_session, checksummed.lower())
    assert found is not None
    assert found.id == completed.id


@requires_postgres
def test_find_completed_static_cache_miss_no_job(db_session):
    """No prior jobs for this address returns None."""
    from db.queue import find_completed_static_cache

    assert find_completed_static_cache(db_session, ADDR_B) is None


@requires_postgres
def test_find_completed_static_cache_miss_failed_job(db_session):
    """Failed job is not returned as cache."""
    from db.models import JobStatus
    from db.queue import create_job, find_completed_static_cache

    job = create_job(db_session, {"address": ADDR_A})
    job.status = JobStatus.failed
    db_session.commit()

    assert find_completed_static_cache(db_session, ADDR_A) is None


@requires_postgres
def test_find_completed_static_cache_miss_no_analysis(db_session):
    """Completed job without contract_analysis artifact is not returned."""
    from db.models import Contract, ContractSummary, JobStage, JobStatus
    from db.queue import create_job, find_completed_static_cache, store_source_files

    job = create_job(db_session, {"address": ADDR_A})
    job.status = JobStatus.completed
    job.stage = JobStage.done
    db_session.commit()

    contract = Contract(job_id=job.id, address=ADDR_A, contract_name="X")
    db_session.add(contract)
    db_session.flush()
    db_session.add(ContractSummary(contract_id=contract.id))
    db_session.commit()
    store_source_files(db_session, job.id, {"src/X.sol": "contract X {}"})

    # No contract_analysis artifact → cache miss
    assert find_completed_static_cache(db_session, ADDR_A) is None


@requires_postgres
def test_find_completed_static_cache_miss_no_summary(db_session):
    """Completed job without contract_summaries row is not returned."""
    from db.models import Contract, JobStage, JobStatus
    from db.queue import create_job, find_completed_static_cache, store_artifact, store_source_files

    job = create_job(db_session, {"address": ADDR_A})
    job.status = JobStatus.completed
    job.stage = JobStage.done
    db_session.commit()

    db_session.add(Contract(job_id=job.id, address=ADDR_A, contract_name="X"))
    db_session.commit()
    store_source_files(db_session, job.id, {"src/X.sol": "contract X {}"})
    store_artifact(db_session, job.id, "contract_analysis", data={"summary": {}})

    # No contract_summary → cache miss
    assert find_completed_static_cache(db_session, ADDR_A) is None


# ---------------------------------------------------------------------------
# 2. Cache copy tests
# ---------------------------------------------------------------------------


@requires_postgres
def test_copy_static_cache(db_session):
    """copy_static_cache duplicates all static data into a new job."""
    from db.models import Contract, ContractSummary, PrivilegedFunction, RoleDefinition, SlitherFinding
    from db.queue import (
        copy_static_cache,
        create_job,
        get_artifact,
        get_source_files,
    )
    from sqlalchemy import select

    source_job = _create_completed_job_with_static_data(db_session)
    target_job = create_job(db_session, {"address": ADDR_A})

    new_contract_id = copy_static_cache(db_session, source_job.id, target_job.id)
    assert new_contract_id is not None

    # Contract row copied (immutable fields only — proxy fields left as defaults)
    target_contract = db_session.execute(
        select(Contract).where(Contract.job_id == target_job.id)
    ).scalar_one_or_none()
    assert target_contract is not None
    assert target_contract.contract_name == "TestContract"
    assert target_contract.compiler_version == "v0.8.24"
    assert target_contract.is_proxy is False  # default, not copied from source
    assert target_contract.proxy_type is None
    assert target_contract.implementation is None

    # Source files copied
    sources = get_source_files(db_session, target_job.id)
    assert len(sources) == 2
    assert "src/TestContract.sol" in sources

    # Contract summary copied
    summary = db_session.execute(
        select(ContractSummary).where(ContractSummary.contract_id == new_contract_id)
    ).scalar_one_or_none()
    assert summary is not None
    assert summary.control_model == "ownable"

    # Privileged functions copied
    pfs = db_session.execute(
        select(PrivilegedFunction).where(PrivilegedFunction.contract_id == new_contract_id)
    ).scalars().all()
    assert len(pfs) == 1
    assert pfs[0].function_name == "pause"

    # Role definitions copied
    rds = db_session.execute(
        select(RoleDefinition).where(RoleDefinition.contract_id == new_contract_id)
    ).scalars().all()
    assert len(rds) == 1
    assert rds[0].role_name == "ADMIN_ROLE"

    # Slither findings copied
    sfs = db_session.execute(
        select(SlitherFinding).where(SlitherFinding.contract_id == new_contract_id)
    ).scalars().all()
    assert len(sfs) == 1
    assert sfs[0].detector == "reentrancy-eth"

    # Artifacts copied (contract_flags excluded — mutable, resolved fresh by _resolve_proxy)
    assert get_artifact(db_session, target_job.id, "contract_analysis") is not None
    assert get_artifact(db_session, target_job.id, "slither_results") is not None
    assert get_artifact(db_session, target_job.id, "analysis_report") == "Test analysis report"
    assert get_artifact(db_session, target_job.id, "control_tracking_plan") is not None
    assert get_artifact(db_session, target_job.id, "contract_flags") is None  # not cached


# ---------------------------------------------------------------------------
# 3. Discovery worker cache hit
# ---------------------------------------------------------------------------


@requires_postgres
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
# 4. Discovery worker cache miss
# ---------------------------------------------------------------------------


@requires_postgres
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
# 5. Static worker cache hit
# ---------------------------------------------------------------------------


@requires_postgres
def test_static_worker_cache_hit_skips_analysis(db_session, monkeypatch):
    """Job flagged as static_cached skips Slither/analysis but runs deps."""
    from db.models import Contract
    from db.queue import create_job, store_artifact, store_source_files
    from workers.static_worker import StaticWorker

    # Create a new job with the explicit cache flag set by discovery worker
    job = create_job(db_session, {"address": ADDR_A, "rpc_url": "https://rpc.example", "static_cached": True})
    contract = Contract(
        job_id=job.id,
        address=ADDR_A,
        contract_name="TestContract",
        compiler_version="v0.8.24",
        language="solidity",
        evm_version="shanghai",
        optimization=True,
        optimization_runs=200,
        source_format="flat",
        source_file_count=1,
        remappings=[],
    )
    db_session.add(contract)
    db_session.commit()

    store_source_files(db_session, job.id, {"src/TestContract.sol": "contract TestContract {}"})
    store_artifact(db_session, job.id, "contract_analysis", data={"summary": {}})

    # Track which phases run
    phases_run = []

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: phases_run.append("resolve_proxy"))
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(
        worker,
        "_run_dependency_phase",
        lambda *a, **kw: phases_run.append("dependency"),
    )
    monkeypatch.setattr(
        worker,
        "_run_slither_phase",
        lambda *a, **kw: phases_run.append("slither") or True,
    )
    monkeypatch.setattr(
        worker,
        "_run_analysis_phase",
        lambda *a, **kw: phases_run.append("analysis") or True,
    )
    monkeypatch.setattr(
        worker,
        "_run_tracking_plan_phase",
        lambda *a, **kw: phases_run.append("tracking_plan"),
    )
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    # Dependency phase and proxy resolution should run; Slither/analysis/tracking should NOT
    assert "resolve_proxy" in phases_run
    assert "dependency" in phases_run
    assert "slither" not in phases_run
    assert "analysis" not in phases_run
    assert "tracking_plan" not in phases_run


# ---------------------------------------------------------------------------
# 6. Static worker cache miss
# ---------------------------------------------------------------------------


@requires_postgres
def test_static_worker_cache_miss_runs_analysis(db_session, monkeypatch):
    """Job without cached artifacts runs all analysis phases normally."""
    from db.models import Contract
    from db.queue import create_job, store_source_files
    from workers.static_worker import StaticWorker

    job = create_job(db_session, {"address": ADDR_A, "rpc_url": "https://rpc.example"})
    contract = Contract(
        job_id=job.id,
        address=ADDR_A,
        contract_name="TestContract",
        compiler_version="v0.8.24",
        language="solidity",
        evm_version="shanghai",
        optimization=True,
        optimization_runs=200,
        source_format="flat",
        source_file_count=1,
        remappings=[],
    )
    db_session.add(contract)
    db_session.commit()

    store_source_files(db_session, job.id, {"src/TestContract.sol": "contract TestContract {}"})

    phases_run = []

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: phases_run.append("resolve_proxy"))
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(
        worker,
        "_run_dependency_phase",
        lambda *a, **kw: phases_run.append("dependency"),
    )
    monkeypatch.setattr(
        worker,
        "_run_slither_phase",
        lambda *a, **kw: phases_run.append("slither") or True,
    )
    monkeypatch.setattr(
        worker,
        "_run_analysis_phase",
        lambda *a, **kw: phases_run.append("analysis") or True,
    )
    monkeypatch.setattr(
        worker,
        "_run_tracking_plan_phase",
        lambda *a, **kw: phases_run.append("tracking_plan"),
    )
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    # All phases should run
    assert "resolve_proxy" in phases_run
    assert "dependency" in phases_run
    assert "slither" in phases_run
    assert "analysis" in phases_run
    assert "tracking_plan" in phases_run


# ---------------------------------------------------------------------------
# 7. Data isolation — deleting source job doesn't break target
# ---------------------------------------------------------------------------


@requires_postgres
def test_data_isolation_after_cache_copy(db_session):
    """Deleting the source job does not affect the copied data."""
    from db.models import Contract, ContractSummary, Job, PrivilegedFunction
    from db.queue import (
        copy_static_cache,
        create_job,
        get_artifact,
        get_source_files,
    )
    from sqlalchemy import select

    source_job = _create_completed_job_with_static_data(db_session)
    target_job = create_job(db_session, {"address": ADDR_A})

    new_contract_id = copy_static_cache(db_session, source_job.id, target_job.id)
    assert new_contract_id is not None

    # Delete the source job (CASCADE should remove its contract, artifacts, etc.)
    db_session.delete(db_session.get(Job, source_job.id))
    db_session.commit()

    # Target data should be fully intact
    target_contract = db_session.execute(
        select(Contract).where(Contract.job_id == target_job.id)
    ).scalar_one_or_none()
    assert target_contract is not None
    assert target_contract.contract_name == "TestContract"

    sources = get_source_files(db_session, target_job.id)
    assert len(sources) == 2

    summary = db_session.execute(
        select(ContractSummary).where(ContractSummary.contract_id == new_contract_id)
    ).scalar_one_or_none()
    assert summary is not None

    pfs = db_session.execute(
        select(PrivilegedFunction).where(PrivilegedFunction.contract_id == new_contract_id)
    ).scalars().all()
    assert len(pfs) == 1

    assert get_artifact(db_session, target_job.id, "contract_analysis") is not None
    assert get_artifact(db_session, target_job.id, "analysis_report") == "Test analysis report"


# ---------------------------------------------------------------------------
# 8. Company-mode jobs are unaffected
# ---------------------------------------------------------------------------


@requires_postgres
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


# ---------------------------------------------------------------------------
# 9. No duplicate rows after two runs of the same address
# ---------------------------------------------------------------------------


@requires_postgres
def test_no_duplicate_rows_after_two_runs(db_session, monkeypatch):
    """Running discovery twice for the same address produces exactly one set of
    static rows per job — no duplicates within either job."""
    from db.models import Contract, ContractSummary, PrivilegedFunction, RoleDefinition, SlitherFinding, SourceFile
    from db.queue import create_job, get_artifact
    from sqlalchemy import func, select
    from workers.discovery import DiscoveryWorker

    # Run 1: full discovery (no cache)
    source_job = _create_completed_job_with_static_data(db_session)

    # Run 2: cache hit
    new_job = create_job(db_session, {"address": ADDR_A})
    monkeypatch.setattr(
        "workers.discovery.fetch",
        lambda addr: (_ for _ in ()).throw(AssertionError("fetch should not be called")),
    )

    worker = DiscoveryWorker()
    worker.update_detail = MagicMock()
    worker._process_address(db_session, new_job)

    # Verify exactly 1 contract row per job
    for job in [source_job, new_job]:
        count = db_session.execute(
            select(func.count()).select_from(Contract).where(Contract.job_id == job.id)
        ).scalar()
        assert count == 1, f"Expected 1 contract row for job {job.id}, got {count}"

    # Verify the new job's contract has exactly 1 summary, 1 priv func, 1 role, 1 slither finding
    new_contract = db_session.execute(
        select(Contract).where(Contract.job_id == new_job.id)
    ).scalar_one()

    summary_count = db_session.execute(
        select(func.count()).select_from(ContractSummary).where(
            ContractSummary.contract_id == new_contract.id
        )
    ).scalar()
    assert summary_count == 1, f"Expected 1 summary, got {summary_count}"

    pf_count = db_session.execute(
        select(func.count()).select_from(PrivilegedFunction).where(
            PrivilegedFunction.contract_id == new_contract.id
        )
    ).scalar()
    assert pf_count == 1, f"Expected 1 privileged function, got {pf_count}"

    rd_count = db_session.execute(
        select(func.count()).select_from(RoleDefinition).where(
            RoleDefinition.contract_id == new_contract.id
        )
    ).scalar()
    assert rd_count == 1, f"Expected 1 role definition, got {rd_count}"

    sf_count = db_session.execute(
        select(func.count()).select_from(SlitherFinding).where(
            SlitherFinding.contract_id == new_contract.id
        )
    ).scalar()
    assert sf_count == 1, f"Expected 1 slither finding, got {sf_count}"

    # Verify exactly 2 source files (matches the helper)
    src_count = db_session.execute(
        select(func.count()).select_from(SourceFile).where(SourceFile.job_id == new_job.id)
    ).scalar()
    assert src_count == 2, f"Expected 2 source files, got {src_count}"

    # Verify artifacts are not duplicated (exactly 1 per name)
    for artifact_name in ["contract_analysis", "slither_results", "control_tracking_plan"]:
        art = get_artifact(db_session, new_job.id, artifact_name)
        assert art is not None, f"Missing artifact {artifact_name}"


@requires_postgres
def test_copy_returns_early_if_target_already_populated(db_session):
    """Second call to copy_static_cache returns existing ID without duplicating."""
    from db.models import Contract, ContractSummary, PrivilegedFunction
    from db.queue import copy_static_cache, create_job
    from sqlalchemy import func, select

    source_job = _create_completed_job_with_static_data(db_session)
    target_job = create_job(db_session, {"address": ADDR_A})

    # Copy once
    id1 = copy_static_cache(db_session, source_job.id, target_job.id)
    assert id1 is not None

    # Second call returns the same existing contract ID
    id2 = copy_static_cache(db_session, source_job.id, target_job.id)
    assert id2 == id1

    # Still exactly 1 contract row
    count = db_session.execute(
        select(func.count()).select_from(Contract).where(Contract.job_id == target_job.id)
    ).scalar()
    assert count == 1, f"Expected 1 contract row after double copy, got {count}"

    # Still exactly 1 summary
    summary_count = db_session.execute(
        select(func.count()).select_from(ContractSummary).where(
            ContractSummary.contract_id == id1
        )
    ).scalar()
    assert summary_count == 1, f"Expected 1 summary after double copy, got {summary_count}"

    pf_count = db_session.execute(
        select(func.count()).select_from(PrivilegedFunction).where(
            PrivilegedFunction.contract_id == id1
        )
    ).scalar()
    assert pf_count == 1, f"Expected 1 priv func after double copy, got {pf_count}"


# ---------------------------------------------------------------------------
# 10. Proxy cache optimization — _check_proxy_cache tests
# ---------------------------------------------------------------------------

IMPL_ADDR = "0x5615deb798bb3e4dfa0139dfa1b3d433cc23b72f"
IMPL_ADDR_NEW = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


def _create_source_job_with_proxy(session, address=ADDR_A, is_proxy=True, proxy_type="eip1967",
                                   implementation=IMPL_ADDR, beacon=None, admin=None):
    """Helper: create a completed source job with proxy fields set."""
    from db.models import Contract, ContractSummary, JobStage, JobStatus
    from db.queue import create_job, store_artifact, store_source_files

    job = create_job(session, {"address": address, "name": "ProxyContract"})
    job.status = JobStatus.completed
    job.stage = JobStage.done
    session.commit()

    contract = Contract(
        job_id=job.id,
        address=address,
        contract_name="ProxyContract",
        compiler_version="v0.8.24",
        language="solidity",
        evm_version="shanghai",
        optimization=True,
        optimization_runs=200,
        source_format="flat",
        source_file_count=1,
        remappings=[],
        is_proxy=is_proxy,
        proxy_type=proxy_type if is_proxy else None,
        implementation=implementation if is_proxy else None,
        beacon=beacon,
        admin=admin,
    )
    session.add(contract)
    session.flush()

    session.add(ContractSummary(contract_id=contract.id, control_model="proxy"))
    session.commit()

    store_source_files(session, job.id, {"src/Proxy.sol": "contract Proxy {}"})
    store_artifact(session, job.id, "contract_analysis", data={"summary": {}})
    store_artifact(session, job.id, "slither_results", data={"results": {"detectors": []}})
    store_artifact(session, job.id, "analysis_report", text_data="proxy report")
    store_artifact(session, job.id, "control_tracking_plan", data={"controllers": []})

    return job


def _create_target_job_with_contract(session, source_job_id, address=ADDR_A, rpc_url="https://rpc.example"):
    """Helper: create a new job with static_cached flag and a contract row."""
    from db.models import Contract
    from db.queue import copy_static_cache, create_job, store_source_files

    job = create_job(session, {
        "address": address,
        "rpc_url": rpc_url,
        "static_cached": True,
        "cache_source_job_id": str(source_job_id),
    })

    copy_static_cache(session, source_job_id, job.id)

    store_source_files(session, job.id, {"src/Proxy.sol": "contract Proxy {}"})

    return job


@requires_postgres
def test_proxy_cache_non_proxy_source(db_session, monkeypatch):
    """Cache hit with non-proxy source: _resolve_proxy is NOT called, contract has is_proxy=False."""
    from db.models import Contract
    from db.queue import create_job, store_source_files
    from sqlalchemy import select
    from workers.static_worker import StaticWorker

    source_job = _create_source_job_with_proxy(
        db_session, is_proxy=False, proxy_type=None, implementation=None,
    )
    target_job = _create_target_job_with_contract(db_session, source_job.id)

    phases_run = []
    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: phases_run.append("resolve_proxy"))
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_dependency_phase", lambda *a, **kw: phases_run.append("dependency"))
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: phases_run.append("slither") or True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: phases_run.append("analysis") or True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: phases_run.append("tracking_plan"))
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, target_job)

    assert "resolve_proxy" not in phases_run
    assert "dependency" in phases_run

    contract = db_session.execute(
        select(Contract).where(Contract.job_id == target_job.id)
    ).scalar_one()
    assert contract.is_proxy is False
    assert contract.implementation is None


@requires_postgres
def test_proxy_cache_proxy_unchanged(db_session, monkeypatch):
    """Cache hit with unchanged proxy: _resolve_proxy is NOT called, proxy fields are copied."""
    from db.models import Contract
    from sqlalchemy import select
    from workers.static_worker import StaticWorker

    source_job = _create_source_job_with_proxy(
        db_session, is_proxy=True, proxy_type="eip1967", implementation=IMPL_ADDR,
        beacon="0xbeac000000000000000000000000000000000000",
        admin="0xad1c000000000000000000000000000000000000",
    )
    target_job = _create_target_job_with_contract(db_session, source_job.id)

    # resolve_current_implementation returns the SAME address → no upgrade
    monkeypatch.setattr(
        "workers.static_worker.resolve_current_implementation",
        lambda addr, rpc, **kw: IMPL_ADDR,
    )

    phases_run = []
    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: phases_run.append("resolve_proxy"))
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_dependency_phase", lambda *a, **kw: phases_run.append("dependency"))
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: phases_run.append("slither") or True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: phases_run.append("analysis") or True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: phases_run.append("tracking_plan"))
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, target_job)

    assert "resolve_proxy" not in phases_run

    contract = db_session.execute(
        select(Contract).where(Contract.job_id == target_job.id)
    ).scalar_one()
    assert contract.is_proxy is True
    assert contract.proxy_type == "eip1967"
    assert contract.implementation.lower() == IMPL_ADDR.lower()
    assert contract.beacon is not None
    assert contract.admin is not None


@requires_postgres
def test_proxy_cache_proxy_upgraded(db_session, monkeypatch):
    """Cache hit but proxy upgraded: _resolve_proxy IS called."""
    from workers.static_worker import StaticWorker

    source_job = _create_source_job_with_proxy(
        db_session, is_proxy=True, proxy_type="eip1967", implementation=IMPL_ADDR,
    )
    target_job = _create_target_job_with_contract(db_session, source_job.id)

    # resolve_current_implementation returns a DIFFERENT address → upgrade detected
    monkeypatch.setattr(
        "workers.static_worker.resolve_current_implementation",
        lambda addr, rpc, **kw: IMPL_ADDR_NEW,
    )

    phases_run = []
    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: phases_run.append("resolve_proxy"))
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_dependency_phase", lambda *a, **kw: phases_run.append("dependency"))
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: phases_run.append("slither") or True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: phases_run.append("analysis") or True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: phases_run.append("tracking_plan"))
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, target_job)

    assert "resolve_proxy" in phases_run


@requires_postgres
def test_proxy_cache_rpc_fails(db_session, monkeypatch):
    """Cache hit but RPC fails: falls back to full _resolve_proxy."""
    from workers.static_worker import StaticWorker

    source_job = _create_source_job_with_proxy(
        db_session, is_proxy=True, proxy_type="eip1967", implementation=IMPL_ADDR,
    )
    target_job = _create_target_job_with_contract(db_session, source_job.id)

    def mock_resolve(addr, rpc, **kw):
        raise ConnectionError("RPC node down")

    monkeypatch.setattr("workers.static_worker.resolve_current_implementation", mock_resolve)

    phases_run = []
    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: phases_run.append("resolve_proxy"))
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_dependency_phase", lambda *a, **kw: phases_run.append("dependency"))
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: phases_run.append("slither") or True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: phases_run.append("analysis") or True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: phases_run.append("tracking_plan"))
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, target_job)

    assert "resolve_proxy" in phases_run


@requires_postgres
def test_proxy_cache_no_cache_flag(db_session, monkeypatch):
    """Job without static_cached flag: _resolve_proxy IS called normally."""
    from db.models import Contract
    from db.queue import create_job, store_source_files
    from workers.static_worker import StaticWorker

    job = create_job(db_session, {"address": ADDR_A, "rpc_url": "https://rpc.example"})
    contract = Contract(
        job_id=job.id,
        address=ADDR_A,
        contract_name="TestContract",
        compiler_version="v0.8.24",
        language="solidity",
        evm_version="shanghai",
        optimization=True,
        optimization_runs=200,
        source_format="flat",
        source_file_count=1,
        remappings=[],
    )
    db_session.add(contract)
    db_session.commit()
    store_source_files(db_session, job.id, {"src/Test.sol": "contract Test {}"})

    phases_run = []
    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: phases_run.append("resolve_proxy"))
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_dependency_phase", lambda *a, **kw: phases_run.append("dependency"))
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: phases_run.append("slither") or True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: phases_run.append("analysis") or True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: phases_run.append("tracking_plan"))
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    assert "resolve_proxy" in phases_run


@requires_postgres
def test_proxy_cache_immutable_eip1167(db_session, monkeypatch):
    """Cache hit with eip1167 (immutable) proxy: reuse without any RPC call."""
    from db.models import Contract
    from sqlalchemy import select
    from workers.static_worker import StaticWorker

    source_job = _create_source_job_with_proxy(
        db_session, is_proxy=True, proxy_type="eip1167", implementation=IMPL_ADDR,
    )
    target_job = _create_target_job_with_contract(db_session, source_job.id)

    # resolve_current_implementation should NOT be called for immutable types
    resolve_called = []

    def mock_resolve(addr, rpc, **kw):
        resolve_called.append(addr)
        return IMPL_ADDR

    monkeypatch.setattr("workers.static_worker.resolve_current_implementation", mock_resolve)

    phases_run = []
    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: phases_run.append("resolve_proxy"))
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_dependency_phase", lambda *a, **kw: phases_run.append("dependency"))
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: phases_run.append("slither") or True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: phases_run.append("analysis") or True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: phases_run.append("tracking_plan"))
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, target_job)

    assert "resolve_proxy" not in phases_run
    assert resolve_called == []  # No RPC for immutable proxy type

    contract = db_session.execute(
        select(Contract).where(Contract.job_id == target_job.id)
    ).scalar_one()
    assert contract.is_proxy is True
    assert contract.proxy_type == "eip1167"
    assert contract.implementation.lower() == IMPL_ADDR.lower()


@requires_postgres
def test_proxy_cache_diamond_proxy_falls_back(db_session, monkeypatch):
    """Cache hit with diamond proxy (eip2535): falls back to full _resolve_proxy."""
    from workers.static_worker import StaticWorker

    source_job = _create_source_job_with_proxy(
        db_session, is_proxy=True, proxy_type="eip2535", implementation=IMPL_ADDR,
    )
    target_job = _create_target_job_with_contract(db_session, source_job.id)

    phases_run = []
    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: phases_run.append("resolve_proxy"))
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_dependency_phase", lambda *a, **kw: phases_run.append("dependency"))
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: phases_run.append("slither") or True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: phases_run.append("analysis") or True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: phases_run.append("tracking_plan"))
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, target_job)

    assert "resolve_proxy" in phases_run
