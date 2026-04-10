"""Tests for static data caching — reuses immutable analysis across jobs.

Uses an in-memory SQLite database so tests run without PostgreSQL.  The
fixture creates a separate metadata/model set with SQLite-compatible column
types (JSON instead of JSONB, CHAR instead of UUID, TEXT instead of ARRAY)
and patches ``store_artifact`` with a dialect-agnostic upsert.
"""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import String, Text, event, types
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Address constants
ADDR_A = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
ADDR_B = "0x0000000000000000000000000000000000000099"


# ---------------------------------------------------------------------------
# SQLite compatibility helpers
# ---------------------------------------------------------------------------


def _sqlite_compatible_store_artifact(session, job_id, name, data=None, text_data=None):
    """SQLite-compatible replacement for the Postgres pg_insert upsert."""
    from db.models import Artifact

    existing = (
        session.query(Artifact)
        .filter(Artifact.job_id == job_id, Artifact.name == name)
        .first()
    )
    if existing:
        existing.data = data
        existing.text_data = text_data
    else:
        session.add(Artifact(job_id=job_id, name=name, data=data, text_data=text_data))
    session.commit()


def _register_sqlite_type_compilers():
    """Register SQLite compilation rules for Postgres-specific types.

    Uses ``@compiles`` to teach the SQLite dialect how to render JSONB, UUID,
    and ARRAY column DDL.  Also registers type-adaptation hooks so that UUID
    and JSON values round-trip correctly through SQLite.

    These registrations are idempotent and persist for the process lifetime,
    which is fine because they are scoped to the ``sqlite`` dialect.
    """
    from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
    from sqlalchemy.ext.compiler import compiles

    # DDL compilation hooks (only affect SQLite CREATE TABLE)
    @compiles(JSONB, "sqlite")
    def _compile_jsonb(element, compiler, **kw):
        return "TEXT"

    @compiles(UUID, "sqlite")
    def _compile_uuid(element, compiler, **kw):
        return "VARCHAR(36)"

    @compiles(ARRAY, "sqlite")
    def _compile_array(element, compiler, **kw):
        return "TEXT"

    # Value adaptation hooks — teach the PG types to handle SQLite bind/result
    _orig_uuid_bind = UUID.bind_processor

    def _uuid_bind_processor(self, dialect):
        if dialect.name == "sqlite":
            def process(value):
                if value is not None:
                    return str(value)
                return value
            return process
        if _orig_uuid_bind:
            return _orig_uuid_bind(self, dialect)
        return None

    UUID.bind_processor = _uuid_bind_processor

    _orig_uuid_result = UUID.result_processor

    def _uuid_result_processor(self, dialect, coltype):
        if dialect.name == "sqlite":
            def process(value):
                if value is not None and not isinstance(value, uuid.UUID):
                    return uuid.UUID(value)
                return value
            return process
        if _orig_uuid_result:
            return _orig_uuid_result(self, dialect, coltype)
        return None

    UUID.result_processor = _uuid_result_processor

    _orig_jsonb_bind = JSONB.bind_processor

    def _jsonb_bind_processor(self, dialect):
        if dialect.name == "sqlite":
            def process(value):
                if value is not None:
                    return json.dumps(value)
                return value
            return process
        if _orig_jsonb_bind:
            return _orig_jsonb_bind(self, dialect)
        return None

    JSONB.bind_processor = _jsonb_bind_processor

    _orig_jsonb_result = JSONB.result_processor

    def _jsonb_result_processor(self, dialect, coltype):
        if dialect.name == "sqlite":
            def process(value):
                if value is not None and isinstance(value, str):
                    return json.loads(value)
                return value
            return process
        if _orig_jsonb_result:
            return _orig_jsonb_result(self, dialect, coltype)
        return None

    JSONB.result_processor = _jsonb_result_processor

    _orig_array_bind = ARRAY.bind_processor

    def _array_bind_processor(self, dialect):
        if dialect.name == "sqlite":
            def process(value):
                if value is not None:
                    return json.dumps(value)
                return value
            return process
        if _orig_array_bind:
            return _orig_array_bind(self, dialect)
        return None

    ARRAY.bind_processor = _array_bind_processor

    _orig_array_result = ARRAY.result_processor

    def _array_result_processor(self, dialect, coltype):
        if dialect.name == "sqlite":
            def process(value):
                if value is not None and isinstance(value, str):
                    return json.loads(value)
                return value
            return process
        if _orig_array_result:
            return _orig_array_result(self, dialect, coltype)
        return None

    ARRAY.result_processor = _array_result_processor


# Register once at import time — these are dialect-scoped and won't affect
# Postgres connections.
_register_sqlite_type_compilers()


@pytest.fixture()
def db_session(monkeypatch):
    """In-memory SQLite database with all PSAT tables.

    Temporarily swaps Postgres-specific column types with SQLite equivalents,
    creates all tables, and monkey-patches ``store_artifact`` so the
    pg_insert-based upsert is replaced with a standard ORM upsert.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from db.models import Base

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    session = Session(engine, expire_on_commit=False)

    # Patch store_artifact everywhere it's imported so the SQLite-compatible
    # upsert is used instead of the pg_insert-based original.
    monkeypatch.setattr("db.queue.store_artifact", _sqlite_compatible_store_artifact)
    # Workers bind store_artifact at import time via ``from db.queue import store_artifact``
    for mod_path in [
        "workers.discovery",
        "workers.static_worker",
    ]:
        try:
            monkeypatch.setattr(f"{mod_path}.store_artifact", _sqlite_compatible_store_artifact)
        except AttributeError:
            pass  # module not yet imported — safe to skip

    try:
        yield session
    finally:
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



def test_find_completed_static_cache_hit(db_session):
    """Completed job with all static data is found."""
    from db.queue import find_completed_static_cache

    completed = _create_completed_job_with_static_data(db_session)
    found = find_completed_static_cache(db_session, ADDR_A)
    assert found is not None
    assert found.id == completed.id



def test_find_completed_static_cache_case_insensitive(db_session):
    """Cache lookup matches regardless of address casing."""
    from db.queue import find_completed_static_cache

    checksummed = ADDR_A  # 0xdAC17F958D2ee523a2206206994597C13D831ec7
    completed = _create_completed_job_with_static_data(db_session, address=checksummed)
    found = find_completed_static_cache(db_session, checksummed.lower())
    assert found is not None
    assert found.id == completed.id



def test_find_completed_static_cache_miss_no_job(db_session):
    """No prior jobs for this address returns None."""
    from db.queue import find_completed_static_cache

    assert find_completed_static_cache(db_session, ADDR_B) is None



def test_find_completed_static_cache_miss_failed_job(db_session):
    """Failed job is not returned as cache."""
    from db.models import JobStatus
    from db.queue import create_job, find_completed_static_cache

    job = create_job(db_session, {"address": ADDR_A})
    job.status = JobStatus.failed
    db_session.commit()

    assert find_completed_static_cache(db_session, ADDR_A) is None



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

    # Proxy contracts raise JobHandledDirectly because the proxy wrapper is
    # completed directly and a child job handles the implementation analysis.
    from workers.base import JobHandledDirectly

    with pytest.raises(JobHandledDirectly):
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

    from workers.base import JobHandledDirectly

    with pytest.raises(JobHandledDirectly):
        worker.process(db_session, target_job)

    assert "resolve_proxy" not in phases_run
    assert resolve_called == []  # No RPC for immutable proxy type

    contract = db_session.execute(
        select(Contract).where(Contract.job_id == target_job.id)
    ).scalar_one()
    assert contract.is_proxy is True
    assert contract.proxy_type == "eip1167"
    assert contract.implementation.lower() == IMPL_ADDR.lower()



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


# ---------------------------------------------------------------------------
# 14. Static dependency caching
# ---------------------------------------------------------------------------


FAKE_STATIC_DEPS = {
    "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "dependencies": [
        "0x0000000000000000000000000000000000000042",
        "0x0000000000000000000000000000000000000043",
    ],
    "rpc": "https://rpc.example",
}



def test_static_deps_stored_on_first_run(db_session, monkeypatch):
    """After a normal (non-cached) dependency phase, the static_dependencies
    artifact is stored so future jobs can reuse it."""
    from db.models import Contract
    from db.queue import create_job, get_artifact, store_source_files
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

    # Mock find_dependencies to return known output
    monkeypatch.setattr(
        "workers.static_worker.find_dependencies",
        lambda *a, **kw: FAKE_STATIC_DEPS,
    )
    # Mock the rest of the dependency phase helpers to no-op
    monkeypatch.setattr(
        "workers.static_worker.find_dynamic_dependencies",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        "workers.static_worker.classify_contracts",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        "workers.static_worker.build_unified_dependencies",
        lambda *a, **kw: {"target_address": ADDR_A, "dependencies": {}},
    )
    monkeypatch.setattr(
        "workers.static_worker.enrich_dependency_metadata",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        "workers.static_worker.write_dependency_visualization",
        lambda *a, **kw: None,
    )

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    # Verify the static_dependencies artifact was stored
    art = get_artifact(db_session, job.id, "static_dependencies")
    assert art is not None
    assert art["address"] == FAKE_STATIC_DEPS["address"]
    assert art["dependencies"] == FAKE_STATIC_DEPS["dependencies"]



def test_static_deps_reused_on_cache_hit(db_session, monkeypatch):
    """On a cached job, find_dependencies() is NOT called and the cached
    static deps are used instead."""
    from db.models import Contract
    from db.queue import create_job, store_artifact, store_source_files
    from workers.static_worker import StaticWorker

    # Create source job with static_dependencies artifact
    source_job = _create_completed_job_with_static_data(db_session)
    store_artifact(db_session, source_job.id, "static_dependencies", data=FAKE_STATIC_DEPS)

    # Create target job flagged as cached
    job = create_job(db_session, {
        "address": ADDR_A,
        "rpc_url": "https://rpc.example",
        "static_cached": True,
        "cache_source_job_id": str(source_job.id),
    })
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
    # Copy static artifacts (including static_dependencies)
    store_artifact(db_session, job.id, "static_dependencies", data=FAKE_STATIC_DEPS)
    store_artifact(db_session, job.id, "contract_analysis", data={"summary": {}})

    # find_dependencies should NOT be called
    find_deps_called = []

    def mock_find_deps(*a, **kw):
        find_deps_called.append(True)
        return FAKE_STATIC_DEPS

    monkeypatch.setattr("workers.static_worker.find_dependencies", mock_find_deps)
    monkeypatch.setattr(
        "workers.static_worker.find_dynamic_dependencies",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        "workers.static_worker.classify_contracts",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        "workers.static_worker.build_unified_dependencies",
        lambda *a, **kw: {"target_address": ADDR_A, "dependencies": {}},
    )
    monkeypatch.setattr(
        "workers.static_worker.enrich_dependency_metadata",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        "workers.static_worker.write_dependency_visualization",
        lambda *a, **kw: None,
    )

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    # find_dependencies was NOT called
    assert find_deps_called == []



def test_dynamic_deps_still_run_on_cache_hit(db_session, monkeypatch):
    """Even with cached static deps, find_dynamic_dependencies() still runs."""
    from db.models import Contract
    from db.queue import create_job, store_artifact, store_source_files
    from workers.static_worker import StaticWorker

    source_job = _create_completed_job_with_static_data(db_session)
    store_artifact(db_session, source_job.id, "static_dependencies", data=FAKE_STATIC_DEPS)

    job = create_job(db_session, {
        "address": ADDR_A,
        "rpc_url": "https://rpc.example",
        "static_cached": True,
        "cache_source_job_id": str(source_job.id),
    })
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
    store_artifact(db_session, job.id, "static_dependencies", data=FAKE_STATIC_DEPS)
    store_artifact(db_session, job.id, "contract_analysis", data={"summary": {}})

    dynamic_called = []

    monkeypatch.setattr("workers.static_worker.find_dependencies", lambda *a, **kw: FAKE_STATIC_DEPS)
    monkeypatch.setattr(
        "workers.static_worker.find_dynamic_dependencies",
        lambda *a, **kw: dynamic_called.append(True) or {"dependencies": [], "dependency_graph": []},
    )
    monkeypatch.setattr(
        "workers.static_worker.classify_contracts",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        "workers.static_worker.build_unified_dependencies",
        lambda *a, **kw: {"target_address": ADDR_A, "dependencies": {}},
    )
    monkeypatch.setattr(
        "workers.static_worker.enrich_dependency_metadata",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        "workers.static_worker.write_dependency_visualization",
        lambda *a, **kw: None,
    )

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    # Dynamic dependency discovery was called
    assert dynamic_called == [True]



def test_static_deps_artifact_copied_by_cache(db_session):
    """static_dependencies is included in _STATIC_ARTIFACT_NAMES and gets
    copied by copy_static_cache."""
    from db.queue import copy_static_cache, create_job, get_artifact, store_artifact

    source_job = _create_completed_job_with_static_data(db_session)
    store_artifact(db_session, source_job.id, "static_dependencies", data=FAKE_STATIC_DEPS)

    target_job = create_job(db_session, {"address": ADDR_A})
    copy_static_cache(db_session, source_job.id, target_job.id)

    art = get_artifact(db_session, target_job.id, "static_dependencies")
    assert art is not None
    assert art["dependencies"] == FAKE_STATIC_DEPS["dependencies"]


# ---------------------------------------------------------------------------
# 15. Dynamic dependency append-only caching
# ---------------------------------------------------------------------------


FAKE_DYN_DEPS_OLD = {
    "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "rpc": "https://rpc.example",
    "transactions_analyzed": [
        {"tx_hash": "0xaaa", "block_number": 100, "method_selector": "0x12345678"},
        {"tx_hash": "0xbbb", "block_number": 200, "method_selector": "0xabcdef01"},
    ],
    "trace_methods": ["debug_traceTransaction"],
    "dependencies": [
        "0x0000000000000000000000000000000000000042",
    ],
    "provenance": {
        "0x0000000000000000000000000000000000000042": [
            {"tx_hash": "0xaaa", "block_number": 100, "from": "0xdac17f958d2ee523a2206206994597c13d831ec7", "op": "CALL"},
        ],
    },
    "dependency_graph": [
        {
            "from": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "to": "0x0000000000000000000000000000000000000042",
            "op": "CALL",
            "provenance": [{"tx_hash": "0xaaa", "block_number": 100}],
        },
    ],
    "trace_errors": [],
}

FAKE_DYN_DEPS_NEW = {
    "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "rpc": "https://rpc.example",
    "transactions_analyzed": [
        {"tx_hash": "0xccc", "block_number": 300, "method_selector": "0x99999999"},
    ],
    "trace_methods": ["debug_traceTransaction"],
    "dependencies": [
        "0x0000000000000000000000000000000000000042",
        "0x0000000000000000000000000000000000000099",
    ],
    "provenance": {
        "0x0000000000000000000000000000000000000042": [
            {"tx_hash": "0xccc", "block_number": 300, "from": "0xdac17f958d2ee523a2206206994597c13d831ec7", "op": "STATICCALL"},
        ],
        "0x0000000000000000000000000000000000000099": [
            {"tx_hash": "0xccc", "block_number": 300, "from": "0xdac17f958d2ee523a2206206994597c13d831ec7", "op": "CALL"},
        ],
    },
    "dependency_graph": [
        {
            "from": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "to": "0x0000000000000000000000000000000000000042",
            "op": "STATICCALL",
            "provenance": [{"tx_hash": "0xccc", "block_number": 300}],
        },
        {
            "from": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "to": "0x0000000000000000000000000000000000000099",
            "op": "CALL",
            "provenance": [{"tx_hash": "0xccc", "block_number": 300}],
        },
    ],
    "trace_errors": [],
}


def test_merge_dynamic_deps():
    """_merge_dynamic_deps produces the union of old and new data."""
    from workers.static_worker import _merge_dynamic_deps

    merged = _merge_dynamic_deps(FAKE_DYN_DEPS_OLD, FAKE_DYN_DEPS_NEW)

    # Dependencies are a sorted union
    assert "0x0000000000000000000000000000000000000042" in merged["dependencies"]
    assert "0x0000000000000000000000000000000000000099" in merged["dependencies"]
    assert len(merged["dependencies"]) == 2

    # Transactions are concatenated (no duplicates)
    tx_hashes = [tx["tx_hash"] for tx in merged["transactions_analyzed"]]
    assert tx_hashes == ["0xaaa", "0xbbb", "0xccc"]

    # Provenance is merged per-address
    prov_42 = merged["provenance"]["0x0000000000000000000000000000000000000042"]
    assert len(prov_42) == 2  # one from old, one from new
    assert any(p["tx_hash"] == "0xaaa" for p in prov_42)
    assert any(p["tx_hash"] == "0xccc" for p in prov_42)

    # New dependency has provenance
    prov_99 = merged["provenance"]["0x0000000000000000000000000000000000000099"]
    assert len(prov_99) == 1

    # Dependency graph: old CALL edge + new STATICCALL edge + new CALL edge = 3 distinct edges
    assert len(merged["dependency_graph"]) == 3

    # Trace methods union
    assert "debug_traceTransaction" in merged["trace_methods"]


def _make_dep_phase_job(session, address=ADDR_A, extra_request=None):
    """Helper: create a job suitable for _run_dependency_phase testing."""
    from db.models import Contract
    from db.queue import create_job, store_source_files

    req = {"address": address, "rpc_url": "https://rpc.example"}
    if extra_request:
        req.update(extra_request)
    job = create_job(session, req)
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
        source_file_count=1,
        remappings=[],
    )
    session.add(contract)
    session.commit()
    store_source_files(session, job.id, {"src/Test.sol": "contract Test {}"})
    return job


def _patch_dep_phase_helpers(monkeypatch, find_dyn_fn):
    """Patch all helpers used by _run_dependency_phase except find_dynamic_dependencies."""
    monkeypatch.setattr("workers.static_worker.find_dependencies", lambda *a, **kw: FAKE_STATIC_DEPS)
    monkeypatch.setattr("workers.static_worker.find_dynamic_dependencies", find_dyn_fn)
    monkeypatch.setattr("workers.static_worker.classify_contracts", lambda *a, **kw: None)
    monkeypatch.setattr(
        "workers.static_worker.build_unified_dependencies",
        lambda *a, **kw: {"target_address": ADDR_A, "dependencies": {}},
    )
    monkeypatch.setattr("workers.static_worker.enrich_dependency_metadata", lambda *a, **kw: None)
    monkeypatch.setattr("workers.static_worker.write_dependency_visualization", lambda *a, **kw: None)


def test_dynamic_deps_artifact_stored_on_first_run(db_session, monkeypatch):
    """After find_dynamic_dependencies succeeds, the dynamic_dependencies artifact is stored."""
    from db.queue import get_artifact
    from workers.static_worker import StaticWorker

    job = _make_dep_phase_job(db_session)

    fake_dyn = dict(FAKE_DYN_DEPS_OLD)
    _patch_dep_phase_helpers(monkeypatch, lambda *a, **kw: fake_dyn)

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    art = get_artifact(db_session, job.id, "dynamic_dependencies")
    assert art is not None
    assert art["dependencies"] == FAKE_DYN_DEPS_OLD["dependencies"]
    assert len(art["transactions_analyzed"]) == 2


def test_dynamic_deps_append_only_merge_on_rerun(db_session, monkeypatch):
    """On re-run with previous dynamic deps, only new txs are traced and results merged."""
    from db.queue import get_artifact, store_artifact
    from workers.static_worker import StaticWorker

    job = _make_dep_phase_job(db_session)

    # Store previous dynamic deps on the job (simulating a previous attempt)
    store_artifact(db_session, job.id, "dynamic_dependencies", data=FAKE_DYN_DEPS_OLD)

    # Track the start_block passed to find_dynamic_dependencies
    captured_kwargs = {}

    def mock_find_dyn(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return FAKE_DYN_DEPS_NEW

    _patch_dep_phase_helpers(monkeypatch, mock_find_dyn)

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    # start_block should be last_block + 1 = 201
    assert captured_kwargs.get("start_block") == 201

    # The stored artifact should be the merged result
    art = get_artifact(db_session, job.id, "dynamic_dependencies")
    assert art is not None
    # Union of old + new deps
    assert "0x0000000000000000000000000000000000000042" in art["dependencies"]
    assert "0x0000000000000000000000000000000000000099" in art["dependencies"]
    # Union of old + new transactions
    tx_hashes = {tx["tx_hash"] for tx in art["transactions_analyzed"]}
    assert tx_hashes == {"0xaaa", "0xbbb", "0xccc"}


def test_dynamic_deps_no_new_transactions_uses_previous(db_session, monkeypatch):
    """When no new transactions exist, previous dynamic deps are used as-is."""
    from db.queue import get_artifact, store_artifact
    from workers.static_worker import StaticWorker

    job = _make_dep_phase_job(db_session)
    store_artifact(db_session, job.id, "dynamic_dependencies", data=FAKE_DYN_DEPS_OLD)

    from services.discovery.dynamic_dependencies import NoNewTransactionsError

    def mock_find_dyn(*args, **kwargs):
        raise NoNewTransactionsError(f"No representative transactions found for {ADDR_A}")

    _patch_dep_phase_helpers(monkeypatch, mock_find_dyn)

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    # Should use previous output as-is (no error)
    art = get_artifact(db_session, job.id, "dynamic_dependencies")
    assert art is not None
    assert art["dependencies"] == FAKE_DYN_DEPS_OLD["dependencies"]
    assert len(art["transactions_analyzed"]) == 2


def test_dynamic_deps_explicit_tx_hashes_skip_merge(db_session, monkeypatch):
    """When explicit tx_hashes are provided, no merge logic runs."""
    from db.queue import get_artifact, store_artifact
    from workers.static_worker import StaticWorker

    job = _make_dep_phase_job(db_session, extra_request={
        "dynamic_tx_hashes": ["0xddd"],
    })
    # Store previous dynamic deps — should be ignored
    store_artifact(db_session, job.id, "dynamic_dependencies", data=FAKE_DYN_DEPS_OLD)

    captured_kwargs = {}

    def mock_find_dyn(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return FAKE_DYN_DEPS_NEW

    _patch_dep_phase_helpers(monkeypatch, mock_find_dyn)

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    # start_block should be None (no incremental fetch)
    assert captured_kwargs.get("start_block") is None
    # tx_hashes should be passed through
    assert captured_kwargs.get("tx_hashes") == ["0xddd"]

    # The stored artifact should be the NEW output only (no merge with old)
    art = get_artifact(db_session, job.id, "dynamic_dependencies")
    assert art is not None
    # Should have new deps only (no merge with old)
    assert art["transactions_analyzed"] == FAKE_DYN_DEPS_NEW["transactions_analyzed"]


def test_dynamic_deps_source_job_fallback(db_session, monkeypatch):
    """When dynamic deps are copied from a source job (via copy_static_cache),
    they serve as the baseline for append-only merge."""
    from db.queue import get_artifact, store_artifact
    from workers.static_worker import StaticWorker

    # Create target job with dynamic deps already copied (as copy_static_cache would do)
    job = _make_dep_phase_job(db_session, extra_request={
        "static_cached": True,
    })
    store_artifact(db_session, job.id, "dynamic_dependencies", data=FAKE_DYN_DEPS_OLD)

    captured_kwargs = {}

    def mock_find_dyn(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return FAKE_DYN_DEPS_NEW

    _patch_dep_phase_helpers(monkeypatch, mock_find_dyn)

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    # start_block should be 201 (from source job's last block + 1)
    assert captured_kwargs.get("start_block") == 201

    # The stored artifact should be the merged result
    art = get_artifact(db_session, job.id, "dynamic_dependencies")
    assert art is not None
    assert "0x0000000000000000000000000000000000000042" in art["dependencies"]
    assert "0x0000000000000000000000000000000000000099" in art["dependencies"]


# ---------------------------------------------------------------------------
# 16. Classification caching
# ---------------------------------------------------------------------------

FAKE_CLS_OUTPUT = {
    "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "rpc": "https://rpc.example",
    "classifications": {
        "0x0000000000000000000000000000000000000042": {"type": "regular"},
        "0x0000000000000000000000000000000000000043": {"type": "proxy", "proxy_type": "eip1967"},
    },
    "discovered_addresses": [],
}


def test_classifications_stored_on_first_run(db_session, monkeypatch):
    """After classify_contracts runs, the classifications artifact is stored."""
    from db.queue import get_artifact
    from workers.static_worker import StaticWorker

    job = _make_dep_phase_job(db_session)

    captured_kwargs = {}

    def mock_classify(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return FAKE_CLS_OUTPUT

    monkeypatch.setattr("workers.static_worker.find_dependencies", lambda *a, **kw: FAKE_STATIC_DEPS)
    monkeypatch.setattr(
        "workers.static_worker.find_dynamic_dependencies",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr("workers.static_worker.classify_contracts", mock_classify)
    monkeypatch.setattr(
        "workers.static_worker.build_unified_dependencies",
        lambda *a, **kw: {"target_address": ADDR_A, "dependencies": {}},
    )
    monkeypatch.setattr("workers.static_worker.enrich_dependency_metadata", lambda *a, **kw: None)
    monkeypatch.setattr("workers.static_worker.write_dependency_visualization", lambda *a, **kw: None)

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    art = get_artifact(db_session, job.id, "classifications")
    assert art is not None
    assert art["classifications"]["0x0000000000000000000000000000000000000042"]["type"] == "regular"
    assert art["classifications"]["0x0000000000000000000000000000000000000043"]["type"] == "proxy"


def test_classifications_reused_via_pre_classified(db_session, monkeypatch):
    """On re-run with cached classifications, previous results are passed as
    pre_classified so only new addresses trigger fresh RPC calls."""
    from db.queue import get_artifact, store_artifact
    from workers.static_worker import StaticWorker

    job = _make_dep_phase_job(db_session)

    # Store previous classifications on the job (simulating seed from copy_static_cache)
    store_artifact(db_session, job.id, "classifications", data=FAKE_CLS_OUTPUT)

    captured_kwargs = {}

    def mock_classify(*args, **kwargs):
        captured_kwargs.update(kwargs)
        # Return extended output with a new address
        extended = dict(FAKE_CLS_OUTPUT)
        extended["classifications"] = dict(FAKE_CLS_OUTPUT["classifications"])
        extended["classifications"]["0x0000000000000000000000000000000000000099"] = {"type": "library"}
        return extended

    monkeypatch.setattr("workers.static_worker.find_dependencies", lambda *a, **kw: FAKE_STATIC_DEPS)
    monkeypatch.setattr(
        "workers.static_worker.find_dynamic_dependencies",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr("workers.static_worker.classify_contracts", mock_classify)
    monkeypatch.setattr(
        "workers.static_worker.build_unified_dependencies",
        lambda *a, **kw: {"target_address": ADDR_A, "dependencies": {}},
    )
    monkeypatch.setattr("workers.static_worker.enrich_dependency_metadata", lambda *a, **kw: None)
    monkeypatch.setattr("workers.static_worker.write_dependency_visualization", lambda *a, **kw: None)

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    # Previous classifications should be in pre_classified
    pre = captured_kwargs.get("pre_classified")
    assert pre is not None
    assert "0x0000000000000000000000000000000000000042" in pre
    assert "0x0000000000000000000000000000000000000043" in pre

    # Updated artifact should include the new address
    art = get_artifact(db_session, job.id, "classifications")
    assert art is not None
    assert "0x0000000000000000000000000000000000000099" in art["classifications"]


def test_classifications_artifact_copied_as_seed(db_session):
    """classifications is included in _SEED_ARTIFACT_NAMES and gets
    copied by copy_static_cache."""
    from db.queue import copy_static_cache, create_job, get_artifact, store_artifact

    source_job = _create_completed_job_with_static_data(db_session)
    store_artifact(db_session, source_job.id, "classifications", data=FAKE_CLS_OUTPUT)

    target_job = create_job(db_session, {"address": ADDR_A})
    copy_static_cache(db_session, source_job.id, target_job.id)

    art = get_artifact(db_session, target_job.id, "classifications")
    assert art is not None
    assert art["classifications"] == FAKE_CLS_OUTPUT["classifications"]


# ---------------------------------------------------------------------------
# 17. Upgrade history caching (append-only)
# ---------------------------------------------------------------------------

FAKE_UH_PREV = {
    "schema_version": "0.1",
    "target_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "proxies": {
        "0xdac17f958d2ee523a2206206994597c13d831ec7": {
            "proxy_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "proxy_type": "eip1967",
            "current_implementation": "0x0000000000000000000000000000000000000042",
            "upgrade_count": 1,
            "first_upgrade_block": 50,
            "last_upgrade_block": 50,
            "implementations": [
                {"address": "0x0000000000000000000000000000000000000042", "block_introduced": 50, "tx_hash": "0xaaa"},
            ],
            "events": [
                {"event_type": "upgraded", "block_number": 50, "tx_hash": "0xaaa", "log_index": 0,
                 "implementation": "0x0000000000000000000000000000000000000042"},
            ],
        },
    },
    "total_upgrades": 1,
}

FAKE_UH_NEW = {
    "schema_version": "0.1",
    "target_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "proxies": {
        "0xdac17f958d2ee523a2206206994597c13d831ec7": {
            "proxy_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "proxy_type": "eip1967",
            "current_implementation": "0x0000000000000000000000000000000000000099",
            "upgrade_count": 1,
            "first_upgrade_block": 100,
            "last_upgrade_block": 100,
            "implementations": [
                {"address": "0x0000000000000000000000000000000000000099", "block_introduced": 100, "tx_hash": "0xbbb"},
            ],
            "events": [
                {"event_type": "upgraded", "block_number": 100, "tx_hash": "0xbbb", "log_index": 0,
                 "implementation": "0x0000000000000000000000000000000000000099"},
            ],
        },
    },
    "total_upgrades": 1,
}


def test_merge_upgrade_history():
    """_merge_upgrade_history produces the union of old and new events."""
    from workers.static_worker import _merge_upgrade_history

    merged = _merge_upgrade_history(FAKE_UH_PREV, FAKE_UH_NEW)

    proxy_addr = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    assert proxy_addr in merged["proxies"]
    proxy = merged["proxies"][proxy_addr]

    # Events are merged and deduplicated
    assert len(proxy["events"]) == 2
    tx_hashes = [e["tx_hash"] for e in proxy["events"]]
    assert "0xaaa" in tx_hashes
    assert "0xbbb" in tx_hashes

    # Timeline rebuilt
    assert len(proxy["implementations"]) == 2
    assert proxy["upgrade_count"] == 2
    assert proxy["first_upgrade_block"] == 50
    assert proxy["last_upgrade_block"] == 100

    assert merged["total_upgrades"] == 2


def test_merge_upgrade_history_disjoint_proxies():
    """_merge_upgrade_history handles proxies that appear in only one side."""
    from workers.static_worker import _merge_upgrade_history

    other_proxy = {
        "schema_version": "0.1",
        "target_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "proxies": {
            "0x0000000000000000000000000000000000000077": {
                "proxy_address": "0x0000000000000000000000000000000000000077",
                "proxy_type": "eip1967",
                "current_implementation": "0x0000000000000000000000000000000000000088",
                "upgrade_count": 1,
                "first_upgrade_block": 200,
                "last_upgrade_block": 200,
                "implementations": [
                    {"address": "0x0000000000000000000000000000000000000088", "block_introduced": 200, "tx_hash": "0xccc"},
                ],
                "events": [
                    {"event_type": "upgraded", "block_number": 200, "tx_hash": "0xccc", "log_index": 0,
                     "implementation": "0x0000000000000000000000000000000000000088"},
                ],
            },
        },
        "total_upgrades": 1,
    }

    merged = _merge_upgrade_history(FAKE_UH_PREV, other_proxy)
    # Both proxies present
    assert "0xdac17f958d2ee523a2206206994597c13d831ec7" in merged["proxies"]
    assert "0x0000000000000000000000000000000000000077" in merged["proxies"]
    assert merged["total_upgrades"] == 2


def test_upgrade_history_append_only_on_rerun(db_session, monkeypatch):
    """Previous upgrade history exists; new fetch starts from max block + 1
    and results are merged."""
    from db.queue import get_artifact, store_artifact
    from workers.static_worker import StaticWorker

    job = _make_dep_phase_job(db_session)

    # Store previous upgrade history on the job
    store_artifact(db_session, job.id, "upgrade_history", data=FAKE_UH_PREV)

    captured_kwargs = {}

    def mock_build_uh(deps_path, *, enrich=True, from_block=0):
        captured_kwargs["from_block"] = from_block
        return FAKE_UH_NEW

    monkeypatch.setattr("workers.static_worker.find_dependencies", lambda *a, **kw: FAKE_STATIC_DEPS)
    monkeypatch.setattr("workers.static_worker.find_dynamic_dependencies", lambda *a, **kw: None)
    monkeypatch.setattr("workers.static_worker.classify_contracts", lambda *a, **kw: None)
    monkeypatch.setattr(
        "workers.static_worker.build_unified_dependencies",
        lambda *a, **kw: {"target_address": ADDR_A, "dependencies": {}},
    )
    monkeypatch.setattr("workers.static_worker.enrich_dependency_metadata", lambda *a, **kw: None)
    monkeypatch.setattr("workers.static_worker.write_dependency_visualization", lambda *a, **kw: None)
    monkeypatch.setattr(
        "services.discovery.upgrade_history.build_upgrade_history",
        mock_build_uh,
    )

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    # from_block should be max_block + 1 = 51
    assert captured_kwargs["from_block"] == 51

    # The stored artifact should be the merged result
    art = get_artifact(db_session, job.id, "upgrade_history")
    assert art is not None
    proxy_addr = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    assert len(art["proxies"][proxy_addr]["events"]) == 2
    assert art["total_upgrades"] == 2


def test_upgrade_history_no_new_events_uses_previous(db_session, monkeypatch):
    """When build_upgrade_history returns empty proxies but previous has data,
    use previous as-is."""
    from db.queue import get_artifact, store_artifact
    from workers.static_worker import StaticWorker

    job = _make_dep_phase_job(db_session)
    store_artifact(db_session, job.id, "upgrade_history", data=FAKE_UH_PREV)

    def mock_build_uh(deps_path, *, enrich=True, from_block=0):
        return {
            "schema_version": "0.1",
            "target_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "proxies": {},
            "total_upgrades": 0,
        }

    monkeypatch.setattr("workers.static_worker.find_dependencies", lambda *a, **kw: FAKE_STATIC_DEPS)
    monkeypatch.setattr("workers.static_worker.find_dynamic_dependencies", lambda *a, **kw: None)
    monkeypatch.setattr("workers.static_worker.classify_contracts", lambda *a, **kw: None)
    monkeypatch.setattr(
        "workers.static_worker.build_unified_dependencies",
        lambda *a, **kw: {"target_address": ADDR_A, "dependencies": {}},
    )
    monkeypatch.setattr("workers.static_worker.enrich_dependency_metadata", lambda *a, **kw: None)
    monkeypatch.setattr("workers.static_worker.write_dependency_visualization", lambda *a, **kw: None)
    monkeypatch.setattr(
        "services.discovery.upgrade_history.build_upgrade_history",
        mock_build_uh,
    )

    worker = StaticWorker()
    monkeypatch.setattr(worker, "_resolve_proxy", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_scaffold_project", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "_run_slither_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_analysis_phase", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "_run_tracking_plan_phase", lambda *a, **kw: None)
    monkeypatch.setattr(worker, "update_detail", lambda *a, **kw: None)

    worker.process(db_session, job)

    # Previous data should be preserved
    art = get_artifact(db_session, job.id, "upgrade_history")
    assert art is not None
    proxy_addr = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    assert proxy_addr in art["proxies"]
    assert art["total_upgrades"] == 1


def test_upgrade_history_artifact_copied_as_seed(db_session):
    """upgrade_history is included in _SEED_ARTIFACT_NAMES and gets
    copied by copy_static_cache."""
    from db.queue import copy_static_cache, create_job, get_artifact, store_artifact

    source_job = _create_completed_job_with_static_data(db_session)
    store_artifact(db_session, source_job.id, "upgrade_history", data=FAKE_UH_PREV)

    target_job = create_job(db_session, {"address": ADDR_A})
    copy_static_cache(db_session, source_job.id, target_job.id)

    art = get_artifact(db_session, target_job.id, "upgrade_history")
    assert art is not None
    assert art["total_upgrades"] == FAKE_UH_PREV["total_upgrades"]
    proxy_addr = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    assert proxy_addr in art["proxies"]
