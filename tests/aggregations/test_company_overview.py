"""Integration tests for ``services.aggregations.company_overview``.

Hits a real Postgres via ``db_session`` so the resolver code paths
(legacy company fallback, address/chain Contract fallback, impl
resolution) actually exercise the SQLAlchemy queries.
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from db.models import Contract, Job, JobStage, JobStatus, Protocol  # noqa: E402
from services.aggregations.company_overview import (  # noqa: E402
    CompanyNotFound,
    build_company_overview,
    prefetch_contracts,
    resolve_company_jobs,
    resolve_implementation_contracts,
)
from tests.conftest import requires_postgres  # noqa: E402

pytestmark = requires_postgres


def _addr(seed: str) -> str:
    """Deterministic-but-test-unique 0x address keyed by ``seed``.

    The conftest db_session fixture cleans up Protocol but not Contract or
    Job rows (they have ON DELETE SET NULL). Hardcoding addresses across
    tests collides on uq_contract_address_chain — UUID-derive instead.
    """
    return "0x" + (uuid.uuid4().hex + seed.encode().hex())[:40]


def _add_protocol(session, name: str) -> Protocol:
    p = Protocol(name=name)
    session.add(p)
    session.commit()
    session.refresh(p)
    return p


def _add_job(
    session,
    *,
    address: str,
    name: str | None = None,
    company: str | None = None,
    protocol_id: int | None = None,
    status: JobStatus = JobStatus.completed,
    request: dict | None = None,
    is_proxy: bool = False,
) -> Job:
    job = Job(
        id=uuid.uuid4(),
        address=address,
        company=company,
        protocol_id=protocol_id,
        name=name or address,
        status=status,
        stage=JobStage.done,
        request=request or {"address": address},
        is_proxy=is_proxy,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def _add_contract(
    session,
    *,
    address: str,
    job: Job,
    protocol_id: int | None = None,
    chain: str = "ethereum",
    is_proxy: bool = False,
    implementation: str | None = None,
    contract_name: str | None = None,
) -> Contract:
    c = Contract(
        address=address,
        job_id=job.id,
        protocol_id=protocol_id,
        chain=chain,
        contract_name=contract_name or address,
        is_proxy=is_proxy,
        implementation=implementation,
    )
    session.add(c)
    session.commit()
    session.refresh(c)
    return c


def test_resolve_company_jobs_protocol_path(db_session):
    """Modern data: Protocol row exists and jobs carry ``protocol_id``."""
    p = _add_protocol(db_session, f"alpha-{uuid.uuid4().hex[:8]}")
    j1 = _add_job(db_session, address=_addr("a1"), protocol_id=p.id)
    _add_job(db_session, address=_addr("a2"), protocol_id=p.id, status=JobStatus.queued)

    protocol, jobs = resolve_company_jobs(db_session, p.name)
    assert protocol is not None and protocol.id == p.id
    # Only completed + has-address jobs are returned
    assert {j.id for j in jobs} == {j1.id}


def test_resolve_company_jobs_legacy_fallback_with_parent_chain(db_session):
    """Legacy data: no Protocol row. Parent → child via ``parent_job_id``."""
    company = f"legacy-{uuid.uuid4().hex[:8]}"
    parent_addr = _addr("p")
    child_addr = _addr("c")
    parent = _add_job(db_session, address=parent_addr, company=company, name="parent")
    child = _add_job(
        db_session,
        address=child_addr,
        name="child",
        request={"address": child_addr, "parent_job_id": str(parent.id)},
    )
    # Unrelated job — should NOT be included.
    _add_job(db_session, address=_addr("o"), name="other")

    protocol, jobs = resolve_company_jobs(db_session, company)
    assert protocol is None
    assert {j.id for j in jobs} == {parent.id, child.id}


def test_resolve_company_jobs_unknown_returns_empty(db_session):
    protocol, jobs = resolve_company_jobs(db_session, f"missing-{uuid.uuid4().hex[:8]}")
    assert protocol is None
    assert jobs == []


def test_prefetch_contracts_address_chain_fallback(db_session):
    """When a Contract row has been re-keyed to a newer job, the address+chain
    fallback locates it for the original requesting job."""
    p = _add_protocol(db_session, f"fallback-{uuid.uuid4().hex[:8]}")
    addr = _addr("d")
    original = _add_job(db_session, address=addr, protocol_id=p.id)
    newer = _add_job(db_session, address=addr, protocol_id=p.id)
    # Contract row is keyed to the newer job — original would normally see no row.
    contract = _add_contract(db_session, address=addr, job=newer, protocol_id=p.id, contract_name="Vault")

    out = prefetch_contracts(db_session, [original, newer])
    assert out[original.id].id == contract.id
    assert out[newer.id].id == contract.id


def test_resolve_implementation_contracts_links_proxy_to_impl(db_session):
    """Proxy job → impl Contract row keyed by impl address."""
    p = _add_protocol(db_session, f"proxy-resolver-{uuid.uuid4().hex[:8]}")
    proxy_addr = _addr("px")
    impl_addr = _addr("im")

    proxy_job = _add_job(db_session, address=proxy_addr, protocol_id=p.id, is_proxy=True)
    impl_job = _add_job(db_session, address=impl_addr, protocol_id=p.id)
    _add_contract(
        db_session,
        address=proxy_addr,
        job=proxy_job,
        protocol_id=p.id,
        is_proxy=True,
        implementation=impl_addr,
        contract_name="MyProxy",
    )
    impl_contract = _add_contract(
        db_session, address=impl_addr, job=impl_job, protocol_id=p.id, contract_name="VaultImpl"
    )

    contracts_by_job = prefetch_contracts(db_session, [proxy_job, impl_job])
    impl_job_by_addr, contracts_by_job = resolve_implementation_contracts(
        db_session, [proxy_job, impl_job], contracts_by_job
    )

    assert impl_addr.lower() in impl_job_by_addr
    assert impl_job_by_addr[impl_addr.lower()].id == impl_job.id
    # contracts_by_job_id picked up the impl contract under its own job id.
    assert contracts_by_job[impl_job.id].id == impl_contract.id


def test_build_company_overview_end_to_end_protocol_path(db_session):
    """Top-level: protocol + a single non-proxy contract returns a sane payload."""
    p = _add_protocol(db_session, f"e2e-alpha-{uuid.uuid4().hex[:8]}")
    addr = _addr("e2e1")
    job = _add_job(db_session, address=addr, protocol_id=p.id, name="Vault")
    _add_contract(db_session, address=addr, job=job, protocol_id=p.id, contract_name="Vault")

    payload = build_company_overview(db_session, p.name)

    assert payload["company"] == p.name
    assert payload["protocol_id"] == p.id
    assert payload["contract_count"] == 1
    addrs = {c["address"] for c in payload["contracts"]}
    assert addr in addrs
    # all_addresses includes every Contract row for the protocol
    assert any(a["address"] == addr for a in payload["all_addresses"])


def test_build_company_overview_raises_when_unknown(db_session):
    with pytest.raises(CompanyNotFound):
        build_company_overview(db_session, f"missing-{uuid.uuid4().hex[:8]}")


def test_build_company_overview_proxy_uses_impl_name(db_session):
    """A proxy contract's overview entry inherits the impl's contract name
    instead of the generic ``ERC1967Proxy``-style template name.
    """
    p = _add_protocol(db_session, f"e2e-proxy-{uuid.uuid4().hex[:8]}")
    proxy_addr = _addr("px2")
    impl_addr = _addr("im2")

    proxy_job = _add_job(db_session, address=proxy_addr, protocol_id=p.id, is_proxy=True)
    impl_job = _add_job(db_session, address=impl_addr, protocol_id=p.id)
    _add_contract(
        db_session,
        address=proxy_addr,
        job=proxy_job,
        protocol_id=p.id,
        is_proxy=True,
        implementation=impl_addr,
        contract_name="ERC1967Proxy",
    )
    _add_contract(db_session, address=impl_addr, job=impl_job, protocol_id=p.id, contract_name="VaultImpl")

    payload = build_company_overview(db_session, p.name)
    proxy_entry = next(c for c in payload["contracts"] if c["address"] == proxy_addr)
    assert proxy_entry["is_proxy"] is True
    assert proxy_entry["implementation"] == impl_addr
    # Inherited name from the impl, not the generic proxy name
    assert proxy_entry["name"] == "VaultImpl"
