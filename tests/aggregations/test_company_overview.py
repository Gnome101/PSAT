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

from db.models import (  # noqa: E402
    Contract,
    EffectiveFunction,
    FunctionPrincipal,
    Job,
    JobStage,
    JobStatus,
    Protocol,
)
from services.aggregations.company_overview import (  # noqa: E402
    CompanyNotFound,
    build_company_overview,
    build_functions_for_protocol,
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
    # The full inventory moved to /api/company/{name}/addresses; the main
    # payload only carries the count.
    assert payload["all_addresses_count"] == 1


def test_build_company_overview_raises_when_unknown(db_session):
    with pytest.raises(CompanyNotFound):
        build_company_overview(db_session, f"missing-{uuid.uuid4().hex[:8]}")


def test_build_company_overview_omits_functions_field(db_session):
    """``functions`` no longer ships in the main payload — it's served by
    ``/api/company/{name}/functions`` and fetched lazily by the frontend.
    """
    p = _add_protocol(db_session, f"e2e-nofn-{uuid.uuid4().hex[:8]}")
    addr = _addr("nofn1")
    job = _add_job(db_session, address=addr, protocol_id=p.id, name="Vault")
    c = _add_contract(db_session, address=addr, job=job, protocol_id=p.id, contract_name="Vault")
    # Seed an EF row so the lightweight projection has something to walk.
    db_session.add(
        EffectiveFunction(
            contract_id=c.id,
            function_name="pause",
            selector="0xabcdef01",
            abi_signature="pause()",
            effect_labels=["pause_toggle"],
            effect_targets=[],
            action_summary="pause",
            authority_public=False,
            authority_roles=[],
        )
    )
    db_session.commit()

    payload = build_company_overview(db_session, p.name)
    entry = next(c for c in payload["contracts"] if c["address"] == addr)
    assert "functions" not in entry
    # value_effects / capabilities should still derive from the lightweight
    # ef_effects projection — pause_toggle → "pause" capability.
    assert "pause" in entry["capabilities"]


def test_build_functions_for_protocol_returns_keyed_function_list(db_session):
    """``build_functions_for_protocol`` returns ``{address: [function_entries]}``
    using the same shape that previously lived on each contract entry.
    """
    p = _add_protocol(db_session, f"functions-{uuid.uuid4().hex[:8]}")
    addr = _addr("fn1")
    job = _add_job(db_session, address=addr, protocol_id=p.id, name="Vault")
    contract = _add_contract(db_session, address=addr, job=job, protocol_id=p.id, contract_name="Vault")
    ef = EffectiveFunction(
        contract_id=contract.id,
        function_name="transfer",
        selector="0xa9059cbb",
        abi_signature="transfer(address,uint256)",
        effect_labels=["asset_send"],
        effect_targets=[],
        action_summary="transfer assets",
        authority_public=True,
        authority_roles=[],
    )
    db_session.add(ef)
    db_session.flush()
    db_session.add(
        FunctionPrincipal(
            function_id=ef.id,
            address=_addr("safe1"),
            resolved_type="safe",
            origin="role 0",
            principal_type="authority_role",
            details={"owners": [_addr("o1"), _addr("o2")], "threshold": 2},
        )
    )
    db_session.commit()

    out = build_functions_for_protocol(db_session, p.name)
    assert addr in out
    entries = out[addr]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["function"] == "transfer(address,uint256)"
    assert entry["selector"] == "0xa9059cbb"
    assert entry["effect_labels"] == ["asset_send"]
    assert entry["authority_public"] is True
    # The FP row was principal_type=authority_role, so it should bucket
    # under authority_roles rather than direct_owner.
    assert entry["authority_roles"], "authority_roles should be populated"
    assert entry["direct_owner"] is None


def test_build_functions_for_protocol_unknown_company_raises(db_session):
    with pytest.raises(CompanyNotFound):
        build_functions_for_protocol(db_session, f"missing-{uuid.uuid4().hex[:8]}")


def test_build_functions_for_protocol_proxy_uses_impl(db_session):
    """Proxy entries inherit functions from the impl contract's EF rows."""
    p = _add_protocol(db_session, f"functions-proxy-{uuid.uuid4().hex[:8]}")
    proxy_addr = _addr("pxfn")
    impl_addr = _addr("imfn")

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
    impl_contract = _add_contract(
        db_session, address=impl_addr, job=impl_job, protocol_id=p.id, contract_name="VaultImpl"
    )
    db_session.add(
        EffectiveFunction(
            contract_id=impl_contract.id,
            function_name="upgradeTo",
            selector="0x3659cfe6",
            abi_signature="upgradeTo(address)",
            effect_labels=["implementation_update"],
            effect_targets=[],
            action_summary="upgrade",
            authority_public=False,
            authority_roles=[],
        )
    )
    db_session.commit()

    out = build_functions_for_protocol(db_session, p.name)
    # Function is keyed to the proxy's address (what the user sees), not
    # the impl's address.
    assert proxy_addr in out
    assert any(entry["function"] == "upgradeTo(address)" for entry in out[proxy_addr])


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
