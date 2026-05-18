"""Bridge peer analysis status and queueing helpers."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from db.models import Contract, ContractSummary, Job, JobStage, JobStatus, Protocol
from db.queue import create_job, find_existing_job_for_address, upsert_discovered_contract
from services.bridges.chains import chain_id_for_chain, normalize_chain_name, rpc_url_for_chain


def _job_status(job: Job | None) -> str:
    if job is None:
        return "not_queued"
    if job.status == JobStatus.completed and job.stage == JobStage.done:
        return "analyzed"
    if job.status in {JobStatus.queued, JobStatus.processing}:
        return job.status.value
    return job.status.value


def _contract_for_peer(session: Session, address: str, chain: str | None) -> Contract | None:
    stmt = select(Contract).where(func.lower(Contract.address) == address.lower())
    if chain is not None:
        stmt = stmt.where(Contract.chain == chain)
    return session.execute(stmt.limit(1)).scalar_one_or_none()


def _summary_for_contract(session: Session, contract: Contract | None) -> ContractSummary | None:
    if contract is None:
        return None
    return session.execute(
        select(ContractSummary).where(ContractSummary.contract_id == contract.id).limit(1)
    ).scalar_one_or_none()


def _route_peer_status(session: Session, route: dict[str, Any]) -> dict[str, Any]:
    peer = route.get("peer_address")
    if not isinstance(peer, str) or not peer.startswith("0x") or len(peer) != 42:
        return {"status": "non_evm_peer" if route.get("peer") else "not_applicable"}

    chain = normalize_chain_name(str(route.get("chain") or "")) or route.get("chain")
    if not chain:
        return {"status": "missing_chain", "address": peer.lower()}

    contract = _contract_for_peer(session, peer, chain)
    job = contract.job if contract and contract.job_id else find_existing_job_for_address(session, peer, chain=chain)
    summary = _summary_for_contract(session, contract)
    out: dict[str, Any] = {
        "status": _job_status(job),
        "address": peer.lower(),
        "chain": chain,
    }
    if job is not None:
        out["job_id"] = str(job.id)
    if contract is not None:
        out.update(
            {
                "contract_id": contract.id,
                "name": contract.contract_name,
                "source_verified": contract.source_verified if contract.source_verified is not None else None,
                "is_proxy": bool(contract.is_proxy),
                "implementation": contract.implementation,
            }
        )
    if summary is not None:
        out.update(
            {
                "control_model": summary.control_model,
                "is_upgradeable": summary.is_upgradeable,
                "risk_level": summary.risk_level,
            }
        )
    return out


def annotate_bridge_peer_analysis(session: Session, runtime: dict[str, Any]) -> dict[str, Any]:
    """Return runtime context with route-level peer analysis status filled in."""
    routes = []
    for route in runtime.get("routes") or []:
        if not isinstance(route, dict):
            continue
        routes.append({**route, "peer_analysis": _route_peer_status(session, route)})
    return {**runtime, "routes": routes}


def queue_bridge_peer_analysis(
    session: Session,
    *,
    source_job: Job,
    source_contract: Contract | None,
    runtime: dict[str, Any],
    default_rpc_url: str,
) -> dict[str, Any]:
    """Upsert and queue every EVM peer route returned by a bridge resolver."""
    if runtime.get("status") != "resolved":
        return annotate_bridge_peer_analysis(session, runtime)

    protocol_id = source_job.protocol_id or (source_contract.protocol_id if source_contract else None)
    protocol_name = source_job.company
    if protocol_id and not protocol_name:
        protocol = session.get(Protocol, protocol_id)
        protocol_name = protocol.name if protocol else None

    updated_routes: list[dict[str, Any]] = []
    for route in runtime.get("routes") or []:
        if not isinstance(route, dict):
            continue
        route = dict(route)
        peer = route.get("peer_address")
        chain = normalize_chain_name(str(route.get("chain") or "")) or route.get("chain")
        if not isinstance(peer, str) or not peer.startswith("0x") or len(peer) != 42 or not chain:
            route["peer_analysis"] = _route_peer_status(session, route)
            updated_routes.append(route)
            continue

        peer = peer.lower()
        peer_rpc = rpc_url_for_chain(chain, default_rpc_url)
        peer_chain_id = chain_id_for_chain(chain)
        if peer_chain_id is None or not peer_rpc:
            route["peer_analysis"] = {
                "status": "unsupported_chain" if peer_chain_id is None else "missing_rpc",
                "address": peer,
                "chain": chain,
                "chain_id": peer_chain_id,
                "rpc_url_available": bool(peer_rpc),
            }
            updated_routes.append(route)
            continue
        row = upsert_discovered_contract(
            session,
            address=peer,
            chain=chain,
            protocol_id=protocol_id,
            new_sources=["bridge_runtime"],
            contract_name=f"{route.get('chain_display_name') or chain} bridge peer",
            confidence=0.95,
            chains=[chain],
            discovery_url=f"bridge_runtime:{source_job.address or ''}",
        )
        existing = find_existing_job_for_address(session, peer, chain=chain)
        if existing is None:
            request = {
                "address": peer,
                "name": row.contract_name or peer,
                "chain": chain,
                "chain_id": peer_chain_id,
                "rpc_url": peer_rpc,
                "parent_job_id": str(source_job.id),
                "discovered_by": "bridge_runtime",
                "bridge_source_address": source_job.address,
                "bridge_source_chain": (source_contract.chain if source_contract else None) or "ethereum",
                "bridge_protocol": runtime.get("protocol"),
                "protocol_id": protocol_id,
                "company": protocol_name,
            }
            peer_job = create_job(session, request, initial_stage=JobStage.discovery)
            row.job_id = peer_job.id
            status = "queued"
            job_id = peer_job.id
        else:
            row.job_id = existing.id
            status = _job_status(existing)
            job_id = existing.id
        session.commit()

        route["peer_analysis"] = {
            "status": status,
            "address": peer,
            "chain": chain,
            "chain_id": peer_chain_id,
            "job_id": str(job_id),
            "contract_id": row.id,
            "source_verified": row.source_verified,
            "is_proxy": bool(row.is_proxy),
            "implementation": row.implementation,
            "rpc_url_available": bool(peer_rpc),
            "queue_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{chain}:{peer}:{source_job.id}")),
        }
        updated_routes.append(route)

    return {**runtime, "routes": updated_routes}
