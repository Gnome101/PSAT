"""MonitoredContract listing and updates + MonitoredEvent listing."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select

from db.models import Contract, MonitoredContract, MonitoredEvent, Protocol
from schemas.api_requests import UpdateMonitoredContractRequest, UpsertMonitoredContractRequest

from . import deps

router = APIRouter()


def _monitored_contract_payload(c: MonitoredContract) -> dict[str, Any]:
    return {
        "id": str(c.id),
        "address": c.address,
        "chain": c.chain,
        "protocol_id": c.protocol_id,
        "contract_id": c.contract_id,
        "contract_type": c.contract_type,
        "monitoring_config": c.monitoring_config,
        "last_known_state": c.last_known_state,
        "last_scanned_block": c.last_scanned_block,
        "needs_polling": c.needs_polling,
        "is_active": c.is_active,
        "enrollment_source": c.enrollment_source,
        "created_at": c.created_at.isoformat() if c.created_at else None,
    }


@router.get("/api/monitored-contracts")
def list_monitored_contracts(
    protocol_id: int | None = None,
    chain: str | None = None,
) -> list[dict[str, Any]]:
    """List all MonitoredContract rows, optionally filtered."""
    with deps.SessionLocal() as session:
        stmt = select(MonitoredContract).order_by(MonitoredContract.created_at.desc())
        if protocol_id is not None:
            stmt = stmt.where(MonitoredContract.protocol_id == protocol_id)
        if chain is not None:
            stmt = stmt.where(MonitoredContract.chain == chain)
        contracts = session.execute(stmt).scalars().all()
        return [_monitored_contract_payload(c) for c in contracts]


@router.post("/api/protocols/{protocol_id}/monitoring", dependencies=[Depends(deps.require_admin_key)])
def upsert_protocol_monitoring(protocol_id: int, request: UpsertMonitoredContractRequest) -> dict[str, Any]:
    """Create or update one monitored contract for a protocol."""
    with deps.SessionLocal() as session:
        protocol = session.get(Protocol, protocol_id)
        if protocol is None:
            raise HTTPException(status_code=404, detail="Protocol not found")

        contract_stmt = select(Contract).where(
            Contract.protocol_id == protocol_id,
            func.lower(Contract.address) == request.address.lower(),
        )
        if request.chain:
            contract_stmt = contract_stmt.where(Contract.chain == request.chain)
        contract = session.execute(contract_stmt).scalar_one_or_none()

        existing = session.execute(
            select(MonitoredContract).where(
                MonitoredContract.address == request.address,
                MonitoredContract.chain == request.chain,
            )
        ).scalar_one_or_none()

        if existing is None:
            existing = MonitoredContract(
                address=request.address,
                chain=request.chain,
                protocol_id=protocol_id,
                contract_id=contract.id if contract else None,
                contract_type=request.contract_type,
                monitoring_config=request.monitoring_config,
                last_known_state={},
                last_scanned_block=0,
                needs_polling=request.needs_polling,
                is_active=request.is_active,
                enrollment_source="surface_alert",
            )
            session.add(existing)
        else:
            existing.protocol_id = protocol_id
            existing.contract_id = contract.id if contract else existing.contract_id
            existing.contract_type = request.contract_type
            existing.monitoring_config = request.monitoring_config
            existing.needs_polling = request.needs_polling
            existing.is_active = request.is_active
            existing.enrollment_source = existing.enrollment_source or "surface_alert"

        session.commit()
        session.refresh(existing)
        return _monitored_contract_payload(existing)


@router.patch("/api/monitored-contracts/{contract_id}", dependencies=[Depends(deps.require_admin_key)])
def update_monitored_contract(contract_id: str, request: UpdateMonitoredContractRequest) -> dict[str, Any]:
    """Update monitoring_config, is_active, or needs_polling on a MonitoredContract."""
    with deps.SessionLocal() as session:
        mc = session.get(MonitoredContract, uuid.UUID(contract_id))
        if mc is None:
            raise HTTPException(status_code=404, detail="MonitoredContract not found")

        if request.monitoring_config is not None:
            mc.monitoring_config = request.monitoring_config
        if request.is_active is not None:
            mc.is_active = request.is_active
        if request.needs_polling is not None:
            mc.needs_polling = request.needs_polling

        session.commit()
        session.refresh(mc)
        return _monitored_contract_payload(mc)


@router.get("/api/monitored-events")
def list_monitored_events(
    contract_id: str | None = None,
    event_type: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """List all MonitoredEvent rows, optionally filtered."""
    with deps.SessionLocal() as session:
        stmt = select(MonitoredEvent).order_by(MonitoredEvent.detected_at.desc()).limit(limit)
        if contract_id is not None:
            stmt = stmt.where(MonitoredEvent.monitored_contract_id == contract_id)
        if event_type is not None:
            stmt = stmt.where(MonitoredEvent.event_type == event_type)
        events = session.execute(stmt).scalars().all()
        return [
            {
                "id": str(e.id),
                "monitored_contract_id": str(e.monitored_contract_id),
                "event_type": e.event_type,
                "block_number": e.block_number,
                "tx_hash": e.tx_hash,
                "data": e.data,
                "detected_at": e.detected_at.isoformat() if e.detected_at else None,
            }
            for e in events
        ]
