"""MonitoredContract listing and updates + MonitoredEvent listing."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from db.models import MonitoredContract, MonitoredEvent
from schemas.api_requests import UpdateMonitoredContractRequest

from . import deps

router = APIRouter()


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
        return [
            {
                "id": str(c.id),
                "address": c.address,
                "chain": c.chain,
                "protocol_id": c.protocol_id,
                "contract_type": c.contract_type,
                "monitoring_config": c.monitoring_config,
                "last_known_state": c.last_known_state,
                "last_scanned_block": c.last_scanned_block,
                "needs_polling": c.needs_polling,
                "is_active": c.is_active,
                "enrollment_source": c.enrollment_source,
                "created_at": c.created_at.isoformat() if c.created_at else None,
            }
            for c in contracts
        ]


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
        return {
            "id": str(mc.id),
            "address": mc.address,
            "chain": mc.chain,
            "protocol_id": mc.protocol_id,
            "contract_type": mc.contract_type,
            "monitoring_config": mc.monitoring_config,
            "last_known_state": mc.last_known_state,
            "last_scanned_block": mc.last_scanned_block,
            "needs_polling": mc.needs_polling,
            "is_active": mc.is_active,
            "enrollment_source": mc.enrollment_source,
            "created_at": mc.created_at.isoformat() if mc.created_at else None,
        }


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
