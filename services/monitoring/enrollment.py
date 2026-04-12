"""Auto-enrollment of protocol contracts into the unified monitoring system."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import (
    Contract,
    ContractSummary,
    ControlGraphNode,
    ControllerValue,
    Job,
    JobStatus,
    MonitoredContract,
    WatchedProxy,
)
from utils.rpc import rpc_request

logger = logging.getLogger(__name__)


def maybe_enroll_protocol(
    session: Session,
    protocol_id: int,
    rpc_url: str,
    chain: str = "ethereum",
    exclude_job_id: Any = None,
) -> bool:
    """Enroll a protocol's contracts if all jobs are complete.

    Called at the end of PolicyWorker.process(). Returns True if enrollment
    was performed, False if skipped (in-flight jobs or no completed jobs).

    *exclude_job_id* should be the current job's id — it's still in
    ``processing`` status when this is called from inside ``process()``,
    so it must be excluded from the in-flight check.
    """
    # Check for in-flight jobs for this protocol (excluding the calling job)
    stmt = select(Job).where(
        Job.protocol_id == protocol_id,
        Job.status.in_([JobStatus.queued, JobStatus.processing]),
    )
    if exclude_job_id is not None:
        stmt = stmt.where(Job.id != exclude_job_id)
    in_flight = session.execute(stmt).scalars().first()

    if in_flight:
        logger.debug("Protocol %s has in-flight jobs, skipping enrollment", protocol_id)
        return False

    # Check that at least 1 completed job exists
    completed = session.execute(
        select(Job).where(
            Job.protocol_id == protocol_id,
            Job.status == JobStatus.completed,
        )
    ).scalars().first()

    if not completed:
        logger.debug("Protocol %s has no completed jobs, skipping enrollment", protocol_id)
        return False

    enroll_protocol_contracts(session, protocol_id, rpc_url, chain)
    return True


def enroll_protocol_contracts(
    session: Session,
    protocol_id: int,
    rpc_url: str,
    chain: str = "ethereum",
) -> list[MonitoredContract]:
    """Create MonitoredContract rows for all contracts in a protocol.

    Performs upsert (ON CONFLICT address+chain DO UPDATE) so this is
    idempotent. Also creates WatchedProxy rows for proxy contracts and
    discovers controller addresses (safes, timelocks) from the control graph.

    Returns list of created/updated MonitoredContract rows.
    """
    contracts = (
        session.execute(select(Contract).where(Contract.protocol_id == protocol_id))
        .scalars()
        .all()
    )

    if not contracts:
        logger.info("Protocol %s has no contracts, nothing to enroll", protocol_id)
        return []

    # Get current block number for last_scanned_block
    try:
        result = rpc_request(rpc_url, "eth_blockNumber", [])
        current_block = int(result, 16)
    except Exception:
        logger.warning("Could not get current block, defaulting to 0")
        current_block = 0

    enrolled: list[MonitoredContract] = []

    for contract in contracts:
        contract_chain = contract.chain or chain

        # Load summary
        summary = session.execute(
            select(ContractSummary).where(ContractSummary.contract_id == contract.id)
        ).scalar_one_or_none()

        # Load controller values
        cv_rows = (
            session.execute(
                select(ControllerValue).where(ControllerValue.contract_id == contract.id)
            )
            .scalars()
            .all()
        )

        # Determine contract type
        contract_type = _determine_contract_type(summary, cv_rows)

        # Build monitoring config and initial state
        monitoring_config = _build_monitoring_config(summary, cv_rows, contract_type)
        initial_state = _build_initial_state(contract, cv_rows)

        # Check for existing MonitoredContract
        existing = session.execute(
            select(MonitoredContract).where(
                MonitoredContract.address == contract.address.lower(),
                MonitoredContract.chain == contract_chain,
            )
        ).scalar_one_or_none()

        if existing:
            existing.protocol_id = protocol_id
            existing.contract_id = contract.id
            existing.contract_type = contract_type
            existing.monitoring_config = monitoring_config
            existing.last_known_state = initial_state
            existing.is_active = True
            mc = existing
        else:
            mc = MonitoredContract(
                id=uuid.uuid4(),
                address=contract.address.lower(),
                chain=contract_chain,
                protocol_id=protocol_id,
                contract_id=contract.id,
                contract_type=contract_type,
                monitoring_config=monitoring_config,
                last_known_state=initial_state,
                last_scanned_block=current_block,
                needs_polling=contract_type in ("proxy", "safe", "timelock"),
                is_active=True,
                enrollment_source="auto",
            )
            session.add(mc)
            session.flush()

        # For proxy contracts, also create/link WatchedProxy
        if contract_type == "proxy":
            _bridge_to_watched_proxy(session, mc, contract, current_block)

        enrolled.append(mc)

    # Discover controller addresses from the control graph
    _enroll_controller_addresses(session, contracts, protocol_id, chain, current_block)

    session.commit()
    logger.info(
        "Enrolled %d contracts for protocol %s",
        len(enrolled),
        protocol_id,
    )
    return enrolled


def _determine_contract_type(
    summary: ContractSummary | None,
    controller_values: Sequence[ControllerValue],
) -> str:
    """Determine the contract_type based on analysis results."""
    if summary:
        if summary.is_upgradeable:
            return "proxy"
        if summary.has_timelock:
            return "timelock"
        if summary.is_pausable:
            return "pausable"

    # Check controller values for resolved types
    for cv in controller_values:
        if cv.resolved_type == "safe":
            return "safe"
        if cv.resolved_type == "timelock":
            return "timelock"
        if cv.resolved_type == "proxy_admin":
            return "proxy"

    return "regular"


def _build_monitoring_config(
    summary: ContractSummary | None,
    controller_values: Sequence[ControllerValue],  # noqa: ARG001 — reserved for future use
    contract_type: str,
) -> dict[str, Any]:
    """Build the monitoring_config JSONB based on detected capabilities."""
    config: dict[str, Any] = {
        "watch_upgrades": contract_type == "proxy",
        "watch_ownership": True,
        "watch_pause": False,
        "watch_roles": False,
        "watch_safe_signers": contract_type == "safe",
        "watch_timelock": contract_type == "timelock",
    }

    if summary:
        if summary.is_pausable:
            config["watch_pause"] = True
        if summary.control_model and "role" in (summary.control_model or "").lower():
            config["watch_roles"] = True

    return config


def _build_initial_state(
    contract: Contract,
    controller_values: Sequence[ControllerValue],
) -> dict[str, Any]:
    """Build the last_known_state dict from existing pipeline data."""
    state: dict[str, Any] = {}

    if contract.implementation:
        state["implementation"] = contract.implementation

    for cv in controller_values:
        cid = cv.controller_id.lower() if cv.controller_id else ""
        if "owner" in cid and cv.value:
            state["owner"] = cv.value
        elif "admin" in cid and cv.value:
            state["admin"] = cv.value

    return state


def _bridge_to_watched_proxy(
    session: Session,
    mc: MonitoredContract,
    contract: Contract,
    current_block: int,
) -> None:
    """Create or link a WatchedProxy row for backward compatibility."""
    existing_wp = session.execute(
        select(WatchedProxy).where(
            WatchedProxy.proxy_address == contract.address.lower(),
            WatchedProxy.chain == (contract.chain or "ethereum"),
        )
    ).scalar_one_or_none()

    if existing_wp:
        mc.watched_proxy_id = existing_wp.id
    else:
        wp = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address=contract.address.lower(),
            chain=contract.chain or "ethereum",
            label=contract.contract_name,
            proxy_type=contract.proxy_type,
            last_known_implementation=contract.implementation,
            last_scanned_block=current_block,
            needs_polling=True,
        )
        session.add(wp)
        session.flush()
        mc.watched_proxy_id = wp.id


def _enroll_controller_addresses(
    session: Session,
    contracts: Sequence[Contract],
    protocol_id: int,
    chain: str,
    current_block: int,
) -> None:
    """Discover and enroll controller addresses from control graph nodes."""
    # Collect all contract addresses already enrolled
    enrolled_addrs = {c.address.lower() for c in contracts}

    for contract in contracts:
        nodes = (
            session.execute(
                select(ControlGraphNode).where(
                    ControlGraphNode.contract_id == contract.id
                )
            )
            .scalars()
            .all()
        )

        for node in nodes:
            addr = node.address.lower() if node.address else ""
            if not addr or addr in enrolled_addrs:
                continue

            # Determine type from node
            node_type = "regular"
            if node.resolved_type in ("safe", "gnosis_safe"):
                node_type = "safe"
            elif node.resolved_type == "timelock":
                node_type = "timelock"
            elif node.resolved_type in ("proxy", "proxy_admin"):
                node_type = "proxy"

            if node_type == "regular":
                continue

            existing = session.execute(
                select(MonitoredContract).where(
                    MonitoredContract.address == addr,
                    MonitoredContract.chain == chain,
                )
            ).scalar_one_or_none()

            if not existing:
                config = _build_monitoring_config(None, [], node_type)
                mc = MonitoredContract(
                    id=uuid.uuid4(),
                    address=addr,
                    chain=chain,
                    protocol_id=protocol_id,
                    contract_type=node_type,
                    monitoring_config=config,
                    last_known_state={},
                    last_scanned_block=current_block,
                    needs_polling=node_type in ("safe", "timelock"),
                    is_active=True,
                    enrollment_source="auto",
                )
                session.add(mc)
                enrolled_addrs.add(addr)
