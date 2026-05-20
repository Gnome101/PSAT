from __future__ import annotations

import uuid

from db.models import Contract, Job, JobStage, JobStatus, Protocol
from services.bridges.peer_analysis import queue_bridge_peer_analysis
from tests.conftest import requires_postgres

pytestmark = requires_postgres


def _addr() -> str:
    return "0x" + (uuid.uuid4().hex + uuid.uuid4().hex)[:40]


def test_queue_bridge_peer_analysis_queues_chain_scoped_peer(db_session, monkeypatch) -> None:
    monkeypatch.setenv("ERPC_BASE_URL", "https://erpc-proxy.example")
    protocol = Protocol(name=f"bridge-peer-{uuid.uuid4().hex[:8]}")
    db_session.add(protocol)
    db_session.flush()

    source_addr = _addr()
    peer_addr = _addr()
    source_job = Job(
        id=uuid.uuid4(),
        address=source_addr,
        company=protocol.name,
        protocol_id=protocol.id,
        status=JobStatus.completed,
        stage=JobStage.done,
        request={"address": source_addr, "chain": "ethereum"},
    )
    db_session.add(source_job)
    db_session.flush()
    source_contract = Contract(
        address=source_addr,
        chain="ethereum",
        job_id=source_job.id,
        protocol_id=protocol.id,
        contract_name="BridgeSource",
    )
    db_session.add(source_contract)
    db_session.commit()

    queued = queue_bridge_peer_analysis(
        db_session,
        source_job=source_job,
        source_contract=source_contract,
        runtime={
            "status": "resolved",
            "protocol": "LayerZero",
            "routes": [
                {
                    "chain": "base",
                    "chain_display_name": "Base",
                    "peer": peer_addr,
                    "peer_address": peer_addr,
                }
            ],
        },
        default_rpc_url="https://ethereum.example",
    )

    route_status = queued["routes"][0]["peer_analysis"]
    assert route_status["status"] == "queued"
    assert route_status["chain"] == "base"
    assert route_status["chain_id"] == 8453
    assert route_status["rpc_url_available"] is True

    peer_job = db_session.get(Job, uuid.UUID(route_status["job_id"]))
    assert peer_job is not None
    assert peer_job.request["chain"] == "base"
    assert peer_job.request["chain_id"] == 8453
    assert peer_job.request["rpc_url"] == "https://erpc-proxy.example/main/evm/8453"

    peer_contract = (
        db_session.query(Contract).filter(Contract.address == peer_addr.lower(), Contract.chain == "base").one()
    )
    assert peer_contract.protocol_id == protocol.id
    assert "bridge_runtime" in peer_contract.discovery_sources


def test_queue_bridge_peer_analysis_marks_missing_rpc_without_erpc(db_session, monkeypatch) -> None:
    monkeypatch.delenv("ERPC_BASE_URL", raising=False)
    source_addr = _addr()
    peer_addr = _addr()
    source_job = Job(
        id=uuid.uuid4(),
        address=source_addr,
        status=JobStatus.completed,
        stage=JobStage.done,
        request={"address": source_addr, "chain": "ethereum"},
    )
    db_session.add(source_job)
    db_session.commit()

    queued = queue_bridge_peer_analysis(
        db_session,
        source_job=source_job,
        source_contract=None,
        runtime={
            "status": "resolved",
            "protocol": "LayerZero",
            "routes": [{"chain": "base", "peer_address": peer_addr}],
        },
        default_rpc_url="https://ethereum.example",
    )

    assert queued["routes"][0]["peer_analysis"]["status"] == "missing_rpc"
