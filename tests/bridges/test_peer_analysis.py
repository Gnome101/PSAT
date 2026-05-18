from __future__ import annotations

import uuid

from db.models import Contract, Job, JobStage, JobStatus, Protocol
from services.bridges.peer_analysis import annotate_bridge_peer_analysis, queue_bridge_peer_analysis
from tests.conftest import requires_postgres

pytestmark = requires_postgres


def _addr() -> str:
    return "0x" + (uuid.uuid4().hex + uuid.uuid4().hex)[:40]


def test_queue_bridge_peer_analysis_queues_distinct_chain_peer(db_session) -> None:
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

    runtime = {
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
    }

    queued = queue_bridge_peer_analysis(
        db_session,
        source_job=source_job,
        source_contract=source_contract,
        runtime=runtime,
        default_rpc_url="https://eth-mainnet.g.alchemy.com/v2/test-key",
    )

    route_status = queued["routes"][0]["peer_analysis"]
    assert route_status["status"] == "queued"
    assert route_status["chain"] == "base"
    assert route_status["chain_id"] == 8453
    assert route_status["job_id"]

    peer_contract = (
        db_session.query(Contract).filter(Contract.address == peer_addr.lower(), Contract.chain == "base").one()
    )
    assert peer_contract.protocol_id == protocol.id
    assert "bridge_runtime" in peer_contract.discovery_sources


def test_queue_bridge_peer_analysis_skips_unsupported_chain_peer(db_session) -> None:
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
    db_session.flush()
    source_contract = Contract(address=source_addr, chain="ethereum", job_id=source_job.id)
    db_session.add(source_contract)
    db_session.commit()

    queued = queue_bridge_peer_analysis(
        db_session,
        source_job=source_job,
        source_contract=source_contract,
        runtime={
            "status": "resolved",
            "protocol": "LayerZero",
            "routes": [{"chain": "fantom", "peer": peer_addr, "peer_address": peer_addr}],
        },
        default_rpc_url="https://eth-mainnet.g.alchemy.com/v2/test-key",
    )

    route_status = queued["routes"][0]["peer_analysis"]
    assert route_status["status"] == "unsupported_chain"
    assert route_status["chain"] == "fantom"
    assert route_status["chain_id"] is None
    assert route_status["rpc_url_available"] is False
    assert "job_id" not in route_status
    assert db_session.query(Job).filter(Job.address == peer_addr.lower()).count() == 0
    assert db_session.query(Contract).filter(Contract.address == peer_addr.lower()).count() == 0


def test_annotate_bridge_peer_analysis_reports_existing_peer_status(db_session) -> None:
    peer_addr = _addr()
    job = Job(
        id=uuid.uuid4(),
        address=peer_addr,
        status=JobStatus.completed,
        stage=JobStage.done,
        request={"address": peer_addr, "chain": "base"},
    )
    db_session.add(job)
    db_session.flush()
    db_session.add(Contract(address=peer_addr, chain="base", job_id=job.id, contract_name="RemotePeer"))
    db_session.commit()

    annotated = annotate_bridge_peer_analysis(
        db_session,
        {
            "status": "resolved",
            "protocol": "LayerZero",
            "routes": [{"chain": "base", "peer_address": peer_addr}],
        },
    )

    assert annotated["routes"][0]["peer_analysis"]["status"] == "analyzed"
    assert annotated["routes"][0]["peer_analysis"]["name"] == "RemotePeer"
