from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from dotenv import load_dotenv

from db.models import Contract, Job, JobStage, JobStatus, Protocol
from services.chat import agent as agent_mod
from tests.conftest import requires_postgres


def _openrouter_key_available() -> bool:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    return bool(os.getenv("OPEN_ROUTER_KEY"))


pytestmark = [
    pytest.mark.live,
    requires_postgres,
    pytest.mark.skipif(not _openrouter_key_available(), reason="OPEN_ROUTER_KEY not set"),
]


class _AgentSessionProxy:
    def __init__(self, session):
        self._session = session

    def __getattr__(self, name):
        return getattr(self._session, name)

    def close(self) -> None:
        pass


def test_live_agent_answers_selected_contract_question(db_session, monkeypatch):
    company = f"psat-live-agent-{uuid.uuid4().hex[:8]}"
    address = "0x" + uuid.uuid4().hex + uuid.uuid4().hex[:8]

    protocol = Protocol(name=company, chains=["ethereum"])
    db_session.add(protocol)
    db_session.flush()

    job = Job(
        address=address,
        company=company,
        name="LiveAgentVault",
        status=JobStatus.completed,
        stage=JobStage.done,
        protocol_id=protocol.id,
    )
    db_session.add(job)
    db_session.flush()

    contract = Contract(
        job_id=job.id,
        protocol_id=protocol.id,
        address=address,
        chain="ethereum",
        contract_name="LiveAgentVault",
        source_verified=True,
    )
    db_session.add(contract)
    db_session.commit()

    monkeypatch.setattr(agent_mod, "SessionLocal", lambda: _AgentSessionProxy(db_session))
    monkeypatch.setattr(agent_mod, "MAX_ITERATIONS", 2)
    if live_model := os.getenv("PSAT_LIVE_AGENT_MODEL"):
        monkeypatch.setattr(agent_mod, "AGENT_MODEL", live_model)

    try:
        events = list(
            agent_mod.run_agent_stream(
                (
                    "Answer in one short sentence. Include the exact selected contract name "
                    "LiveAgentVault and its exact address."
                ),
                [],
                agent_mod.AgentContext(company=company, selected_address=address, selected_chain="ethereum"),
            )
        )
    finally:
        db_session.query(Contract).filter_by(id=contract.id).delete()
        db_session.query(Job).filter_by(id=job.id).delete()
        db_session.query(Protocol).filter_by(id=protocol.id).delete()
        db_session.commit()

    errors = [event for event in events if event["event"] == "error"]
    assert not errors
    assert any(event["event"] == "done" for event in events)

    answer = "".join(event["data"].get("text", "") for event in events if event["event"] == "token")
    assert answer.strip()
    assert "liveagentvault" in answer.lower()
    assert address.lower() in answer.lower()
