from __future__ import annotations

from db.models import Contract
from db.queue import _contract_chain_id, upsert_discovered_contract
from tests.conftest import requires_postgres


def test_contract_chain_id_prefers_explicit_positive_value() -> None:
    assert _contract_chain_id("ethereum", "8453") == 8453


def test_contract_chain_id_falls_back_to_chain_name() -> None:
    assert _contract_chain_id("base", "not-a-chain-id") == 8453
    assert _contract_chain_id("unknown") is None


@requires_postgres
def test_upsert_discovered_contract_stores_chain_id(db_session) -> None:
    row = upsert_discovered_contract(
        db_session,
        address="0x" + "11" * 20,
        chain="base",
        protocol_id=None,
        new_sources=["inventory"],
    )
    db_session.commit()

    assert row.chain_id == 8453


@requires_postgres
def test_upsert_discovered_contract_backfills_missing_chain_id(db_session) -> None:
    address = "0x" + "22" * 20
    db_session.add(Contract(address=address, chain="arbitrum"))
    db_session.commit()

    row = upsert_discovered_contract(
        db_session,
        address=address,
        chain="arbitrum",
        protocol_id=None,
        new_sources=["inventory"],
    )
    db_session.commit()

    assert row.chain_id == 42161
