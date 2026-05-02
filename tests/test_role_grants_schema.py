"""Schema tests for the role_grants pipeline:

  * ``role_grants_events`` table + indexes exist
  * ``role_grants_cursors`` table exists
  * ``chain_finality_config`` is seeded with the 7 default chains

Skipped when no Postgres is reachable (mirrors the pattern in
``tests/test_queue.py``)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_DB_URL: str = os.environ.get(
    "TEST_DATABASE_URL", os.environ.get("DATABASE_URL", "")
) or ""


def _can_connect() -> bool:
    if not _DB_URL:
        return False
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(_DB_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(
    not _can_connect(), reason="PostgreSQL not available"
)


@requires_postgres
def test_role_grants_events_table_present():
    from sqlalchemy import create_engine, inspect

    engine = create_engine(_DB_URL)
    insp = inspect(engine)
    cols = {c["name"] for c in insp.get_columns("role_grants_events")}
    assert {
        "chain_id",
        "contract_id",
        "tx_hash",
        "log_index",
        "role",
        "member",
        "direction",
        "block_number",
        "block_hash",
        "transaction_index",
        "detected_at",
    } <= cols
    pk = insp.get_pk_constraint("role_grants_events")
    assert pk["constrained_columns"] == ["chain_id", "contract_id", "tx_hash", "log_index"]
    index_names = {ix["name"] for ix in insp.get_indexes("role_grants_events")}
    assert "ix_role_grants_events_lookup" in index_names
    assert "ix_role_grants_events_block" in index_names
    engine.dispose()


@requires_postgres
def test_role_grants_cursors_table_present():
    from sqlalchemy import create_engine, inspect

    engine = create_engine(_DB_URL)
    insp = inspect(engine)
    cols = {c["name"] for c in insp.get_columns("role_grants_cursors")}
    assert {
        "chain_id",
        "contract_id",
        "last_indexed_block",
        "last_indexed_block_hash",
        "last_run_at",
    } <= cols
    pk = insp.get_pk_constraint("role_grants_cursors")
    assert pk["constrained_columns"] == ["chain_id", "contract_id"]
    engine.dispose()


@requires_postgres
def test_role_grants_event_roundtrip():
    """Insert a synthetic RoleGrantsEvent + cursor row via the
    ORM, read it back, confirm bytea round-trip and that PK upserts
    on a duplicate (chain_id, contract_id, tx_hash, log_index)
    are rejected (the indexer relies on this for idempotency)."""
    from sqlalchemy import create_engine
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import Session

    from db.models import (
        Contract,
        Protocol,
        RoleGrantsCursor,
        RoleGrantsEvent,
    )

    engine = create_engine(_DB_URL)
    session = Session(engine, expire_on_commit=False)
    try:
        # FK to contracts.id requires a real Contract row.
        proto = Protocol(name="test_role_grants_proto")
        session.add(proto)
        session.flush()
        contract = Contract(
            address="0x" + "ab" * 20,
            chain="ethereum",
            protocol_id=proto.id,
        )
        session.add(contract)
        session.flush()

        role = b"\x01" * 32
        member = "0x" + "cd" * 20
        tx = b"\xaa" * 32
        block_hash = b"\xbb" * 32
        ev = RoleGrantsEvent(
            chain_id=1,
            contract_id=contract.id,
            tx_hash=tx,
            log_index=0,
            role=role,
            member=member,
            direction="grant",
            block_number=18_000_000,
            block_hash=block_hash,
            transaction_index=42,
        )
        session.add(ev)
        cursor = RoleGrantsCursor(
            chain_id=1,
            contract_id=contract.id,
            last_indexed_block=18_000_000,
            last_indexed_block_hash=block_hash,
        )
        session.add(cursor)
        session.commit()

        # Read back.
        got = session.query(RoleGrantsEvent).filter_by(contract_id=contract.id).one()
        assert got.role == role  # bytea round-trips
        assert got.member == member
        assert got.direction == "grant"
        assert got.block_number == 18_000_000

        # Duplicate PK insert raises IntegrityError — the indexer's
        # idempotency guarantee for re-scanning the same range.
        # Detach the persisted instance first so SQLAlchemy doesn't
        # short-circuit the duplicate detection in its identity map
        # (we want the constraint to fire at the DB level).
        session.expunge_all()
        dup = RoleGrantsEvent(
            chain_id=1,
            contract_id=contract.id,
            tx_hash=tx,
            log_index=0,
            role=role,
            member=member,
            direction="grant",
            block_number=18_000_000,
            block_hash=block_hash,
            transaction_index=42,
        )
        session.add(dup)
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()
    finally:
        # Clean up so the test is idempotent against repeat runs.
        session.query(RoleGrantsEvent).filter_by(contract_id=contract.id).delete()
        session.query(RoleGrantsCursor).filter_by(contract_id=contract.id).delete()
        session.query(Contract).filter_by(id=contract.id).delete()
        session.query(Protocol).filter_by(id=proto.id).delete()
        session.commit()
        session.close()
        engine.dispose()


@requires_postgres
def test_chain_finality_config_seeded():
    """The migration seeds 7 chains. Anyone running migrations on a
    fresh DB should see those rows; existing DBs may have local
    edits, so we assert the *baseline* set is present, not the exact
    set."""
    from sqlalchemy import create_engine, text

    engine = create_engine(_DB_URL)
    expected = {
        1: ("mainnet", 12),
        10: ("optimism", 24),
        137: ("polygon", 128),
        8453: ("base", 24),
        42161: ("arbitrum", 20),
        59144: ("linea", 24),
        534352: ("scroll", 24),
    }
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT chain_id, name, confirmation_depth FROM chain_finality_config")
        ).fetchall()
    actual = {row[0]: (row[1], row[2]) for row in rows}
    for cid, (name, depth) in expected.items():
        assert cid in actual, f"chain_id {cid} ({name}) missing from chain_finality_config"
        assert actual[cid] == (name, depth)
    engine.dispose()
