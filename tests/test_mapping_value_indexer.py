"""Unit tests for the mapping_value indexer step + Postgres repo.

Mirrors ``tests/test_role_grants_indexer.py``: a real Postgres
exercises the ``ON CONFLICT DO NOTHING`` semantics, advisory-lock
serialization (off in tests so they don't deadlock with each other),
finality-depth cap, and reorg rewind. Skipped when no DB is reachable.

The repo test seeds events directly (bypassing the indexer) and
verifies ``latest_keys_passing_predicate`` honors operator + RHS
correctly under the canonical ``(block_number, log_index)`` ordering.
"""

from __future__ import annotations

import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_DB_URL: str = os.environ.get("TEST_DATABASE_URL", os.environ.get("DATABASE_URL", "")) or ""


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


requires_postgres = pytest.mark.skipif(not _can_connect(), reason="PostgreSQL not available")


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class _FakeWrite:
    block_number: int
    block_hash: bytes
    tx_hash: bytes
    log_index: int
    transaction_index: int
    mapping_name: str
    key_hex: str
    value_hex: str


class FakeLogFetcher:
    def __init__(self, writes: list[_FakeWrite] | None = None):
        self.writes: list[_FakeWrite] = list(writes or [])
        self.calls: list[tuple[int, int]] = []

    def fetch_writes(self, *, chain_id, contract_address, from_block, to_block):
        from workers.mapping_value_indexer import FetchedMappingWrite

        self.calls.append((from_block, to_block))
        return [
            FetchedMappingWrite(
                block_number=w.block_number,
                block_hash=w.block_hash,
                tx_hash=w.tx_hash,
                log_index=w.log_index,
                transaction_index=w.transaction_index,
                mapping_name=w.mapping_name,
                key_hex=w.key_hex,
                value_hex=w.value_hex,
            )
            for w in self.writes
            if from_block <= w.block_number <= to_block
        ]


class FakeBlockHashFetcher:
    def __init__(self, hashes: dict[int, bytes] | None = None):
        self.hashes: dict[int, bytes] = dict(hashes or {})

    def block_hash(self, *, chain_id, block_number):
        return self.hashes.get(block_number)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def session_with_contract():
    if not _can_connect():
        pytest.skip("PostgreSQL not available")
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from db.models import (
        Contract,
        MappingValueCursor,
        MappingValueEvent,
        Protocol,
    )

    engine = create_engine(_DB_URL)
    session = Session(engine, expire_on_commit=False)
    suffix = uuid.uuid4().hex[:8]
    proto = Protocol(name=f"mapping_value_idx_{suffix}")
    session.add(proto)
    session.flush()
    addr = "0x" + uuid.uuid4().hex[:8] + "00" * 16
    contract = Contract(address=addr, chain="ethereum", protocol_id=proto.id)
    session.add(contract)
    session.flush()
    session.commit()

    cid = contract.id
    proto_id = proto.id
    try:
        yield session, cid, addr
    finally:
        session.query(MappingValueEvent).filter_by(contract_id=cid).delete()
        session.query(MappingValueCursor).filter_by(contract_id=cid).delete()
        session.query(Contract).filter_by(id=cid).delete()
        session.query(Protocol).filter_by(id=proto_id).delete()
        session.commit()
        session.close()
        engine.dispose()


def _set_event(
    block: int,
    log_index: int,
    mapping: str,
    key: str,
    value: int,
    *,
    hash_byte: int = 0xAA,
) -> _FakeWrite:
    bh = bytes([hash_byte]) * 32
    value_hex = "0x" + f"{value:064x}"
    return _FakeWrite(
        block_number=block,
        block_hash=bh,
        tx_hash=block.to_bytes(2, "big") + log_index.to_bytes(2, "big") + b"\x00" * 28,
        log_index=log_index,
        transaction_index=0,
        mapping_name=mapping,
        key_hex=key.lower(),
        value_hex=value_hex,
    )


# ---------------------------------------------------------------------------
# Indexer step tests
# ---------------------------------------------------------------------------


@requires_postgres
def test_fresh_index_inserts_and_advances_cursor(session_with_contract):
    from workers.mapping_value_indexer import index_mapping_values_step

    session, cid, addr = session_with_contract
    fetcher = FakeLogFetcher(
        [
            _set_event(100, 0, "owners", "0x" + "11" * 20, 10),
            _set_event(150, 0, "owners", "0x" + "22" * 20, 7),
        ]
    )
    hashes = FakeBlockHashFetcher({988: b"\xbb" * 32})
    result = index_mapping_values_step(
        session,
        chain_id=1,
        contract_id=cid,
        contract_address=addr,
        head_block=1000,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()

    assert result.inserted == 2
    assert result.new_cursor == 988
    assert not result.rewound

    # Idempotent re-run.
    result2 = index_mapping_values_step(
        session,
        chain_id=1,
        contract_id=cid,
        contract_address=addr,
        head_block=1000,
        log_fetcher=FakeLogFetcher([]),
        block_hash_fetcher=hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    assert result2.inserted == 0
    assert result2.new_cursor == 988


@requires_postgres
def test_reorg_rewind_deletes_events_and_rolls_cursor(session_with_contract):
    """When the cursor's stored hash disagrees with the chain at
    ``last_indexed_block``, the indexer rewinds ``finality_depth``
    blocks: events past the rewind point get deleted; the cursor is
    rolled back; the next scan re-ingests them.
    """
    from db.models import MappingValueEvent
    from workers.mapping_value_indexer import index_mapping_values_step

    session, cid, addr = session_with_contract
    # Phase 1: ingest with one chain history.
    chain_a_hashes = FakeBlockHashFetcher({988: b"\xaa" * 32, 980: b"\xa9" * 32})
    fetcher = FakeLogFetcher(
        [
            _set_event(950, 0, "owners", "0x" + "11" * 20, 10, hash_byte=0xAA),
            _set_event(985, 0, "owners", "0x" + "22" * 20, 7, hash_byte=0xAA),
        ]
    )
    index_mapping_values_step(
        session,
        chain_id=1,
        contract_id=cid,
        contract_address=addr,
        head_block=1000,
        log_fetcher=fetcher,
        block_hash_fetcher=chain_a_hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    assert session.query(MappingValueEvent).filter_by(contract_id=cid).count() == 2

    # Phase 2: chain head changes — block 988 hash now differs.
    chain_b_hashes = FakeBlockHashFetcher(
        {988: b"\xff" * 32, 976: b"\xff" * 32}  # different hash
    )
    fetcher2 = FakeLogFetcher([])  # no new writes
    result = index_mapping_values_step(
        session,
        chain_id=1,
        contract_id=cid,
        contract_address=addr,
        head_block=1000,
        log_fetcher=fetcher2,
        block_hash_fetcher=chain_b_hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    assert result.rewound
    # The 985 event must be gone (past the rewind point: 988 - 12 = 976).
    remaining = session.query(MappingValueEvent).filter_by(contract_id=cid).all()
    assert {e.block_number for e in remaining} == {950}


# ---------------------------------------------------------------------------
# Repo tests (latest_keys_passing_predicate)
# ---------------------------------------------------------------------------


@requires_postgres
def test_repo_filters_keys_by_eq_predicate(session_with_contract):
    """Seed three events: ``a=10``, ``b=7``, ``a=5``. Predicate
    ``eq 10`` returns empty (latest ``a`` is 5); ``eq 5`` returns
    ``[a]``; ``eq 7`` returns ``[b]``.
    """
    from db.models import MappingValueEvent
    from services.resolution.repos.mapping_value_pg import PostgresMappingValueRepo

    session, cid, addr = session_with_contract
    a = "0x" + "11" * 20
    b = "0x" + "22" * 20
    rows = [
        {
            "chain_id": 1,
            "contract_id": cid,
            "mapping_name": "owners",
            "tx_hash": (1).to_bytes(32, "big"),
            "log_index": 0,
            "key_hex": a,
            "value_hex": "0x" + f"{10:064x}",
            "block_number": 100,
            "block_hash": b"\xaa" * 32,
            "transaction_index": 0,
        },
        {
            "chain_id": 1,
            "contract_id": cid,
            "mapping_name": "owners",
            "tx_hash": (2).to_bytes(32, "big"),
            "log_index": 0,
            "key_hex": b,
            "value_hex": "0x" + f"{7:064x}",
            "block_number": 100,
            "block_hash": b"\xaa" * 32,
            "transaction_index": 0,
        },
        # later assignment — a now = 5.
        {
            "chain_id": 1,
            "contract_id": cid,
            "mapping_name": "owners",
            "tx_hash": (3).to_bytes(32, "big"),
            "log_index": 0,
            "key_hex": a,
            "value_hex": "0x" + f"{5:064x}",
            "block_number": 110,
            "block_hash": b"\xbb" * 32,
            "transaction_index": 0,
        },
    ]
    for row in rows:
        session.add(MappingValueEvent(**row))
    session.commit()

    repo = PostgresMappingValueRepo(session)
    writer_specs = [{"mapping_name": "owners", "value_position": 1}]

    # eq 10 — no key has latest 10
    out = repo.latest_keys_passing_predicate(
        chain_id=1,
        contract_address=addr,
        writer_specs=writer_specs,
        value_predicate={"op": "eq", "rhs_values": ["10"], "value_type": "uint256"},
    )
    assert out == []

    # eq 5 — only a
    out = repo.latest_keys_passing_predicate(
        chain_id=1,
        contract_address=addr,
        writer_specs=writer_specs,
        value_predicate={"op": "eq", "rhs_values": ["5"], "value_type": "uint256"},
    )
    assert out == [a.lower()]

    # eq 7 — only b
    out = repo.latest_keys_passing_predicate(
        chain_id=1,
        contract_address=addr,
        writer_specs=writer_specs,
        value_predicate={"op": "eq", "rhs_values": ["7"], "value_type": "uint256"},
    )
    assert out == [b.lower()]


@requires_postgres
def test_repo_block_constraint_uses_pre_block_state(session_with_contract):
    """``block=105`` query must only see events at or before 105 —
    the later ``a=5`` (at block 110) is invisible, so ``eq 10``
    returns ``[a]`` against a pre-overwrite snapshot.
    """
    from db.models import MappingValueEvent
    from services.resolution.repos.mapping_value_pg import PostgresMappingValueRepo

    session, cid, addr = session_with_contract
    a = "0x" + "11" * 20
    session.add_all(
        [
            MappingValueEvent(
                chain_id=1,
                contract_id=cid,
                mapping_name="owners",
                tx_hash=(1).to_bytes(32, "big"),
                log_index=0,
                key_hex=a,
                value_hex="0x" + f"{10:064x}",
                block_number=100,
                block_hash=b"\xaa" * 32,
                transaction_index=0,
            ),
            MappingValueEvent(
                chain_id=1,
                contract_id=cid,
                mapping_name="owners",
                tx_hash=(2).to_bytes(32, "big"),
                log_index=0,
                key_hex=a,
                value_hex="0x" + f"{5:064x}",
                block_number=110,
                block_hash=b"\xbb" * 32,
                transaction_index=0,
            ),
        ]
    )
    session.commit()

    repo = PostgresMappingValueRepo(session)
    out = repo.latest_keys_passing_predicate(
        chain_id=1,
        contract_address=addr,
        writer_specs=[{"mapping_name": "owners", "value_position": 1}],
        value_predicate={"op": "eq", "rhs_values": ["10"], "value_type": "uint256"},
        block=105,
    )
    assert out == [a.lower()]
