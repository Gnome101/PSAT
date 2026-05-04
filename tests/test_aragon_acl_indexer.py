"""Unit tests for ``index_aragon_acl_step``.

Mirrors the role_grants indexer test set: fake LogFetcher +
BlockHashFetcher, real Postgres for the schema's ON CONFLICT
behavior. Pins the same set of properties the role_grants
indexer pins (idempotent re-run, finality cap, batch chunking,
no-op-at-tip, reorg detect+rewind, cursor-hash-at-cursor-block).
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


@dataclass
class _FakeLog:
    block_number: int
    block_hash: bytes
    tx_hash: bytes
    log_index: int
    transaction_index: int
    entity: str
    app: str
    role: bytes
    allowed: bool


class FakeLogFetcher:
    def __init__(self, logs: list[_FakeLog] | None = None):
        self.logs = list(logs or [])
        self.calls: list[tuple[int, int]] = []

    def fetch_logs(self, *, chain_id, contract_address, from_block, to_block):
        from workers.aragon_acl_indexer import FetchedAragonLog

        self.calls.append((from_block, to_block))
        return [
            FetchedAragonLog(
                block_number=log.block_number,
                block_hash=log.block_hash,
                tx_hash=log.tx_hash,
                log_index=log.log_index,
                transaction_index=log.transaction_index,
                entity=log.entity,
                app=log.app,
                role=log.role,
                allowed=log.allowed,
            )
            for log in self.logs
            if from_block <= log.block_number <= to_block
        ]


class FakeBlockHashFetcher:
    def __init__(self, hashes=None):
        self.hashes = dict(hashes or {})

    def block_hash(self, *, chain_id, block_number):
        return self.hashes.get(block_number)


@pytest.fixture
def session_with_acl_contract():
    if not _can_connect():
        pytest.skip("PostgreSQL not available")
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from db.models import (
        AragonAclCursor,
        AragonAclEvent,
        Contract,
        Protocol,
    )

    engine = create_engine(_DB_URL)
    session = Session(engine, expire_on_commit=False)
    suffix = uuid.uuid4().hex[:8]
    proto = Protocol(name=f"aragon_idx_{suffix}")
    session.add(proto)
    session.flush()
    acl = Contract(
        address="0x" + suffix + "ac" * 16,
        chain="ethereum",
        protocol_id=proto.id,
    )
    session.add(acl)
    session.flush()
    session.commit()

    cid = acl.id
    addr = acl.address
    proto_id = proto.id
    try:
        yield session, cid, addr
    finally:
        session.query(AragonAclEvent).filter_by(acl_contract_id=cid).delete()
        session.query(AragonAclCursor).filter_by(acl_contract_id=cid).delete()
        session.query(Contract).filter_by(id=cid).delete()
        session.query(Protocol).filter_by(id=proto_id).delete()
        session.commit()
        session.close()
        engine.dispose()


def _grant(block, log_index, role, entity, app, *, hash_byte: int = 0xAA):
    return _FakeLog(
        block_number=block,
        block_hash=bytes([hash_byte]) * 32,
        tx_hash=block.to_bytes(2, "big") + log_index.to_bytes(2, "big") + b"\x00" * 28,
        log_index=log_index,
        transaction_index=0,
        entity=entity.lower(),
        app=app.lower(),
        role=role,
        allowed=True,
    )


def _revoke(block, log_index, role, entity, app, *, hash_byte: int = 0xAA):
    g = _grant(block, log_index, role, entity, app, hash_byte=hash_byte)
    g.allowed = False
    return g


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_postgres
def test_fresh_index_inserts_grant_and_revoke(session_with_acl_contract):
    from workers.aragon_acl_indexer import index_aragon_acl_step

    session, cid, addr = session_with_acl_contract
    role = b"\x11" * 32
    app = "0x" + "ee" * 20
    fetcher = FakeLogFetcher(
        [
            _grant(100, 0, role, "0x" + "11" * 20, app),
            _revoke(150, 0, role, "0x" + "11" * 20, app),
            _grant(200, 0, role, "0x" + "22" * 20, app),
        ]
    )
    hashes = FakeBlockHashFetcher({988: b"\xee" * 32})
    result = index_aragon_acl_step(
        session,
        chain_id=1,
        acl_contract_id=cid,
        acl_address=addr,
        head_block=1000,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    assert result.inserted == 3
    assert result.new_cursor == 988

    # Idempotent re-run with no new logs.
    fetcher.logs = []
    result2 = index_aragon_acl_step(
        session,
        chain_id=1,
        acl_contract_id=cid,
        acl_address=addr,
        head_block=1000,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    assert result2.inserted == 0


@requires_postgres
def test_finality_depth_caps_scan(session_with_acl_contract):
    from workers.aragon_acl_indexer import index_aragon_acl_step

    session, cid, addr = session_with_acl_contract
    role = b"\x22" * 32
    app = "0x" + "44" * 20
    fetcher = FakeLogFetcher(
        [
            _grant(100, 0, role, "0x" + "33" * 20, app),
            # Past head - finality_depth (1000-12=988); must not be ingested.
            _grant(995, 0, role, "0x" + "55" * 20, app),
        ]
    )
    hashes = FakeBlockHashFetcher()
    result = index_aragon_acl_step(
        session,
        chain_id=1,
        acl_contract_id=cid,
        acl_address=addr,
        head_block=1000,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    assert result.inserted == 1
    assert result.new_cursor == 988
    assert fetcher.calls[-1][1] == 988


@requires_postgres
def test_reorg_rewind_deletes_window_and_re_indexes(session_with_acl_contract):
    """Same shape as the role_grants reorg test: reorg invalidates
    only events past ``last_indexed_block - finality_depth``."""
    from db.models import AragonAclEvent
    from workers.aragon_acl_indexer import index_aragon_acl_step

    session, cid, addr = session_with_acl_contract
    role = b"\x33" * 32
    app = "0x" + "66" * 20

    fetcher = FakeLogFetcher(
        [
            _grant(100, 0, role, "0x" + "aa" * 20, app, hash_byte=0xAA),
            _grant(150, 0, role, "0x" + "bb" * 20, app, hash_byte=0xAA),
            _grant(285, 0, role, "0x" + "cc" * 20, app, hash_byte=0xAA),  # within rewind window
        ]
    )
    hashes = FakeBlockHashFetcher({288: b"\xa0" * 32})
    index_aragon_acl_step(
        session,
        chain_id=1,
        acl_contract_id=cid,
        acl_address=addr,
        head_block=300,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    assert session.query(AragonAclEvent).filter_by(acl_contract_id=cid).count() == 3

    # Reorg: hash at 288 differs; rewind to 276, the block-285
    # event must be wiped and re-fetched (post-reorg state has a
    # different entity at block 287).
    hashes.hashes[288] = b"\xb0" * 32
    fetcher.logs = [
        _grant(100, 0, role, "0x" + "aa" * 20, app, hash_byte=0xAA),
        _grant(150, 0, role, "0x" + "bb" * 20, app, hash_byte=0xAA),
        _grant(287, 0, role, "0x" + "dd" * 20, app, hash_byte=0xC0),
    ]
    result = index_aragon_acl_step(
        session,
        chain_id=1,
        acl_contract_id=cid,
        acl_address=addr,
        head_block=300,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    assert result.rewound is True
    entities = {ev.entity for ev in session.query(AragonAclEvent).filter_by(acl_contract_id=cid).all()}
    assert "0x" + "aa" * 20 in entities
    assert "0x" + "bb" * 20 in entities
    assert "0x" + "cc" * 20 not in entities
    assert "0x" + "dd" * 20 in entities


@requires_postgres
def test_batch_size_chunks_large_range(session_with_acl_contract):
    from workers.aragon_acl_indexer import index_aragon_acl_step

    session, cid, addr = session_with_acl_contract
    fetcher = FakeLogFetcher([])
    hashes = FakeBlockHashFetcher({988: b"\xee" * 32})
    index_aragon_acl_step(
        session,
        chain_id=1,
        acl_contract_id=cid,
        acl_address=addr,
        head_block=1000,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        batch_size=300,
        use_advisory_lock=False,
    )
    session.commit()
    assert fetcher.calls == [(1, 300), (301, 600), (601, 900), (901, 988)]


@requires_postgres
def test_already_at_tip_no_op(session_with_acl_contract):
    from workers.aragon_acl_indexer import index_aragon_acl_step

    session, cid, addr = session_with_acl_contract
    fetcher = FakeLogFetcher([])
    hashes = FakeBlockHashFetcher({988: b"\xff" * 32})
    index_aragon_acl_step(
        session,
        chain_id=1,
        acl_contract_id=cid,
        acl_address=addr,
        head_block=1000,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    fetcher.calls.clear()
    result = index_aragon_acl_step(
        session,
        chain_id=1,
        acl_contract_id=cid,
        acl_address=addr,
        head_block=1000,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    assert result.inserted == 0
    assert result.new_cursor == 988
    assert fetcher.calls == []


@requires_postgres
def test_cursor_records_hash_at_finalized_head(session_with_acl_contract):
    """Same regression pin as role_grants: cursor block_hash is
    the hash AT the cursor block, not the last event's block hash."""
    from db.models import AragonAclCursor
    from workers.aragon_acl_indexer import index_aragon_acl_step

    session, cid, addr = session_with_acl_contract
    role = b"\x55" * 32
    app = "0x" + "77" * 20
    fetcher = FakeLogFetcher([_grant(100, 0, role, "0x" + "88" * 20, app, hash_byte=0xCC)])
    head_hash = b"\xff" * 32
    hashes = FakeBlockHashFetcher({988: head_hash})

    index_aragon_acl_step(
        session,
        chain_id=1,
        acl_contract_id=cid,
        acl_address=addr,
        head_block=1000,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    cursor_row = session.query(AragonAclCursor).filter_by(acl_contract_id=cid).one()
    assert cursor_row.last_indexed_block == 988
    assert cursor_row.last_indexed_block_hash == head_hash


@requires_postgres
def test_enroll_acl_contract_idempotent(session_with_acl_contract):
    from db.models import AragonAclCursor
    from workers.aragon_acl_indexer import enroll_acl_contract

    session, cid, _ = session_with_acl_contract
    enroll_acl_contract(session, chain_id=1, acl_contract_id=cid)
    session.commit()
    enroll_acl_contract(session, chain_id=1, acl_contract_id=cid)
    session.commit()
    rows = session.query(AragonAclCursor).filter_by(chain_id=1, acl_contract_id=cid).all()
    assert len(rows) == 1
    assert rows[0].last_indexed_block == 0
