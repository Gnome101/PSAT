"""Unit tests for the role_grants indexer step.

Uses fakes for LogFetcher / BlockHashFetcher so the algorithm is
covered without an RPC round-trip. The indexer's idempotency,
batch-window arithmetic, finality-depth cap, and reorg-detect/rewind
logic are all exercised against a real Postgres so the
``ON CONFLICT DO NOTHING`` upserts match production behavior.

Skipped when no Postgres is reachable."""

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
class _FakeLog:
    block_number: int
    block_hash: bytes
    tx_hash: bytes
    log_index: int
    transaction_index: int
    role: bytes
    member: str
    direction: str  # "grant" / "revoke"


class FakeLogFetcher:
    """Returns logs from a mutable list, filtered by block range.
    Mutating ``logs`` between calls simulates re-scans returning
    different results (e.g., post-reorg state)."""

    def __init__(self, logs: list[_FakeLog] | None = None):
        self.logs: list[_FakeLog] = list(logs or [])
        self.calls: list[tuple[int, int]] = []

    def fetch_logs(self, *, chain_id, contract_address, from_block, to_block):
        from workers.role_grants_indexer import FetchedLog

        self.calls.append((from_block, to_block))
        return [
            FetchedLog(
                block_number=log.block_number,
                block_hash=log.block_hash,
                tx_hash=log.tx_hash,
                log_index=log.log_index,
                transaction_index=log.transaction_index,
                role=log.role,
                member=log.member,
                direction=log.direction,
            )
            for log in self.logs
            if from_block <= log.block_number <= to_block
        ]


class FakeBlockHashFetcher:
    """Returns the hash for a block from a mutable dict; missing
    blocks return None, mimicking a node that hasn't seen them yet."""

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
        Protocol,
        RoleGrantsCursor,
        RoleGrantsEvent,
    )

    engine = create_engine(_DB_URL)
    session = Session(engine, expire_on_commit=False)
    suffix = uuid.uuid4().hex[:8]
    proto = Protocol(name=f"role_grants_idx_{suffix}")
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
        session.query(RoleGrantsEvent).filter_by(contract_id=cid).delete()
        session.query(RoleGrantsCursor).filter_by(contract_id=cid).delete()
        session.query(Contract).filter_by(id=cid).delete()
        session.query(Protocol).filter_by(id=proto_id).delete()
        session.commit()
        session.close()
        engine.dispose()


def _grant(block, log_index, role, member, *, hash_byte: int = 0xAA):
    bh = bytes([hash_byte]) * 32
    return _FakeLog(
        block_number=block,
        block_hash=bh,
        tx_hash=block.to_bytes(2, "big") + log_index.to_bytes(2, "big") + b"\x00" * 28,
        log_index=log_index,
        transaction_index=0,
        role=role,
        member=member.lower(),
        direction="grant",
    )


def _revoke(block, log_index, role, member, *, hash_byte: int = 0xAA):
    g = _grant(block, log_index, role, member, hash_byte=hash_byte)
    g.direction = "revoke"
    return g


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_postgres
def test_fresh_index_inserts_and_advances_cursor(session_with_contract):
    from workers.role_grants_indexer import index_role_grants_step

    session, cid, addr = session_with_contract
    role = b"\x11" * 32
    fetcher = FakeLogFetcher(
        [
            _grant(100, 0, role, "0x" + "11" * 20),
            _grant(150, 0, role, "0x" + "22" * 20),
        ]
    )
    hashes = FakeBlockHashFetcher({988: b"\xbb" * 32})
    result = index_role_grants_step(
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
    # Re-run with same fetcher: ON CONFLICT DO NOTHING → 0 inserts.
    fetcher.logs.clear()  # nothing new in newly-scanned range
    result2 = index_role_grants_step(
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
    assert result2.inserted == 0
    assert result2.new_cursor == 988


@requires_postgres
def test_finality_depth_caps_scan(session_with_contract):
    """Events at ``head - finality_depth + 1`` and beyond are not
    indexed — the upper bound is exclusive of unfinalized blocks."""
    from workers.role_grants_indexer import index_role_grants_step

    session, cid, addr = session_with_contract
    role = b"\x22" * 32
    fetcher = FakeLogFetcher(
        [
            _grant(100, 0, role, "0x" + "33" * 20),
            # block 989 is finality_depth=12 below head=1000, so 989 is
            # finalized; 995 is past the cap and must not be ingested.
            _grant(995, 0, role, "0x" + "44" * 20),
        ]
    )
    hashes = FakeBlockHashFetcher()
    result = index_role_grants_step(
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
    assert result.inserted == 1
    assert result.new_cursor == 988

    # The fetcher's range request must have stopped at 988, not
    # asked for 1000-bounded data.
    assert fetcher.calls
    assert fetcher.calls[-1][1] == 988


@requires_postgres
def test_reorg_rewind_deletes_window_and_re_indexes(session_with_contract):
    """Cursor's ``last_indexed_block_hash`` mismatches the live
    chain → indexer rewinds ``finality_depth`` blocks, deletes
    events in the window, and re-fetches. Idempotent across runs."""
    from db.models import RoleGrantsEvent
    from workers.role_grants_indexer import index_role_grants_step

    session, cid, addr = session_with_contract
    role = b"\x33" * 32

    # Initial good state: 3 grants. Two are well-finalized (100,
    # 150) and one is INSIDE the rewind window (block 285). Head=300,
    # finality=12 → indexer takes through block 288. After a reorg
    # at the cursor, rewind=288-12=276 → events with block > 276
    # are wiped (block 285 goes), block ≤ 276 is kept (100, 150).
    initial_logs = [
        _grant(100, 0, role, "0x" + "aa" * 20, hash_byte=0xAA),
        _grant(150, 0, role, "0x" + "bb" * 20, hash_byte=0xAA),
        _grant(285, 0, role, "0x" + "cc" * 20, hash_byte=0xAA),
    ]
    fetcher = FakeLogFetcher(list(initial_logs))
    # block_hash at 288 (final cursor) — record so reorg detection
    # has a baseline.
    hashes = FakeBlockHashFetcher({288: b"\xa0" * 32})
    result = index_role_grants_step(
        session,
        chain_id=1,
        contract_id=cid,
        contract_address=addr,
        head_block=300,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    assert result.inserted == 3
    assert result.new_cursor == 288
    pre_count = session.query(RoleGrantsEvent).filter_by(contract_id=cid).count()
    assert pre_count == 3

    # Reorg: hash at 288 changed AND the chain replaced block 285's
    # grant with a different member at block 287. The indexer should
    # rewind to 288-12=276, delete events with block > 276 (block
    # 285 goes), and re-fetch the window — picking up the new grant.
    hashes.hashes[288] = b"\xb0" * 32  # different from the cursor's recorded hash
    fetcher.logs = [
        _grant(100, 0, role, "0x" + "aa" * 20, hash_byte=0xAA),
        _grant(150, 0, role, "0x" + "bb" * 20, hash_byte=0xAA),
        # The post-reorg replacement grant — different member at a
        # block within the rewound window.
        _grant(287, 0, role, "0x" + "dd" * 20, hash_byte=0xC0),
    ]
    result2 = index_role_grants_step(
        session,
        chain_id=1,
        contract_id=cid,
        contract_address=addr,
        head_block=300,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        use_advisory_lock=False,
    )
    session.commit()
    assert result2.rewound is True
    # The block-200 event is gone; 100/150 survive (within the
    # rewound-to point); the post-reorg block-280 grant is added.
    members = {ev.member for ev in session.query(RoleGrantsEvent).filter_by(contract_id=cid).all()}
    assert "0x" + "aa" * 20 in members
    assert "0x" + "bb" * 20 in members
    assert "0x" + "cc" * 20 not in members  # rewound away
    assert "0x" + "dd" * 20 in members  # post-reorg insert


@requires_postgres
def test_batch_size_chunks_large_range(session_with_contract):
    """A range larger than ``batch_size`` is fetched in multiple
    chunks. Verifies the loop arithmetic doesn't double-fetch the
    boundary block."""
    from workers.role_grants_indexer import index_role_grants_step

    session, cid, addr = session_with_contract
    fetcher = FakeLogFetcher([])
    hashes = FakeBlockHashFetcher({988: b"\xee" * 32})

    index_role_grants_step(
        session,
        chain_id=1,
        contract_id=cid,
        contract_address=addr,
        head_block=1000,
        log_fetcher=fetcher,
        block_hash_fetcher=hashes,
        finality_depth=12,
        batch_size=300,
        use_advisory_lock=False,
    )
    session.commit()
    # Range [1, 988], batch=300 → 4 chunks: [1-300], [301-600],
    # [601-900], [901-988].
    assert fetcher.calls == [(1, 300), (301, 600), (601, 900), (901, 988)]


@requires_postgres
def test_already_at_tip_no_op(session_with_contract):
    """When cursor is already at finalized_head, the indexer
    skips the fetch and just refreshes the cursor's hash + run-at."""
    from workers.role_grants_indexer import index_role_grants_step

    session, cid, addr = session_with_contract
    fetcher = FakeLogFetcher([])
    hashes = FakeBlockHashFetcher({988: b"\xff" * 32})
    # Seed cursor at 988.
    index_role_grants_step(
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
    fetcher.calls.clear()
    result = index_role_grants_step(
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
    assert result.inserted == 0
    assert result.new_cursor == 988
    # No fetch round-trips when already at tip.
    assert fetcher.calls == []


@requires_postgres
def test_cursor_records_hash_at_finalized_head_not_last_event(session_with_contract):
    """Regression for the codex-flagged bug: when the scan range
    contains events at earlier blocks but no event AT the cursor's
    finalized_head, the cursor must still record the hash AT
    finalized_head (not the last event's hash). Otherwise the next
    pass falsely detects a reorg when comparing against the live
    chain hash at the same height."""
    from db.models import RoleGrantsCursor
    from workers.role_grants_indexer import index_role_grants_step

    session, cid, addr = session_with_contract
    role = b"\x55" * 32
    # Event at block 100; finalized cursor will be at 988. The two
    # hashes differ — the test fails if the cursor records the
    # event's hash instead of the cursor's hash.
    fetcher = FakeLogFetcher([_grant(100, 0, role, "0x" + "55" * 20, hash_byte=0xCC)])
    head_hash = b"\xff" * 32
    hashes = FakeBlockHashFetcher({988: head_hash})

    index_role_grants_step(
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

    cursor_row = session.query(RoleGrantsCursor).filter_by(contract_id=cid).one()
    assert cursor_row.last_indexed_block == 988
    # Critical: hash matches the height-988 hash, not the event's
    # hash at block 100.
    assert cursor_row.last_indexed_block_hash == head_hash


@requires_postgres
def test_event_direction_topic_decoding():
    from workers.role_grants_indexer import (
        ROLE_GRANTED_TOPIC0,
        ROLE_REVOKED_TOPIC0,
        event_direction,
    )

    assert event_direction(ROLE_GRANTED_TOPIC0) == "grant"
    assert event_direction(ROLE_REVOKED_TOPIC0) == "revoke"
    assert event_direction(b"\x00" * 32) is None
