"""Tests for ``scan_enrolled_acl_contracts`` — the Aragon ACL
indexer's scheduling layer. Mirrors test_role_grants_scan_loop."""

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
    entity: str
    app: str
    role: bytes
    allowed: bool


class FakeLogFetcher:
    def __init__(self, logs: list[_FakeLog] | None = None):
        self.logs = list(logs or [])
        self.calls: list[tuple[str, int, int]] = []

    def fetch_logs(self, *, chain_id, contract_address, from_block, to_block):
        from workers.aragon_acl_indexer import FetchedAragonLog

        self.calls.append((contract_address, from_block, to_block))
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


class ExplodingLogFetcher:
    def __init__(self):
        self.calls = 0

    def fetch_logs(self, **_):
        self.calls += 1
        raise RuntimeError("simulated RPC failure")


class FakeBlockHashFetcher:
    def __init__(self, hashes=None):
        self.hashes = dict(hashes or {})

    def block_hash(self, *, chain_id, block_number):
        return self.hashes.get(block_number)


class FakeHeadBlockFetcher:
    def __init__(self, heads=None):
        self.heads = dict(heads or {})
        self.calls = 0

    def head_block(self, *, chain_id):
        self.calls += 1
        return self.heads.get(chain_id, 0)


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def session_with_two_acl_contracts():
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
    proto = Protocol(name=f"aragon_loop_{suffix}")
    session.add(proto)
    session.flush()
    a = Contract(address="0x" + uuid.uuid4().hex[:8] + "11" * 16, chain="ethereum", protocol_id=proto.id)
    b = Contract(address="0x" + uuid.uuid4().hex[:8] + "22" * 16, chain="ethereum", protocol_id=proto.id)
    session.add_all([a, b])
    session.flush()
    session.commit()

    a_id, b_id = a.id, b.id
    a_addr, b_addr = a.address, b.address
    proto_id = proto.id
    try:
        yield session, (a_id, a_addr), (b_id, b_addr)
    finally:
        for cid in (a_id, b_id):
            session.query(AragonAclEvent).filter_by(acl_contract_id=cid).delete()
            session.query(AragonAclCursor).filter_by(acl_contract_id=cid).delete()
            session.query(Contract).filter_by(id=cid).delete()
        session.query(Protocol).filter_by(id=proto_id).delete()
        session.commit()
        session.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_postgres
def test_scan_iterates_all_acl_cursors_and_caches_head(session_with_two_acl_contracts):
    from db.models import AragonAclEvent
    from workers.aragon_acl_indexer import (
        enroll_acl_contract,
        scan_enrolled_acl_contracts,
    )

    session, (a_id, a_addr), (b_id, b_addr) = session_with_two_acl_contracts
    enroll_acl_contract(session, chain_id=1, acl_contract_id=a_id)
    enroll_acl_contract(session, chain_id=1, acl_contract_id=b_id)
    session.commit()

    role = b"\x11" * 32
    a_logs = FakeLogFetcher(
        [
            _FakeLog(
                block_number=100,
                block_hash=b"\xaa" * 32,
                tx_hash=b"\x01" * 32,
                log_index=0,
                transaction_index=0,
                entity="0x" + "11" * 20,
                app="0x" + "ee" * 20,
                role=role,
                allowed=True,
            ),
        ]
    )
    b_logs = FakeLogFetcher(
        [
            _FakeLog(
                block_number=200,
                block_hash=b"\xbb" * 32,
                tx_hash=b"\x02" * 32,
                log_index=0,
                transaction_index=0,
                entity="0x" + "22" * 20,
                app="0x" + "ff" * 20,
                role=role,
                allowed=True,
            ),
        ]
    )

    class Dispatcher:
        def fetch_logs(self, *, chain_id, contract_address, from_block, to_block):
            if contract_address == a_addr:
                return a_logs.fetch_logs(
                    chain_id=chain_id,
                    contract_address=contract_address,
                    from_block=from_block,
                    to_block=to_block,
                )
            if contract_address == b_addr:
                return b_logs.fetch_logs(
                    chain_id=chain_id,
                    contract_address=contract_address,
                    from_block=from_block,
                    to_block=to_block,
                )
            return []

    head = FakeHeadBlockFetcher({1: 1000})
    block_hashes = FakeBlockHashFetcher({988: b"\xee" * 32})

    results = scan_enrolled_acl_contracts(
        session,
        log_fetcher_for_chain={1: Dispatcher()},
        block_hash_fetcher_for_chain={1: block_hashes},
        head_block_fetcher=head,
        finality_for_chain={1: 12},
        use_advisory_lock=False,
    )

    assert len(results) == 2
    assert all(r.inserted == 1 for r in results)
    # Head fetched once per chain, not per ACL contract.
    assert head.calls == 1
    a_count = session.query(AragonAclEvent).filter_by(acl_contract_id=a_id).count()
    b_count = session.query(AragonAclEvent).filter_by(acl_contract_id=b_id).count()
    assert a_count == 1
    assert b_count == 1


@requires_postgres
def test_scan_isolates_failures(session_with_two_acl_contracts):
    from db.models import AragonAclEvent
    from workers.aragon_acl_indexer import (
        enroll_acl_contract,
        scan_enrolled_acl_contracts,
    )

    session, (a_id, a_addr), (b_id, b_addr) = session_with_two_acl_contracts
    enroll_acl_contract(session, chain_id=1, acl_contract_id=a_id)
    enroll_acl_contract(session, chain_id=1, acl_contract_id=b_id)
    session.commit()

    role = b"\x33" * 32
    good_logs = FakeLogFetcher(
        [
            _FakeLog(
                block_number=100,
                block_hash=b"\xcc" * 32,
                tx_hash=b"\x03" * 32,
                log_index=0,
                transaction_index=0,
                entity="0x" + "33" * 20,
                app="0x" + "ee" * 20,
                role=role,
                allowed=True,
            ),
        ]
    )
    bad = ExplodingLogFetcher()

    class Dispatcher:
        def fetch_logs(self, *, chain_id, contract_address, from_block, to_block):
            if contract_address == a_addr:
                return bad.fetch_logs()  # raises
            return good_logs.fetch_logs(
                chain_id=chain_id,
                contract_address=contract_address,
                from_block=from_block,
                to_block=to_block,
            )

    results = scan_enrolled_acl_contracts(
        session,
        log_fetcher_for_chain={1: Dispatcher()},
        block_hash_fetcher_for_chain={1: FakeBlockHashFetcher({988: b"\xee" * 32})},
        head_block_fetcher=FakeHeadBlockFetcher({1: 1000}),
        finality_for_chain={1: 12},
        use_advisory_lock=False,
    )

    assert len(results) == 1
    a_count = session.query(AragonAclEvent).filter_by(acl_contract_id=a_id).count()
    b_count = session.query(AragonAclEvent).filter_by(acl_contract_id=b_id).count()
    assert a_count == 0  # rolled back
    assert b_count == 1


@requires_postgres
def test_scan_skips_chains_without_fetchers(session_with_two_acl_contracts):
    from workers.aragon_acl_indexer import (
        enroll_acl_contract,
        scan_enrolled_acl_contracts,
    )

    session, (a_id, _), _ = session_with_two_acl_contracts
    enroll_acl_contract(session, chain_id=999, acl_contract_id=a_id)
    session.commit()

    head = FakeHeadBlockFetcher()
    results = scan_enrolled_acl_contracts(
        session,
        log_fetcher_for_chain={1: FakeLogFetcher()},
        block_hash_fetcher_for_chain={1: FakeBlockHashFetcher()},
        head_block_fetcher=head,
        finality_for_chain={1: 12},
        use_advisory_lock=False,
    )
    assert results == []
    assert head.calls == 0


@requires_postgres
def test_scan_skips_chain_with_zero_head(session_with_two_acl_contracts):
    from workers.aragon_acl_indexer import (
        enroll_acl_contract,
        scan_enrolled_acl_contracts,
    )

    session, (a_id, _), _ = session_with_two_acl_contracts
    enroll_acl_contract(session, chain_id=1, acl_contract_id=a_id)
    session.commit()

    fetcher = FakeLogFetcher()
    results = scan_enrolled_acl_contracts(
        session,
        log_fetcher_for_chain={1: fetcher},
        block_hash_fetcher_for_chain={1: FakeBlockHashFetcher()},
        head_block_fetcher=FakeHeadBlockFetcher({1: 0}),
        finality_for_chain={1: 12},
        use_advisory_lock=False,
    )
    assert results == []
    assert fetcher.calls == []
