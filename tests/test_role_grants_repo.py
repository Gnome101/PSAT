"""End-to-end tests for ``PostgresRoleGrantsRepo``.

Each test seeds a fresh Contract + a sequence of RoleGrantsEvent
rows, then exercises the repo against the same Session that wrote
the data. Tests are guarded by ``requires_postgres`` and skipped on
machines without ``TEST_DATABASE_URL``."""

from __future__ import annotations

import os
import sys
import uuid
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


@pytest.fixture
def session_with_contract():
    """Yield a (Session, contract_id) pair backed by a fresh
    Protocol+Contract row. Cleans up role_grants_events / cursor /
    contract / protocol on exit so tests are idempotent."""
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
    proto = Protocol(name=f"role_grants_repo_test_{suffix}")
    session.add(proto)
    session.flush()
    contract = Contract(
        address="0x" + "ab" * 20,
        chain="ethereum",
        protocol_id=proto.id,
    )
    session.add(contract)
    session.flush()
    session.commit()

    contract_id = contract.id
    proto_id = proto.id
    contract_address = contract.address

    try:
        yield session, contract_id, contract_address
    finally:
        session.query(RoleGrantsEvent).filter_by(contract_id=contract_id).delete()
        session.query(RoleGrantsCursor).filter_by(contract_id=contract_id).delete()
        session.query(Contract).filter_by(id=contract_id).delete()
        session.query(Protocol).filter_by(id=proto_id).delete()
        session.commit()
        session.close()
        engine.dispose()


def _add_event(
    session,
    *,
    contract_id: int,
    role: bytes,
    member: str,
    direction: str,
    block_number: int,
    log_index: int,
    tx_hash: bytes | None = None,
):
    from db.models import RoleGrantsEvent

    session.add(
        RoleGrantsEvent(
            chain_id=1,
            contract_id=contract_id,
            tx_hash=tx_hash or (block_number.to_bytes(2, "big") + b"\x00" * 30),
            log_index=log_index,
            role=role,
            member=member.lower(),
            direction=direction,
            block_number=block_number,
            block_hash=b"\xaa" * 32,
            transaction_index=0,
        )
    )


@requires_postgres
def test_members_for_role_replays_grant_then_revoke(session_with_contract):
    from services.resolution.repos.role_grants_pg import PostgresRoleGrantsRepo

    session, contract_id, addr = session_with_contract
    role = b"\x01" * 32
    addr_a = "0x" + "11" * 20
    addr_b = "0x" + "22" * 20
    _add_event(
        session, contract_id=contract_id, role=role, member=addr_a, direction="grant", block_number=100, log_index=0
    )
    _add_event(
        session, contract_id=contract_id, role=role, member=addr_b, direction="grant", block_number=200, log_index=0
    )
    _add_event(
        session, contract_id=contract_id, role=role, member=addr_a, direction="revoke", block_number=300, log_index=0
    )
    session.commit()

    repo = PostgresRoleGrantsRepo(session)
    result = repo.members_for_role(chain_id=1, contract_address=addr, role=role)
    assert sorted(result.members) == sorted([addr_b.lower()])
    assert result.confidence == "enumerable"


@requires_postgres
def test_members_for_role_block_filter(session_with_contract):
    """``block`` parameter excludes events past that block — used
    by point-in-time queries."""
    from services.resolution.repos.role_grants_pg import PostgresRoleGrantsRepo

    session, contract_id, addr = session_with_contract
    role = b"\x02" * 32
    member = "0x" + "33" * 20
    _add_event(
        session, contract_id=contract_id, role=role, member=member, direction="grant", block_number=100, log_index=0
    )
    _add_event(
        session, contract_id=contract_id, role=role, member=member, direction="revoke", block_number=300, log_index=0
    )
    session.commit()

    repo = PostgresRoleGrantsRepo(session)
    # At block 200, only the grant is visible — member is in the set.
    at_200 = repo.members_for_role(chain_id=1, contract_address=addr, role=role, block=200)
    assert sorted(at_200.members) == [member.lower()]
    # At block 400, both events visible — member is no longer in set.
    at_400 = repo.members_for_role(chain_id=1, contract_address=addr, role=role, block=400)
    assert at_400.members == []


@requires_postgres
def test_members_for_role_unknown_contract_returns_empty():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from services.resolution.repos.role_grants_pg import PostgresRoleGrantsRepo

    engine = create_engine(_DB_URL)
    session = Session(engine, expire_on_commit=False)
    try:
        repo = PostgresRoleGrantsRepo(session)
        result = repo.members_for_role(
            chain_id=1,
            contract_address="0x" + "ff" * 20,
            role=b"\x00" * 32,
        )
        assert result.members == []
        assert result.confidence == "enumerable"
        assert result.last_indexed_block is None
    finally:
        session.close()
        engine.dispose()


@requires_postgres
def test_has_member_returns_yes_no_unknown(session_with_contract):
    from services.resolution.adapters import Trit
    from services.resolution.repos.role_grants_pg import PostgresRoleGrantsRepo

    session, contract_id, addr = session_with_contract
    role = b"\x03" * 32
    granted = "0x" + "44" * 20
    revoked = "0x" + "55" * 20
    never = "0x" + "66" * 20
    _add_event(
        session, contract_id=contract_id, role=role, member=granted, direction="grant", block_number=100, log_index=0
    )
    _add_event(
        session, contract_id=contract_id, role=role, member=revoked, direction="grant", block_number=110, log_index=0
    )
    _add_event(
        session, contract_id=contract_id, role=role, member=revoked, direction="revoke", block_number=120, log_index=0
    )
    session.commit()

    repo = PostgresRoleGrantsRepo(session)
    assert repo.has_member(chain_id=1, contract_address=addr, role=role, member=granted) == Trit.YES
    assert repo.has_member(chain_id=1, contract_address=addr, role=role, member=revoked) == Trit.NO
    assert repo.has_member(chain_id=1, contract_address=addr, role=role, member=never) == Trit.NO
    # Unknown contract → UNKNOWN (the AC adapter then falls back to
    # external-check-only).
    assert (
        repo.has_member(
            chain_id=1,
            contract_address="0x" + "00" * 20,
            role=role,
            member=granted,
        )
        == Trit.UNKNOWN
    )


@requires_postgres
def test_list_observed_roles_distinct(session_with_contract):
    from services.resolution.repos.role_grants_pg import PostgresRoleGrantsRepo

    session, contract_id, addr = session_with_contract
    role_a = b"\xa0" * 32
    role_b = b"\xb0" * 32
    member = "0x" + "77" * 20
    _add_event(
        session, contract_id=contract_id, role=role_a, member=member, direction="grant", block_number=100, log_index=0
    )
    _add_event(
        session, contract_id=contract_id, role=role_a, member=member, direction="revoke", block_number=110, log_index=0
    )
    _add_event(
        session, contract_id=contract_id, role=role_b, member=member, direction="grant", block_number=120, log_index=0
    )
    session.commit()

    repo = PostgresRoleGrantsRepo(session)
    observed = repo.list_observed_roles(chain_id=1, contract_address=addr)
    assert sorted(observed) == sorted([role_a, role_b])


@requires_postgres
def test_get_role_admin_returns_none_pending_indexer_extension(session_with_contract):
    """RoleAdminChanged events aren't indexed yet; the repo
    returns None and the AC adapter falls back to default admin."""
    from services.resolution.repos.role_grants_pg import PostgresRoleGrantsRepo

    session, _contract_id, addr = session_with_contract
    repo = PostgresRoleGrantsRepo(session)
    assert repo.get_role_admin(chain_id=1, contract_address=addr, role=b"\x01" * 32) is None


@requires_postgres
def test_members_for_role_uses_cursor_for_last_indexed_block(session_with_contract):
    """Repo reports ``last_indexed_block`` from the cursor row, not
    the highest event block. This matters when the indexer has
    advanced past the last event (no events in recent blocks) —
    capabilities should still report the cursor as the freshness
    marker."""
    from db.models import RoleGrantsCursor
    from services.resolution.repos.role_grants_pg import PostgresRoleGrantsRepo

    session, contract_id, addr = session_with_contract
    role = b"\xcc" * 32
    member = "0x" + "88" * 20
    _add_event(
        session, contract_id=contract_id, role=role, member=member, direction="grant", block_number=100, log_index=0
    )
    session.add(
        RoleGrantsCursor(
            chain_id=1,
            contract_id=contract_id,
            last_indexed_block=18_500_000,
            last_indexed_block_hash=b"\xee" * 32,
        )
    )
    session.commit()

    repo = PostgresRoleGrantsRepo(session)
    result = repo.members_for_role(chain_id=1, contract_address=addr, role=role)
    assert result.last_indexed_block == 18_500_000
