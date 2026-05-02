"""Integration tests for ``PostgresAragonACLRepo``.

Hand-seeds AragonAclEvent rows + AragonAclCursor and exercises the
repo against the same Session. Mirrors the role_grants_repo test
shape — guarded by ``requires_postgres`` and skipped when no
TEST_DATABASE_URL is reachable.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_DB_URL: str = (
    os.environ.get("TEST_DATABASE_URL", os.environ.get("DATABASE_URL", "")) or ""
)


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
    proto = Protocol(name=f"aragon_acl_repo_{suffix}")
    session.add(proto)
    session.flush()
    acl = Contract(
        address="0x" + uuid.uuid4().hex[:8] + "ac" * 16,
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


def _add_event(
    session,
    *,
    acl_contract_id: int,
    app: str,
    role: bytes,
    entity: str,
    allowed: bool,
    block_number: int,
    log_index: int = 0,
):
    from db.models import AragonAclEvent

    session.add(
        AragonAclEvent(
            chain_id=1,
            acl_contract_id=acl_contract_id,
            tx_hash=block_number.to_bytes(2, "big") + log_index.to_bytes(2, "big") + b"\x00" * 28,
            log_index=log_index,
            app=app.lower(),
            role=role,
            entity=entity.lower(),
            allowed=allowed,
            block_number=block_number,
            block_hash=b"\xaa" * 32,
            transaction_index=0,
        )
    )


@requires_postgres
def test_members_for_permission_replays_grant_then_revoke(session_with_acl_contract):
    """A SetPermission(allowed=true) followed by SetPermission(
    allowed=false) for the same (entity, app, role) leaves the
    entity OUT of the member set."""
    from services.resolution.repos.aragon_acl_pg import PostgresAragonACLRepo

    session, cid, acl_addr = session_with_acl_contract
    role = b"\x01" * 32
    app = "0x" + "ee" * 20
    granted = "0x" + "11" * 20
    revoked = "0x" + "22" * 20
    _add_event(session, acl_contract_id=cid, app=app, role=role, entity=granted, allowed=True, block_number=100)
    _add_event(session, acl_contract_id=cid, app=app, role=role, entity=revoked, allowed=True, block_number=110)
    _add_event(session, acl_contract_id=cid, app=app, role=role, entity=revoked, allowed=False, block_number=120)
    session.commit()

    repo = PostgresAragonACLRepo(session)
    result = repo.members_for_permission(
        chain_id=1, acl_address=acl_addr, target_app=app, role=role
    )
    assert result.members == [granted]
    assert result.confidence == "enumerable"


@requires_postgres
def test_members_for_permission_app_role_scoped(session_with_acl_contract):
    """An ACL row for a DIFFERENT (app, role) must not bleed into
    the queried set — the index/replay are scoped per (app, role)."""
    from services.resolution.repos.aragon_acl_pg import PostgresAragonACLRepo

    session, cid, acl_addr = session_with_acl_contract
    role_a = b"\xaa" * 32
    role_b = b"\xbb" * 32
    app_x = "0x" + "11" * 20
    app_y = "0x" + "22" * 20
    member = "0x" + "33" * 20
    # Member granted role_a on app_x, but NOT on app_y nor role_b
    # on app_x.
    _add_event(session, acl_contract_id=cid, app=app_x, role=role_a, entity=member, allowed=True, block_number=100)
    _add_event(session, acl_contract_id=cid, app=app_y, role=role_a, entity=member, allowed=False, block_number=110)
    _add_event(session, acl_contract_id=cid, app=app_x, role=role_b, entity=member, allowed=False, block_number=120)
    session.commit()

    repo = PostgresAragonACLRepo(session)
    on_x_a = repo.members_for_permission(chain_id=1, acl_address=acl_addr, target_app=app_x, role=role_a)
    on_y_a = repo.members_for_permission(chain_id=1, acl_address=acl_addr, target_app=app_y, role=role_a)
    on_x_b = repo.members_for_permission(chain_id=1, acl_address=acl_addr, target_app=app_x, role=role_b)
    assert on_x_a.members == [member]
    assert on_y_a.members == []
    assert on_x_b.members == []


@requires_postgres
def test_members_for_permission_block_filter(session_with_acl_contract):
    from services.resolution.repos.aragon_acl_pg import PostgresAragonACLRepo

    session, cid, acl_addr = session_with_acl_contract
    role = b"\x02" * 32
    app = "0x" + "44" * 20
    member = "0x" + "55" * 20
    _add_event(session, acl_contract_id=cid, app=app, role=role, entity=member, allowed=True, block_number=100)
    _add_event(session, acl_contract_id=cid, app=app, role=role, entity=member, allowed=False, block_number=300)
    session.commit()

    repo = PostgresAragonACLRepo(session)
    at_200 = repo.members_for_permission(
        chain_id=1, acl_address=acl_addr, target_app=app, role=role, block=200
    )
    at_400 = repo.members_for_permission(
        chain_id=1, acl_address=acl_addr, target_app=app, role=role, block=400
    )
    assert at_200.members == [member]
    assert at_400.members == []


@requires_postgres
def test_members_for_permission_unknown_acl_returns_empty():
    """An ACL contract not present in ``contracts`` returns the
    same empty/enumerable result as RoleGrantsRepo for unknown
    contracts — the indexer would have created the row before
    landing events."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from services.resolution.repos.aragon_acl_pg import PostgresAragonACLRepo

    engine = create_engine(_DB_URL)
    session = Session(engine, expire_on_commit=False)
    try:
        repo = PostgresAragonACLRepo(session)
        result = repo.members_for_permission(
            chain_id=1,
            acl_address="0x" + "ff" * 20,
            target_app="0x" + "00" * 20,
            role=b"\x00" * 32,
        )
        assert result.members == []
        assert result.last_indexed_block is None
    finally:
        session.close()
        engine.dispose()


@requires_postgres
def test_members_for_permission_uses_cursor_for_freshness(session_with_acl_contract):
    """``last_indexed_block`` reports the indexer's cursor block,
    not the highest event block. Same contract as
    PostgresRoleGrantsRepo."""
    from db.models import AragonAclCursor

    from services.resolution.repos.aragon_acl_pg import PostgresAragonACLRepo

    session, cid, acl_addr = session_with_acl_contract
    role = b"\x03" * 32
    app = "0x" + "66" * 20
    member = "0x" + "77" * 20
    _add_event(session, acl_contract_id=cid, app=app, role=role, entity=member, allowed=True, block_number=100)
    session.add(
        AragonAclCursor(
            chain_id=1,
            acl_contract_id=cid,
            last_indexed_block=18_500_000,
            last_indexed_block_hash=b"\xee" * 32,
        )
    )
    session.commit()

    repo = PostgresAragonACLRepo(session)
    result = repo.members_for_permission(chain_id=1, acl_address=acl_addr, target_app=app, role=role)
    assert result.last_indexed_block == 18_500_000


@requires_postgres
def test_re_grant_after_revoke_returns_member(session_with_acl_contract):
    """grant -> revoke -> grant should leave the entity IN the set
    (last event wins per entity)."""
    from services.resolution.repos.aragon_acl_pg import PostgresAragonACLRepo

    session, cid, acl_addr = session_with_acl_contract
    role = b"\x04" * 32
    app = "0x" + "88" * 20
    member = "0x" + "99" * 20
    _add_event(session, acl_contract_id=cid, app=app, role=role, entity=member, allowed=True, block_number=100)
    _add_event(session, acl_contract_id=cid, app=app, role=role, entity=member, allowed=False, block_number=110)
    _add_event(session, acl_contract_id=cid, app=app, role=role, entity=member, allowed=True, block_number=120)
    session.commit()

    repo = PostgresAragonACLRepo(session)
    result = repo.members_for_permission(chain_id=1, acl_address=acl_addr, target_app=app, role=role)
    assert result.members == [member]
