"""Shared fixtures and helpers for proxy monitoring tests."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.models import ProxySubscription, ProxyUpgradeEvent, WatchedProxy

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def ADDR(n: int) -> str:
    """Generate a deterministic 0x-prefixed address from an integer."""
    return "0x" + hex(n)[2:].zfill(40)


def _topic_for(addr: str) -> str:
    """Pad a 20-byte address to a 32-byte topic."""
    return "0x" + "0" * 24 + addr[2:]


def _admin_data(old: str, new: str) -> str:
    """ABI-encode two addresses as data for AdminChanged events."""
    return "0x" + "0" * 24 + old[2:] + "0" * 24 + new[2:]


def _make_log(
    address: str,
    topic0: str,
    topic1: str | None = None,
    data: str = "0x",
    block: str = "0x64",
    tx: str = "0xaaa",
    log_index: str = "0x0",
    timestamp: str = "0x65a00000",
) -> dict:
    """Build a mock eth_getLogs result entry."""
    return {
        "address": address,
        "topics": [topic0] + ([topic1] if topic1 else []),
        "data": data,
        "blockNumber": block,
        "transactionHash": tx,
        "logIndex": log_index,
        "timeStamp": timestamp,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_session():
    """In-memory SQLite database with proxy monitoring tables.

    Uses StaticPool + check_same_thread=False so the session can be shared
    with FastAPI's TestClient (which runs endpoints in a worker thread).
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    WatchedProxy.__table__.create(engine, checkfirst=True)  # type: ignore[attr-defined]
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)  # type: ignore[attr-defined]
    ProxySubscription.__table__.create(engine, checkfirst=True)  # type: ignore[attr-defined]

    session = Session(engine, expire_on_commit=False)
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def _add_proxy(
    session: Session,
    address: str,
    chain: str = "ethereum",
    label: str | None = None,
    last_known_impl: str | None = None,
    last_scanned_block: int = 0,
    needs_polling: bool = False,
    proxy_type: str | None = None,
) -> WatchedProxy:
    """Insert a WatchedProxy row and return it."""
    proxy = WatchedProxy(
        id=uuid.uuid4(),
        proxy_address=address,
        chain=chain,
        label=label,
        proxy_type=proxy_type,
        last_known_implementation=last_known_impl,
        last_scanned_block=last_scanned_block,
        needs_polling=needs_polling,
    )
    session.add(proxy)
    session.commit()
    return proxy


def _add_subscription(
    session: Session,
    proxy: WatchedProxy,
    discord_url: str,
    label: str | None = None,
) -> ProxySubscription:
    """Insert a ProxySubscription row and return it."""
    sub = ProxySubscription(
        id=uuid.uuid4(),
        watched_proxy_id=proxy.id,
        discord_webhook_url=discord_url,
        label=label,
    )
    session.add(sub)
    session.commit()
    return sub
