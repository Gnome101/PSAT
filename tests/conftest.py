"""Shared fixtures and helpers for proxy monitoring tests."""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.models import (
    Base,
    MonitoredContract,
    MonitoredEvent,
    Protocol,
    ProtocolSubscription,
    ProxySubscription,
    ProxyUpgradeEvent,
    TvlSnapshot,
    WatchedProxy,
)

# ---------------------------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------------------------

DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")

os.environ.setdefault("PSAT_ADMIN_KEY", "test-admin-key")


@pytest.fixture(autouse=True)
def _bypass_admin_key():
    """Override the admin-key dependency for every test so existing API tests keep working."""
    try:
        import api as _api
    except Exception:
        yield
        return
    _api.app.dependency_overrides[_api.require_admin_key] = lambda: None
    try:
        yield
    finally:
        _api.app.dependency_overrides.pop(_api.require_admin_key, None)


def _can_connect() -> bool:
    if not DATABASE_URL:
        return False
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(not _can_connect(), reason="PostgreSQL not available (set TEST_DATABASE_URL)")


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
    """PostgreSQL database session with full schema.

    Creates all tables if they don't exist, yields a session, then
    cleans up test-created rows on teardown.
    """
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)

    session = Session(engine, expire_on_commit=False)
    try:
        yield session
    finally:
        session.rollback()
        # Clean monitoring tables (order respects FK constraints)
        for model in [
            MonitoredEvent,
            MonitoredContract,
            ProtocolSubscription,
            TvlSnapshot,
            ProxyUpgradeEvent,
            ProxySubscription,
            WatchedProxy,
            Protocol,
        ]:
            session.query(model).delete()
        session.commit()
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
