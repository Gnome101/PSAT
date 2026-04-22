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

_STORAGE_ENV_KEYS = (
    "ARTIFACT_STORAGE_ENDPOINT",
    "ARTIFACT_STORAGE_BUCKET",
    "ARTIFACT_STORAGE_ACCESS_KEY",
    "ARTIFACT_STORAGE_SECRET_KEY",
    "ARTIFACT_STORAGE_PREFIX",
)

from db.models import (  # noqa: E402
    AuditContractCoverage,
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

# ---------------------------------------------------------------------------
# Object storage (minio in dev, Tigris in prod) — opt-in via storage_bucket
# ---------------------------------------------------------------------------

TEST_STORAGE_ENDPOINT = os.environ.get("TEST_ARTIFACT_STORAGE_ENDPOINT", "")
TEST_STORAGE_BUCKET = os.environ.get("TEST_ARTIFACT_STORAGE_BUCKET", "")
TEST_STORAGE_ACCESS_KEY = os.environ.get("TEST_ARTIFACT_STORAGE_ACCESS_KEY", "")
TEST_STORAGE_SECRET_KEY = os.environ.get("TEST_ARTIFACT_STORAGE_SECRET_KEY", "")


def _can_connect_storage() -> bool:
    if not all([TEST_STORAGE_ENDPOINT, TEST_STORAGE_BUCKET, TEST_STORAGE_ACCESS_KEY, TEST_STORAGE_SECRET_KEY]):
        return False
    try:
        from db.storage import StorageClient

        client = StorageClient(
            TEST_STORAGE_ENDPOINT,
            TEST_STORAGE_BUCKET,
            TEST_STORAGE_ACCESS_KEY,
            TEST_STORAGE_SECRET_KEY,
        )
        client.ensure_bucket()
        return True
    except Exception:
        return False


requires_storage = pytest.mark.skipif(
    not _can_connect_storage(),
    reason="Object storage not available (set TEST_ARTIFACT_STORAGE_* and run minio)",
)


def _purge_bucket(client) -> None:
    paginator = client._client.get_paginator("list_objects_v2")
    keys: list[dict[str, str]] = []
    for page in paginator.paginate(Bucket=client.bucket):
        for obj in page.get("Contents", []):
            keys.append({"Key": obj["Key"]})
    for i in range(0, len(keys), 1000):
        if keys[i : i + 1000]:
            client._client.delete_objects(Bucket=client.bucket, Delete={"Objects": keys[i : i + 1000]})


@pytest.fixture()
def storage_bucket(monkeypatch):
    """Wire ARTIFACT_STORAGE_* to the test bucket and clean it on teardown."""
    if not _can_connect_storage():
        pytest.skip("TEST_ARTIFACT_STORAGE_* not set or minio unreachable")

    monkeypatch.setenv("ARTIFACT_STORAGE_ENDPOINT", TEST_STORAGE_ENDPOINT)
    monkeypatch.setenv("ARTIFACT_STORAGE_BUCKET", TEST_STORAGE_BUCKET)
    monkeypatch.setenv("ARTIFACT_STORAGE_ACCESS_KEY", TEST_STORAGE_ACCESS_KEY)
    monkeypatch.setenv("ARTIFACT_STORAGE_SECRET_KEY", TEST_STORAGE_SECRET_KEY)

    from db.storage import get_storage_client, reset_client_cache

    reset_client_cache()
    client = get_storage_client()
    assert client is not None
    client.ensure_bucket()
    _purge_bucket(client)
    try:
        yield client
    finally:
        _purge_bucket(client)
        reset_client_cache()


@pytest.fixture(autouse=True)
def _scrub_storage_env(monkeypatch):
    """Clear ARTIFACT_STORAGE_* before every test.

    `db/models.py` calls `load_dotenv()` at import, which re-populates these
    from a developer's `.env` after any one-time scrub. Doing it per-test
    via monkeypatch is the only reliable way to keep the storage-off path
    available; tests that need real storage receive `storage_bucket`, which
    re-sets the same vars (monkeypatch lets the later setenv win).
    """
    for k in _STORAGE_ENV_KEYS:
        monkeypatch.delenv(k, raising=False)
    try:
        from db.storage import reset_client_cache
    except ImportError:
        pass
    else:
        reset_client_cache()
    yield


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


class SessionFactory:
    """Stand-in for sessionmaker that yields a single shared Session.

    Use to wire ``api.SessionLocal`` (or any code expecting a sessionmaker)
    at the test DB without mutating ``DATABASE_URL`` globally.
    """

    def __init__(self, session):
        self._session = session

    def __call__(self):
        return self

    def __enter__(self):
        return self._session

    def __exit__(self, *exc):
        return False


@pytest.fixture()
def api_client(monkeypatch, db_session):
    """TestClient with ``api.SessionLocal`` pointed at the test DB session.

    Avoids the prod-default engine (DATABASE_URL=postgresql://...:5433/psat)
    that the FastAPI app would otherwise use.
    """
    from fastapi.testclient import TestClient

    import api as api_module

    monkeypatch.setattr(api_module, "SessionLocal", SessionFactory(db_session))
    return TestClient(api_module.app)


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


@pytest.fixture(scope="session", autouse=True)
def _apply_storage_schema_once():
    """Ensure new artifact storage columns exist on TEST_DATABASE_URL.

    Many test modules build their own engine and call ``Base.metadata.create_all``,
    which is a no-op for tables that pre-exist — so on a long-lived test DB the
    storage_key/size_bytes/content_type columns would be missing. This session-
    scoped autouse fixture runs the ALTER TABLE statements once at the start of
    the test session against the canonical TEST_DATABASE_URL.
    """
    if not _can_connect():
        return
    from db.models import apply_storage_migrations

    test_engine = create_engine(DATABASE_URL)
    try:
        # Ensure tables exist before altering.
        from db.models import Base as _Base

        _Base.metadata.create_all(test_engine)
        apply_storage_migrations(test_engine)
    finally:
        test_engine.dispose()


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
    from db.models import apply_storage_migrations

    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    apply_storage_migrations(engine)

    session = Session(engine, expire_on_commit=False)
    try:
        yield session
    finally:
        session.rollback()
        # Clean monitoring + coverage tables (order respects FK constraints).
        # AuditContractCoverage references Contract + AuditReport + Protocol;
        # delete it before those get cascaded away via Protocol cleanup.
        for model in [
            AuditContractCoverage,
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
