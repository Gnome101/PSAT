"""Integration tests for the Discord notification pipeline.

Tests the full chain: scan/poll detects upgrade → notifier queries subscriptions
→ Discord webhook POST. Also tests subscription CRUD via the API endpoints.

All tests run without live services — in-memory SQLite for DB, mocked RPC
and mocked requests.post for Discord.
"""

from __future__ import annotations

import sys
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.models import ProxySubscription, ProxyUpgradeEvent, WatchedProxy
from services.discovery.upgrade_history import UPGRADED_TOPIC0
from services.monitoring.notifier import notify_upgrades
from services.monitoring.proxy_watcher import poll_for_upgrades, scan_for_upgrades

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def ADDR(n: int) -> str:
    return "0x" + hex(n)[2:].zfill(40)


def _topic_for(addr: str) -> str:
    return "0x" + "0" * 24 + addr[2:]


def _make_log(address, topic0, topic1=None, data="0x", block="0x64", tx="0xaaa", log_index="0x0"):
    return {
        "address": address,
        "topics": [topic0] + ([topic1] if topic1 else []),
        "data": data,
        "blockNumber": block,
        "transactionHash": tx,
        "logIndex": log_index,
        "timeStamp": "0x65a00000",
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_session():
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
    session,
    address,
    label=None,
    last_known_impl=None,
    last_scanned_block=0,
    needs_polling=False,
    proxy_type=None,
):
    proxy = WatchedProxy(
        id=uuid.uuid4(),
        proxy_address=address,
        chain="ethereum",
        label=label,
        proxy_type=proxy_type,
        last_known_implementation=last_known_impl,
        last_scanned_block=last_scanned_block,
        needs_polling=needs_polling,
    )
    session.add(proxy)
    session.commit()
    return proxy


def _add_subscription(session, proxy, discord_url, label=None):
    sub = ProxySubscription(
        id=uuid.uuid4(),
        watched_proxy_id=proxy.id,
        discord_webhook_url=discord_url,
        label=label,
    )
    session.add(sub)
    session.commit()
    return sub


# ---------------------------------------------------------------------------
# Integration: scan_for_upgrades → notify_upgrades
# ---------------------------------------------------------------------------


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_scan_triggers_discord_notification(mock_rpc, mock_discord, db_session):
    """Full integration: scanner detects an Upgraded event → notifier sends
    a Discord webhook to subscribed users."""
    mock_discord.return_value = MagicMock(ok=True)

    proxy_addr = ADDR(1)
    old_impl = ADDR(10)
    new_impl = ADDR(11)
    proxy = _add_proxy(db_session, proxy_addr, label="Aave Pool", last_known_impl=old_impl, last_scanned_block=90)
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/111/aaa")

    log = _make_log(proxy_addr, UPGRADED_TOPIC0, _topic_for(new_impl), block=hex(95), tx="0x" + "de" * 32)

    def rpc_side_effect(url, method, params):
        if method == "eth_blockNumber":
            return hex(100)
        if method == "eth_getLogs":
            return [log]
        return None

    mock_rpc.side_effect = rpc_side_effect

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    assert len(events) == 1

    # Now trigger notifications with the same session (as the real loop does)
    notify_upgrades(db_session, events)

    mock_discord.assert_called_once()
    payload = mock_discord.call_args[1]["json"]
    embed = payload["embeds"][0]
    assert "Aave Pool" in embed["title"]
    # Verify embed contains the actual addresses
    field_values = {f["name"]: f["value"] for f in embed["fields"]}
    assert proxy_addr in field_values["Proxy"]
    assert new_impl.lower() in field_values["New Implementation"].lower()


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_scan_no_subscription_no_discord_call(mock_rpc, mock_discord, db_session):
    """Scanner detects an upgrade but no subscriptions exist — Discord is never called."""
    proxy_addr = ADDR(1)
    _add_proxy(db_session, proxy_addr, last_known_impl=ADDR(10), last_scanned_block=90)
    # No subscription added

    log = _make_log(proxy_addr, UPGRADED_TOPIC0, _topic_for(ADDR(11)), block=hex(95), tx="0x" + "ab" * 32)

    mock_rpc.side_effect = lambda url, method, params: (
        hex(100) if method == "eth_blockNumber" else [log] if method == "eth_getLogs" else None
    )

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    assert len(events) == 1

    notify_upgrades(db_session, events)
    mock_discord.assert_not_called()


# ---------------------------------------------------------------------------
# Integration: poll_for_upgrades → notify_upgrades
# ---------------------------------------------------------------------------


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_batch_request")
def test_poll_triggers_discord_notification(mock_batch, mock_discord, db_session):
    """Full integration: poller detects an implementation change via storage
    slot → notifier sends a Discord webhook."""
    mock_discord.return_value = MagicMock(ok=True)

    old_impl = ADDR(10)
    new_impl = ADDR(11)
    proxy = _add_proxy(db_session, ADDR(1), label="Compound cUSDC", last_known_impl=old_impl, needs_polling=True)
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/222/bbb")

    storage_value = "0x" + "0" * 24 + new_impl[2:]
    zero = "0x" + "0" * 64
    mock_batch.return_value = [storage_value] + [zero] * 7

    events = poll_for_upgrades(db_session, "http://localhost:8545")
    assert len(events) == 1
    assert events[0].event_type == "storage_poll"

    notify_upgrades(db_session, events)

    mock_discord.assert_called_once()
    embed = mock_discord.call_args[1]["json"]["embeds"][0]
    assert "Compound cUSDC" in embed["title"]


# ---------------------------------------------------------------------------
# Fan-out: multiple subscriptions, multiple proxies
# ---------------------------------------------------------------------------


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_multiple_subscribers_all_notified(mock_rpc, mock_discord, db_session):
    """Two users subscribed to the same proxy both get Discord notifications."""
    mock_discord.return_value = MagicMock(ok=True)

    proxy = _add_proxy(db_session, ADDR(1), last_known_impl=ADDR(10), last_scanned_block=90)
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/1/alice")
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/2/bob")

    log = _make_log(ADDR(1), UPGRADED_TOPIC0, _topic_for(ADDR(11)), block=hex(95), tx="0x" + "ff" * 32)

    mock_rpc.side_effect = lambda url, method, params: (
        hex(100) if method == "eth_blockNumber" else [log] if method == "eth_getLogs" else None
    )

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    notify_upgrades(db_session, events)

    assert mock_discord.call_count == 2
    urls = {c[0][0] for c in mock_discord.call_args_list}
    assert urls == {
        "https://discord.com/api/webhooks/1/alice",
        "https://discord.com/api/webhooks/2/bob",
    }


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_only_subscribers_of_upgraded_proxy_notified(mock_rpc, mock_discord, db_session):
    """Two proxies watched, only one upgrades — only that proxy's subscribers
    are notified, the other proxy's subscriber is not."""
    mock_discord.return_value = MagicMock(ok=True)

    proxy_a = _add_proxy(db_session, ADDR(1), last_known_impl=ADDR(10), last_scanned_block=90)
    proxy_b = _add_proxy(db_session, ADDR(2), last_known_impl=ADDR(20), last_scanned_block=90)
    _add_subscription(db_session, proxy_a, "https://discord.com/api/webhooks/a/sub")
    _add_subscription(db_session, proxy_b, "https://discord.com/api/webhooks/b/sub")

    # Only proxy A upgrades
    log = _make_log(ADDR(1), UPGRADED_TOPIC0, _topic_for(ADDR(11)), block=hex(95), tx="0x" + "aa" * 32)

    mock_rpc.side_effect = lambda url, method, params: (
        hex(100) if method == "eth_blockNumber" else [log] if method == "eth_getLogs" else None
    )

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    assert len(events) == 1

    notify_upgrades(db_session, events)

    assert mock_discord.call_count == 1
    assert mock_discord.call_args[0][0] == "https://discord.com/api/webhooks/a/sub"


# ---------------------------------------------------------------------------
# Resilience: webhook failure doesn't break the pipeline
# ---------------------------------------------------------------------------


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_webhook_failure_does_not_crash_scan_loop(mock_rpc, mock_discord, db_session):
    """Discord returns 500 — notify_upgrades logs it but doesn't raise,
    so the scan loop continues on the next cycle."""
    mock_discord.side_effect = Exception("Discord is down")

    proxy = _add_proxy(db_session, ADDR(1), last_known_impl=ADDR(10), last_scanned_block=90)
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/broken/url")

    log = _make_log(ADDR(1), UPGRADED_TOPIC0, _topic_for(ADDR(11)), block=hex(95), tx="0x" + "cc" * 32)

    mock_rpc.side_effect = lambda url, method, params: (
        hex(100) if method == "eth_blockNumber" else [log] if method == "eth_getLogs" else None
    )

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    assert len(events) == 1

    # Should not raise even though Discord is down
    notify_upgrades(db_session, events)

    # Event was still persisted to DB despite notification failure
    db_session.refresh(proxy)
    assert proxy.last_known_implementation == ADDR(11).lower()
    assert proxy.last_scanned_block == 100


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_one_bad_webhook_doesnt_block_others(mock_rpc, mock_discord, db_session):
    """Two subscriptions: first webhook fails, second still gets called."""
    call_log = []

    def discord_side_effect(url, **kwargs):
        call_log.append(url)
        if "broken" in url:
            raise Exception("timeout")
        return MagicMock(ok=True)

    mock_discord.side_effect = discord_side_effect

    proxy = _add_proxy(db_session, ADDR(1), last_known_impl=ADDR(10), last_scanned_block=90)
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/broken/one")
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/good/two")

    log = _make_log(ADDR(1), UPGRADED_TOPIC0, _topic_for(ADDR(11)), block=hex(95), tx="0x" + "dd" * 32)

    mock_rpc.side_effect = lambda url, method, params: (
        hex(100) if method == "eth_blockNumber" else [log] if method == "eth_getLogs" else None
    )

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    notify_upgrades(db_session, events)

    # Both were attempted
    assert len(call_log) == 2
    assert "https://discord.com/api/webhooks/good/two" in call_log


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@patch("services.monitoring.notifier.requests.post")
def test_notify_empty_events_is_noop(mock_discord, db_session):
    """Empty event list does nothing."""
    notify_upgrades(db_session, [])
    mock_discord.assert_not_called()


@patch("services.monitoring.notifier.requests.post")
def test_subscription_without_webhook_url_is_skipped(mock_discord, db_session):
    """A subscription with discord_webhook_url=None is not called."""
    proxy = _add_proxy(db_session, ADDR(1), last_known_impl=ADDR(10))
    # Subscription with no URL
    sub = ProxySubscription(
        id=uuid.uuid4(),
        watched_proxy_id=proxy.id,
        discord_webhook_url=None,
        label="no-url",
    )
    db_session.add(sub)
    db_session.commit()

    evt = ProxyUpgradeEvent(
        id=uuid.uuid4(),
        watched_proxy_id=proxy.id,
        block_number=100,
        tx_hash="0x" + "ab" * 32,
        old_implementation=ADDR(10),
        new_implementation=ADDR(11),
        event_type="upgraded",
    )
    evt.watched_proxy = proxy
    db_session.add(evt)
    db_session.commit()

    notify_upgrades(db_session, [evt])
    mock_discord.assert_not_called()


# ---------------------------------------------------------------------------
# Embed format validation
# ---------------------------------------------------------------------------


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_embed_format_complete(mock_rpc, mock_discord, db_session):
    """Verifies the Discord embed has all expected fields with correct values."""
    mock_discord.return_value = MagicMock(ok=True)

    proxy_addr = ADDR(1)
    old_impl = ADDR(10)
    new_impl = ADDR(11)
    proxy = _add_proxy(db_session, proxy_addr, label="Lido stETH", last_known_impl=old_impl, last_scanned_block=90)
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/1/x")

    tx_hash = "0x" + "ef" * 32
    log = _make_log(proxy_addr, UPGRADED_TOPIC0, _topic_for(new_impl), block=hex(12345), tx=tx_hash)

    mock_rpc.side_effect = lambda url, method, params: (
        hex(13000) if method == "eth_blockNumber" else [log] if method == "eth_getLogs" else None
    )

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    notify_upgrades(db_session, events)

    embed = mock_discord.call_args[1]["json"]["embeds"][0]
    field_map = {f["name"]: f["value"] for f in embed["fields"]}

    assert "Lido stETH" in embed["title"]
    assert embed["color"] == 0xFF9900
    assert proxy_addr in field_map["Proxy"]
    assert "ethereum" == field_map["Chain"]
    assert "upgraded" == field_map["Event"]
    assert new_impl.lower() in field_map["New Implementation"].lower()
    assert old_impl in field_map["Old Implementation"]
    assert "12345" == field_map["Block"]
    assert tx_hash in field_map["Tx"]


# ---------------------------------------------------------------------------
# API integration: subscription CRUD
# ---------------------------------------------------------------------------


@pytest.fixture()
def api_client(db_session):
    """FastAPI test client wired to the in-memory SQLite session."""

    @contextmanager
    def fake_session_local():
        yield db_session

    classifier_patch = "services.discovery.classifier.classify_single"
    resolve_patch = "services.monitoring.proxy_watcher.resolve_current_implementation"
    block_patch = "services.monitoring.proxy_watcher.get_latest_block"
    with (
        patch("api.SessionLocal", fake_session_local),
        patch(classifier_patch, return_value={"type": "proxy", "proxy_type": "eip1967"}),
        patch(resolve_patch, return_value=ADDR(99).lower()),
        patch(block_patch, return_value=1000),
    ):
        from fastapi.testclient import TestClient

        import api

        yield TestClient(api.app)


def test_api_watch_proxy_with_discord_creates_subscription(api_client, db_session):
    """POST /api/watched-proxies with discord_webhook_url creates both
    the WatchedProxy and a ProxySubscription in one request."""
    resp = api_client.post(
        "/api/watched-proxies",
        json={
            "address": ADDR(1),
            "label": "Test Proxy",
            "discord_webhook_url": "https://discord.com/api/webhooks/test/hook",
        },
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["proxy_address"] == ADDR(1).lower()
    assert "subscription_id" in data

    # Verify subscription was persisted
    from sqlalchemy import select

    subs = db_session.execute(select(ProxySubscription)).scalars().all()
    assert len(subs) == 1
    assert subs[0].discord_webhook_url == "https://discord.com/api/webhooks/test/hook"


def test_api_watch_proxy_without_discord_no_subscription(api_client, db_session):
    """POST /api/watched-proxies without discord_webhook_url creates the proxy
    but no subscription."""
    resp = api_client.post(
        "/api/watched-proxies",
        json={
            "address": ADDR(2),
        },
    )

    assert resp.status_code == 200
    assert "subscription_id" not in resp.json()

    from sqlalchemy import select

    subs = db_session.execute(select(ProxySubscription)).scalars().all()
    assert len(subs) == 0


def test_api_second_watch_adds_subscription_only(api_client, db_session):
    """Watching the same proxy twice doesn't duplicate the WatchedProxy —
    it just adds a second subscription."""
    # First watch
    resp1 = api_client.post(
        "/api/watched-proxies",
        json={
            "address": ADDR(3),
            "discord_webhook_url": "https://discord.com/api/webhooks/user1/hook",
        },
    )
    assert resp1.status_code == 200

    # Second watch — same address, different webhook
    resp2 = api_client.post(
        "/api/watched-proxies",
        json={
            "address": ADDR(3),
            "discord_webhook_url": "https://discord.com/api/webhooks/user2/hook",
        },
    )
    assert resp2.status_code == 200

    from sqlalchemy import select

    proxies = db_session.execute(select(WatchedProxy)).scalars().all()
    subs = db_session.execute(select(ProxySubscription)).scalars().all()

    assert len(proxies) == 1, "Should be one WatchedProxy, not two"
    assert len(subs) == 2, "Should have two subscriptions"
    urls = {s.discord_webhook_url for s in subs}
    assert urls == {
        "https://discord.com/api/webhooks/user1/hook",
        "https://discord.com/api/webhooks/user2/hook",
    }


def test_api_list_subscriptions(api_client, db_session):
    """GET /api/watched-proxies/{id}/subscriptions returns all subscriptions."""
    proxy = _add_proxy(db_session, ADDR(4))
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/list/test", label="my alerts")

    resp = api_client.get(f"/api/watched-proxies/{proxy.id}/subscriptions")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["discord_webhook_url"] == "https://discord.com/api/webhooks/list/test"
    assert data[0]["label"] == "my alerts"


def test_api_list_subscriptions_404_for_missing_proxy(api_client):
    """GET subscriptions for a nonexistent proxy returns 404."""
    resp = api_client.get(f"/api/watched-proxies/{uuid.uuid4()}/subscriptions")
    assert resp.status_code == 404


def test_api_add_subscription_to_existing_proxy(api_client, db_session):
    """POST /api/watched-proxies/{id}/subscriptions adds a subscription."""
    proxy = _add_proxy(db_session, ADDR(5))

    resp = api_client.post(
        f"/api/watched-proxies/{proxy.id}/subscriptions",
        json={
            "discord_webhook_url": "https://discord.com/api/webhooks/add/test",
            "label": "added later",
        },
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["discord_webhook_url"] == "https://discord.com/api/webhooks/add/test"
    assert data["label"] == "added later"
    assert data["watched_proxy_id"] == str(proxy.id)


def test_api_add_subscription_404_for_missing_proxy(api_client):
    """POST subscription to a nonexistent proxy returns 404."""
    resp = api_client.post(
        f"/api/watched-proxies/{uuid.uuid4()}/subscriptions",
        json={
            "discord_webhook_url": "https://discord.com/api/webhooks/x/y",
        },
    )
    assert resp.status_code == 404


def test_api_delete_subscription(api_client, db_session):
    """DELETE /api/subscriptions/{id} removes the subscription."""
    proxy = _add_proxy(db_session, ADDR(6))
    sub = _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/del/test")

    resp = api_client.delete(f"/api/subscriptions/{sub.id}")

    assert resp.status_code == 200
    assert resp.json() == {"status": "removed"}

    # Verify it's gone
    from sqlalchemy import select

    remaining = db_session.execute(select(ProxySubscription)).scalars().all()
    assert len(remaining) == 0


def test_api_delete_subscription_404_for_missing(api_client):
    """DELETE a nonexistent subscription returns 404."""
    resp = api_client.delete(f"/api/subscriptions/{uuid.uuid4()}")
    assert resp.status_code == 404


def test_api_delete_proxy_cascades_subscriptions(api_client, db_session):
    """Deleting a watched proxy also removes its subscriptions (CASCADE)."""
    proxy = _add_proxy(db_session, ADDR(7))
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/cascade/test")

    resp = api_client.delete(f"/api/watched-proxies/{proxy.id}")
    assert resp.status_code == 200

    from sqlalchemy import select

    subs = db_session.execute(select(ProxySubscription)).scalars().all()
    assert len(subs) == 0
