"""Integration tests for the Discord notification pipeline.

Tests the full chain: scan/poll detects upgrade → notifier queries subscriptions
→ Discord webhook POST. Also tests subscription CRUD via the API endpoints.

All tests run without live services — PostgreSQL for DB, mocked RPC
and mocked requests.post for Discord.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from conftest import ADDR, _add_proxy, _add_subscription, _make_log, _topic_for, requires_postgres

pytestmark = requires_postgres
from sqlalchemy import select

from db.models import ProxySubscription, ProxyUpgradeEvent, WatchedProxy
from services.discovery.upgrade_history import UPGRADED_TOPIC0
from services.monitoring.notifier import notify_upgrades
from services.monitoring.proxy_watcher import poll_for_upgrades, scan_for_upgrades

# ---------------------------------------------------------------------------
# Helper: mock RPC that returns a single log
# ---------------------------------------------------------------------------


def _rpc_returning_log(log, latest_block=100):
    """Return an rpc_request side_effect that serves one log."""
    return lambda url, method, params: (
        hex(latest_block) if method == "eth_blockNumber" else [log] if method == "eth_getLogs" else None
    )


# ---------------------------------------------------------------------------
# Integration: scan/poll → notify
# ---------------------------------------------------------------------------


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_scan_triggers_discord_notification(mock_rpc, mock_discord, db_session):
    """Full chain: scanner detects Upgraded event → notifier POSTs to Discord."""
    mock_discord.return_value = MagicMock(ok=True)

    proxy = _add_proxy(db_session, ADDR(1), label="Aave Pool", last_known_impl=ADDR(10), last_scanned_block=90)
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/111/aaa")

    log = _make_log(ADDR(1), UPGRADED_TOPIC0, _topic_for(ADDR(11)), block=hex(95), tx="0x" + "de" * 32)
    mock_rpc.side_effect = _rpc_returning_log(log)

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    notify_upgrades(db_session, events)

    mock_discord.assert_called_once()
    embed = mock_discord.call_args[1]["json"]["embeds"][0]
    assert "Aave Pool" in embed["title"]
    field_values = {f["name"]: f["value"] for f in embed["fields"]}
    assert ADDR(1) in field_values["Proxy"]
    assert ADDR(11).lower() in field_values["New Implementation"].lower()


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_batch_request")
def test_poll_triggers_discord_notification(mock_batch, mock_discord, db_session):
    """Poller detects implementation change via storage slot → Discord webhook."""
    mock_discord.return_value = MagicMock(ok=True)

    proxy = _add_proxy(db_session, ADDR(1), label="Compound cUSDC", last_known_impl=ADDR(10), needs_polling=True)
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/222/bbb")

    zero = "0x" + "0" * 64
    mock_batch.return_value = ["0x" + "0" * 24 + ADDR(11)[2:]] + [zero] * 7

    events = poll_for_upgrades(db_session, "http://localhost:8545")
    notify_upgrades(db_session, events)

    mock_discord.assert_called_once()
    assert "Compound cUSDC" in mock_discord.call_args[1]["json"]["embeds"][0]["title"]


# ---------------------------------------------------------------------------
# Fan-out and targeting
# ---------------------------------------------------------------------------


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_multiple_subscribers_all_notified(mock_rpc, mock_discord, db_session):
    """Two webhooks on the same proxy — both get called."""
    mock_discord.return_value = MagicMock(ok=True)

    proxy = _add_proxy(db_session, ADDR(1), last_known_impl=ADDR(10), last_scanned_block=90)
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/1/alice")
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/2/bob")

    log = _make_log(ADDR(1), UPGRADED_TOPIC0, _topic_for(ADDR(11)), block=hex(95), tx="0x" + "ff" * 32)
    mock_rpc.side_effect = _rpc_returning_log(log)

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    notify_upgrades(db_session, events)

    assert mock_discord.call_count == 2
    urls = {c[0][0] for c in mock_discord.call_args_list}
    assert urls == {"https://discord.com/api/webhooks/1/alice", "https://discord.com/api/webhooks/2/bob"}


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_only_subscribers_of_upgraded_proxy_notified(mock_rpc, mock_discord, db_session):
    """Proxy B's subscriber is not pinged when only proxy A upgrades."""
    mock_discord.return_value = MagicMock(ok=True)

    proxy_a = _add_proxy(db_session, ADDR(1), last_known_impl=ADDR(10), last_scanned_block=90)
    proxy_b = _add_proxy(db_session, ADDR(2), last_known_impl=ADDR(20), last_scanned_block=90)
    _add_subscription(db_session, proxy_a, "https://discord.com/api/webhooks/a/sub")
    _add_subscription(db_session, proxy_b, "https://discord.com/api/webhooks/b/sub")

    log = _make_log(ADDR(1), UPGRADED_TOPIC0, _topic_for(ADDR(11)), block=hex(95), tx="0x" + "aa" * 32)
    mock_rpc.side_effect = _rpc_returning_log(log)

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    notify_upgrades(db_session, events)

    assert mock_discord.call_count == 1
    assert mock_discord.call_args[0][0] == "https://discord.com/api/webhooks/a/sub"


# ---------------------------------------------------------------------------
# Resilience
# ---------------------------------------------------------------------------


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_webhook_failure_does_not_crash_scan_loop(mock_rpc, mock_discord, db_session):
    """Discord down — event still persisted, no crash."""
    mock_discord.side_effect = Exception("Discord is down")

    proxy = _add_proxy(db_session, ADDR(1), last_known_impl=ADDR(10), last_scanned_block=90)
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/broken/url")

    log = _make_log(ADDR(1), UPGRADED_TOPIC0, _topic_for(ADDR(11)), block=hex(95), tx="0x" + "cc" * 32)
    mock_rpc.side_effect = _rpc_returning_log(log)

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    notify_upgrades(db_session, events)

    db_session.refresh(proxy)
    assert proxy.last_known_implementation == ADDR(11).lower()


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_one_bad_webhook_doesnt_block_others(mock_rpc, mock_discord, db_session):
    """First webhook fails, second still gets called."""
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
    mock_rpc.side_effect = _rpc_returning_log(log)

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    notify_upgrades(db_session, events)

    assert len(call_log) == 2
    assert "https://discord.com/api/webhooks/good/two" in call_log


# ---------------------------------------------------------------------------
# Edge case: null webhook URL
# ---------------------------------------------------------------------------


@patch("services.monitoring.notifier.requests.post")
def test_subscription_without_webhook_url_is_skipped(mock_discord, db_session):
    """A subscription with discord_webhook_url=None is not called."""
    proxy = _add_proxy(db_session, ADDR(1), last_known_impl=ADDR(10))
    sub = ProxySubscription(id=uuid.uuid4(), watched_proxy_id=proxy.id, discord_webhook_url=None, label="no-url")
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
# Embed format
# ---------------------------------------------------------------------------


@patch("services.monitoring.notifier.requests.post")
@patch("services.monitoring.proxy_watcher.rpc_request")
def test_embed_format_complete(mock_rpc, mock_discord, db_session):
    """Discord embed has all expected fields with correct values."""
    mock_discord.return_value = MagicMock(ok=True)

    proxy = _add_proxy(db_session, ADDR(1), label="Lido stETH", last_known_impl=ADDR(10), last_scanned_block=90)
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/1/x")

    tx_hash = "0x" + "ef" * 32
    log = _make_log(ADDR(1), UPGRADED_TOPIC0, _topic_for(ADDR(11)), block=hex(12345), tx=tx_hash)
    mock_rpc.side_effect = _rpc_returning_log(log, latest_block=13000)

    events = scan_for_upgrades(db_session, "http://localhost:8545")
    notify_upgrades(db_session, events)

    embed = mock_discord.call_args[1]["json"]["embeds"][0]
    field_map = {f["name"]: f["value"] for f in embed["fields"]}

    assert "Lido stETH" in embed["title"]
    assert embed["color"] == 0xFF9900
    assert ADDR(1) in field_map["Proxy"]
    assert "ethereum" == field_map["Chain"]
    assert "upgraded" == field_map["Event"]
    assert ADDR(11).lower() in field_map["New Implementation"].lower()
    assert ADDR(10) in field_map["Old Implementation"]
    assert "12345" == field_map["Block"]
    assert tx_hash in field_map["Tx"]


# ---------------------------------------------------------------------------
# API: subscription CRUD
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
    """POST /api/watched-proxies with discord_webhook_url creates both proxy and subscription."""
    resp = api_client.post(
        "/api/watched-proxies",
        json={"address": ADDR(1), "label": "Test", "discord_webhook_url": "https://discord.com/api/webhooks/t/h"},
    )
    assert resp.status_code == 200
    assert "subscription_id" in resp.json()

    subs = db_session.execute(select(ProxySubscription)).scalars().all()
    assert len(subs) == 1
    assert subs[0].discord_webhook_url == "https://discord.com/api/webhooks/t/h"


def test_api_watch_proxy_without_discord_no_subscription(api_client, db_session):
    """POST without discord_webhook_url creates proxy but no subscription."""
    resp = api_client.post("/api/watched-proxies", json={"address": ADDR(2)})
    assert resp.status_code == 200
    assert "subscription_id" not in resp.json()
    assert db_session.execute(select(ProxySubscription)).scalars().all() == []


def test_api_second_watch_adds_subscription_only(api_client, db_session):
    """Same proxy watched twice — one WatchedProxy, two subscriptions."""
    api_client.post("/api/watched-proxies", json={"address": ADDR(3), "discord_webhook_url": "https://d.co/1"})
    api_client.post("/api/watched-proxies", json={"address": ADDR(3), "discord_webhook_url": "https://d.co/2"})

    assert len(db_session.execute(select(WatchedProxy)).scalars().all()) == 1
    subs = db_session.execute(select(ProxySubscription)).scalars().all()
    assert len(subs) == 2
    assert {s.discord_webhook_url for s in subs} == {"https://d.co/1", "https://d.co/2"}


def test_api_list_subscriptions(api_client, db_session):
    proxy = _add_proxy(db_session, ADDR(4))
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/list/test", label="my alerts")

    resp = api_client.get(f"/api/watched-proxies/{proxy.id}/subscriptions")
    assert resp.status_code == 200
    assert len(resp.json()) == 1
    assert resp.json()[0]["label"] == "my alerts"


def test_api_list_subscriptions_404_for_missing_proxy(api_client):
    assert api_client.get(f"/api/watched-proxies/{uuid.uuid4()}/subscriptions").status_code == 404


def test_api_add_subscription_to_existing_proxy(api_client, db_session):
    proxy = _add_proxy(db_session, ADDR(5))
    resp = api_client.post(
        f"/api/watched-proxies/{proxy.id}/subscriptions",
        json={"discord_webhook_url": "https://discord.com/api/webhooks/add/test", "label": "added later"},
    )
    assert resp.status_code == 200
    assert resp.json()["watched_proxy_id"] == str(proxy.id)


def test_api_add_subscription_404_for_missing_proxy(api_client):
    resp = api_client.post(
        f"/api/watched-proxies/{uuid.uuid4()}/subscriptions",
        json={"discord_webhook_url": "https://discord.com/api/webhooks/x/y"},
    )
    assert resp.status_code == 404


def test_api_delete_subscription(api_client, db_session):
    proxy = _add_proxy(db_session, ADDR(6))
    sub = _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/del/test")

    assert api_client.delete(f"/api/subscriptions/{sub.id}").status_code == 200
    assert db_session.execute(select(ProxySubscription)).scalars().all() == []


def test_api_delete_subscription_404_for_missing(api_client):
    assert api_client.delete(f"/api/subscriptions/{uuid.uuid4()}").status_code == 404


def test_api_delete_proxy_cascades_subscriptions(api_client, db_session):
    proxy = _add_proxy(db_session, ADDR(7))
    _add_subscription(db_session, proxy, "https://discord.com/api/webhooks/cascade/test")

    assert api_client.delete(f"/api/watched-proxies/{proxy.id}").status_code == 200
    assert db_session.execute(select(ProxySubscription)).scalars().all() == []
