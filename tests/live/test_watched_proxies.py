"""Watched-proxies + subscriptions roundtrip against Aave V3 Pool (EIP-1967 proxy on mainnet)."""

from __future__ import annotations

from typing import Any

import pytest
import requests

from tests.live.conftest import LiveClient

# Distinct from test_proxy_flow's USDC so the two files can run concurrently.
AAVE_V3_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"

TEST_DISCORD_WEBHOOK = "https://discord.com/api/webhooks/0/psat-live-test-never-delivered"


@pytest.fixture(scope="module")
def watched_proxy(live_client: LiveClient, request) -> dict[str, Any]:
    # POST is idempotent at (address, chain); finalizer FK-cascades to subs + monitored.
    payload = {"address": AAVE_V3_POOL, "chain": "ethereum", "label": "psat-live-test"}
    proxy = live_client.add_watched_proxy(payload)
    assert proxy.get("id"), f"add_watched_proxy response missing id: {proxy}"

    def _cleanup():
        try:
            live_client.delete_watched_proxy(proxy["id"])
        except requests.HTTPError:
            pass

    request.addfinalizer(_cleanup)
    return proxy


def test_watched_proxy_persisted(watched_proxy, live_client: LiveClient):
    listed = live_client.list_watched_proxies()
    ids = {p["id"] for p in listed}
    assert watched_proxy["id"] in ids, f"Watched proxy {watched_proxy['id']} missing from list"


def test_watched_proxy_metadata(watched_proxy):
    assert watched_proxy["proxy_address"].lower() == AAVE_V3_POOL.lower()
    assert watched_proxy["chain"] == "ethereum"
    # proxy_type is best-effort (None on flaky RPC); chain + created_at are required.
    assert watched_proxy["created_at"]


@pytest.fixture(scope="module")
def subscription(watched_proxy, live_client: LiveClient, request) -> dict[str, Any]:
    payload = {"discord_webhook_url": TEST_DISCORD_WEBHOOK, "label": "psat-live-test"}
    sub = live_client.add_subscription(watched_proxy["id"], payload)
    assert sub.get("id")

    def _cleanup():
        try:
            live_client.delete_subscription(sub["id"])
        except requests.HTTPError:
            pass

    request.addfinalizer(_cleanup)
    return sub


def test_subscription_created(subscription, watched_proxy):
    assert subscription["watched_proxy_id"] == watched_proxy["id"]
    assert subscription["discord_webhook_url"] == TEST_DISCORD_WEBHOOK


def test_subscription_listed(subscription, watched_proxy, live_client: LiveClient):
    subs = live_client.list_subscriptions(watched_proxy["id"])
    ids = {s["id"] for s in subs}
    assert subscription["id"] in ids


def test_subscription_delete(watched_proxy, live_client: LiveClient):
    # Own subscription so DELETE can be verified independent of fixture cleanup.
    payload = {"discord_webhook_url": TEST_DISCORD_WEBHOOK, "label": "psat-live-test-ephemeral"}
    sub = live_client.add_subscription(watched_proxy["id"], payload)
    live_client.delete_subscription(sub["id"])

    remaining = live_client.list_subscriptions(watched_proxy["id"])
    assert sub["id"] not in {s["id"] for s in remaining}


def test_proxy_events_endpoint(live_client: LiveClient):
    # Shape-only — most previews have zero events unless a real upgrade fired.
    events = live_client.list_proxy_events()
    assert isinstance(events, list)
    for event in events[:5]:
        for key in ("id", "watched_proxy_id", "block_number", "tx_hash", "new_implementation"):
            assert key in event, f"proxy event missing {key!r}: {event}"
