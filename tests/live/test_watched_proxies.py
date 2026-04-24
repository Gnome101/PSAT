"""Live integration tests for the watched-proxies + subscriptions API.

Exercises the full roundtrip of adding a proxy to the watcher, attaching
a notification subscription, reading both back, and tearing everything
down. Uses a well-known public EIP-1967 proxy so the server-side
classification inside POST /api/watched-proxies resolves cleanly against
mainnet RPC.
"""

from __future__ import annotations

from typing import Any

import pytest
import requests

from tests.live.conftest import LiveClient

# Aave V3 Pool on Ethereum mainnet — stable, well-known TransparentUpgradeable
# proxy. Chosen over USDC (which test_proxy_flow uses) so the two files
# can run concurrently without stepping on each other's DB state.
AAVE_V3_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"

# Fake webhook host — never actually called because proxy upgrade events
# only trigger on real on-chain upgrades, which won't fire inside a test run.
TEST_DISCORD_WEBHOOK = "https://discord.com/api/webhooks/0/psat-live-test-never-delivered"


@pytest.fixture(scope="module")
def watched_proxy(live_client: LiveClient, request) -> dict[str, Any]:
    """Add Aave V3 Pool to the watch list; remove it + its subscriptions on teardown.

    The POST endpoint is idempotent at the (address, chain) level — if a
    previous failed run left the row behind, it returns the existing row
    rather than erroring. Finalizer drops the proxy, which FK-cascades
    to subscriptions and monitored-contracts.
    """
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
    # Classification is best-effort — proxy_type may be None if the RPC
    # was flaky during the initial POST. But the row must record the chain
    # we asked for and a stamped creation time.
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
    # Deliberately not using the session fixture — create an ephemeral
    # subscription here so we can assert DELETE removes it independent
    # of other tests' cleanup.
    payload = {"discord_webhook_url": TEST_DISCORD_WEBHOOK, "label": "psat-live-test-ephemeral"}
    sub = live_client.add_subscription(watched_proxy["id"], payload)
    live_client.delete_subscription(sub["id"])

    remaining = live_client.list_subscriptions(watched_proxy["id"])
    assert sub["id"] not in {s["id"] for s in remaining}


def test_proxy_events_endpoint(live_client: LiveClient):
    # Read-only shape test — the preview will typically have zero events
    # unless an actual upgrade fired during the watched window, so we
    # assert structure rather than content.
    events = live_client.list_proxy_events()
    assert isinstance(events, list)
    for event in events[:5]:
        for key in ("id", "watched_proxy_id", "block_number", "tx_hash", "new_implementation"):
            assert key in event, f"proxy event missing {key!r}: {event}"
