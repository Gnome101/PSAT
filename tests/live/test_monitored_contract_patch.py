"""Live integration test for PATCH /api/monitored-contracts/{id}.

The watched-proxies POST endpoint creates a MonitoredContract row as a
side effect (api.py:2844-2857). We piggyback on the watched_proxies test
flow to acquire that row, PATCH it, and confirm the changes round-trip
through the listing endpoint.

Kept in its own file (rather than appended to test_watched_proxies.py)
so the proxy lifecycle tests don't share fixture state with the
mutation we're exercising here, and so this file owns its own teardown.
"""

from __future__ import annotations

from typing import Any

import pytest
import requests

from tests.live.conftest import LiveClient

# Distinct from the address used in test_watched_proxies.py so the two
# files can run concurrently without colliding on the (address, chain)
# unique key inside POST /api/watched-proxies.
USDC_PROXY = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"


@pytest.fixture(scope="module")
def watched_proxy_with_monitored_contract(live_client: LiveClient, request) -> dict[str, Any]:
    """Add USDC to the watch list and resolve the auto-created MonitoredContract row.

    Returns a dict with both the watched proxy and the looked-up
    monitored-contract row, since every test below needs both.
    """
    payload = {"address": USDC_PROXY, "chain": "ethereum", "label": "psat-live-test-patch"}
    proxy = live_client.add_watched_proxy(payload)
    assert proxy.get("id"), f"add_watched_proxy response missing id: {proxy}"

    # The MonitoredContract is created in the same transaction as the
    # WatchedProxy row, so a fresh listing immediately after the POST
    # should see it. Filter by chain to keep the scan cheap on busy
    # previews.
    rows = live_client.list_monitored_contracts(chain="ethereum")
    address = USDC_PROXY.lower()
    monitored = next((r for r in rows if (r.get("address") or "").lower() == address), None)
    assert monitored is not None, (
        f"watched proxy {proxy['id']} did not produce a MonitoredContract row for {address} in chain=ethereum"
    )

    def _cleanup():
        try:
            # Deleting the proxy SET-NULLs the MonitoredContract.watched_proxy_id
            # link (db/models.py:647-649) but leaves the row itself; that's fine
            # because the (address, chain) unique key means a re-run will find
            # the same row rather than accumulate duplicates. There is no
            # admin DELETE for MonitoredContract today.
            live_client.delete_watched_proxy(proxy["id"])
        except requests.HTTPError:
            pass

    request.addfinalizer(_cleanup)
    return {"proxy": proxy, "monitored": monitored}


def test_monitored_contract_initial_state(watched_proxy_with_monitored_contract):
    monitored = watched_proxy_with_monitored_contract["monitored"]
    # The watched-proxies endpoint sets these explicitly (api.py:2845-2856).
    assert monitored["is_active"] is True
    assert monitored["enrollment_source"] == "proxy_watch"
    assert isinstance(monitored.get("monitoring_config"), dict)


def test_monitored_contract_patch_monitoring_config(
    watched_proxy_with_monitored_contract,
    live_client: LiveClient,
):
    monitored = watched_proxy_with_monitored_contract["monitored"]
    new_config = {"watch_upgrades": False, "watch_ownership": True, "extra_flag": "test-marker"}
    updated = live_client.patch_monitored_contract(monitored["id"], {"monitoring_config": new_config})
    assert updated["monitoring_config"] == new_config

    # Read back via the listing to confirm persistence (vs. the PATCH handler
    # echoing the request without committing).
    rows = live_client.list_monitored_contracts(chain="ethereum")
    persisted = next((r for r in rows if r.get("id") == monitored["id"]), None)
    assert persisted is not None, f"PATCH'd row {monitored['id']} disappeared from listing"
    assert persisted["monitoring_config"] == new_config


def test_monitored_contract_patch_toggle_active(
    watched_proxy_with_monitored_contract,
    live_client: LiveClient,
):
    monitored = watched_proxy_with_monitored_contract["monitored"]
    # Toggle off, then back on, so the fixture's other tests aren't observing
    # an inactive row by accident.
    off = live_client.patch_monitored_contract(monitored["id"], {"is_active": False})
    assert off["is_active"] is False

    on = live_client.patch_monitored_contract(monitored["id"], {"is_active": True})
    assert on["is_active"] is True


def test_monitored_contract_patch_needs_polling(
    watched_proxy_with_monitored_contract,
    live_client: LiveClient,
):
    monitored = watched_proxy_with_monitored_contract["monitored"]
    original = monitored.get("needs_polling", False)
    flipped = live_client.patch_monitored_contract(monitored["id"], {"needs_polling": not original})
    assert flipped["needs_polling"] is (not original)
    # Restore so we don't leave the row in a weird state for the next test in the module.
    live_client.patch_monitored_contract(monitored["id"], {"needs_polling": original})


def test_monitored_contract_patch_unknown_id_404(live_client: LiveClient):
    # The handler does uuid.UUID(contract_id), so we need a valid UUID format
    # that just isn't in the DB — otherwise it 422s on the cast.
    missing_uuid = "00000000-0000-0000-0000-000000000000"
    r = live_client._session.patch(
        live_client._url(f"/api/monitored-contracts/{missing_uuid}"),
        json={"is_active": False},
        timeout=15,
    )
    assert r.status_code == 404, f"unknown contract id should 404, got {r.status_code}: {r.text[:200]}"
