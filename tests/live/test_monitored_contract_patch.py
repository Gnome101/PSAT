"""PATCH /api/monitored-contracts/{id}. Row is auto-created by POST /api/watched-proxies (api.py:2844-2857)."""

from __future__ import annotations

from typing import Any

import pytest
import requests

from tests.live.conftest import LiveClient

# Distinct from test_watched_proxies.py so the two can run concurrently without (address, chain) collision.
USDC_PROXY = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"


@pytest.fixture(scope="module")
def watched_proxy_with_monitored_contract(live_client: LiveClient, request) -> dict[str, Any]:
    """Add USDC + resolve the auto-created MonitoredContract row."""
    payload = {"address": USDC_PROXY, "chain": "ethereum", "label": "psat-live-test-patch"}
    proxy = live_client.add_watched_proxy(payload)
    assert proxy.get("id"), f"add_watched_proxy response missing id: {proxy}"

    rows = live_client.list_monitored_contracts(chain="ethereum")
    address = USDC_PROXY.lower()
    monitored = next((r for r in rows if (r.get("address") or "").lower() == address), None)
    assert monitored is not None, (
        f"watched proxy {proxy['id']} did not produce a MonitoredContract row for {address} in chain=ethereum"
    )

    def _cleanup():
        # Delete SET-NULLs MonitoredContract.watched_proxy_id (db/models.py:647-649); the row
        # persists but (address, chain) uniqueness prevents duplicates on re-run.
        try:
            live_client.delete_watched_proxy(proxy["id"])
        except requests.HTTPError:
            pass

    request.addfinalizer(_cleanup)
    return {"proxy": proxy, "monitored": monitored}


def test_monitored_contract_initial_state(watched_proxy_with_monitored_contract):
    # Defaults set by POST /api/watched-proxies (api.py:2845-2856).
    monitored = watched_proxy_with_monitored_contract["monitored"]
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

    # Read-back via listing guards against a PATCH that echoes without committing.
    rows = live_client.list_monitored_contracts(chain="ethereum")
    persisted = next((r for r in rows if r.get("id") == monitored["id"]), None)
    assert persisted is not None, f"PATCH'd row {monitored['id']} disappeared from listing"
    assert persisted["monitoring_config"] == new_config


def test_monitored_contract_patch_toggle_active(
    watched_proxy_with_monitored_contract,
    live_client: LiveClient,
):
    # Toggle off then back on so later tests don't observe an inactive row.
    monitored = watched_proxy_with_monitored_contract["monitored"]
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
    live_client.patch_monitored_contract(monitored["id"], {"needs_polling": original})


def test_monitored_contract_patch_unknown_id_404(live_client: LiveClient):
    # Valid UUID format (handler casts via uuid.UUID — bad format would 422, not 404).
    missing_uuid = "00000000-0000-0000-0000-000000000000"
    r = live_client._session.patch(
        live_client._url(f"/api/monitored-contracts/{missing_uuid}"),
        json={"is_active": False},
        timeout=15,
    )
    assert r.status_code == 404, f"unknown contract id should 404, got {r.status_code}: {r.text[:200]}"
