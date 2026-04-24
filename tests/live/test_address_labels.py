"""Live integration tests for address-label CRUD.

Small, fast endpoint-triple — list/put/delete — that's load-bearing for
the UI's ability to show human names for Safe signers and EOA
principals. Asserts each verb in isolation so a single broken handler
doesn't mask the others.
"""

from __future__ import annotations

import pytest
import requests

from tests.live.conftest import LiveClient

# Burner-pattern address — unlikely to collide with any real label row
# on staging. Lowercase because the handler normalizes before upsert.
TEST_LABEL_ADDRESS = "0x000000000000000000000000000000000070f47e"
TEST_LABEL_NAME = "psat-live-test-label"


@pytest.fixture
def created_label(live_client: LiveClient, request):
    """PUT a label and schedule its deletion; fixture returns the PUT response."""
    row = live_client.put_address_label(TEST_LABEL_ADDRESS, {"name": TEST_LABEL_NAME, "note": "ephemeral"})

    def _cleanup():
        try:
            live_client.delete_address_label(TEST_LABEL_ADDRESS)
        except requests.HTTPError:
            pass

    request.addfinalizer(_cleanup)
    return row


def test_label_put_returns_normalized_row(created_label):
    assert created_label["address"].lower() == TEST_LABEL_ADDRESS.lower()
    assert created_label["name"] == TEST_LABEL_NAME
    assert created_label["note"] == "ephemeral"


def test_label_visible_in_list(created_label, live_client: LiveClient):
    listing = live_client.list_address_labels()
    labels = listing.get("labels") or {}
    # Handler normalizes addresses on both read + write, so looking up by
    # either case should hit the same row.
    row = labels.get(TEST_LABEL_ADDRESS.lower()) or labels.get(TEST_LABEL_ADDRESS)
    assert row is not None, f"created label not present in /api/address_labels list (got {len(labels)} rows)"
    assert row["name"] == TEST_LABEL_NAME


def test_label_update_is_idempotent(created_label, live_client: LiveClient):
    # Second PUT with a different name should overwrite, not 409.
    updated = live_client.put_address_label(TEST_LABEL_ADDRESS, {"name": "psat-live-test-renamed"})
    assert updated["name"] == "psat-live-test-renamed"
    # Read back via list to confirm the write landed.
    listing = live_client.list_address_labels()
    row = (listing.get("labels") or {}).get(TEST_LABEL_ADDRESS.lower())
    assert row is not None and row["name"] == "psat-live-test-renamed"


def test_label_delete(live_client: LiveClient):
    # Dedicated create-and-delete cycle so this test isn't entangled with
    # the shared fixture's finalizer.
    address = "0x00000000000000000000000000000000decafbad"
    live_client.put_address_label(address, {"name": "psat-live-test-delete"})
    live_client.delete_address_label(address)
    # Idempotent: a second delete should 404.
    r = live_client._session.delete(live_client._url(f"/api/address_labels/{address}"), timeout=15)
    assert r.status_code == 404, f"expected 404 on double-delete, got {r.status_code}"
