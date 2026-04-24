"""Live read-path tests for unified-monitoring endpoints.

These are read-only smoke tests — the deployed preview's monitoring
rows depend on whatever's been enrolled over the preview's lifetime,
so we don't assert specific counts. What we DO assert is that the
endpoints return a well-formed list and every row has the shape the
frontend relies on.

Mutation endpoints (re-enroll, subscribe, patch) need richer fixtures
and are covered separately — or not at all yet. See the summary at end.
"""

from __future__ import annotations

from tests.live.conftest import LiveClient


def test_list_monitored_contracts_shape(live_client: LiveClient):
    rows = live_client.list_monitored_contracts()
    assert isinstance(rows, list)
    if not rows:
        # Empty preview is fine — just checking the endpoint returns a list.
        return
    for row in rows[:5]:  # spot-check the first few; full scan isn't cheap on a busy preview
        for key in ("id", "address", "chain", "contract_type", "is_active"):
            assert key in row, f"monitored contract row missing {key!r}: {row}"


def test_list_monitored_events_shape(live_client: LiveClient):
    rows = live_client.list_monitored_events(limit=20)
    assert isinstance(rows, list)
    for row in rows:
        for key in ("id", "monitored_contract_id", "event_type", "detected_at"):
            assert key in row, f"monitored event row missing {key!r}: {row}"


def test_list_proxy_events_shape(live_client: LiveClient):
    rows = live_client.list_proxy_events()
    assert isinstance(rows, list)
    for row in rows:
        for key in ("id", "watched_proxy_id", "block_number", "tx_hash", "new_implementation"):
            assert key in row, f"proxy event row missing {key!r}: {row}"
