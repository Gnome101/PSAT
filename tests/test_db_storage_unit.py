"""Unit tests for ``db.storage`` that don't touch a real bucket.

The integration suite (``test_artifact_storage_integration.py``) covers the
boto3 wire path. These tests pin the in-memory contract that the API layer
relies on — specifically, ``get_many`` must surface per-key transport
failures as ``None`` rather than raising, so a flaky bucket can't take down
``/api/analyses`` or ``/api/jobs/{id}/stage_timings``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.storage import StorageClient, StorageKeyMissing, StorageUnavailable  # noqa: E402


def _bare_client() -> StorageClient:
    """Build a StorageClient without going through ``__init__`` (which
    would try to talk to boto3). The methods we exercise only touch
    ``self.get`` / ``self.bucket`` — both stubbed in the tests."""
    client = StorageClient.__new__(StorageClient)
    client.bucket = "test-bucket"
    client._client = MagicMock()
    return client


def test_get_many_returns_none_for_missing_key() -> None:
    client = _bare_client()
    with patch.object(client, "get", side_effect=lambda k: (_ for _ in ()).throw(StorageKeyMissing(k))):
        result = client.get_many(["a", "b"])
    assert result == {"a": None, "b": None}


def test_get_many_swallows_transport_errors_per_key() -> None:
    """A transport failure on one key must not raise out of get_many — the
    failed key returns ``None`` and successful keys still resolve. Pre-fix
    this raised ``StorageUnavailable`` and api.py callers had to wrap it in
    try/except, with one of the two callers forgetting (``stage_timings``).
    Pinning the contract here means a future change can't quietly regress.
    """
    client = _bare_client()

    def fake_get(k: str) -> bytes:
        if k == "boom":
            raise StorageUnavailable("simulated transport failure")
        return f"body-{k}".encode()

    with patch.object(client, "get", side_effect=fake_get):
        result = client.get_many(["ok1", "boom", "ok2"])

    assert result == {
        "ok1": b"body-ok1",
        "boom": None,
        "ok2": b"body-ok2",
    }


def test_get_many_total_outage_returns_all_none() -> None:
    """Belt-and-suspenders: if every key fails, callers see all-None and
    can choose how to surface that. The function must not raise."""
    client = _bare_client()
    with patch.object(client, "get", side_effect=StorageUnavailable("bucket gone")):
        result = client.get_many(["a", "b", "c"])
    assert result == {"a": None, "b": None, "c": None}


def test_get_many_dedupes_input_keys() -> None:
    client = _bare_client()
    calls: list[str] = []

    def fake_get(k: str) -> bytes:
        calls.append(k)
        return b"x"

    with patch.object(client, "get", side_effect=fake_get):
        result = client.get_many(["a", "a", "b", "a"])

    assert result == {"a": b"x", "b": b"x"}
    assert sorted(calls) == ["a", "b"]


def test_get_many_empty_input_no_pool_spawned() -> None:
    """Cheap shortcut: empty input must not spin up a thread pool."""
    client = _bare_client()
    with patch("db.storage.ThreadPoolExecutor") as mock_pool:
        result = client.get_many([])
    assert result == {}
    mock_pool.assert_not_called()
