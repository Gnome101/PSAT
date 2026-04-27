"""Regression tests for the Postgres-backed Etherscan cache layer in
``utils.etherscan``.

Phase B Step 5. The in-memory cache (``_cache``) was per-process; this
adds a Postgres-backed layer (``etherscan_cache`` table) so worker
processes share hits across the fleet — a cold cascade on worker A
that fetched WETH source code populates the cache for worker B's
sibling cascade later.

What we pin:
1. params hashing: same equivalence class as in-memory _cache_key
2. PG-cache disabled (env flag off) → no DB calls attempted
3. PG-cache enabled, DB unavailable → graceful degradation (no crash,
   no exception bubbling)
4. PG-cache hit → returned without calling Etherscan AND populated
   into the in-memory layer (so the second call in same process is
   free again)
5. PG-cache miss → Etherscan called, response cached in BOTH layers

Mocks at the module-level boundary (db.models.SessionLocal,
requests.get for Etherscan) so no real DB or network access.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils import etherscan


@pytest.fixture(autouse=True)
def _isolated_inmem_cache():
    etherscan._cache.clear()
    yield
    etherscan._cache.clear()


def _stable_etherscan_response_mock(payload: dict):
    """Build a mock for `requests.get` returning a successful Etherscan envelope."""
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status.return_value = None
    resp.json.return_value = payload
    return resp


def test_params_hash_is_stable_for_same_inputs():
    """Hash must be deterministic — same module/action/chain_id/params
    in any param order yields the same hash."""
    h1 = etherscan._params_hash("contract", "getsourcecode", 1, {"address": "0xabc", "extra": "x"})
    h2 = etherscan._params_hash("contract", "getsourcecode", 1, {"extra": "x", "address": "0xabc"})
    assert h1 == h2
    assert len(h1) == 64  # sha256 hex


def test_params_hash_changes_with_params():
    """Different params must yield different hashes (collision rejection)."""
    h1 = etherscan._params_hash("contract", "getsourcecode", 1, {"address": "0xa"})
    h2 = etherscan._params_hash("contract", "getsourcecode", 1, {"address": "0xb"})
    assert h1 != h2


def test_pg_cache_disabled_skips_db(monkeypatch):
    """ETHERSCAN_PG_CACHE=0 → _pg_cache_get must NOT attempt to import
    db.models / open a session. CLI tooling that doesn't have a DB
    must keep working."""
    monkeypatch.setattr(etherscan, "_PG_CACHE_ENABLED", False)
    # If a DB call were attempted this would fail; instead we expect None.
    result = etherscan._pg_cache_get("contract", "getsourcecode", 1, {"address": "0xa"})
    assert result is None


def test_pg_cache_get_returns_none_on_db_unavailable(monkeypatch):
    """Graceful degradation: importing/using db.models can fail (DB not
    configured, connection refused). Must not crash; just return None."""
    monkeypatch.setattr(etherscan, "_PG_CACHE_ENABLED", True)

    def _raise_session(*_a, **_kw):
        raise RuntimeError("DB connection refused")

    # Patch the lazy-imported SessionLocal to blow up on use.
    with patch.dict("sys.modules", {"db.models": MagicMock(SessionLocal=_raise_session)}):
        result = etherscan._pg_cache_get("contract", "getsourcecode", 1, {"address": "0xa"})
    assert result is None


def test_pg_cache_get_hit_promotes_to_in_memory(monkeypatch):
    """A PG-cache hit must populate the in-memory cache too — second
    call in the same process should be free."""
    monkeypatch.setattr(etherscan, "_PG_CACHE_ENABLED", True)
    monkeypatch.setattr(etherscan, "_CACHE_ENABLED", True)
    cached_response = {"status": "1", "result": "from-pg"}
    monkeypatch.setattr(etherscan, "_pg_cache_get", lambda *a, **kw: cached_response)
    monkeypatch.setattr(etherscan, "_pg_cache_put", lambda *a, **kw: None)

    # If we hit Etherscan, fail loudly.
    monkeypatch.setattr(
        etherscan, "requests", MagicMock(get=MagicMock(side_effect=AssertionError("must not call Etherscan")))
    )
    monkeypatch.setattr(etherscan, "_get_api_key", lambda: "fake")

    result = etherscan.get("contract", "getsourcecode", 1, address="0xabc")
    assert result == cached_response

    # In-memory cache now populated — second call doesn't even hit PG.
    def _no_pg(*_a, **_kw):
        raise AssertionError("PG hit must promote to in-memory; second call must short-circuit")

    monkeypatch.setattr(etherscan, "_pg_cache_get", _no_pg)
    second = etherscan.get("contract", "getsourcecode", 1, address="0xabc")
    assert second == cached_response


def test_pg_cache_miss_calls_etherscan_then_writes_back(monkeypatch):
    """PG-cache miss → Etherscan is hit → response is written to BOTH
    in-memory and PG. Verify the write-back fires."""
    monkeypatch.setattr(etherscan, "_PG_CACHE_ENABLED", True)
    monkeypatch.setattr(etherscan, "_CACHE_ENABLED", True)
    monkeypatch.setattr(etherscan, "_pg_cache_get", lambda *a, **kw: None)

    pg_writes: list[dict] = []

    def _track_put(_m, _a, _c, _p, response):
        pg_writes.append(response)

    monkeypatch.setattr(etherscan, "_pg_cache_put", _track_put)
    monkeypatch.setattr(etherscan, "_get_api_key", lambda: "fake-key")
    monkeypatch.setattr(etherscan, "_wait_rate_limit", lambda: None)

    etherscan_response = {"status": "1", "result": "from-etherscan"}
    fake_resp = _stable_etherscan_response_mock(etherscan_response)
    monkeypatch.setattr(etherscan, "requests", MagicMock(get=MagicMock(return_value=fake_resp)))

    result = etherscan.get("contract", "getsourcecode", 1, address="0xdef")
    assert result == etherscan_response
    assert len(pg_writes) == 1, "successful Etherscan response must be written to PG cache"
    assert pg_writes[0] == etherscan_response


def test_pg_cache_put_swallows_db_errors(monkeypatch):
    """Best-effort write: DB errors during _pg_cache_put must NOT
    propagate (the in-memory cache + caller's retry loop are the safety
    net). A flaky cache write should never fail an otherwise-successful
    Etherscan call."""
    monkeypatch.setattr(etherscan, "_PG_CACHE_ENABLED", True)

    def _raise_session(*_a, **_kw):
        raise RuntimeError("DB write timeout")

    with patch.dict("sys.modules", {"db.models": MagicMock(SessionLocal=_raise_session)}):
        # Should not raise.
        etherscan._pg_cache_put("contract", "getsourcecode", 1, {"address": "0xa"}, {"status": "1"})


# ---------------------------------------------------------------------------
# Codex iter-4 P1: PG cache whitelist gates non-immutable actions
# ---------------------------------------------------------------------------


def test_pg_cache_skips_non_whitelisted_actions(monkeypatch):
    """Codex iter-4 P1: with PG cache enabled, dynamic Etherscan actions
    (account/balance, stats/ethprice, etc.) MUST NOT be persisted —
    after the first balance lookup, every worker would see that stale
    value forever. Whitelist gates which actions get the PG layer."""
    monkeypatch.setattr(etherscan, "_PG_CACHE_ENABLED", True)
    # account/balance is intentionally NOT in _PG_CACHE_WHITELIST.
    assert etherscan._pg_cache_eligible("account", "balance") is False
    assert etherscan._pg_cache_eligible("stats", "ethprice") is False

    # _pg_cache_get must short-circuit (return None) for non-whitelisted
    # without ever reaching the DB.
    def _no_db(*_a, **_kw):
        raise AssertionError("non-whitelisted action must not touch DB")

    with patch.dict("sys.modules", {"db.models": MagicMock(SessionLocal=_no_db)}):
        result = etherscan._pg_cache_get("account", "balance", 1, {"address": "0xa"})
    assert result is None

    # Same for puts.
    with patch.dict("sys.modules", {"db.models": MagicMock(SessionLocal=_no_db)}):
        etherscan._pg_cache_put("account", "balance", 1, {"address": "0xa"}, {"status": "1"})


def test_pg_cache_whitelisted_actions_pass_through(monkeypatch):
    """Whitelisted actions (contract/getsourcecode + adjacent immutable
    contract metadata) DO go to the DB. Without this the entire PG
    layer is dead code."""
    assert etherscan._pg_cache_eligible("contract", "getsourcecode") is True
    assert etherscan._pg_cache_eligible("contract", "getabi") is True
    assert etherscan._pg_cache_eligible("contract", "getcontractcreation") is True
