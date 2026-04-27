"""Regression tests for the process-wide static-artifact cache in
``services/resolution/recursive.py``.

Within a single cascade the BFS already dedupes by address (``processed``
set). This cache exists for *cross-cascade* reuse: when a sibling job
walks the same OZ library / common implementation, we skip the
scaffold + ``collect_contract_analysis`` + ``build_control_tracking_plan``
trio.

What we pin here:
1. Static artifacts (analysis + plan) are cached by effective_address
   and reused on the second call → only one scaffold run.
2. Snapshot + permissions are rebuilt fresh on every call (they depend
   on RPC state via build_control_snapshot) → never served stale.
3. Cache returns deepcopies — mutating the returned dict must NOT
   poison the next call.
4. TTL expiry: an entry past PSAT_RESOLUTION_ARTIFACT_CACHE_TTL_S is
   re-fetched.
5. skip_slither=False bypasses the cache (Slither CLI writes
   side-effect artifacts the cached path doesn't).
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution import recursive
from services.resolution.recursive import (
    _get_cached_static_artifacts,
    _materialize_contract_artifacts,
    _store_cached_static_artifacts,
    clear_artifact_cache,
)


@pytest.fixture(autouse=True)
def _isolated_cache():
    clear_artifact_cache()
    yield
    clear_artifact_cache()


def _patch_pipeline(monkeypatch, *, scaffold_calls, collect_calls, snapshot_calls):
    """Wire up the dependency chain with counters so we can assert which
    layers got skipped on cache hit."""

    def _classify(_addr, _rpc):
        return {"type": "contract"}

    def _fetch(_addr):
        return {"ContractName": "TestContract", "SourceCode": "// stub"}

    def _scaffold(_addr, _result, project_dir):
        scaffold_calls.append(project_dir)
        project_dir.mkdir(parents=True, exist_ok=True)
        return project_dir

    def _collect(_project_dir):
        collect_calls.append(_project_dir)
        return {
            "subject": {"address": "0xabc", "name": "TestContract"},
            "functions": [],
            "state_vars": [],
        }

    def _build_plan(_analysis):
        return {"contract_address": "0xabc", "controllers": []}

    def _build_snapshot(_plan, _rpc_url):
        snapshot_calls.append(_plan)
        return {"controllers": []}

    def _build_perms(_analysis, _snapshot):
        return None

    monkeypatch.setattr("services.discovery.classifier.classify_single", _classify)
    monkeypatch.setattr(recursive, "fetch", _fetch)
    monkeypatch.setattr(recursive, "scaffold", _scaffold)
    monkeypatch.setattr(recursive, "collect_contract_analysis", _collect)
    monkeypatch.setattr(recursive, "build_control_tracking_plan", _build_plan)
    monkeypatch.setattr(recursive, "build_control_snapshot", _build_snapshot)
    monkeypatch.setattr(recursive, "_build_effective_permissions", _build_perms)


def test_second_call_serves_static_artifacts_from_cache(monkeypatch):
    """The whole point: scaffold + collect run once; second call hits cache."""
    scaffold_calls: list[Any] = []
    collect_calls: list[Any] = []
    snapshot_calls: list[Any] = []
    _patch_pipeline(
        monkeypatch,
        scaffold_calls=scaffold_calls,
        collect_calls=collect_calls,
        snapshot_calls=snapshot_calls,
    )

    _materialize_contract_artifacts("0xABC", "http://rpc", workspace_prefix="test")
    _materialize_contract_artifacts("0xABC", "http://rpc", workspace_prefix="test")

    assert len(scaffold_calls) == 1, "second call should skip scaffold"
    assert len(collect_calls) == 1, "second call should skip collect_contract_analysis"


def test_snapshot_always_rebuilt(monkeypatch):
    """Snapshot reads on-chain state — must not be cached. If a future
    refactor extends the cache to cover snapshot, this test catches it."""
    scaffold_calls: list[Any] = []
    collect_calls: list[Any] = []
    snapshot_calls: list[Any] = []
    _patch_pipeline(
        monkeypatch,
        scaffold_calls=scaffold_calls,
        collect_calls=collect_calls,
        snapshot_calls=snapshot_calls,
    )

    _materialize_contract_artifacts("0xABC", "http://rpc", workspace_prefix="test")
    _materialize_contract_artifacts("0xABC", "http://rpc", workspace_prefix="test")
    _materialize_contract_artifacts("0xABC", "http://rpc", workspace_prefix="test")

    assert len(snapshot_calls) == 3, "snapshot must be built every call (state-dependent)"


def test_cached_artifacts_are_deep_copied(monkeypatch):
    """Mutating returned dicts (callers add fields, e.g. contract_address
    override for proxies) must not poison the next cache lookup."""
    scaffold_calls: list[Any] = []
    collect_calls: list[Any] = []
    snapshot_calls: list[Any] = []
    _patch_pipeline(
        monkeypatch,
        scaffold_calls=scaffold_calls,
        collect_calls=collect_calls,
        snapshot_calls=snapshot_calls,
    )

    first = _materialize_contract_artifacts("0xABC", "http://rpc", workspace_prefix="test")
    first["analysis"]["functions"].append({"poisoned": True})  # type: ignore[attr-defined]
    first["tracking_plan"]["controllers"].append({"poisoned": True})

    second = _materialize_contract_artifacts("0xABC", "http://rpc", workspace_prefix="test")
    assert second["analysis"]["functions"] == []
    assert second["tracking_plan"]["controllers"] == []


def test_cache_ttl_expiry(monkeypatch):
    """An entry older than the TTL must be re-fetched. We bypass real
    time by patching the monotonic clock used by the cache."""
    scaffold_calls: list[Any] = []
    collect_calls: list[Any] = []
    snapshot_calls: list[Any] = []
    _patch_pipeline(
        monkeypatch,
        scaffold_calls=scaffold_calls,
        collect_calls=collect_calls,
        snapshot_calls=snapshot_calls,
    )

    real_monotonic = time.monotonic
    fake_now = [real_monotonic()]
    monkeypatch.setattr(recursive._time, "monotonic", lambda: fake_now[0])

    _materialize_contract_artifacts("0xABC", "http://rpc", workspace_prefix="test")
    fake_now[0] += recursive._ARTIFACT_CACHE_TTL_S + 1  # jump past TTL
    _materialize_contract_artifacts("0xABC", "http://rpc", workspace_prefix="test")

    assert len(scaffold_calls) == 2, "expired entry should be rebuilt"


def test_cache_keyed_by_effective_address_not_input(monkeypatch):
    """When two different proxies point to the same impl, the cache key
    is the impl address — both proxies share the cached static artifacts.
    This is the cross-cascade reuse we want."""
    scaffold_calls: list[Any] = []
    collect_calls: list[Any] = []
    snapshot_calls: list[Any] = []
    _patch_pipeline(
        monkeypatch,
        scaffold_calls=scaffold_calls,
        collect_calls=collect_calls,
        snapshot_calls=snapshot_calls,
    )

    impl_addr = "0x" + "11" * 20

    def _classify_proxy_to_impl(_addr, _rpc):
        return {"type": "proxy", "implementation": impl_addr}

    monkeypatch.setattr(
        "services.discovery.classifier.classify_single", _classify_proxy_to_impl
    )

    _materialize_contract_artifacts(
        "0x" + "AA" * 20, "http://rpc", workspace_prefix="test"
    )  # proxy A → impl
    _materialize_contract_artifacts(
        "0x" + "BB" * 20, "http://rpc", workspace_prefix="test"
    )  # proxy B → same impl

    assert len(scaffold_calls) == 1, "same impl must be scaffolded once even for different proxies"


def test_cache_helpers_round_trip():
    """Direct unit test for _get/_store helpers — guards the eviction
    + deepcopy contract independently of the BFS integration."""
    analysis = {"k": [1, 2, 3]}
    plan = {"controllers": []}
    _store_cached_static_artifacts("0xABC", "Test", analysis, plan)
    cached = _get_cached_static_artifacts("0xabc")  # case-insensitive
    assert cached is not None
    name, a, p = cached
    assert name == "Test"
    a["k"].append(99)  # mutate returned copy
    second = _get_cached_static_artifacts("0xabc")
    assert second is not None
    assert second[1]["k"] == [1, 2, 3], "store must deepcopy on read"
