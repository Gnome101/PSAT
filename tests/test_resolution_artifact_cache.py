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
    # _materialize_contract_artifacts now calls utils.rpc.get_code_with_keccak
    # to populate the bytecode-keccak secondary cache index. Stub it so
    # tests don't make real eth_getCode RPCs (was making each test ~20s
    # before this stub).
    monkeypatch.setattr(
        "utils.rpc.get_code_with_keccak",
        lambda _rpc, _addr: ("0x60", "0x" + "ab" * 32),
    )


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


# ---------------------------------------------------------------------------
# Phase B Step 2: bytecode keccak secondary index for cross-cascade reuse
# ---------------------------------------------------------------------------


def test_keccak_secondary_index_hits_for_different_address_same_bytecode():
    """The whole point of step 2: two contracts deployed at DIFFERENT
    addresses but with the SAME impl bytecode (every OZ ERC1967Proxy
    instance, every standard Gnosis Safe singleton) share the cached
    static analysis since contract_analysis is purely a function of
    source code, not address."""
    keccak = "0x" + "11" * 32
    _store_cached_static_artifacts(
        "0x" + "AA" * 20,
        "SharedImpl",
        {"functions": [{"sig": "f()"}]},
        {"controllers": []},
        bytecode_keccak=keccak,
    )
    # Different address, SAME keccak → cache hit.
    cached = _get_cached_static_artifacts("0x" + "BB" * 20, bytecode_keccak=keccak)
    assert cached is not None
    name, analysis, plan = cached
    assert name == "SharedImpl"
    assert analysis["functions"][0]["sig"] == "f()"


def test_keccak_secondary_index_misses_when_keccak_differs():
    """Different bytecode → not the same impl → must NOT hit the cache.
    Catches a refactor that conflates the two indices."""
    _store_cached_static_artifacts(
        "0x" + "AA" * 20,
        "ImplA",
        {"v": 1},
        {"v": 1},
        bytecode_keccak="0x" + "11" * 32,
    )
    cached = _get_cached_static_artifacts("0x" + "CC" * 20, bytecode_keccak="0x" + "22" * 32)
    assert cached is None


def test_address_index_preferred_over_keccak_index():
    """When both indices could match (same address, but the keccak
    points to a DIFFERENT cached entry from another store), the
    address-keyed entry wins. Address is the more-specific match."""
    addr = "0x" + "AA" * 20
    # First store: address AA, keccak ZZ → both indices point to "v1"
    _store_cached_static_artifacts(addr, "v1-name", {"v": 1}, {}, bytecode_keccak="0x" + "ZZ".replace("Z", "z") * 32)
    # Second store: a totally different address, keccak XX → installed in keccak index too
    _store_cached_static_artifacts(
        "0x" + "DD" * 20, "v2-name", {"v": 2}, {}, bytecode_keccak="0x" + "XX".lower() * 32
    )
    # Lookup by addr AA + keccak XX should still get the v1 (address wins).
    cached = _get_cached_static_artifacts(addr, bytecode_keccak="0x" + "XX".lower() * 32)
    assert cached is not None
    name, _a, _p = cached
    assert name == "v1-name", "address-key match must take precedence over keccak fallback"


def test_keccak_index_respects_ttl(monkeypatch):
    """TTL applies to keccak-keyed lookups too — eventually re-fetch."""
    real_monotonic = time.monotonic
    fake_now = [real_monotonic()]
    monkeypatch.setattr(recursive._time, "monotonic", lambda: fake_now[0])

    keccak = "0x" + "ee" * 32
    _store_cached_static_artifacts(
        "0x" + "AA" * 20, "ImplE", {"v": 1}, {}, bytecode_keccak=keccak
    )
    fake_now[0] += recursive._ARTIFACT_CACHE_TTL_S + 1
    # Keccak hit on a DIFFERENT address, after TTL → must miss.
    assert _get_cached_static_artifacts("0x" + "BB" * 20, bytecode_keccak=keccak) is None


def test_clear_cache_clears_both_indices():
    """clear_artifact_cache must wipe the keccak index too — otherwise
    test isolation breaks (a stale keccak entry survives the helper)."""
    _store_cached_static_artifacts(
        "0x" + "AA" * 20, "Test", {}, {}, bytecode_keccak="0x" + "ff" * 32
    )
    recursive.clear_artifact_cache()
    assert _get_cached_static_artifacts("0x" + "AA" * 20) is None
    assert _get_cached_static_artifacts("0x" + "BB" * 20, bytecode_keccak="0x" + "ff" * 32) is None


# ---------------------------------------------------------------------------
# Codex iter-4 P1: bytecode-keccak hit must retarget plan to the new address
# ---------------------------------------------------------------------------


def test_bytecode_keccak_hit_retargets_plan_to_new_address(monkeypatch):
    """Codex iter-4 P1: when a keccak-index hit returns analysis+plan
    cached for a DIFFERENT address with the same bytecode (e.g., two
    UUPSProxy instances pointing to different impls), the cached
    plan["contract_address"] points at the FIRST address. Without
    retargeting, build_control_snapshot reads controller state from
    the wrong contract storage.

    Fix: on cache hit, deepcopy the analysis+plan and overwrite
    contract_address with the address THIS call is materializing.
    Verify that two materializations of the same-bytecode-different-
    address pair both end up reading from the right contract."""
    snapshot_calls: list[Any] = []
    scaffold_calls: list[Any] = []
    collect_calls: list[Any] = []
    _patch_pipeline(
        monkeypatch,
        scaffold_calls=scaffold_calls,
        collect_calls=collect_calls,
        snapshot_calls=snapshot_calls,
    )

    # Both addresses share the same bytecode → same keccak.
    keccak = "0x" + "ab" * 32
    monkeypatch.setattr("utils.rpc.get_code_with_keccak", lambda _rpc, _addr: ("0x60", keccak))

    addr_a = "0x" + "11" * 20
    addr_b = "0x" + "22" * 20

    _materialize_contract_artifacts(addr_a, "http://rpc", workspace_prefix="test")
    _materialize_contract_artifacts(addr_b, "http://rpc", workspace_prefix="test")

    assert len(snapshot_calls) == 2
    # First call is a cache MISS — uses the test fixture's _build_plan
    # output (hardcoded "0xabc"). Not under test here; the retarget
    # only fires on cache HIT.
    # Second call is a cache HIT via the keccak index — must retarget
    # plan["contract_address"] from "0xabc" to addr_b. Without the fix,
    # build_control_snapshot would read controller state from the
    # cache-populating contract instead of addr_b.
    assert snapshot_calls[1]["contract_address"] == addr_b.lower()
