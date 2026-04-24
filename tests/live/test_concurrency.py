"""Live integration test for concurrent submissions.

The worker pool, Postgres connection pool, and shared object-storage
backend should handle several simultaneous /api/analyze submissions
without anything wedging on lock contention or DB-pool exhaustion.

Each submission gets its own ``LiveClient`` (and therefore its own
``requests.Session``) because requests.Session is documented as
thread-unsafe for concurrent use across requests — the handoff calls
this out explicitly.
"""

from __future__ import annotations

import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from tests.live.conftest import DEFAULT_SINGLE_TIMEOUT, LiveClient

# Five well-known stable mainnet contracts. Mix of proxy (USDC) and non-
# proxy (WETH/DAI/LINK/UNI) so the worker pool exercises both code paths
# concurrently. All cached aggressively on Etherscan so cold runs aren't
# punitively slow on a CI preview.
PARALLEL_ADDRESSES = [
    "0xC02aaA39b223FE8D0A0e5c4F27eAD9083C756Cc2",  # WETH
    "0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48",  # USDC (proxy — also exercises impl spawn)
    "0x6B175474E89094C44Da98b954EedeAC495271d0F",  # DAI
    "0x514910771AF9Ca656af840dff83E8264EcF986CA",  # LINK
    "0x1f9840a85d5aF5bf1D1762F925BdAdcc4201F984",  # UNI
]


def _submit_and_wait(base_url: str, admin_key: str, address: str) -> dict:
    # Fresh client per thread — see file docstring.
    client = LiveClient(base_url, admin_key)
    return client.submit_and_wait(address, timeout=DEFAULT_SINGLE_TIMEOUT)


def test_concurrent_analyses_all_complete(live_base_url: str, live_admin_key: str):
    """Submit five analyses simultaneously; assert every one reaches
    ``completed`` without DB-contention errors leaking into job.error."""
    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=len(PARALLEL_ADDRESSES)) as pool:
        futures = {
            pool.submit(_submit_and_wait, live_base_url, live_admin_key, addr): addr for addr in PARALLEL_ADDRESSES
        }
        for fut in as_completed(futures):
            addr = futures[fut]
            results[addr] = fut.result()

    failed = {addr: r for addr, r in results.items() if r["status"] != "completed"}
    assert not failed, "concurrent submissions did not all complete: " + "; ".join(
        f"{a}: status={r['status']} error={(r.get('error') or '')[:120]}" for a, r in failed.items()
    )


def test_concurrent_analyses_balanced_runtimes(live_base_url: str, live_admin_key: str):
    """No single concurrent run should take >2.5x the median of the rest.

    Rough worker-saturation check: if the pool is undersized, one job
    waits on every other to finish before its own discovery worker
    claims it, blowing its runtime well above the median.

    Soft margin (2.5x not 2x) because scaffolding + slither has
    legitimate per-contract variance — USDC's impl chain is meaningfully
    more work than DAI's flat source — so a tighter ratio would flap.
    """
    durations: dict[str, float] = {}
    with ThreadPoolExecutor(max_workers=len(PARALLEL_ADDRESSES)) as pool:
        futures = {
            pool.submit(_submit_and_wait, live_base_url, live_admin_key, addr): addr for addr in PARALLEL_ADDRESSES
        }
        for fut in as_completed(futures):
            addr = futures[fut]
            job = fut.result()
            if job["status"] == "completed":
                durations[addr] = LiveClient.job_duration_seconds(job)

    if len(durations) < 3:
        pytest.skip(f"need at least 3 successful runs to compute a balanced-runtime check, got {len(durations)}")

    median = statistics.median(durations.values())
    # Floor on tiny medians: when every run was cached and finished in
    # <10s, ratios stop being meaningful (a 2-second run vs a 20-second
    # run is a 10x ratio that says nothing about pool saturation).
    if median < 10:
        pytest.skip(f"median runtime {median:.1f}s too short for a saturation check")

    outliers = {a: d for a, d in durations.items() if d > median * 2.5}
    assert not outliers, (
        f"concurrent runtime imbalance — median={median:.1f}s, "
        f"outliers={ {a: round(d, 1) for a, d in outliers.items()} }"
    )
