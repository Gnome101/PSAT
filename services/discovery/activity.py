"""On-chain activity scoring for contract inventory ranking.

Fetches the most recent transaction timestamp from Etherscan for each
discovered contract and computes a half-life decay score.  Contracts that
are actively used rank higher, ensuring the analysis pipeline targets the
most relevant addresses first.

Scoring
-------
- ``activity_score = 1 / (1 + days_since_last_tx / HALF_LIFE)``
  with HALF_LIFE = 30 days.  A contract active today scores ~1.0;
  one inactive for 30 days scores 0.5; one inactive for a year scores ~0.08.
- When activity data is unavailable (unsupported chain, Etherscan error),
  the contract receives a neutral score of 0.5 so it is neither penalised
  nor boosted.

Blended ranking
---------------
``rank_score = confidence * 0.35 + activity_score * 0.65``

This keeps evidence-quality (confidence) as a factor while letting on-chain
activity dominate the ordering so the most-used contracts surface first.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from utils import etherscan

from .inventory_domain import CHAIN_SORT_ORDER, _debug_log

# Etherscan v2 chain IDs for chains the inventory pipeline can discover.
CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "optimism": 10,
    "polygon": 137,
    "base": 8453,
    "avalanche": 43114,
    "bsc": 56,
    "linea": 59144,
    "scroll": 534352,
    "zksync": 324,
    "blast": 81457,
}

# Half-life in days for the activity decay function.
_HALF_LIFE_DAYS = 30

# Score assigned when activity data is unavailable (e.g. unsupported chain).
_NEUTRAL_SCORE = 0.5

# Etherscan free-tier limit is 3 req/s.  Use 2 to stay safely under
# (the rate limiter + thread scheduling can burst slightly above target).
_MAX_REQUESTS_PER_SECOND = 2

# Blended ranking weights.
_W_CONFIDENCE = 0.35
_W_ACTIVITY = 0.65


class _RateLimiter:
    """Thread-safe rate limiter enforcing a minimum interval between calls."""

    def __init__(self, calls_per_second: float):
        self._min_interval = 1.0 / calls_per_second
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fetch_last_active_ts(
    address: str,
    chain_id: int = 1,
    debug: bool = False,
) -> float | None:
    """Return the Unix timestamp of the most recent transaction, or None."""
    try:
        data = etherscan.get(
            "account",
            "txlist",
            chain_id=chain_id,
            address=address,
            startblock=0,
            endblock=99999999,
            page=1,
            offset=1,
            sort="desc",
        )
        results = data.get("result", [])
        if isinstance(results, list) and results:
            ts = results[0].get("timeStamp")
            if ts:
                return float(ts)
    except Exception as exc:
        _debug_log(debug, f"Activity fetch failed for {address}: {exc}")
    return None


def _activity_score(last_active_ts: float | None) -> float:
    """Compute an activity score in [0, 1] using half-life decay.

    Returns ``_NEUTRAL_SCORE`` when the timestamp is unknown so that
    contracts on unsupported chains are neither penalised nor boosted.
    """
    if last_active_ts is None:
        return _NEUTRAL_SCORE
    now = datetime.now(timezone.utc).timestamp()
    days_since = max(0.0, (now - last_active_ts)) / 86400
    return 1.0 / (1.0 + days_since / _HALF_LIFE_DAYS)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _enrich_one(
    contract: dict[str, Any],
    limiter: _RateLimiter,
    debug: bool = False,
) -> None:
    """Fetch activity for a single contract (thread-safe, rate-limited)."""
    address = contract["address"]
    chain = contract.get("chain", "unknown")
    chain_id = CHAIN_IDS.get(chain, CHAIN_IDS["ethereum"])

    limiter.wait()
    last_ts = _fetch_last_active_ts(address, chain_id=chain_id, debug=debug)
    score = _activity_score(last_ts)

    contract["activity"] = {
        "last_active": (
            datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat() if last_ts is not None else None
        ),
        "score": round(score, 4),
    }

    confidence = contract.get("confidence", 0.5)
    contract["rank_score"] = round(
        confidence * _W_CONFIDENCE + score * _W_ACTIVITY,
        4,
    )


def enrich_with_activity(
    contracts: list[dict[str, Any]],
    debug: bool = False,
) -> list[dict[str, Any]]:
    """Add activity metrics to each contract and re-sort by blended rank score.

    Mutates the contract dicts in-place (adds ``activity`` and ``rank_score``
    keys) and returns the list sorted by ``rank_score`` descending.

    Uses a thread pool to parallelize Etherscan calls while respecting
    the API rate limit via a shared ``_RateLimiter``.
    """
    if not contracts:
        return contracts

    _debug_log(debug, f"Fetching on-chain activity for {len(contracts)} contract(s)")

    limiter = _RateLimiter(_MAX_REQUESTS_PER_SECOND)
    workers = min(len(contracts), _MAX_REQUESTS_PER_SECOND)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_enrich_one, contract, limiter, debug): contract
            for contract in contracts
        }
        for future in as_completed(futures):
            exc = future.exception()
            if exc:
                contract = futures[future]
                _debug_log(debug, f"Activity enrichment failed for {contract['address']}: {exc}")

    _debug_log(debug, "Activity enrichment complete")

    contracts.sort(
        key=lambda c: (
            -c.get("rank_score", 0),
            -c.get("confidence", 0),
            c.get("name") is None,
            str(c.get("name") or ""),
            CHAIN_SORT_ORDER.get(c.get("chain", "unknown"), 50),
            c["address"],
        ),
    )

    return contracts
