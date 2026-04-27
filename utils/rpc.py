"""Shared low-level helpers for JSON-RPC and EVM encoding."""

from __future__ import annotations

import os
import threading
import time
from typing import Any

import requests
from eth_utils.crypto import keccak
from requests.adapters import HTTPAdapter

JSON_RPC_TIMEOUT_SECONDS = 10

# Maximum calls per JSON-RPC batch (stay under provider limits)
MAX_BATCH_SIZE = 500

RETRYABLE_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}

# Process-wide cache for eth_getCode. Bytecode at a deployed address is
# effectively immutable for the lifetime of any single cascade — once
# the contract is deployed, only SELFDESTRUCT changes it (rare, and
# any subsequent code at that address has its own `eth_getCode` reading
# anyway). Caching saves the per-call RTT on repeated lookups, which
# happen heavily across stages: discovery probes code, resolution
# classifies (which probes code again), policy may re-classify, and
# any cascade that crosses the same shared OZ library / common impl
# repeats the same probe across jobs.
#
# Stored value is (bytecode_hex, keccak_hex, monotonic_insertion_time).
# keccak is computed lazily on first cache MISS (B10 in roadmap will
# key Slither result caching off it; computing once here avoids
# recomputing per-job).
#
# Manual cache (not lru_cache) so we can:
#   - skip caching when the underlying RPC raised (treats transient
#     failures as un-cacheable to avoid cementing a bad classification)
#   - apply a TTL (latest-block reads should eventually re-probe in
#     case of SELFDESTRUCT or proxy upgrade redirecting at this address)
_GETCODE_CACHE: dict[tuple[str, str], tuple[str, str, float]] = {}
_GETCODE_CACHE_LOCK = threading.Lock()
_GETCODE_CACHE_MAX = 8192
_GETCODE_CACHE_TTL_S = float(os.getenv("PSAT_GETCODE_CACHE_TTL_S", "1800"))


def clear_getcode_cache() -> None:
    """Clear the process-wide eth_getCode cache. For tests + manual reset."""
    with _GETCODE_CACHE_LOCK:
        _GETCODE_CACHE.clear()


def _normalized_addr(address: str) -> str:
    return address.lower() if address.startswith("0x") else "0x" + address.lower()

# Per-thread requests.Session so RPC calls reuse the underlying TCP/TLS
# connection. Bare ``requests.post()`` opens a new socket per call —
# that's a TCP handshake + TLS handshake (~50-200ms RTT each) on every
# eth_call, which dominates the cost of RPC-heavy stages like the
# resolution recursive walk.
#
# requests.Session is NOT thread-safe across calls, so we key by thread
# rather than sharing a global one. Workers also use threadpools for
# parallel cascades, so threading.local() is the safe primitive.
#
# HTTPAdapter pool sizes are intentionally generous — these are within
# one thread, so the pool only holds connections to the few RPC URLs
# this thread has hit recently.
_session_local = threading.local()


def _get_session() -> requests.Session:
    s = getattr(_session_local, "session", None)
    if s is None:
        s = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _session_local.session = s
    return s


def normalize_address(address: str) -> str:
    """Normalize an Ethereum address to lowercase with a single 0x prefix."""
    return "0x" + address.lower().replace("0x", "", 1)


def rpc_request(rpc_url: str, method: str, params: list[Any], retries: int = 1) -> Any:
    session = _get_session()
    for attempt in range(retries + 1):
        try:
            response = session.post(
                rpc_url,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=JSON_RPC_TIMEOUT_SECONDS,
                headers={"Content-Type": "application/json"},
            )
            if response.status_code in RETRYABLE_HTTP_CODES and attempt < retries:
                time.sleep(0.3 * (2**attempt))
                continue
            response.raise_for_status()
            payload = response.json()
            if payload.get("error"):
                raise RuntimeError(str(payload["error"]))
            return payload.get("result")
        except (requests.ConnectionError, requests.Timeout, OSError) as exc:
            if attempt < retries:
                time.sleep(0.3 * (2**attempt))
                continue
            raise RuntimeError(f"RPC request failed for {rpc_url}: {exc}") from exc
    raise RuntimeError(f"RPC request failed for {rpc_url}: all {retries + 1} attempts exhausted")


def get_code(rpc_url: str, address: str) -> str:
    """Fetch deployed EVM bytecode at an address via eth_getCode.

    Process-wide cached (TTL ``PSAT_GETCODE_CACHE_TTL_S``, default 30 min)
    so repeated probes of the same address across stages and jobs hit
    the cache instead of the wire. RPC errors are NOT cached — they
    propagate as ``RuntimeError`` so callers can decide retry behavior.
    """
    code, _keccak = get_code_with_keccak(rpc_url, address)
    return code


def get_code_with_keccak(rpc_url: str, address: str) -> tuple[str, str]:
    """Like ``get_code`` but also returns the keccak-256 of the bytecode.

    Returns ``(bytecode_hex, keccak_hex)``. Both are cached together so
    downstream content-addressed caches (Slither result by bytecode
    keccak, etc.) get the keccak for free without re-hashing per job.

    The keccak is computed once on cache miss; subsequent hits return
    the same string without re-hashing.
    """
    addr = _normalized_addr(address)
    key = (rpc_url, addr)
    now = time.monotonic()
    with _GETCODE_CACHE_LOCK:
        cached = _GETCODE_CACHE.get(key)
        if cached is not None:
            code, keccak_hex, inserted_at = cached
            if now - inserted_at < _GETCODE_CACHE_TTL_S:
                return code, keccak_hex
            # TTL expired; fall through to re-fetch.
            del _GETCODE_CACHE[key]

    # RPC call OUTSIDE the lock so concurrent misses for different
    # addresses don't serialize. A redundant fetch under contention
    # (two threads racing on the same address) is cheap; a held lock
    # across an HTTP roundtrip is not.
    raw = rpc_request(rpc_url, "eth_getCode", [address, "latest"])
    code = raw if isinstance(raw, str) and raw.startswith("0x") else "0x"
    # Hash the raw bytes (strip 0x prefix). Empty bytecode → keccak of empty string.
    code_bytes = bytes.fromhex(code[2:]) if len(code) >= 2 else b""
    keccak_hex = "0x" + keccak(code_bytes).hex()

    with _GETCODE_CACHE_LOCK:
        # Bound the cache: drop the oldest 25% when we hit the ceiling.
        # Better than random eviction for long-running workers because
        # the recently-probed addresses tend to recur.
        if len(_GETCODE_CACHE) >= _GETCODE_CACHE_MAX:
            cutoff = sorted(
                _GETCODE_CACHE.values(), key=lambda v: v[2]
            )[len(_GETCODE_CACHE) // 4][2]
            for k in [k for k, v in _GETCODE_CACHE.items() if v[2] <= cutoff]:
                _GETCODE_CACHE.pop(k, None)
        _GETCODE_CACHE[key] = (code, keccak_hex, now)
    return code, keccak_hex


def rpc_batch_request(rpc_url: str, calls: list[tuple[str, list[Any]]]) -> list[Any]:
    """Send a JSON-RPC batch request and return results in call order.

    Each element of *calls* is ``(method, params)``.  Returns a list of the
    same length where each position holds the ``result`` value from the
    response, or ``None`` if that individual call errored.
    """
    if not calls:
        return []

    results: list[Any] = [None] * len(calls)

    for chunk_start in range(0, len(calls), MAX_BATCH_SIZE):
        chunk = calls[chunk_start : chunk_start + MAX_BATCH_SIZE]
        batch = [
            {"jsonrpc": "2.0", "id": chunk_start + i, "method": method, "params": params}
            for i, (method, params) in enumerate(chunk)
        ]

        response = _get_session().post(
            rpc_url,
            json=batch,
            timeout=max(JSON_RPC_TIMEOUT_SECONDS, len(chunk) * 0.1),
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()

        payload = response.json()
        if isinstance(payload, dict):
            payload = [payload]

        for item in payload:
            idx = item.get("id")
            if idx is not None and not item.get("error"):
                results[idx] = item.get("result")

    return results


def rpc_batch_request_with_status(
    rpc_url: str, calls: list[tuple[str, list[Any]]]
) -> list[tuple[Any, bool]]:
    """Same as ``rpc_batch_request`` but distinguishes "RPC errored" from
    "RPC succeeded but returned None".

    Returns a list of ``(result, had_error)`` in call order:
      - ``had_error=True``  → the JSON-RPC response carried an ``error``
                              field for that call, OR the whole batch
                              transport failed (network, HTTP 5xx, JSON
                              parse, missing-id-in-response).
      - ``had_error=False`` → the call succeeded; ``result`` may still be
                              ``None`` (the function legitimately returned
                              no data, e.g. eth_call to a method that
                              isn't there returns ``"0x"``).

    This split is what callers like classify_resolved_address need:
    treating "RPC failed" as "function absent" cements transient
    misclassifications in caches. The plain ``rpc_batch_request`` helper
    above conflates both — keep it for sites that don't care.
    """
    if not calls:
        return []

    # Default to (None, True) so any chunk that fails wholesale leaves
    # its slots flagged as errored — matches the sequential path's
    # behavior of raising on any RPC failure.
    results: list[tuple[Any, bool]] = [(None, True)] * len(calls)

    for chunk_start in range(0, len(calls), MAX_BATCH_SIZE):
        chunk = calls[chunk_start : chunk_start + MAX_BATCH_SIZE]
        batch = [
            {"jsonrpc": "2.0", "id": chunk_start + i, "method": method, "params": params}
            for i, (method, params) in enumerate(chunk)
        ]

        try:
            response = _get_session().post(
                rpc_url,
                json=batch,
                timeout=max(JSON_RPC_TIMEOUT_SECONDS, len(chunk) * 0.1),
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()
        except Exception:
            # Whole-chunk failure — leave defaults as (None, True). This
            # is the conservative choice: caller will skip caching and
            # treat results as transient.
            continue

        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            # Unexpected shape (some providers refuse batches with a
            # non-list error object) — flag every slot in this chunk.
            continue

        for item in payload:
            if not isinstance(item, dict):
                continue
            idx = item.get("id")
            if not isinstance(idx, int) or idx < 0 or idx >= len(calls):
                continue
            if item.get("error"):
                results[idx] = (None, True)
            else:
                results[idx] = (item.get("result"), False)

    return results


def parse_address_result(raw: Any) -> str | None:
    """Extract a valid address from a raw ``eth_getStorageAt`` / ``eth_call`` result.

    Returns None for empty, zero-address, too-short, or revert-like responses.
    A valid ABI-encoded address is at least 66 chars (``0x`` + 64 hex digits).
    Shorter responses are reverts, error selectors, or empty returns.
    """
    if not raw or not isinstance(raw, str) or len(raw) < 66:
        return None
    if raw == "0x" + "0" * 64:
        return None
    addr = "0x" + raw[-40:]
    if addr == "0x" + "0" * 40:
        return None
    return normalize_hex(addr)


def selector(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()[:8]


def normalize_hex(value: str | None) -> str:
    if not isinstance(value, str) or not value.startswith("0x"):
        return "0x"
    return value.lower()


def decode_address(raw_value: str) -> str | None:
    normalized = normalize_hex(raw_value)
    if len(normalized) != 66:
        return None
    return "0x" + normalized[-40:]
