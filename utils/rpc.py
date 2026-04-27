"""Shared low-level helpers for JSON-RPC and EVM encoding."""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

import requests
from eth_utils.crypto import keccak
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

JSON_RPC_TIMEOUT_SECONDS = 10

# Maximum calls per JSON-RPC batch (stay under provider limits)
MAX_BATCH_SIZE = 500

RETRYABLE_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}

# Process-wide cache for eth_getCode (bytecode + its keccak); skips caching on RPC error and applies a TTL for safety.
_GETCODE_CACHE: dict[tuple[str, str], tuple[str, str, float]] = {}
_GETCODE_CACHE_LOCK = threading.Lock()
_GETCODE_CACHE_MAX = 8192
_GETCODE_CACHE_TTL_S = float(os.getenv("PSAT_GETCODE_CACHE_TTL_S", "1800"))


def clear_getcode_cache() -> None:
    """Clear the process-wide eth_getCode cache. For tests + manual reset."""
    from utils.memory import reset_cache_pressure_state

    with _GETCODE_CACHE_LOCK:
        _GETCODE_CACHE.clear()
    reset_cache_pressure_state("getcode")


def _log_getcode_pressure() -> None:
    """Log when _GETCODE_CACHE crosses 50/75/95% of its bound (caller holds the lock)."""
    from utils.memory import cache_pressure_message

    msg = cache_pressure_message("getcode", len(_GETCODE_CACHE), _GETCODE_CACHE_MAX)
    if msg:
        logger.info("[CACHE_PRESSURE] %s", msg)


def _normalized_addr(address: str) -> str:
    return address.lower() if address.startswith("0x") else "0x" + address.lower()


# Per-thread requests.Session for TCP/TLS reuse on RPC calls (Session is not thread-safe across calls, hence
# threading.local()).
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
    """Return ``(bytecode_hex, keccak_hex)`` cached together so downstream content-addressed lookups get the keccak for
    free."""
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

    # RPC outside the lock so concurrent misses for different addresses don't serialize.
    raw = rpc_request(rpc_url, "eth_getCode", [address, "latest"])
    code = raw if isinstance(raw, str) and raw.startswith("0x") else "0x"
    # Normalize "0x0" → "0x" so bytes.fromhex doesn't raise on odd-length hex.
    if code in {"0x", "0x0"}:
        code = "0x"
    code_bytes = bytes.fromhex(code[2:]) if len(code) > 2 else b""
    keccak_hex = "0x" + keccak(code_bytes).hex()

    with _GETCODE_CACHE_LOCK:
        _evict_getcode_if_needed()
        _GETCODE_CACHE[key] = (code, keccak_hex, now)
        _log_getcode_pressure()
    return code, keccak_hex


def _evict_getcode_if_needed() -> None:
    """Drop the oldest 25% of _GETCODE_CACHE entries when the bound is reached (caller holds _GETCODE_CACHE_LOCK)."""
    if len(_GETCODE_CACHE) < _GETCODE_CACHE_MAX:
        return
    cutoff = sorted(_GETCODE_CACHE.values(), key=lambda v: v[2])[len(_GETCODE_CACHE) // 4][2]
    for k in [k for k, v in _GETCODE_CACHE.items() if v[2] <= cutoff]:
        _GETCODE_CACHE.pop(k, None)


def get_code_batch(rpc_url: str, addresses: list[str]) -> dict[str, str]:
    """Cache-aware batched eth_getCode; errored slots are omitted from the returned ``{address: bytecode}`` map."""
    if not addresses:
        return {}

    normalized = [_normalized_addr(a) for a in addresses]
    now = time.monotonic()
    out: dict[str, str] = {}

    to_fetch: list[str] = []
    with _GETCODE_CACHE_LOCK:
        for addr in normalized:
            cached = _GETCODE_CACHE.get((rpc_url, addr))
            if cached is not None:
                code, _keccak, inserted_at = cached
                if now - inserted_at < _GETCODE_CACHE_TTL_S:
                    out[addr] = code
                    continue
            to_fetch.append(addr)

    if not to_fetch:
        return out

    calls: list[tuple[str, list[Any]]] = [("eth_getCode", [addr, "latest"]) for addr in to_fetch]
    raw_results = rpc_batch_request_with_status(rpc_url, calls)
    with _GETCODE_CACHE_LOCK:
        for addr, (raw, had_error) in zip(to_fetch, raw_results):
            if had_error:
                continue  # caller treats absence as missing/error
            code = raw if isinstance(raw, str) and raw.startswith("0x") else "0x"
            # Normalize "0x0" → "0x" so bytes.fromhex doesn't raise on
            # odd-length hex (some providers return "0x0" for EOAs).
            if code in {"0x", "0x0"}:
                code = "0x"
            code_bytes = bytes.fromhex(code[2:]) if len(code) > 2 else b""
            keccak_hex = "0x" + keccak(code_bytes).hex()
            # Honour the cache bound — codex iter-5 P2: batch path was
            # bypassing eviction, letting long-lived workers exceed
            # _GETCODE_CACHE_MAX with full bytecode payloads.
            _evict_getcode_if_needed()
            _GETCODE_CACHE[(rpc_url, addr)] = (code, keccak_hex, now)
            _log_getcode_pressure()
            out[addr] = code
    return out


def rpc_batch_request(rpc_url: str, calls: list[tuple[str, list[Any]]]) -> list[Any]:
    """Send a JSON-RPC batch and return results in call order; per-call errors yield ``None``."""
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


def rpc_batch_request_with_status(rpc_url: str, calls: list[tuple[str, list[Any]]]) -> list[tuple[Any, bool]]:
    """Like ``rpc_batch_request`` but returns ``(result, had_error)`` so callers can distinguish RPC failure from a
    legitimate ``None`` result."""
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
