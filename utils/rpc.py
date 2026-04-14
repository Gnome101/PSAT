"""Shared low-level helpers for JSON-RPC and EVM encoding."""

from __future__ import annotations

import time
from typing import Any

import requests
from eth_utils.crypto import keccak

JSON_RPC_TIMEOUT_SECONDS = 10

# Maximum calls per JSON-RPC batch (stay under provider limits)
MAX_BATCH_SIZE = 500

RETRYABLE_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}


def normalize_address(address: str) -> str:
    """Normalize an Ethereum address to lowercase with a single 0x prefix."""
    return "0x" + address.lower().replace("0x", "", 1)


def rpc_request(rpc_url: str, method: str, params: list[Any], retries: int = 1) -> Any:
    for attempt in range(retries + 1):
        try:
            response = requests.post(
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
    """Fetch deployed EVM bytecode at an address via eth_getCode."""
    return rpc_request(rpc_url, "eth_getCode", [address, "latest"]) or "0x"


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

        response = requests.post(
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
