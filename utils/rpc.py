"""Shared low-level helpers for JSON-RPC and EVM encoding."""

from __future__ import annotations

from typing import Any

import requests
from eth_utils.crypto import keccak

JSON_RPC_TIMEOUT_SECONDS = 10

# Maximum calls per JSON-RPC batch (stay under provider limits)
MAX_BATCH_SIZE = 500


def rpc_request(rpc_url: str, method: str, params: list[Any]) -> Any:
    response = requests.post(
        rpc_url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=JSON_RPC_TIMEOUT_SECONDS,
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError(str(payload["error"]))
    return payload.get("result")


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
    """Extract a valid address from a raw ``eth_getStorageAt`` / ``eth_call`` result."""
    if not raw or raw == "0x" + "0" * 64:
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
