"""Shared low-level helpers for JSON-RPC and EVM encoding."""

from __future__ import annotations

from typing import Any

import requests
from eth_utils.crypto import keccak

JSON_RPC_TIMEOUT_SECONDS = 10


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
