"""Etherscan API client.

All Etherscan calls are routed through :func:`get`, which enforces a
global rate limit (``ETHERSCAN_RATE_LIMIT`` calls/sec).  Callers do
**not** need to add their own sleeps or per-module limiters.
"""

import json as _json
import logging
import os
import threading
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from eth_utils.crypto import keccak

logger = logging.getLogger(__name__)

ETHERSCAN_API = "https://api.etherscan.io/v2/api"
_RATE_LIMIT_RETRIES = 5
_RATE_LIMIT_BACKOFF = 1.0  # seconds, doubles each retry

# Global Etherscan rate limit — applies to every call through get().
ETHERSCAN_RATE_LIMIT = 15  # requests per second

_min_interval = 1.0 / ETHERSCAN_RATE_LIMIT
_rate_lock = threading.Lock()
_last_call = 0.0


def _wait_rate_limit() -> None:
    """Block until the minimum interval since the last call has elapsed."""
    global _last_call
    with _rate_lock:
        now = time.monotonic()
        elapsed = now - _last_call
        if elapsed < _min_interval:
            time.sleep(_min_interval - elapsed)
        _last_call = time.monotonic()


def _get_api_key() -> str:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    key = os.getenv("ETHERSCAN_API_KEY")
    if not key:
        raise RuntimeError("ETHERSCAN_API_KEY not set in .env")
    return key


def get(module: str, action: str, chain_id: int = 1, **params) -> dict:
    """Make an Etherscan API call with automatic retry on rate-limit errors.

    Automatically throttled to ``ETHERSCAN_RATE_LIMIT`` req/s — callers
    should not add their own sleeps.
    """
    api_key = _get_api_key()
    backoff = _RATE_LIMIT_BACKOFF

    for attempt in range(_RATE_LIMIT_RETRIES + 1):
        _wait_rate_limit()
        resp = requests.get(
            ETHERSCAN_API,
            params={
                "chainid": str(chain_id),
                "module": module,
                "action": action,
                "apikey": api_key,
                **params,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "1":
            return data

        result_str = str(data.get("result", ""))
        if "rate limit" in result_str.lower() and attempt < _RATE_LIMIT_RETRIES:
            logger.warning(
                "Etherscan rate limit hit, retrying in %.1fs (attempt %d/%d)",
                backoff,
                attempt + 1,
                _RATE_LIMIT_RETRIES,
            )
            time.sleep(backoff)
            backoff *= 2
            continue

        raise RuntimeError(f"Etherscan error: {data.get('message', 'unknown')} - {result_str}")

    raise RuntimeError("Etherscan rate limit: max retries exceeded")


def _canonical_abi_type(inp: dict) -> str:
    """Expand an ABI input type to its canonical form, recursing into tuple components."""
    if inp.get("type") == "tuple":
        components = inp.get("components", [])
        inner = ",".join(_canonical_abi_type(c) for c in components)
        return f"({inner})"
    if inp.get("type", "").startswith("tuple["):
        # tuple[] or tuple[N] — expand the base tuple and keep the array suffix
        suffix = inp["type"][5:]  # e.g. "[]" or "[3]"
        components = inp.get("components", [])
        inner = ",".join(_canonical_abi_type(c) for c in components)
        return f"({inner}){suffix}"
    return inp.get("type", "")


def _build_selector_map(abi_json: str) -> dict[str, str]:
    """Parse an ABI JSON string into a selector → function name mapping."""
    try:
        abi = _json.loads(abi_json)
    except (ValueError, TypeError):
        return {}
    selector_map: dict[str, str] = {}
    for entry in abi:
        if entry.get("type") != "function":
            continue
        name = entry.get("name", "")
        inputs = entry.get("inputs", [])
        sig = f"{name}({','.join(_canonical_abi_type(inp) for inp in inputs)})"
        selector = "0x" + keccak(text=sig).hex()[:8]
        selector_map[selector] = name
    return selector_map


def get_contract_info(address: str) -> tuple[str | None, dict[str, str]]:
    """Fetch contract name and selector map in a single Etherscan call.

    Returns (name_or_None, {selector: function_name}).
    """
    try:
        data = get("contract", "getsourcecode", address=address)
        result = data["result"][0]
    except Exception:
        return None, {}
    name = (result.get("ContractName") or "").strip() or None
    selector_map = _build_selector_map(result.get("ABI", ""))
    return name, selector_map


def get_contract_name(address: str) -> str | None:
    """Return the verified contract name for *address*, or None if unavailable."""
    name, _ = get_contract_info(address)
    return name


def get_source(address: str) -> dict:
    """Fetch verified source code for a contract address. Returns the first result."""
    data = get("contract", "getsourcecode", address=address)
    result = data["result"][0]

    if not result.get("SourceCode"):
        raise RuntimeError(f"No verified source code for {address}")

    return result
