#!/usr/bin/env python3
"""Discover statically embedded dependent contract addresses from EVM bytecode."""

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

EMPTY_CODE_VALUES = {"0x", "0x0"}
RETRYABLE_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}
RPC_TIMEOUT_SECONDS = 8
DEFAULT_PUBLIC_RPCS_BY_NETWORK = {
    # Order: batch-capable RPCs first, then individual-only fallbacks.
    "ethereum": (
        "https://rpc.flashbots.net",
        "https://eth.llamarpc.com",
        "https://eth-mainnet.public.blastapi.io",
        "https://cloudflare-eth.com",
        "https://ethereum-rpc.publicnode.com",
    ),
    "base": (
        "https://mainnet.base.org",
        "https://base.llamarpc.com",
        "https://base-rpc.publicnode.com",
    ),
    "optimism": (
        "https://mainnet.optimism.io",
        "https://optimism.llamarpc.com",
        "https://optimism-rpc.publicnode.com",
    ),
    "arbitrum": (
        "https://arb1.arbitrum.io/rpc",
        "https://arbitrum.llamarpc.com",
        "https://arbitrum-one-rpc.publicnode.com",
    ),
    "polygon": (
        "https://polygon-rpc.com",
        "https://polygon.llamarpc.com",
        "https://polygon-bor-rpc.publicnode.com",
    ),
    "avalanche": (
        "https://api.avax.network/ext/bc/C/rpc",
        "https://avalanche-c-chain-rpc.publicnode.com",
    ),
    "bsc": (
        "https://bsc-dataseed.binance.org",
        "https://bsc-rpc.publicnode.com",
    ),
    "linea": (
        "https://rpc.linea.build",
        "https://linea-rpc.publicnode.com",
    ),
    "scroll": (
        "https://rpc.scroll.io",
        "https://scroll-rpc.publicnode.com",
    ),
    "zksync": (
        "https://mainnet.era.zksync.io",
        "https://zksync-era-rpc.publicnode.com",
    ),
    "blast": (
        "https://rpc.blast.io",
        "https://blast-rpc.publicnode.com",
    ),
}

# Max addresses per JSON-RPC batch request.
_BATCH_RPC_SIZE = 100


def normalize_address(address: str) -> str:
    """Normalize an Ethereum address to lowercase with a single 0x prefix."""
    return "0x" + address.lower().replace("0x", "", 1)


def has_deployed_code(bytecode_hex: str) -> bool:
    """Return True if an eth_getCode response represents deployed contract bytecode."""
    return bytecode_hex not in EMPTY_CODE_VALUES


def rpc_call(rpc_url: str, method: str, params: list, retries: int = 1) -> Any:
    """Send a JSON-RPC POST request with retries/backoff and return the 'result' field."""
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode("utf-8")
    request = urllib.request.Request(
        rpc_url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "getContractAddresses/1.0",
        },
    )
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=RPC_TIMEOUT_SECONDS) as response:
                body = json.loads(response.read().decode("utf-8"))
            if body.get("error"):
                raise RuntimeError(str(body["error"]))
            return body.get("result") or "0x"
        except urllib.error.HTTPError as exc:
            if exc.code in RETRYABLE_HTTP_CODES and attempt < retries:
                time.sleep(0.3 * (2**attempt))
                continue
            raise RuntimeError(f"RPC request failed for {rpc_url}: HTTP Error {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt < retries:
                time.sleep(0.3 * (2**attempt))
                continue
            raise RuntimeError(f"RPC request failed for {rpc_url}: {exc}") from exc
    raise RuntimeError(f"RPC request failed for {rpc_url}: all {retries + 1} attempts exhausted")


def get_code(rpc_url: str, address: str) -> str:
    """Fetch the deployed EVM bytecode at an address via eth_getCode."""
    return rpc_call(rpc_url, "eth_getCode", [address, "latest"])


def _individual_get_code_throttled(rpc_url: str, addr: str, limiter) -> tuple[str, str]:
    """Fetch code for a single address with rate limiting — returns (addr, bytecode_hex)."""
    limiter.wait()
    try:
        return addr, get_code(rpc_url, addr)
    except RuntimeError:
        return addr, "0x"


class _RpcRateLimiter:
    """Thread-safe rate limiter enforcing a minimum interval between calls."""

    def __init__(self, calls_per_second: float):
        import threading
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


# Public RPCs typically allow ~4-5 individual requests/sec before throttling.
_RPC_RATE_LIMIT = 4
_FALLBACK_WORKERS = 4


def batch_get_code(rpc_url: str, addresses: list[str]) -> dict[str, str]:
    """Batch-fetch eth_getCode for many addresses in a single HTTP request.

    Returns ``{address: bytecode_hex}`` for each address.  Splits into
    sub-batches of ``_BATCH_RPC_SIZE`` to stay within public RPC limits.
    Falls back to rate-limited concurrent individual calls if the RPC
    rejects batching.
    """
    from concurrent.futures import ThreadPoolExecutor

    if not addresses:
        return {}

    results: dict[str, str] = {}
    for i in range(0, len(addresses), _BATCH_RPC_SIZE):
        batch = addresses[i : i + _BATCH_RPC_SIZE]
        payload = json.dumps(
            [
                {"jsonrpc": "2.0", "id": idx, "method": "eth_getCode", "params": [addr, "latest"]}
                for idx, addr in enumerate(batch)
            ]
        ).encode("utf-8")
        request = urllib.request.Request(
            rpc_url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "getContractAddresses/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=max(RPC_TIMEOUT_SECONDS, 30)) as response:
                body = json.loads(response.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError):
            body = None

        # A successful batch returns a JSON list.  If we got a dict instead
        # (e.g. RPC error like "too many calls in batch") or an HTTP error,
        # fall back to rate-limited concurrent individual calls.
        if not isinstance(body, list):
            limiter = _RpcRateLimiter(_RPC_RATE_LIMIT)
            with ThreadPoolExecutor(max_workers=_FALLBACK_WORKERS) as executor:
                futures = [
                    executor.submit(_individual_get_code_throttled, rpc_url, addr, limiter)
                    for addr in batch
                ]
                for future in futures:
                    addr, code = future.result()
                    results[addr] = code
            continue

        for item in body:
            idx = item.get("id")
            if idx is not None and 0 <= idx < len(batch):
                code = item.get("result") or "0x"
                results[batch[idx]] = code if isinstance(code, str) and code.startswith("0x") else "0x"
        # Fill in any missing addresses (e.g. from errors in individual items).
        for addr in batch:
            if addr not in results:
                results[addr] = "0x"

    return results


def resolve_rpc_for_address(address: str, rpc_url: str | None = None) -> tuple[str, str]:
    """Pick an RPC endpoint where the address has deployed bytecode."""
    address = normalize_address(address)
    if rpc_url:
        if not has_deployed_code(get_code(rpc_url, address)):
            raise RuntimeError(f"Address {address} has no deployed bytecode.")
        return "custom", rpc_url

    errors = []
    for network, candidates in DEFAULT_PUBLIC_RPCS_BY_NETWORK.items():
        for candidate_rpc in candidates:
            try:
                if has_deployed_code(get_code(candidate_rpc, address)):
                    return network, candidate_rpc
            except RuntimeError as exc:
                errors.append(f"{network}: {exc}")

    message = f"Could not find deployed bytecode for {address} on known public RPC endpoints."
    if errors:
        message += " " + " | ".join(errors[:3])
    raise RuntimeError(message)


def extract_push20_addresses(bytecode_hex: str) -> set[str]:
    """Parse EVM bytecode and extract 20-byte constants from PUSH20 (0x73) opcodes."""
    raw = bytecode_hex[2:] if bytecode_hex.startswith("0x") else bytecode_hex
    if len(raw) % 2 != 0:
        return set()
    code = bytes.fromhex(raw) if raw else b""

    out = set()
    i = 0
    while i < len(code):
        op = code[i]
        if op == 0x73 and i + 20 < len(code):
            out.add("0x" + code[i + 1 : i + 21].hex())
            i += 21
            continue
        if 0x60 <= op <= 0x7F:
            i += 1 + (op - 0x5F)
            continue
        i += 1

    out.discard("0x" + ("0" * 40))
    return out


def discover_dependencies(rpc_url: str, root: str) -> list[str]:
    """BFS-traverse embedded PUSH20 addresses and return deployed contract dependencies."""
    root = normalize_address(root)
    code_cache = {}

    # Cache eth_getCode lookups so repeated scans don’t spam the RPC endpoint.
    def cached_get_code(address: str) -> str:
        normalized = normalize_address(address)
        if normalized not in code_cache:
            code_cache[normalized] = get_code(rpc_url, normalized)
        return code_cache[normalized]

    if not has_deployed_code(cached_get_code(root)):
        raise RuntimeError(f"Address {root} has no deployed bytecode.")

    stack = [root]
    seen = {root}
    deps = set()

    while stack:
        current = stack.pop()
        for candidate in extract_push20_addresses(cached_get_code(current)):
            candidate = normalize_address(candidate)
            if candidate in seen:
                continue
            seen.add(candidate)
            if has_deployed_code(cached_get_code(candidate)):
                deps.add(candidate)
                stack.append(candidate)

    return sorted(deps)


def find_dependencies(address: str, rpc_url: str | None = None) -> dict:
    """Resolve an RPC endpoint and return discovered static contract dependencies."""
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    env_rpc = os.getenv("ETH_RPC")
    effective_rpc = rpc_url or env_rpc

    if effective_rpc:
        try:
            network, resolved_rpc = resolve_rpc_for_address(address, effective_rpc)
        except RuntimeError:
            if rpc_url:
                raise
            network, resolved_rpc = resolve_rpc_for_address(address, None)
    else:
        network, resolved_rpc = resolve_rpc_for_address(address, None)

    deps = discover_dependencies(resolved_rpc, address)

    output = {
        "address": normalize_address(address),
        "dependencies": deps,
        "rpc": resolved_rpc,
    }
    if network != "custom":
        output["network"] = network
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("address")
    parser.add_argument("--rpc")
    args = parser.parse_args()

    try:
        output = find_dependencies(args.address.strip(), args.rpc)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    print(json.dumps(output))


if __name__ == "__main__":
    main()
