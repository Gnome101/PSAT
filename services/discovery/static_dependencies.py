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
    "ethereum": (
        "https://eth.llamarpc.com",
        "https://rpc.flashbots.net",
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
}


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


def discover_dependencies(
    rpc_url: str,
    root: str,
    code_cache: dict[str, str] | None = None,
) -> list[str]:
    """BFS-traverse embedded PUSH20 addresses and return deployed contract dependencies."""
    root = normalize_address(root)
    if code_cache is None:
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


def find_dependencies(address: str, rpc_url: str | None = None, code_cache: dict[str, str] | None = None) -> dict:
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

    deps = discover_dependencies(resolved_rpc, address, code_cache=code_cache)

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
