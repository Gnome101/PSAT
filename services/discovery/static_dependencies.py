#!/usr/bin/env python3
"""Discover statically embedded dependent contract addresses from EVM bytecode."""

import argparse
import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from utils.rpc import JSON_RPC_TIMEOUT_SECONDS as RPC_TIMEOUT_SECONDS  # noqa: F401 — re-export
from utils.rpc import get_code  # noqa: F401 — re-export for backward compat
from utils.rpc import normalize_address  # noqa: F401 — re-export for backward compat
from utils.rpc import rpc_request

EMPTY_CODE_VALUES = {"0x", "0x0"}


def has_deployed_code(bytecode_hex: str) -> bool:
    """Return True if an eth_getCode response represents deployed contract bytecode."""
    return bytecode_hex not in EMPTY_CODE_VALUES


def rpc_call(rpc_url: str, method: str, params: list, retries: int = 1) -> Any:
    """Backward-compatible wrapper. Prefer utils.rpc.rpc_request for new code."""
    return rpc_request(rpc_url, method, params, retries=retries) or "0x"


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
    effective_rpc = rpc_url or os.getenv("ETH_RPC")
    if not effective_rpc:
        raise RuntimeError("No RPC URL provided and ETH_RPC not set")

    address = normalize_address(address)
    deps = discover_dependencies(effective_rpc, address, code_cache=code_cache)
    return {"address": address, "dependencies": deps, "rpc": effective_rpc}


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
