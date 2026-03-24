#!/usr/bin/env python3
"""Classify contract dependencies as proxy, implementation, beacon, factory, library, or regular.

Detection methods:
  - EIP-1167 minimal proxy bytecode pattern
  - EIP-1967 storage slots (implementation, beacon, admin)
  - EIP-1822 UUPS logic slot
  - OpenZeppelin legacy implementation slot
  - EIP-2535 diamond proxy (facetAddresses() call)
  - implementation() call (catches custom proxies with non-standard slots)
  - Bytecode heuristic (short code + DELEGATECALL, confirmed via trace probe)
  - Dynamic trace edges (CREATE/CREATE2 -> factory, DELEGATECALL-only -> library)
  - Relational (proxy slot targets -> implementation/beacon)
"""

import json

from services.discovery.static_dependencies import get_code, normalize_address, rpc_call

# ---------------------------------------------------------------------------
# Storage slot constants
# ---------------------------------------------------------------------------

# EIP-1967 (keccak256 of label string minus 1)
EIP1967_IMPL_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
EIP1967_BEACON_SLOT = "0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50"
EIP1967_ADMIN_SLOT = "0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103"

# EIP-1822 UUPS
EIP1822_LOGIC_SLOT = "0xc5f16f0fcc639fa48a6947836d9850f504798523bf8c9a3a87d5876cf622bcf7"

# OpenZeppelin legacy (keccak256("org.zeppelinos.proxy.implementation"))
OZ_IMPL_SLOT = "0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3"

# ---------------------------------------------------------------------------
# EIP-1167 minimal proxy bytecode markers
# ---------------------------------------------------------------------------

EIP1167_PREFIX = "363d3d373d3d3d363d73"
EIP1167_SUFFIX = "5af43d82803e903d91602b57fd5bf3"

# ---------------------------------------------------------------------------
# Thresholds / selectors
# ---------------------------------------------------------------------------

# Max bytecode hex-char length for the DELEGATECALL heuristic (300 bytes).
SHORT_BYTECODE_THRESHOLD = 600

# Function selectors
IMPLEMENTATION_SELECTOR = "0x5c60da1b"  # implementation()
FACET_ADDRESSES_SELECTOR = "0x52ef6b2c"  # facetAddresses() — EIP-2535


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def get_storage_at(rpc_url: str, address: str, slot: str) -> str:
    """Read a single 32-byte storage slot."""
    return rpc_call(rpc_url, "eth_getStorageAt", [address, slot, "latest"], retries=1)


def _slot_to_address(slot_value: str) -> str | None:
    """Extract a 20-byte address from a 32-byte storage value.  Returns None for zero/empty."""
    if not slot_value or slot_value in ("0x", "0x0"):
        return None
    raw = slot_value.replace("0x", "").zfill(64)
    addr_hex = raw[-40:]
    if all(c == "0" for c in addr_hex):
        return None
    return normalize_address("0x" + addr_hex)


def detect_eip1167(bytecode_hex: str) -> str | None:
    """Return the implementation address if *bytecode_hex* is an EIP-1167 minimal proxy."""
    raw = (bytecode_hex[2:] if bytecode_hex.startswith("0x") else bytecode_hex).lower()
    if raw.startswith(EIP1167_PREFIX) and raw.endswith(EIP1167_SUFFIX):
        addr_hex = raw[len(EIP1167_PREFIX) : len(EIP1167_PREFIX) + 40]
        if len(addr_hex) == 40:
            return normalize_address("0x" + addr_hex)
    return None


def _bytecode_has_delegatecall(bytecode_hex: str) -> bool:
    """Return True if the bytecode contains a real DELEGATECALL (0xF4) opcode,
    skipping bytes that are part of PUSH immediates."""
    raw = bytecode_hex[2:] if bytecode_hex.startswith("0x") else bytecode_hex
    if not raw or len(raw) % 2 != 0:
        return False
    try:
        code = bytes.fromhex(raw)
    except ValueError:
        return False
    i = 0
    while i < len(code):
        op = code[i]
        if op == 0xF4:
            return True
        # PUSH1 (0x60) through PUSH32 (0x7F): skip immediate bytes
        if 0x60 <= op <= 0x7F:
            i += 1 + (op - 0x5F)
            continue
        i += 1
    return False


# Synthetic calldata: a 4-byte selector unlikely to match any real function.
_PROBE_CALLDATA = "0xdeadbeef"


def _extract_delegatecall_target_geth(node) -> str | None:
    """Extract the first DELEGATECALL target address from a Geth callTracer result."""
    if not isinstance(node, dict):
        return None
    if str(node.get("type", "")).upper() == "DELEGATECALL":
        raw = node.get("to")
        return normalize_address(raw) if isinstance(raw, str) and len(raw) >= 42 else ""
    for child in node.get("calls", []) or []:
        target = _extract_delegatecall_target_geth(child)
        if target is not None:
            return target
    return None


def _extract_delegatecall_target_parity(result) -> str | None:
    """Extract the first DELEGATECALL target address from a Parity-style trace result."""
    traces = result if isinstance(result, list) else (result.get("trace", []) if isinstance(result, dict) else [])
    for item in traces:
        if not isinstance(item, dict):
            continue
        action = item.get("action", {}) or {}
        if str(action.get("callType", "")).lower() == "delegatecall":
            raw = action.get("to")
            return normalize_address(raw) if isinstance(raw, str) and len(raw) >= 42 else ""
    return None


def _probe_delegatecall(rpc_url: str, address: str) -> str | None | bool:
    """Send a synthetic eth_call with tracing to check if DELEGATECALL fires
    in the fallback path.

    Returns:
      - An address string if DELEGATECALL is triggered (the implementation).
      - ``""`` (empty string) if DELEGATECALL fired but the target couldn't be parsed.
      - ``False`` if no DELEGATECALL was triggered (not a proxy).
      - ``None`` if tracing is unavailable (caller should fall back to static heuristic).

    Any truthy return means the contract is a proxy.
    """
    call_obj = {"to": address, "data": _PROBE_CALLDATA}

    # Try debug_traceCall (Geth-style) with callTracer
    for params in [
        [call_obj, "latest", {"tracer": "callTracer", "timeout": "10s"}],
        [call_obj, "latest", {"tracer": "callTracer"}],
    ]:
        try:
            result = rpc_call(rpc_url, "debug_traceCall", params, retries=0)
            target = _extract_delegatecall_target_geth(result)
            return target if target is not None else False
        except RuntimeError:
            pass

    # Try trace_call (Parity / OpenEthereum / Erigon-style)
    try:
        result = rpc_call(rpc_url, "trace_call", [call_obj, ["trace"], "latest"], retries=0)
        target = _extract_delegatecall_target_parity(result)
        return target if target is not None else False
    except RuntimeError:
        pass

    return None  # tracing unavailable


def _try_implementation_call(rpc_url: str, address: str) -> str | None:
    """Call ``implementation()`` (selector 0x5c60da1b) on a contract.
    Returns the address on success, or None."""
    try:
        result = rpc_call(
            rpc_url,
            "eth_call",
            [{"to": address, "data": IMPLEMENTATION_SELECTOR}, "latest"],
            retries=0,
        )
        return _slot_to_address(result)
    except RuntimeError:
        return None


def _decode_address_array(hex_data: str) -> list[str] | None:
    """Decode an ABI-encoded ``address[]`` return value."""
    raw = hex_data[2:] if hex_data.startswith("0x") else hex_data
    try:
        data = bytes.fromhex(raw)
    except ValueError:
        return None
    if len(data) < 64:
        return None
    offset = int.from_bytes(data[:32], "big")
    if offset + 32 > len(data):
        return None
    length = int.from_bytes(data[offset : offset + 32], "big")
    if length == 0 or length > 100:
        return None
    start = offset + 32
    if start + length * 32 > len(data):
        return None
    addresses = []
    for i in range(length):
        addr = _slot_to_address("0x" + data[start + i * 32 : start + (i + 1) * 32].hex())
        if addr:
            addresses.append(addr)
    return addresses or None


def _try_facet_addresses_call(rpc_url: str, address: str) -> list[str] | None:
    """Call ``facetAddresses()`` (EIP-2535) on a contract.
    Returns a list of facet addresses on success, or None."""
    try:
        result = rpc_call(
            rpc_url,
            "eth_call",
            [{"to": address, "data": FACET_ADDRESSES_SELECTOR}, "latest"],
            retries=0,
        )
        return _decode_address_array(result)
    except RuntimeError:
        return None


# ---------------------------------------------------------------------------
# Single-contract classification (Phase 1)
# ---------------------------------------------------------------------------


def classify_single(address: str, rpc_url: str, bytecode: str | None = None) -> dict:
    """Classify one contract via bytecode patterns and storage slot inspection.

    Returns a dict with ``address``, ``type``, and type-specific metadata.
    """
    address = normalize_address(address)
    if bytecode is None:
        bytecode = get_code(rpc_url, address)

    info: dict = {"address": address}

    # 1. EIP-1167 minimal proxy (bytecode pattern)
    eip1167_impl = detect_eip1167(bytecode)
    if eip1167_impl:
        info.update(type="proxy", proxy_type="eip1167", implementation=eip1167_impl)
        return info

    # 2. EIP-1967 storage slots
    impl = _slot_to_address(get_storage_at(rpc_url, address, EIP1967_IMPL_SLOT))
    beacon = _slot_to_address(get_storage_at(rpc_url, address, EIP1967_BEACON_SLOT))
    admin = _slot_to_address(get_storage_at(rpc_url, address, EIP1967_ADMIN_SLOT))

    if beacon:
        info.update(type="proxy", proxy_type="beacon_proxy", beacon=beacon)
        if impl:
            info["implementation"] = impl
        else:
            # Resolve implementation through the beacon contract
            beacon_impl = _try_implementation_call(rpc_url, beacon)
            if beacon_impl:
                info["implementation"] = beacon_impl
        if admin:
            info["admin"] = admin
        return info

    if impl:
        info.update(type="proxy", proxy_type="eip1967", implementation=impl)
        if admin:
            info["admin"] = admin
        return info

    # 3. EIP-1822 UUPS
    uups = _slot_to_address(get_storage_at(rpc_url, address, EIP1822_LOGIC_SLOT))
    if uups:
        info.update(type="proxy", proxy_type="eip1822", implementation=uups)
        return info

    # 4. OpenZeppelin legacy slot
    oz = _slot_to_address(get_storage_at(rpc_url, address, OZ_IMPL_SLOT))
    if oz:
        info.update(type="proxy", proxy_type="oz_legacy", implementation=oz)
        return info

    # 5. EIP-2535 diamond proxy — facetAddresses() call
    facets = _try_facet_addresses_call(rpc_url, address)
    if facets:
        info.update(type="proxy", proxy_type="eip2535", facets=facets)
        return info

    # 6. implementation() call — catches custom proxies with non-standard
    #    storage slots that still expose the standard interface.
    impl_call = _try_implementation_call(rpc_url, address)
    if impl_call:
        info.update(type="proxy", proxy_type="custom", implementation=impl_call)
        return info

    # 7. Heuristic: short bytecode (<= 300 bytes) with DELEGATECALL opcode.
    #    When tracing is available, probe with synthetic calldata to confirm
    #    DELEGATECALL actually fires in the fallback path (eliminates library
    #    false positives) and extract the implementation address from the trace.
    raw = bytecode[2:] if bytecode.startswith("0x") else bytecode
    if 10 <= len(raw) <= SHORT_BYTECODE_THRESHOLD and _bytecode_has_delegatecall(bytecode):
        probe = _probe_delegatecall(rpc_url, address)
        if probe is False:
            # DELEGATECALL exists but isn't triggered by arbitrary calldata —
            # this is a library or utility, not a proxy.
            info["type"] = "regular"
            return info
        if probe is None:
            # Tracing unavailable — fall back to static heuristic.
            info.update(type="proxy", proxy_type="unknown")
            return info
        # probe is a str: the DELEGATECALL target (implementation address)
        info.update(type="proxy", proxy_type="unknown")
        if probe:  # non-empty address string
            info["implementation"] = probe
        return info

    info["type"] = "regular"
    return info


# ---------------------------------------------------------------------------
# Multi-contract classification (Phases 1-3)
# ---------------------------------------------------------------------------


def classify_contracts(
    target: str,
    dependencies: list[str],
    rpc_url: str,
    dynamic_edges: list[dict] | None = None,
) -> dict:
    """Classify the target contract and all its dependencies.

    Three phases:
      1. **Intrinsic** -- storage slots and bytecode patterns.
      2. **Relational** -- mark implementations / beacons discovered via proxy pointers.
      3. **Behavioral** -- factory / library labels from dynamic call-graph edges.
    """
    target = normalize_address(target)
    all_addrs = list(dict.fromkeys([target] + [normalize_address(a) for a in dependencies]))

    # Phase 1 -- intrinsic classification
    classifications: dict[str, dict] = {}
    impl_to_proxies: dict[str, list[str]] = {}
    beacon_to_proxies: dict[str, list[str]] = {}
    discovered: set[str] = set()
    all_addrs_set = set(all_addrs)

    for addr in all_addrs:
        try:
            info = classify_single(addr, rpc_url)
        except RuntimeError:
            info = {"address": addr, "type": "regular"}
        classifications[addr] = info

        # Track reverse mappings
        if impl := info.get("implementation"):
            impl_to_proxies.setdefault(impl, []).append(addr)
            if impl not in all_addrs_set:
                discovered.add(impl)
        if bcon := info.get("beacon"):
            beacon_to_proxies.setdefault(bcon, []).append(addr)
            if bcon not in all_addrs_set:
                discovered.add(bcon)
        for facet in info.get("facets", []):
            impl_to_proxies.setdefault(facet, []).append(addr)
            if facet not in all_addrs_set:
                discovered.add(facet)

    # Classify newly-discovered addresses (found in proxy slots)
    for addr in sorted(discovered):
        try:
            info = classify_single(addr, rpc_url)
        except RuntimeError:
            info = {"address": addr, "type": "regular"}
        classifications[addr] = info

    # Phase 2 -- relational: mark implementations and beacons
    for addr, info in classifications.items():
        # Beacon takes priority: the EIP-1967 beacon slot is a strong signal.
        # Phase 1 may have classified the beacon as "proxy/custom" because
        # UpgradeableBeacon exposes implementation() — override that here.
        if addr in beacon_to_proxies:
            for key in ("proxy_type", "beacon", "admin"):
                info.pop(key, None)
            info["type"] = "beacon"
            info["proxies"] = sorted(beacon_to_proxies[addr])
            if "implementation" not in info:
                impl = _try_implementation_call(rpc_url, addr)
                if impl:
                    info["implementation"] = impl
            continue
        if info["type"] != "regular":
            continue
        if addr in impl_to_proxies:
            info["type"] = "implementation"
            info["proxies"] = sorted(impl_to_proxies[addr])

    # Phase 3 -- behavioral: factory / library from dynamic edges
    if dynamic_edges:
        creators: set[str] = set()
        delegatecall_only: dict[str, bool] = {}

        for edge in dynamic_edges:
            src = normalize_address(edge["from"])
            dst = normalize_address(edge["to"])
            op = edge.get("op", "")

            if op in ("CREATE", "CREATE2"):
                creators.add(src)
            elif op == "DELEGATECALL":
                if dst in classifications and dst not in delegatecall_only:
                    delegatecall_only[dst] = True
            elif op in ("CALL", "STATICCALL", "CALLCODE"):
                if dst in classifications:
                    delegatecall_only[dst] = False

        for addr, info in classifications.items():
            if info["type"] != "regular":
                continue
            if addr in creators:
                info["type"] = "factory"
            elif delegatecall_only.get(addr, False):
                info["type"] = "library"

    return {
        "address": target,
        "rpc": rpc_url,
        "classifications": classifications,
        "discovered_addresses": sorted(discovered),
    }


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------


def main():
    import argparse
    import os
    from pathlib import Path

    from dotenv import load_dotenv

    from services.discovery.static_dependencies import resolve_rpc_for_address

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    parser = argparse.ArgumentParser(description="Classify contract dependencies")
    parser.add_argument("address", help="Contract address to classify")
    parser.add_argument("--rpc", help="RPC URL")
    parser.add_argument("--deps", nargs="*", default=[], help="Dependency addresses")
    args = parser.parse_args()

    rpc = args.rpc or os.getenv("ETH_RPC")
    _, resolved_rpc = resolve_rpc_for_address(args.address.strip(), rpc)

    result = classify_contracts(args.address.strip(), args.deps, resolved_rpc)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
