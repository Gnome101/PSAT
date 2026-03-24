import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services import classifier as cls

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def ADDR(n: int) -> str:
    return "0x" + hex(n)[2:].zfill(40)


RPC = "https://rpc.example"
BIG_BYTECODE = "0x" + "60" * 500
SHORT_BYTECODE = "0x" + "6000" * 10 + "f4" + "00" * 5
ZERO_SLOT = "0x" + "0" * 64


def _slot_for(addr: str) -> str:
    return "0x" + "0" * 24 + addr[2:]


def _abi_encode_address_array(addrs: list[str]) -> str:
    """Minimal ABI encoder for address[] return values."""
    buf = (0x20).to_bytes(32, "big")  # offset
    buf += len(addrs).to_bytes(32, "big")  # length
    for a in addrs:
        buf += bytes.fromhex(a[2:].zfill(64))
    return "0x" + buf.hex()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def test_helper_functions():
    assert cls._slot_to_address(ZERO_SLOT) is None
    assert cls._slot_to_address("0x") is None
    assert cls._slot_to_address(None) is None  # type: ignore[arg-type]
    assert cls._slot_to_address(_slot_for(ADDR(2))) == ADDR(2)

    impl = "aabbccddee11223344556677889900aabbccddee"
    assert cls.detect_eip1167("0x" + cls.EIP1167_PREFIX + impl + cls.EIP1167_SUFFIX) == "0x" + impl
    assert cls.detect_eip1167("0x60016000") is None

    assert cls._bytecode_has_delegatecall("0x6000f4") is True
    assert cls._bytecode_has_delegatecall("0x61f400") is False  # 0xf4 inside PUSH2
    assert cls._bytecode_has_delegatecall("0x61f400f4") is True
    assert cls._bytecode_has_delegatecall("0x600") is False  # odd-length hex
    assert cls._bytecode_has_delegatecall("0xZZZZ") is False  # invalid hex

    # _decode_address_array
    encoded = _abi_encode_address_array([ADDR(1), ADDR(2)])
    assert cls._decode_address_array(encoded) == [ADDR(1), ADDR(2)]
    assert cls._decode_address_array("0x") is None
    assert cls._decode_address_array("0x" + "00" * 64) is None  # length=0
    # length > 100 rejected
    big = (0x20).to_bytes(32, "big") + (101).to_bytes(32, "big") + b"\x00" * (101 * 32)
    assert cls._decode_address_array("0x" + big.hex()) is None


# ---------------------------------------------------------------------------
# Full classification pipeline: all proxy types, probe paths, phases 1-3
# ---------------------------------------------------------------------------


def test_full_classification_pipeline(monkeypatch):
    """Single classify_contracts call exercising every detection path and phase.

    Phase 1 coverage:
      - EIP-1167 (bytecode pattern), EIP-1967 (storage), beacon (storage),
        EIP-1822 (storage), OZ legacy (storage), EIP-2535 diamond (facetAddresses),
        custom (implementation() call), heuristic with probe confirmed (Geth),
        heuristic with probe confirmed (Parity fallback), heuristic with probe
        rejected (library), heuristic with probe unavailable (static fallback),
        large bytecode with DELEGATECALL (stays regular).
    Phase 2: implementation/beacon discovery from proxy pointers + probe + facets.
    Phase 3: factory, library, CALL+DELEGATECALL not-library, proxy priority.
    """
    target = ADDR(1)
    eip1167 = ADDR(2)
    eip1967 = ADDR(3)
    beacon_proxy = ADDR(4)
    uups = ADDR(5)
    oz = ADDR(6)
    diamond = ADDR(7)
    custom = ADDR(8)
    geth_proxy = ADDR(9)  # short bytecode — probe confirms via Geth
    parity_proxy = "0x" + "aa" * 20  # short bytecode — Geth fails, Parity confirms
    lib_dep = "0x" + "bb" * 20  # short bytecode — probe rejects (library)
    static_proxy = "0x" + "cc" * 20  # short bytecode — tracing unavailable
    factory = "0x" + "dd" * 20
    not_lib = "0x" + "ee" * 20  # CALL + DELEGATECALL — stays regular
    large_dc = "0x" + "ff" * 20  # large bytecode with DELEGATECALL — regular

    # Implementation / facet addresses
    eip1967_impl = "0x" + "01" * 20
    eip1967_admin = "0x" + "02" * 20
    beacon_addr = "0x" + "03" * 20
    uups_impl = "0x" + "04" * 20
    oz_impl = "0x" + "05" * 20
    facet1 = "0x" + "06" * 20
    facet2 = "0x" + "07" * 20
    custom_impl = "0x" + "08" * 20
    geth_impl = "0x" + "09" * 20
    parity_impl = "0x" + "0a" * 20
    beacon_impl = "0x" + "0b" * 20
    impl_hex = "aabbccddee11223344556677889900aabbccddee"
    eip1167_bc = "0x" + cls.EIP1167_PREFIX + impl_hex + cls.EIP1167_SUFFIX

    short_addrs = {geth_proxy, parity_proxy, lib_dep, static_proxy}

    def fake_code(_rpc, addr):
        if addr == eip1167:
            return eip1167_bc
        if addr in short_addrs:
            return SHORT_BYTECODE
        if addr == large_dc:
            return "0x" + "60" * 400 + "f4"
        return BIG_BYTECODE

    storage = {
        (eip1967, cls.EIP1967_IMPL_SLOT): _slot_for(eip1967_impl),
        (eip1967, cls.EIP1967_ADMIN_SLOT): _slot_for(eip1967_admin),
        (beacon_proxy, cls.EIP1967_BEACON_SLOT): _slot_for(beacon_addr),
        (uups, cls.EIP1822_LOGIC_SLOT): _slot_for(uups_impl),
        (oz, cls.OZ_IMPL_SLOT): _slot_for(oz_impl),
    }

    def fake_rpc(_rpc, method, params, retries=1):
        if method == "eth_getStorageAt":
            return storage.get((params[0], params[1]), ZERO_SLOT)
        if method == "eth_getCode":
            return fake_code(_rpc, params[0])
        if method == "eth_call":
            addr = params[0].get("to", "")
            sel = params[0].get("data", "")[:10]
            if addr == diamond and sel == cls.FACET_ADDRESSES_SELECTOR:
                return _abi_encode_address_array([facet1, facet2])
            if addr == custom and sel == cls.IMPLEMENTATION_SELECTOR:
                return _slot_for(custom_impl)
            if addr == beacon_addr and sel == cls.IMPLEMENTATION_SELECTOR:
                return _slot_for(beacon_impl)
            raise RuntimeError("revert")
        if method in ("debug_traceCall", "trace_call"):
            addr = params[0].get("to", "")
            if addr == geth_proxy and method == "debug_traceCall":
                return {
                    "type": "CALL",
                    "calls": [{"type": "DELEGATECALL", "from": geth_proxy, "to": geth_impl}],
                }
            if addr == lib_dep and method == "debug_traceCall":
                return {"type": "CALL", "calls": []}
            if addr == parity_proxy:
                if method == "debug_traceCall":
                    raise RuntimeError("debug not available")
                return [
                    {
                        "type": "call",
                        "action": {
                            "callType": "delegatecall",
                            "from": parity_proxy,
                            "to": parity_impl,
                        },
                    }
                ]
            raise RuntimeError("tracing unavailable")
        return ZERO_SLOT

    monkeypatch.setattr(cls, "get_code", fake_code)
    monkeypatch.setattr(cls, "rpc_call", fake_rpc)

    edges = [
        {"from": target, "to": lib_dep, "op": "DELEGATECALL"},
        {"from": factory, "to": ADDR(1), "op": "CREATE2"},
        {"from": target, "to": not_lib, "op": "DELEGATECALL"},
        {"from": target, "to": not_lib, "op": "CALL"},
        {
            "from": geth_proxy,
            "to": ADDR(1),
            "op": "CREATE2",
        },  # should NOT override proxy
    ]

    deps = [
        eip1167,
        eip1967,
        beacon_proxy,
        uups,
        oz,
        diamond,
        custom,
        geth_proxy,
        parity_proxy,
        lib_dep,
        static_proxy,
        factory,
        not_lib,
        large_dc,
    ]
    result = cls.classify_contracts(target, deps, RPC, dynamic_edges=edges)
    c = result["classifications"]

    # --- Phase 1: every proxy type ---
    assert c[eip1167]["proxy_type"] == "eip1167"
    assert c[eip1167]["implementation"] == "0x" + impl_hex

    assert c[eip1967]["proxy_type"] == "eip1967"
    assert c[eip1967]["implementation"] == eip1967_impl
    assert c[eip1967]["admin"] == eip1967_admin

    assert c[beacon_proxy]["proxy_type"] == "beacon_proxy"
    assert c[beacon_proxy]["beacon"] == beacon_addr
    assert c[beacon_proxy]["implementation"] == beacon_impl  # resolved through beacon

    assert c[uups]["proxy_type"] == "eip1822"
    assert c[uups]["implementation"] == uups_impl

    assert c[oz]["proxy_type"] == "oz_legacy"
    assert c[oz]["implementation"] == oz_impl

    assert c[diamond]["proxy_type"] == "eip2535"
    assert set(c[diamond]["facets"]) == {facet1, facet2}

    assert c[custom]["proxy_type"] == "custom"
    assert c[custom]["implementation"] == custom_impl

    # Heuristic: probe confirmed (Geth), impl extracted
    assert c[geth_proxy]["proxy_type"] == "unknown"
    assert c[geth_proxy]["implementation"] == geth_impl

    # Heuristic: probe confirmed (Parity fallback), impl extracted
    assert c[parity_proxy]["proxy_type"] == "unknown"
    assert c[parity_proxy]["implementation"] == parity_impl

    # Heuristic: probe rejected — stays regular, Phase 3 marks library
    assert c[lib_dep]["type"] == "library"

    # Heuristic: probe unavailable — static fallback
    assert c[static_proxy]["proxy_type"] == "unknown"

    # Large bytecode with DELEGATECALL — still regular
    assert c[large_dc]["type"] == "regular"

    # --- Phase 2: relational discovery ---
    assert c[eip1967_impl]["type"] == "implementation"
    assert eip1967_impl in result["discovered_addresses"]
    assert c[geth_impl]["type"] == "implementation"
    assert c[parity_impl]["type"] == "implementation"
    assert c[custom_impl]["type"] == "implementation"
    # Beacon classified as beacon (not custom proxy), impl preserved from Phase 1
    assert c[beacon_addr]["type"] == "beacon"
    assert beacon_proxy in c[beacon_addr]["proxies"]
    assert c[beacon_addr]["implementation"] == beacon_impl
    assert "proxy_type" not in c[beacon_addr]  # cleaned up from Phase 1
    # Diamond facets discovered and marked as implementations
    assert facet1 in result["discovered_addresses"]
    assert c[facet1]["type"] == "implementation"
    assert c[facet2]["type"] == "implementation"

    # --- Phase 3: behavioral ---
    assert c[factory]["type"] == "factory"
    assert c[not_lib]["type"] == "regular"  # CALL+DELEGATECALL — not library
    # Proxy not overridden by CREATE2 edge
    assert c[geth_proxy]["type"] == "proxy"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_classify_contracts_handles_rpc_failure(monkeypatch):
    """RPC failure for one address falls back to 'regular', doesn't block others."""

    def fake_classify(addr, _rpc, bytecode=None):
        if addr == ADDR(2):
            raise RuntimeError("RPC error")
        return {"address": addr, "type": "regular"}

    monkeypatch.setattr(cls, "classify_single", fake_classify)

    result = cls.classify_contracts(ADDR(1), [ADDR(2), ADDR(3)], RPC)
    assert result["classifications"][ADDR(2)]["type"] == "regular"
    assert result["classifications"][ADDR(3)]["type"] == "regular"


# ---------------------------------------------------------------------------
# Direct classify_single tests
# ---------------------------------------------------------------------------


def test_classify_single_eip1967_proxy(monkeypatch):
    """classify_single detects an EIP-1967 proxy via storage slots."""
    addr = ADDR(0xA)
    impl = ADDR(0xB)
    admin = ADDR(0xC)

    storage = {
        (addr, cls.EIP1967_IMPL_SLOT): _slot_for(impl),
        (addr, cls.EIP1967_ADMIN_SLOT): _slot_for(admin),
    }

    monkeypatch.setattr(cls, "get_code", lambda _rpc, _addr: BIG_BYTECODE)
    monkeypatch.setattr(
        cls,
        "rpc_call",
        lambda _rpc, method, params, retries=1: (
            storage.get((params[0], params[1]), ZERO_SLOT)
            if method == "eth_getStorageAt"
            else (_ for _ in ()).throw(RuntimeError("unexpected"))
        ),
    )

    result = cls.classify_single(addr, RPC)
    assert result["type"] == "proxy"
    assert result["proxy_type"] == "eip1967"
    assert result["implementation"] == impl
    assert result["admin"] == admin


def test_classify_single_eip1167(monkeypatch):
    """classify_single detects an EIP-1167 minimal proxy from bytecode."""
    impl_hex = "aabbccddee11223344556677889900aabbccddee"
    bytecode = "0x" + cls.EIP1167_PREFIX + impl_hex + cls.EIP1167_SUFFIX
    addr = ADDR(0xD)

    monkeypatch.setattr(cls, "get_code", lambda _rpc, _addr: bytecode)

    result = cls.classify_single(addr, RPC)
    assert result["type"] == "proxy"
    assert result["proxy_type"] == "eip1167"
    assert result["implementation"] == "0x" + impl_hex


def test_classify_single_regular(monkeypatch):
    """classify_single returns 'regular' when no proxy pattern is found."""
    addr = ADDR(0xE)

    monkeypatch.setattr(cls, "get_code", lambda _rpc, _addr: BIG_BYTECODE)
    monkeypatch.setattr(
        cls,
        "rpc_call",
        lambda _rpc, method, params, retries=1: (
            ZERO_SLOT if method == "eth_getStorageAt" else (_ for _ in ()).throw(RuntimeError("revert"))
        ),
    )

    result = cls.classify_single(addr, RPC)
    assert result["type"] == "regular"


def test_classify_single_with_bytecode_param(monkeypatch):
    """Passing bytecode= skips the get_code call."""
    impl_hex = "aabbccddee11223344556677889900aabbccddee"
    bytecode = "0x" + cls.EIP1167_PREFIX + impl_hex + cls.EIP1167_SUFFIX

    monkeypatch.setattr(cls, "get_code", lambda *a: (_ for _ in ()).throw(AssertionError("should not be called")))
    result = cls.classify_single(ADDR(0xF), RPC, bytecode=bytecode)
    assert result["type"] == "proxy"
    assert result["implementation"] == "0x" + impl_hex


# ---------------------------------------------------------------------------
# Live RPC
# ---------------------------------------------------------------------------


def test_live_classify_usdc_proxy():
    """USDC on Ethereum mainnet is a well-known proxy."""
    rpc_url = os.environ.get("ETH_RPC")
    if not rpc_url:
        pytest.skip("Set ETH_RPC before running this test.")
    try:
        from services.discovery.static_dependencies import rpc_call

        rpc_call(rpc_url, "eth_blockNumber", [], retries=0)
    except Exception as exc:
        pytest.skip(f"RPC unreachable: {exc}")

    usdc_proxy = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    result = cls.classify_single(usdc_proxy, rpc_url)
    assert result["type"] == "proxy"
    assert "implementation" in result
