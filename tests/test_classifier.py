import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services import classifier as cls

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def ADDR(n):
    return f"0x{str(n) * 40}"


RPC = "https://rpc.example"
BIG_BYTECODE = "0x" + "60" * 500
ZERO_SLOT = "0x" + "0" * 64


def _slot_for(addr: str) -> str:
    """Pad a 20-byte address into a 32-byte storage slot value."""
    return "0x" + "0" * 24 + addr[2:]


# ---------------------------------------------------------------------------
# Helpers: _slot_to_address, detect_eip1167, _bytecode_has_delegatecall
# ---------------------------------------------------------------------------


def test_helper_functions():
    # _slot_to_address
    assert cls._slot_to_address(ZERO_SLOT) is None
    assert cls._slot_to_address("0x") is None
    assert cls._slot_to_address("0x0") is None
    assert cls._slot_to_address(None) is None  # type: ignore[arg-type]
    assert cls._slot_to_address("") is None
    assert cls._slot_to_address(_slot_for(ADDR(2))) == ADDR(2)
    assert cls._slot_to_address("0x1") == "0x0000000000000000000000000000000000000001"

    # detect_eip1167
    impl = "aabbccddee11223344556677889900aabbccddee"
    assert (
        cls.detect_eip1167("0x" + cls.EIP1167_PREFIX + impl + cls.EIP1167_SUFFIX)
        == "0x" + impl
    )
    assert cls.detect_eip1167("0x60016000") is None
    assert cls.detect_eip1167("0x") is None

    # _bytecode_has_delegatecall
    assert cls._bytecode_has_delegatecall("0x6000f4") is True  # real DELEGATECALL
    assert cls._bytecode_has_delegatecall("0x6000600100") is False  # no DELEGATECALL
    assert cls._bytecode_has_delegatecall("0x61f400") is False  # 0xf4 inside PUSH2 data
    assert (
        cls._bytecode_has_delegatecall("0x61f400f4") is True
    )  # PUSH2 data then real DELEGATECALL
    assert cls._bytecode_has_delegatecall("0x") is False
    assert cls._bytecode_has_delegatecall("") is False


# ---------------------------------------------------------------------------
# classify_single: all proxy detection methods + regular fallback
# ---------------------------------------------------------------------------


def test_classify_single_detects_all_proxy_types(monkeypatch):
    """Each proxy standard is detected with correct proxy_type and metadata."""
    cases = []

    # EIP-1167 (bytecode pattern — no storage reads needed)
    impl_hex = "aabbccddee11223344556677889900aabbccddee"
    eip1167_bytecode = "0x" + cls.EIP1167_PREFIX + impl_hex + cls.EIP1167_SUFFIX
    cases.append(("eip1167", eip1167_bytecode, {}, {"implementation": "0x" + impl_hex}))

    # EIP-1967 with admin
    cases.append(
        (
            "eip1967",
            BIG_BYTECODE,
            {
                cls.EIP1967_IMPL_SLOT: _slot_for(ADDR(3)),
                cls.EIP1967_ADMIN_SLOT: _slot_for(ADDR(4)),
            },
            {"implementation": ADDR(3), "admin": ADDR(4)},
        )
    )

    # Beacon proxy
    cases.append(
        (
            "beacon_proxy",
            BIG_BYTECODE,
            {
                cls.EIP1967_BEACON_SLOT: _slot_for(ADDR(5)),
            },
            {"beacon": ADDR(5)},
        )
    )

    # EIP-1822 UUPS
    cases.append(
        (
            "eip1822",
            BIG_BYTECODE,
            {
                cls.EIP1822_LOGIC_SLOT: _slot_for(ADDR(6)),
            },
            {"implementation": ADDR(6)},
        )
    )

    # OpenZeppelin legacy
    cases.append(
        (
            "oz_legacy",
            BIG_BYTECODE,
            {
                cls.OZ_IMPL_SLOT: _slot_for(ADDR(7)),
            },
            {"implementation": ADDR(7)},
        )
    )

    # Heuristic: short bytecode with DELEGATECALL
    short_bytecode = "0x" + "6000" * 10 + "f4" + "00" * 5
    cases.append(("unknown", short_bytecode, {}, {}))

    for proxy_type, bytecode, slots, expected_meta in cases:
        monkeypatch.setattr(cls, "get_code", lambda _r, _a, bc=bytecode: bc)
        monkeypatch.setattr(
            cls,
            "get_storage_at",
            lambda _r, _a, slot, s=slots: s.get(slot, ZERO_SLOT),
        )
        result = cls.classify_single(ADDR(1), RPC)
        assert result["type"] == "proxy", f"Failed for {proxy_type}"
        assert result["proxy_type"] == proxy_type, (
            f"Expected {proxy_type}, got {result['proxy_type']}"
        )
        for key, val in expected_meta.items():
            assert result[key] == val, f"{key} mismatch for {proxy_type}"


def test_classify_single_regular_and_large_delegatecall(monkeypatch):
    """Regular contract and large bytecode with DELEGATECALL are both 'regular'."""
    monkeypatch.setattr(cls, "get_storage_at", lambda _r, _a, _s: ZERO_SLOT)

    # Normal large bytecode
    monkeypatch.setattr(cls, "get_code", lambda _r, _a: BIG_BYTECODE)
    assert cls.classify_single(ADDR(1), RPC)["type"] == "regular"

    # Large bytecode WITH delegatecall — still regular (exceeds heuristic threshold)
    monkeypatch.setattr(cls, "get_code", lambda _r, _a: "0x" + "60" * 400 + "f4")
    assert cls.classify_single(ADDR(1), RPC)["type"] == "regular"


# ---------------------------------------------------------------------------
# classify_contracts: Phase 2 (relational) + address discovery
# ---------------------------------------------------------------------------


def test_classify_contracts_relational(monkeypatch):
    """Proxies mark their targets as implementation/beacon; new addresses are discovered."""
    proxy = ADDR(1)
    impl = ADDR(2)
    beacon_proxy = ADDR(3)
    beacon = ADDR(4)
    new_impl = ADDR(5)  # not in original dep list

    monkeypatch.setattr(cls, "get_code", lambda _r, _a: BIG_BYTECODE)
    monkeypatch.setattr(
        cls,
        "_try_implementation_call",
        lambda _r, _a: None,
    )

    def fake_storage(_r, addr, slot):
        if addr == proxy and slot == cls.EIP1967_IMPL_SLOT:
            return _slot_for(impl)
        if addr == beacon_proxy and slot == cls.EIP1967_BEACON_SLOT:
            return _slot_for(beacon)
        # A dep that's a proxy pointing to an address NOT in the dep list
        if addr == ADDR(6) and slot == cls.EIP1967_IMPL_SLOT:
            return _slot_for(new_impl)
        return ZERO_SLOT

    monkeypatch.setattr(cls, "get_storage_at", fake_storage)

    result = cls.classify_contracts(
        ADDR(9), [proxy, impl, beacon_proxy, beacon, ADDR(6)], RPC
    )

    assert result["classifications"][proxy]["type"] == "proxy"
    assert result["classifications"][impl]["type"] == "implementation"
    assert proxy in result["classifications"][impl]["proxies"]
    assert result["classifications"][beacon_proxy]["type"] == "proxy"
    assert result["classifications"][beacon_proxy]["proxy_type"] == "beacon_proxy"
    assert result["classifications"][beacon]["type"] == "beacon"
    # new_impl discovered via proxy slot, not in original dep list
    assert new_impl in result["discovered_addresses"]
    assert new_impl in result["classifications"]
    assert result["classifications"][new_impl]["type"] == "implementation"


# ---------------------------------------------------------------------------
# classify_contracts: Phase 3 (behavioral) + priority
# ---------------------------------------------------------------------------


def test_classify_contracts_behavioral(monkeypatch):
    """Dynamic edges classify factory/library; CALL+DELEGATECALL prevents library label."""
    target = ADDR(1)
    factory = ADDR(2)
    lib = ADDR(3)
    not_lib = ADDR(4)  # called via both DELEGATECALL and CALL

    monkeypatch.setattr(cls, "get_code", lambda _r, _a: BIG_BYTECODE)
    monkeypatch.setattr(cls, "get_storage_at", lambda _r, _a, _s: ZERO_SLOT)

    edges = [
        {"from": factory, "to": ADDR(8), "op": "CREATE2"},
        {"from": target, "to": lib, "op": "DELEGATECALL"},
        {"from": target, "to": not_lib, "op": "DELEGATECALL"},
        {"from": target, "to": not_lib, "op": "CALL"},
    ]
    result = cls.classify_contracts(
        target, [factory, lib, not_lib], RPC, dynamic_edges=edges
    )

    assert result["classifications"][factory]["type"] == "factory"
    assert result["classifications"][lib]["type"] == "library"
    assert result["classifications"][not_lib]["type"] == "regular"


def test_proxy_not_overridden_by_behavioral(monkeypatch):
    """Phase 1 proxy classification takes priority over Phase 3 behavioral."""
    target = ADDR(1)
    proxy_dep = ADDR(2)
    impl = ADDR(3)

    monkeypatch.setattr(cls, "get_code", lambda _r, _a: BIG_BYTECODE)

    def fake_storage(_r, addr, slot):
        if addr == proxy_dep and slot == cls.EIP1967_IMPL_SLOT:
            return _slot_for(impl)
        return ZERO_SLOT

    monkeypatch.setattr(cls, "get_storage_at", fake_storage)

    # Dynamic edges show proxy_dep using CREATE2 — should NOT override proxy
    edges = [{"from": proxy_dep, "to": ADDR(8), "op": "CREATE2"}]
    result = cls.classify_contracts(target, [proxy_dep, impl], RPC, dynamic_edges=edges)

    assert result["classifications"][proxy_dep]["type"] == "proxy"
    assert result["classifications"][impl]["type"] == "implementation"


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
# Live RPC (skip-guarded)
# ---------------------------------------------------------------------------


def test_live_classify_usdc_proxy():
    """USDC on Ethereum mainnet is a well-known proxy."""
    rpc_url = os.environ.get("ETH_RPC")
    if not rpc_url:
        pytest.skip("Set ETH_RPC before running this test.")
    try:
        from services.dependent_contracts import rpc_call

        rpc_call(rpc_url, "eth_blockNumber", [], retries=0)
    except Exception as exc:
        pytest.skip(f"RPC unreachable: {exc}")

    usdc_proxy = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    result = cls.classify_single(usdc_proxy, rpc_url)
    assert result["type"] == "proxy"
    assert "implementation" in result
