import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.discovery import static_dependencies as fdc

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


# ---------------------------------------------------------------------------
# Core helpers: normalize_address, extract_push20_addresses
# ---------------------------------------------------------------------------


def test_normalize_address_and_extract_push20():
    # normalize_address: strips prefix, lowercases
    assert fdc.normalize_address("0xAbCd" + "0" * 36) == "0xabcd" + "0" * 36
    assert fdc.normalize_address("AbCd" + "0" * 36) == "0xabcd" + "0" * 36

    # has_deployed_code
    assert fdc.has_deployed_code("0x60016000") is True
    assert fdc.has_deployed_code("0x") is False
    assert fdc.has_deployed_code("0x0") is False

    # extract_push20_addresses: empty / no PUSH20
    assert fdc.extract_push20_addresses("0x") == set()
    assert fdc.extract_push20_addresses("0x6001") == set()

    # extract_push20_addresses: single embedded address
    addr = "aabbccddee11223344556677889900aabbccddee"
    bytecode = "0x73" + addr + "60"  # PUSH20 <addr> PUSH1
    result = fdc.extract_push20_addresses(bytecode)
    assert "0x" + addr in result

    # extract_push20_addresses: filters zero address
    zero_addr = "0" * 40
    bytecode = "0x73" + zero_addr + "73" + addr + "00"
    result = fdc.extract_push20_addresses(bytecode)
    assert "0x" + zero_addr not in result
    assert "0x" + addr in result

    # extract_push20_addresses: skips PUSH data correctly
    # PUSH32 (0x7f) has 32 bytes of data — a PUSH20 opcode inside that data should be ignored
    push32_data = "73" + addr + "00" * 11  # 0x73 inside PUSH32 data (32 bytes total)
    bytecode = "0x7f" + push32_data
    result = fdc.extract_push20_addresses(bytecode)
    assert result == set()

    # extract_push20_addresses: multiple addresses
    addr2 = "1122334455667788990011223344556677889900"
    bytecode = "0x73" + addr + "73" + addr2 + "00"
    result = fdc.extract_push20_addresses(bytecode)
    assert result == {"0x" + addr, "0x" + addr2}

    # extract_push20_addresses: odd-length hex returns empty
    assert fdc.extract_push20_addresses("0x600") == set()


# ---------------------------------------------------------------------------
# RPC resolution
# ---------------------------------------------------------------------------


# Verifies auto-discovery tries multiple public networks so contracts can be found without custom RPC input.
def test_resolve_rpc_for_address_auto_discovers_public_rpc(monkeypatch):
    monkeypatch.setattr(
        fdc,
        "DEFAULT_PUBLIC_RPCS_BY_NETWORK",
        {
            "first": ("https://rpc1.example",),
            "second": ("https://rpc2.example",),
        },
    )

    first_rpc = "https://rpc1.example"
    second_network, second_rpc = "second", "https://rpc2.example"
    calls = []

    def fake_get_code(rpc_url, _address):
        calls.append(rpc_url)
        if rpc_url == first_rpc:
            return "0x"
        if rpc_url == second_rpc:
            return "0x60016000"
        return "0x"

    monkeypatch.setattr(fdc, "get_code", fake_get_code)
    network, rpc_url = fdc.resolve_rpc_for_address("0x1111111111111111111111111111111111111111")

    assert network == second_network
    assert rpc_url == second_rpc
    assert calls == [first_rpc, second_rpc]


# Verifies fallback endpoint behavior so transient RPC failures do not break auto-discovery on the same network.
def test_resolve_rpc_for_address_uses_backup_endpoint_on_error(monkeypatch):
    monkeypatch.setattr(
        fdc,
        "DEFAULT_PUBLIC_RPCS_BY_NETWORK",
        {"ethereum": ("https://rpc-bad.example", "https://rpc-good.example")},
    )

    calls = []

    def fake_get_code(rpc_url, _address):
        calls.append(rpc_url)
        if rpc_url == "https://rpc-bad.example":
            raise RuntimeError("temporary failure")
        if rpc_url == "https://rpc-good.example":
            return "0x60016000"
        return "0x"

    monkeypatch.setattr(fdc, "get_code", fake_get_code)
    network, rpc_url = fdc.resolve_rpc_for_address("0x1111111111111111111111111111111111111111")

    assert network == "ethereum"
    assert rpc_url == "https://rpc-good.example"
    assert calls == ["https://rpc-bad.example", "https://rpc-good.example"]


# Verifies explicit --rpc validation so we fail fast when a provided endpoint has no deployed contract code.
def test_resolve_rpc_for_address_custom_rpc_requires_contract(monkeypatch):
    monkeypatch.setattr(fdc, "get_code", lambda _rpc, _address: "0x")
    with pytest.raises(RuntimeError, match="has no deployed bytecode"):
        fdc.resolve_rpc_for_address(
            "0x1111111111111111111111111111111111111111",
            "https://my-rpc.example",
        )


# Verifies ETH_RPC fallback behavior so a throttled .env endpoint can gracefully fall back to public discovery.
def test_find_dependencies_falls_back_when_env_rpc_fails(monkeypatch):
    env_rpc = "https://env-rpc.example"
    monkeypatch.setenv("ETH_RPC", env_rpc)
    monkeypatch.setattr(fdc, "load_dotenv", lambda _path: None)

    calls = []

    def fake_resolve(_address, rpc_url):
        calls.append(rpc_url)
        if rpc_url == env_rpc:
            raise RuntimeError("rate limited")
        return "ethereum", "https://public-rpc.example"

    monkeypatch.setattr(fdc, "resolve_rpc_for_address", fake_resolve)
    monkeypatch.setattr(
        fdc,
        "discover_dependencies",
        lambda _rpc_url, _root, code_cache=None: ["0x2222222222222222222222222222222222222222"],
    )

    out = fdc.find_dependencies("0x1111111111111111111111111111111111111111")
    assert out["rpc"] == "https://public-rpc.example"
    assert calls == [env_rpc, None]


# Verifies explicit RPC input remains strict and does not silently fall back to public endpoints.
def test_find_dependencies_explicit_rpc_does_not_fallback(monkeypatch):
    monkeypatch.setattr(fdc, "load_dotenv", lambda _path: None)
    monkeypatch.setattr(
        fdc,
        "resolve_rpc_for_address",
        lambda _address, _rpc_url: (_ for _ in ()).throw(RuntimeError("bad rpc")),
    )

    with pytest.raises(RuntimeError, match="bad rpc"):
        fdc.find_dependencies(
            "0x1111111111111111111111111111111111111111",
            "https://explicit-rpc.example",
        )


# ---------------------------------------------------------------------------
# Mocked BFS traversal
# ---------------------------------------------------------------------------


def test_discover_dependencies_bfs_mocked(monkeypatch):
    """BFS discovers transitive deps, handles back-references, and excludes EOAs."""
    root = "0x1111111111111111111111111111111111111111"
    dep_a = "0x2222222222222222222222222222222222222222"
    dep_b = "0x3333333333333333333333333333333333333333"
    dep_c = "0x4444444444444444444444444444444444444444"

    def _bc(*addrs: str) -> str:
        """Build minimal bytecode with PUSH20 for each address."""
        return "0x" + "".join("73" + a[2:] for a in addrs) + "00"

    code_map = {
        root: _bc(dep_a, dep_b),
        dep_a: _bc(dep_c),
        dep_b: _bc(dep_a),  # back-reference — must not loop
        dep_c: "0x6000",  # no PUSH20
    }

    def fake_rpc(_rpc, method, params, retries=1):
        if method == "eth_getCode":
            return code_map.get(fdc.normalize_address(params[0]), "0x")
        raise RuntimeError(f"unexpected: {method}")

    monkeypatch.setattr(fdc, "rpc_call", fake_rpc)

    deps = fdc.discover_dependencies("https://rpc.example", root)
    assert sorted(deps) == sorted([dep_a, dep_b, dep_c])


def test_discover_dependencies_raises_on_empty_root(monkeypatch):
    """discover_dependencies raises if root has no deployed bytecode."""
    monkeypatch.setattr(fdc, "rpc_call", lambda *a, **kw: "0x")
    with pytest.raises(RuntimeError, match="no deployed bytecode"):
        fdc.discover_dependencies("https://rpc.example", "0x" + "11" * 20)


# ---------------------------------------------------------------------------
# Live RPC
# ---------------------------------------------------------------------------


# Verifies public auto-discovery against a known mainnet contract
# so the default no-custom-RPC path is exercised end-to-end.
def test_public_auto_discovery_live_rpc():
    # 1inch Aggregation Router V5 on Ethereum mainnet
    contract = "0x1111111254eeb25477b68fb85ed929f73a960582"
    # Wrapped Ether (WETH) on Ethereum mainnet
    expected = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    # Prefer ETH_RPC when available to avoid public RPC rate limits
    # during full test suite runs; fall back to auto-discovery otherwise.
    env_rpc = os.environ.get("ETH_RPC")
    try:
        network, rpc_url = fdc.resolve_rpc_for_address(contract, env_rpc)
    except RuntimeError as exc:
        pytest.skip(f"RPC unavailable in this environment: {exc}")

    deps = set(fdc.discover_dependencies(rpc_url, contract))
    # Network is "ethereum" via public RPC auto-discovery, "custom" when ETH_RPC is used
    assert network in ("ethereum", "custom")
    assert expected in deps


# Verifies dependency extraction with a user-supplied live RPC so custom endpoint support remains functional.
def test_public_dependencies_live_rpc():
    rpc_url = os.environ.get("ETH_RPC")
    if not rpc_url:
        pytest.skip("Set ETH_RPC before running this test.")

    # 1inch Aggregation Router V5 on Ethereum mainnet
    contract = "0x1111111254eeb25477b68fb85ed929f73a960582"
    # Wrapped Ether (WETH) on Ethereum mainnet
    expected = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    deps = set(fdc.discover_dependencies(rpc_url, contract))
    assert expected in deps
