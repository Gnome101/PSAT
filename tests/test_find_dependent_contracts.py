import os

import pytest

from services import dependent_contracts as fdc


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


# Verifies public auto-discovery against a known mainnet contract so the default no-custom-RPC path is exercised end-to-end.
def test_public_auto_discovery_live_rpc():
    # 1inch Aggregation Router V5 on Ethereum mainnet
    contract = "0x1111111254eeb25477b68fb85ed929f73a960582"
    # Wrapped Ether (WETH) on Ethereum mainnet
    expected = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    try:
        network, rpc_url = fdc.resolve_rpc_for_address(contract)
    except RuntimeError as exc:
        pytest.skip(f"Public RPC unavailable in this environment: {exc}")

    deps = set(fdc.discover_dependencies(rpc_url, contract))
    assert network == "ethereum"
    assert expected in deps


# Verifies dependency extraction with a user-supplied live RPC so custom endpoint support remains functional.
def test_public_dependencies_live_rpc():
    rpc_url = os.environ.get("LIVE_RPC_URL")
    if not rpc_url:
        pytest.skip("Set LIVE_RPC_URL before running this test.")

    # 1inch Aggregation Router V5 on Ethereum mainnet
    contract = "0x1111111254eeb25477b68fb85ed929f73a960582"
    # Wrapped Ether (WETH) on Ethereum mainnet
    expected = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    deps = set(fdc.discover_dependencies(rpc_url, contract))
    assert expected in deps
