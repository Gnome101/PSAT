from __future__ import annotations

from pathlib import Path
from typing import Any

from eth_abi.abi import encode

from services.bridges import runtime


def _encoded_bytes32(value: str) -> str:
    return "0x" + encode(["bytes32"], [bytes.fromhex(value.removeprefix("0x"))]).hex()


def _encoded_address(value: str) -> str:
    return "0x" + encode(["address"], [value]).hex()


def _encoded_address_bool(value: str, flag: bool) -> str:
    return "0x" + encode(["address", "bool"], [value, flag]).hex()


def test_layerzero_registry_is_generated_from_sdk_data() -> None:
    entries = runtime.layerzero_eid_registry()
    eids = {entry["eid"] for entry in entries}

    assert len(entries) > 100
    assert len(eids) == len(entries)
    assert {30101, 30111, 30183, 30184, 30214}.issubset(eids)


def test_read_layerzero_registry_dedupes_and_sorts(tmp_path: Path) -> None:
    registry = tmp_path / "layerzero_eids.json"
    registry.write_text(
        """
        {
          "eids": [
            {"eid": 30184, "network": "base-mainnet", "chain": "base", "chain_type": "evm"},
            {"eid": "30101", "network": "ethereum-mainnet", "chain": "ethereum", "chain_type": "evm"},
            {"eid": 30184, "network": "duplicate-mainnet", "chain": "duplicate", "chain_type": "evm"},
            {"network": "missing-eid"}
          ]
        }
        """,
        encoding="utf-8",
    )

    assert runtime._read_layerzero_eid_registry(registry) == (
        {"eid": 30101, "network": "ethereum-mainnet", "chain": "ethereum", "chain_type": "evm"},
        {"eid": 30184, "network": "base-mainnet", "chain": "base", "chain_type": "evm"},
    )


def test_layerzero_runtime_uses_registry_entries_for_peer_discovery(monkeypatch) -> None:
    oapp = "0x" + "11" * 20
    endpoint = "0x" + "22" * 20
    peer = "0x" + "33" * 20
    send_library = "0x" + "44" * 20
    receive_library = "0x" + "55" * 20
    owner = "0x" + "66" * 20
    custom_eid = 39999

    monkeypatch.setattr(
        runtime,
        "layerzero_eid_registry",
        lambda: (
            {
                "eid": custom_eid,
                "network": "custom-mainnet",
                "chain": "custom",
                "chain_type": "evm",
            },
        ),
    )

    def fake_batch(_rpc: str, calls: list[tuple[str, list[Any]]]) -> list[str | None]:
        first_target = calls[0][1][0]["to"]
        if first_target == oapp:
            return [_encoded_bytes32("0x" + ("00" * 12) + peer[2:])] + [None] * (len(calls) - 1)
        if first_target == endpoint and len(calls) == 2:
            return [_encoded_address(send_library), _encoded_address_bool(receive_library, True)]
        return [None] * len(calls)

    monkeypatch.setattr(runtime, "rpc_batch_request", fake_batch)

    def fake_call(
        _rpc_url: str,
        address: str,
        signature: str,
        arg_types: list[str] | None = None,
        args: list[Any] | None = None,
        output_types: list[str] | None = None,
    ) -> tuple[Any, ...] | None:
        if address == oapp and signature == "endpoint()":
            return (endpoint,)
        if address == oapp and signature == "owner()":
            return (owner,)
        if address == endpoint and signature == "eid()":
            return (30101,)
        if address == endpoint and signature == "delegates(address)":
            return (runtime.ZERO_ADDRESS,)
        if address == endpoint and signature == "getSendLibrary(address,uint32)":
            return (send_library,)
        if address == endpoint and signature == "getReceiveLibrary(address,uint32)":
            return (receive_library, True)
        return None

    monkeypatch.setattr(runtime, "_call", fake_call)

    resolved = runtime.resolve_layerzero_runtime(
        rpc_url="https://rpc.invalid",
        contract={"address": oapp, "name": "OApp", "bridge_context": {"protocols": ["LayerZero"]}},
        functions=[],
    )

    assert resolved["status"] == "resolved"
    route = resolved["routes"][0]
    expected = {
        "eid": custom_eid,
        "chain": "custom",
        "network": "custom-mainnet",
        "chain_type": "evm",
        "peer": peer,
        "peer_address": peer,
        "peer_bytes32": "0x" + ("00" * 12) + peer[2:],
        "peer_source": "getReceiver(uint32)",
        "send_library": send_library,
        "receive_library": receive_library,
        "receive_library_default": True,
        "send_uln": None,
        "receive_uln": None,
        "executor": None,
    }
    for key, value in expected.items():
        assert route[key] == value
    assert route["protocol"] == "LayerZero"
    assert route["peer_analysis"]["status"] == "not_queued"
    assert resolved["policies"] == [
        {"label": "owner controls local app admin functions", "address": owner, "source": "owner()"}
    ]


def test_layerzero_runtime_keeps_non_evm_peer_bytes32(monkeypatch) -> None:
    oapp = "0x" + "11" * 20
    endpoint = "0x" + "22" * 20
    non_evm_peer = "0x" + "aa" * 32

    monkeypatch.setattr(
        runtime,
        "layerzero_eid_registry",
        lambda: (
            {
                "eid": 30108,
                "network": "aptos-mainnet",
                "chain": "aptos",
                "chain_type": "aptos",
            },
        ),
    )

    def fake_batch(_rpc: str, calls: list[tuple[str, list[Any]]]) -> list[str | None]:
        first_target = calls[0][1][0]["to"]
        if first_target == oapp:
            return [_encoded_bytes32(non_evm_peer), None]
        return [None] * len(calls)

    monkeypatch.setattr(runtime, "rpc_batch_request", fake_batch)

    def fake_call(
        _rpc_url: str,
        address: str,
        signature: str,
        arg_types: list[str] | None = None,
        args: list[Any] | None = None,
        output_types: list[str] | None = None,
    ) -> tuple[Any, ...] | None:
        if address == oapp and signature == "endpoint()":
            return (endpoint,)
        if address == endpoint and signature == "eid()":
            return (30101,)
        return None

    monkeypatch.setattr(runtime, "_call", fake_call)

    resolved = runtime.resolve_layerzero_runtime(
        rpc_url="https://rpc.invalid",
        contract={"address": oapp, "name": "OApp", "bridge_context": {"protocols": ["LayerZero"]}},
        functions=[],
    )

    route = resolved["routes"][0]
    assert route["peer"] == non_evm_peer
    assert route["peer_address"] is None
    assert route["peer_bytes32"] == non_evm_peer
    assert route["chain_type"] == "aptos"


def test_hyperlane_runtime_resolves_router_peer_and_ism(monkeypatch) -> None:
    router = "0x" + "11" * 20
    mailbox = "0x" + "22" * 20
    ism = "0x" + "33" * 20
    peer = "0x" + "44" * 20

    monkeypatch.setattr(
        runtime,
        "hyperlane_domain_entries",
        lambda: (
            {
                "domain": 8453,
                "chain": "base",
                "display_name": "Base",
                "chain_id": 8453,
            },
        ),
    )

    def fake_call(
        _rpc_url: str,
        address: str,
        signature: str,
        arg_types: list[str] | None = None,
        args: list[Any] | None = None,
        output_types: list[str] | None = None,
    ) -> tuple[Any, ...] | None:
        if address == router and signature == "mailbox()":
            return (mailbox,)
        if address == mailbox and signature == "localDomain()":
            return (1,)
        if address == router and signature == "interchainSecurityModule()":
            return (ism,)
        if address == router and signature == "routers(uint32)" and args == [8453]:
            return (bytes.fromhex(("00" * 12) + peer[2:]),)
        if address == ism and signature == "moduleType()":
            return (3,)
        return None

    monkeypatch.setattr(runtime, "_call", fake_call)

    resolved = runtime.resolve_bridge_runtime(
        rpc_url="https://rpc.invalid",
        contract={"address": router, "name": "Router", "bridge_static_context": {"protocols": ["Hyperlane"]}},
        functions=[],
    )

    assert resolved["status"] == "resolved"
    assert resolved["protocol"] == "Hyperlane"
    assert resolved["mailbox"] == {"address": mailbox, "local_domain": 1}
    assert resolved["policies"][0]["address"] == ism
    assert resolved["policies"][0]["module_type"] == 3
    assert resolved["routes"][0]["peer_address"] == peer
    assert resolved["routes"][0]["chain"] == "base"


def test_wormhole_runtime_reports_protocol_specific_unsupported() -> None:
    resolved = runtime.resolve_bridge_runtime(
        rpc_url="https://rpc.invalid",
        contract={"address": "0x" + "11" * 20, "name": "WormholeAdapter", "standards": ["Bridge", "Wormhole"]},
        functions=[],
    )

    assert resolved["status"] == "unsupported_runtime"
    assert resolved["protocol"] == "Wormhole"
    assert "Wormhole" in resolved["reason"]
