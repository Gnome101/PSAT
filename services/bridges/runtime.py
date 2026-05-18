"""Runtime bridge configuration resolvers.

Static labels can say "this function updates bridge config", but the useful
answer is the current configured state: peers, endpoint, libraries, DVNs, and
executor limits. This module keeps that live-read logic isolated from the
company aggregation path so `/api/company` stays cheap and the UI resolves
bridge details only when a bridge contract is selected.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from eth_abi.abi import decode, encode
from eth_utils.crypto import keccak

from services.bridges.chains import (
    chain_id_for_chain,
    chain_name_for_chain_id,
    display_name_for_chain,
    hyperlane_domain_entries,
    normalize_chain_name,
)
from utils.rpc import rpc_batch_request, rpc_request

ZERO_ADDRESS = "0x" + "0" * 40
ZERO_BYTES32 = "0x" + "0" * 64
_LAYERZERO_EID_REGISTRY_PATH = Path(__file__).resolve().parents[2] / "data" / "layerzero_eids.json"

_ULN_CONFIG_TYPE = 2
_EXECUTOR_CONFIG_TYPE = 1
_OP_STACK_NAMES = (
    "optimismportal",
    "superchainconfig",
    "anchorstateregistry",
    "systemconfig",
    "crossdomainmessenger",
    "standardbridge",
)


class BridgeRuntimeError(RuntimeError):
    """Raised for non-fatal resolver failures."""


def _selector(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()[:8]


def _calldata(
    signature: str,
    arg_types: list[str] | None = None,
    args: list[Any] | None = None,
) -> str:
    arg_types = arg_types or []
    args = args or []
    return _selector(signature) + (encode(arg_types, args).hex() if arg_types else "")


def _decode_output(output_types: list[str], raw: str | None) -> tuple[Any, ...] | None:
    if not isinstance(raw, str) or raw in {"0x", "0x0"}:
        return None
    try:
        return decode(output_types, bytes.fromhex(raw[2:]))
    except Exception:
        return None


def _call(
    rpc_url: str,
    address: str,
    signature: str,
    arg_types: list[str] | None = None,
    args: list[Any] | None = None,
    output_types: list[str] | None = None,
) -> tuple[Any, ...] | None:
    output_types = output_types or []
    try:
        raw = rpc_request(
            rpc_url,
            "eth_call",
            [{"to": address, "data": _calldata(signature, arg_types, args)}, "latest"],
            retries=0,
        )
    except Exception:
        return None
    return _decode_output(output_types, raw)


def _protocol_result(
    *,
    status: str,
    protocol: str,
    reason: str | None = None,
    routes: list[dict[str, Any]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    out = {
        "status": status,
        "protocol": protocol,
        "protocols": [protocol],
        "routes": routes or [],
    }
    if reason:
        out["reason"] = reason
    out.update(extra)
    return out


def _is_op_stack_contract(contract: dict[str, Any]) -> bool:
    name = str(contract.get("name") or contract.get("contract_name") or "").lower().replace("_", "")
    standards = {str(item) for item in contract.get("standards") or []}
    return "OP Stack" in standards or any(marker in name for marker in _OP_STACK_NAMES)


def _route_chain_fields(chain: str | None) -> dict[str, Any]:
    return {
        "chain": chain,
        "chain_id": chain_id_for_chain(chain),
        "chain_display_name": display_name_for_chain(chain),
    }


def _default_peer_analysis(route: dict[str, Any]) -> dict[str, Any]:
    if route.get("peer_address"):
        return {"status": "not_queued"}
    if route.get("peer"):
        return {"status": "non_evm_peer"}
    return {"status": "not_applicable"}


def _finalize_route(route: dict[str, Any], *, protocol: str) -> dict[str, Any]:
    finalized = {
        **route,
        "protocol": protocol,
        **_route_chain_fields(str(route.get("chain")) if route.get("chain") else None),
    }
    finalized.setdefault("peer_analysis", _default_peer_analysis(finalized))
    return finalized


def _batch_call(
    rpc_url: str,
    specs: list[tuple[str, str, list[str], list[Any], list[str]]],
) -> list[tuple[Any, ...] | None]:
    if not specs:
        return []
    calls = [
        ("eth_call", [{"to": address, "data": _calldata(signature, arg_types, args)}, "latest"])
        for address, signature, arg_types, args, _output_types in specs
    ]
    try:
        raw_results = rpc_batch_request(rpc_url, calls)
    except Exception:
        return [
            _call(
                rpc_url,
                address,
                signature,
                arg_types,
                args,
                output_types,
            )
            for address, signature, arg_types, args, output_types in specs
        ]
    return [
        _decode_output(output_types, raw)
        for raw, (_address, _signature, _arg_types, _args, output_types) in zip(raw_results, specs)
    ]


def _read_layerzero_eid_registry(path: Path) -> tuple[dict[str, Any], ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    entries = payload.get("eids") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        return ()
    rows: list[dict[str, Any]] = []
    seen: set[int] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        try:
            eid = int(entry["eid"])
        except (KeyError, TypeError, ValueError):
            continue
        if eid in seen:
            continue
        seen.add(eid)
        network = str(entry.get("network") or f"eid-{eid}")
        chain = str(entry.get("chain") or network)
        chain_type = entry.get("chain_type")
        rows.append(
            {
                "eid": eid,
                "network": network,
                "chain": chain,
                "chain_type": str(chain_type) if chain_type else None,
            }
        )
    return tuple(sorted(rows, key=lambda item: item["eid"]))


@lru_cache(maxsize=1)
def layerzero_eid_registry() -> tuple[dict[str, Any], ...]:
    """LayerZero V2 mainnet EIDs generated from @layerzerolabs/lz-definitions."""
    try:
        return _read_layerzero_eid_registry(_LAYERZERO_EID_REGISTRY_PATH)
    except Exception:
        return ()


def _first_address(rpc_url: str, address: str, signatures: list[str]) -> str | None:
    for signature in signatures:
        out = _call(rpc_url, address, signature, output_types=["address"])
        if out and out[0] != ZERO_ADDRESS:
            return str(out[0]).lower()
    return None


def _bytes32_to_address(raw: bytes | str | None) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        hex_value = "0x" + raw.hex()
    else:
        hex_value = raw.lower()
    if hex_value == ZERO_BYTES32 or len(hex_value) != 66:
        return None
    if hex_value[2:26] == "0" * 24:
        addr = "0x" + hex_value[-40:]
        return None if addr == ZERO_ADDRESS else addr
    return None


def _bytes32_hex(raw: bytes | str | None) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return "0x" + raw.hex()
    return raw.lower()


def _nonzero_bytes32(raw: bytes | str | None) -> str | None:
    raw_hex = _bytes32_hex(raw)
    if raw_hex == ZERO_BYTES32:
        return None
    return raw_hex


def _decode_uln_config(raw: bytes | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        config = decode(["(uint64,uint8,uint8,uint8,address[],address[])"], raw)[0]
    except Exception:
        return None
    confirmations, required_count, optional_count, optional_threshold, required_dvns, optional_dvns = config
    return {
        "confirmations": int(confirmations),
        "required_dvn_count": int(required_count),
        "optional_dvn_count": int(optional_count),
        "optional_dvn_threshold": int(optional_threshold),
        "required_dvns": [str(addr).lower() for addr in required_dvns],
        "optional_dvns": [str(addr).lower() for addr in optional_dvns],
    }


def _decode_executor_config(raw: bytes | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        max_message_size, executor = decode(["uint32", "address"], raw)
    except Exception:
        return None
    return {
        "max_message_size": int(max_message_size),
        "executor": str(executor).lower(),
    }


def _endpoint_config(
    rpc_url: str,
    endpoint: str,
    oapp: str,
    message_lib: str | None,
    eid: int,
    config_type: int,
) -> bytes | None:
    if not message_lib or message_lib == ZERO_ADDRESS:
        return None
    out = _call(
        rpc_url,
        endpoint,
        "getConfig(address,address,uint32,uint32)",
        ["address", "address", "uint32", "uint32"],
        [oapp, message_lib, eid, config_type],
        ["bytes"],
    )
    if not out:
        return None
    return out[0]


def _peer_from_decoded(raw: bytes | str | None, signature: str) -> dict[str, Any] | None:
    peer_bytes32 = _nonzero_bytes32(raw)
    if not peer_bytes32:
        return None
    peer_address = _bytes32_to_address(raw)
    return {
        "peer": peer_address or peer_bytes32,
        "peer_address": peer_address,
        "peer_bytes32": peer_bytes32,
        "peer_source": signature,
    }


def _peer_from_raw_call(raw: str | None, signature: str) -> dict[str, Any] | None:
    out = _decode_output(["bytes32"], raw)
    if not out:
        return None
    return _peer_from_decoded(out[0], signature)


def _peer_for_eid(rpc_url: str, address: str, eid: int) -> dict[str, Any] | None:
    for signature in ("getReceiver(uint32)", "peers(uint32)"):
        out = _call(rpc_url, address, signature, ["uint32"], [eid], ["bytes32"])
        if not out:
            continue
        peer = _peer_from_decoded(out[0], signature)
        if peer:
            return peer
    return None


def _peer_map_for_eids(
    rpc_url: str,
    address: str,
    eid_entries: tuple[dict[str, Any], ...],
) -> dict[int, dict[str, Any]]:
    calls: list[tuple[str, list[Any]]] = []
    metadata: list[tuple[int, str]] = []
    for entry in eid_entries:
        eid = int(entry["eid"])
        for signature in ("getReceiver(uint32)", "peers(uint32)"):
            calls.append(("eth_call", [{"to": address, "data": _calldata(signature, ["uint32"], [eid])}, "latest"]))
            metadata.append((eid, signature))

    try:
        raw_results = rpc_batch_request(rpc_url, calls)
    except Exception:
        return {
            int(entry["eid"]): peer
            for entry in eid_entries
            if (peer := _peer_for_eid(rpc_url, address, int(entry["eid"]))) is not None
        }

    peers: dict[int, dict[str, Any]] = {}
    for raw, (eid, signature) in zip(raw_results, metadata):
        if eid in peers:
            continue
        peer = _peer_from_raw_call(raw, signature)
        if peer:
            peers[eid] = peer
    return peers


def _layerzero_route(
    rpc_url: str,
    *,
    endpoint: str,
    oapp: str,
    eid: int,
    chain: str,
    network: str | None = None,
    chain_type: str | None = None,
    peer_info: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    peer_info = peer_info or _peer_for_eid(rpc_url, oapp, eid)
    if not peer_info:
        return None

    send_library_out = _call(
        rpc_url,
        endpoint,
        "getSendLibrary(address,uint32)",
        ["address", "uint32"],
        [oapp, eid],
        ["address"],
    )
    send_library = str(send_library_out[0]).lower() if send_library_out else None

    receive_library_out = _call(
        rpc_url,
        endpoint,
        "getReceiveLibrary(address,uint32)",
        ["address", "uint32"],
        [oapp, eid],
        ["address", "bool"],
    )
    receive_library = str(receive_library_out[0]).lower() if receive_library_out else None
    receive_library_default = bool(receive_library_out[1]) if receive_library_out else None

    send_uln = _decode_uln_config(_endpoint_config(rpc_url, endpoint, oapp, send_library, eid, _ULN_CONFIG_TYPE))
    receive_uln = _decode_uln_config(_endpoint_config(rpc_url, endpoint, oapp, receive_library, eid, _ULN_CONFIG_TYPE))
    executor = _decode_executor_config(
        _endpoint_config(rpc_url, endpoint, oapp, send_library, eid, _EXECUTOR_CONFIG_TYPE)
    )

    return _finalize_route(
        {
            "eid": eid,
            "chain": chain,
            "network": network,
            "chain_type": chain_type,
            "peer": peer_info["peer"],
            "peer_address": peer_info["peer_address"],
            "peer_bytes32": peer_info["peer_bytes32"],
            "peer_source": peer_info["peer_source"],
            "send_library": send_library,
            "receive_library": receive_library,
            "receive_library_default": receive_library_default,
            "send_uln": send_uln,
            "receive_uln": receive_uln,
            "executor": executor,
        },
        protocol="LayerZero",
    )


def _layerzero_routes(
    rpc_url: str,
    *,
    endpoint: str,
    oapp: str,
    route_inputs: list[tuple[dict[str, Any], dict[str, Any]]],
) -> list[dict[str, Any]]:
    library_specs: list[tuple[str, str, list[str], list[Any], list[str]]] = []
    for entry, _peer_info in route_inputs:
        eid = int(entry["eid"])
        library_specs.extend(
            [
                (endpoint, "getSendLibrary(address,uint32)", ["address", "uint32"], [oapp, eid], ["address"]),
                (
                    endpoint,
                    "getReceiveLibrary(address,uint32)",
                    ["address", "uint32"],
                    [oapp, eid],
                    ["address", "bool"],
                ),
            ]
        )

    library_results = _batch_call(rpc_url, library_specs)
    routes: list[dict[str, Any]] = []
    for index, (entry, peer_info) in enumerate(route_inputs):
        send_library_out = library_results[index * 2] if index * 2 < len(library_results) else None
        receive_library_out = library_results[index * 2 + 1] if index * 2 + 1 < len(library_results) else None
        send_library = str(send_library_out[0]).lower() if send_library_out else None
        receive_library = str(receive_library_out[0]).lower() if receive_library_out else None
        routes.append(
            _finalize_route(
                {
                    "eid": int(entry["eid"]),
                    "chain": str(entry["chain"]),
                    "network": str(entry["network"]),
                    "chain_type": str(entry["chain_type"]) if entry.get("chain_type") else None,
                    "peer": peer_info["peer"],
                    "peer_address": peer_info["peer_address"],
                    "peer_bytes32": peer_info["peer_bytes32"],
                    "peer_source": peer_info["peer_source"],
                    "send_library": send_library,
                    "receive_library": receive_library,
                    "receive_library_default": bool(receive_library_out[1]) if receive_library_out else None,
                    "send_uln": None,
                    "receive_uln": None,
                    "executor": None,
                },
                protocol="LayerZero",
            )
        )

    config_specs: list[tuple[str, str, list[str], list[Any], list[str]]] = []
    config_slots: list[tuple[int, str]] = []
    for index, route in enumerate(routes):
        send_library = route.get("send_library")
        receive_library = route.get("receive_library")
        eid = int(route["eid"])
        if send_library:
            config_specs.extend(
                [
                    (
                        endpoint,
                        "getConfig(address,address,uint32,uint32)",
                        ["address", "address", "uint32", "uint32"],
                        [oapp, send_library, eid, _ULN_CONFIG_TYPE],
                        ["bytes"],
                    ),
                    (
                        endpoint,
                        "getConfig(address,address,uint32,uint32)",
                        ["address", "address", "uint32", "uint32"],
                        [oapp, send_library, eid, _EXECUTOR_CONFIG_TYPE],
                        ["bytes"],
                    ),
                ]
            )
            config_slots.extend([(index, "send_uln"), (index, "executor")])
        if receive_library:
            config_specs.append(
                (
                    endpoint,
                    "getConfig(address,address,uint32,uint32)",
                    ["address", "address", "uint32", "uint32"],
                    [oapp, receive_library, eid, _ULN_CONFIG_TYPE],
                    ["bytes"],
                )
            )
            config_slots.append((index, "receive_uln"))

    for out, (index, field) in zip(_batch_call(rpc_url, config_specs), config_slots):
        raw = out[0] if out else None
        if field == "executor":
            routes[index][field] = _decode_executor_config(raw)
        else:
            routes[index][field] = _decode_uln_config(raw)

    return routes


def resolve_layerzero_runtime(
    *,
    rpc_url: str,
    contract: dict[str, Any],
    functions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Resolve current LayerZero state for one selected bridge contract."""
    address = str(contract.get("address") or "").lower()
    if not address:
        raise BridgeRuntimeError("contract address is required")

    controllers = contract.get("controllers") if isinstance(contract.get("controllers"), dict) else {}
    endpoint = None
    endpoint_controller = controllers.get("external_contract:endpoint") if isinstance(controllers, dict) else None
    if isinstance(endpoint_controller, str) and endpoint_controller.startswith("0x"):
        endpoint = endpoint_controller.lower()
    endpoint = endpoint or _first_address(rpc_url, address, ["endpoint()", "getEndpoint()"])

    if not endpoint:
        return _protocol_result(
            status="unresolved",
            protocol="LayerZero",
            reason="No endpoint address was found from contract state or controller extraction.",
        )

    local_eid_out = _call(rpc_url, endpoint, "eid()", output_types=["uint32"])
    delegate_out = _call(rpc_url, endpoint, "delegates(address)", ["address"], [address], ["address"])
    owner = _first_address(rpc_url, address, ["owner()"])
    token_out = _first_address(rpc_url, address, ["getTokenOut()", "token()", "asset()"])
    lock_box = _first_address(rpc_url, address, ["getLockBox()", "lockbox()"])

    eid_entries = layerzero_eid_registry()
    peers_by_eid = _peer_map_for_eids(rpc_url, address, eid_entries)
    route_inputs = [(entry, peer) for entry in eid_entries if (peer := peers_by_eid.get(int(entry["eid"])))]
    routes = _layerzero_routes(rpc_url, endpoint=endpoint, oapp=address, route_inputs=route_inputs)

    config_functions = [
        fn.get("function")
        for fn in functions or []
        if set(fn.get("effect_labels") or []).intersection({"bridge_config_update", "bridge_security_config"})
    ]

    return {
        "status": "resolved" if routes else "partial",
        "protocol": "LayerZero",
        "protocols": ["LayerZero"],
        "source": "eth_call",
        "contract": {"address": address, "name": contract.get("name")},
        "endpoint": {"address": endpoint, "local_eid": int(local_eid_out[0]) if local_eid_out else None},
        "delegate": str(delegate_out[0]).lower() if delegate_out and delegate_out[0] != ZERO_ADDRESS else None,
        "owner": owner,
        "assets": [
            item
            for item in (
                {"label": "token out", "address": token_out} if token_out else None,
                {"label": "lockbox", "address": lock_box} if lock_box else None,
            )
            if item is not None
        ],
        "routes": routes,
        "config_functions": [fn for fn in config_functions if fn],
        "limits": [
            {
                "label": f"{route['chain']} max message size",
                "value": route["executor"]["max_message_size"],
                "source": "LayerZero executor config",
            }
            for route in routes
            if route.get("executor")
        ],
        "policies": [
            item
            for item in (
                {
                    "label": "delegate can configure app settings",
                    "address": str(delegate_out[0]).lower(),
                    "source": "EndpointV2.delegates",
                }
                if delegate_out and delegate_out[0] != ZERO_ADDRESS
                else None,
                {
                    "label": "owner controls local app admin functions",
                    "address": owner,
                    "source": "owner()",
                }
                if owner
                else None,
            )
            if item is not None
        ],
    }


def _hyperlane_peer_for_domain(rpc_url: str, address: str, domain: int) -> dict[str, Any] | None:
    for signature in ("routers(uint32)", "remoteRouters(uint32)", "enrolledRouters(uint32)"):
        out = _call(rpc_url, address, signature, ["uint32"], [domain], ["bytes32"])
        if not out:
            continue
        peer = _peer_from_decoded(out[0], signature)
        if peer:
            return peer
    return None


def _hyperlane_module_type(ism: str | None, rpc_url: str) -> int | None:
    if not ism:
        return None
    out = _call(rpc_url, ism, "moduleType()", output_types=["uint8"])
    return int(out[0]) if out else None


def resolve_hyperlane_runtime(
    *,
    rpc_url: str,
    contract: dict[str, Any],
    functions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Resolve Hyperlane mailbox/router/ISM state for router-like contracts."""
    address = str(contract.get("address") or "").lower()
    if not address:
        raise BridgeRuntimeError("contract address is required")

    mailbox = _first_address(rpc_url, address, ["mailbox()", "localMailbox()", "getMailbox()"])
    local_domain_out = _call(rpc_url, mailbox or address, "localDomain()", output_types=["uint32"])
    ism = _first_address(rpc_url, address, ["interchainSecurityModule()", "ism()"])
    default_ism = _first_address(rpc_url, mailbox or address, ["defaultIsm()"])
    hook = _first_address(rpc_url, address, ["hook()", "defaultHook()"])
    owner = _first_address(rpc_url, address, ["owner()"])

    domain_entries = hyperlane_domain_entries()
    routes: list[dict[str, Any]] = []
    for entry in domain_entries:
        domain = int(entry["domain"])
        peer = _hyperlane_peer_for_domain(rpc_url, address, domain)
        if not peer:
            continue
        routes.append(
            _finalize_route(
                {
                    "domain": domain,
                    "chain": str(entry["chain"]),
                    "network": str(entry["display_name"]),
                    "chain_type": "evm",
                    "peer": peer["peer"],
                    "peer_address": peer["peer_address"],
                    "peer_bytes32": peer["peer_bytes32"],
                    "peer_source": peer["peer_source"],
                    "mailbox": mailbox,
                    "ism": ism or default_ism,
                    "hook": hook,
                },
                protocol="Hyperlane",
            )
        )

    config_functions = [
        fn.get("function")
        for fn in functions or []
        if set(fn.get("effect_labels") or []).intersection({"bridge_config_update", "bridge_security_config"})
    ]
    active_ism = ism or default_ism
    policies = [
        item
        for item in (
            {
                "label": "interchain security module verifies messages",
                "address": active_ism,
                "source": "interchainSecurityModule/defaultIsm",
                "module_type": _hyperlane_module_type(active_ism, rpc_url),
            }
            if active_ism
            else None,
            {
                "label": "owner controls local app admin functions",
                "address": owner,
                "source": "owner()",
            }
            if owner
            else None,
        )
        if item is not None
    ]
    return {
        "status": "resolved" if routes else "partial" if mailbox or active_ism else "unresolved",
        "protocol": "Hyperlane",
        "protocols": ["Hyperlane"],
        "source": "eth_call",
        "contract": {"address": address, "name": contract.get("name")},
        "mailbox": {"address": mailbox, "local_domain": int(local_domain_out[0]) if local_domain_out else None}
        if mailbox or local_domain_out
        else None,
        "ism": active_ism,
        "hook": hook,
        "owner": owner,
        "routes": routes,
        "config_functions": [fn for fn in config_functions if fn],
        "limits": [],
        "policies": policies,
    }


def resolve_op_stack_runtime(
    *,
    rpc_url: str,
    contract: dict[str, Any],
    functions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Resolve OP Stack bridge-infra relationships without inventing app peers."""
    address = str(contract.get("address") or "").lower()
    if not address:
        raise BridgeRuntimeError("contract address is required")

    name = str(contract.get("name") or contract.get("contract_name") or "OP Stack bridge")
    guardian = _first_address(rpc_url, address, ["guardian()", "GUARDIAN()"])
    superchain_config = _first_address(rpc_url, address, ["superchainConfig()", "SUPERCHAIN_CONFIG()"])
    system_config = _first_address(rpc_url, address, ["systemConfig()", "SYSTEM_CONFIG()"])
    messenger = _first_address(rpc_url, address, ["messenger()", "MESSENGER()"])
    portal = _first_address(rpc_url, address, ["portal()", "optimismPortal()", "OPTIMISM_PORTAL()"])
    other_bridge = _first_address(rpc_url, address, ["otherBridge()", "OTHER_BRIDGE()"])
    paused_out = _call(rpc_url, address, "paused()", output_types=["bool"])
    l2_chain_out = _call(rpc_url, address, "l2ChainId()", output_types=["uint256"])
    l2_chain_id = int(l2_chain_out[0]) if l2_chain_out else None

    relationships = [
        item
        for item in (
            {"label": "guardian", "address": guardian, "source": "guardian()"} if guardian else None,
            {
                "label": "superchain config",
                "address": superchain_config,
                "source": "superchainConfig()",
            }
            if superchain_config
            else None,
            {"label": "system config", "address": system_config, "source": "systemConfig()"} if system_config else None,
            {"label": "messenger", "address": messenger, "source": "messenger()"} if messenger else None,
            {"label": "portal", "address": portal, "source": "portal()/optimismPortal()"} if portal else None,
            {"label": "remote bridge", "address": other_bridge, "source": "otherBridge()"} if other_bridge else None,
        )
        if item is not None
    ]
    route_chain = chain_name_for_chain_id(l2_chain_id) or normalize_chain_name(contract.get("chain"))
    routes = [
        _finalize_route(
            {
                "chain": route_chain,
                "network": display_name_for_chain(route_chain) or "OP Stack",
                "chain_type": "evm",
                "route_type": "op_stack_canonical_bridge",
                "peer": other_bridge,
                "peer_address": other_bridge,
                "peer_source": "otherBridge()" if other_bridge else None,
                "l2_chain_id": l2_chain_id,
                "portal": portal or address if "portal" in name.lower() else portal,
                "messenger": messenger,
                "system_config": system_config,
            },
            protocol="OP Stack",
        )
    ]
    policies = [
        item
        for item in (
            {
                "label": "guardian can pause or configure bridge infra",
                "address": guardian,
                "source": "guardian()",
            }
            if guardian
            else None,
            {
                "label": "bridge pause state",
                "value": bool(paused_out[0]),
                "source": "paused()",
            }
            if paused_out
            else None,
        )
        if item is not None
    ]
    return {
        "status": "resolved" if relationships or policies else "partial",
        "protocol": "OP Stack",
        "protocols": ["OP Stack"],
        "source": "eth_call",
        "contract": {"address": address, "name": contract.get("name")},
        "relationships": relationships,
        "routes": routes,
        "limits": [],
        "policies": policies,
        "config_functions": [
            fn.get("function")
            for fn in functions or []
            if set(fn.get("effect_labels") or []).intersection({"bridge_config_update", "bridge_security_config"})
        ],
    }


def _unsupported_bridge_runtime(protocol: str, protocols: set[str]) -> dict[str, Any]:
    reasons = {
        "Wormhole": "Wormhole guardian/relayer runtime resolution is not implemented yet.",
        "Axelar": "Axelar gateway/gas-service runtime resolution is not implemented yet.",
        "CCIP": "CCIP router/token-pool runtime resolution is not implemented yet.",
        "Connext": "Connext router/domain runtime resolution is not implemented yet.",
    }
    return _protocol_result(
        status="unsupported_runtime",
        protocol=protocol,
        reason=reasons.get(protocol, "No runtime resolver is available for this bridge protocol yet."),
        protocols=sorted(protocols) if protocols else [protocol],
    )


def resolve_bridge_runtime(
    *,
    rpc_url: str,
    contract: dict[str, Any],
    functions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    raw_bridge_context = contract.get("bridge_context")
    raw_static_context = contract.get("bridge_static_context")
    bridge_context: dict[str, Any] = raw_bridge_context if isinstance(raw_bridge_context, dict) else {}
    bridge_static_context: dict[str, Any] = raw_static_context if isinstance(raw_static_context, dict) else {}
    protocols = set(
        bridge_context.get("protocols") or bridge_static_context.get("protocols") or contract.get("standards") or []
    )
    if _is_op_stack_contract(contract):
        return resolve_op_stack_runtime(rpc_url=rpc_url, contract=contract, functions=functions)
    if "LayerZero" in protocols:
        return resolve_layerzero_runtime(rpc_url=rpc_url, contract=contract, functions=functions)
    if "Hyperlane" in protocols:
        return resolve_hyperlane_runtime(rpc_url=rpc_url, contract=contract, functions=functions)
    for protocol in ("Wormhole", "Axelar", "CCIP", "Connext"):
        if protocol in protocols:
            return _unsupported_bridge_runtime(protocol, protocols)
    return {
        "status": "unsupported_runtime",
        "protocol": sorted(protocols)[0] if protocols else "Bridge",
        "protocols": sorted(protocols),
        "routes": [],
        "reason": "No runtime resolver is available for this bridge protocol yet.",
    }
