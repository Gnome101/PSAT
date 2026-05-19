"""Protocol-agnostic semantic facts and protocol claims.

The facts in this artifact are the durable interface for protocol modules.
Raw names can appear as facts, but protocol classification happens through
claims that cite concrete facts and proof kinds.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from .shared import _all_state_variables, _entry_points

SCHEMA_VERSION = "semantic_facts.v1"

_CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_TOKEN_SPLIT_RE = re.compile(r"[^A-Za-z0-9]+")

_LAYERZERO_RECEIVE_NAMES = {"lzreceive", "_lzreceive", "lzcompose"}
_LAYERZERO_PEER_SIGNATURES = {
    "setPeer(uint32,bytes32)",
    "peers(uint32)",
    "setTrustedRemote(uint16,bytes)",
    "trustedRemoteLookup(uint16)",
}
_LAYERZERO_ENDPOINT_METHODS = {
    "send",
    "quote",
    "setdelegate",
    "setconfig",
    "setsendlibrary",
    "setreceivelibrary",
    "setreceivelibrarytimeout",
    "sendcompose",
    "eid",
    "delegates",
}
_LAYERZERO_LONG_TYPE_MARKERS = {
    "ilayerzeroendpoint",
    "layerzeroendpoint",
    "endpointv2",
    "oappcore",
    "oappauth",
    "oftcore",
    "oftadapter",
    "onft",
    "messagingfee",
    "sendparam",
    "enforcedoptionparam",
    "origin",
}
_LAYERZERO_TOKEN_MARKERS = {"oapp", "oft", "onft", "layerzero"}
_LAYERZERO_SECURITY_TOKENS = {
    "dvn",
    "uln",
    "messagelib",
    "sendlibrary",
    "receivelibrary",
    "enforcedoptions",
    "executor",
}

_PROTOCOL_DISPLAY_NAMES = {
    "layerzero": "LayerZero",
    "ccip": "CCIP",
    "hyperlane": "Hyperlane",
    "wormhole": "Wormhole",
    "axelar": "Axelar",
    "connext": "Connext",
}

_GENERIC_PROTOCOL_SPECS: dict[str, dict[str, Any]] = {
    "ccip": {
        "classification": "ccip_app",
        "asset_classification": "ccip_asset_bridge",
        "long_type_markers": {
            "irouterclient",
            "routerclient",
            "ccipreceiver",
            "cciprouter",
            "clientevm2anymessage",
            "clientany2evmmessage",
            "evm2anymessage",
            "any2evmmessage",
        },
        "token_markers": {"ccip"},
        "receiver_names": {"router", "cciprouter"},
        "receive_names": {"ccipreceive", "_ccipreceive"},
        "ambiguous_receive_names": set(),
        "peer_signatures": set(),
        "config_names": {
            "allowlistsourcechain",
            "allowlistdestinationchain",
            "allowlistsender",
            "setrouter",
            "setcciprouter",
        },
        "endpoint_methods": {"ccipsend", "getfee", "ischainsupported"},
        "route_tokens": {
            "chainselector",
            "sourcechainselector",
            "destinationchainselector",
            "destchainselector",
            "sourcechain",
            "destinationchain",
        },
        "security_tokens": {"allowlist", "allowlisted", "sender", "sourcechain", "destinationchain"},
    },
    "hyperlane": {
        "classification": "hyperlane_app",
        "asset_classification": "hyperlane_asset_bridge",
        "long_type_markers": {
            "imailbox",
            "mailboxclient",
            "iinterchainsecuritymodule",
            "interchainsecuritymodule",
            "ipostdispatchhook",
            "postdispatchhook",
            "hyperlanerouter",
        },
        "token_markers": {"hyperlane", "interchain"},
        "receiver_names": {"mailbox"},
        "receive_names": {"handle", "_handle"},
        "ambiguous_receive_names": {"handle", "_handle"},
        "peer_signatures": {"routers(uint32)", "remoteRouters(uint32)", "enrolledRouters(uint32)"},
        "config_names": {"setinterchainsecuritymodule", "sethook", "enrollremoterouter", "setremoterouter"},
        "endpoint_methods": {"dispatch", "quotedispatch", "process"},
        "route_tokens": {"domain", "destinationdomain", "sourcedomain", "origindomain", "mailbox"},
        "security_tokens": {"ism", "interchainsecuritymodule", "securitymodule", "hook", "postdispatchhook"},
    },
    "wormhole": {
        "classification": "wormhole_app",
        "asset_classification": "wormhole_asset_bridge",
        "long_type_markers": {
            "iwormhole",
            "iwormholerelayer",
            "wormholerelayer",
            "wormholereceiver",
            "vmemory",
            "structsvm",
            "vaa",
        },
        "token_markers": {"wormhole", "vaa"},
        "receiver_names": {"wormhole", "relayer", "wormholerelayer"},
        "receive_names": {"receivewormholemessages", "wormholereceive", "receivepayload"},
        "ambiguous_receive_names": {"receivepayload"},
        "peer_signatures": set(),
        "config_names": {"setwormhole", "setrelayer", "setregisteredsender", "settrustedsender"},
        "endpoint_methods": {
            "publishmessage",
            "parseandverifyvm",
            "verifyvm",
            "sendpayloadtoevm",
            "sendtoevm",
            "quoteevmdeliveryprice",
            "forwardpayloadtoevm",
        },
        "route_tokens": {"targetchain", "chainid", "emitterchainid", "wormholechainid"},
        "security_tokens": {"vaa", "emitter", "consistency", "receiver", "trustedsender"},
    },
    "axelar": {
        "classification": "axelar_app",
        "asset_classification": "axelar_asset_bridge",
        "long_type_markers": {
            "iaxelargateway",
            "iaxelargasservice",
            "axelarexecutable",
            "axelarexecutablewithtoken",
        },
        "token_markers": {"axelar"},
        "receiver_names": {"gateway", "gasservice", "gasreceiver"},
        "receive_names": {"execute", "_execute", "executewithtoken", "_executewithtoken"},
        "ambiguous_receive_names": {"execute", "_execute"},
        "peer_signatures": set(),
        "config_names": {"settrustedaddress", "setgateway", "setgasservice"},
        "endpoint_methods": {
            "callcontract",
            "callcontractwithtoken",
            "paynativegasforcontractcall",
            "paygasforcontractcall",
            "validatecontractcall",
            "validatecontractcallandmint",
        },
        "route_tokens": {"sourcechain", "sourceaddress", "destinationchain", "destinationaddress", "commandid"},
        "security_tokens": {"gateway", "gasservice", "trustedaddress", "trustedsource"},
    },
    "connext": {
        "classification": "connext_app",
        "asset_classification": "connext_asset_bridge",
        "long_type_markers": {"iconnext", "ixreceiver", "connextreceiver", "xreceiver", "connextdiamond"},
        "token_markers": {"connext"},
        "receiver_names": {"connext"},
        "receive_names": {"xreceive"},
        "ambiguous_receive_names": set(),
        "peer_signatures": set(),
        "config_names": {"setconnext", "setorigin", "setexecutor", "setdomain"},
        "endpoint_methods": {"xcall", "bumptransfer", "forcereceive"},
        "route_tokens": {"origindomain", "destinationdomain", "transferid"},
        "security_tokens": {"executor", "origin", "domain", "delegate"},
    },
}


def _full_name(fn: Any) -> str:
    return str(getattr(fn, "full_name", None) or getattr(fn, "name", None) or "<anonymous>")


def _compact(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _tokens(value: object) -> set[str]:
    raw_parts = [part for part in _TOKEN_SPLIT_RE.split(str(value or "")) if part]
    out: set[str] = set()
    for part in raw_parts:
        for token in _CAMEL_BOUNDARY_RE.sub(" ", part).split():
            normalized = token.lower()
            if normalized:
                out.add(normalized)
    return out


def _type_matches_layerzero(value: object) -> bool:
    compact = _compact(value)
    if any(marker in compact for marker in _LAYERZERO_LONG_TYPE_MARKERS):
        return True
    return bool(_tokens(value).intersection(_LAYERZERO_TOKEN_MARKERS))


def _function_fact(fn: Any, idx: int) -> dict[str, Any]:
    signature = _full_name(fn)
    params = []
    for param_idx, param in enumerate(getattr(fn, "parameters", []) or []):
        params.append(
            {
                "id": f"fn:{idx}:param:{param_idx}",
                "name": str(getattr(param, "name", "") or ""),
                "type": str(getattr(param, "type", "") or ""),
            }
        )
    modifiers = [
        str(getattr(modifier, "name", "") or getattr(modifier, "full_name", "") or "")
        for modifier in getattr(fn, "modifiers", []) or []
    ]
    return {
        "id": f"function:{signature}",
        "kind": "function_declared",
        "contract": str(getattr(getattr(fn, "contract", None), "name", "") or ""),
        "name": str(getattr(fn, "name", "") or ""),
        "signature": signature,
        "visibility": str(getattr(fn, "visibility", "") or ""),
        "parameters": params,
        "modifiers": [modifier for modifier in modifiers if modifier],
    }


def _state_variable_fact(variable: Any, idx: int) -> dict[str, Any]:
    name = str(getattr(variable, "name", "") or "")
    return {
        "id": f"state:{idx}:{name}",
        "kind": "state_variable",
        "name": name,
        "type": str(getattr(variable, "type", "") or ""),
        "visibility": str(getattr(variable, "visibility", "") or ""),
    }


def _callee_signature(ir: Any) -> str | None:
    callee = getattr(ir, "function", None)
    for attr in ("full_name", "signature_str"):
        value = getattr(callee, attr, None)
        if callable(value):
            value = value()
        if isinstance(value, str) and "(" in value and value.endswith(")"):
            return value.rsplit(".", 1)[-1]
    value = getattr(ir, "function_name", None)
    if isinstance(value, str) and "(" in value and value.endswith(")"):
        return value.rsplit(".", 1)[-1]
    return None


def _call_facts_for_function(fn: Any) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for node_idx, node in enumerate(getattr(fn, "nodes", []) or []):
        for ir_idx, ir in enumerate(getattr(node, "irs", []) or []):
            op = type(ir).__name__
            if op not in {"HighLevelCall", "LibraryCall", "LowLevelCall"}:
                continue
            destination = getattr(ir, "destination", None)
            receiver = getattr(destination, "name", None) or str(destination or "")
            receiver_type = str(getattr(destination, "type", "") or "")
            if op == "LibraryCall":
                args = list(getattr(ir, "arguments", []) or [])
                if args:
                    receiver = getattr(args[0], "name", None) or str(args[0] or receiver)
                    receiver_type = str(getattr(args[0], "type", "") or receiver_type)
            callee = str(getattr(ir, "function_name", "") or "")
            signature = _callee_signature(ir)
            facts.append(
                {
                    "id": f"call:{_full_name(fn)}:{node_idx}:{ir_idx}",
                    "kind": "external_call",
                    "from_function": _full_name(fn),
                    "receiver": receiver,
                    "receiver_type": receiver_type,
                    "callee": callee,
                    "signature": signature,
                    "target": f"{receiver}.{callee}" if receiver and callee else receiver or callee,
                }
            )
    return facts


def _inheritance_facts(contract: Any) -> list[dict[str, Any]]:
    facts = []
    for idx, inherited in enumerate(getattr(contract, "inheritance", []) or []):
        name = str(getattr(inherited, "name", "") or "")
        if name:
            facts.append({"id": f"inheritance:{idx}:{name}", "kind": "inherits", "name": name})
    return facts


def _claim(
    *,
    claim_type: str,
    proof: str,
    fact_id: str,
    functions: list[str] | None = None,
    protocol: str = "LayerZero",
    **extra: Any,
) -> dict[str, Any]:
    out = {
        "claim_type": claim_type,
        "protocol": protocol,
        "proof": proof,
        "facts": [fact_id],
    }
    if functions:
        out["functions"] = functions
    out.update(extra)
    return out


def _layerzero_claims(
    *,
    function_facts: list[dict[str, Any]],
    state_facts: list[dict[str, Any]],
    inheritance_facts: list[dict[str, Any]],
    call_facts: list[dict[str, Any]],
    effects: Mapping[str, Any] | None,
) -> dict[str, Any]:
    claims: list[dict[str, Any]] = []
    state_types = {str(fact.get("name") or "").lower(): str(fact.get("type") or "") for fact in state_facts}

    for fact in inheritance_facts:
        if _type_matches_layerzero(fact.get("name")):
            claims.append(
                _claim(
                    claim_type="protocol_interface_implemented",
                    proof="inheritance_match",
                    fact_id=str(fact["id"]),
                    symbol=fact.get("name"),
                )
            )

    for fact in state_facts:
        if _type_matches_layerzero(fact.get("type")):
            claims.append(
                _claim(
                    claim_type="protocol_interface_implemented",
                    proof="typed_state_variable",
                    fact_id=str(fact["id"]),
                    symbol=fact.get("type"),
                )
            )

    for fact in function_facts:
        signature = str(fact.get("signature") or "")
        name = str(fact.get("name") or "").lower()
        name_compact = _compact(name)
        function_tokens = _tokens(name)
        param_types = [str(param.get("type") or "") for param in fact.get("parameters") or []]
        param_names = [str(param.get("name") or "") for param in fact.get("parameters") or []]
        has_layerzero_type = any(_type_matches_layerzero(param_type) for param_type in param_types)

        if name_compact in _LAYERZERO_RECEIVE_NAMES:
            claims.append(
                _claim(
                    claim_type="cross_chain_receive_entrypoint",
                    proof="exact_signature",
                    fact_id=str(fact["id"]),
                    functions=[signature],
                    signature=signature,
                )
            )
        if signature in _LAYERZERO_PEER_SIGNATURES:
            claims.append(
                _claim(
                    claim_type="protocol_peer_configured",
                    proof="exact_signature",
                    fact_id=str(fact["id"]),
                    functions=[signature],
                    signature=signature,
                )
            )
        if has_layerzero_type:
            claims.append(
                _claim(
                    claim_type="protocol_interface_implemented",
                    proof="typed_parameter",
                    fact_id=str(fact["id"]),
                    functions=[signature],
                    signature=signature,
                )
            )
        param_tokens = (
            set().union(*(_tokens(value) for value in [*param_names, *param_types])) if param_types else set()
        )
        if (
            function_tokens.intersection(_LAYERZERO_SECURITY_TOKENS)
            or param_tokens.intersection(_LAYERZERO_SECURITY_TOKENS)
        ) and ("eid" in param_tokens or "dsteid" in {_compact(name) for name in param_names}):
            claims.append(
                _claim(
                    claim_type="protocol_security_config",
                    proof="typed_config_shape",
                    fact_id=str(fact["id"]),
                    functions=[signature],
                    signature=signature,
                )
            )

    for fact in call_facts:
        callee = _compact(fact.get("callee"))
        receiver = str(fact.get("receiver") or "")
        receiver_type = str(fact.get("receiver_type") or "")
        receiver_state_type = state_types.get(receiver.lower(), "")
        function_name = str(fact.get("from_function") or "")
        receiver_is_endpoint = _type_matches_layerzero(receiver_type) or _type_matches_layerzero(receiver_state_type)
        receiver_tokens = _tokens(receiver)
        if callee in _LAYERZERO_ENDPOINT_METHODS and receiver_is_endpoint:
            claims.append(
                _claim(
                    claim_type="protocol_endpoint_called",
                    proof="typed_receiver_call",
                    fact_id=str(fact["id"]),
                    functions=[function_name],
                    receiver=receiver,
                    receiver_type=receiver_type or receiver_state_type,
                    callee=fact.get("callee"),
                )
            )
        if callee in {"send", "quotesend"} and receiver_tokens.intersection({"oft", "onft"}):
            claims.append(
                _claim(
                    claim_type="cross_chain_asset_path",
                    proof="typed_receiver_call",
                    fact_id=str(fact["id"]),
                    functions=[function_name],
                    receiver=receiver,
                    callee=fact.get("callee"),
                )
            )

    effect_functions = (effects or {}).get("functions") if isinstance(effects, Mapping) else {}
    if isinstance(effect_functions, Mapping):
        for signature, info in effect_functions.items():
            if not isinstance(info, Mapping):
                continue
            labels = set(str(label) for label in info.get("effect_labels") or [])
            if labels.intersection({"asset_pull", "asset_send", "mint", "burn"}):
                for claim in list(claims):
                    calls_endpoint = claim["claim_type"] == "protocol_endpoint_called"
                    if signature in (claim.get("functions") or []) and calls_endpoint:
                        claims.append(
                            _claim(
                                claim_type="cross_chain_asset_path",
                                proof="asset_effect_plus_endpoint_call",
                                fact_id=str(claim["facts"][0]),
                                functions=[str(signature)],
                            )
                        )

    claim_types = {claim["claim_type"] for claim in claims}
    is_oapp = bool(
        "protocol_endpoint_bound" in claim_types
        or {"protocol_interface_implemented", "protocol_endpoint_called"}.issubset(claim_types)
        or {"cross_chain_receive_entrypoint", "protocol_peer_configured"}.issubset(claim_types)
        or {"cross_chain_receive_entrypoint", "protocol_interface_implemented"}.issubset(claim_types)
        or "protocol_security_config" in claim_types
    )
    if is_oapp and "cross_chain_asset_path" in claim_types:
        classification = "layerzero_asset_bridge"
    elif is_oapp:
        classification = "layerzero_oapp"
    elif "related_protocol_dependency" in claim_types:
        classification = "layerzero_related_dependency"
    else:
        classification = "not_applicable"

    return {
        "protocol": "LayerZero",
        "classification": classification,
        "claims": claims,
    }


def _spec_type_match(spec: Mapping[str, Any], value: object) -> bool:
    compact = _compact(value)
    markers = spec.get("long_type_markers") or set()
    if any(str(marker) in compact for marker in markers):
        return True
    return bool(_tokens(value).intersection(set(spec.get("token_markers") or set())))


def _generic_protocol_claims(
    protocol_key: str,
    *,
    function_facts: list[dict[str, Any]],
    state_facts: list[dict[str, Any]],
    inheritance_facts: list[dict[str, Any]],
    call_facts: list[dict[str, Any]],
    effects: Mapping[str, Any] | None,
) -> dict[str, Any]:
    spec = _GENERIC_PROTOCOL_SPECS[protocol_key]
    protocol = _PROTOCOL_DISPLAY_NAMES[protocol_key]
    claims: list[dict[str, Any]] = []
    state_types = {str(fact.get("name") or "").lower(): str(fact.get("type") or "") for fact in state_facts}
    receiver_names = set(spec.get("receiver_names") or set())
    token_markers = set(spec.get("token_markers") or set())
    route_tokens = set(spec.get("route_tokens") or set())
    security_tokens = set(spec.get("security_tokens") or set())
    contract_has_protocol_anchor = any(_spec_type_match(spec, fact.get("name")) for fact in inheritance_facts)
    contract_has_protocol_anchor = contract_has_protocol_anchor or any(
        _spec_type_match(spec, fact.get("type"))
        or bool(_tokens(fact.get("name")).intersection(receiver_names | token_markers))
        for fact in state_facts
    )

    for fact in inheritance_facts:
        if _spec_type_match(spec, fact.get("name")):
            claims.append(
                _claim(
                    claim_type="protocol_interface_implemented",
                    protocol=protocol,
                    proof="inheritance_match",
                    fact_id=str(fact["id"]),
                    symbol=fact.get("name"),
                )
            )

    for fact in state_facts:
        if _spec_type_match(spec, fact.get("type")):
            claims.append(
                _claim(
                    claim_type="protocol_interface_implemented",
                    protocol=protocol,
                    proof="typed_state_variable",
                    fact_id=str(fact["id"]),
                    symbol=fact.get("type"),
                )
            )

    for fact in function_facts:
        signature = str(fact.get("signature") or "")
        name = str(fact.get("name") or "")
        name_compact = _compact(name)
        function_tokens = _tokens(name)
        param_types = [str(param.get("type") or "") for param in fact.get("parameters") or []]
        param_names = [str(param.get("name") or "") for param in fact.get("parameters") or []]
        param_tokens = (
            set().union(*(_tokens(value) for value in [*param_names, *param_types])) if param_types else set()
        )
        param_compacts = {_compact(value) for value in [*param_names, *param_types]}
        has_protocol_type = any(_spec_type_match(spec, param_type) for param_type in param_types)
        has_route_shape = bool(param_tokens.intersection(route_tokens) or param_compacts.intersection(route_tokens))
        receive_names = set(spec.get("receive_names") or set())
        ambiguous_receive_names = set(spec.get("ambiguous_receive_names") or set())
        endpoint_methods = set(spec.get("endpoint_methods") or set())
        has_protocol_anchor = bool(
            contract_has_protocol_anchor
            or has_protocol_type
            or function_tokens.intersection(token_markers)
            or name_compact in receive_names
            or name_compact in endpoint_methods
        )

        if name_compact in receive_names and (
            name_compact not in ambiguous_receive_names
            or has_protocol_type
            or has_route_shape
            or function_tokens.intersection(token_markers)
        ):
            claims.append(
                _claim(
                    claim_type="cross_chain_receive_entrypoint",
                    protocol=protocol,
                    proof="exact_signature",
                    fact_id=str(fact["id"]),
                    functions=[signature],
                    signature=signature,
                )
            )
        if name_compact in endpoint_methods and (
            has_protocol_type
            or has_route_shape
            or function_tokens.intersection(token_markers)
            or name_compact
            in {
                "ccipsend",
                "parseandverifyvm",
                "verifyvm",
                "sendpayloadtoevm",
                "callcontract",
                "callcontractwithtoken",
                "validatecontractcall",
                "validatecontractcallandmint",
                "xcall",
            }
        ):
            claims.append(
                _claim(
                    claim_type="protocol_endpoint_declared",
                    protocol=protocol,
                    proof="exact_signature",
                    fact_id=str(fact["id"]),
                    functions=[signature],
                    signature=signature,
                )
            )
        if signature in set(spec.get("peer_signatures") or set()) or name_compact in set(
            spec.get("config_names") or set()
        ):
            claims.append(
                _claim(
                    claim_type="protocol_peer_configured",
                    protocol=protocol,
                    proof="exact_signature",
                    fact_id=str(fact["id"]),
                    functions=[signature],
                    signature=signature,
                )
            )
        if has_protocol_type:
            claims.append(
                _claim(
                    claim_type="protocol_interface_implemented",
                    protocol=protocol,
                    proof="typed_parameter",
                    fact_id=str(fact["id"]),
                    functions=[signature],
                    signature=signature,
                )
            )
        if (
            (
                function_tokens.intersection(security_tokens)
                or param_tokens.intersection(security_tokens)
                or name_compact in set(spec.get("config_names") or set())
            )
            and has_protocol_anchor
            and (has_route_shape or has_protocol_type or function_tokens.intersection(token_markers))
        ):
            claims.append(
                _claim(
                    claim_type="protocol_security_config",
                    protocol=protocol,
                    proof="typed_config_shape",
                    fact_id=str(fact["id"]),
                    functions=[signature],
                    signature=signature,
                )
            )

    for fact in call_facts:
        callee = _compact(fact.get("callee"))
        receiver = str(fact.get("receiver") or "")
        receiver_type = str(fact.get("receiver_type") or "")
        receiver_state_type = state_types.get(receiver.lower(), "")
        function_name = str(fact.get("from_function") or "")
        receiver_tokens = _tokens(receiver)
        receiver_is_protocol = (
            _spec_type_match(spec, receiver_type)
            or _spec_type_match(spec, receiver_state_type)
            or bool(receiver_tokens.intersection(receiver_names | token_markers))
        )
        if callee in set(spec.get("endpoint_methods") or set()) and receiver_is_protocol:
            claims.append(
                _claim(
                    claim_type="protocol_endpoint_called",
                    protocol=protocol,
                    proof="typed_receiver_call",
                    fact_id=str(fact["id"]),
                    functions=[function_name],
                    receiver=receiver,
                    receiver_type=receiver_type or receiver_state_type,
                    callee=fact.get("callee"),
                )
            )

    effect_functions = (effects or {}).get("functions") if isinstance(effects, Mapping) else {}
    if isinstance(effect_functions, Mapping):
        for signature, info in effect_functions.items():
            if not isinstance(info, Mapping):
                continue
            labels = set(str(label) for label in info.get("effect_labels") or [])
            if not labels.intersection({"asset_pull", "asset_send", "mint", "burn"}):
                continue
            for claim in list(claims):
                calls_endpoint = claim["claim_type"] == "protocol_endpoint_called"
                if signature in (claim.get("functions") or []) and calls_endpoint:
                    claims.append(
                        _claim(
                            claim_type="cross_chain_asset_path",
                            protocol=protocol,
                            proof="asset_effect_plus_endpoint_call",
                            fact_id=str(claim["facts"][0]),
                            functions=[str(signature)],
                        )
                    )

    claim_types = {claim["claim_type"] for claim in claims}
    has_runtime_shape = bool(
        claim_types.intersection(
            {
                "cross_chain_receive_entrypoint",
                "protocol_peer_configured",
                "protocol_endpoint_called",
                "protocol_endpoint_declared",
                "protocol_security_config",
                "cross_chain_asset_path",
            }
        )
    )
    if has_runtime_shape and "cross_chain_asset_path" in claim_types:
        classification = str(spec["asset_classification"])
    elif has_runtime_shape:
        classification = str(spec["classification"])
    else:
        classification = "not_applicable"

    return {
        "protocol": protocol,
        "classification": classification,
        "claims": claims,
    }


def build_semantic_facts(contract: Any, effects: Mapping[str, Any] | None = None) -> dict[str, Any]:
    function_facts = [_function_fact(fn, idx) for idx, fn in enumerate(_entry_points(contract))]
    state_facts = [_state_variable_fact(var, idx) for idx, var in enumerate(_all_state_variables(contract))]
    inheritance_facts = _inheritance_facts(contract)
    call_facts: list[dict[str, Any]] = []
    for fn in _entry_points(contract):
        call_facts.extend(_call_facts_for_function(fn))

    layerzero = _layerzero_claims(
        function_facts=function_facts,
        state_facts=state_facts,
        inheritance_facts=inheritance_facts,
        call_facts=call_facts,
        effects=effects,
    )
    protocol_modules = {"layerzero": layerzero}
    for protocol_key in _GENERIC_PROTOCOL_SPECS:
        protocol_modules[protocol_key] = _generic_protocol_claims(
            protocol_key,
            function_facts=function_facts,
            state_facts=state_facts,
            inheritance_facts=inheritance_facts,
            call_facts=call_facts,
            effects=effects,
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "contract_name": getattr(contract, "name", None),
        "facts": [*function_facts, *state_facts, *inheritance_facts, *call_facts],
        "protocol_modules": protocol_modules,
    }


def protocol_claims_for_function(
    semantic_facts: Mapping[str, Any] | None,
    protocol: str,
    function_signature: str,
) -> list[dict[str, Any]]:
    if not isinstance(semantic_facts, Mapping):
        return []
    modules = semantic_facts.get("protocol_modules")
    if not isinstance(modules, Mapping):
        return []
    module_key = _compact(protocol)
    module = modules.get(protocol) or modules.get(module_key)
    if not isinstance(module, Mapping):
        return []
    claims = module.get("claims")
    if not isinstance(claims, list):
        return []
    out: list[dict[str, Any]] = []
    for claim in claims:
        if isinstance(claim, Mapping) and function_signature in (claim.get("functions") or []):
            out.append(dict(claim))
    return out


def classified_protocols(semantic_facts: Mapping[str, Any] | None) -> set[str]:
    if not isinstance(semantic_facts, Mapping):
        return set()
    modules = semantic_facts.get("protocol_modules")
    if not isinstance(modules, Mapping):
        return set()
    protocols: set[str] = set()
    for key, module in modules.items():
        if not isinstance(module, Mapping):
            continue
        if module.get("classification") in {None, "not_applicable"}:
            continue
        protocol = module.get("protocol")
        if isinstance(protocol, str) and protocol:
            protocols.add(protocol)
        else:
            protocols.add(_PROTOCOL_DISPLAY_NAMES.get(str(key), str(key)))
    return protocols
