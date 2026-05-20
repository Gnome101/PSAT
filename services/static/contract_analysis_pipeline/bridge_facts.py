"""Static bridge-shape extraction for contract analysis."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from schemas.contract_analysis import BridgeStaticContext, BridgeStaticFact, BridgeStaticFunction

SCHEMA_VERSION = "bridge_static.v1"

_CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_TOKEN_SPLIT_RE = re.compile(r"[^A-Za-z0-9]+")

_LAYERZERO_TYPE_MARKERS = {
    "ilayerzeroendpoint",
    "layerzeroendpoint",
    "endpointv2",
    "oapp",
    "oappcore",
    "oft",
    "oftcore",
    "oftadapter",
    "onft",
    "messagingfee",
    "sendparam",
    "origin",
}
_LAYERZERO_RECEIVE_NAMES = {"lzreceive", "_lzreceive", "lzcompose", "_lzcompose"}
_LAYERZERO_PEER_NAMES = {"setpeer", "peers", "settrustedremote", "trustedremotelookup", "getreceiver"}
_LAYERZERO_ENDPOINT_NAMES = {
    "endpoint",
    "getendpoint",
    "send",
    "quote",
    "setdelegate",
    "sendcompose",
}
_LAYERZERO_SECURITY_NAMES = {
    "setconfig",
    "setsendlibrary",
    "setreceivelibrary",
    "setreceivelibrarytimeout",
    "setenforcedoptions",
    "dvn",
    "uln",
    "messagelib",
    "sendlibrary",
    "receivelibrary",
    "executor",
}
_CHAIN_PARAM_MARKERS = {
    "dsteid",
    "srceid",
    "eid",
    "dstchain",
    "srcchain",
    "destinationchain",
    "sourcechain",
    "targetchain",
    "chainselector",
    "destinationdomain",
    "sourcedomain",
    "remotedomain",
}
_GENERIC_BRIDGE_MARKERS = {
    "bridge",
    "gateway",
    "router",
    "endpoint",
    "mailbox",
    "relayer",
    "remote",
    "peer",
    "trustedremote",
}
_BRIDGE_EFFECT_LABELS = {
    "asset_pull",
    "asset_send",
    "mint",
    "burn",
    "external_contract_call",
    "arbitrary_external_call",
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


def _function_identifiers(fn: Any, effect_info: Mapping[str, Any] | None = None) -> list[str]:
    identifiers = [
        str(getattr(fn, "name", "") or ""),
        _full_name(fn),
        str(getattr(getattr(fn, "contract", None), "name", "") or ""),
    ]
    for param in getattr(fn, "parameters", []) or []:
        identifiers.append(str(getattr(param, "name", "") or ""))
        identifiers.append(str(getattr(param, "type", "") or ""))
    for attr in ("all_state_variables_read", "all_state_variables_written"):
        getter = getattr(fn, attr, None)
        if not callable(getter):
            continue
        try:
            values = getter()
        except Exception:
            continue
        if not isinstance(values, (list, tuple, set)):
            continue
        for variable in values:
            identifiers.append(str(getattr(variable, "name", "") or ""))
            identifiers.append(str(getattr(variable, "type", "") or ""))
    if isinstance(effect_info, Mapping):
        for sink in effect_info.get("sinks") or []:
            if isinstance(sink, Mapping):
                identifiers.append(str(sink.get("target") or ""))
    return identifiers


def _function_tokens(fn: Any, effect_info: Mapping[str, Any] | None = None) -> set[str]:
    out: set[str] = set()
    for identifier in _function_identifiers(fn, effect_info):
        out.update(_tokens(identifier))
    return out


def _function_compact_text(fn: Any, effect_info: Mapping[str, Any] | None = None) -> str:
    return " ".join(_compact(identifier) for identifier in _function_identifiers(fn, effect_info))


def _layerzero_protocol_matches(fn: Any, effect_info: Mapping[str, Any] | None = None) -> bool:
    text = _function_compact_text(fn, effect_info)
    if any(marker in text for marker in _LAYERZERO_TYPE_MARKERS):
        return True
    return bool(_function_tokens(fn, effect_info).intersection({"layerzero", "oapp", "oft", "onft"}))


def _generic_bridge_matches(fn: Any, effect_info: Mapping[str, Any] | None = None) -> bool:
    tokens = _function_tokens(fn, effect_info)
    if tokens.intersection(_GENERIC_BRIDGE_MARKERS | _CHAIN_PARAM_MARKERS):
        return True
    return any(marker in _function_compact_text(fn, effect_info) for marker in _GENERIC_BRIDGE_MARKERS)


def _fact(kind: str, protocol: str, fn: Any, evidence: str, confidence: str = "medium") -> BridgeStaticFact:
    return {
        "kind": kind,
        "protocol": protocol,
        "function": _full_name(fn),
        "evidence": evidence,
        "confidence": confidence,
    }


def _layerzero_facts(fn: Any, effect_info: Mapping[str, Any] | None) -> list[BridgeStaticFact]:
    if not _layerzero_protocol_matches(fn, effect_info):
        return []
    name = _compact(getattr(fn, "name", "") or _full_name(fn))
    tokens = _function_tokens(fn, effect_info)
    facts: list[BridgeStaticFact] = [_fact("protocol_shape", "LayerZero", fn, "LayerZero/OApp/OFT identifiers")]
    if name in _LAYERZERO_RECEIVE_NAMES:
        facts.append(_fact("bridge_receive", "LayerZero", fn, "LayerZero receive entrypoint", "high"))
    if name in _LAYERZERO_PEER_NAMES or tokens.intersection({"peer", "trusted", "remote"}):
        facts.append(_fact("bridge_peer_config", "LayerZero", fn, "peer/trusted remote configuration"))
    if name in _LAYERZERO_ENDPOINT_NAMES or tokens.intersection({"endpoint", "delegate"}):
        facts.append(_fact("bridge_endpoint", "LayerZero", fn, "endpoint/send/delegate shape"))
    if name in _LAYERZERO_SECURITY_NAMES or tokens.intersection({"dvn", "uln", "executor", "library"}):
        facts.append(_fact("bridge_security_config", "LayerZero", fn, "DVN/library/executor configuration"))
    if "send" in tokens or bool(tokens.intersection(_CHAIN_PARAM_MARKERS)):
        facts.append(_fact("bridge_send", "LayerZero", fn, "send or remote-chain parameters"))
    labels = set(effect_info.get("effect_labels") or []) if isinstance(effect_info, Mapping) else set()
    if labels.intersection({"asset_pull", "asset_send", "mint", "burn"}):
        facts.append(_fact("bridge_asset_path", "LayerZero", fn, "asset movement effects"))
    return facts


def _generic_bridge_facts(fn: Any, effect_info: Mapping[str, Any] | None) -> list[BridgeStaticFact]:
    if not _generic_bridge_matches(fn, effect_info):
        return []
    tokens = _function_tokens(fn, effect_info)
    labels = set(effect_info.get("effect_labels") or []) if isinstance(effect_info, Mapping) else set()
    facts = [_fact("bridge_shape", "Bridge", fn, "bridge/peer/remote identifiers", "low")]
    if tokens.intersection(_CHAIN_PARAM_MARKERS):
        facts.append(_fact("bridge_route_hint", "Bridge", fn, "chain/domain/eid parameter"))
    if labels.intersection(_BRIDGE_EFFECT_LABELS):
        facts.append(_fact("bridge_effect_hint", "Bridge", fn, "bridge-shaped function has external/value effects"))
    return facts


def build_bridge_static_context(contract: Any, effects: Mapping[str, Any] | None = None) -> BridgeStaticContext:
    effect_map = effects.get("functions") if isinstance(effects, Mapping) else {}
    if not isinstance(effect_map, Mapping):
        effect_map = {}

    facts: list[BridgeStaticFact] = []
    functions: list[BridgeStaticFunction] = []
    for fn in getattr(contract, "functions", []) or []:
        signature = _full_name(fn)
        effect_info = effect_map.get(signature)
        effect_info = effect_info if isinstance(effect_info, Mapping) else None
        fn_facts = _layerzero_facts(fn, effect_info) or _generic_bridge_facts(fn, effect_info)
        if not fn_facts:
            continue
        facts.extend(fn_facts)
        functions.append(
            {
                "function": signature,
                "protocols": sorted({fact["protocol"] for fact in fn_facts}),
                "fact_kinds": sorted({fact["kind"] for fact in fn_facts}),
                "effect_labels": list(effect_info.get("effect_labels") or []) if effect_info else [],
                "effect_targets": list(effect_info.get("effect_targets") or []) if effect_info else [],
                "action_summary": str(effect_info.get("action_summary") or "") if effect_info else "",
            }
        )

    protocols = sorted({fact["protocol"] for fact in facts if fact["protocol"] != "Bridge"})
    if facts and not protocols:
        protocols = ["Bridge"]
    return {
        "schema_version": SCHEMA_VERSION,
        "is_bridge": bool(facts),
        "protocols": protocols,
        "fact_count": len(facts),
        "facts": facts,
        "functions": functions,
    }
