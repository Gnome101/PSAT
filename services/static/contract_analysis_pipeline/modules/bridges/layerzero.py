"""LayerZero-specific bridge-context detection."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from schemas.contract_analysis import BridgeStaticFact

from .base import (
    BridgeModule,
    contract_type_names,
    effect_labels,
    effect_targets,
    fact,
    function_name_compact,
    function_name_tokens,
    parameter_names,
    parameter_tokens,
    parameter_type_names,
    state_variable_tokens,
    state_variable_type_names,
)

LAYERZERO_MODULE = "layerzero"

_LAYERZERO_TYPES = {
    "ilayerzeroendpoint",
    "ilayerzeroendpointv2",
    "ilzendpoint",
    "ilzendpointv2",
    "messagingfee",
    "sendparam",
    "origin",
}
_LAYERZERO_BASES = {
    "oapp",
    "oappcore",
    "oft",
    "oftcore",
    "oftadapter",
    "onft",
    "onft721",
    "onft1155",
}
_ENDPOINT_ACCESSORS = {"endpoint", "getendpoint", "lzendpoint"}
_PEER_FUNCTIONS = {"setpeer", "peers", "settrustedremote", "trustedremotelookup", "getreceiver"}
_RECEIVE_FUNCTIONS = {"lzreceive", "_lzreceive", "lzcompose", "_lzcompose"}
_SECURITY_FUNCTIONS = {
    "setconfig",
    "setsendlibrary",
    "setreceivelibrary",
    "setreceivelibrarytimeout",
    "setenforcedoptions",
    "setdelegate",
}
_LAYERZERO_ROUTE_PARAMS = {"dsteid", "srceid", "eid"}
_VALUE_LABELS = {"asset_pull", "asset_send", "mint", "burn"}


def _has_layerzero_anchor(fn: Any, effect_info: Mapping[str, Any] | None) -> bool:
    name = function_name_compact(fn)
    if name in _RECEIVE_FUNCTIONS | _PEER_FUNCTIONS | _ENDPOINT_ACCESSORS | _SECURITY_FUNCTIONS:
        return True
    if contract_type_names(fn).intersection(_LAYERZERO_BASES):
        return True
    if parameter_type_names(fn).intersection(_LAYERZERO_TYPES):
        return True
    if state_variable_type_names(fn).intersection(_LAYERZERO_TYPES):
        return True
    targets = effect_targets(effect_info)
    return bool(targets.intersection({"ilayerzeroendpoint", "ilayerzeroendpointv2"}))


def _has_route_param(fn: Any) -> bool:
    return bool(parameter_names(fn).intersection(_LAYERZERO_ROUTE_PARAMS))


class LayerZeroBridgeModule(BridgeModule):
    name = LAYERZERO_MODULE

    def detect_function(self, fn: Any, effect_info: Mapping[str, Any] | None = None) -> list[BridgeStaticFact]:
        if not _has_layerzero_anchor(fn, effect_info):
            return []

        name = function_name_compact(fn)
        tokens = function_name_tokens(fn) | parameter_tokens(fn) | state_variable_tokens(fn)
        labels = effect_labels(effect_info)

        facts: list[BridgeStaticFact] = [
            fact(
                "protocol_shape",
                "LayerZero",
                fn,
                "exact LayerZero interface/base/function evidence",
                "high",
                module=self.name,
                display="default",
            )
        ]

        if name in _RECEIVE_FUNCTIONS:
            facts.append(
                fact(
                    "bridge_receive",
                    "LayerZero",
                    fn,
                    "exact LayerZero receive entrypoint",
                    "high",
                    module=self.name,
                    display="default",
                )
            )
        if name in _PEER_FUNCTIONS or parameter_names(fn).intersection(_LAYERZERO_ROUTE_PARAMS) and "peer" in tokens:
            facts.append(
                fact(
                    "bridge_peer_config",
                    "LayerZero",
                    fn,
                    "exact peer/trusted-remote function or route-keyed peer parameter",
                    "high",
                    module=self.name,
                    display="default",
                )
            )
        if name in _ENDPOINT_ACCESSORS or state_variable_type_names(fn).intersection(_LAYERZERO_TYPES):
            facts.append(
                fact(
                    "bridge_endpoint",
                    "LayerZero",
                    fn,
                    "exact endpoint accessor or LayerZero endpoint type",
                    "high",
                    module=self.name,
                    display="default",
                )
            )
        if name in _SECURITY_FUNCTIONS or {"dvn", "uln", "executor", "library"}.intersection(tokens):
            facts.append(
                fact(
                    "bridge_security_config",
                    "LayerZero",
                    fn,
                    "exact LayerZero config/library/delegate shape",
                    "high",
                    module=self.name,
                    display="default",
                )
            )
        if name == "send" or ("send" in tokens and _has_route_param(fn)):
            facts.append(
                fact(
                    "bridge_send",
                    "LayerZero",
                    fn,
                    "send function with exact LayerZero EID parameter",
                    "high",
                    module=self.name,
                    display="default",
                )
            )
        if labels.intersection(_VALUE_LABELS):
            facts.append(
                fact(
                    "bridge_asset_path",
                    "LayerZero",
                    fn,
                    "LayerZero-shaped function has value movement effects",
                    "medium",
                    module=self.name,
                    display="default",
                )
            )
        return facts
