"""Conservative generic bridge-context detection."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from schemas.contract_analysis import BridgeStaticFact

from .base import (
    BridgeModule,
    effect_labels,
    fact,
    function_name_compact,
    function_name_tokens,
    parameter_names,
    parameter_tokens,
    state_variable_names,
    state_variable_tokens,
)

GENERIC_BRIDGE_MODULE = "generic_bridge"

_ROUTE_PARAM_NAMES = {
    "dsteid",
    "srceid",
    "dstchain",
    "srcchain",
    "dstchainid",
    "srcchainid",
    "destinationchain",
    "sourcechain",
    "destinationchainid",
    "sourcechainid",
    "targetchain",
    "chainselector",
    "destinationdomain",
    "sourcedomain",
    "remotedomain",
}
_PEER_STORAGE_NAMES = {
    "peers",
    "trustedremote",
    "trustedremotes",
    "trustedremotelookup",
    "remotepeers",
    "receivers",
}
_PEER_FUNCTION_NAMES = {"setpeer", "peers", "settrustedremote", "trustedremotelookup", "setreceiver", "getreceiver"}
_SEND_FUNCTION_NAMES = {"bridge", "bridgeto", "sendmessage", "sendtoken", "sendtokens", "xsend", "transferremote"}
_RECEIVE_FUNCTION_NAMES = {"receivemessage", "handlemessages", "handlemessage", "receivetoken", "receivebridge"}
_BRIDGE_EFFECT_LABELS = {
    "asset_pull",
    "asset_send",
    "mint",
    "burn",
    "external_contract_call",
    "arbitrary_external_call",
}


def _has_route_evidence(fn: Any) -> bool:
    return bool(parameter_names(fn).intersection(_ROUTE_PARAM_NAMES))


def _has_peer_evidence(fn: Any) -> bool:
    name = function_name_compact(fn)
    return name in _PEER_FUNCTION_NAMES or bool(state_variable_names(fn).intersection(_PEER_STORAGE_NAMES))


class GenericBridgeModule(BridgeModule):
    name = GENERIC_BRIDGE_MODULE

    def detect_function(self, fn: Any, effect_info: Mapping[str, Any] | None = None) -> list[BridgeStaticFact]:
        name = function_name_compact(fn)
        labels = effect_labels(effect_info)
        fn_tokens = function_name_tokens(fn)
        param_tokens = parameter_tokens(fn)
        state_tokens = state_variable_tokens(fn)
        has_route = _has_route_evidence(fn)
        has_peer = _has_peer_evidence(fn)
        has_bridge_effect = bool(labels.intersection(_BRIDGE_EFFECT_LABELS))

        facts: list[BridgeStaticFact] = []
        if has_route:
            facts.append(
                fact(
                    "bridge_route_hint",
                    "Bridge",
                    fn,
                    "exact route/domain/chain parameter",
                    "medium",
                    module=self.name,
                    display="candidate",
                )
            )
        if has_peer:
            facts.append(
                fact(
                    "bridge_peer_config",
                    "Bridge",
                    fn,
                    "exact peer/trusted-remote function or storage",
                    "medium",
                    module=self.name,
                    display="candidate",
                )
            )
        if name in _SEND_FUNCTION_NAMES or (
            has_route and has_bridge_effect and {"send", "bridge"}.intersection(fn_tokens)
        ):
            facts.append(
                fact(
                    "bridge_send",
                    "Bridge",
                    fn,
                    "exact bridge/send function with route evidence",
                    "medium",
                    module=self.name,
                    display="candidate",
                )
            )
        if name in _RECEIVE_FUNCTION_NAMES or (
            {"remote", "source", "origin"}.intersection(param_tokens | state_tokens) and "receive" in fn_tokens
        ):
            facts.append(
                fact(
                    "bridge_receive",
                    "Bridge",
                    fn,
                    "exact receive/handle function with remote evidence",
                    "medium",
                    module=self.name,
                    display="candidate",
                )
            )
        if has_bridge_effect and (has_route or has_peer):
            facts.append(
                fact(
                    "bridge_effect_hint",
                    "Bridge",
                    fn,
                    "route/peer-shaped function has external/value effects",
                    "medium",
                    module=self.name,
                    display="candidate",
                )
            )
        return facts
