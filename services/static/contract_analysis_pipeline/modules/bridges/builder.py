"""Bridge-context module orchestration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from schemas.contract_analysis import BridgeStaticContext, BridgeStaticFact, BridgeStaticFunction

from .base import BridgeModule, full_name
from .generic import GenericBridgeModule
from .layerzero import LayerZeroBridgeModule

SCHEMA_VERSION = "bridge_static.v1"

_MODULES: tuple[BridgeModule, ...] = (LayerZeroBridgeModule(), GenericBridgeModule())
_CONFIDENCE_RANK = {"unknown": 0, "low": 1, "medium": 2, "high": 3, "exact": 4}
_PROMOTED_GENERIC_KINDS = {"bridge_send", "bridge_receive", "bridge_peer_config", "bridge_effect_hint"}


def _confidence_rank(fact: BridgeStaticFact) -> int:
    return _CONFIDENCE_RANK.get(str(fact.get("confidence") or "unknown"), 0)


def _promoted_protocols(facts: list[BridgeStaticFact]) -> list[str]:
    protocols = {
        str(fact["protocol"])
        for fact in facts
        if fact.get("protocol") != "Bridge"
        and str(fact.get("display") or "") == "default"
        and _confidence_rank(fact) >= 2
    }
    return sorted(protocols)


def _generic_is_promoted(facts: list[BridgeStaticFact]) -> bool:
    generic_facts = [fact for fact in facts if fact.get("protocol") == "Bridge" and _confidence_rank(fact) >= 2]
    kinds = {str(fact.get("kind") or "") for fact in generic_facts}
    if len(kinds.intersection(_PROMOTED_GENERIC_KINDS)) >= 2 and "bridge_route_hint" in kinds:
        return True
    if "bridge_peer_config" in kinds and "bridge_send" in kinds:
        return True
    return False


def _promotion(facts: list[BridgeStaticFact]) -> tuple[bool, str, str]:
    if _promoted_protocols(facts) or _generic_is_promoted(facts):
        return True, "confirmed", "default"
    if facts:
        return False, "candidate", "debug"
    return False, "none", "hidden"


def _context_protocols(facts: list[BridgeStaticFact], is_bridge: bool) -> list[str]:
    protocols = _promoted_protocols(facts)
    if protocols:
        return protocols
    if is_bridge and _generic_is_promoted(facts):
        return ["Bridge"]
    return []


def build_bridge_static_context(contract: Any, effects: Mapping[str, Any] | None = None) -> BridgeStaticContext:
    effect_map = effects.get("functions") if isinstance(effects, Mapping) else {}
    if not isinstance(effect_map, Mapping):
        effect_map = {}

    facts: list[BridgeStaticFact] = []
    functions: list[BridgeStaticFunction] = []
    modules_used: set[str] = set()
    for fn in getattr(contract, "functions", []) or []:
        signature = full_name(fn)
        effect_info = effect_map.get(signature)
        effect_info = effect_info if isinstance(effect_info, Mapping) else None

        fn_facts: list[BridgeStaticFact] = []
        for module in _MODULES:
            module_facts = module.detect_function(fn, effect_info)
            if module_facts:
                fn_facts.extend(module_facts)
                modules_used.add(module.name)
                if any(fact.get("protocol") != "Bridge" for fact in module_facts):
                    break
        if not fn_facts:
            continue

        facts.extend(fn_facts)
        functions.append(
            {
                "function": signature,
                "protocols": sorted({str(fact["protocol"]) for fact in fn_facts}),
                "fact_kinds": sorted({str(fact["kind"]) for fact in fn_facts}),
                "effect_labels": list(effect_info.get("effect_labels") or []) if effect_info else [],
                "effect_targets": list(effect_info.get("effect_targets") or []) if effect_info else [],
                "action_summary": str(effect_info.get("action_summary") or "") if effect_info else "",
                "modules": sorted({str(fact.get("module") or "") for fact in fn_facts if fact.get("module")}),
            }
        )

    is_bridge, promotion, visibility = _promotion(facts)
    return {
        "schema_version": SCHEMA_VERSION,
        "is_bridge": is_bridge,
        "protocols": _context_protocols(facts, is_bridge),
        "fact_count": len(facts),
        "facts": facts,
        "functions": functions,
        "modules": sorted(modules_used),
        "promotion": promotion,
        "visibility": visibility,
    }
