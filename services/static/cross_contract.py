"""Cross-contract effect label enrichment.

After all contracts in a company are individually analyzed, this module
cross-references external call targets to propagate effect labels across
contract boundaries.

Example: Contract A calls a token method with selector ``0x40c10f19``.
If we've analyzed the token contract and know that selector carries the
``mint`` effect label, we propagate ``mint`` to A's calling function.
"""

from __future__ import annotations

import logging
from typing import Any

from eth_utils.crypto import keccak

logger = logging.getLogger(__name__)


def _compute_selector(signature: str) -> str | None:
    """Compute the 4-byte selector for a canonical ABI signature."""
    if not signature or "(" not in signature or not signature.endswith(")"):
        return None
    return "0x" + keccak(text=signature).hex()[:8]


def _merge_labels(out: dict[str, list[str]], selector: str | None, labels: Any) -> None:
    if not selector or not isinstance(labels, list):
        return
    existing = set(out.get(selector, []))
    existing.update(str(label) for label in labels if label)
    if existing:
        out[selector] = sorted(existing)


def build_callee_effect_map(
    analyses: dict[str, dict[str, Any]],
    *,
    effects_by_address: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, list[str]]]:
    """Build a lookup: {contract_address → {selector → [effect_labels]}}.

    Parameters
    ----------
    analyses : dict
        Mapping of contract address (lowered) to its contract_analysis dict.
    effects_by_address : dict, optional
        Mapping of contract address (lowered) to its semantic ``effects``
        artifact. Effect labels come from the per-function effects
        carrier, not from the static semantic summary.
    """
    callee_map: dict[str, dict[str, list[str]]] = {}
    effects_by_address = effects_by_address or {}

    for address in set(analyses) | set(effects_by_address):
        address = address.lower()
        selector_effects: dict[str, list[str]] = {}

        effects_artifact = effects_by_address.get(address) or {}
        for fn_sig, fn_record in (effects_artifact.get("functions") or {}).items():
            if not isinstance(fn_record, dict):
                continue
            effects = fn_record.get("effect_labels") or []
            if not fn_sig or not effects:
                continue
            raw_selector = fn_record.get("selector")
            selector = raw_selector if isinstance(raw_selector, str) and raw_selector.startswith("0x") else None
            _merge_labels(selector_effects, selector or _compute_selector(str(fn_sig)), effects)

        if selector_effects:
            callee_map[address] = selector_effects

    return callee_map


def _var_to_address(controller_values: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for controller_id, cv in controller_values.items():
        if not isinstance(cv, dict):
            continue
        val = cv.get("value", "")
        if not isinstance(val, str) or not val.startswith("0x"):
            continue
        parts = controller_id.split(":", 1)
        if len(parts) == 2:
            out[parts[1].lower()] = val.lower()
    return out


def _external_call_sinks_by_function(target_effects: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(target_effects, dict):
        return {}
    functions = target_effects.get("functions")
    if not isinstance(functions, dict):
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for fn_sig, record in functions.items():
        if not isinstance(fn_sig, str) or not isinstance(record, dict):
            continue
        sinks = []
        for raw_sink in record.get("sinks") or []:
            if not isinstance(raw_sink, dict):
                continue
            if raw_sink.get("kind") != "external_call":
                continue
            target = raw_sink.get("target")
            selector = raw_sink.get("selector")
            if isinstance(target, str) and "." in target and isinstance(selector, str) and selector.startswith("0x"):
                sinks.append(raw_sink)
        if sinks:
            out[fn_sig] = sinks
    return out


def enrich_cross_contract_effects(
    target_analysis: dict[str, Any],
    controller_values: dict[str, Any],
    callee_map: dict[str, dict[str, list[str]]],
    *,
    target_effects: dict[str, Any] | None = None,
) -> dict[str, list[str]]:
    """Enrich a contract's semantic effect records with cross-contract labels.

    Parameters
    ----------
    target_analysis : dict
        The contract_analysis of the contract to enrich.
    controller_values : dict
        The control_snapshot's controller_values — maps state var names to
        their on-chain addresses.
    callee_map : dict
        Output of build_callee_effect_map — {address → {selector → effects}}.
    target_effects : dict, optional
        The target contract's semantic ``effects`` artifact. External calls are
        matched by ``sinks[*].selector``; no source-name fallback is used.

    Returns
    -------
    dict
        Mapping of function signature → list of new effect labels added.
        Only functions that gained new labels are included.
    """
    del target_analysis
    var_to_address = _var_to_address(controller_values)
    external_sinks = _external_call_sinks_by_function(target_effects)
    functions = (target_effects or {}).get("functions") if isinstance(target_effects, dict) else {}
    enriched: dict[str, list[str]] = {}

    for fn_sig, effect_info in external_sinks.items():
        if not isinstance(fn_sig, str):
            continue
        fn_record = functions.get(fn_sig) if isinstance(functions, dict) else None
        current_labels = set(fn_record.get("effect_labels") or []) if isinstance(fn_record, dict) else set()
        new_labels: set[str] = set()

        for sink in effect_info:
            target = str(sink.get("target", "")).lower()
            if "." not in target:
                continue
            var_name = target.split(".", 1)[0]
            callee_addr = var_to_address.get(var_name)
            if not callee_addr or callee_addr not in callee_map:
                continue
            selector = str(sink.get("selector", "")).lower()
            callee_effects = callee_map[callee_addr]
            for label in callee_effects.get(selector, []):
                if label not in current_labels:
                    new_labels.add(label)
                    logger.info(
                        "Cross-contract: %s calls %s selector %s; propagating '%s'",
                        fn_sig.split("(")[0],
                        var_name,
                        selector,
                        label,
                    )

        if new_labels:
            enriched[fn_sig] = sorted(new_labels)

    return enriched
