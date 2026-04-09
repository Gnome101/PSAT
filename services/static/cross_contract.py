"""Cross-contract effect label enrichment.

After all contracts in a company are individually analyzed, this module
cross-references external call targets to propagate effect labels across
contract boundaries.

Example: Contract A calls token.randomMint(to, amt). If we've analyzed
the token contract and know that randomMint has effect label "mint",
we propagate "mint" to A's calling function.
"""

from __future__ import annotations

import logging
from typing import Any

from eth_utils.crypto import keccak

logger = logging.getLogger(__name__)


def _compute_selector(signature: str) -> str:
    """Compute the 4-byte selector for a function signature like 'mint(address,uint256)'."""
    return "0x" + keccak(text=signature).hex()[:8]


def build_callee_effect_map(
    analyses: dict[str, dict[str, Any]],
) -> dict[str, dict[str, list[str]]]:
    """Build a lookup: {contract_address → {selector → [effect_labels]}}.

    Parameters
    ----------
    analyses : dict
        Mapping of contract address (lowered) to its contract_analysis dict.
    """
    callee_map: dict[str, dict[str, list[str]]] = {}

    for address, analysis in analyses.items():
        address = address.lower()
        selector_effects: dict[str, list[str]] = {}

        # From privileged_functions in access_control
        ac = analysis.get("access_control", {})
        for pf in ac.get("privileged_functions", []):
            fn_sig = pf.get("function", "")
            effects = pf.get("effect_labels", [])
            if fn_sig and effects:
                selector = _compute_selector(fn_sig)
                selector_effects[selector] = effects
                # Also store by function name for high-level call matching
                fn_name = fn_sig.split("(")[0]
                selector_effects[f"name:{fn_name}"] = effects

        # Also include ALL entry point functions (not just privileged ones)
        # since a public mint/burn might not have access control
        for fn_data in analysis.get("permission_graph", {}).get("sinks", []):
            fn_sig = fn_data.get("function", "")
            effects = fn_data.get("effect_labels", [])
            if fn_sig and effects:
                selector = _compute_selector(fn_sig)
                if selector not in selector_effects:
                    selector_effects[selector] = effects
                fn_name = fn_sig.split("(")[0]
                if f"name:{fn_name}" not in selector_effects:
                    selector_effects[f"name:{fn_name}"] = effects

        if selector_effects:
            callee_map[address] = selector_effects

    return callee_map


def enrich_cross_contract_effects(
    target_analysis: dict[str, Any],
    controller_values: dict[str, Any],
    callee_map: dict[str, dict[str, list[str]]],
) -> dict[str, list[str]]:
    """Enrich a contract's privileged functions with cross-contract effect labels.

    Parameters
    ----------
    target_analysis : dict
        The contract_analysis of the contract to enrich.
    controller_values : dict
        The control_snapshot's controller_values — maps state var names to
        their on-chain addresses.
    callee_map : dict
        Output of build_callee_effect_map — {address → {selector → effects}}.

    Returns
    -------
    dict
        Mapping of function signature → list of new effect labels added.
        Only functions that gained new labels are included.
    """
    # Build a mapping: state_var_name → on-chain address
    var_to_address: dict[str, str] = {}
    for controller_id, cv in controller_values.items():
        val = cv.get("value", "")
        if not isinstance(val, str) or not val.startswith("0x"):
            continue
        # controller_id is like "state_variable:token" or "external_contract:authority"
        parts = controller_id.split(":", 1)
        if len(parts) == 2:
            var_name = parts[1].lower()
            var_to_address[var_name] = val.lower()

    enriched: dict[str, list[str]] = {}
    ac = target_analysis.get("access_control", {})

    for pf in ac.get("privileged_functions", []):
        fn_sig = pf.get("function", "")
        current_labels = set(pf.get("effect_labels", []))
        new_labels: set[str] = set()

        for target in pf.get("effect_targets", []):
            target_lower = target.lower()
            # Parse "varname.functionName" pattern
            if "." not in target_lower:
                continue
            parts = target_lower.split(".", 1)
            var_name = parts[0]
            called_fn = parts[1]

            # Resolve the variable to an address
            callee_addr = var_to_address.get(var_name)
            if not callee_addr or callee_addr not in callee_map:
                continue

            callee_effects = callee_map[callee_addr]

            # Look up by function name
            name_key = f"name:{called_fn}"
            if name_key in callee_effects:
                for label in callee_effects[name_key]:
                    if label not in current_labels:
                        new_labels.add(label)
                        logger.info(
                            "Cross-contract: %s calls %s.%s → propagating '%s'",
                            fn_sig.split("(")[0],
                            var_name,
                            called_fn,
                            label,
                        )

        if new_labels:
            pf["effect_labels"] = sorted(current_labels | new_labels)
            enriched[fn_sig] = sorted(new_labels)

    return enriched
