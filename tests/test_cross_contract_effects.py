"""Cross-contract effect label enrichment tests.

Tests that when contract A calls contract B.someFunc(), and we've analyzed
contract B and know someFunc's effect labels, those labels propagate to A.
"""

import random
import string
import sys
from pathlib import Path

from eth_utils.crypto import keccak

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static.cross_contract import build_callee_effect_map, enrich_cross_contract_effects


def _rand(n: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=n))


def _selector(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()[:8]


def _callee_effects(callee_signature: str, labels: list[str]) -> dict:
    return {
        "schema_version": "semantic",
        "functions": {
            callee_signature: {
                "selector": _selector(callee_signature),
                "effect_labels": labels,
            }
        },
    }


def _target_effects(
    caller_signature: str,
    target_var: str,
    callee_signature: str,
    *,
    effect_labels: list[str] | None = None,
) -> dict:
    callee_name = callee_signature.split("(", 1)[0]
    return {
        "schema_version": "semantic",
        "functions": {
            caller_signature: {
                "effect_labels": effect_labels or [],
                "sinks": [
                    {
                        "kind": "external_call",
                        "target": f"{target_var}.{callee_name}",
                        "selector": _selector(callee_signature),
                    }
                ],
            }
        },
    }


TOKEN_ADDRESS = "0x1111111111111111111111111111111111111111"
CALLER_ADDRESS = "0x2222222222222222222222222222222222222222"


def test_basic_cross_contract_mint():
    """A calls token.randomMint() → B's randomMint has 'mint' label → A gets 'mint'."""
    mint_fn = _rand()
    callee_signature = f"{mint_fn}(address,uint256)"
    caller_signature = "doStuff(address,uint256)"

    # Contract B's analysis (the token)
    token_analysis = {
        "semantic_control": {
            "semantic_functions": [
                {
                    "function": callee_signature,
                    "effect_labels": ["mint"],
                    "effect_targets": ["balances", "totalSupply"],
                }
            ]
        },
    }

    # Build callee map from B's analysis
    callee_map = build_callee_effect_map(
        {TOKEN_ADDRESS: token_analysis},
        effects_by_address={TOKEN_ADDRESS: _callee_effects(callee_signature, ["mint"])},
    )
    assert TOKEN_ADDRESS in callee_map
    assert f"name:{mint_fn}" not in callee_map[TOKEN_ADDRESS]
    assert _selector(callee_signature) in callee_map[TOKEN_ADDRESS]

    # Contract A's analysis (calls token.randomMint)
    caller_analysis = {
        "semantic_control": {
            "semantic_functions": [
                {
                    "function": caller_signature,
                    "effect_labels": ["external_contract_call"],
                    "effect_targets": [f"token.{mint_fn}"],
                }
            ]
        },
    }

    # Controller values: token state var → actual address
    controller_values = {
        "state_variable:token": {"value": TOKEN_ADDRESS},
    }

    enriched = enrich_cross_contract_effects(
        caller_analysis,
        controller_values,
        callee_map,
        target_effects=_target_effects(caller_signature, "token", callee_signature),
    )
    assert caller_signature in enriched
    assert "mint" in enriched[caller_signature]


def test_cross_contract_burn():
    """A calls token.randomBurn() → propagates 'burn'."""
    burn_fn = _rand()
    callee_signature = f"{burn_fn}(address,uint256)"
    caller_signature = "doStuff(uint256)"

    token_analysis = {
        "semantic_control": {
            "semantic_functions": [
                {
                    "function": callee_signature,
                    "effect_labels": ["burn"],
                    "effect_targets": [],
                }
            ]
        },
    }

    callee_map = build_callee_effect_map(
        {TOKEN_ADDRESS: token_analysis},
        effects_by_address={TOKEN_ADDRESS: _callee_effects(callee_signature, ["burn"])},
    )

    caller_analysis = {
        "semantic_control": {
            "semantic_functions": [
                {
                    "function": caller_signature,
                    "effect_labels": ["external_contract_call"],
                    "effect_targets": [f"token.{burn_fn}"],
                }
            ]
        },
    }

    controller_values = {"state_variable:token": {"value": TOKEN_ADDRESS}}
    enriched = enrich_cross_contract_effects(
        caller_analysis,
        controller_values,
        callee_map,
        target_effects=_target_effects(caller_signature, "token", callee_signature),
    )
    assert "burn" in enriched.get(caller_signature, [])


def test_cross_contract_multiple_effects():
    """Target function has multiple effects → all propagate."""
    fn = _rand()
    callee_signature = f"{fn}(address,uint256)"
    caller_signature = "execute(address,uint256)"

    token_analysis = {
        "semantic_control": {
            "semantic_functions": [
                {
                    "function": callee_signature,
                    "effect_labels": ["mint", "role_management", "external_contract_call"],
                    "effect_targets": [],
                }
            ]
        },
    }

    callee_map = build_callee_effect_map(
        {TOKEN_ADDRESS: token_analysis},
        effects_by_address={
            TOKEN_ADDRESS: _callee_effects(callee_signature, ["mint", "role_management", "external_contract_call"])
        },
    )

    caller_analysis = {
        "semantic_control": {
            "semantic_functions": [
                {
                    "function": caller_signature,
                    "effect_labels": [],
                    "effect_targets": [f"token.{fn}"],
                }
            ]
        },
    }

    controller_values = {"state_variable:token": {"value": TOKEN_ADDRESS}}
    enriched = enrich_cross_contract_effects(
        caller_analysis,
        controller_values,
        callee_map,
        target_effects=_target_effects(caller_signature, "token", callee_signature),
    )
    new_labels = enriched.get(caller_signature, [])
    assert "mint" in new_labels
    assert "role_management" in new_labels


def test_no_enrichment_when_callee_not_analyzed():
    """If the callee contract isn't in the callee map, nothing happens."""
    caller_signature = "doStuff()"
    callee_signature = "randomMint(address,uint256)"
    caller_analysis = {
        "semantic_control": {
            "semantic_functions": [
                {
                    "function": caller_signature,
                    "effect_labels": ["external_contract_call"],
                    "effect_targets": ["token.randomMint"],
                }
            ]
        },
    }

    controller_values = {"state_variable:token": {"value": TOKEN_ADDRESS}}
    enriched = enrich_cross_contract_effects(
        caller_analysis,
        controller_values,
        {},
        target_effects=_target_effects(caller_signature, "token", callee_signature),
    )
    assert enriched == {}


def test_no_enrichment_when_address_unknown():
    """If the state variable's address isn't resolved, nothing happens."""
    caller_signature = "doStuff()"
    callee_signature = "mint(address,uint256)"
    callee_map = build_callee_effect_map(
        {TOKEN_ADDRESS: {}},
        effects_by_address={TOKEN_ADDRESS: _callee_effects(callee_signature, ["mint"])},
    )

    caller_analysis = {
        "semantic_control": {
            "semantic_functions": [
                {
                    "function": caller_signature,
                    "effect_labels": ["external_contract_call"],
                    "effect_targets": ["token.mint"],
                }
            ]
        },
    }

    # No controller_values → can't resolve "token" to an address
    enriched = enrich_cross_contract_effects(
        caller_analysis,
        {},
        callee_map,
        target_effects=_target_effects(caller_signature, "token", callee_signature),
    )
    assert enriched == {}


def test_no_duplicate_labels():
    """If the caller already has the label, don't add it again."""
    caller_signature = "withdraw(address)"
    callee_signature = "transfer(address,uint256)"
    callee_map = build_callee_effect_map(
        {TOKEN_ADDRESS: {}},
        effects_by_address={TOKEN_ADDRESS: _callee_effects(callee_signature, ["asset_send"])},
    )

    caller_analysis = {
        "semantic_control": {
            "semantic_functions": [
                {
                    "function": caller_signature,
                    "effect_labels": ["asset_send"],  # already has it
                    "effect_targets": ["token.transfer"],
                }
            ]
        },
    }

    controller_values = {"state_variable:token": {"value": TOKEN_ADDRESS}}
    enriched = enrich_cross_contract_effects(
        caller_analysis,
        controller_values,
        callee_map,
        target_effects=_target_effects(caller_signature, "token", callee_signature, effect_labels=["asset_send"]),
    )
    assert enriched == {}  # nothing new added


def test_external_contract_controller_id_format():
    """Controller IDs can be 'external_contract:xxx' not just 'state_variable:xxx'."""
    fn = _rand()
    callee_signature = f"{fn}(uint256)"
    caller_signature = "manage()"

    callee_map = build_callee_effect_map(
        {TOKEN_ADDRESS: {}},
        effects_by_address={TOKEN_ADDRESS: _callee_effects(callee_signature, ["pause_toggle"])},
    )

    caller_analysis = {
        "semantic_control": {
            "semantic_functions": [
                {
                    "function": caller_signature,
                    "effect_labels": [],
                    "effect_targets": [f"registry.{fn}"],
                }
            ]
        },
    }

    controller_values = {"external_contract:registry": {"value": TOKEN_ADDRESS}}
    enriched = enrich_cross_contract_effects(
        caller_analysis,
        controller_values,
        callee_map,
        target_effects=_target_effects(caller_signature, "registry", callee_signature),
    )
    assert "pause_toggle" in enriched.get(caller_signature, [])


def test_chain_propagation():
    """A calls B.foo(), B.foo() calls C.bar() — if we analyze in sequence,
    B's enriched labels should propagate to A."""
    fn_b = _rand()
    fn_c = _rand()
    sig_b = f"{fn_b}(address,uint256)"
    sig_c = f"{fn_c}(address,uint256)"
    sig_a = "execute(address,uint256)"
    addr_b = "0x3333333333333333333333333333333333333333"
    addr_c = "0x4444444444444444444444444444444444444444"

    # C's analysis: fn_c has "mint"
    c_analysis = {
        "semantic_control": {
            "semantic_functions": [{"function": sig_c, "effect_labels": ["mint"], "effect_targets": []}]
        },
    }

    # B's analysis: fn_b calls C's fn_c
    b_analysis = {
        "semantic_control": {
            "semantic_functions": [
                {
                    "function": sig_b,
                    "effect_labels": ["external_contract_call"],
                    "effect_targets": [f"registry.{fn_c}"],
                }
            ]
        },
    }

    # First pass: enrich B with C's effects
    callee_map_1 = build_callee_effect_map(
        {addr_c: c_analysis},
        effects_by_address={addr_c: _callee_effects(sig_c, ["mint"])},
    )
    b_controllers = {"state_variable:registry": {"value": addr_c}}
    enriched_b = enrich_cross_contract_effects(
        b_analysis,
        b_controllers,
        callee_map_1,
        target_effects=_target_effects(sig_b, "registry", sig_c),
    )

    assert "mint" in enriched_b.get(sig_b, [])

    # Second pass: enrich A with B's semantically updated effects carrier.
    callee_map_2 = build_callee_effect_map(
        {addr_b: b_analysis},
        effects_by_address={addr_b: _callee_effects(sig_b, ["external_contract_call", *enriched_b.get(sig_b, [])])},
    )

    a_analysis = {
        "semantic_control": {
            "semantic_functions": [
                {
                    "function": sig_a,
                    "effect_labels": [],
                    "effect_targets": [f"handler.{fn_b}"],
                }
            ]
        },
    }
    a_controllers = {"state_variable:handler": {"value": addr_b}}
    enriched = enrich_cross_contract_effects(
        a_analysis,
        a_controllers,
        callee_map_2,
        target_effects=_target_effects(sig_a, "handler", sig_b),
    )

    # A should also have "mint" now (propagated through B)
    assert "mint" in enriched.get(sig_a, [])
