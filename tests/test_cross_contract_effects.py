"""Cross-contract effect label enrichment tests.

Tests that when contract A calls contract B.someFunc(), and we've analyzed
contract B and know someFunc's effect labels, those labels propagate to A.
"""

import random
import string
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static.cross_contract import build_callee_effect_map, enrich_cross_contract_effects


def _rand(n: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=n))


TOKEN_ADDRESS = "0x1111111111111111111111111111111111111111"
CALLER_ADDRESS = "0x2222222222222222222222222222222222222222"


def test_basic_cross_contract_mint():
    """A calls token.randomMint() → B's randomMint has 'mint' label → A gets 'mint'."""
    mint_fn = _rand()

    # Contract B's analysis (the token)
    token_analysis = {
        "access_control": {
            "privileged_functions": [
                {
                    "function": f"{mint_fn}(address,uint256)",
                    "effect_labels": ["mint"],
                    "effect_targets": ["balances", "totalSupply"],
                }
            ]
        },
    }

    # Build callee map from B's analysis
    callee_map = build_callee_effect_map({TOKEN_ADDRESS: token_analysis})
    assert TOKEN_ADDRESS in callee_map
    assert f"name:{mint_fn}" in callee_map[TOKEN_ADDRESS]

    # Contract A's analysis (calls token.randomMint)
    caller_analysis = {
        "access_control": {
            "privileged_functions": [
                {
                    "function": "doStuff(address,uint256)",
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

    enriched = enrich_cross_contract_effects(caller_analysis, controller_values, callee_map)
    assert "doStuff(address,uint256)" in enriched
    assert "mint" in enriched["doStuff(address,uint256)"]

    # Verify the analysis was mutated
    pf = caller_analysis["access_control"]["privileged_functions"][0]
    assert "mint" in pf["effect_labels"]


def test_cross_contract_burn():
    """A calls token.randomBurn() → propagates 'burn'."""
    burn_fn = _rand()

    token_analysis = {
        "access_control": {
            "privileged_functions": [
                {
                    "function": f"{burn_fn}(address,uint256)",
                    "effect_labels": ["burn"],
                    "effect_targets": [],
                }
            ]
        },
    }

    callee_map = build_callee_effect_map({TOKEN_ADDRESS: token_analysis})

    caller_analysis = {
        "access_control": {
            "privileged_functions": [
                {
                    "function": "doStuff(uint256)",
                    "effect_labels": ["external_contract_call"],
                    "effect_targets": [f"token.{burn_fn}"],
                }
            ]
        },
    }

    controller_values = {"state_variable:token": {"value": TOKEN_ADDRESS}}
    enriched = enrich_cross_contract_effects(caller_analysis, controller_values, callee_map)
    assert "burn" in enriched.get("doStuff(uint256)", [])


def test_cross_contract_multiple_effects():
    """Target function has multiple effects → all propagate."""
    fn = _rand()

    token_analysis = {
        "access_control": {
            "privileged_functions": [
                {
                    "function": f"{fn}(address,uint256)",
                    "effect_labels": ["mint", "role_management", "external_contract_call"],
                    "effect_targets": [],
                }
            ]
        },
    }

    callee_map = build_callee_effect_map({TOKEN_ADDRESS: token_analysis})

    caller_analysis = {
        "access_control": {
            "privileged_functions": [
                {
                    "function": "execute(address,uint256)",
                    "effect_labels": [],
                    "effect_targets": [f"token.{fn}"],
                }
            ]
        },
    }

    controller_values = {"state_variable:token": {"value": TOKEN_ADDRESS}}
    enriched = enrich_cross_contract_effects(caller_analysis, controller_values, callee_map)
    new_labels = enriched.get("execute(address,uint256)", [])
    assert "mint" in new_labels
    assert "role_management" in new_labels


def test_no_enrichment_when_callee_not_analyzed():
    """If the callee contract isn't in the callee map, nothing happens."""
    caller_analysis = {
        "access_control": {
            "privileged_functions": [
                {
                    "function": "doStuff()",
                    "effect_labels": ["external_contract_call"],
                    "effect_targets": ["token.randomMint"],
                }
            ]
        },
    }

    controller_values = {"state_variable:token": {"value": TOKEN_ADDRESS}}
    enriched = enrich_cross_contract_effects(caller_analysis, controller_values, {})
    assert enriched == {}


def test_no_enrichment_when_address_unknown():
    """If the state variable's address isn't resolved, nothing happens."""
    callee_map = build_callee_effect_map(
        {
            TOKEN_ADDRESS: {
                "access_control": {
                    "privileged_functions": [
                        {"function": "mint(address,uint256)", "effect_labels": ["mint"], "effect_targets": []}
                    ]
                }
            }
        }
    )

    caller_analysis = {
        "access_control": {
            "privileged_functions": [
                {
                    "function": "doStuff()",
                    "effect_labels": ["external_contract_call"],
                    "effect_targets": ["token.mint"],
                }
            ]
        },
    }

    # No controller_values → can't resolve "token" to an address
    enriched = enrich_cross_contract_effects(caller_analysis, {}, callee_map)
    assert enriched == {}


def test_no_duplicate_labels():
    """If the caller already has the label, don't add it again."""
    callee_map = build_callee_effect_map(
        {
            TOKEN_ADDRESS: {
                "access_control": {
                    "privileged_functions": [
                        {"function": "transfer(address,uint256)", "effect_labels": ["asset_send"], "effect_targets": []}
                    ]
                }
            }
        }
    )

    caller_analysis = {
        "access_control": {
            "privileged_functions": [
                {
                    "function": "withdraw(address)",
                    "effect_labels": ["asset_send"],  # already has it
                    "effect_targets": ["token.transfer"],
                }
            ]
        },
    }

    controller_values = {"state_variable:token": {"value": TOKEN_ADDRESS}}
    enriched = enrich_cross_contract_effects(caller_analysis, controller_values, callee_map)
    assert enriched == {}  # nothing new added


def test_external_contract_controller_id_format():
    """Controller IDs can be 'external_contract:xxx' not just 'state_variable:xxx'."""
    fn = _rand()

    callee_map = build_callee_effect_map(
        {
            TOKEN_ADDRESS: {
                "access_control": {
                    "privileged_functions": [
                        {"function": f"{fn}(uint256)", "effect_labels": ["pause_toggle"], "effect_targets": []}
                    ]
                }
            }
        }
    )

    caller_analysis = {
        "access_control": {
            "privileged_functions": [
                {
                    "function": "manage()",
                    "effect_labels": [],
                    "effect_targets": [f"registry.{fn}"],
                }
            ]
        },
    }

    controller_values = {"external_contract:registry": {"value": TOKEN_ADDRESS}}
    enriched = enrich_cross_contract_effects(caller_analysis, controller_values, callee_map)
    assert "pause_toggle" in enriched.get("manage()", [])


def test_chain_propagation():
    """A calls B.foo(), B.foo() calls C.bar() — if we analyze in sequence,
    B's enriched labels should propagate to A."""
    fn_b = _rand()
    fn_c = _rand()
    addr_b = "0x3333333333333333333333333333333333333333"
    addr_c = "0x4444444444444444444444444444444444444444"

    # C's analysis: fn_c has "mint"
    c_analysis = {
        "access_control": {
            "privileged_functions": [
                {"function": f"{fn_c}(address,uint256)", "effect_labels": ["mint"], "effect_targets": []}
            ]
        },
    }

    # B's analysis: fn_b calls C's fn_c
    b_analysis = {
        "access_control": {
            "privileged_functions": [
                {
                    "function": f"{fn_b}(address,uint256)",
                    "effect_labels": ["external_contract_call"],
                    "effect_targets": [f"registry.{fn_c}"],
                }
            ]
        },
    }

    # First pass: enrich B with C's effects
    callee_map_1 = build_callee_effect_map({addr_c: c_analysis})
    b_controllers = {"state_variable:registry": {"value": addr_c}}
    enrich_cross_contract_effects(b_analysis, b_controllers, callee_map_1)

    # B should now have "mint"
    assert "mint" in b_analysis["access_control"]["privileged_functions"][0]["effect_labels"]

    # Second pass: enrich A with B's (now enriched) effects
    callee_map_2 = build_callee_effect_map({addr_b: b_analysis})

    a_analysis = {
        "access_control": {
            "privileged_functions": [
                {
                    "function": "execute(address,uint256)",
                    "effect_labels": [],
                    "effect_targets": [f"handler.{fn_b}"],
                }
            ]
        },
    }
    a_controllers = {"state_variable:handler": {"value": addr_b}}
    enriched = enrich_cross_contract_effects(a_analysis, a_controllers, callee_map_2)

    # A should also have "mint" now (propagated through B)
    assert "mint" in enriched.get("execute(address,uint256)", [])
