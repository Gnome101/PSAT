"""Unit tests for predicate-tree-derived method -> role mapping."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static.contract_analysis_pipeline.summaries import _build_method_to_role_map  # noqa: E402

ROLE_GRANTED_TOPIC0 = "0x2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d"


def _oz_role_tree(role_source: dict) -> dict:
    return {
        "op": "LEAF",
        "leaf": {
            "kind": "membership",
            "operator": "truthy",
            "authority_role": "caller_authority",
            "operands": [role_source, {"source": "msg_sender"}],
            "set_descriptor": {
                "kind": "mapping_membership",
                "key_sources": [role_source, {"source": "msg_sender"}],
                "enumeration_hint": [
                    {
                        "topic0": ROLE_GRANTED_TOPIC0,
                        "topics_to_keys": {1: 0, 2: 1},
                        "data_to_keys": {},
                        "direction": "add",
                    }
                ],
            },
        },
    }


def test_method_to_role_map_uses_oz_role_key_descriptor():
    artifact = {
        "trees": {
            "onlyDepositWithdrawPauser(address)": _oz_role_tree(
                {"source": "state_variable", "state_variable_name": "DEPOSIT_WITHDRAW_PAUSER"}
            )
        }
    }

    assert _build_method_to_role_map(artifact) == {"onlyDepositWithdrawPauser": ["DEPOSIT_WITHDRAW_PAUSER"]}


def test_method_to_role_map_preserves_external_role_getter_key():
    artifact = {
        "trees": {"onlyBreakGlass(address)": _oz_role_tree({"source": "external_call", "callee": "BREAK_GLASS"})}
    }

    assert _build_method_to_role_map(artifact) == {"onlyBreakGlass": ["BREAK_GLASS"]}


def test_method_to_role_map_uses_bytes32_constant_in_authority_leaf():
    artifact = {
        "trees": {
            "gate(address)": {
                "op": "LEAF",
                "leaf": {
                    "kind": "external_bool",
                    "operator": "truthy",
                    "authority_role": "delegated_authority",
                    "operands": [
                        {"source": "state_variable", "state_variable_name": "BREAK_GLASS"},
                        {"source": "msg_sender"},
                    ],
                },
            }
        }
    }
    state_vars = {"BREAK_GLASS": SimpleNamespace(type="bytes32", is_constant=True)}

    assert _build_method_to_role_map(artifact, state_vars) == {"gate": ["BREAK_GLASS"]}


def test_method_to_role_map_ignores_name_without_semantic_role_shape():
    artifact = {
        "trees": {
            "notRole(address)": {
                "op": "LEAF",
                "leaf": {
                    "kind": "external_bool",
                    "operator": "truthy",
                    "authority_role": "business",
                    "operands": [{"source": "external_call", "callee": "ADMIN_ROLE"}],
                },
            }
        }
    }

    assert _build_method_to_role_map(artifact) == {}
