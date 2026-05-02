"""Tests for the v2 → v1 semantic_guards shim.

Synthetic v2 trees → expected v1 semantic_guards entries. The
mapping is structural and pure — no DB / Slither involvement.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static.contract_analysis_pipeline.semantic_guards import (  # noqa: E402
    synthesize_semantic_guards_from_predicate_trees,
)


def _trees(*entries):
    """Build a v2 predicate_trees dict from (function_signature, leaf_dict) tuples."""
    return {
        "schema_version": "v2",
        "trees": {fn: {"op": "LEAF", "leaf": leaf} for fn, leaf in entries},
    }


def _and_tree(fn: str, *leaves):
    return {
        "schema_version": "v2",
        "trees": {
            fn: {"op": "AND", "children": [{"op": "LEAF", "leaf": leaf} for leaf in leaves]}
        },
    }


# ---------------------------------------------------------------------------
# Per-leaf mappings
# ---------------------------------------------------------------------------


def _eq_state_var_leaf() -> dict:
    return {
        "kind": "equality",
        "operator": "eq",
        "authority_role": "caller_authority",
        "operands": [
            {"source": "msg_sender"},
            {"source": "state_variable", "state_variable_name": "owner"},
        ],
        "references_msg_sender": True,
        "parameter_indices": [],
        "expression": "msg.sender == owner",
        "basis": [],
    }


def _membership_leaf() -> dict:
    return {
        "kind": "membership",
        "operator": "truthy",
        "authority_role": "caller_authority",
        "operands": [{"source": "msg_sender"}],
        "set_descriptor": {
            "kind": "mapping_membership",
            "key_sources": [
                {
                    "source": "constant",
                    "constant_value": "0x" + "01" * 32,
                },
                {"source": "msg_sender"},
            ],
            "storage_var": "_roles",
        },
        "references_msg_sender": True,
        "parameter_indices": [],
        "expression": "_roles[ROLE][msg.sender]",
        "basis": [],
    }


def _signature_auth_leaf() -> dict:
    return {
        "kind": "signature_auth",
        "operator": "eq",
        "authority_role": "caller_authority",
        "operands": [
            {"source": "signature_recovery"},
            {"source": "state_variable", "state_variable_name": "trustedSigner"},
        ],
        "references_msg_sender": False,
        "parameter_indices": [],
        "expression": "ecrecover(...) == trustedSigner",
        "basis": [],
    }


def _external_bool_leaf() -> dict:
    return {
        "kind": "external_bool",
        "operator": "truthy",
        "authority_role": "delegated_authority",
        "operands": [{"source": "external_call", "callee": "canCall"}],
        "references_msg_sender": False,
        "parameter_indices": [],
        "expression": "authority.canCall(msg.sender, this, sig)",
        "basis": [],
    }


def _unsupported_leaf() -> dict:
    return {
        "kind": "unsupported",
        "operator": "truthy",
        "authority_role": "business",
        "operands": [],
        "unsupported_reason": "opaque_try_catch",
        "references_msg_sender": False,
        "parameter_indices": [],
        "expression": "h.helper()",
        "basis": [],
    }


def _reentrancy_leaf() -> dict:
    return {
        "kind": "membership",
        "operator": "falsy",
        "authority_role": "reentrancy",
        "operands": [{"source": "state_variable", "state_variable_name": "_status"}],
        "references_msg_sender": False,
        "parameter_indices": [],
        "expression": "_status != _ENTERED",
        "basis": [],
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_eq_state_var_maps_to_caller_equals_controller():
    out = synthesize_semantic_guards_from_predicate_trees(
        _trees(("f()", _eq_state_var_leaf())),
        contract_address="0x1",
        contract_name="C",
    )
    assert out["schema_version"] == "0.1"
    assert out["_synthetic_from"] == "v2_predicate_trees"
    assert len(out["functions"]) == 1
    fn = out["functions"][0]
    assert fn["function"] == "f()"
    assert fn["status"] == "resolved"
    assert len(fn["predicates"]) == 1
    p = fn["predicates"][0]
    assert p["kind"] == "caller_equals_controller"
    assert p["controller_kind"] == "state_variable"
    assert p["controller_label"] == "owner"
    assert p["controller_source"] == "owner"
    assert "access_control" in fn["guard_kinds"]


def test_membership_maps_to_mapping_membership():
    out = synthesize_semantic_guards_from_predicate_trees(
        _trees(("f()", _membership_leaf())),
        contract_address="0x1",
        contract_name="C",
    )
    p = out["functions"][0]["predicates"][0]
    assert p["kind"] == "mapping_membership"
    # v1's mapping_membership emit uses the mapping NAME as
    # controller_source, NOT the inner role-key constant —
    # downstream effective_permissions's controller_lookup is
    # keyed on the mapping name. The shim matches that contract
    # via set_descriptor.storage_var (here: '_roles'). Earlier
    # versions picked the non-caller key (the role constant) which
    # caused effective_permissions to resolve different controllers
    # under the v2 path; pinned by test_effective_permissions_v1_v2_-
    # equivalence.
    assert p["controller_kind"] == "mapping_membership"
    assert p["controller_label"] == "_roles"
    assert p["controller_source"] == "_roles"


def test_caller_only_1key_membership_falls_back_to_storage_var():
    """For 1-key caller-only mappings (Maker wards, OZ Pausable's
    _paused, blacklists keyed only on msg.sender), all key_sources
    are caller-related — the previous shim picked None as the
    controller_op and emitted controller_source=null. v1's native
    emit uses the mapping's OWN NAME as controller_source so
    downstream effective_permissions can resolve via
    controller_lookup. This regression test pins the storage_var
    fallback.
    """
    leaf = {
        "kind": "membership",
        "operator": "truthy",
        "authority_role": "caller_authority",
        "operands": [{"source": "msg_sender"}],
        "set_descriptor": {
            "kind": "mapping_membership",
            "key_sources": [{"source": "msg_sender"}],
            "truthy_value": "1",
            "storage_var": "wards",
        },
        "references_msg_sender": True,
        "parameter_indices": [],
        "expression": "wards[msg.sender] == 1",
        "basis": [],
    }
    out = synthesize_semantic_guards_from_predicate_trees(
        _trees(("rely(address)", leaf)),
        contract_address="0x1",
        contract_name="C",
    )
    p = out["functions"][0]["predicates"][0]
    assert p["kind"] == "mapping_membership"
    assert p["controller_source"] == "wards"
    assert p["controller_label"] == "wards"
    assert p["controller_kind"] == "mapping_membership"


def test_signature_auth_maps_to_policy_check():
    out = synthesize_semantic_guards_from_predicate_trees(
        _trees(("f()", _signature_auth_leaf())),
        contract_address="0x1",
        contract_name="C",
    )
    p = out["functions"][0]["predicates"][0]
    assert p["kind"] == "policy_check"
    assert "policy" in out["functions"][0]["guard_kinds"]


def test_external_bool_maps_to_external_helper():
    out = synthesize_semantic_guards_from_predicate_trees(
        _trees(("f()", _external_bool_leaf())),
        contract_address="0x1",
        contract_name="C",
    )
    fn = out["functions"][0]
    p = fn["predicates"][0]
    assert p["kind"] == "external_helper"
    assert p["status"] == "unresolved"
    assert "external_helper" in fn["guard_kinds"]


def test_unsupported_leaf_marks_function_unresolved():
    out = synthesize_semantic_guards_from_predicate_trees(
        _trees(("f()", _unsupported_leaf())),
        contract_address="0x1",
        contract_name="C",
    )
    fn = out["functions"][0]
    assert fn["status"] == "unresolved"
    assert fn["predicates"] == []
    assert any("opaque_try_catch" in n for n in fn["notes"])


def test_reentrancy_leaf_dropped_status_unresolved():
    """Pure-reentrancy gate (no auth) — v1 didn't include
    reentrancy in semantic_guards.predicates. Status reflects
    that auth still wasn't found."""
    out = synthesize_semantic_guards_from_predicate_trees(
        _trees(("f()", _reentrancy_leaf())),
        contract_address="0x1",
        contract_name="C",
    )
    fn = out["functions"][0]
    assert fn["predicates"] == []
    assert fn["status"] == "unresolved"
    assert any("side-condition" in n for n in fn["notes"])


def test_partial_status_when_some_leaves_unsupported():
    """AND of an auth leaf + unsupported leaf — status partial."""
    out = synthesize_semantic_guards_from_predicate_trees(
        _and_tree("f()", _eq_state_var_leaf(), _unsupported_leaf()),
        contract_address="0x1",
        contract_name="C",
    )
    fn = out["functions"][0]
    assert fn["status"] == "partial"
    assert len(fn["predicates"]) == 1
    assert fn["predicates"][0]["kind"] == "caller_equals_controller"


def test_empty_trees_dict_produces_empty_functions_list():
    """Contract with only unguarded functions -> trees={} -> no
    functions in semantic_guards (consumers know "absent =
    public" or read the v1 access_control field directly)."""
    out = synthesize_semantic_guards_from_predicate_trees(
        {"schema_version": "v2", "trees": {}},
        contract_address="0x1",
        contract_name="C",
    )
    assert out["functions"] == []


def test_function_signatures_sorted():
    """Output ordering is deterministic — sorted by function
    signature so consumers can diff stably."""
    out = synthesize_semantic_guards_from_predicate_trees(
        _trees(
            ("zeta()", _eq_state_var_leaf()),
            ("alpha()", _eq_state_var_leaf()),
        ),
        contract_address="0x1",
        contract_name="C",
    )
    fn_names = [f["function"] for f in out["functions"]]
    assert fn_names == ["alpha()", "zeta()"]


def test_contract_address_and_name_echoed_into_envelope():
    out = synthesize_semantic_guards_from_predicate_trees(
        {"schema_version": "v2", "trees": {}},
        contract_address="0xabc",
        contract_name="MyContract",
    )
    assert out["contract_address"] == "0xabc"
    assert out["contract_name"] == "MyContract"


def test_business_role_dropped_status_unresolved():
    leaf = {
        "kind": "equality",
        "operator": "eq",
        "authority_role": "business",
        "operands": [{"source": "parameter"}, {"source": "constant"}],
        "references_msg_sender": False,
        "parameter_indices": [0],
        "expression": "amount > 0",
        "basis": [],
    }
    out = synthesize_semantic_guards_from_predicate_trees(
        _trees(("f()", leaf)),
        contract_address="0x1",
        contract_name="C",
    )
    fn = out["functions"][0]
    assert fn["predicates"] == []
    assert fn["status"] == "unresolved"
