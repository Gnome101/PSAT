"""Unit tests for the cross-contract external-call-guard bridge in
`workers.policy_worker`.

The bridge is the policy-stage join that turns a guard like
`roleManager.onlyDepositWithdrawPauser(msg.sender)` into
FunctionPrincipal rows pointing at the actual on-chain addresses that
hold `DEPOSIT_WITHDRAW_PAUSER`. It consumes three inputs:

1. `external_call_guards` on the static-stage privileged-function record
2. `control_snapshot.controller_values` from the resolver for state-var
   address lookups
3. `control_graph_nodes` carrying `details.method_to_role` on authority
   nodes and `details.controller_label` on principal nodes

These tests mock the DB session and graph so the bridge can be exercised
without a live pipeline.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from workers.policy_worker import (  # noqa: E402
    _apply_external_call_guard_bridge,
    _method_to_role_for_address,
    _principals_for_role_from_graph,
    _resolve_target_state_var_address,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


ROLE_MANAGER = "0x4994efc62101a9e3f885d872514c2dc7b3235849"
PAUSER_EOA = "0x19d74871a530c97065d95278223e8b7a7cd5ba27"
PAUSER_TIMELOCK = "0x81f6e9914136da1a1d3b1efd14f7e0761c3d4cc7"
ADMIN_EOA = "0x3b8c27038848592a51384334d8090dd869a816cb"


def _graph_nodes() -> list[dict]:
    """A realistic mini graph with one authority node carrying
    `method_to_role`, plus three principal nodes tagged with roles."""
    return [
        {
            "address": ROLE_MANAGER,
            "resolved_type": "contract",
            "details": {
                "authority_kind": "access_control_like",
                "method_to_role": {
                    "onlyDepositWithdrawPauser": ["DEPOSIT_WITHDRAW_PAUSER"],
                    "onlyProtocolUpgrader": ["PROTOCOL_UPGRADER_ROLE"],
                },
            },
        },
        {
            "address": PAUSER_EOA,
            "resolved_type": "eoa",
            "details": {"controller_label": "DEPOSIT_WITHDRAW_PAUSER"},
        },
        {
            "address": PAUSER_TIMELOCK,
            "resolved_type": "timelock",
            "details": {"controller_label": "DEPOSIT_WITHDRAW_PAUSER", "delay": 604800},
        },
        {
            "address": ADMIN_EOA,
            "resolved_type": "eoa",
            "details": {"controller_label": "PROTOCOL_UPGRADER_ROLE"},
        },
    ]


def _snapshot() -> dict:
    return {
        "controller_values": {
            "external_contract:roleManager": {
                "value": ROLE_MANAGER,
                "resolved_type": "contract",
            },
            "state_variable:admin": {
                "value": "",
            },
        }
    }


def _function_record(guards: list[dict]) -> dict:
    return {"function": "pause()", "external_call_guards": guards}


# ---------------------------------------------------------------------------
# _resolve_target_state_var_address
# ---------------------------------------------------------------------------


def test_resolve_state_var_matches_external_contract_key():
    """`external_contract:roleManager` should match target_state_var
    "roleManager" regardless of the controller_id prefix."""
    assert _resolve_target_state_var_address("roleManager", _snapshot()) == ROLE_MANAGER


def test_resolve_state_var_returns_none_when_address_empty():
    """The key is present but holds no address — can't resolve."""
    assert _resolve_target_state_var_address("admin", _snapshot()) is None


def test_resolve_state_var_returns_none_when_snapshot_missing():
    assert _resolve_target_state_var_address("roleManager", None) is None


def test_resolve_state_var_returns_none_when_key_missing():
    assert _resolve_target_state_var_address("gate", _snapshot()) is None


def test_resolve_state_var_requires_valid_address_shape():
    """Reject malformed/short addresses rather than propagating noise."""
    bad_snapshot = {
        "controller_values": {
            "state_variable:gate": {"value": "0x1234"},
        }
    }
    assert _resolve_target_state_var_address("gate", bad_snapshot) is None


# ---------------------------------------------------------------------------
# _method_to_role_for_address
# ---------------------------------------------------------------------------


def test_method_to_role_lookup_happy_path():
    result = _method_to_role_for_address(ROLE_MANAGER, _graph_nodes())
    assert result == {
        "onlyDepositWithdrawPauser": ["DEPOSIT_WITHDRAW_PAUSER"],
        "onlyProtocolUpgrader": ["PROTOCOL_UPGRADER_ROLE"],
    }


def test_method_to_role_case_insensitive_match():
    """Address matching must be case-insensitive — the authority's
    address might come in mixed case from the snapshot."""
    nodes = _graph_nodes()
    result = _method_to_role_for_address(ROLE_MANAGER.upper(), nodes)
    assert "onlyDepositWithdrawPauser" in result


def test_method_to_role_returns_empty_when_node_missing():
    assert _method_to_role_for_address("0x" + "0" * 40, _graph_nodes()) == {}


def test_method_to_role_returns_empty_when_details_lacks_map():
    nodes = [{"address": ROLE_MANAGER, "details": {}}]
    assert _method_to_role_for_address(ROLE_MANAGER, nodes) == {}


# ---------------------------------------------------------------------------
# _principals_for_role_from_graph
# ---------------------------------------------------------------------------


def test_principals_for_role_collects_all_nodes_with_matching_label():
    """Both the EOA and the Timelock hold DEPOSIT_WITHDRAW_PAUSER — both
    should come back."""
    result = _principals_for_role_from_graph("DEPOSIT_WITHDRAW_PAUSER", _graph_nodes())
    addresses = {p["address"] for p in result}
    assert addresses == {PAUSER_EOA, PAUSER_TIMELOCK}


def test_principals_for_role_returns_empty_when_no_matches():
    assert _principals_for_role_from_graph("NONEXISTENT_ROLE", _graph_nodes()) == []


def test_principals_for_role_preserves_resolved_type():
    """The caller needs `resolved_type` to stamp the FunctionPrincipal
    row — make sure we carry it through."""
    result = _principals_for_role_from_graph("DEPOSIT_WITHDRAW_PAUSER", _graph_nodes())
    types = {p["resolved_type"] for p in result}
    assert types == {"eoa", "timelock"}


# ---------------------------------------------------------------------------
# _apply_external_call_guard_bridge (end-to-end)
# ---------------------------------------------------------------------------


def test_bridge_adds_one_principal_per_role_holder():
    """Happy path: one guard on `roleManager.onlyDepositWithdrawPauser`
    → two holders (EOA + Timelock) → two FunctionPrincipal rows added."""
    session = MagicMock()
    ef = SimpleNamespace(id=42)
    fn = _function_record(
        [
            {
                "kind": "modifier",
                "target_state_var": "roleManager",
                "target_type": "IRoleManager",
                "method": "onlyDepositWithdrawPauser",
                "sender_in_args": True,
            }
        ]
    )
    added = _apply_external_call_guard_bridge(
        session,
        effective_function=ef,
        function_record=fn,
        control_snapshot=_snapshot(),
        control_graph_nodes=_graph_nodes(),
    )
    assert added == 2
    # Two session.add calls — one per principal
    assert session.add.call_count == 2
    rows = [call.args[0] for call in session.add.call_args_list]
    addresses = {row.address for row in rows}
    assert addresses == {PAUSER_EOA, PAUSER_TIMELOCK}
    # Every row must carry the origin + role context so the UI can trace it.
    for row in rows:
        assert row.function_id == 42
        assert row.origin == "roleManager.onlyDepositWithdrawPauser"
        assert row.principal_type == "external_call_guard"
        assert row.details["role"] == "DEPOSIT_WITHDRAW_PAUSER"
        assert row.details["authority_address"] == ROLE_MANAGER
        assert row.details["guard_method"] == "onlyDepositWithdrawPauser"


def test_bridge_deduplicates_same_principal_across_guards():
    """Two modifiers that both resolve to the same principal must emit
    one row, not two — otherwise the UI double-counts."""
    session = MagicMock()
    ef = SimpleNamespace(id=1)
    fn = _function_record(
        [
            {
                "kind": "modifier",
                "target_state_var": "roleManager",
                "target_type": "IRoleManager",
                "method": "onlyDepositWithdrawPauser",
                "sender_in_args": True,
                "modifier_name": "onlyPauserA",
            },
            {
                "kind": "modifier",
                "target_state_var": "roleManager",
                "target_type": "IRoleManager",
                "method": "onlyDepositWithdrawPauser",
                "sender_in_args": True,
                "modifier_name": "onlyPauserB",
            },
        ]
    )
    added = _apply_external_call_guard_bridge(
        session,
        effective_function=ef,
        function_record=fn,
        control_snapshot=_snapshot(),
        control_graph_nodes=_graph_nodes(),
    )
    # Two principals, not four — same (address, role, method) triple
    # on both guards collapses.
    assert added == 2


def test_bridge_adds_nothing_when_state_var_unresolved():
    """If `gate` doesn't appear in controller_values, don't invent a
    principal. The guard is flagged as unresolved at the UI level."""
    session = MagicMock()
    ef = SimpleNamespace(id=1)
    fn = _function_record(
        [
            {
                "target_state_var": "gate",
                "method": "check",
                "kind": "inline",
                "sender_in_args": True,
            }
        ]
    )
    added = _apply_external_call_guard_bridge(
        session,
        effective_function=ef,
        function_record=fn,
        control_snapshot=_snapshot(),
        control_graph_nodes=_graph_nodes(),
    )
    assert added == 0
    session.add.assert_not_called()


def test_bridge_adds_nothing_when_authority_lacks_method_to_role():
    """Resolver classifies the authority as access_control_like but
    couldn't extract a method_to_role map (unverified source, etc.).
    Guard stays unresolved."""
    session = MagicMock()
    ef = SimpleNamespace(id=1)
    fn = _function_record(
        [
            {
                "target_state_var": "roleManager",
                "method": "onlyDepositWithdrawPauser",
                "kind": "modifier",
                "sender_in_args": True,
            }
        ]
    )
    nodes = [{"address": ROLE_MANAGER, "details": {}}]  # no method_to_role
    added = _apply_external_call_guard_bridge(
        session,
        effective_function=ef,
        function_record=fn,
        control_snapshot=_snapshot(),
        control_graph_nodes=nodes,
    )
    assert added == 0


def test_bridge_adds_nothing_when_method_not_in_map():
    """Guard method name doesn't appear in the authority's map —
    e.g. the authority renamed its role checker since the target
    was last compiled."""
    session = MagicMock()
    ef = SimpleNamespace(id=1)
    fn = _function_record(
        [
            {
                "target_state_var": "roleManager",
                "method": "onlyUnknownRole",
                "kind": "modifier",
                "sender_in_args": True,
            }
        ]
    )
    added = _apply_external_call_guard_bridge(
        session,
        effective_function=ef,
        function_record=fn,
        control_snapshot=_snapshot(),
        control_graph_nodes=_graph_nodes(),
    )
    assert added == 0


def test_bridge_skips_guards_with_empty_method_or_target():
    session = MagicMock()
    ef = SimpleNamespace(id=1)
    fn = _function_record(
        [
            {"target_state_var": "", "method": "check", "kind": "modifier"},
            {"target_state_var": "roleManager", "method": "", "kind": "modifier"},
        ]
    )
    added = _apply_external_call_guard_bridge(
        session,
        effective_function=ef,
        function_record=fn,
        control_snapshot=_snapshot(),
        control_graph_nodes=_graph_nodes(),
    )
    assert added == 0


def test_bridge_uses_role_args_pattern_b_before_method_to_role():
    """Pattern B (ether.fi LP): guard has `role_args=["PROTOCOL_PAUSER"]`
    directly from the caller AST. The bridge must use those and skip the
    method_to_role lookup (which, for the generic `hasRole` method, will
    be empty anyway because `hasRole(bytes32,address)` has no role
    constant in its own body)."""
    session = MagicMock()
    ef = SimpleNamespace(id=7)
    graph = [
        {
            "address": ROLE_MANAGER,
            "resolved_type": "contract",
            # No method_to_role for `hasRole` — that's the whole point:
            # the generic hasRole can't carry a role in its signature,
            # so the caller's role_args is the authoritative signal.
            "details": {"authority_kind": "access_control_like"},
        },
        {
            "address": PAUSER_EOA,
            "resolved_type": "eoa",
            "details": {"controller_label": "PROTOCOL_PAUSER"},
        },
    ]
    fn = _function_record(
        [
            {
                "kind": "inline",
                "target_state_var": "roleManager",
                "target_type": "IRoleRegistry",
                "method": "hasRole",
                "sender_in_args": True,
                "role_args": ["PROTOCOL_PAUSER"],
            }
        ]
    )
    added = _apply_external_call_guard_bridge(
        session,
        effective_function=ef,
        function_record=fn,
        control_snapshot=_snapshot(),
        control_graph_nodes=graph,
    )
    assert added == 1
    row = session.add.call_args.args[0]
    assert row.address == PAUSER_EOA
    assert row.details["role"] == "PROTOCOL_PAUSER"
    assert row.details["guard_pattern"] == "role_args"


def test_bridge_falls_back_to_method_to_role_when_no_role_args():
    """Pattern A (Renzo): no role_args, method name encodes the role.
    The bridge falls back to the authority's method_to_role map."""
    session = MagicMock()
    ef = SimpleNamespace(id=9)
    fn = _function_record(
        [
            {
                "kind": "modifier",
                "target_state_var": "roleManager",
                "target_type": "IRoleManager",
                "method": "onlyDepositWithdrawPauser",
                "sender_in_args": True,
                # role_args intentionally absent
            }
        ]
    )
    added = _apply_external_call_guard_bridge(
        session,
        effective_function=ef,
        function_record=fn,
        control_snapshot=_snapshot(),
        control_graph_nodes=_graph_nodes(),
    )
    assert added == 2
    rows = [c.args[0] for c in session.add.call_args_list]
    patterns = {row.details["guard_pattern"] for row in rows}
    assert patterns == {"method_to_role"}


def test_bridge_with_no_guards_is_a_noop():
    session = MagicMock()
    ef = SimpleNamespace(id=1)
    fn = {"function": "pause()"}  # no external_call_guards key at all
    added = _apply_external_call_guard_bridge(
        session,
        effective_function=ef,
        function_record=fn,
        control_snapshot=_snapshot(),
        control_graph_nodes=_graph_nodes(),
    )
    assert added == 0
    session.add.assert_not_called()
