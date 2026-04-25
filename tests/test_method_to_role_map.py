"""Unit tests for the access-control method->role map extractor.

`_build_method_to_role_map` walks an access-control authority
contract's external entrypoints and records which role constant each
method's body checks. The cross-contract policy-stage join consumes
this map to resolve external-call guards like
`roleManager.onlyDepositWithdrawPauser(msg.sender)` to a concrete role
without keyword heuristics.

Slither objects are mocked via SimpleNamespace — the extractor reads
`.functions`, `.visibility`, `.is_constructor`, `.name`, and the
standard call-walker attrs (`.all_internal_calls`, `.nodes[...].irs`).
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static.contract_analysis_pipeline.summaries import (  # noqa: E402
    _build_method_to_role_map,
    _function_calls_role_check,
)

# ---------------------------------------------------------------------------
# Slither IR mocks
# ---------------------------------------------------------------------------


class _FakeInternalCall:
    pass


_FakeInternalCall.__name__ = "InternalCall"


class _FakeHighLevelCall:
    def __init__(self, callee_name: str):
        self.function_name = SimpleNamespace(name=callee_name)
        self.arguments: list = []
        self.destination = None


_FakeHighLevelCall.__name__ = "HighLevelCall"


def _internal_call(callee_name: str):
    return SimpleNamespace(function=SimpleNamespace(name=callee_name))


def _hasrole_ir(callee_name: str = "hasRole"):
    """Build a HighLevelCall IR whose function_name resolves to the
    role-checker we want to detect."""
    return _FakeHighLevelCall(callee_name)


def _node(irs: list) -> SimpleNamespace:
    return SimpleNamespace(irs=irs, node_id=0)


def _role_constant(name: str) -> SimpleNamespace:
    """A role-like state variable that `_role_constants_from_function`
    will pick up via `all_state_variables_read`."""
    return SimpleNamespace(name=name)


def _function(
    *,
    name: str,
    visibility: str = "external",
    is_constructor: bool = False,
    internal_calls: list | None = None,
    state_vars_read: list | None = None,
    ir_nodes: list | None = None,
) -> SimpleNamespace:
    """A fake slither Function object."""
    return SimpleNamespace(
        name=name,
        full_name=f"{name}(address)",
        visibility=visibility,
        is_constructor=is_constructor,
        all_internal_calls=lambda: internal_calls or [],
        all_state_variables_read=lambda: state_vars_read or [],
        modifiers=[],
        nodes=ir_nodes or [],
    )


def _contract(functions: list) -> SimpleNamespace:
    """A fake Contract — `_contract_functions` prefers `.functions` if
    present, and we set `.name` so evidence walkers don't crash."""
    return SimpleNamespace(
        name="RoleManager",
        functions=functions,
        inheritance=[],
    )


# ---------------------------------------------------------------------------
# _function_calls_role_check
# ---------------------------------------------------------------------------


def test_function_calls_role_check_detects_internal_call():
    """Contracts that inherit OZ AccessControl see `hasRole` as an
    InternalCall, not a HighLevelCall — must catch both."""
    fn = _function(
        name="onlyDepositWithdrawPauser",
        internal_calls=[_internal_call("hasRole")],
    )
    assert _function_calls_role_check(fn) is True


def test_function_calls_role_check_detects_high_level_call():
    """Contracts delegating to a separate access-control helper see
    `hasRole` as a HighLevelCall in the IR."""
    fn = _function(
        name="onlyFoo",
        ir_nodes=[_node([_hasrole_ir()])],
    )
    assert _function_calls_role_check(fn) is True


def test_function_calls_role_check_accepts_underscore_checkRole_alias():
    """OZ AccessControl v5 uses `_checkRole` as the internal helper."""
    fn = _function(
        name="onlyAdmin",
        internal_calls=[_internal_call("_checkRole")],
    )
    assert _function_calls_role_check(fn) is True


def test_function_calls_role_check_rejects_unrelated_calls():
    """Regular admin functions that call `emit`/`_authorizeUpgrade`/etc.
    must not register — otherwise every `onlyOwner upgrade` would end
    up in the map."""
    fn = _function(
        name="upgradeTo",
        internal_calls=[_internal_call("_authorizeUpgrade")],
        ir_nodes=[_node([_hasrole_ir(callee_name="transfer")])],
    )
    assert _function_calls_role_check(fn) is False


# ---------------------------------------------------------------------------
# _build_method_to_role_map
# ---------------------------------------------------------------------------


def _run_map(contract) -> dict:
    """Wrapper that stubs out `_role_constants_from_function` so the
    extractor's output depends only on what we set on the fake
    functions, not on filesystem source walks."""

    def fake_role_constants(fn, _project_dir):
        # Surface whatever role-looking state vars the test attached.
        return [v.name for v in (fn.all_state_variables_read() or [])]

    with patch(
        "services.static.contract_analysis_pipeline.summaries._role_constants_from_function",
        side_effect=fake_role_constants,
    ):
        return _build_method_to_role_map(contract, Path("/tmp/irrelevant"))


def test_extractor_captures_method_to_single_role():
    """Happy path: `onlyX(address)` with an internal `hasRole(ROLE_X, _)`
    call and a ROLE_X state var read — records `onlyX -> [ROLE_X]`."""
    fn = _function(
        name="onlyDepositWithdrawPauser",
        internal_calls=[_internal_call("hasRole")],
        state_vars_read=[_role_constant("DEPOSIT_WITHDRAW_PAUSER")],
    )
    result = _run_map(_contract([fn]))
    assert result == {"onlyDepositWithdrawPauser": ["DEPOSIT_WITHDRAW_PAUSER"]}


def test_extractor_captures_multiple_roles_per_method():
    """A method can check more than one role. The map preserves the set."""
    fn = _function(
        name="onlyAdminOrPauser",
        internal_calls=[_internal_call("hasRole")],
        state_vars_read=[_role_constant("ADMIN_ROLE"), _role_constant("PAUSER_ROLE")],
    )
    result = _run_map(_contract([fn]))
    assert result == {"onlyAdminOrPauser": ["ADMIN_ROLE", "PAUSER_ROLE"]}


def test_extractor_skips_non_public_methods():
    """Internal/private helpers are never called by other contracts,
    so they can't be the target of an external-call guard — skip."""
    fn = _function(
        name="_internalHelper",
        visibility="internal",
        internal_calls=[_internal_call("hasRole")],
        state_vars_read=[_role_constant("ADMIN_ROLE")],
    )
    result = _run_map(_contract([fn]))
    assert result == {}


def test_extractor_skips_constructor():
    fn = _function(
        name="constructor",
        is_constructor=True,
        internal_calls=[_internal_call("hasRole")],
        state_vars_read=[_role_constant("DEFAULT_ADMIN_ROLE")],
    )
    result = _run_map(_contract([fn]))
    assert result == {}


def test_extractor_skips_methods_without_role_check():
    """A method that reads a role constant but never calls hasRole is
    typically a *getter* on the role constant (e.g. `ADMIN_ROLE()`
    auto-getter). Not a role checker — skip."""
    fn = _function(
        name="adminRoleGetter",
        state_vars_read=[_role_constant("ADMIN_ROLE")],
    )
    result = _run_map(_contract([fn]))
    assert result == {}


def test_extractor_skips_methods_with_role_check_but_no_role_constants():
    """`_checkRole(role, account)` with a parameter-typed role — role
    constant not resolvable statically. Skip rather than emit a broken
    `method -> []` entry."""
    fn = _function(
        name="checkRoleGeneric",
        internal_calls=[_internal_call("hasRole")],
        state_vars_read=[],
    )
    result = _run_map(_contract([fn]))
    assert result == {}


def test_extractor_merges_across_multiple_matching_methods():
    """Different methods produce independent entries."""
    only_pauser = _function(
        name="onlyPauser",
        internal_calls=[_internal_call("hasRole")],
        state_vars_read=[_role_constant("PAUSER_ROLE")],
    )
    only_upgrader = _function(
        name="onlyUpgrader",
        internal_calls=[_internal_call("hasRole")],
        state_vars_read=[_role_constant("UPGRADER_ROLE")],
    )
    result = _run_map(_contract([only_pauser, only_upgrader]))
    assert result == {
        "onlyPauser": ["PAUSER_ROLE"],
        "onlyUpgrader": ["UPGRADER_ROLE"],
    }


def test_extractor_deduplicates_role_constants():
    fn = _function(
        name="onlyAdmin",
        internal_calls=[_internal_call("hasRole")],
        state_vars_read=[_role_constant("ADMIN_ROLE"), _role_constant("ADMIN_ROLE")],
    )
    result = _run_map(_contract([fn]))
    assert result == {"onlyAdmin": ["ADMIN_ROLE"]}


def test_extractor_returns_empty_on_ownable_contract():
    """A plain Ownable contract (no hasRole anywhere) produces no map."""
    fn = _function(
        name="transferOwnership",
        internal_calls=[_internal_call("_transferOwnership")],
        state_vars_read=[_role_constant("_owner")],
    )
    result = _run_map(_contract([fn]))
    assert result == {}
