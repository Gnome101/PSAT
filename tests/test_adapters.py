"""Tests for the SetAdapter framework + AccessControl + Safe adapters."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution.adapters import (  # noqa: E402
    AdapterRegistry,
    EnumerationResult,
    EvaluationContext,
    Trit,
)
from services.resolution.adapters.access_control import (  # noqa: E402
    ROLE_GRANTED_TOPIC0,
    AccessControlAdapter,
)
from services.resolution.adapters.safe import SafeAdapter  # noqa: E402

ADDR_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDR_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
ADDR_C = "0xcccccccccccccccccccccccccccccccccccccccc"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeRoleGrantsRepo:
    def __init__(
        self,
        members_by_role: dict[bytes, list[str]] | None = None,
        role_admin_map: dict[bytes, bytes] | None = None,
    ):
        self.members_by_role = members_by_role or {}
        self.role_admin_map = role_admin_map or {}

    def members_for_role(self, *, chain_id, contract_address, role, block=None):
        return EnumerationResult(
            members=list(self.members_by_role.get(role, [])),
            confidence="enumerable",
            last_indexed_block=18_000_000,
        )

    def has_member(self, *, chain_id, contract_address, role, member):
        addrs = {a.lower() for a in self.members_by_role.get(role, [])}
        if member.lower() in addrs:
            return Trit.YES
        return Trit.NO

    def list_observed_roles(self, *, chain_id, contract_address):
        return list(self.members_by_role.keys())

    def get_role_admin(self, *, chain_id, contract_address, role, block=None):
        return self.role_admin_map.get(role)


class FakeSafeRepo:
    def __init__(self, owners: list[str], threshold: int):
        self.owners = owners
        self.threshold = threshold

    def get_owners_threshold(self, *, chain_id, contract_address, block=None):
        return (self.owners, self.threshold)


class FakeBytecodeRepo:
    def __init__(self, selectors: set[str]):
        self.selectors = {s.lower() for s in selectors}

    def has_selector(self, *, chain_id, contract_address, selector):
        return selector.lower() in self.selectors

    def declares_event(self, *, chain_id, contract_address, topic0):
        return False


# ---------------------------------------------------------------------------
# AccessControlAdapter — matches()
# ---------------------------------------------------------------------------


def _ac_2key_descriptor(role_constant: bytes | None = None, with_event_hint: bool = False) -> dict:
    keys = []
    if role_constant is not None:
        keys.append(
            {
                "source": "constant",
                "constant_value": "0x" + role_constant.hex(),
            }
        )
    else:
        keys.append({"source": "parameter", "parameter_index": 0, "parameter_name": "role"})
    keys.append({"source": "msg_sender"})
    descriptor: dict = {
        "kind": "mapping_membership",
        "key_sources": keys,
        "storage_var": "_members",
    }
    if with_event_hint:
        descriptor["enumeration_hint"] = [
            {
                "event_address": "0x0",
                "topic0": ROLE_GRANTED_TOPIC0,
                "topics_to_keys": {1: 0, 2: 1},
                "data_to_keys": {},
                "direction": "add",
            }
        ]
    return descriptor


def test_ac_matches_with_role_granted_event():
    descriptor = _ac_2key_descriptor(with_event_hint=True)
    ctx = EvaluationContext()
    score = AccessControlAdapter.matches(descriptor, ctx)
    assert score >= 90


def test_ac_matches_with_hasrole_selector():
    descriptor = _ac_2key_descriptor()
    ctx = EvaluationContext(
        contract_address=ADDR_A,
        bytecode=FakeBytecodeRepo({"0x91d14854"}),
    )
    score = AccessControlAdapter.matches(descriptor, ctx)
    assert score >= 50


def test_ac_does_not_match_1key_mapping():
    descriptor = {
        "kind": "mapping_membership",
        "key_sources": [{"source": "msg_sender"}],
        "storage_var": "claimed",
    }
    ctx = EvaluationContext()
    assert AccessControlAdapter.matches(descriptor, ctx) == 0


def test_ac_does_not_match_non_mapping():
    descriptor = {"kind": "external_set"}
    assert AccessControlAdapter.matches(descriptor, EvaluationContext()) == 0


# ---------------------------------------------------------------------------
# AccessControlAdapter — enumerate()
# ---------------------------------------------------------------------------


def test_ac_enumerate_concrete_role():
    role = b"\x00" * 32
    descriptor = _ac_2key_descriptor(role_constant=role, with_event_hint=True)
    repo = FakeRoleGrantsRepo({role: [ADDR_A, ADDR_B]})
    ctx = EvaluationContext(
        chain_id=1,
        contract_address=ADDR_C,
        role_grants=repo,
    )
    cap = AccessControlAdapter().enumerate(descriptor, ctx)
    assert cap.kind == "finite_set"
    assert cap.members == [ADDR_A.lower(), ADDR_B.lower()]
    assert cap.confidence == "enumerable"
    assert cap.membership_quality == "exact"


def test_ac_enumerate_parametric_role_falls_back_to_default_admin():
    descriptor = _ac_2key_descriptor(role_constant=None, with_event_hint=True)
    role_admin = b"\x00" * 32
    repo = FakeRoleGrantsRepo({role_admin: [ADDR_A]})
    ctx = EvaluationContext(
        chain_id=1,
        contract_address=ADDR_C,
        role_grants=repo,
    )
    cap = AccessControlAdapter().enumerate(descriptor, ctx)
    # parametric role: returns lower_bound finite_set with the
    # default-admin members as a placeholder. Week-6 role-domain
    # expansion will iterate through all known roles.
    assert cap.kind == "finite_set"
    assert cap.confidence == "partial"
    assert cap.membership_quality == "lower_bound"
    assert cap.members is not None
    assert ADDR_A.lower() in cap.members


def test_ac_enumerate_recursive_role_admin_expansion():
    """Per v3 round-3 #10: parametric role descriptors should
    expand the role domain via list_observed_roles + walking
    getRoleAdmin to fixed point. The capability covers the union
    of members across the full domain (lower_bound + partial
    because the runtime role argument selects one specific role
    from the domain — the union is over-permissive)."""
    minter_role = (1).to_bytes(32, "big")
    admin_role = (2).to_bytes(32, "big")
    default_admin = b"\x00" * 32
    repo = FakeRoleGrantsRepo(
        members_by_role={
            default_admin: [ADDR_A],
            admin_role: [ADDR_B],
            minter_role: [ADDR_C],
        },
        # MINTER's admin is admin_role; admin_role's admin is default_admin.
        role_admin_map={
            minter_role: admin_role,
            admin_role: default_admin,
        },
    )
    # Parametric descriptor: role key sources from a parameter.
    descriptor = _ac_2key_descriptor(role_constant=None, with_event_hint=True)
    # Test contract observed only the MINTER role; expansion must
    # walk to admin_role + default_admin via getRoleAdmin.
    repo.members_by_role.pop(default_admin, None)  # not directly observed
    repo.members_by_role.pop(admin_role, None)
    repo.members_by_role[default_admin] = [ADDR_A]
    repo.members_by_role[admin_role] = [ADDR_B]
    ctx = EvaluationContext(
        chain_id=1,
        contract_address=ADDR_C,
        role_grants=repo,
    )
    cap = AccessControlAdapter().enumerate(descriptor, ctx)
    assert cap.kind == "finite_set"
    assert cap.members is not None
    # All three roles' members included via the walked admin chain.
    member_set = set(cap.members)
    assert ADDR_A.lower() in member_set
    assert ADDR_B.lower() in member_set
    assert ADDR_C.lower() in member_set
    # Parametric → lower_bound + partial.
    assert cap.confidence == "partial"
    assert cap.membership_quality == "lower_bound"


def test_ac_enumerate_no_backend_yields_partial():
    descriptor = _ac_2key_descriptor(with_event_hint=True)
    ctx = EvaluationContext(contract_address=ADDR_A)
    cap = AccessControlAdapter().enumerate(descriptor, ctx)
    assert cap.kind == "finite_set"
    assert cap.confidence == "partial"


# ---------------------------------------------------------------------------
# SafeAdapter
# ---------------------------------------------------------------------------


def test_safe_matches_with_abi_hint():
    descriptor = {
        "kind": "external_set",
        "authority_contract": {"abi_hint": "gnosis_safe", "address_source": {"source": "constant"}},
    }
    score = SafeAdapter.matches(descriptor, EvaluationContext())
    assert score >= 90


def test_safe_matches_via_bytecode_selectors():
    descriptor = {"kind": "external_set"}
    ctx = EvaluationContext(
        contract_address=ADDR_A,
        bytecode=FakeBytecodeRepo({"0xa0e67e2b", "0xe75235b8"}),
    )
    score = SafeAdapter.matches(descriptor, ctx)
    assert score >= 80


def test_safe_enumerate_returns_threshold_group():
    descriptor = {"kind": "external_set", "authority_contract": {"abi_hint": "gnosis_safe"}}
    ctx = EvaluationContext(
        chain_id=1,
        contract_address=ADDR_A,
        safe_repo=FakeSafeRepo([ADDR_B, ADDR_C], threshold=2),
    )
    cap = SafeAdapter().enumerate(descriptor, ctx)
    assert cap.kind == "threshold_group"
    assert cap.threshold is not None
    m, signers = cap.threshold
    assert m == 2
    assert sorted(signers) == [ADDR_B.lower(), ADDR_C.lower()]


def test_safe_enumerate_no_backend_yields_unsupported():
    descriptor = {"kind": "external_set", "authority_contract": {"abi_hint": "gnosis_safe"}}
    ctx = EvaluationContext(contract_address=ADDR_A)
    cap = SafeAdapter().enumerate(descriptor, ctx)
    assert cap.kind == "unsupported"


# ---------------------------------------------------------------------------
# AdapterRegistry
# ---------------------------------------------------------------------------


def test_registry_picks_highest_scorer():
    registry = AdapterRegistry()
    registry.register(SafeAdapter)
    registry.register(AccessControlAdapter)
    descriptor = _ac_2key_descriptor(with_event_hint=True)
    picked = registry.pick(descriptor, EvaluationContext())
    assert picked is AccessControlAdapter


def test_registry_returns_unsupported_when_no_match():
    registry = AdapterRegistry()
    registry.register(SafeAdapter)
    registry.register(AccessControlAdapter)
    descriptor = {"kind": "mapping_membership", "key_sources": [{"source": "constant"}]}
    cap = registry.enumerate(descriptor, EvaluationContext())
    assert cap.kind == "unsupported"
    assert cap.unsupported_reason == "no_adapter"


def test_registry_dedup_register():
    registry = AdapterRegistry()
    registry.register(AccessControlAdapter)
    registry.register(AccessControlAdapter)
    assert len(registry.adapters) == 1


# ---------------------------------------------------------------------------
# Integration: evaluator dispatches through registry
# ---------------------------------------------------------------------------


def test_evaluator_with_registry_resolves_membership_via_adapter():
    """End-to-end plumbing test: a manually-crafted PredicateTree
    with an AC-shaped membership leaf, dispatched through the
    registry-backed evaluator, resolves to the fake repo's member
    set."""
    from services.resolution.predicate_evaluator import (
        evaluate_tree_with_registry,
    )

    role = b"\x00" * 32
    descriptor = _ac_2key_descriptor(role_constant=role, with_event_hint=True)
    leaf = {
        "kind": "membership",
        "operator": "truthy",
        "authority_role": "caller_authority",
        "operands": descriptor["key_sources"],
        "set_descriptor": descriptor,
        "references_msg_sender": True,
        "parameter_indices": [],
        "expression": "_members[role][msg.sender]",
        "basis": [],
    }
    tree = {"op": "LEAF", "leaf": leaf}

    repo = FakeRoleGrantsRepo({role: [ADDR_A, ADDR_B]})
    registry = AdapterRegistry()
    registry.register(AccessControlAdapter)
    ctx = EvaluationContext(
        chain_id=1,
        contract_address=ADDR_C,
        role_grants=repo,
    )
    cap = evaluate_tree_with_registry(tree, registry, ctx)  # type: ignore[arg-type]
    assert cap.kind == "finite_set"
    assert cap.members is not None
    assert sorted(cap.members) == [ADDR_A.lower(), ADDR_B.lower()]


def test_evaluator_with_registry_no_match_returns_unsupported():
    """When no adapter scores, the registry returns
    unsupported(no_adapter), which the evaluator passes through."""
    from services.resolution.predicate_evaluator import (
        evaluate_tree_with_registry,
    )

    descriptor = {
        "kind": "mapping_membership",
        "key_sources": [{"source": "msg_sender"}],  # 1-key, no AC fit
        "storage_var": "claimed",
    }
    leaf = {
        "kind": "membership",
        "operator": "truthy",
        "authority_role": "caller_authority",
        "operands": descriptor["key_sources"],
        "set_descriptor": descriptor,
        "references_msg_sender": True,
        "parameter_indices": [],
        "expression": "claimed[msg.sender]",
        "basis": [],
    }
    tree = {"op": "LEAF", "leaf": leaf}
    registry = AdapterRegistry()
    registry.register(AccessControlAdapter)
    registry.register(SafeAdapter)
    cap = evaluate_tree_with_registry(tree, registry, EvaluationContext())  # type: ignore[arg-type]
    assert cap.kind == "unsupported"
    assert cap.unsupported_reason == "no_adapter"
