"""Tests for the Aragon ACL + DSAuth + EIP-1271 adapters."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution.adapters import (  # noqa: E402
    AdapterRegistry,
    EnumerationResult,
    EvaluationContext,
)
from services.resolution.adapters.aragon_acl import (  # noqa: E402
    ARAGON_CAN_PERFORM_SELECTOR,
    ARAGON_SET_PERMISSION_TOPIC0,
    DS_AUTH_CAN_CALL_SELECTOR,
    AragonACLAdapter,
    DSAuthAdapter,
    EIP1271Adapter,
)

ADDR_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDR_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
ADDR_C = "0xcccccccccccccccccccccccccccccccccccccccc"


class FakeAragonRepo:
    def __init__(self, members: list[str]):
        self.members = members

    def members_for_permission(self, *, chain_id, acl_address, target_app, role, block=None):
        return EnumerationResult(
            members=list(self.members),
            confidence="enumerable",
            last_indexed_block=18_000_000,
        )


class FakeDSAuthRepo:
    def __init__(self, members: list[str]):
        self.members = members

    def members_for_callable(self, *, chain_id, authority_address, target_contract, selector, block=None):
        return EnumerationResult(
            members=list(self.members),
            confidence="enumerable",
            last_indexed_block=18_000_000,
        )


class FakeBytecodeRepo:
    def __init__(self, selectors: set[str]):
        self.selectors = {s.lower() for s in selectors}

    def has_selector(self, *, chain_id, contract_address, selector):
        return selector.lower() in self.selectors

    def declares_event(self, *, chain_id, contract_address, topic0):
        return False


# ---------------------------------------------------------------------------
# Aragon ACL
# ---------------------------------------------------------------------------


def test_aragon_matches_via_abi_hint():
    descriptor = {
        "kind": "external_set",
        "authority_contract": {"abi_hint": "aragon_acl"},
    }
    score = AragonACLAdapter.matches(descriptor, EvaluationContext())
    assert score >= 90


def test_aragon_matches_via_set_permission_topic():
    descriptor = {
        "kind": "external_set",
        "enumeration_hint": [
            {
                "event_address": "0x0",
                "topic0": ARAGON_SET_PERMISSION_TOPIC0,
                "topics_to_keys": {},
                "data_to_keys": {},
                "direction": "add",
            }
        ],
    }
    score = AragonACLAdapter.matches(descriptor, EvaluationContext())
    assert score >= 80


def test_aragon_matches_via_can_perform_selector():
    descriptor = {"kind": "external_set"}
    ctx = EvaluationContext(
        contract_address=ADDR_A,
        bytecode=FakeBytecodeRepo({ARAGON_CAN_PERFORM_SELECTOR}),
    )
    score = AragonACLAdapter.matches(descriptor, ctx)
    assert score >= 70


def test_aragon_enumerate_with_repo():
    descriptor = {
        "kind": "external_set",
        "authority_contract": {"abi_hint": "aragon_acl", "target_app": ADDR_B},
    }
    ctx = EvaluationContext(
        chain_id=1,
        contract_address=ADDR_A,
        meta={"aragon_acl_repo": FakeAragonRepo([ADDR_C])},
    )
    cap = AragonACLAdapter().enumerate(descriptor, ctx)
    assert cap.kind == "finite_set"
    assert cap.members == [ADDR_C.lower()]


def test_aragon_enumerate_no_backend_yields_check_only():
    descriptor = {"kind": "external_set", "authority_contract": {"abi_hint": "aragon_acl"}}
    ctx = EvaluationContext(contract_address=ADDR_A)
    cap = AragonACLAdapter().enumerate(descriptor, ctx)
    assert cap.kind == "external_check_only"
    assert cap.check.target_call_selector == ARAGON_CAN_PERFORM_SELECTOR


# ---------------------------------------------------------------------------
# DSAuth
# ---------------------------------------------------------------------------


def test_dsauth_matches_via_abi_hint():
    descriptor = {"kind": "external_set", "authority_contract": {"abi_hint": "dsauth"}}
    assert DSAuthAdapter.matches(descriptor, EvaluationContext()) >= 90


def test_dsauth_matches_via_can_call_selector():
    descriptor = {"kind": "external_set"}
    ctx = EvaluationContext(
        contract_address=ADDR_A,
        bytecode=FakeBytecodeRepo({DS_AUTH_CAN_CALL_SELECTOR}),
    )
    assert DSAuthAdapter.matches(descriptor, ctx) >= 60


def test_dsauth_enumerate_with_repo():
    descriptor = {
        "kind": "external_set",
        "authority_contract": {"abi_hint": "dsauth", "target_app": ADDR_B},
        "selector_context": {"selectors": ["0x12345678"]},
    }
    ctx = EvaluationContext(
        chain_id=1,
        contract_address=ADDR_A,
        meta={"dsauth_repo": FakeDSAuthRepo([ADDR_C])},
    )
    cap = DSAuthAdapter().enumerate(descriptor, ctx)
    assert cap.kind == "finite_set"
    assert cap.members == [ADDR_C.lower()]


def test_dsauth_enumerate_no_backend_yields_check_only():
    descriptor = {"kind": "external_set", "authority_contract": {"abi_hint": "dsauth"}}
    ctx = EvaluationContext(contract_address=ADDR_A)
    cap = DSAuthAdapter().enumerate(descriptor, ctx)
    assert cap.kind == "external_check_only"
    assert cap.check.target_call_selector == DS_AUTH_CAN_CALL_SELECTOR


# ---------------------------------------------------------------------------
# EIP-1271
# ---------------------------------------------------------------------------


def test_eip1271_matches_via_abi_hint():
    descriptor = {"kind": "external_set", "authority_contract": {"abi_hint": "eip1271"}}
    assert EIP1271Adapter.matches(descriptor, EvaluationContext()) >= 90


def test_eip1271_does_not_match_without_hint():
    descriptor = {"kind": "external_set"}
    # EIP-1271 detection at the leaf level (signature_auth) is
    # static-stage's job; the resolver-side adapter only fires
    # when the descriptor explicitly hints eip1271.
    assert EIP1271Adapter.matches(descriptor, EvaluationContext()) == 0


def test_eip1271_enumerate_always_check_only():
    descriptor = {"kind": "external_set", "authority_contract": {"abi_hint": "eip1271"}}
    ctx = EvaluationContext(contract_address=ADDR_A)
    cap = EIP1271Adapter().enumerate(descriptor, ctx)
    assert cap.kind == "external_check_only"
    assert cap.check.target_call_selector == "0x1626ba7e"


# ---------------------------------------------------------------------------
# Registry integration: multiple adapters scored
# ---------------------------------------------------------------------------


def test_registry_picks_aragon_over_safe_for_aragon_descriptor():
    from services.resolution.adapters.access_control import AccessControlAdapter
    from services.resolution.adapters.safe import SafeAdapter

    registry = AdapterRegistry()
    registry.register(SafeAdapter)
    registry.register(AccessControlAdapter)
    registry.register(AragonACLAdapter)
    registry.register(DSAuthAdapter)

    descriptor = {"kind": "external_set", "authority_contract": {"abi_hint": "aragon_acl"}}
    picked = registry.pick(descriptor, EvaluationContext())
    assert picked is AragonACLAdapter
