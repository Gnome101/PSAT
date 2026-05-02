"""Tests for the generic event-indexed adapter."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution.adapters import (  # noqa: E402
    AdapterRegistry,
    EnumerationResult,
    EvaluationContext,
)
from services.resolution.adapters.access_control import AccessControlAdapter  # noqa: E402
from services.resolution.adapters.event_indexed import EventIndexedAdapter  # noqa: E402

ADDR_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDR_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
ADDR_C = "0xcccccccccccccccccccccccccccccccccccccccc"


class FakeEventLogRepo:
    def __init__(self, events_by_topic: dict[str, list[tuple[str, str]]]):
        # events_by_topic: topic0 → list of (direction, member_address)
        self.events_by_topic = events_by_topic

    def fold_event_writes(
        self, *, chain_id, event_address, topic0, topics_to_keys, data_to_keys, direction, block=None
    ):
        records = self.events_by_topic.get(topic0, [])
        members = [addr for d, addr in records if d == direction]
        return EnumerationResult(
            members=members,
            confidence="enumerable",
            last_indexed_block=18_000_000,
        )


def test_event_indexed_matches_with_add_event_hint():
    descriptor = {
        "kind": "mapping_membership",
        "enumeration_hint": [
            {"topic0": "0xaa", "direction": "add", "event_address": ADDR_A},
        ],
    }
    score = EventIndexedAdapter.matches(descriptor, EvaluationContext())
    assert 0 < score <= 60  # low score so specialized adapters win


def test_event_indexed_does_not_match_without_hints():
    descriptor = {"kind": "mapping_membership"}
    assert EventIndexedAdapter.matches(descriptor, EvaluationContext()) == 0


def test_event_indexed_enumerate_with_repo():
    descriptor = {
        "kind": "mapping_membership",
        "enumeration_hint": [
            {
                "topic0": "0xaa",
                "direction": "add",
                "event_address": ADDR_A,
                "topics_to_keys": {1: 0},
                "data_to_keys": {},
            },
        ],
    }
    repo = FakeEventLogRepo({"0xaa": [("add", ADDR_B), ("add", ADDR_C)]})
    ctx = EvaluationContext(
        chain_id=1,
        contract_address=ADDR_A,
        meta={"event_log_repo": repo},
    )
    cap = EventIndexedAdapter().enumerate(descriptor, ctx)
    assert cap.kind == "finite_set"
    assert sorted(cap.members) == sorted([ADDR_B.lower(), ADDR_C.lower()])


def test_event_indexed_handles_add_then_remove():
    descriptor = {
        "kind": "mapping_membership",
        "enumeration_hint": [
            {"topic0": "0xaa", "direction": "add", "event_address": ADDR_A, "topics_to_keys": {}, "data_to_keys": {}},
            {
                "topic0": "0xbb",
                "direction": "remove",
                "event_address": ADDR_A,
                "topics_to_keys": {},
                "data_to_keys": {},
            },
        ],
    }
    repo = FakeEventLogRepo(
        {
            "0xaa": [("add", ADDR_B), ("add", ADDR_C)],
            "0xbb": [("remove", ADDR_B)],
        }
    )
    ctx = EvaluationContext(
        chain_id=1,
        contract_address=ADDR_A,
        meta={"event_log_repo": repo},
    )
    cap = EventIndexedAdapter().enumerate(descriptor, ctx)
    assert cap.kind == "finite_set"
    # ADDR_B was added then removed; only ADDR_C remains.
    assert cap.members == [ADDR_C.lower()]


def test_event_indexed_no_backend_yields_check_only():
    descriptor = {
        "kind": "mapping_membership",
        "enumeration_hint": [
            {"topic0": "0xaa", "direction": "add", "event_address": ADDR_A, "topics_to_keys": {}, "data_to_keys": {}},
        ],
    }
    ctx = EvaluationContext(contract_address=ADDR_A)
    cap = EventIndexedAdapter().enumerate(descriptor, ctx)
    assert cap.kind == "external_check_only"
    assert cap.check.extra["topic0"] == "0xaa"


def test_registry_specialized_beats_event_indexed():
    """When AC and EventIndexed both score, AC wins (higher score)."""
    # Construct an AC-shaped descriptor: 2-key with caller + role,
    # WITH the AC RoleGranted topic in enumeration_hint.
    from services.resolution.adapters.access_control import ROLE_GRANTED_TOPIC0

    descriptor = {
        "kind": "mapping_membership",
        "key_sources": [
            {"source": "parameter", "parameter_index": 0, "parameter_name": "role"},
            {"source": "msg_sender"},
        ],
        "enumeration_hint": [
            {
                "topic0": ROLE_GRANTED_TOPIC0,
                "direction": "add",
                "event_address": ADDR_A,
                "topics_to_keys": {1: 0, 2: 1},
                "data_to_keys": {},
            },
        ],
    }
    registry = AdapterRegistry()
    registry.register(EventIndexedAdapter)
    registry.register(AccessControlAdapter)
    picked = registry.pick(descriptor, EvaluationContext())
    assert picked is AccessControlAdapter


def test_registry_event_indexed_picks_when_no_specialized_match():
    """For a mapping with events but no recognized standard ABI,
    EventIndexed catches it generically."""
    descriptor = {
        "kind": "mapping_membership",
        "key_sources": [{"source": "msg_sender"}],
        "enumeration_hint": [
            {"topic0": "0xff", "direction": "add", "event_address": ADDR_A, "topics_to_keys": {}, "data_to_keys": {}},
        ],
    }
    registry = AdapterRegistry()
    registry.register(AccessControlAdapter)
    registry.register(EventIndexedAdapter)
    picked = registry.pick(descriptor, EvaluationContext())
    assert picked is EventIndexedAdapter
