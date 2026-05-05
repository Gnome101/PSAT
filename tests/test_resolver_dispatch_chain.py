"""Dispatch-chain ordering for ``mapping_membership`` descriptors with
``value_predicate`` (PR D.5).

Verifies that ``AdapterRegistry.pick`` selects the right adapter for
each shape — and that the chain naturally falls through when a
higher-priority adapter doesn't match:

  1. AccessControlAdapter wins for 2-key role/caller mappings.
  2. EventIndexedAdapter (D.2/D.3) wins for value-predicate
     descriptors with set-direction enumeration_hint.
  3. MappingTraceAdapter (D.4) wins for value-predicate descriptors
     with writer_selectors and a trace_fetcher, when no event hint
     is present.
  4. No adapter matches → ``registry.pick(...) is None`` and the
     resolver emits ``unsupported(no_adapter)`` / external_check_only.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution.adapters import AdapterRegistry, EvaluationContext  # noqa: E402
from services.resolution.adapters.access_control import AccessControlAdapter  # noqa: E402
from services.resolution.adapters.event_indexed import EventIndexedAdapter  # noqa: E402
from services.resolution.adapters.mapping_trace import MappingTraceAdapter  # noqa: E402


def _registry() -> AdapterRegistry:
    """Production registration order from
    ``capability_resolver.resolve_contract_capabilities``."""
    reg = AdapterRegistry()
    for cls in (AccessControlAdapter, EventIndexedAdapter, MappingTraceAdapter):
        reg.register(cls)
    return reg


class _StubFetcher:
    """Bare placeholder so ``ctx.trace_fetcher is not None``; the
    adapter never actually calls it in matches()."""

    def fetch_traces(self, **_):
        return []


def test_event_hint_routes_to_event_indexed_over_trace():
    """Descriptor with both set-event hint AND writer_selectors —
    EventIndexedAdapter (55) beats MappingTraceAdapter (40)."""
    desc = {
        "kind": "mapping_membership",
        "storage_var": "owners",
        "key_sources": [],
        "value_predicate": {"op": "eq", "rhs_values": ["10"], "value_type": "uint256"},
        "writer_selectors": ["0x12345678|address,uint256"],
        "enumeration_hint": [
            {
                "topic0": "0x" + "ab" * 32,
                "direction": "set",
                "value_position": 1,
                "key_position": 0,
                "event_signature": "OwnerSet(address,uint256)",
                "indexed_positions": [0],
            }
        ],
    }
    ctx = EvaluationContext(
        contract_address="0xCC00000000000000000000000000000000000001",
        trace_fetcher=_StubFetcher(),
    )
    chosen = _registry().pick(desc, ctx)
    assert chosen is EventIndexedAdapter


def test_writer_selectors_only_routes_to_trace_adapter():
    """Descriptor with no enumeration_hint but writer_selectors +
    trace_fetcher — only MappingTraceAdapter matches."""
    desc = {
        "kind": "mapping_membership",
        "storage_var": "owners",
        "key_sources": [],
        "value_predicate": {"op": "eq", "rhs_values": ["10"], "value_type": "uint256"},
        "writer_selectors": ["0x12345678|address,uint256"],
    }
    ctx = EvaluationContext(
        contract_address="0xCC00000000000000000000000000000000000001",
        trace_fetcher=_StubFetcher(),
    )
    chosen = _registry().pick(desc, ctx)
    assert chosen is MappingTraceAdapter


def test_no_hint_no_writer_selectors_no_match():
    """Descriptor with value_predicate alone — no adapter matches.
    The resolver must surface this as external_check_only / unsupported.
    """
    desc = {
        "kind": "mapping_membership",
        "storage_var": "owners",
        "key_sources": [],
        "value_predicate": {"op": "eq", "rhs_values": ["10"], "value_type": "uint256"},
    }
    ctx = EvaluationContext(contract_address="0xCC00000000000000000000000000000000000001")
    assert _registry().pick(desc, ctx) is None


def test_writer_selectors_without_trace_fetcher_no_match():
    """``writer_selectors`` alone is not enough — without a
    trace_fetcher to call, MappingTraceAdapter must NOT match.
    Emitting a finite_set with no fetcher would lie about the
    population.
    """
    desc = {
        "kind": "mapping_membership",
        "storage_var": "owners",
        "key_sources": [],
        "value_predicate": {"op": "eq", "rhs_values": ["10"], "value_type": "uint256"},
        "writer_selectors": ["0x12345678|address,uint256"],
    }
    ctx = EvaluationContext(contract_address="0xCC00000000000000000000000000000000000001")
    assert _registry().pick(desc, ctx) is None


def test_chain_picks_access_control_for_2key_role_caller_mapping():
    """Score ordering: AC scores 95 on a 2-key (role, caller)
    mapping_membership with a RoleGranted enumeration_hint, beating
    EventIndexedAdapter's 55 and MappingTraceAdapter's 40."""
    role_granted_topic = "0x2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d"
    desc = {
        "kind": "mapping_membership",
        "storage_var": "_roles",
        "key_sources": [
            {"source": "constant", "value": b"\x00" * 32},
            {"source": "msg_sender"},
        ],
        "enumeration_hint": [
            {"topic0": role_granted_topic, "direction": "add"},
        ],
        "role_domain": {"kind": "constant_set", "values": [b"\x00" * 32]},
    }
    ctx = EvaluationContext(
        contract_address="0xCC00000000000000000000000000000000000001",
        trace_fetcher=_StubFetcher(),
    )
    chosen = _registry().pick(desc, ctx)
    assert chosen is AccessControlAdapter
