"""Unit tests for ``MappingTraceAdapter`` (D.4).

The adapter resolves ``mapping_membership`` descriptors with
``value_predicate`` + ``writer_selectors`` by replaying call traces.
We feed it a fake ``TraceFetcher`` (no HyperSync round-trip) with
crafted ``setOwner(address, uint256)`` calldata and verify:

  * matches() requires value_predicate + writer_selectors + a
    trace_fetcher.
  * Latest assignment per key wins under
    ``(block, tx_index, trace_address)`` ordering.
  * ABI decode failure → external_check_only with
    ``basis=["abi_decode_failed"]``.
  * staticcall traces are skipped; reverted traces are skipped.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from eth_abi.abi import encode  # noqa: E402

from services.resolution.adapters import EvaluationContext  # noqa: E402
from services.resolution.adapters.mapping_trace import FetchedTrace, MappingTraceAdapter  # noqa: E402

SET_OWNER_SELECTOR = "0x49d8e30c"  # arbitrary — fixture-only; the adapter just matches input[0..4]


def _set_owner_calldata(addr: str, value: int) -> str:
    body = encode(["address", "uint256"], [addr, value]).hex()
    return SET_OWNER_SELECTOR + body


class FakeTraceFetcher:
    def __init__(self, traces: list[FetchedTrace]):
        self.traces = traces

    def fetch_traces(self, *, chain_id, contract_address, from_block=0, to_block=None):
        return list(self.traces)


def _descriptor(value_pred: dict) -> dict:
    return {
        "kind": "mapping_membership",
        "storage_var": "owners",
        "key_sources": [],
        "value_predicate": value_pred,
        "writer_selectors": [SET_OWNER_SELECTOR + "|address,uint256"],
    }


# ---------------------------------------------------------------------------
# matches()
# ---------------------------------------------------------------------------


def test_matches_requires_value_predicate():
    desc = {"kind": "mapping_membership", "writer_selectors": [SET_OWNER_SELECTOR]}
    ctx = EvaluationContext(trace_fetcher=FakeTraceFetcher([]))
    assert MappingTraceAdapter.matches(desc, ctx) == 0


def test_matches_requires_writer_selectors():
    desc = _descriptor({"op": "eq", "rhs_values": ["10"], "value_type": "uint256"})
    desc.pop("writer_selectors")
    ctx = EvaluationContext(trace_fetcher=FakeTraceFetcher([]))
    assert MappingTraceAdapter.matches(desc, ctx) == 0


def test_matches_requires_trace_fetcher():
    desc = _descriptor({"op": "eq", "rhs_values": ["10"], "value_type": "uint256"})
    ctx = EvaluationContext()  # no trace_fetcher
    assert MappingTraceAdapter.matches(desc, ctx) == 0


def test_matches_when_all_present():
    desc = _descriptor({"op": "eq", "rhs_values": ["10"], "value_type": "uint256"})
    ctx = EvaluationContext(trace_fetcher=FakeTraceFetcher([]))
    assert MappingTraceAdapter.matches(desc, ctx) == 40


# ---------------------------------------------------------------------------
# enumerate()
# ---------------------------------------------------------------------------


def test_enumerate_returns_keys_matching_eq_predicate():
    a = "0x" + "11" * 20
    b = "0x" + "22" * 20
    traces = [
        FetchedTrace(
            block_number=100,
            transaction_index=0,
            trace_address=(0,),
            input_data=_set_owner_calldata(a, 10),
            call_type="call",
            error=None,
        ),
        FetchedTrace(
            block_number=100,
            transaction_index=1,
            trace_address=(0,),
            input_data=_set_owner_calldata(b, 7),
            call_type="call",
            error=None,
        ),
    ]
    desc = _descriptor({"op": "eq", "rhs_values": ["10"], "value_type": "uint256"})
    ctx = EvaluationContext(
        contract_address="0xCC00000000000000000000000000000000000001",
        trace_fetcher=FakeTraceFetcher(traces),
    )
    cap = MappingTraceAdapter().enumerate(desc, ctx)
    assert cap.kind == "finite_set"
    assert cap.members == [a.lower()]


def test_enumerate_latest_wins_over_earlier_assignment():
    a = "0x" + "11" * 20
    traces = [
        FetchedTrace(
            block_number=100,
            transaction_index=0,
            trace_address=(0,),
            input_data=_set_owner_calldata(a, 10),
            call_type="call",
            error=None,
        ),
        # Same key reset to 5 — at a later block.
        FetchedTrace(
            block_number=110,
            transaction_index=0,
            trace_address=(0,),
            input_data=_set_owner_calldata(a, 5),
            call_type="call",
            error=None,
        ),
    ]
    desc = _descriptor({"op": "eq", "rhs_values": ["10"], "value_type": "uint256"})
    ctx = EvaluationContext(
        contract_address="0xCC00000000000000000000000000000000000001",
        trace_fetcher=FakeTraceFetcher(traces),
    )
    cap = MappingTraceAdapter().enumerate(desc, ctx)
    assert cap.kind == "finite_set"
    assert cap.members == []  # latest a is 5, doesn't match eq 10

    desc2 = _descriptor({"op": "eq", "rhs_values": ["5"], "value_type": "uint256"})
    cap2 = MappingTraceAdapter().enumerate(desc2, ctx)
    assert cap2.members == [a.lower()]


def test_enumerate_skips_staticcall_and_reverted_traces():
    a = "0x" + "11" * 20
    traces = [
        # Reverted — must not count.
        FetchedTrace(
            block_number=100,
            transaction_index=0,
            trace_address=(0,),
            input_data=_set_owner_calldata(a, 10),
            call_type="call",
            error="Reverted",
        ),
        # staticcall — must not count.
        FetchedTrace(
            block_number=101,
            transaction_index=0,
            trace_address=(0,),
            input_data=_set_owner_calldata(a, 99),
            call_type="staticcall",
            error=None,
        ),
        # Real write at block 102.
        FetchedTrace(
            block_number=102,
            transaction_index=0,
            trace_address=(0,),
            input_data=_set_owner_calldata(a, 7),
            call_type="call",
            error=None,
        ),
    ]
    desc = _descriptor({"op": "eq", "rhs_values": ["7"], "value_type": "uint256"})
    ctx = EvaluationContext(
        contract_address="0xCC00000000000000000000000000000000000001",
        trace_fetcher=FakeTraceFetcher(traces),
    )
    cap = MappingTraceAdapter().enumerate(desc, ctx)
    assert cap.members == [a.lower()]


def test_enumerate_decode_failure_falls_back_to_external_check_only():
    """Calldata that doesn't conform to the declared arg types ⇒
    adapter must surface ``external_check_only`` with
    ``basis=["abi_decode_failed"]`` rather than silently returning
    an empty finite_set.
    """
    traces = [
        FetchedTrace(
            block_number=100,
            transaction_index=0,
            trace_address=(0,),
            input_data=SET_OWNER_SELECTOR + "ff",  # truncated body
            call_type="call",
            error=None,
        )
    ]
    desc = _descriptor({"op": "eq", "rhs_values": ["10"], "value_type": "uint256"})
    ctx = EvaluationContext(
        contract_address="0xCC00000000000000000000000000000000000001",
        trace_fetcher=FakeTraceFetcher(traces),
    )
    cap = MappingTraceAdapter().enumerate(desc, ctx)
    assert cap.kind == "external_check_only"
    assert cap.check is not None
    extra = cap.check.extra or {}
    assert extra.get("basis") == ["abi_decode_failed"]
