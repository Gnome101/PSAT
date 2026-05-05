"""Trace-replay adapter for ``mapping(K => V)`` setters that emit no
events (D.4).

Some contracts assign mapping values without emitting a corresponding
event — Maker DSS's older modules, custom ACL designs, contracts
that store the value in a packed slot. The on-demand event replay
(D.2) and the durable indexer (D.3) both depend on the contract
emitting a setter event; if it doesn't, those layers leave the
descriptor at ``external_check_only``.

This adapter closes the gap by replaying call traces. HyperSync's
trace surface exposes ``input``, ``output``, ``call_type``, ``error``,
``trace_address`` for every internal call to the subject contract;
filtering by writer-function selectors and decoding calldata via
``eth_abi.decode`` reconstructs ``(key, value)`` history for the
mapping. Latest assignment per key wins under
``(block_number, transaction_index, trace_address)`` ordering.

Caveats encoded in the adapter:
  * ABI decode failure (unverified contract, non-standard arg shape)
    → return ``external_check_only`` with ``basis=["abi_decode_failed"]``.
    Honest fallback rather than a wrong empty set.
  * ``staticcall`` and reverted calls (non-empty ``error``) skipped.
  * **Proxy + delegatecall: known limitation.** The production trace
    fetcher (``repos/mapping_value_hypersync.HyperSyncTraceFetcher``)
    queries traces with ``to == subject_address``. For UUPS / TUP
    proxies the actual write happens in a child trace with
    ``to == implementation_address, call_type == delegatecall`` —
    that trace is filtered out at the fetcher boundary, so the
    adapter never sees it and the population looks empty. Full
    proxy support requires resolving the EIP-1967 implementation
    address and adding it to the trace ``to`` filter; until then
    the adapter only handles non-proxy contracts. Codex review #2.
  * Constructor / create traces included so initial assignments
    are captured.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

from eth_abi.abi import decode

from ..capabilities import CapabilityExpr, ExternalCheck
from ..mapping_enumerator import _value_predicate_passes
from . import EvaluationContext

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Trace shape + Protocol
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchedTrace:
    """One internal call observed against the subject contract.

    Fields mirror HyperSync's ``TraceField`` set we actually consume.
    ``trace_address`` is the canonical EIP path to this trace within
    the parent transaction (``[1, 0]`` means the first child of the
    second top-level call, etc.) — used as a tiebreaker in the
    ``(block, tx_index, trace_address)`` ordering.
    """

    block_number: int
    transaction_index: int
    trace_address: tuple[int, ...]
    input_data: str  # 0x-prefixed hex of the calldata
    call_type: str  # "call" | "delegatecall" | "staticcall" | "create"
    error: str | None  # non-empty when the call reverted


class TraceFetcher(Protocol):
    """Backend that returns internal-call traces against the subject
    contract within a block range. Production impl wraps HyperSync's
    trace API; tests inject a static list."""

    def fetch_traces(
        self,
        *,
        chain_id: int,
        contract_address: str,
        from_block: int = 0,
        to_block: int | None = None,
    ) -> list[FetchedTrace]: ...


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _WriterSpec:
    """Decoded writer-function metadata. The static pipeline emits
    ``writer_selectors`` as a list of either bare 0x-selectors
    (``"0x12345678"``) or ``selector|arg_types`` strings
    (``"0x12345678|address,uint256"``) so the adapter knows how to
    decode the calldata. Without arg types the trace adapter can't
    decode and skips the trace (recorded in basis).
    """

    selector: str
    arg_types: list[str] | None  # None when only the selector was supplied


class MappingTraceAdapter:
    """Resolves ``mapping_membership`` descriptors with
    ``value_predicate`` AND ``writer_selectors`` AND a
    ``ctx.trace_fetcher`` wired.

    Score is **40** — below the AccessControlAdapter (80+), the
    durable EventIndexedAdapter value path (55), and the on-demand
    event replay (also 55), so the trace adapter is the last resort
    before ``external_check_only``. That's the right ordering: traces
    are slowest to fetch and decode, and they only help when no
    cheaper signal exists.
    """

    @classmethod
    def matches(cls, descriptor: dict, ctx: EvaluationContext) -> int:
        if not descriptor.get("value_predicate"):
            return 0
        if not descriptor.get("writer_selectors"):
            return 0
        if ctx.trace_fetcher is None:
            return 0
        return 40

    @classmethod
    def supports_external_check_only(cls) -> bool:
        return True

    def enumerate(self, descriptor: dict, ctx: EvaluationContext) -> CapabilityExpr:
        contract_address = ctx.contract_address or ""
        value_predicate = descriptor["value_predicate"]
        writer_specs = self._parse_writer_specs(descriptor.get("writer_selectors") or [])
        if not writer_specs:
            return self._fallback(contract_address, ["no_writer_selectors"])

        if ctx.trace_fetcher is None:
            return self._fallback(contract_address, ["no_trace_fetcher"])

        try:
            traces = ctx.trace_fetcher.fetch_traces(
                chain_id=ctx.chain_id,
                contract_address=contract_address,
                from_block=0,
                to_block=ctx.block,
            )
        except Exception as exc:
            logger.warning("MappingTraceAdapter: trace fetch failed: %s", exc)
            return self._fallback(contract_address, ["trace_fetch_failed"])

        # latest_state[key] = (value_hex, block, tx_index, trace_address)
        latest: dict[str, tuple[str, int, int, tuple[int, ...]]] = {}
        decode_failures = 0
        decoded = 0
        for trace in traces:
            if trace.error:
                continue
            if trace.call_type == "staticcall":
                continue
            if not trace.input_data or len(trace.input_data) < 10:
                continue
            sel = trace.input_data[:10].lower()
            spec = next((s for s in writer_specs if s.selector == sel), None)
            if spec is None or spec.arg_types is None:
                continue
            try:
                key_hex, value_hex = self._decode_call_args(trace.input_data, spec.arg_types)
            except Exception:
                decode_failures += 1
                continue
            if key_hex is None or value_hex is None:
                decode_failures += 1
                continue
            decoded += 1
            ordering = (trace.block_number, trace.transaction_index, trace.trace_address)
            prior = latest.get(key_hex)
            if prior is None or ordering > (prior[1], prior[2], prior[3]):
                latest[key_hex] = (value_hex, trace.block_number, trace.transaction_index, trace.trace_address)

        if decoded == 0 and decode_failures > 0:
            return self._fallback(contract_address, ["abi_decode_failed"])

        keys = sorted(
            key
            for key, (value_hex, _b, _t, _ta) in latest.items()
            if _value_predicate_passes(value_hex, value_predicate)
        )
        return CapabilityExpr.finite_set(
            keys,
            quality="exact",
            confidence="enumerable",
        )

    def _parse_writer_specs(self, raw: list[str]) -> list[_WriterSpec]:
        out: list[_WriterSpec] = []
        for entry in raw:
            if not isinstance(entry, str) or not entry.startswith("0x"):
                continue
            if "|" in entry:
                sel, _, arg_blob = entry.partition("|")
                arg_types = [t.strip() for t in arg_blob.split(",") if t.strip()]
                out.append(_WriterSpec(selector=sel.lower(), arg_types=arg_types))
            else:
                out.append(_WriterSpec(selector=entry.lower(), arg_types=None))
        return out

    def _decode_call_args(self, input_hex: str, arg_types: list[str]) -> tuple[str | None, str | None]:
        """Decode the call's calldata and emit (key_hex, value_hex).

        The convention: first arg is the mapping key, second arg is
        the assigned value. ``setOwner(address, uint256)``,
        ``setBalance(address, uint256)`` etc. match. Multi-key
        mappings (e.g. ``setApproval(owner, spender, allowed)``)
        aren't covered — the static pipeline emits a single-key
        descriptor for those today.
        """
        if len(arg_types) < 2:
            return None, None
        body = input_hex[10:] if input_hex.startswith("0x") else input_hex[8:]
        data = bytes.fromhex(body)
        try:
            decoded = decode(arg_types, data)
        except Exception:
            return None, None
        if len(decoded) < 2:
            return None, None
        key_raw = decoded[0]
        value_raw = decoded[1]
        if isinstance(key_raw, str):
            key_hex = key_raw.lower()
        elif isinstance(key_raw, (bytes, bytearray)):
            key_hex = "0x" + key_raw.hex()
        else:
            key_hex = "0x" + format(int(key_raw), "064x")
        if isinstance(value_raw, int):
            value_hex = "0x" + format(value_raw, "064x")
        elif isinstance(value_raw, str):
            # address — pad to a 32-byte word
            value_hex = "0x" + ("00" * 12) + value_raw[2:].lower()
        elif isinstance(value_raw, (bytes, bytearray)):
            value_hex = "0x" + value_raw.hex().rjust(64, "0")
        else:
            value_hex = "0x" + format(int(value_raw), "064x")
        return key_hex, value_hex

    def _fallback(self, contract_address: str, basis: list[str]) -> CapabilityExpr:
        return CapabilityExpr.external_check_only(
            ExternalCheck(
                target_address=contract_address,
                target_call_selector=None,
                extra={"basis": basis},
            )
        )
