"""Generic event-indexed adapter.

Covers any storage var whose writes are observable through events.
The static stage's ``mapping_events.py`` detector populates
``set_descriptor.enumeration_hint`` with one or more EventHint
records describing the (event_address, topic0, key positions,
direction) for each writer. This adapter walks those hints
generically — no per-protocol code.

Detection: matches() fires when ``enumeration_hint`` is populated
AND no more-specific adapter (AC, Aragon, etc.) has claimed the
descriptor. Score is intentionally low (50) so specific adapters
win when they apply.

Enumeration: walks the EventLogRepo (Protocol backend) to fetch
add/remove events for the contract, folds them into a current
member set per key position. Confidence depends on the repo's
last_indexed_block; if unindexed, returns external_check_only
with the event topic as the probe interface.
"""

from __future__ import annotations

from typing import Any, Protocol

from ..capabilities import CapabilityExpr, ExternalCheck
from . import EnumerationResult, EvaluationContext


class EventLogRepo(Protocol):
    """Generic event-log reader used by the event_indexed adapter.

    Test fixtures provide in-memory data; production wires this to
    the same indexer infrastructure that powers RoleGrantsRepo.
    """

    def fold_event_writes(
        self,
        *,
        chain_id: int,
        event_address: str,
        topic0: str,
        topics_to_keys: dict[int, int],
        data_to_keys: dict[int, int],
        direction: str,
        block: int | None = None,
    ) -> EnumerationResult: ...


class EventIndexedAdapter:
    """Generic adapter for storage vars with enumeration_hint
    populated. Folds event-add/remove records into a current member
    set."""

    @classmethod
    def matches(cls, descriptor: dict, ctx: EvaluationContext) -> int:
        # Lower score than specialized adapters so AC / Safe / Aragon
        # / DSAuth take precedence when they match. We're the
        # fallback for "structurally a mapping with events but no
        # standard ABI fingerprint."
        hints = descriptor.get("enumeration_hint")
        if not hints:
            return 0
        # Require at least one add/remove event hint with topic0.
        for hint in hints:
            if hint.get("topic0") and hint.get("direction") in ("add", "remove"):
                return 50
        return 0

    @classmethod
    def supports_external_check_only(cls) -> bool:
        return True

    def enumerate(self, descriptor: dict, ctx: EvaluationContext) -> CapabilityExpr:
        repo = ctx.meta.get("event_log_repo") if ctx.meta else None
        hints = descriptor.get("enumeration_hint") or []
        if repo is None or ctx.contract_address is None or not hints:
            # Without a backend we surface the event as a probe
            # interface — UI can render "membership-checkable, query
            # by address" with the event topic as context.
            primary = next(
                (h for h in hints if h.get("topic0") and h.get("direction") in ("add", "remove")),
                None,
            )
            if primary is None:
                return CapabilityExpr.unsupported("event_indexed_no_hint")
            return CapabilityExpr.external_check_only(
                ExternalCheck(
                    target_address=primary.get("event_address") or ctx.contract_address,
                    target_call_selector=None,
                    extra={"topic0": primary["topic0"], "direction": primary["direction"]},
                )
            )

        merged: list[str] = []
        worst_confidence = "enumerable"
        last_block: int | None = None
        for hint in hints:
            topic0 = hint.get("topic0")
            direction = hint.get("direction")
            if not topic0 or direction not in ("add", "remove"):
                continue
            event_address = hint.get("event_address") or ctx.contract_address
            try:
                result = repo.fold_event_writes(
                    chain_id=ctx.chain_id,
                    event_address=event_address,
                    topic0=topic0,
                    topics_to_keys=hint.get("topics_to_keys") or {},
                    data_to_keys=hint.get("data_to_keys") or {},
                    direction=direction,
                    block=ctx.block,
                )
            except Exception:
                continue
            if direction == "add":
                merged.extend(result.members)
            elif direction == "remove":
                # Remove members that were revoked.
                bad = {m.lower() for m in result.members}
                merged = [m for m in merged if m.lower() not in bad]
            if result.confidence == "partial" and worst_confidence == "enumerable":
                worst_confidence = "partial"
            last_block = result.last_indexed_block

        return CapabilityExpr.finite_set(
            merged,
            quality="exact" if worst_confidence == "enumerable" else "lower_bound",
            confidence=worst_confidence,  # type: ignore[arg-type]
            last_indexed_block=last_block,
        )
