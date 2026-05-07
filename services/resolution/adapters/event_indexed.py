"""Generic event-indexed adapter.

Covers any storage var whose writes are observable through events.
The static stage's ``mapping_events.py`` detector populates
``set_descriptor.enumeration_hint`` with EventHint records describing
the event address, topic, key positions, and fold direction. This
adapter consumes those records directly with no per-standard adapter.
"""

from __future__ import annotations

from ..capabilities import CapabilityExpr, ExternalCheck
from . import EvaluationContext


class EventIndexedAdapter:
    """Generic adapter for storage vars with enumeration_hint
    populated. Folds event-add/remove records into a current member
    set."""

    @classmethod
    def matches(cls, descriptor: dict, ctx: EvaluationContext) -> int:
        hints = descriptor.get("enumeration_hint")
        if not hints:
            return 0
        # Require at least one add/remove event hint with topic0.
        for hint in hints:
            if hint.get("topic0") and hint.get("direction") in ("add", "remove"):
                return 50
        # D.2 — value-predicate dispatch. ``set`` direction + a
        # ``value_predicate`` on the descriptor + a ``value_position``
        # in the hint is the shape ``OwnerSet(addr, val)`` produces.
        if descriptor.get("value_predicate"):
            for hint in hints:
                if hint.get("topic0") and hint.get("direction") == "set" and hint.get("value_position") is not None:
                    return 55
        return 0

    @classmethod
    def supports_external_check_only(cls) -> bool:
        return True

    def enumerate(self, descriptor: dict, ctx: EvaluationContext) -> CapabilityExpr:
        # D.2 — when the descriptor carries a ValuePredicate and at
        # least one ``set``-direction hint, dispatch to the value-aware
        # fold path (latest-value-per-key, filtered by predicate)
        # rather than the legacy add/remove present-set fold.
        value_predicate = descriptor.get("value_predicate")
        hints = descriptor.get("enumeration_hint") or []
        if value_predicate and ctx.contract_address is not None:
            set_hints = [
                h
                for h in hints
                if h.get("direction") == "set" and h.get("value_position") is not None and h.get("topic0")
            ]
            if set_hints:
                return self._enumerate_value_predicate(descriptor, set_hints, value_predicate, ctx)

        repo = ctx.event_log_repo or (ctx.meta.get("event_log_repo") if ctx.meta else None)
        if repo is None or not hints:
            primary = next(
                (h for h in hints if h.get("topic0") and h.get("direction") in ("add", "remove")),
                None,
            )
            if primary is None:
                return CapabilityExpr.unsupported("event_indexed_no_hint")
            return self._external_check(descriptor, primary, ctx, ["no_event_log_repo"])

        merged: list[str] = []
        worst_confidence = "enumerable"
        last_block: int | None = None
        for hint in hints:
            topic0 = hint.get("topic0")
            direction = hint.get("direction")
            if not topic0 or direction not in ("add", "remove"):
                continue
            event_address = _resolve_event_address(descriptor, hint, ctx)
            if event_address is None:
                return self._external_check(descriptor, hint, ctx, ["event_address_unresolved"])
            try:
                result = repo.fold_event_writes(
                    chain_id=ctx.chain_id,
                    event_address=event_address,
                    topic0=topic0,
                    topics_to_keys=hint.get("topics_to_keys") or {},
                    data_to_keys=hint.get("data_to_keys") or {},
                    key_sources=descriptor.get("key_sources") or [],
                    direction=direction,
                    block=ctx.block,
                )
            except Exception:
                continue
            if result.confidence == "partial" and result.partial_reason == "unresolved_event_key":
                return self._external_check(descriptor, hint, ctx, [result.partial_reason])
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

    def _external_check(
        self,
        descriptor: dict,
        hint: dict,
        ctx: EvaluationContext,
        basis: list[str],
    ) -> CapabilityExpr:
        return CapabilityExpr.external_check_only(
            ExternalCheck(
                target_address=_resolve_event_address(descriptor, hint, ctx),
                target_call_selector=descriptor.get("callee_selector"),
                extra={
                    "basis": basis,
                    "topic0": hint.get("topic0"),
                    "direction": hint.get("direction"),
                    "callee_function": descriptor.get("callee_function"),
                    "callee_signature": descriptor.get("callee_signature"),
                },
            )
        )

    def _enumerate_value_predicate(
        self,
        descriptor: dict,
        set_hints: list[dict],
        value_predicate: dict,
        ctx: EvaluationContext,
    ) -> CapabilityExpr:
        """D.2 fold: latest-value-per-key, filtered by ``value_predicate``.

        Uses ``services.resolution.mapping_enumerator`` to replay events
        on demand. The result is a ``finite_set`` of keys whose latest
        value satisfies the predicate. ``status != complete`` from the
        underlying scan demotes ``quality`` to ``lower_bound``.
        """
        contract_address = ctx.contract_address or ""
        # Reconstruct WriterEventSpec dicts the enumerator expects from
        # the EventHint payload. The hint already carries ``topic0``,
        # ``event_signature``, ``key_position`` etc. — we just rename
        # to the spec keys.
        writer_specs = []
        for hint in set_hints:
            writer_specs.append(
                {
                    "mapping_name": hint.get("mapping_name") or descriptor.get("storage_var") or "",
                    "event_signature": hint.get("event_signature") or "",
                    "event_name": hint.get("event_name") or "",
                    "key_position": int(hint.get("key_position") or 0),
                    "indexed_positions": list(hint.get("indexed_positions") or []),
                    "direction": "set",
                    "writer_function": hint.get("writer_function") or "",
                    "value_position": int(hint["value_position"]),
                }
            )

        from ..mapping_enumerator import enumerate_mapping_values_sync, filter_value_entries

        try:
            scan = enumerate_mapping_values_sync(
                contract_address,
                writer_specs,  # type: ignore[arg-type]
                value_predicate=value_predicate,
            )
        except Exception:
            return CapabilityExpr.unsupported("mapping_value_scan_failed")

        keys = filter_value_entries(scan["entries"], value_predicate)
        is_complete = scan["status"] == "complete"
        return CapabilityExpr.finite_set(
            keys,
            quality="exact" if is_complete else "lower_bound",
            confidence="enumerable" if is_complete else "partial",
            last_indexed_block=scan["last_block_scanned"] or None,
        )


def _resolve_event_address(descriptor: dict, hint: dict, ctx: EvaluationContext) -> str | None:
    raw = hint.get("event_address")
    if isinstance(raw, str) and raw.startswith("0x") and len(raw) == 42:
        return raw.lower()

    authority = descriptor.get("authority_contract") or {}
    raw = authority.get("address")
    if isinstance(raw, str) and raw.startswith("0x") and len(raw) == 42:
        return raw.lower()

    source = authority.get("address_source") or {}
    if source.get("source") == "state_variable":
        name = source.get("state_variable_name")
        values = ctx.state_var_values or {}
        value = values.get(name) if isinstance(name, str) else None
        if isinstance(value, str) and value.startswith("0x") and len(value) == 42:
            return value.lower()

    if ctx.contract_address and ctx.contract_address.startswith("0x") and len(ctx.contract_address) == 42:
        return ctx.contract_address.lower()
    return None
