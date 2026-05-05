"""Postgres-backed repo for the durable mapping_value_events table (D.3).

Mirrors ``PostgresRoleGrantsRepo``: reads decoded set-events that the
``mapping_value_indexer`` worker has persisted, computes "latest value
per key" for a given mapping, and applies a ``ValuePredicate`` to
emit the matching keys. The capability resolver wires this into
``EvaluationContext.meta['mapping_value_repo']`` so the
``EventIndexedAdapter`` value path prefers durable data over the
on-demand HyperSync scan.

The repo never writes — the indexer worker is the sole writer.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import Contract, MappingValueCursor, MappingValueEvent

# Reuse the predicate-evaluation helper from the on-demand path so the
# durable + on-demand paths agree on edge cases (zero-words, address
# truncation, in-set membership).
from services.resolution.mapping_enumerator import _value_predicate_passes


class PostgresMappingValueRepo:
    """Reads ``mapping_value_events`` to answer
    ``latest_keys_passing_predicate(...)``.

    ``session`` is request-scoped and not owned by the repo. All
    queries are read-only; the indexer worker writes.
    """

    def __init__(self, session: Session) -> None:
        self.session = session

    def latest_keys_passing_predicate(
        self,
        *,
        chain_id: int,
        contract_address: str,
        writer_specs: list[dict[str, Any]],
        value_predicate: dict[str, Any],
        block: int | None = None,
    ) -> list[str]:
        """Return keys whose latest value (at or before ``block``)
        satisfies ``value_predicate``.

        ``writer_specs`` is the list the EventIndexedAdapter built
        from the descriptor's enumeration_hint — only the
        ``mapping_name`` matters here (it's the table-level
        partition). Keys are returned lowercased.
        """
        cid = self._resolve_contract_id(contract_address)
        if cid is None:
            return []
        # All mapping_names mentioned by the writer specs.
        mapping_names = sorted({s["mapping_name"] for s in writer_specs if s.get("mapping_name")})
        if not mapping_names:
            return []

        latest = self._latest_values(chain_id, cid, mapping_names, block)
        out: list[str] = []
        for key, value_hex in latest.items():
            if _value_predicate_passes(value_hex, value_predicate):
                out.append(key)
        return sorted(out)

    def latest_values_for_mapping(
        self,
        *,
        chain_id: int,
        contract_address: str,
        mapping_name: str,
        block: int | None = None,
    ) -> dict[str, str]:
        """Public read for tests / introspection: return ``{key: value_hex}``
        of the latest assignment per key.
        """
        cid = self._resolve_contract_id(contract_address)
        if cid is None:
            return {}
        return self._latest_values(chain_id, cid, [mapping_name], block)

    def cursor_block(self, *, chain_id: int, contract_address: str) -> int | None:
        cid = self._resolve_contract_id(contract_address)
        if cid is None:
            return None
        row = self.session.execute(
            select(MappingValueCursor.last_indexed_block)
            .where(MappingValueCursor.chain_id == chain_id)
            .where(MappingValueCursor.contract_id == cid)
        ).first()
        return row[0] if row else None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _latest_values(
        self,
        chain_id: int,
        contract_id: int,
        mapping_names: list[str],
        block: int | None,
    ) -> dict[str, str]:
        """Walk events in ``(block_number, log_index)`` order and
        keep the most recent value per key, scoped to the supplied
        mapping names. Cheaper than a window function for the small
        per-mapping cardinalities we expect (10s-100s of keys).
        """
        # Order by ``(block_number, transaction_index, log_index)``:
        # within a block, events fire in tx-index order then per-tx
        # log-index order. Two assignments to the same key in the
        # same tx (rare but legal — e.g. a multicall that sets twice)
        # need the per-tx position to break the tie correctly. Codex
        # review surfaced ``transaction_index`` as a stored-but-unused
        # column; it's now part of the canonical ordering.
        q = (
            select(
                MappingValueEvent.key_hex,
                MappingValueEvent.value_hex,
                MappingValueEvent.block_number,
                MappingValueEvent.transaction_index,
                MappingValueEvent.log_index,
            )
            .where(MappingValueEvent.chain_id == chain_id)
            .where(MappingValueEvent.contract_id == contract_id)
            .where(MappingValueEvent.mapping_name.in_(mapping_names))
            .order_by(
                MappingValueEvent.block_number.asc(),
                MappingValueEvent.transaction_index.asc(),
                MappingValueEvent.log_index.asc(),
            )
        )
        if block is not None:
            q = q.where(MappingValueEvent.block_number <= block)
        latest: dict[str, str] = {}
        for key_hex, value_hex, _block, _tx_idx, _log in self.session.execute(q).all():
            latest[key_hex.lower()] = value_hex
        return latest

    def _resolve_contract_id(self, contract_address: str) -> int | None:
        addr_lower = contract_address.lower()
        row = self.session.execute(select(Contract.id).where(Contract.address.ilike(addr_lower)).limit(1)).first()
        return row[0] if row else None
