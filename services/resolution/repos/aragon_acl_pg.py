"""Postgres-backed ``AragonACLRepo``.

Reads ``aragon_acl_events`` (append-only log of Aragon
``SetPermission`` events) and ``aragon_acl_cursors`` (per-(chain_id,
acl_contract_id) progress marker). Implements the
``AragonACLRepo`` Protocol from
``services.resolution.adapters.aragon_acl`` so the AragonACLAdapter
can enumerate currently-allowed entities for a given ``(app, role)``
exactly.

Aragon stores permissions as ``(entity, app, role, allowed)`` —
the same `SetPermission` event flips ``allowed`` on grant vs revoke
(no separate revoke topic). Membership state is computed by
replaying events in ``(block_number, log_index)`` order and keeping
the latest ``allowed`` value per entity.

For role-domain expansion semantics that mirror the ``RoleGrantsRepo``
contract, this repo also:

  * sources ``last_indexed_block`` from ``aragon_acl_cursors`` so
    capabilities reflect the indexer's freshness even when the
    ACL has no recent events.
  * accepts ethereum/mainnet aliasing for legacy chain-name rows
    in the contracts table.
"""

from __future__ import annotations

from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import (
    AragonAclCursor,
    AragonAclEvent,
    ChainFinalityConfig,
    Contract,
)
from services.resolution.adapters import EnumerationResult


_FALLBACK_CHAIN_NAMES = {
    1: "ethereum",
    10: "optimism",
    137: "polygon",
    8453: "base",
    42161: "arbitrum",
    59144: "linea",
    534352: "scroll",
}

# Same ethereum/mainnet alias set used by role_grants_pg so a DB
# seeded with the v1 "mainnet" name still resolves contracts whose
# Contract.chain field uses the project-wide "ethereum" convention.
_CHAIN_NAME_ALIASES = {
    "ethereum": ("ethereum", "mainnet"),
    "polygon": ("polygon", "matic"),
}


class PostgresAragonACLRepo:
    """Reads aragon_acl_events through a SQLAlchemy Session."""

    def __init__(self, session: Session) -> None:
        self.session = session
        self._chain_name_cache: dict[int, str] | None = None

    def members_for_permission(
        self,
        *,
        chain_id: int,
        acl_address: str,
        target_app: str,
        role: bytes,
        block: int | None = None,
    ) -> EnumerationResult:
        cid = self._resolve_contract_id(chain_id, acl_address)
        if cid is None:
            return EnumerationResult(members=[], confidence="enumerable", last_indexed_block=None)
        members = self._replay(chain_id, cid, target_app, role, block)
        cursor_block = self._cursor_block(chain_id, cid)
        return EnumerationResult(
            members=sorted(members),
            confidence="enumerable",
            last_indexed_block=cursor_block,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _replay(
        self,
        chain_id: int,
        acl_contract_id: int,
        target_app: str,
        role: bytes,
        block: int | None,
    ) -> Iterable[str]:
        """Walk every (entity, allowed) event for ``(app, role)``
        in (block_number, log_index) order; the final ``allowed``
        flag per entity decides membership."""
        q = (
            select(
                AragonAclEvent.entity,
                AragonAclEvent.allowed,
            )
            .where(AragonAclEvent.chain_id == chain_id)
            .where(AragonAclEvent.acl_contract_id == acl_contract_id)
            .where(AragonAclEvent.app == target_app.lower())
            .where(AragonAclEvent.role == role)
            .order_by(
                AragonAclEvent.block_number.asc(),
                AragonAclEvent.log_index.asc(),
            )
        )
        if block is not None:
            q = q.where(AragonAclEvent.block_number <= block)
        state: dict[str, bool] = {}
        for entity, allowed in self.session.execute(q).all():
            state[entity.lower()] = bool(allowed)
        return [addr for addr, is_member in state.items() if is_member]

    def _cursor_block(self, chain_id: int, acl_contract_id: int) -> int | None:
        row = self.session.execute(
            select(AragonAclCursor.last_indexed_block)
            .where(AragonAclCursor.chain_id == chain_id)
            .where(AragonAclCursor.acl_contract_id == acl_contract_id)
        ).first()
        return row[0] if row else None

    def _resolve_contract_id(
        self, chain_id: int, contract_address: str
    ) -> int | None:
        chain_name = self._chain_name(chain_id)
        accepted_names = set(_CHAIN_NAME_ALIASES.get(chain_name, (chain_name,)))
        addr_lower = contract_address.lower()
        candidates = self.session.execute(
            select(Contract.id, Contract.chain).where(
                Contract.address.ilike(addr_lower)
            )
        ).all()
        for row_id, row_chain in candidates:
            if row_chain in accepted_names:
                return row_id
            if row_chain is None and chain_id == 1:
                return row_id
        return None

    def _chain_name(self, chain_id: int) -> str:
        if self._chain_name_cache is None:
            rows = self.session.execute(
                select(ChainFinalityConfig.chain_id, ChainFinalityConfig.name)
            ).all()
            self._chain_name_cache = {cid: name for cid, name in rows}
        return self._chain_name_cache.get(
            chain_id, _FALLBACK_CHAIN_NAMES.get(chain_id, "ethereum")
        )
