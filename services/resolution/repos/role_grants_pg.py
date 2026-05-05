"""Postgres-backed RoleGrantsRepo.

Reads ``role_grants_events`` (append-only log of RoleGranted /
RoleRevoked) and ``role_grants_cursors`` (the indexer's progress
marker per ``(chain_id, contract_id)``). Implements the
``RoleGrantsRepo`` Protocol from
``services.resolution.adapters.__init__`` so the AccessControlAdapter
can enumerate role members exactly.

For role-domain expansion the AccessControlAdapter also calls
``list_observed_roles`` and ``get_role_admin``:

  * ``list_observed_roles`` returns every role bytes32 ever seen in
    a grant/revoke event for the contract — used to seed the role
    domain for parametric ``hasRole(role, msg.sender)`` reads.
  * ``get_role_admin`` returns ``None`` until the indexer is
    extended to also persist ``RoleAdminChanged`` events. The AC
    adapter falls back to DEFAULT_ADMIN_ROLE in that case, which is
    correct for OZ-style contracts that never call ``_setRoleAdmin``.

Membership state is computed by replaying events in
``(block_number, log_index)`` order and keeping the latest
direction per ``(role, member)``.
"""

from __future__ import annotations

from typing import Iterable

from sqlalchemy import distinct, select
from sqlalchemy.orm import Session

from db.models import ChainFinalityConfig, Contract, RoleGrantsCursor, RoleGrantsEvent
from services.resolution.adapters import EnumerationResult, Trit

# Hardcoded fallback for chain_id → chain_name when the
# chain_finality_config table is empty (development DB without the
# seed migration). The migration seeds the full set; in production
# this dict is unused.
_FALLBACK_CHAIN_NAMES = {
    1: "ethereum",
    10: "optimism",
    137: "polygon",
    8453: "base",
    42161: "arbitrum",
    59144: "linea",
    534352: "scroll",
}

# Some legacy code stores chain_id=1 contracts with chain="mainnet"
# instead of "ethereum" — the chain_finality_config used "mainnet"
# in v1 of the seed. Accept both at lookup time so re-seeded DBs
# don't silently drop their AC enumeration.
_CHAIN_NAME_ALIASES = {
    "ethereum": ("ethereum", "mainnet"),
    "polygon": ("polygon", "matic"),
}


class PostgresRoleGrantsRepo:
    """Reads role_grants_events through a SQLAlchemy Session.

    ``session`` should be a request-scoped Session; the repo does
    not own the lifecycle. Queries are read-only — the indexer is
    the sole writer.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self._chain_name_cache: dict[int, str] | None = None

    def members_for_role(
        self,
        *,
        chain_id: int,
        contract_address: str,
        role: bytes,
        block: int | None = None,
    ) -> EnumerationResult:
        cid = self._resolve_contract_id(chain_id, contract_address)
        if cid is None:
            # Unknown contract — treat as enumerable empty rather
            # than partial; the indexer would have created a row
            # before any events were processed.
            return EnumerationResult(members=[], confidence="enumerable", last_indexed_block=None)
        members = self._replay_to_set(chain_id, cid, role, block)
        cursor_block = self._cursor_block(chain_id, cid)
        return EnumerationResult(
            members=sorted(members),
            confidence="enumerable",
            last_indexed_block=cursor_block,
        )

    def has_member(
        self,
        *,
        chain_id: int,
        contract_address: str,
        role: bytes,
        member: str,
    ) -> Trit:
        cid = self._resolve_contract_id(chain_id, contract_address)
        if cid is None:
            return Trit.UNKNOWN
        # Pull only this member's events — small set, replay in order.
        rows = self.session.execute(
            select(
                RoleGrantsEvent.direction,
                RoleGrantsEvent.block_number,
                RoleGrantsEvent.log_index,
            )
            .where(RoleGrantsEvent.chain_id == chain_id)
            .where(RoleGrantsEvent.contract_id == cid)
            .where(RoleGrantsEvent.role == role)
            .where(RoleGrantsEvent.member == member.lower())
            .order_by(
                RoleGrantsEvent.block_number.asc(),
                RoleGrantsEvent.log_index.asc(),
            )
        ).all()
        if not rows:
            return Trit.NO
        # Last event wins.
        return Trit.YES if rows[-1][0] == "grant" else Trit.NO

    def list_observed_roles(self, *, chain_id: int, contract_address: str) -> list[bytes]:
        cid = self._resolve_contract_id(chain_id, contract_address)
        if cid is None:
            return []
        rows = self.session.execute(
            select(distinct(RoleGrantsEvent.role))
            .where(RoleGrantsEvent.chain_id == chain_id)
            .where(RoleGrantsEvent.contract_id == cid)
        ).all()
        return [r[0] for r in rows]

    def get_role_admin(
        self,
        *,
        chain_id: int,
        contract_address: str,
        role: bytes,
        block: int | None = None,
    ) -> bytes | None:
        # Not yet indexed — RoleAdminChanged events are a follow-up
        # extension to the indexer. The AC adapter falls back to
        # DEFAULT_ADMIN_ROLE, which is the OZ default until
        # ``_setRoleAdmin`` is invoked. Returning None here is the
        # documented "I don't know" answer.
        return None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _replay_to_set(self, chain_id: int, contract_id: int, role: bytes, block: int | None) -> Iterable[str]:
        """Replay (member, direction) events in (block, log_index)
        order; the final direction per member determines membership."""
        q = (
            select(
                RoleGrantsEvent.member,
                RoleGrantsEvent.direction,
            )
            .where(RoleGrantsEvent.chain_id == chain_id)
            .where(RoleGrantsEvent.contract_id == contract_id)
            .where(RoleGrantsEvent.role == role)
            .order_by(
                RoleGrantsEvent.block_number.asc(),
                RoleGrantsEvent.log_index.asc(),
            )
        )
        if block is not None:
            q = q.where(RoleGrantsEvent.block_number <= block)
        state: dict[str, bool] = {}
        for member, direction in self.session.execute(q).all():
            state[member.lower()] = direction == "grant"
        return [addr for addr, has_role in state.items() if has_role]

    def _cursor_block(self, chain_id: int, contract_id: int) -> int | None:
        row = self.session.execute(
            select(RoleGrantsCursor.last_indexed_block)
            .where(RoleGrantsCursor.chain_id == chain_id)
            .where(RoleGrantsCursor.contract_id == contract_id)
        ).first()
        return row[0] if row else None

    def _resolve_contract_id(self, chain_id: int, contract_address: str) -> int | None:
        chain_name = self._chain_name(chain_id)
        # Match address case-insensitively (canonical addresses in
        # the contracts table are mixed-case checksummed; events are
        # lowercased by the indexer).
        # NOTE: Contract.chain is nullable — some legacy rows have
        # NULL. We accept both NULL and the resolved chain_name when
        # chain_id == 1 to keep ethereum-mainnet contracts mapped.
        addr_lower = contract_address.lower()
        accepted_names = set(_CHAIN_NAME_ALIASES.get(chain_name, (chain_name,)))
        candidates = self.session.execute(
            select(Contract.id, Contract.chain).where(Contract.address.ilike(addr_lower))
        ).all()
        for row_id, row_chain in candidates:
            if row_chain in accepted_names:
                return row_id
            if row_chain is None and chain_id == 1:
                return row_id
        return None

    def _chain_name(self, chain_id: int) -> str:
        if self._chain_name_cache is None:
            rows = self.session.execute(select(ChainFinalityConfig.chain_id, ChainFinalityConfig.name)).all()
            self._chain_name_cache = {cid: name for cid, name in rows}
        return self._chain_name_cache.get(chain_id, _FALLBACK_CHAIN_NAMES.get(chain_id, "ethereum"))
