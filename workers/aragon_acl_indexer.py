"""Indexer for Aragon ACL ``SetPermission`` events.

Mirrors the algorithm shape of ``workers/role_grants_indexer.py``
but writes ``aragon_acl_events`` + ``aragon_acl_cursors``. Aragon
permissions are ``(entity, app, role, allowed)`` tuples — the same
``SetPermission`` event toggles ``allowed`` on grant vs revoke
(no separate revoke topic). Decoded into ``FetchedAragonLog`` and
upserted with ON CONFLICT DO NOTHING for idempotent re-scan.

Per-(chain_id, acl_contract_id):

  1. Acquire ``pg_advisory_xact_lock(chain_hash, acl_contract_id)``
     so concurrent worker replicas serialize.
  2. Read the cursor (last_indexed_block + last_indexed_block_hash).
  3. Reorg detect: live block-hash at last_indexed_block vs the
     stored hash. On mismatch rewind ``finality_depth`` blocks
     by deleting events past ``rewind_to`` and rolling the cursor
     back.
  4. Fetch logs from ``cursor + 1`` through
     ``head - finality_depth`` (the finality cap excludes
     unfinalized blocks).
  5. Bulk insert with ON CONFLICT DO NOTHING.
  6. Advance the cursor + record the block hash AT the new
     finalized head (NOT the last log's hash — same codex-flagged
     fix the role_grants indexer carries).

Caveat (same as role_grants): rewind only handles reorgs ≤
``finality_depth``. Deeper reorgs would silently leak stale
events. The seeded ``chain_finality_config`` depths (mainnet=12,
polygon=128, ...) are conservative for the supported chains.
A common-ancestor walk would handle arbitrary depth and is a
future shared improvement across both indexers.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Iterable, Protocol

from sqlalchemy import and_, delete, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from db.models import AragonAclCursor, AragonAclEvent

logger = logging.getLogger(__name__)


# SetPermission(address indexed entity, address indexed app, bytes32 indexed role, bool allowed)
# topic0 = keccak256("SetPermission(address,address,bytes32,bool)")
SET_PERMISSION_TOPIC0 = bytes.fromhex(
    "80f1d1bdcdef74de9d34a2cf3a5b5cb56d40b6cc20cffd1bd328eaa6f5a96ed3"
)


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchedAragonLog:
    """Decoded Aragon ``SetPermission`` event.

    The ``allowed`` flag carries the bool from the event's data
    region — flipping ``true``→``false`` is how Aragon revokes a
    permission (no separate revoke topic).
    """

    block_number: int
    block_hash: bytes
    tx_hash: bytes
    log_index: int
    transaction_index: int
    entity: str  # 0x-lowercased
    app: str  # 0x-lowercased
    role: bytes
    allowed: bool


class LogFetcher(Protocol):
    """Abstracts ``eth_getLogs`` for SetPermission events. The
    fetcher decodes the indexed topics + data region and yields
    ``FetchedAragonLog``."""

    def fetch_logs(
        self,
        *,
        chain_id: int,
        contract_address: str,
        from_block: int,
        to_block: int,
    ) -> list[FetchedAragonLog]: ...


class BlockHashFetcher(Protocol):
    """Returns the canonical block hash at a given height. Used
    for reorg detection."""

    def block_hash(self, *, chain_id: int, block_number: int) -> bytes | None: ...


# ---------------------------------------------------------------------------
# Step
# ---------------------------------------------------------------------------


@dataclass
class IndexResult:
    inserted: int
    rewound: bool
    new_cursor: int
    new_cursor_hash: bytes | None


def index_aragon_acl_step(
    session: Session,
    *,
    chain_id: int,
    acl_contract_id: int,
    acl_address: str,
    head_block: int,
    log_fetcher: LogFetcher,
    block_hash_fetcher: BlockHashFetcher,
    finality_depth: int,
    batch_size: int = 5_000,
    use_advisory_lock: bool = True,
) -> IndexResult:
    """One indexing pass for ``(chain_id, acl_contract_id)``."""
    if use_advisory_lock:
        session.execute(
            text("SELECT pg_advisory_xact_lock(:k1, :k2)"),
            {"k1": _lock_key(chain_id), "k2": acl_contract_id},
        )

    finalized_head = max(head_block - finality_depth, 0)
    cursor_row = session.execute(
        select(AragonAclCursor)
        .where(AragonAclCursor.chain_id == chain_id)
        .where(AragonAclCursor.acl_contract_id == acl_contract_id)
        .with_for_update()
    ).scalar_one_or_none()

    last_indexed_block = cursor_row.last_indexed_block if cursor_row else 0
    rewound = False

    if cursor_row is not None and cursor_row.last_indexed_block_hash and last_indexed_block > 0:
        live_hash = block_hash_fetcher.block_hash(
            chain_id=chain_id, block_number=last_indexed_block
        )
        if live_hash is not None and live_hash != cursor_row.last_indexed_block_hash:
            rewind_to = max(last_indexed_block - finality_depth, 0)
            session.execute(
                delete(AragonAclEvent).where(
                    and_(
                        AragonAclEvent.chain_id == chain_id,
                        AragonAclEvent.acl_contract_id == acl_contract_id,
                        AragonAclEvent.block_number > rewind_to,
                    )
                )
            )
            last_indexed_block = rewind_to
            rewound = True

    if last_indexed_block >= finalized_head:
        new_hash = (
            block_hash_fetcher.block_hash(chain_id=chain_id, block_number=last_indexed_block)
            if last_indexed_block > 0
            else None
        )
        _upsert_cursor(session, chain_id, acl_contract_id, last_indexed_block, new_hash)
        return IndexResult(
            inserted=0,
            rewound=rewound,
            new_cursor=last_indexed_block,
            new_cursor_hash=new_hash,
        )

    inserted = 0
    cursor = last_indexed_block + 1
    target = finalized_head
    while cursor <= target:
        chunk_end = min(cursor + batch_size - 1, target)
        logs = log_fetcher.fetch_logs(
            chain_id=chain_id,
            contract_address=acl_address,
            from_block=cursor,
            to_block=chunk_end,
        )
        if logs:
            inserted += _bulk_insert_logs(
                session, chain_id=chain_id, acl_contract_id=acl_contract_id, logs=logs
            )
        cursor = chunk_end + 1

    # Hash AT the cursor block, NOT the last event's hash — same
    # codex-flagged bug the role_grants indexer carries fix for:
    # using logs[-1].block_hash falsely triggers a reorg-rewind
    # next pass when the finalized head has no SetPermission events.
    new_cursor_block = target
    new_cursor_hash = block_hash_fetcher.block_hash(
        chain_id=chain_id, block_number=new_cursor_block
    )
    _upsert_cursor(session, chain_id, acl_contract_id, new_cursor_block, new_cursor_hash)
    return IndexResult(
        inserted=inserted,
        rewound=rewound,
        new_cursor=new_cursor_block,
        new_cursor_hash=new_cursor_hash,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bulk_insert_logs(
    session: Session,
    *,
    chain_id: int,
    acl_contract_id: int,
    logs: Iterable[FetchedAragonLog],
) -> int:
    rows = [
        {
            "chain_id": chain_id,
            "acl_contract_id": acl_contract_id,
            "tx_hash": log.tx_hash,
            "log_index": log.log_index,
            "app": log.app.lower(),
            "role": log.role,
            "entity": log.entity.lower(),
            "allowed": bool(log.allowed),
            "block_number": log.block_number,
            "block_hash": log.block_hash,
            "transaction_index": log.transaction_index,
        }
        for log in logs
    ]
    if not rows:
        return 0
    stmt = pg_insert(AragonAclEvent).values(rows).on_conflict_do_nothing(
        index_elements=["chain_id", "acl_contract_id", "tx_hash", "log_index"]
    )
    result = session.execute(stmt)
    return result.rowcount or 0


def _upsert_cursor(
    session: Session,
    chain_id: int,
    acl_contract_id: int,
    block_number: int,
    block_hash: bytes | None,
) -> None:
    stmt = pg_insert(AragonAclCursor).values(
        chain_id=chain_id,
        acl_contract_id=acl_contract_id,
        last_indexed_block=block_number,
        last_indexed_block_hash=block_hash,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["chain_id", "acl_contract_id"],
        set_={
            "last_indexed_block": stmt.excluded.last_indexed_block,
            "last_indexed_block_hash": stmt.excluded.last_indexed_block_hash,
            "last_run_at": text("now()"),
        },
    )
    session.execute(stmt)


def _lock_key(chain_id: int) -> int:
    return int(hashlib.sha1(str(chain_id).encode()).hexdigest()[:7], 16) & 0x7FFFFFFF


def enroll_acl_contract(
    session: Session, *, chain_id: int, acl_contract_id: int
) -> None:
    """Idempotently insert an ``aragon_acl_cursors`` row at
    block 0. Caller commits."""
    stmt = pg_insert(AragonAclCursor).values(
        chain_id=chain_id,
        acl_contract_id=acl_contract_id,
        last_indexed_block=0,
    ).on_conflict_do_nothing(index_elements=["chain_id", "acl_contract_id"])
    session.execute(stmt)
