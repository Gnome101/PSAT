"""Indexer for mapping-value set events (D.3).

Mirrors ``workers/role_grants_indexer.py`` exactly: per-(chain_id,
contract_id), the indexer:

  1. Acquires a Postgres transactional advisory lock on
     ``(chain_id, contract_id)`` so concurrent worker replicas
     serialize their work on the same contract.
  2. Reads the cursor (``last_indexed_block`` +
     ``last_indexed_block_hash``).
  3. Detects reorg by comparing the cursor's stored hash against the
     current chain's hash at ``last_indexed_block``. On mismatch,
     rewinds ``finality_depth`` blocks by deleting events in the
     rewind window and rolling the cursor back.
  4. Fetches set-events from ``cursor + 1`` through
     ``head - finality_depth`` (the finality cap excludes blocks that
     could still reorg).
  5. Inserts via ``INSERT ... ON CONFLICT DO NOTHING`` so re-runs are
     idempotent against the same on-chain state.
  6. Advances the cursor to the new high-water-mark + records the
     block hash for the next reorg check.

The fetcher is a Protocol — production wiring is a thin RPC /
HyperSync adapter; tests inject a static list. Same reorg-depth
caveat as ``role_grants_indexer``: rewind-by-finality-depth is correct
only when reorg depth ≤ finality_depth.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Iterable, Protocol

from sqlalchemy import and_, delete, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from db.models import MappingValueCursor, MappingValueEvent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fetcher Protocols
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchedMappingWrite:
    """One decoded set-event. The fetcher handles RPC + topic0
    matching + per-spec key/value extraction; the indexer just
    persists."""

    block_number: int
    block_hash: bytes
    tx_hash: bytes
    log_index: int
    transaction_index: int
    mapping_name: str
    key_hex: str  # 0x-lowercased canonical hex (20 bytes for address keys)
    value_hex: str  # 0x-lowercased 32-byte word


class MappingValueLogFetcher(Protocol):
    """RPC abstraction for the mapping-value indexer. Production
    impl decodes ``WriterEventSpec`` against eth_getLogs results;
    tests provide static lists."""

    def fetch_writes(
        self,
        *,
        chain_id: int,
        contract_address: str,
        from_block: int,
        to_block: int,
    ) -> list[FetchedMappingWrite]: ...


class BlockHashFetcher(Protocol):
    """Returns the canonical block hash at a given height. Used for
    reorg detection."""

    def block_hash(self, *, chain_id: int, block_number: int) -> bytes | None: ...


# ---------------------------------------------------------------------------
# Indexer step (pure-ish over fetchers + Session)
# ---------------------------------------------------------------------------


@dataclass
class IndexResult:
    inserted: int
    rewound: bool
    new_cursor: int
    new_cursor_hash: bytes | None


def index_mapping_values_step(
    session: Session,
    *,
    chain_id: int,
    contract_id: int,
    contract_address: str,
    head_block: int,
    log_fetcher: MappingValueLogFetcher,
    block_hash_fetcher: BlockHashFetcher,
    finality_depth: int,
    batch_size: int = 5_000,
    use_advisory_lock: bool = True,
) -> IndexResult:
    """Run one indexing pass for ``(chain_id, contract_id)``.

    Mirrors ``index_role_grants_step``. The advisory lock key uses a
    different namespace seed than the role-grants indexer so the two
    indexers don't contend on the same lock when scanning the same
    contract.
    """
    if use_advisory_lock:
        session.execute(
            text("SELECT pg_advisory_xact_lock(:k1, :k2)"),
            {"k1": _lock_key(chain_id), "k2": contract_id},
        )

    finalized_head = max(head_block - finality_depth, 0)
    cursor_row = session.execute(
        select(MappingValueCursor)
        .where(MappingValueCursor.chain_id == chain_id)
        .where(MappingValueCursor.contract_id == contract_id)
        .with_for_update()
    ).scalar_one_or_none()

    last_indexed_block = cursor_row.last_indexed_block if cursor_row else 0
    rewound = False

    if cursor_row is not None and cursor_row.last_indexed_block_hash and last_indexed_block > 0:
        live_hash = block_hash_fetcher.block_hash(chain_id=chain_id, block_number=last_indexed_block)
        if live_hash is not None and live_hash != cursor_row.last_indexed_block_hash:
            rewind_to = max(last_indexed_block - finality_depth, 0)
            session.execute(
                delete(MappingValueEvent).where(
                    and_(
                        MappingValueEvent.chain_id == chain_id,
                        MappingValueEvent.contract_id == contract_id,
                        MappingValueEvent.block_number > rewind_to,
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
        _upsert_cursor(session, chain_id, contract_id, last_indexed_block, new_hash)
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
        writes = log_fetcher.fetch_writes(
            chain_id=chain_id,
            contract_address=contract_address,
            from_block=cursor,
            to_block=chunk_end,
        )
        if writes:
            inserted += _bulk_insert_writes(session, chain_id=chain_id, contract_id=contract_id, writes=writes)
        cursor = chunk_end + 1

    new_cursor_block = target
    new_cursor_hash = block_hash_fetcher.block_hash(chain_id=chain_id, block_number=new_cursor_block)
    _upsert_cursor(session, chain_id, contract_id, new_cursor_block, new_cursor_hash)
    return IndexResult(
        inserted=inserted,
        rewound=rewound,
        new_cursor=new_cursor_block,
        new_cursor_hash=new_cursor_hash,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bulk_insert_writes(
    session: Session,
    *,
    chain_id: int,
    contract_id: int,
    writes: Iterable[FetchedMappingWrite],
) -> int:
    rows = [
        {
            "chain_id": chain_id,
            "contract_id": contract_id,
            "mapping_name": w.mapping_name,
            "tx_hash": w.tx_hash,
            "log_index": w.log_index,
            "key_hex": w.key_hex.lower(),
            "value_hex": w.value_hex.lower(),
            "block_number": w.block_number,
            "block_hash": w.block_hash,
            "transaction_index": w.transaction_index,
        }
        for w in writes
    ]
    if not rows:
        return 0
    stmt = (
        pg_insert(MappingValueEvent)
        .values(rows)
        .on_conflict_do_nothing(index_elements=["chain_id", "contract_id", "mapping_name", "tx_hash", "log_index"])
    )
    result = session.execute(stmt)
    return getattr(result, "rowcount", 0) or 0


def _upsert_cursor(
    session: Session,
    chain_id: int,
    contract_id: int,
    block_number: int,
    block_hash: bytes | None,
) -> None:
    stmt = pg_insert(MappingValueCursor).values(
        chain_id=chain_id,
        contract_id=contract_id,
        last_indexed_block=block_number,
        last_indexed_block_hash=block_hash,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["chain_id", "contract_id"],
        set_={
            "last_indexed_block": stmt.excluded.last_indexed_block,
            "last_indexed_block_hash": stmt.excluded.last_indexed_block_hash,
            "last_run_at": text("now()"),
        },
    )
    session.execute(stmt)


def _lock_key(chain_id: int) -> int:
    """Stable 32-bit integer derived from chain_id with a different
    namespace seed than the role-grants indexer's, so the two
    indexers don't contend on the same advisory lock when scanning
    the same contract concurrently.
    """
    return int(hashlib.sha1(("mvi:" + str(chain_id)).encode()).hexdigest()[:7], 16) & 0x7FFFFFFF


def enroll_contract(session: Session, *, chain_id: int, contract_id: int) -> None:
    """Idempotently insert a ``mapping_value_cursors`` row with
    ``last_indexed_block=0``. The next scan pass picks it up and
    backfills from genesis. Caller commits."""
    stmt = (
        pg_insert(MappingValueCursor)
        .values(
            chain_id=chain_id,
            contract_id=contract_id,
            last_indexed_block=0,
        )
        .on_conflict_do_nothing(index_elements=["chain_id", "contract_id"])
    )
    session.execute(stmt)
