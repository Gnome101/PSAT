"""Indexer for OZ AccessControl ``RoleGranted`` / ``RoleRevoked``
events. Per-(chain_id, contract_id), the indexer:

  1. Acquires a Postgres transactional advisory lock on the
     contract_id so concurrent worker replicas serialize.
  2. Reads the cursor (last_indexed_block + last_indexed_block_hash).
  3. Detects reorg by comparing the cursor's stored hash against
     the current chain's block hash at last_indexed_block. On
     mismatch, rewinds ``finality_depth`` blocks by deleting events
     in the rewind window and rolling the cursor back.
  4. Fetches logs from ``cursor + 1`` through ``head - finality_depth``
     (the finality cap excludes blocks that could still reorg).
  5. Inserts via ``INSERT ... ON CONFLICT DO NOTHING`` so re-runs
     are idempotent against the same on-chain state.
  6. Advances the cursor to the new high-water-mark + records the
     block hash for the next reorg check.

The algorithmic core (``index_role_grants_step``) is a pure
function over a ``LogFetcher`` and ``BlockHashFetcher`` Protocol so
it can be unit-tested without an RPC. The RPC wiring is a thin
adapter layered on top.

Reorg-depth caveat (codex round on this): the rewind-by-
``finality_depth``-blocks approach is correct only when the reorg
depth is ≤ ``finality_depth``. Deeper reorgs would silently leave
stale events past the rewind window. The seeded confirmation
depths in ``chain_finality_config`` (e.g. mainnet=12, polygon=128)
are conservative against observed reorg depths on those chains;
operators MUST raise the depth for any chain where deeper reorgs
are realistic. A common-ancestor walk would handle arbitrary depth
but is not implemented here.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable, Protocol

from sqlalchemy import and_, delete, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from db.models import RoleGrantsCursor, RoleGrantsEvent

# RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)
# keccak256 of the canonical signature.
ROLE_GRANTED_TOPIC0 = bytes.fromhex(
    "2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d"
)
# RoleRevoked(bytes32 indexed role, address indexed account, address indexed sender)
ROLE_REVOKED_TOPIC0 = bytes.fromhex(
    "f6391f5c32d9c69d2a47ea670b442974b53935d1edc7fd64eb21e047a839171b"
)


# ---------------------------------------------------------------------------
# Fetchers (Protocols — RPC adapter wires real implementations)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchedLog:
    """Decoded RoleGranted / RoleRevoked event — the fetcher
    handles RPC + topic decoding and yields these to the indexer."""

    block_number: int
    block_hash: bytes
    tx_hash: bytes
    log_index: int
    transaction_index: int
    role: bytes
    member: str  # 0x-lowercased
    direction: str  # "grant" or "revoke"


class LogFetcher(Protocol):
    """Abstracts the RPC's ``eth_getLogs`` call. Implementations:

      * RPC-backed (production) — ``utils.eth_logs.RpcLogFetcher``
      * In-memory (tests) — fixture lists.
    """

    def fetch_logs(
        self,
        *,
        chain_id: int,
        contract_address: str,
        from_block: int,
        to_block: int,
    ) -> list[FetchedLog]: ...


class BlockHashFetcher(Protocol):
    """Returns the canonical block hash at a given height. Used
    for reorg detection — the indexer compares this against the
    cursor's stored ``last_indexed_block_hash``."""

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


def index_role_grants_step(
    session: Session,
    *,
    chain_id: int,
    contract_id: int,
    contract_address: str,
    head_block: int,
    log_fetcher: LogFetcher,
    block_hash_fetcher: BlockHashFetcher,
    finality_depth: int,
    batch_size: int = 5_000,
    use_advisory_lock: bool = True,
) -> IndexResult:
    """Run one indexing pass for ``(chain_id, contract_id)``.

    ``head_block`` is the current chain head; the indexer caps its
    scan at ``head_block - finality_depth`` so unfinalized blocks
    aren't recorded.
    """
    if use_advisory_lock:
        # Two-key advisory lock keyed on (chain_id, contract_id) so
        # different (chain_id, contract_id) combinations don't
        # contend even if their integer IDs collide.
        session.execute(
            text("SELECT pg_advisory_xact_lock(:k1, :k2)"),
            {"k1": _lock_key(chain_id), "k2": contract_id},
        )

    finalized_head = max(head_block - finality_depth, 0)
    cursor_row = session.execute(
        select(RoleGrantsCursor)
        .where(RoleGrantsCursor.chain_id == chain_id)
        .where(RoleGrantsCursor.contract_id == contract_id)
        .with_for_update()
    ).scalar_one_or_none()

    last_indexed_block = cursor_row.last_indexed_block if cursor_row else 0
    rewound = False

    # Reorg detection: if the cursor recorded a hash, compare it
    # against the current chain at that height.
    if cursor_row is not None and cursor_row.last_indexed_block_hash and last_indexed_block > 0:
        live_hash = block_hash_fetcher.block_hash(
            chain_id=chain_id, block_number=last_indexed_block
        )
        if live_hash is not None and live_hash != cursor_row.last_indexed_block_hash:
            rewind_to = max(last_indexed_block - finality_depth, 0)
            session.execute(
                delete(RoleGrantsEvent).where(
                    and_(
                        RoleGrantsEvent.chain_id == chain_id,
                        RoleGrantsEvent.contract_id == contract_id,
                        RoleGrantsEvent.block_number > rewind_to,
                    )
                )
            )
            last_indexed_block = rewind_to
            rewound = True

    if last_indexed_block >= finalized_head:
        # Nothing to do — already at the finalized tip.
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
        logs = log_fetcher.fetch_logs(
            chain_id=chain_id,
            contract_address=contract_address,
            from_block=cursor,
            to_block=chunk_end,
        )
        if logs:
            inserted += _bulk_insert_logs(
                session, chain_id=chain_id, contract_id=contract_id, logs=logs
            )
        cursor = chunk_end + 1

    # The cursor's hash MUST be the hash AT new_cursor_block, not the
    # hash of the last event we saw. If the finalized head has no
    # RoleGranted/Revoked event in this scan, the last event's hash
    # is for an earlier block — using it as the cursor's checkpoint
    # would falsely trigger a reorg-rewind on the next pass when we
    # compare against the live hash at new_cursor_block. (codex
    # review caught this.)
    new_cursor_block = target
    new_cursor_hash = block_hash_fetcher.block_hash(
        chain_id=chain_id, block_number=new_cursor_block
    )
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


def _bulk_insert_logs(
    session: Session,
    *,
    chain_id: int,
    contract_id: int,
    logs: Iterable[FetchedLog],
) -> int:
    rows = [
        {
            "chain_id": chain_id,
            "contract_id": contract_id,
            "tx_hash": log.tx_hash,
            "log_index": log.log_index,
            "role": log.role,
            "member": log.member.lower(),
            "direction": log.direction,
            "block_number": log.block_number,
            "block_hash": log.block_hash,
            "transaction_index": log.transaction_index,
        }
        for log in logs
    ]
    if not rows:
        return 0
    stmt = pg_insert(RoleGrantsEvent).values(rows).on_conflict_do_nothing(
        index_elements=["chain_id", "contract_id", "tx_hash", "log_index"]
    )
    result = session.execute(stmt)
    return result.rowcount or 0


def _upsert_cursor(
    session: Session,
    chain_id: int,
    contract_id: int,
    block_number: int,
    block_hash: bytes | None,
) -> None:
    stmt = pg_insert(RoleGrantsCursor).values(
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
    """Stable 32-bit integer derived from chain_id for use as the
    first key of ``pg_advisory_xact_lock(int, int)``. Hashing keeps
    chain_ids well-spread even though most are small integers."""
    return int(hashlib.sha1(str(chain_id).encode()).hexdigest()[:7], 16) & 0x7FFFFFFF


# ---------------------------------------------------------------------------
# Topic-set convenience
# ---------------------------------------------------------------------------


def event_direction(topic0: bytes) -> str | None:
    """Map a topic0 to ``'grant'`` / ``'revoke'`` / ``None``."""
    if topic0 == ROLE_GRANTED_TOPIC0:
        return "grant"
    if topic0 == ROLE_REVOKED_TOPIC0:
        return "revoke"
    return None
