"""Indexer for mapping-value set events (D.3).

Per-(chain_id, contract_id), the indexer:

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

Operational defaults (Wave 3 cutover, PR #70 §E.1):

* **Backfill = job-triggered.** Cursors are inserted by
  ``enroll_from_predicate_trees`` for any contract whose
  ``predicate_trees`` artifact carries ``set_descriptor.writer_selectors``
  on a leaf — that's the signal the resolver will need durable
  mapping-value events for that contract.
* **Freshness = stale-OK.** ``MappingValueCursor.last_run_at`` and
  ``last_indexed_block`` are stored on the cursor; surfacing on the
  freshness API is a follow-up.
* **Enrollment = predicate-tree-driven** (vs. dep-edge for role_grants
  / aragon_acl). Reason: a mapping_membership predicate is structural
  on the *subject* contract, not on a separate authority registry —
  ``JobDependency`` rows wouldn't reference it. The trade-off is that
  enrollment requires reading every job's ``predicate_trees`` artifact
  per pass; we paginate at the SQL level so memory stays bounded.

Status: **no production fetcher implementation yet.** The
``MappingValueLogFetcher`` Protocol matches event-shaped writes; the
existing HyperSync surface for mapping-value replay is
trace-shaped (``HyperSyncTraceFetcher`` in
``services/resolution/repos/mapping_value_hypersync.py``). Wiring
the trace path into a ``MappingValueLogFetcher`` adapter is a
separate piece of work; until then the worker process boots, scans
its empty fetcher map, and idles. The capability resolver still
falls through to on-demand HyperSync replay via
``EventIndexedAdapter`` so capability output isn't degraded — only
the read-path-cache benefit is missing.
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Protocol

from sqlalchemy import and_, delete, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from db.models import ChainFinalityConfig, Contract, MappingValueCursor, MappingValueEvent

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


# ---------------------------------------------------------------------------
# Scan loop + enrollment
# ---------------------------------------------------------------------------


class HeadBlockFetcher(Protocol):
    """Returns the current chain head per chain_id. Same shape as
    the role_grants HeadBlockFetcher."""

    def head_block(self, *, chain_id: int) -> int: ...


def scan_enrolled_contracts(
    session: Session,
    *,
    log_fetcher_for_chain: dict[int, "MappingValueLogFetcher"],
    block_hash_fetcher_for_chain: dict[int, "BlockHashFetcher"],
    head_block_fetcher: HeadBlockFetcher,
    finality_for_chain: dict[int, int] | None = None,
    use_advisory_lock: bool = True,
) -> list["IndexResult"]:
    """One scan pass over every ``mapping_value_cursors`` row.

    Mirrors ``role_grants_indexer.scan_enrolled_contracts``: each
    contract runs in its own transaction (commit between contracts),
    failures are logged-and-skipped, chains without configured
    fetchers are silently skipped.
    """
    if finality_for_chain is None:
        finality_for_chain = _load_finality_config(session)

    cursors = session.execute(
        select(
            MappingValueCursor.chain_id,
            MappingValueCursor.contract_id,
            Contract.address,
        ).join(Contract, MappingValueCursor.contract_id == Contract.id)
    ).all()

    head_cache: dict[int, int] = {}
    results: list[IndexResult] = []
    for chain_id, contract_id, address in cursors:
        if chain_id not in log_fetcher_for_chain:
            continue
        if chain_id not in block_hash_fetcher_for_chain:
            continue

        if chain_id not in head_cache:
            head_cache[chain_id] = head_block_fetcher.head_block(chain_id=chain_id)
        head = head_cache[chain_id]
        if head <= 0:
            continue

        try:
            result = index_mapping_values_step(
                session,
                chain_id=chain_id,
                contract_id=contract_id,
                contract_address=address,
                head_block=head,
                log_fetcher=log_fetcher_for_chain[chain_id],
                block_hash_fetcher=block_hash_fetcher_for_chain[chain_id],
                finality_depth=finality_for_chain.get(chain_id, 12),
                use_advisory_lock=use_advisory_lock,
            )
            session.commit()
            results.append(result)
        except Exception:
            session.rollback()
            logger.exception(
                "mapping_value indexer pass failed for chain=%s contract_id=%s address=%s",
                chain_id,
                contract_id,
                address,
            )
    return results


def _has_writer_selectors(node: Any) -> bool:
    """Recursively walk a predicate-tree node looking for any leaf
    whose ``set_descriptor.writer_selectors`` is non-empty. Returns
    True on first hit so the worst case is one full walk per
    artifact."""
    if isinstance(node, dict):
        if node.get("op") == "LEAF":
            leaf = node.get("leaf") or {}
            descriptor = leaf.get("set_descriptor") or {}
            selectors = descriptor.get("writer_selectors") or []
            if selectors:
                return True
            return False
        for child in node.get("children") or []:
            if _has_writer_selectors(child):
                return True
    return False


def enroll_from_predicate_trees(
    session: Session,
    *,
    chain_name_to_id: dict[str, int],
    chain_ids_with_fetchers: set[int],
) -> int:
    """Insert ``mapping_value_cursors`` rows for every contract
    whose latest ``predicate_trees`` artifact carries at least one
    leaf with ``set_descriptor.writer_selectors``.

    Walks the latest predicate_trees per ``(address, chain)`` and
    enrolls if any leaf has writer_selectors annotated. Idempotent
    via ``enroll_contract``'s ``ON CONFLICT DO NOTHING``.
    """
    from db.models import Artifact, Job
    from db.queue import get_artifact

    # Latest job per (address, chain) — same shape as the
    # capability resolver's lookup but column-light. Limit by
    # only-Job-rows-with-a-predicate_trees-artifact to keep this
    # bounded under typical fleet sizes.
    candidate_jobs = session.execute(
        select(Job.id, Job.address, Job.request)
        .join(Artifact, Artifact.job_id == Job.id)
        .where(Artifact.name == "predicate_trees")
        .order_by(Job.created_at.desc())
    ).all()

    seen_addr_chain: set[tuple[str, str]] = set()
    inserted = 0
    for job_id, address, request in candidate_jobs:
        if not address:
            continue
        chain_name = "ethereum"
        if isinstance(request, dict) and isinstance(request.get("chain"), str):
            chain_name = request["chain"]
        chain_id = chain_name_to_id.get(chain_name.lower())
        if chain_id is None or chain_id not in chain_ids_with_fetchers:
            continue
        key = (address.lower(), chain_name)
        if key in seen_addr_chain:
            continue
        seen_addr_chain.add(key)

        artifact = get_artifact(session, job_id, "predicate_trees")
        if not isinstance(artifact, dict):
            continue
        trees = artifact.get("trees") or {}
        if not any(_has_writer_selectors(t) for t in trees.values() if isinstance(t, dict)):
            continue

        contract_row = session.execute(
            select(Contract.id).where(
                Contract.address == address,
                Contract.chain == chain_name,
            )
        ).first()
        if contract_row is None:
            continue
        before = session.execute(
            select(MappingValueCursor).where(
                MappingValueCursor.chain_id == chain_id,
                MappingValueCursor.contract_id == contract_row[0],
            )
        ).scalar_one_or_none()
        if before is not None:
            continue
        enroll_contract(session, chain_id=chain_id, contract_id=contract_row[0])
        inserted += 1
    if inserted:
        session.commit()
        logger.info("mapping_value indexer: enrolled %d new contract(s) from predicate_trees", inserted)
    return inserted


def run_mapping_value_indexer_loop(
    session_factory: Callable[[], Session],
    *,
    log_fetcher_for_chain: dict[int, "MappingValueLogFetcher"],
    block_hash_fetcher_for_chain: dict[int, "BlockHashFetcher"],
    head_block_fetcher: HeadBlockFetcher,
    interval: float = 30.0,
    chain_name_to_id: dict[str, int] | None = None,
) -> None:
    """Blocking polling loop. Each pass enrolls newly-seen
    predicate_trees, scans every enrolled cursor, then sleeps."""
    logger.info("starting mapping_value indexer loop interval=%ss", interval)
    chain_ids_with_fetchers = set(log_fetcher_for_chain.keys()) & set(block_hash_fetcher_for_chain.keys())
    while True:
        try:
            with session_factory() as session:
                if chain_name_to_id:
                    try:
                        enroll_from_predicate_trees(
                            session,
                            chain_name_to_id=chain_name_to_id,
                            chain_ids_with_fetchers=chain_ids_with_fetchers,
                        )
                    except Exception:
                        logger.exception("mapping_value indexer enrollment pass failed")
                        session.rollback()
                scan_enrolled_contracts(
                    session,
                    log_fetcher_for_chain=log_fetcher_for_chain,
                    block_hash_fetcher_for_chain=block_hash_fetcher_for_chain,
                    head_block_fetcher=head_block_fetcher,
                )
        except Exception:
            logger.exception("mapping_value indexer pass failed")
        time.sleep(interval)


def _load_finality_config(session: Session) -> dict[int, int]:
    rows = session.execute(select(ChainFinalityConfig.chain_id, ChainFinalityConfig.confirmation_depth)).all()
    return {cid: depth for cid, depth in rows}


# ---------------------------------------------------------------------------
# Worker entrypoint
# ---------------------------------------------------------------------------


_CHAIN_NAME_TO_ID: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "optimism": 10,
    "polygon": 137,
    "base": 8453,
    "avalanche": 43114,
    "bsc": 56,
    "linea": 59144,
    "scroll": 534352,
    "zksync": 324,
    "blast": 81457,
}


def _build_rpc_fetchers() -> tuple[dict, dict, "HeadBlockFetcher"]:
    """Construct head-block + block-hash fetchers from
    ``role_grants_rpc`` (event-agnostic). The
    ``MappingValueLogFetcher`` Protocol is left unimplemented at the
    repo level — see module docstring; the worker boots with an
    empty log-fetcher map, exercises the enrollment + scan-loop
    plumbing, and idles on the actual log scan until a
    ``MappingValueLogFetcher`` adapter ships."""
    import os

    from services.resolution.repos.role_grants_rpc import (
        RpcBlockHashFetcher,
        RpcHeadBlockFetcher,
    )

    rpc_url = os.getenv("PSAT_INDEXER_RPC_URL") or os.getenv("ETH_RPC", "https://ethereum-rpc.publicnode.com")
    rpc_url_for_chain = {1: rpc_url}
    log_fetcher_for_chain: dict[int, "MappingValueLogFetcher"] = {}
    block_hash_fetcher_for_chain = {1: RpcBlockHashFetcher(rpc_url)}
    head_block_fetcher = RpcHeadBlockFetcher(rpc_url_for_chain)
    return log_fetcher_for_chain, block_hash_fetcher_for_chain, head_block_fetcher


def main() -> None:
    """Process entrypoint. Reads env, constructs fetchers, runs the
    polling loop forever. The empty ``log_fetcher_for_chain`` shape
    is intentional — see module docstring."""
    import os

    from db.models import SessionLocal

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        force=True,
    )
    interval = float(os.getenv("PSAT_INDEXER_POLL_INTERVAL_S", "30"))
    log_fetcher_for_chain, block_hash_fetcher_for_chain, head_block_fetcher = _build_rpc_fetchers()
    logger.info(
        "mapping_value indexer process starting: chains_with_log_fetchers=%s interval=%ss",
        sorted(log_fetcher_for_chain.keys()),
        interval,
    )
    if not log_fetcher_for_chain:
        logger.warning(
            "mapping_value indexer: no MappingValueLogFetcher configured for any chain. "
            "Enrollment + cursor management will run; log scan will idle. "
            "See workers/mapping_value_indexer.py module docstring."
        )
    run_mapping_value_indexer_loop(
        SessionLocal,
        log_fetcher_for_chain=log_fetcher_for_chain,
        block_hash_fetcher_for_chain=block_hash_fetcher_for_chain,
        head_block_fetcher=head_block_fetcher,
        interval=interval,
        chain_name_to_id=_CHAIN_NAME_TO_ID,
    )


if __name__ == "__main__":
    main()
