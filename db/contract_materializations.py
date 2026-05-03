"""Cross-job, cross-process materialization cache.

A row per ``(chain, bytecode_keccak)`` holding the static analysis +
tracking-plan bundle so two impl jobs requesting the same contract pay
the expensive forge+Slither cost exactly once. Concurrent requests are
serialized via ``pg_advisory_xact_lock(hashtext(chain || ':' || keccak))``:
the lock winner runs the builder; the loser blocks on the lock, finds
``status='ready'`` on its second read, and returns the cached bundle
without rebuilding.

The module is deliberately small and stateless — every entry point opens
its own short-lived session so the caller doesn't have to share its DB
connection with potentially blocking advisory locks.

The "default chain" used when callers don't pass one is "ethereum",
matching how ``Job.request['chain']`` is populated by the API. NULL
chains were considered but lose information when an operator inspects
the table.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Mapping

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from db.models import ContractMaterialization, SessionLocal

logger = logging.getLogger(__name__)

DEFAULT_CHAIN = "ethereum"


def is_enabled() -> bool:
    """Env-gated kill switch (mirrors ``PSAT_BYTECODE_PG_CACHE``).

    Default ON in production. Tests that don't intend to exercise this
    layer turn it off via the autouse ``_scrub_contract_materializations_env``
    fixture; tests that do exercise it re-enable via ``cm_session_local``.
    """
    return os.getenv("PSAT_CONTRACT_MATERIALIZATIONS", "1").lower() in ("1", "true", "yes")


def _normalize(chain: str | None, address: str, bytecode_keccak: str) -> tuple[str, str, str]:
    return (
        (chain or DEFAULT_CHAIN).lower(),
        address.lower(),
        bytecode_keccak.lower() if bytecode_keccak.startswith("0x") else "0x" + bytecode_keccak.lower(),
    )


def find_by_keccak(
    session: Session,
    *,
    chain: str | None,
    bytecode_keccak: str,
) -> ContractMaterialization | None:
    """Return the row for ``(chain, bytecode_keccak)`` if status='ready'.

    ``status='pending'`` rows are NOT returned — a pending row means a
    builder is still in flight; the caller should take the advisory
    lock and re-read inside it.
    """
    chain_norm = (chain or DEFAULT_CHAIN).lower()
    keccak_norm = bytecode_keccak.lower() if bytecode_keccak.startswith("0x") else "0x" + bytecode_keccak.lower()
    row = session.execute(
        select(ContractMaterialization).where(
            ContractMaterialization.chain == chain_norm,
            ContractMaterialization.bytecode_keccak == keccak_norm,
            ContractMaterialization.status == "ready",
        )
    ).scalar_one_or_none()
    return row


def find_by_address(
    session: Session,
    *,
    chain: str | None,
    address: str,
) -> ContractMaterialization | None:
    """Return the row for ``(chain, address)`` if status='ready'.

    Address-keyed lookup is the legacy entry path — same-bytecode-different-address
    contracts share one row keyed by keccak, but a known address still
    resolves to that row via the unique index.
    """
    chain_norm = (chain or DEFAULT_CHAIN).lower()
    addr_norm = address.lower()
    row = session.execute(
        select(ContractMaterialization).where(
            ContractMaterialization.chain == chain_norm,
            ContractMaterialization.address == addr_norm,
            ContractMaterialization.status == "ready",
        )
    ).scalar_one_or_none()
    return row


def _advisory_lock(session: Session, chain_norm: str, keccak_norm: str) -> None:
    """Take ``pg_advisory_xact_lock`` for the dedup key.

    ``hashtext`` is built into Postgres and returns a 32-bit signed int —
    fine for the advisory-lock space which is a 64-bit int. Using the
    composite ``chain || ':' || keccak`` rather than just keccak keeps
    chains independent so an Ethereum and a Base contract sharing keccak
    don't serialize on the same lock unnecessarily.
    """
    session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
        {"key": f"{chain_norm}:{keccak_norm}"},
    )


def materialize_or_wait(
    *,
    chain: str | None,
    address: str,
    bytecode_keccak: str,
    builder: Callable[[], Mapping[str, Any]],
) -> ContractMaterialization:
    """Look up or build the materialization row for the given content key.

    Behaviour:
      1. Open a short-lived session, take the advisory lock for the
         ``(chain, bytecode_keccak)`` pair. Concurrent callers serialize
         on this lock.
      2. Inside the lock, re-read the row. If ``status='ready'``,
         return it (the lock loser path).
      3. Otherwise, call ``builder()`` to produce the bundle. The
         builder returns a dict with at minimum ``contract_name``,
         ``analysis``, ``tracking_plan``. The bundle is persisted
         INLINE on the row (Tigris-keyed payloads are a future-proof
         column on the table; today we keep things simple).
      4. Commit, releasing the advisory lock. Return the persisted row.

    On builder failure, the row is upserted to ``status='failed'`` with
    the exception text so an operator can triage; the exception is
    re-raised so the caller's retry logic kicks in.
    """
    chain_norm, addr_norm, keccak_norm = _normalize(chain, address, bytecode_keccak)

    session = SessionLocal()
    try:
        # Single transaction from lock acquisition through the final
        # commit — ``pg_advisory_xact_lock`` releases on commit, so any
        # intermediate commit before persisting the bundle would let a
        # concurrent loser race past and rebuild.
        _advisory_lock(session, chain_norm, keccak_norm)

        # Inside the lock — read sees the result of any concurrent
        # winner that committed before us.
        row = session.execute(
            select(ContractMaterialization).where(
                ContractMaterialization.chain == chain_norm,
                ContractMaterialization.bytecode_keccak == keccak_norm,
            )
        ).scalar_one_or_none()
        if row is not None and row.status == "ready":
            session.commit()
            return row

        try:
            bundle = builder()
        except Exception as exc:
            # Persist the failure breadcrumb so an operator can triage,
            # then re-raise. Upsert because the row may not exist yet on
            # a first-time keccak.
            err = f"{type(exc).__name__}: {exc}"[:4000]
            stmt = pg_insert(ContractMaterialization).values(
                chain=chain_norm,
                bytecode_keccak=keccak_norm,
                address=addr_norm,
                status="failed",
                error=err,
            )
            stmt = stmt.on_conflict_do_update(
                constraint="contract_materializations_pkey",
                set_={"status": "failed", "error": err, "updated_at": func.now()},
            )
            session.execute(stmt)
            session.commit()
            raise

        # Upsert the successful bundle in one statement so the lock
        # holds across the whole write. ``pg_insert`` lets SQLAlchemy
        # bind the dict→JSONB columns correctly.
        stmt = pg_insert(ContractMaterialization).values(
            chain=chain_norm,
            bytecode_keccak=keccak_norm,
            address=addr_norm,
            contract_name=bundle.get("contract_name"),
            analysis=bundle.get("analysis"),
            tracking_plan=bundle.get("tracking_plan"),
            status="ready",
        )
        stmt = stmt.on_conflict_do_update(
            constraint="contract_materializations_pkey",
            set_={
                "status": "ready",
                "contract_name": stmt.excluded.contract_name,
                "analysis": stmt.excluded.analysis,
                "tracking_plan": stmt.excluded.tracking_plan,
                "address": stmt.excluded.address,
                "error": None,
                "materialized_at": func.now(),
                "updated_at": func.now(),
            },
        )
        session.execute(stmt)
        session.commit()

        # Re-read so we return a fresh ORM object reflecting committed values.
        ready = session.execute(
            select(ContractMaterialization).where(
                ContractMaterialization.chain == chain_norm,
                ContractMaterialization.bytecode_keccak == keccak_norm,
            )
        ).scalar_one()
        return ready
    finally:
        session.close()
