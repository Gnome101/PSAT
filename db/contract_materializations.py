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

Bundle storage: the ``analysis`` and ``tracking_plan`` payloads can be
multi-megabyte JSON blobs (a Compound-v3-class contract analysis is
~5-20 MB). Postgres JSONB stores fine but detoasts on every read,
inflates page-cache pressure on this hot table, and slows backup /
dump / restore. The schema therefore carries paired columns:

  - ``analysis`` (JSONB) and ``analysis_blob_key`` (Text)
  - ``tracking_plan`` (JSONB) and ``tracking_plan_blob_key`` (Text)

When object storage is configured (``ARTIFACT_STORAGE_*`` env vars set,
``db.storage.get_storage_client()`` returns non-None) new writes go to
blob storage and the JSONB columns are persisted as NULL. The blob_key
columns are then the source of truth. When storage is unconfigured
(local dev, offline tests without minio) writes fall back to inline
JSONB and blob_key is NULL.

Reads are always handled by ``hydrate_analysis`` / ``hydrate_tracking_plan``
which try the blob first and transparently fall back to inline JSONB.
That fallback is what lets pre-migration rows keep working while the
backfill (``scripts/backfill_contract_materializations_to_blob.py``)
catches up — and what insulates the pipeline from a transient Tigris
outage when the inline copy still exists.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable, Mapping

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from db.models import ContractMaterialization, SessionLocal
from db.storage import (
    JSON_CONTENT_TYPE,
    StorageError,
    StorageKeyMissing,
    _key_prefix,
    get_storage_client,
)

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


def _blob_key(chain_norm: str, keccak_norm: str, kind: str) -> str:
    """Deterministic blob key for an analysis/tracking_plan payload.

    ``kind`` is the payload name without extension (``"analysis"`` or
    ``"tracking_plan"``). Includes the PR-preview prefix from
    ``ARTIFACT_STORAGE_PREFIX`` so previews scope cleanly under one
    bucket. Path separators around chain and keccak make S3-console
    browsing usable.
    """
    return f"{_key_prefix()}contract_materializations/{chain_norm}/{keccak_norm}/{kind}.json"


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


def _hydrate(row: ContractMaterialization, *, blob_key_attr: str, inline_attr: str) -> dict | None:
    """Generic blob-or-inline read for analysis / tracking_plan columns.

    Resolution order:
      1. If ``blob_key_attr`` is set and storage is configured, GET the
         blob and parse JSON.
      2. On a transient blob fetch error, fall through to inline JSONB
         when present — better to serve possibly-stale data than to
         crash the pipeline. ``StorageKeyMissing`` is treated the same
         (the row says we have a key but the bucket disagrees, so we
         either had a wipe or the write never landed).
      3. If neither a blob nor inline JSONB is available, return None.

    Callers that need to mutate the returned dict should ``copy.deepcopy``
    it themselves — the inline JSONB read returns the ORM-cached dict
    and mutations would leak across rows.
    """
    blob_key: str | None = getattr(row, blob_key_attr, None)
    inline: dict | None = getattr(row, inline_attr, None)

    if blob_key:
        client = get_storage_client()
        if client is not None:
            try:
                body = client.get(blob_key)
                parsed = json.loads(body.decode("utf-8"))
                if isinstance(parsed, dict):
                    return parsed
                # The serializer always emits JSON objects for these
                # payloads; a non-dict is corruption, not a normal case.
                logger.warning(
                    "contract_materializations: blob %s decoded to %s, expected dict",
                    blob_key,
                    type(parsed).__name__,
                )
            except (StorageError, StorageKeyMissing, ValueError) as exc:
                if inline is not None:
                    logger.warning(
                        "contract_materializations: blob fetch for %s failed (%s); using inline JSONB",
                        blob_key,
                        exc,
                    )
                else:
                    logger.error(
                        "contract_materializations: blob %s unreadable (%s) and no inline fallback",
                        blob_key,
                        exc,
                    )
                    return None
        else:
            # blob_key set but storage unconfigured (e.g. test env that
            # turned ARTIFACT_STORAGE_* off after the row was written) —
            # silently fall through to inline if present, else None.
            if inline is None:
                logger.warning(
                    "contract_materializations: blob_key %s but storage unconfigured; no inline fallback",
                    blob_key,
                )

    return inline


def hydrate_analysis(row: ContractMaterialization) -> dict | None:
    """Load the row's ``analysis`` payload, transparently picking the
    blob path when ``analysis_blob_key`` is set and falling back to the
    inline JSONB column otherwise. ``None`` means the row genuinely
    has no analysis (a corner case for ``status != 'ready'`` rows)."""
    return _hydrate(row, blob_key_attr="analysis_blob_key", inline_attr="analysis")


def hydrate_tracking_plan(row: ContractMaterialization) -> dict | None:
    """Symmetric to ``hydrate_analysis`` for ``tracking_plan``."""
    return _hydrate(row, blob_key_attr="tracking_plan_blob_key", inline_attr="tracking_plan")


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


def _put_blob(client, blob_key: str, payload: dict) -> None:
    """Serialize and upload one payload. Errors propagate so the
    enclosing transaction rolls back — avoids persisting a row that
    points at a key the bucket doesn't have."""
    body = json.dumps(payload, default=str).encode("utf-8")
    client.put(blob_key, body, JSON_CONTENT_TYPE)


def materialize_or_wait(
    *,
    chain: str | None,
    address: str,
    bytecode_keccak: str,
    builder: Callable[[], Mapping[str, Any]],
) -> ContractMaterialization:
    """Look up or build the materialization row for the given content key.

    Three phases, each in its own short-lived transaction so no PG
    connection sits idle during ``builder()``:

      1. **Ready check** — open a session, take the advisory lock for
         ``(chain, bytecode_keccak)``, re-read the row. If ``status='ready'``,
         return it. Otherwise commit (releasing the lock) and proceed.
      2. **Build** — call ``builder()`` with no DB session held. The
         builder returns a dict with at minimum ``contract_name``,
         ``analysis``, ``tracking_plan``. Blob uploads (if storage is
         configured) happen in this phase too — same idle-connection
         concern, same fix.
      3. **Write** — open a fresh session, take the advisory lock, recheck
         (a concurrent caller may have written ``status='ready'`` while
         our builder was running — we serve their bundle and drop ours),
         else upsert this build's bundle to ``status='ready'``, commit.

    Why split the lock: the original design held the lock across the
    builder, which kept the PG connection idle for the 1-3 minutes a
    real forge+Slither run takes. Neon's pooler-side SSL idle timeout
    closes that connection mid-build, the final UPSERT raises
    ``OperationalError``, and the cache row is never written — the
    recursive resolver then falls back to rebuilding the same bytecode.
    Dropping the lock between phases makes both transactions sub-second
    so the timeout never fires; the recheck in phase 3 handles the rare
    case where two callers raced through phase 1 and both built.

    On builder failure, a fresh session writes a ``status='failed'`` row
    with the exception text so an operator can triage, then re-raises.

    On a blob upload failure the exception propagates without writing a
    row — better to leave nothing committed than a row pointing at a
    blob key the bucket doesn't have.
    """
    chain_norm, addr_norm, keccak_norm = _normalize(chain, address, bytecode_keccak)

    # ── Phase 1: ready check under a short-lived lock ──────────────
    with SessionLocal() as session:
        _advisory_lock(session, chain_norm, keccak_norm)
        row = session.execute(
            select(ContractMaterialization).where(
                ContractMaterialization.chain == chain_norm,
                ContractMaterialization.bytecode_keccak == keccak_norm,
            )
        ).scalar_one_or_none()
        if row is not None and row.status == "ready":
            session.commit()
            return row
        # Release the lock before the long-running builder. Concurrent
        # callers may now proceed past their own phase-1 check; the
        # phase-3 recheck is what collapses the resulting race to one
        # stored bundle.
        session.commit()

    # ── Phase 2: builder + blob uploads, no DB connection held ─────
    try:
        bundle = builder()
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"[:4000]
        with SessionLocal() as session:
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

    analysis_payload = bundle.get("analysis")
    tracking_plan_payload = bundle.get("tracking_plan")
    analysis_blob_key: str | None = None
    tracking_plan_blob_key: str | None = None
    analysis_inline: dict | None = analysis_payload if isinstance(analysis_payload, dict) else None
    tracking_plan_inline: dict | None = tracking_plan_payload if isinstance(tracking_plan_payload, dict) else None

    client = get_storage_client()
    if client is not None:
        # Blob uploads happen before reacquiring the PG lock so a slow
        # Tigris PUT doesn't push us back into idle-connection territory.
        # On failure, propagate without writing a row — the next caller
        # retries the build cleanly.
        if analysis_inline is not None:
            analysis_blob_key = _blob_key(chain_norm, keccak_norm, "analysis")
            _put_blob(client, analysis_blob_key, analysis_inline)
            analysis_inline = None
        if tracking_plan_inline is not None:
            tracking_plan_blob_key = _blob_key(chain_norm, keccak_norm, "tracking_plan")
            _put_blob(client, tracking_plan_blob_key, tracking_plan_inline)
            tracking_plan_inline = None

    # ── Phase 3: write under a short-lived lock ────────────────────
    with SessionLocal() as session:
        _advisory_lock(session, chain_norm, keccak_norm)

        # Recheck: a concurrent caller may have raced past phase 1 with
        # us and committed first. Their bundle is keccak-equivalent
        # (same bytecode → same static analysis), so we serve it and
        # discard ours.
        existing = session.execute(
            select(ContractMaterialization).where(
                ContractMaterialization.chain == chain_norm,
                ContractMaterialization.bytecode_keccak == keccak_norm,
            )
        ).scalar_one_or_none()
        if existing is not None and existing.status == "ready":
            session.commit()
            return existing

        stmt = pg_insert(ContractMaterialization).values(
            chain=chain_norm,
            bytecode_keccak=keccak_norm,
            address=addr_norm,
            contract_name=bundle.get("contract_name"),
            analysis=analysis_inline,
            tracking_plan=tracking_plan_inline,
            analysis_blob_key=analysis_blob_key,
            tracking_plan_blob_key=tracking_plan_blob_key,
            status="ready",
        )
        stmt = stmt.on_conflict_do_update(
            constraint="contract_materializations_pkey",
            set_={
                "status": "ready",
                "contract_name": stmt.excluded.contract_name,
                "analysis": stmt.excluded.analysis,
                "tracking_plan": stmt.excluded.tracking_plan,
                "analysis_blob_key": stmt.excluded.analysis_blob_key,
                "tracking_plan_blob_key": stmt.excluded.tracking_plan_blob_key,
                "address": stmt.excluded.address,
                "error": None,
                "materialized_at": func.now(),
                "updated_at": func.now(),
            },
        )
        session.execute(stmt)
        session.commit()

        ready = session.execute(
            select(ContractMaterialization).where(
                ContractMaterialization.chain == chain_norm,
                ContractMaterialization.bytecode_keccak == keccak_norm,
            )
        ).scalar_one()
        return ready
