"""Postgres-based job queue operations using SELECT ... FOR UPDATE SKIP LOCKED."""

from __future__ import annotations

import logging
import os
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from .models import (
    Artifact,
    Base,
    Contract,
    ContractSummary,
    Job,
    JobStage,
    JobStatus,
    Protocol,
    SourceFile,
)
from .storage import (
    StorageError,
    StorageKeyMissing,
    artifact_key,
    deserialize_artifact,
    get_storage_client,
    serialize_artifact,
    source_file_key,
)

logger = logging.getLogger(__name__)

# How long a job can sit in ``status='processing'`` before we assume the
# worker holding it crashed and return the row to the queue. ``updated_at``
# is our implicit heartbeat — every status/detail write stamps NOW(), so a
# stale row reliably means nobody is touching it.
DEFAULT_JOB_STALE_TIMEOUT = int(os.getenv("PSAT_JOB_STALE_TIMEOUT", "900"))


def reclaim_stuck_jobs(session: Session, stale_timeout_seconds: int = DEFAULT_JOB_STALE_TIMEOUT) -> list[str]:
    """Sweep jobs stuck in ``processing`` past the threshold back to ``queued``.

    Uses ``updated_at`` as a heartbeat: any status or detail write bumps it,
    so a row that hasn't moved in ``stale_timeout_seconds`` seconds indicates
    the claiming worker crashed before it could advance, fail, or update
    progress. Flips status back to ``queued`` and clears ``worker_id`` so a
    live worker can claim it on the next poll.

    Runs one ``UPDATE ... RETURNING id`` so the sweep is atomic and we can
    log which rows were rescued. ``SKIP LOCKED`` keeps us from blocking on a
    row whose FOR UPDATE is currently held by another process (including the
    happy-path claim happening concurrently).

    Returns the list of rescued job IDs so callers can log them — operators
    can then correlate these IDs against a worker's last known heartbeat to
    identify which instance crashed.
    """
    result = session.execute(
        text(
            """
            UPDATE jobs
            SET status = 'queued', worker_id = NULL
            WHERE id IN (
                SELECT id FROM jobs
                WHERE status = 'processing'
                  AND updated_at < NOW() - (:timeout * INTERVAL '1 second')
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id
            """
        ),
        {"timeout": stale_timeout_seconds},
    )
    rescued = [str(row_id) for (row_id,) in result]
    if rescued:
        session.commit()
        for job_id in rescued:
            logger.warning(
                "reclaim_stuck_jobs: reset job %s (stuck in processing for > %ss)",
                job_id,
                stale_timeout_seconds,
            )
    else:
        session.rollback()
    return rescued


def create_job(
    session: Session,
    request_dict: dict[str, Any],
    initial_stage: JobStage = JobStage.discovery,
) -> Job:
    """Insert a new job at the given stage with status=queued."""
    job = Job(
        address=request_dict.get("address"),
        company=request_dict.get("company"),
        name=request_dict.get("name"),
        status=JobStatus.queued,
        stage=initial_stage,
        detail="Queued for analysis",
        request=request_dict,
        protocol_id=request_dict.get("protocol_id"),
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def upsert_discovered_contract(
    session: Session,
    *,
    address: str,
    chain: str | None,
    protocol_id: int | None,
    new_sources: list[str],
    contract_name: str | None = None,
    confidence: float | None = None,
    chains: list[str] | None = None,
    discovery_url: str | None = None,
) -> Contract:
    """Insert or update a discovered contract, unioning ``discovery_sources``.

    Every discovery worker — inventory, DApp crawl, DefiLlama scan,
    upgrade-history backfill — funnels through here so "three sources
    agree" shows up in the data as a three-element array, not as
    whichever writer landed first. The ranking module reads the union
    and applies a corroboration boost.

    When the row exists already:
        - ``discovery_sources`` is unioned (new entries appended, dedup
          preserves order so the first discoverer stays first).
        - ``protocol_id`` is backfilled if null (orphan adoption).
        - ``contract_name`` / ``confidence`` / ``chains`` /
          ``discovery_url`` are first-writer-wins: later writers only
          fill them if the stored value is missing, so a later
          lower-quality source doesn't stomp a better one.

    Commit is the caller's responsibility — callers usually batch many
    upserts into one transaction.
    """
    normalized = address.lower()
    existing = session.execute(
        select(Contract).where(
            Contract.address == normalized,
            Contract.chain == chain,
        )
    ).scalar_one_or_none()

    clean_sources = [s for s in new_sources if s]

    if existing is None:
        row = Contract(
            address=normalized,
            chain=chain,
            protocol_id=protocol_id,
            contract_name=contract_name,
            confidence=confidence,
            discovery_sources=list(clean_sources) or None,
            chains=chains,
            discovery_url=discovery_url,
        )
        session.add(row)
        return row

    merged = list(existing.discovery_sources or [])
    for src in clean_sources:
        if src not in merged:
            merged.append(src)
    if merged:
        existing.discovery_sources = merged

    if existing.protocol_id is None and protocol_id is not None:
        existing.protocol_id = protocol_id
    if not existing.contract_name and contract_name:
        existing.contract_name = contract_name
    if existing.confidence is None and confidence is not None:
        existing.confidence = confidence
    if not existing.chains and chains:
        existing.chains = chains
    if not existing.discovery_url and discovery_url:
        existing.discovery_url = discovery_url

    return existing


def get_or_create_protocol(
    session: Session,
    name: str,
    official_domain: str | None = None,
) -> Protocol:
    """Look up Protocol by name, create if missing. Backfill official_domain if still null."""
    row = session.execute(select(Protocol).where(Protocol.name == name)).scalar_one_or_none()
    if row is None:
        row = Protocol(name=name, official_domain=official_domain)
        session.add(row)
        session.flush()
        return row
    if official_domain and not row.official_domain:
        row.official_domain = official_domain
        session.flush()
    return row


def claim_job(session: Session, target_stage: JobStage, worker_id: str) -> Job | None:
    """Claim the next available job for the given stage using SKIP LOCKED."""
    stmt = (
        select(Job)
        .where(Job.stage == target_stage, Job.status == JobStatus.queued)
        .order_by(Job.created_at)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    job = session.execute(stmt).scalar_one_or_none()
    if job is None:
        return None
    job.status = JobStatus.processing
    job.worker_id = worker_id
    session.commit()
    session.refresh(job)
    return job


def update_job_detail(session: Session, job_id: Any, detail: str) -> None:
    """Update the human-readable progress message on a job."""
    job = session.get(Job, job_id)
    if job:
        job.detail = detail
        session.commit()


def advance_job(session: Session, job_id: Any, next_stage: JobStage, detail: str = "") -> None:
    """Move a job to the next stage and reset status to queued."""
    job = session.get(Job, job_id)
    if job is None:
        return
    job.stage = next_stage
    job.status = JobStatus.queued
    job.detail = detail or f"Advanced to {next_stage.value}"
    job.worker_id = None
    session.commit()


def complete_job(session: Session, job_id: Any, detail: str = "Analysis complete") -> None:
    """Mark a job as completed with stage=done."""
    job = session.get(Job, job_id)
    if job is None:
        return
    job.stage = JobStage.done
    job.status = JobStatus.completed
    job.detail = detail
    job.worker_id = None
    session.commit()


def fail_job(session: Session, job_id: Any, error: str) -> None:
    """Mark a job as failed with the error traceback."""
    job = session.get(Job, job_id)
    if job is None:
        return
    job.status = JobStatus.failed
    job.error = error
    job.detail = "Failed"
    job.worker_id = None
    session.commit()


def count_analysis_children(session: Session, root_job_id: str) -> int:
    """Count analysis jobs (jobs with an address) linked to a root job."""
    from sqlalchemy import func

    count = (
        session.execute(
            select(func.count(Job.id)).where(
                Job.address.isnot(None),
                Job.request["root_job_id"].as_string() == root_job_id,
            )
        ).scalar()
        or 0
    )
    return count


def _artifact_row_to_value(artifact: Artifact) -> dict | list | str | None:
    """Resolve an Artifact row to its decoded payload (handles inline + storage)."""
    if artifact.storage_key:
        client = get_storage_client()
        if client is None:
            raise RuntimeError(
                f"Artifact {artifact.name} on job {artifact.job_id} has storage_key but storage is not configured"
            )
        body = client.get(artifact.storage_key)
        return deserialize_artifact(body, artifact.content_type)
    if artifact.data is not None:
        return artifact.data
    return artifact.text_data


def _mirror_contract_flags_to_job(session: Session, job_id: Any, name: str, data: Any) -> None:
    """Mirror ``contract_flags.is_proxy`` onto ``Job.is_proxy`` so /api/jobs
    can answer the proxy-flag question without resolving the artifact body."""
    if name != "contract_flags" or not isinstance(data, dict):
        return
    is_proxy = data.get("is_proxy") is True
    session.execute(sa_update(Job).where(Job.id == job_id).values(is_proxy=is_proxy))


def store_artifact(session: Session, job_id: Any, name: str, data: Any = None, text_data: str | None = None) -> None:
    """Upsert an artifact for a job (unique on job_id + name).

    When ``ARTIFACT_STORAGE_*`` env vars are set, the body is written to object
    storage and only metadata (storage_key, size_bytes, content_type) is stored
    in Postgres. Otherwise, the body lives inline in ``data`` / ``text_data``.

    If the storage put succeeds but the DB write fails, the storage object is
    deleted — but only if the row did not pre-exist. Overwriting a previously-
    committed artifact with the same deterministic key and then rolling back
    leaves the object in place (deleting it would break the previous row).
    """
    client = get_storage_client()
    if client is not None:
        body, content_type = serialize_artifact(data, text_data)
        key = artifact_key(job_id, name)
        preexisting = session.execute(
            select(Artifact.id).where(Artifact.job_id == job_id, Artifact.name == name).limit(1)
        ).scalar_one_or_none()

        client.put(key, body, content_type, metadata={"artifact_name": name, "job_id": str(job_id)})
        stmt = pg_insert(Artifact).values(
            job_id=job_id,
            name=name,
            data=None,
            text_data=None,
            storage_key=key,
            size_bytes=len(body),
            content_type=content_type,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_artifact_job_name",
            set_={
                "data": None,
                "text_data": None,
                "storage_key": stmt.excluded.storage_key,
                "size_bytes": stmt.excluded.size_bytes,
                "content_type": stmt.excluded.content_type,
            },
        )
        try:
            session.execute(stmt)
            _mirror_contract_flags_to_job(session, job_id, name, data)
            session.commit()
        except Exception:
            session.rollback()
            if preexisting is None:
                try:
                    client.delete(key)
                except StorageError:
                    logger.warning("Failed to clean up orphan storage object %s", key)
            raise
        return

    stmt = pg_insert(Artifact).values(
        job_id=job_id,
        name=name,
        data=data,
        text_data=text_data,
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_artifact_job_name",
        set_={
            "data": stmt.excluded.data,
            "text_data": stmt.excluded.text_data,
            "storage_key": None,
            "size_bytes": None,
            "content_type": None,
        },
    )
    session.execute(stmt)
    _mirror_contract_flags_to_job(session, job_id, name, data)
    session.commit()


def get_artifact(session: Session, job_id: Any, name: str) -> dict | list | str | None:
    """Read an artifact by job_id and name."""
    stmt = select(Artifact).where(Artifact.job_id == job_id, Artifact.name == name)
    artifact = session.execute(stmt).scalar_one_or_none()
    if artifact is None:
        return None
    return _artifact_row_to_value(artifact)


def backfill_job_is_proxy_from_storage(session: Session) -> int:
    """Flip ``Job.is_proxy`` for legacy storage-backed ``contract_flags`` rows the inline SQL backfill can't reach."""
    if get_storage_client() is None:
        return 0
    rows = session.execute(
        select(Artifact)
        .join(Job, Artifact.job_id == Job.id)
        .where(
            Artifact.name == "contract_flags",
            Artifact.storage_key.is_not(None),
            Job.is_proxy.is_(False),
        )
    ).scalars()
    updated = 0
    for art in rows:
        try:
            value = _artifact_row_to_value(art)
        except StorageError:
            logger.warning("backfill: contract_flags storage read failed for job %s", art.job_id)
            continue
        if not isinstance(value, dict) or value.get("is_proxy") is not True:
            continue
        session.execute(sa_update(Job).where(Job.id == art.job_id, Job.is_proxy.is_(False)).values(is_proxy=True))
        updated += 1
    session.commit()
    return updated


def get_all_artifacts(session: Session, job_id: Any) -> dict[str, Any]:
    """Read all artifacts for a job. Returns {name: data_or_text}."""
    stmt = select(Artifact).where(Artifact.job_id == job_id)
    artifacts = session.execute(stmt).scalars().all()
    result: dict[str, Any] = {}
    for artifact in artifacts:
        try:
            value = _artifact_row_to_value(artifact)
        except StorageKeyMissing:
            continue
        if value is not None:
            result[artifact.name] = value
    return result


def store_source_files(session: Session, job_id: Any, files: dict[str, str]) -> None:
    """Bulk insert source files for a job (replaces existing).

    When object storage is configured, every body is uploaded first with the
    path carried in user-metadata (so the path is recoverable from storage
    alone). Only after all uploads succeed do we swap the DB rows. If any
    upload fails, already-uploaded objects are deleted so the bucket does not
    accumulate orphans pointing at nothing.
    """
    client = get_storage_client()
    if client is None:
        session.query(SourceFile).filter(SourceFile.job_id == job_id).delete()
        for path, content in files.items():
            session.add(SourceFile(job_id=job_id, path=path, content=content))
        session.commit()
        return

    uploaded_keys: list[str] = []
    try:
        entries: list[tuple[str, str]] = []
        for path, content in files.items():
            key = source_file_key(job_id, path)
            client.put(
                key,
                content.encode("utf-8"),
                "text/plain; charset=utf-8",
                metadata={"path": path, "job_id": str(job_id)},
            )
            uploaded_keys.append(key)
            entries.append((path, key))

        # All uploads succeeded — swap DB rows atomically.
        session.query(SourceFile).filter(SourceFile.job_id == job_id).delete()
        for path, key in entries:
            session.add(SourceFile(job_id=job_id, path=path, content=None, storage_key=key))
        session.commit()
    except Exception:
        session.rollback()
        for key in uploaded_keys:
            try:
                client.delete(key)
            except StorageError:
                logger.warning("Failed to clean up orphan source file object %s", key)
        raise


def get_source_files(session: Session, job_id: Any) -> dict[str, str]:
    """Returns {relative_path: file_content} for all source files of a job."""
    stmt = select(SourceFile).where(SourceFile.job_id == job_id)
    rows = session.execute(stmt).scalars().all()
    out: dict[str, str] = {}
    client = get_storage_client()
    for row in rows:
        if row.storage_key:
            if client is None:
                raise RuntimeError(
                    f"SourceFile {row.path} on job {row.job_id} has storage_key but storage is not configured"
                )
            try:
                out[row.path] = client.get(row.storage_key).decode("utf-8")
            except StorageKeyMissing:
                continue
        elif row.content is not None:
            out[row.path] = row.content
    return out


# ---------------------------------------------------------------------------
# Static data caching
# ---------------------------------------------------------------------------

# Artifact names that constitute cached static data (immutable, never change).
# slither_results / analysis_report were removed when vulnerability-detector
# triage was split out of PSAT's pipeline; downstream stages don't depend on
# them, and the only writer (StaticWorker._run_slither_phase) is gone.
_STATIC_ARTIFACT_NAMES = frozenset(
    {
        "contract_analysis",
        "control_tracking_plan",
        "static_dependencies",
        "enrichment_cache",
    }
)

# Artifacts copied as a starting baseline but appended to on subsequent runs.
_SEED_ARTIFACT_NAMES = frozenset(
    {
        "dynamic_dependencies",
        "classifications",
        "upgrade_history",
    }
)

# Contract columns that are mutable (resolved live by _resolve_proxy) and
# must NOT be carried over from a cached job.
_MUTABLE_CONTRACT_FIELDS = frozenset({"is_proxy", "proxy_type", "implementation", "beacon", "admin"})


def copy_row(session: Session, source: Base, *, exclude: frozenset[str] = frozenset(), **overrides: Any) -> Base:
    """Copy a SQLAlchemy row, returning a new detached instance.

    - Primary keys are always skipped (auto-generated).
    - Columns with a ``server_default`` (e.g. ``created_at``) are skipped
      so the DB assigns fresh values, unless explicitly passed in *overrides*.
    - *exclude* names additional columns to drop.
    - *overrides* supply values that differ from the source (e.g. remapped
      foreign keys, zeroed-out mutable fields).

    Lists are shallow-copied so the new row doesn't share references with
    the source.
    """
    from sqlalchemy import inspect as sa_inspect

    mapper = sa_inspect(type(source))
    kwargs: dict[str, Any] = {}
    for attr in mapper.column_attrs:
        key = attr.key
        if key in exclude:
            continue
        col = attr.columns[0]
        if col.primary_key:
            continue
        if key in overrides:
            kwargs[key] = overrides[key]
            continue
        if col.server_default is not None:
            continue
        value = getattr(source, key)
        if isinstance(value, list):
            value = list(value)
        kwargs[key] = value

    new_row = type(source)(**kwargs)
    session.add(new_row)
    return new_row


def find_completed_static_cache(session: Session, address: str, chain: str | None = None) -> Job | None:
    """Find a previously completed job for *address* (and *chain*) that has all required static data.

    Returns the cached :class:`Job` if one exists with:
    - status = completed, stage = done
    - at least one ``source_files`` row
    - the ``contract_analysis`` artifact (key indicator that the static stage finished)
    - a ``contracts`` row for this address/chain with a ``contract_summaries`` row

    The contract lookup uses (address, chain) rather than ``job_id`` so that
    the cache remains valid even after ``copy_static_cache`` reassigned the
    Contract row to a later target job.

    Returns ``None`` when no suitable cache exists.
    """
    stmt = (
        select(Job)
        .where(
            func.lower(Job.address) == address.lower(),
            Job.status == JobStatus.completed,
            Job.stage == JobStage.done,
        )
        .order_by(Job.updated_at.desc())
    )
    candidates = session.execute(stmt).scalars().all()

    for candidate in candidates:
        # Filter by chain stored in the job's request dict
        if chain is not None:
            req = candidate.request if isinstance(candidate.request, dict) else {}
            if req.get("chain") != chain:
                continue

        src_count = session.execute(
            select(SourceFile).where(SourceFile.job_id == candidate.id).limit(1)
        ).scalar_one_or_none()
        if not src_count:
            continue

        # Look up by (address, chain), not job_id — copy_static_cache may have reassigned.
        # Join ContractSummary so .limit(1) skips stub rows that lack the cached summary.
        contract_stmt = (
            select(Contract)
            .join(ContractSummary, ContractSummary.contract_id == Contract.id)
            .where(func.lower(Contract.address) == address.lower())
        )
        if chain is not None:
            contract_stmt = contract_stmt.where(Contract.chain == chain)
        contract_row = session.execute(contract_stmt.limit(1)).scalar_one_or_none()
        if not contract_row:
            continue

        # Static-stage-finished check. For non-proxy contracts the canonical
        # indicator is ``contract_analysis`` (slither output + summary).
        # Proxies never produce ``contract_analysis`` on their own job —
        # it lives on the impl child — so require ``contract_flags`` instead,
        # which proxies do write (is_proxy + proxy_type). Without this
        # branch, re-discovered proxies would miss the cache and do a full
        # fresh Etherscan fetch + slither run every time.
        required_artifact = "contract_flags" if contract_row.is_proxy else "contract_analysis"
        has_required = session.execute(
            select(Artifact).where(Artifact.job_id == candidate.id, Artifact.name == required_artifact).limit(1)
        ).scalar_one_or_none()
        if not has_required:
            continue

        summary = session.execute(
            select(ContractSummary).where(ContractSummary.contract_id == contract_row.id).limit(1)
        ).scalar_one_or_none()
        if not summary:
            continue

        return candidate

    return None


def find_previous_company_inventory(
    session: Session,
    company: str,
    exclude_job_id: Any = None,
    chain: str | None = None,
) -> Job | None:
    """Find the most recent completed company job with a contract_inventory artifact.

    When *chain* is given, only jobs whose ``request["chain"]`` matches are
    considered, preventing cross-chain inventory contamination.
    """
    stmt = (
        select(Job)
        .where(
            func.lower(Job.company) == company.lower(),
            Job.status == JobStatus.completed,
            Job.stage == JobStage.done,
        )
        .order_by(Job.updated_at.desc())
    )
    candidates = session.execute(stmt).scalars().all()
    for candidate in candidates:
        if exclude_job_id and candidate.id == exclude_job_id:
            continue
        if chain is not None:
            req = candidate.request if isinstance(candidate.request, dict) else {}
            if req.get("chain") != chain:
                continue
        art = session.execute(
            select(Artifact).where(Artifact.job_id == candidate.id, Artifact.name == "contract_inventory").limit(1)
        ).scalar_one_or_none()
        if art:
            return candidate
    return None


def find_existing_job_for_address(session: Session, address: str, chain: str | None = None) -> Job | None:
    """Find a non-failed job for *address* (and *chain*), case-insensitive.

    When *chain* is given, only jobs whose ``request["chain"]`` matches are
    returned, so an Ethereum job won't suppress a Base job at the same address.
    """
    candidates = (
        session.execute(
            select(Job).where(
                func.lower(Job.address) == address.lower(),
                Job.status != JobStatus.failed,
            )
        )
        .scalars()
        .all()
    )
    for c in candidates:
        if chain is not None:
            req = c.request if isinstance(c.request, dict) else {}
            if req.get("chain") != chain:
                continue
        return c
    return None


def is_known_proxy(session: Session, address: str, chain: str | None = None) -> bool:
    """Return True if *address* (on *chain*) has been classified as a proxy in any prior analysis."""
    stmt = select(Contract).where(
        func.lower(Contract.address) == address.lower(),
        Contract.is_proxy.is_(True),
    )
    if chain is not None:
        stmt = stmt.where(Contract.chain == chain)
    return session.execute(stmt.limit(1)).scalar_one_or_none() is not None


def copy_static_cache(session: Session, source_job_id: Any, target_job_id: Any) -> int | None:
    """Copy all cached static data from *source_job_id* to *target_job_id*.

    Copies:
    - ``contracts`` row (immutable fields only; proxy fields left as defaults)
    - ``source_files`` rows
    - ``contract_summaries``, ``privileged_functions``, ``role_definitions``
      rows (linked to the new contract row)
    - Static artifacts (``contract_analysis``, ``control_tracking_plan``,
      ``static_dependencies``, ``enrichment_cache``)

    The source contract is looked up by (address, chain) rather than by
    ``job_id`` so that subsequent cache copies still work after a prior copy
    reassigned the Contract row.

    Returns the new ``Contract.id`` on success, or ``None`` on failure.
    """
    # Guard: if the target already has a contract row, return early.
    existing = session.execute(select(Contract).where(Contract.job_id == target_job_id).limit(1)).scalar_one_or_none()
    if existing:
        return existing.id

    # Resolve the source job's address and chain so we can find the Contract
    # by its natural key (address, chain) rather than by job_id.  A prior
    # copy_static_cache may have reassigned the Contract row's job_id to a
    # different target, so job_id lookup is unreliable after the first copy.
    src_job = session.get(Job, source_job_id)
    if not src_job or not src_job.address:
        return None

    src_req = src_job.request if isinstance(src_job.request, dict) else {}
    src_chain = src_req.get("chain")

    # Join ContractSummary so we copy a summaried row, not a stub (mirrors find_completed_static_cache).
    src_contract_stmt = (
        select(Contract)
        .join(ContractSummary, ContractSummary.contract_id == Contract.id)
        .where(func.lower(Contract.address) == src_job.address.lower())
    )
    if src_chain is not None:
        src_contract_stmt = src_contract_stmt.where(Contract.chain == src_chain)
    src_contract = session.execute(src_contract_stmt.limit(1)).scalar_one_or_none()
    if not src_contract:
        return None

    # The unique constraint on (address, chain) means src_contract IS the
    # only Contract for this address/chain.  Reassign it to the target job.
    src_contract.job_id = target_job_id

    # Save the current proxy state so _check_proxy_cache can compare it
    # against the live on-chain implementation to decide whether
    # re-classification is needed.  The Contract row keeps its proxy
    # fields intact — zeroing them would corrupt data for the old
    # completed job that also references this row via address lookup.
    _cached_proxy_state = {
        "is_proxy": src_contract.is_proxy,
        "proxy_type": src_contract.proxy_type,
        "implementation": src_contract.implementation,
        "beacon": src_contract.beacon,
        "admin": src_contract.admin,
    }
    store_artifact(session, target_job_id, "cached_proxy_state", data=_cached_proxy_state)

    session.flush()
    new_contract = src_contract

    storage = get_storage_client()

    # --- source files ---
    src_files = session.execute(select(SourceFile).where(SourceFile.job_id == source_job_id)).scalars().all()
    for sf in src_files:
        if sf.storage_key and storage is not None:
            new_key = source_file_key(target_job_id, sf.path)
            storage.copy(sf.storage_key, new_key)
            session.add(SourceFile(job_id=target_job_id, path=sf.path, content=None, storage_key=new_key))
        else:
            copy_row(session, sf, job_id=target_job_id)

    # --- artifacts (static + seed) ---
    src_artifacts = (
        session.execute(
            select(Artifact).where(
                Artifact.job_id == source_job_id,
                Artifact.name.in_(_STATIC_ARTIFACT_NAMES | _SEED_ARTIFACT_NAMES),
            )
        )
        .scalars()
        .all()
    )
    for art in src_artifacts:
        if art.storage_key and storage is not None:
            new_key = artifact_key(target_job_id, art.name)
            storage.copy(art.storage_key, new_key)
            stmt = pg_insert(Artifact).values(
                job_id=target_job_id,
                name=art.name,
                data=None,
                text_data=None,
                storage_key=new_key,
                size_bytes=art.size_bytes,
                content_type=art.content_type,
            )
            stmt = stmt.on_conflict_do_update(
                constraint="uq_artifact_job_name",
                set_={
                    "data": None,
                    "text_data": None,
                    "storage_key": stmt.excluded.storage_key,
                    "size_bytes": stmt.excluded.size_bytes,
                    "content_type": stmt.excluded.content_type,
                },
            )
            session.execute(stmt)
        else:
            store_artifact(session, target_job_id, art.name, data=art.data, text_data=art.text_data)

    session.commit()
    return new_contract.id  # type: ignore[attr-defined]
