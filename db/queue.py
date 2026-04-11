"""Postgres-based job queue operations using SELECT ... FOR UPDATE SKIP LOCKED."""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
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
    PrivilegedFunction,
    RoleDefinition,
    SourceFile,
)


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


def store_artifact(session: Session, job_id: Any, name: str, data: Any = None, text_data: str | None = None) -> None:
    """Upsert an artifact for a job (unique on job_id + name)."""
    stmt = pg_insert(Artifact).values(
        job_id=job_id,
        name=name,
        data=data,
        text_data=text_data,
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_artifact_job_name",
        set_={"data": stmt.excluded.data, "text_data": stmt.excluded.text_data},
    )
    session.execute(stmt)
    session.commit()


def get_artifact(session: Session, job_id: Any, name: str) -> dict | str | None:
    """Read an artifact by job_id and name. Returns data (JSONB) or text_data."""
    stmt = select(Artifact).where(Artifact.job_id == job_id, Artifact.name == name)
    artifact = session.execute(stmt).scalar_one_or_none()
    if artifact is None:
        return None
    if artifact.data is not None:
        return artifact.data
    return artifact.text_data


def get_all_artifacts(session: Session, job_id: Any) -> dict[str, Any]:
    """Read all artifacts for a job. Returns {name: data_or_text}."""
    stmt = select(Artifact).where(Artifact.job_id == job_id)
    artifacts = session.execute(stmt).scalars().all()
    result: dict[str, Any] = {}
    for artifact in artifacts:
        if artifact.data is not None:
            result[artifact.name] = artifact.data
        elif artifact.text_data is not None:
            result[artifact.name] = artifact.text_data
    return result


def store_source_files(session: Session, job_id: Any, files: dict[str, str]) -> None:
    """Bulk insert source files for a job (replaces existing)."""
    session.query(SourceFile).filter(SourceFile.job_id == job_id).delete()
    for path, content in files.items():
        session.add(SourceFile(job_id=job_id, path=path, content=content))
    session.commit()


def get_source_files(session: Session, job_id: Any) -> dict[str, str]:
    """Returns {relative_path: file_content} for all source files of a job."""
    stmt = select(SourceFile).where(SourceFile.job_id == job_id)
    rows = session.execute(stmt).scalars().all()
    return {row.path: row.content for row in rows}


# ---------------------------------------------------------------------------
# Static data caching
# ---------------------------------------------------------------------------

# Artifact names that constitute cached static data (immutable, never change).
_STATIC_ARTIFACT_NAMES = frozenset(
    {
        "contract_analysis",
        "slither_results",
        "analysis_report",
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


def find_completed_static_cache(session: Session, address: str) -> Job | None:
    """Find a previously completed job for *address* that has all required static data.

    Returns the cached :class:`Job` if one exists with:
    - status = completed, stage = done
    - a ``contracts`` row for this address
    - at least one ``source_files`` row
    - the ``contract_analysis`` artifact (key indicator that the static stage finished)
    - a ``contract_summaries`` row linked to the contract

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
        contract_row = session.execute(
            select(Contract).where(Contract.job_id == candidate.id).limit(1)
        ).scalar_one_or_none()
        if not contract_row:
            continue

        src_count = session.execute(
            select(SourceFile).where(SourceFile.job_id == candidate.id).limit(1)
        ).scalar_one_or_none()
        if not src_count:
            continue

        analysis_art = session.execute(
            select(Artifact).where(Artifact.job_id == candidate.id, Artifact.name == "contract_analysis").limit(1)
        ).scalar_one_or_none()
        if not analysis_art:
            continue

        summary = session.execute(
            select(ContractSummary).where(ContractSummary.contract_id == contract_row.id).limit(1)
        ).scalar_one_or_none()
        if not summary:
            continue

        return candidate

    return None


def find_previous_company_inventory(session: Session, company: str, exclude_job_id: Any = None) -> Job | None:
    """Find the most recent completed company job with a contract_inventory artifact."""
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
        art = session.execute(
            select(Artifact).where(Artifact.job_id == candidate.id, Artifact.name == "contract_inventory").limit(1)
        ).scalar_one_or_none()
        if art:
            return candidate
    return None


def find_existing_job_for_address(session: Session, address: str) -> Job | None:
    """Find a non-failed job for *address* (case-insensitive)."""
    return session.execute(
        select(Job)
        .where(
            func.lower(Job.address) == address.lower(),
            Job.status != JobStatus.failed,
        )
        .limit(1)
    ).scalar_one_or_none()


def is_known_proxy(session: Session, address: str) -> bool:
    """Return True if *address* has been classified as a proxy in any prior analysis."""
    return (
        session.execute(
            select(Contract)
            .where(
                func.lower(Contract.address) == address.lower(),
                Contract.is_proxy.is_(True),
            )
            .limit(1)
        ).scalar_one_or_none()
        is not None
    )


def copy_static_cache(session: Session, source_job_id: Any, target_job_id: Any) -> int | None:
    """Copy all cached static data from *source_job_id* to *target_job_id*.

    Copies:
    - ``contracts`` row (immutable fields only; proxy fields left as defaults)
    - ``source_files`` rows
    - ``contract_summaries``, ``privileged_functions``, ``role_definitions``
      rows (linked to the new contract row)
    - Static artifacts (``contract_analysis``, ``slither_results``,
      ``analysis_report``, ``control_tracking_plan``)

    Returns the new ``Contract.id`` on success, or ``None`` on failure.
    """
    # Guard: if the target already has a contract row, return early.
    existing = session.execute(select(Contract).where(Contract.job_id == target_job_id).limit(1)).scalar_one_or_none()
    if existing:
        return existing.id

    src_contract = session.execute(
        select(Contract).where(Contract.job_id == source_job_id).limit(1)
    ).scalar_one_or_none()
    if not src_contract:
        return None

    # Check if a Contract with the same (address, chain) already exists
    # (unique constraint from main). If so, update it instead of inserting.
    existing_by_addr = session.execute(
        select(Contract)
        .where(
            Contract.address == src_contract.address,
            Contract.chain == src_contract.chain,
        )
        .limit(1)
    ).scalar_one_or_none()

    reused_existing = False
    if existing_by_addr:
        # Update the existing row to point to the new job
        existing_by_addr.job_id = target_job_id
        # Save the source proxy state before resetting, so _check_proxy_cache
        # can read it from an artifact to decide if re-resolution is needed.
        _cached_proxy_state = {
            "is_proxy": existing_by_addr.is_proxy,
            "proxy_type": existing_by_addr.proxy_type,
            "implementation": existing_by_addr.implementation,
            "beacon": existing_by_addr.beacon,
            "admin": existing_by_addr.admin,
        }
        store_artifact(session, target_job_id, "cached_proxy_state", data=_cached_proxy_state)
        # Reset mutable proxy fields to defaults (will be re-resolved)
        existing_by_addr.is_proxy = False
        existing_by_addr.proxy_type = None
        existing_by_addr.implementation = None
        existing_by_addr.beacon = None
        existing_by_addr.admin = None
        # Copy over immutable fields from source (skip mutable proxy fields)
        from sqlalchemy import inspect as sa_inspect

        mapper = sa_inspect(Contract)
        for attr in mapper.column_attrs:
            key = attr.key
            col = attr.columns[0]
            if col.primary_key or key in _MUTABLE_CONTRACT_FIELDS:
                continue
            if key in ("job_id", "address", "chain", "protocol_id"):
                continue
            if col.server_default is not None:
                continue
            val = getattr(src_contract, key)
            if val is not None:
                setattr(existing_by_addr, key, val)
        session.flush()
        new_contract = existing_by_addr
        reused_existing = True
    else:
        # --- contract (exclude mutable proxy fields) ---
        new_contract = copy_row(
            session,
            src_contract,
            exclude=_MUTABLE_CONTRACT_FIELDS,
            job_id=target_job_id,
            protocol_id=None,
        )
        session.flush()

    # --- source files ---
    src_files = session.execute(select(SourceFile).where(SourceFile.job_id == source_job_id)).scalars().all()
    for sf in src_files:
        copy_row(session, sf, job_id=target_job_id)

    # --- contract child tables (skip if we reused an existing contract that
    # already owns the child rows, e.g. when source == existing_by_addr) ---
    if not reused_existing or new_contract.id != src_contract.id:  # type: ignore[attr-defined]
        for Model in (ContractSummary, PrivilegedFunction, RoleDefinition):
            src_rows = session.execute(select(Model).where(Model.contract_id == src_contract.id)).scalars().all()
            for row in src_rows:
                copy_row(session, row, contract_id=new_contract.id)  # type: ignore[attr-defined]

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
        store_artifact(session, target_job_id, art.name, data=art.data, text_data=art.text_data)

    session.commit()
    return new_contract.id  # type: ignore[attr-defined]
