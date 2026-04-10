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
    SlitherFinding,
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

# Artifact names that constitute cached static data.
_STATIC_ARTIFACT_NAMES = frozenset(
    {
        "contract_analysis",
        "slither_results",
        "analysis_report",
        "control_tracking_plan",
    }
)

# Contract columns that are mutable (resolved live by _resolve_proxy) and
# must NOT be carried over from a cached job.
_MUTABLE_CONTRACT_FIELDS = frozenset(
    {"is_proxy", "proxy_type", "implementation", "beacon", "admin"}
)


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
            select(Artifact).where(
                Artifact.job_id == candidate.id, Artifact.name == "contract_analysis"
            ).limit(1)
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


def copy_static_cache(session: Session, source_job_id: Any, target_job_id: Any) -> int | None:
    """Copy all cached static data from *source_job_id* to *target_job_id*.

    Copies:
    - ``contracts`` row (immutable fields only; proxy fields left as defaults)
    - ``source_files`` rows
    - ``contract_summaries``, ``privileged_functions``, ``role_definitions``,
      ``slither_findings`` rows (linked to the new contract row)
    - Static artifacts (``contract_analysis``, ``slither_results``,
      ``analysis_report``, ``control_tracking_plan``)

    Returns the new ``Contract.id`` on success, or ``None`` on failure.
    """
    src_contract = session.execute(
        select(Contract).where(Contract.job_id == source_job_id).limit(1)
    ).scalar_one_or_none()
    if not src_contract:
        return None

    # Guard: if the target already has a contract row, return early.
    existing = session.execute(
        select(Contract).where(Contract.job_id == target_job_id).limit(1)
    ).scalar_one_or_none()
    if existing:
        return existing.id

    # --- contract (exclude mutable proxy fields) ---
    new_contract = copy_row(
        session, src_contract,
        exclude=_MUTABLE_CONTRACT_FIELDS,
        job_id=target_job_id,
        protocol_id=None,
    )
    session.flush()

    # --- source files ---
    src_files = session.execute(
        select(SourceFile).where(SourceFile.job_id == source_job_id)
    ).scalars().all()
    for sf in src_files:
        copy_row(session, sf, job_id=target_job_id)

    # --- contract child tables ---
    for Model in (ContractSummary, PrivilegedFunction, RoleDefinition, SlitherFinding):
        src_rows = session.execute(
            select(Model).where(Model.contract_id == src_contract.id)
        ).scalars().all()
        for row in src_rows:
            copy_row(session, row, contract_id=new_contract.id)

    # --- artifacts ---
    src_artifacts = session.execute(
        select(Artifact).where(
            Artifact.job_id == source_job_id,
            Artifact.name.in_(_STATIC_ARTIFACT_NAMES),
        )
    ).scalars().all()
    for art in src_artifacts:
        store_artifact(session, target_job_id, art.name, data=art.data, text_data=art.text_data)

    session.commit()
    return new_contract.id
