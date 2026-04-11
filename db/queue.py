"""Postgres-based job queue operations using SELECT ... FOR UPDATE SKIP LOCKED."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from .models import Artifact, Job, JobStage, JobStatus, SourceFile


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
