"""Job lifecycle: list, create, fetch, cancel, stage timings."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import not_ as sa_not_
from sqlalchemy import select, text

from db.models import Artifact, Contract, Job, JobStage, Protocol
from schemas.api_requests import AnalyzeRequest
from schemas.stage_errors import StageError, StageErrors

from . import deps

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/jobs")
def list_jobs() -> list[dict[str, Any]]:
    with deps.SessionLocal() as session:
        stmt = select(Job).order_by(Job.created_at.desc())
        jobs = session.execute(stmt).scalars().all()
        return [job.to_dict() for job in jobs]


@router.post("/api/analyze", dependencies=[Depends(deps.require_admin_key)])
def analyze_address(request: AnalyzeRequest) -> dict[str, Any]:
    if request.address and not request.address.startswith("0x"):
        raise HTTPException(status_code=400, detail="Address must start with 0x")
    with deps.SessionLocal() as session:
        req_dict = request.model_dump()
        if request.dapp_urls:
            job = deps.create_job(session, req_dict, initial_stage=JobStage.dapp_crawl)
        elif request.defillama_protocol:
            job = deps.create_job(session, req_dict, initial_stage=JobStage.defillama_scan)
        else:
            job = deps.create_job(session, req_dict)
        return job.to_dict()


@router.post(
    "/api/company/{company_name}/analyze-remaining",
    dependencies=[Depends(deps.require_admin_key)],
)
def analyze_remaining(company_name: str) -> dict[str, Any]:
    """Queue analysis jobs for all discovered-but-not-analyzed contracts in a company."""
    with deps.SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
            raise HTTPException(status_code=404, detail="Company not found")

        # Exclude backfilled historical impls — these rows exist only to
        # anchor audit-coverage matching, not to be re-analyzed. Analyzing
        # them would waste pipeline cycles on bytecode nobody's using.
        # ``discovery_sources.contains(['upgrade_history'])`` emits
        # Postgres ``@>``; NULL guard covers pre-array legacy rows.
        unanalyzed = (
            session.execute(
                select(Contract).where(
                    Contract.protocol_id == protocol_row.id,
                    Contract.job_id.is_(None),
                    Contract.discovery_sources.is_(None)
                    | sa_not_(Contract.discovery_sources.contains(["upgrade_history"])),
                )
            )
            .scalars()
            .all()
        )

        queued = []
        for contract in unanalyzed:
            # Re-check inside the loop so concurrent calls (double-click or
            # duplicate request) don't each create a job for the same contract.
            session.refresh(contract, attribute_names=["job_id"])
            if contract.job_id is not None:
                continue
            existing = deps.find_existing_job_for_address(session, contract.address, chain=contract.chain)
            if existing is not None:
                contract.job_id = existing.id
                session.commit()
                continue
            req_dict = {
                "address": contract.address,
                "name": contract.contract_name or f"{company_name}_{contract.address[2:10]}",
                "chain": contract.chain,
                "protocol_id": protocol_row.id,
                "company": company_name,
            }
            job = deps.create_job(session, req_dict)
            contract.job_id = job.id
            session.commit()
            queued.append({"job_id": str(job.id), "address": contract.address})

        return {"queued": len(queued), "jobs": queued}


@router.delete(
    "/api/company/{company_name}/queued-jobs",
    dependencies=[Depends(deps.require_admin_key)],
)
def cancel_queued_company_jobs(company_name: str) -> dict[str, Any]:
    """Cancel queued jobs for a company; leaves processing/completed/failed untouched."""
    with deps.SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
            raise HTTPException(status_code=404, detail="Company not found")
        result = session.execute(
            text(
                """
                DELETE FROM jobs
                WHERE company = :company AND status = 'queued'
                RETURNING id
                """
            ),
            {"company": company_name},
        )
        deleted = [str(row_id) for (row_id,) in result]
        session.commit()
    return {"company": company_name, "cancelled": len(deleted), "job_ids": deleted}


@router.delete(
    "/api/company/{company_name}/addresses/{address}",
    dependencies=[Depends(deps.require_admin_key)],
)
def delete_company_address(company_name: str, address: str) -> dict[str, Any]:
    """Remove a Contract row from a protocol.

    Scoped to the protocol so unrelated contracts sharing an address (very
    rare — addresses are chain-global but we key by address only) aren't
    affected. FK cascades on ``contracts.id`` clean up the audit coverage
    rows and any upgrade-event attribution.
    """
    if not deps._ADDRESS_RE.match(address):
        raise HTTPException(status_code=400, detail="Invalid address")
    with deps.SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
            raise HTTPException(status_code=404, detail="Company not found")
        contract = session.execute(
            select(Contract).where(
                Contract.protocol_id == protocol_row.id,
                Contract.address == address,
            )
        ).scalar_one_or_none()
        if contract is None:
            raise HTTPException(status_code=404, detail="Address not found for this protocol")
        session.delete(contract)
        session.commit()
    return {"company": company_name, "address": address, "deleted": True}


@router.get("/api/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    with deps.SessionLocal() as session:
        job = session.get(Job, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job.to_dict()


class JobErrorsResponse(BaseModel):
    """Response shape for ``GET /api/jobs/{job_id}/errors``."""

    job_id: str
    trace_id: str | None
    status: str
    stage: str
    errors: list[StageError]


@router.get("/api/jobs/{job_id}/errors", response_model=JobErrorsResponse)
def get_job_errors(job_id: str) -> JobErrorsResponse:
    """Return the deserialized ``stage_errors`` artifact for a job.

    Returns an empty list when the artifact is missing — every job either
    has zero degraded events and zero failures, or it has the artifact
    documenting them. A 404 is reserved for "no such job".
    """
    # Job.id is a UUID column; a non-UUID string would otherwise raise
    # ``DataError`` at the dialect level — surface as 404 instead so the
    # endpoint matches the rest of the job-routes' behaviour for bad ids.
    import uuid as _uuid

    try:
        parsed = _uuid.UUID(job_id)
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=404, detail="Job not found") from exc
    with deps.SessionLocal() as session:
        job = session.get(Job, parsed)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        raw = deps.get_artifact(session, job.id, "stage_errors")
        errors: list[StageError] = []
        if isinstance(raw, dict):
            try:
                errors = StageErrors.model_validate(raw).errors
            except Exception as exc:
                # Legacy/corrupt payloads shouldn't 500 the endpoint —
                # return them empty and let the operator inspect the
                # underlying artifact directly.
                logger.warning(
                    "stage_errors artifact for job %s did not validate: %s",
                    job.id,
                    exc,
                    extra={"exc_type": type(exc).__name__},
                )
                errors = []
        return JobErrorsResponse(
            job_id=str(job.id),
            trace_id=job.trace_id,
            status=job.status.value,
            stage=job.stage.value,
            errors=errors,
        )


@router.get("/api/jobs/{job_id}/stage_timings", dependencies=[Depends(deps.require_admin_key)])
def get_job_stage_timings(job_id: str) -> dict[str, Any]:
    """Return all per-stage timing artifacts the worker fleet wrote for
    this job, keyed by stage name. Schema-v2 layout (one
    ``stage_timing_<stage>`` artifact per stage). Used by the bench
    harness to populate ``worker_elapsed_seconds`` reliably without
    scraping Fly logs.

    Admin-protected because per-job timings expose internal worker_id /
    runtime metadata.
    """
    with deps.SessionLocal() as session:
        job = session.get(Job, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        # Escape `_` so the legacy `stage_timings` artifact doesn't match this prefix scan.
        rows = (
            session.execute(
                select(Artifact).where(
                    Artifact.job_id == job.id,
                    Artifact.name.like(r"stage\_timing\_%", escape="\\"),
                )
            )
            .scalars()
            .all()
        )
        # Read everything we need off the rows before releasing the session
        # so the storage fan-out below doesn't pin a DB connection during
        # slow HTTP I/O.
        resolved_job_id = str(job.id)
        inline_values: dict[str, Any] = {}
        storage_lookups: dict[str, tuple[str, str | None]] = {}
        for row in rows:
            stage = row.name[len("stage_timing_") :]
            if row.storage_key:
                storage_lookups[stage] = (row.storage_key, row.content_type)
            elif row.data is not None:
                inline_values[stage] = row.data
            elif row.text_data is not None:
                inline_values[stage] = row.text_data

    timings: dict[str, Any] = {stage: v for stage, v in inline_values.items() if isinstance(v, dict)}
    if storage_lookups:
        client = deps.get_storage_client()
        if client is None:
            # Storage env stripped after rows were written. Degrade to inline-only
            # rather than 500 — the SPA copes with a partial timings map.
            logger.warning(
                "stage_timings on job %s reference storage_key but storage is not configured; "
                "returning inline timings only",
                resolved_job_id,
            )
        else:
            bodies = client.get_many([key for key, _ in storage_lookups.values()])
            for stage, (key, content_type) in storage_lookups.items():
                body = bodies.get(key)
                if body is None:
                    continue
                value = deps.deserialize_artifact(body, content_type)
                if isinstance(value, dict):
                    timings[stage] = value

    return {"job_id": resolved_job_id, "stage_timings": timings}
