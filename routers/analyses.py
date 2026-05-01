"""Analysis listing, detail, and artifact endpoints."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy import select

from db.models import Artifact, Contract, Job, JobStatus
from services.aggregations import build_analysis_detail
from services.governance.proxies import _merge_proxy_impl_entries

from . import deps

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/analyses")
def analyses(response: Response) -> list[dict]:
    """List completed analyses with their available artifacts."""
    # Read-mostly listing — let the browser reuse it across navigations.
    # Short max-age + SWR keeps freshness while letting back/forward and
    # rapid re-renders avoid a network round-trip for the multi-MB payload.
    response.headers["Cache-Control"] = "private, max-age=15, stale-while-revalidate=60"
    with deps.SessionLocal() as session:
        stmt = select(Job).where(Job.status == JobStatus.completed).order_by(Job.updated_at.desc())
        jobs = session.execute(stmt).scalars().all()

        jobs_by_id = {str(job.id): job for job in jobs}
        jobs_by_address: dict[str, Job] = {}
        for job in jobs:
            if job.address:
                jobs_by_address.setdefault(job.address.lower(), job)

        # Rank scores, chains, name, proxy_type, implementation come from
        # the ``contracts`` table. is_proxy comes from Job (denormalized via
        # store_artifact). Pulling all of these from columns lets us skip
        # the per-job ``contract_flags`` storage GET entirely — at 25ms
        # production RTT × N jobs, that GET batch was the dominant cost
        # of this endpoint after the parallel-fanout commit.
        contracts_by_address: dict[str, Contract] = {}
        addresses_from_jobs = list(jobs_by_address.keys())
        if addresses_from_jobs:
            for c in session.execute(select(Contract).where(Contract.address.in_(addresses_from_jobs))).scalars():
                addr_lower = (c.address or "").lower()
                if addr_lower:
                    contracts_by_address.setdefault(addr_lower, c)

        job_ids = [job.id for job in jobs]
        artifact_names_by_job: dict[Any, list[str]] = {}
        # Resolve inline rows in-place; defer storage rows so we can fan out
        # the HTTP GETs after the DB session is released.
        inline_resolved: dict[tuple[Any, str], Any] = {}
        storage_lookups: dict[tuple[Any, str], tuple[str, str | None]] = {}
        if job_ids:
            for art in session.execute(select(Artifact).where(Artifact.job_id.in_(job_ids))).scalars():
                artifact_names_by_job.setdefault(art.job_id, []).append(art.name)
                # contract_flags is no longer fetched: every field the listing
                # reads from it (is_proxy/proxy_type/implementation) is on
                # Job/Contract columns above.
                if art.name == "contract_analysis":
                    key = (art.job_id, art.name)
                    if art.storage_key:
                        storage_lookups[key] = (art.storage_key, art.content_type)
                    elif art.data is not None:
                        inline_resolved[key] = art.data
                    elif art.text_data is not None:
                        inline_resolved[key] = art.text_data

    # Session closed. Fan out the storage GETs in parallel and decode bodies.
    # ``get_many`` swallows per-key transport errors and returns ``None`` for
    # them, so partial bucket failure degrades per-row instead of wiping the
    # whole response — a non-storage entry still renders.
    resolved: dict[tuple[Any, str], Any] = dict(inline_resolved)
    if storage_lookups:
        client = deps.get_storage_client()
        if client is None:
            logger.warning(
                "/api/analyses: %d artifact(s) reference storage_key but storage is not configured; "
                "degrading to inline-only response",
                len(storage_lookups),
            )
        else:
            keys = [sk for sk, _ in storage_lookups.values()]
            bodies = client.get_many(keys)
            for cache_key, (storage_key, content_type) in storage_lookups.items():
                body = bodies.get(storage_key)
                if body is None:
                    logger.warning("artifact %s storage read failed for job %s", cache_key[1], cache_key[0])
                    continue
                resolved[cache_key] = deps.deserialize_artifact(body, content_type)

    def _value(job_id: Any, name: str) -> Any:
        return resolved.get((job_id, name))

    def company_for_job(job: Job) -> str | None:
        seen: set[str] = set()
        current: Job | None = job
        while current is not None:
            if current.company:
                return current.company
            request = current.request if isinstance(current.request, dict) else {}
            parent_job_id = request.get("parent_job_id")
            if not isinstance(parent_job_id, str) or parent_job_id in seen:
                return None
            seen.add(parent_job_id)
            current = jobs_by_id.get(parent_job_id)
        return None

    results = []
    for job in jobs:
        run_name = job.name or str(job.id)
        analysis_artifact = _value(job.id, "contract_analysis")
        request = job.request if isinstance(job.request, dict) else {}
        parent_job_id = request.get("parent_job_id")
        company = company_for_job(job)
        addr_lower = (job.address or "").lower()
        contract = contracts_by_address.get(addr_lower)
        entry: dict[str, Any] = {
            "run_name": run_name,
            "job_id": str(job.id),
            "address": job.address,
            "chain": request.get("chain") or (contract.chain if contract else None),
            "company": company,
            "parent_job_id": parent_job_id,
            "rank_score": (float(contract.rank_score) if contract and contract.rank_score is not None else None),
            "is_proxy": bool(job.is_proxy),
            "proxy_type": contract.proxy_type if contract else None,
            "implementation_address": contract.implementation if contract else None,
            "proxy_address": request.get("proxy_address"),
        }
        entry["available_artifacts"] = sorted(artifact_names_by_job.get(job.id, []))

        # Hide proxy entries until the impl is completed — otherwise the
        # listing renders a half-populated card that mutates once the impl
        # lands. jobs_by_address only carries completed jobs. The impl's
        # contract_analysis was prefetched above (every job's artifacts go
        # through the same batch), so the lookup is a dict hit, not an
        # extra HTTP round-trip.
        if entry["is_proxy"] and entry["implementation_address"]:
            impl_job = jobs_by_address.get(entry["implementation_address"].lower())
            if impl_job is None:
                continue
            if not isinstance(analysis_artifact, dict):
                analysis_artifact = _value(impl_job.id, "contract_analysis")

        if isinstance(analysis_artifact, dict):
            subject = analysis_artifact.get("subject", {})
            entry["contract_name"] = subject.get("name", run_name)
            entry["summary"] = analysis_artifact.get("summary")
        elif contract and contract.contract_name:
            # Storage GET missed (transport blip) but we have the name on
            # the prefetched Contract row — keep the listing populated.
            entry["contract_name"] = contract.contract_name
        results.append(entry)
    return _merge_proxy_impl_entries(results)


@router.get("/api/analyses/{run_name:path}/artifact/{artifact_name:path}")
def analysis_artifact(run_name: str, artifact_name: str):
    """Get a specific artifact for an analysis.

    Storage-backed artifacts are fetched from object storage transparently;
    inline (legacy) artifacts are served from Postgres. Either way, the body
    is returned directly to the client.
    """
    with deps.SessionLocal() as session:
        # Find job by name or id or address
        stmt = select(Job).where(Job.name == run_name).order_by(Job.updated_at.desc()).limit(1)
        job = session.execute(stmt).scalar_one_or_none()
        if job is None:
            try:
                job = session.get(Job, run_name)
            except Exception:
                session.rollback()
        if job is None:
            job = session.execute(
                select(Job)
                .where(Job.address == run_name, Job.status == JobStatus.completed)
                .order_by(Job.updated_at.desc())
                .limit(1)
            ).scalar_one_or_none()
        if job is None:
            raise HTTPException(status_code=404, detail="Analysis not found")

        # Strip .json/.txt extension for artifact lookup
        lookup_name = artifact_name
        if artifact_name.endswith(".json"):
            lookup_name = artifact_name[:-5]
        elif artifact_name.endswith(".txt"):
            lookup_name = artifact_name[:-4]

        artifact: Any = None
        try:
            artifact = deps.get_artifact(session, job.id, lookup_name)
            if artifact is None:
                artifact = deps.get_artifact(session, job.id, artifact_name)
        except Exception as exc:
            # Storage backend can be transiently unreachable (MinIO/Tigris
            # outage, expired credentials, missing object). Don't 500 — log
            # and fall through to the per-artifact synthesis fallback.
            logger.warning("artifact %s for job %s unreadable: %s", lookup_name, job.id, exc)

        # upgrade_history is reproducible from UpgradeEvent rows. When the
        # stored artifact is gone or storage is down, regenerate from the
        # relational source so the per-proxy detail view stays usable.
        if artifact is None and lookup_name == "upgrade_history":
            from services.discovery.upgrade_history import synthesize_from_events

            contract = session.execute(select(Contract).where(Contract.job_id == job.id).limit(1)).scalar_one_or_none()
            if contract is not None:
                artifact = synthesize_from_events(session, contract)

        if artifact is None:
            raise HTTPException(status_code=404, detail="Artifact not found")

        if isinstance(artifact, (dict, list)):
            return JSONResponse(content=artifact)
        return PlainTextResponse(str(artifact))


@router.get("/api/analyses/{run_name:path}")
def analysis_detail(run_name: str) -> dict:
    """Get analysis detail by job name (run_name) or job_id."""
    with deps.SessionLocal() as session:
        payload = build_analysis_detail(session, run_name)
        if payload is None:
            raise HTTPException(status_code=404, detail="Analysis not found")
        return payload
