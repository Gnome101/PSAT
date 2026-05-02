#!/usr/bin/env python3
"""FastAPI server for launching and browsing PSAT analyses."""

from __future__ import annotations

import hmac
import logging
import os
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import func, select, text
from sqlalchemy.orm import selectinload
from starlette.types import Scope

from db.models import (
    Artifact,
    Contract,
    ControlGraphEdge,
    ControlGraphNode,
    ControllerValue,
    EffectiveFunction,
    FunctionPrincipal,
    Job,
    JobStage,
    JobStatus,
    MonitoredContract,
    MonitoredEvent,
    Protocol,
    ProtocolSubscription,
    ProxySubscription,
    ProxyUpgradeEvent,
    SessionLocal,
    TvlSnapshot,
    UpgradeEvent,
    WatchedProxy,
)
from db.queue import (
    create_job,
    find_existing_job_for_address,
    get_all_artifacts,
    get_artifact,
)
from db.storage import StorageError, StorageUnavailable, deserialize_artifact, get_storage_client

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent
SITE_DIR = ROOT_DIR / "site"
SITE_DIST_DIR = SITE_DIR / "dist"
SITE_ASSETS_DIR = SITE_DIST_DIR / "assets"

DEFAULT_RPC_URL = os.environ.get("ETH_RPC", "https://ethereum-rpc.publicnode.com")
MAX_TVL_HISTORY_DAYS = 90
GENERIC_PROXY_NAMES = {
    "uupsproxy",
    "erc1967proxy",
    "transparentupgradeableproxy",
    "proxy",
    "beaconproxy",
    "ossifiableproxy",
    "withdrawalsmanagerproxy",
    "upgradeablebeacon",
}


class AnalyzeRequest(BaseModel):
    address: str | None = Field(default=None, min_length=42, max_length=42)
    company: str | None = Field(default=None, min_length=1)
    dapp_urls: list[str] | None = None
    defillama_protocol: str | None = Field(default=None, min_length=1)
    name: str | None = None
    chain: str | None = None
    chain_id: int | None = Field(default=None, ge=1)
    wait: int | None = Field(default=None, ge=1, le=120)
    analyze_limit: int = Field(default=5, ge=1, le=200)
    rpc_url: str | None = None
    force: bool = Field(
        default=False,
        description="Bench-only: skip the static-cache discovery shortcut so every stage re-runs cold.",
    )

    @model_validator(mode="after")
    def _validate_target(self) -> "AnalyzeRequest":
        # address + company is allowed (address is target, company is context)
        primary = [self.address, self.dapp_urls, self.defillama_protocol]
        company_only = self.company and not any(primary)
        has_primary = sum(bool(t) for t in primary) == 1
        if not has_primary and not company_only:
            raise ValueError("Provide exactly one of: address, company, dapp_urls, defillama_protocol")
        return self


class WatchProxyRequest(BaseModel):
    address: str = Field(min_length=42, max_length=42)
    chain: str = "ethereum"
    label: str | None = None
    rpc_url: str | None = None
    from_block: int | None = Field(
        default=None, ge=0, description="Block to start scanning from. Defaults to current block."
    )
    discord_webhook_url: str | None = Field(default=None, description="Discord webhook URL for upgrade notifications.")


class SubscribeRequest(BaseModel):
    discord_webhook_url: str = Field(min_length=1, description="Discord webhook URL for upgrade notifications.")
    label: str | None = None


class ProtocolSubscribeRequest(BaseModel):
    discord_webhook_url: str = Field(min_length=1, description="Discord webhook URL for protocol event notifications.")
    label: str | None = None
    event_filter: dict | None = Field(default=None, description='Optional filter: {"event_types": ["upgraded", ...]}')

    @field_validator("event_filter")
    @classmethod
    def validate_event_filter(cls, v: dict | None) -> dict | None:
        if v is None:
            return v
        if "event_types" not in v:
            raise ValueError(
                'event_filter must contain an \'event_types\' key, e.g. {"event_types": ["upgraded", "paused"]}'
            )
        event_types = v["event_types"]
        if not isinstance(event_types, list):
            raise ValueError(f"event_filter.event_types must be a list of strings, got {type(event_types).__name__}")
        from services.monitoring.event_topics import ALL_EVENT_TOPICS

        valid_types = set(ALL_EVENT_TOPICS.values()) | {"state_changed_poll"}
        for et in event_types:
            if not isinstance(et, str):
                raise ValueError(f"event_filter.event_types entries must be strings, got {type(et).__name__}")
            if et not in valid_types:
                raise ValueError(f"Unknown event type: '{et}'. Valid types: {sorted(valid_types)}")
        return v


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Verify DB is reachable on startup."""
    try:
        from db.models import engine

        with engine.connect() as conn:
            conn.execute(select(1))
        logger.info("Database connection verified")
    except Exception:
        logger.warning("Database not reachable at startup — endpoints will fail until DB is available")
    yield


ADMIN_KEY = os.environ.get("PSAT_ADMIN_KEY")
if not ADMIN_KEY:
    logger.warning(
        "PSAT_ADMIN_KEY is not set — write endpoints will reject every request. "
        "Set PSAT_ADMIN_KEY in the environment to enable admin operations."
    )


def require_admin_key(x_psat_admin_key: str | None = Header(default=None)) -> None:
    """Reject any non-GET request that does not carry a valid admin key."""
    if not ADMIN_KEY or not x_psat_admin_key or not hmac.compare_digest(x_psat_admin_key, ADMIN_KEY):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Admin key required")


_raw_origins = os.environ.get("PSAT_SITE_ORIGIN", "")
ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]
if not ALLOWED_ORIGINS:
    logger.warning(
        "PSAT_SITE_ORIGIN is not set — CORS will deny all cross-origin requests. "
        "Set PSAT_SITE_ORIGIN to a comma-separated list of allowed origins."
    )

app = FastAPI(title="PSAT Demo", version="0.1.0", lifespan=lifespan)
# Compress JSON > 1KB on the wire. /api/company/{name} routinely returns
# 1-3 MB of nested control-graph data; gzip cuts it ~5-10x and is the single
# largest win for the company page's perceived load time.
app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=6)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "X-PSAT-Admin-Key"],
)


class _ImmutableStaticFiles(StaticFiles):
    """StaticFiles that stamps a 1-year immutable Cache-Control on every
    response. Vite emits hashed filenames (``index-<hash>.js``) so the URL
    changes whenever content changes — caching forever is correct, and lets
    repeat visitors skip the ~2MB bundle download entirely.
    """

    async def get_response(self, path: str, scope: Scope):  # type: ignore[override]
        response = await super().get_response(path, scope)
        if response.status_code == 200:
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


if SITE_ASSETS_DIR.exists():
    app.mount("/assets", _ImmutableStaticFiles(directory=SITE_ASSETS_DIR), name="assets")


def _site_index_response():
    # The HTML embeds hash-stamped asset URLs (`/assets/index-<hash>.js`)
    # that change on every build, so it must NOT be cached — otherwise a
    # post-deploy reload would keep pointing at old, evicted bundles.
    headers = {"Cache-Control": "no-cache, must-revalidate"}
    dist_index = SITE_DIST_DIR / "index.html"
    source_index = SITE_DIR / "index.html"
    if dist_index.exists():
        return FileResponse(dist_index, headers=headers)
    if source_index.exists():
        return FileResponse(source_index, headers=headers)
    return PlainTextResponse(
        "Frontend build not found. Run `cd site && npm run build` or start the "
        "Vite dev server with `cd site && npm run dev`.",
        status_code=503,
    )


def _function_principal_payload(fp: FunctionPrincipal) -> dict[str, Any]:
    return {
        "address": fp.address,
        "resolved_type": fp.resolved_type,
        "source_controller_id": fp.origin,
        "details": fp.details or {},
    }


def _is_generic_authority_contract_principal(principal: dict[str, Any]) -> bool:
    details = principal.get("details")
    return (
        principal.get("resolved_type") == "contract"
        and isinstance(details, dict)
        and bool(details.get("authority_kind"))
    )


def _role_value_from_origin(origin: str | None) -> int | str:
    prefix = "role "
    if not origin:
        return "?"
    if origin.startswith(prefix):
        suffix = origin[len(prefix) :]
        if suffix.isdigit():
            return int(suffix)
        return suffix or "?"
    return origin


def _build_company_function_entry(ef: EffectiveFunction, principals: list[FunctionPrincipal]) -> dict[str, Any]:
    direct_owner = None
    controllers_by_label: dict[str, dict[str, Any]] = {}
    authority_roles_by_key: dict[str, dict[str, Any]] = {}

    for fp in principals:
        principal_dict = _function_principal_payload(fp)

        if fp.principal_type == "direct_owner":
            if direct_owner is None:
                direct_owner = principal_dict
            continue

        if fp.principal_type == "authority_role":
            role_value = _role_value_from_origin(fp.origin)
            role_entry = authority_roles_by_key.setdefault(
                str(role_value),
                {
                    "role": role_value,
                    "principals": [],
                },
            )
            role_entry["principals"].append(principal_dict)
            continue

        label = fp.origin or "controller"
        controller_entry = controllers_by_label.setdefault(
            label,
            {
                "label": label,
                "controller_id": label,
                "source": label,
                "principals": [],
            },
        )
        controller_entry["principals"].append(principal_dict)

    authority_roles = list(authority_roles_by_key.values())
    if not authority_roles and ef.authority_roles:
        authority_roles = list(ef.authority_roles)

    controllers = list(controllers_by_label.values())
    has_more_specific_controller = any(
        any(not _is_generic_authority_contract_principal(principal) for principal in entry.get("principals", []))
        for entry in controllers
    )
    if has_more_specific_controller:
        controllers = [
            entry
            for entry in controllers
            if not entry.get("principals")
            or not all(_is_generic_authority_contract_principal(principal) for principal in entry["principals"])
        ]

    return {
        "function": ef.abi_signature or ef.function_name,
        "selector": ef.selector,
        "effect_labels": list(ef.effect_labels or []),
        "effect_targets": list(ef.effect_targets or []),
        "action_summary": ef.action_summary,
        "authority_public": ef.authority_public,
        "controllers": controllers,
        "authority_roles": authority_roles,
        "direct_owner": direct_owner,
    }


def _display_name(entry: dict[str, Any]) -> str:
    chain = str(entry.get("chain") or "").strip()

    def with_chain(name: str) -> str:
        if not name:
            return name
        if not chain:
            return name
        suffix = f" ({chain})"
        return name if name.endswith(suffix) else f"{name}{suffix}"

    explicit = str(entry.get("display_name") or "").strip()
    if explicit:
        return with_chain(explicit)
    contract_name = str(entry.get("contract_name") or "").strip()
    if contract_name and contract_name.lower() not in GENERIC_PROXY_NAMES:
        return with_chain(contract_name)
    return with_chain(str(entry.get("run_name") or contract_name or "").strip())


def _merge_proxy_impl_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    impl_by_proxy: dict[str, dict[str, Any]] = {}
    merged_proxies: set[str] = set()

    for entry in entries:
        proxy_address = str(entry.get("proxy_address") or "").lower()
        if proxy_address:
            impl_by_proxy[proxy_address] = entry

    merged: list[dict[str, Any]] = []
    for entry in entries:
        proxy_address = str(entry.get("proxy_address") or "").lower()
        if proxy_address:
            continue

        address = str(entry.get("address") or "").lower()
        impl = impl_by_proxy.get(address)
        if entry.get("is_proxy") and entry.get("implementation_address") and impl:
            merged.append(
                {
                    **impl,
                    "company": entry.get("company") or impl.get("company"),
                    "chain": entry.get("chain") or impl.get("chain"),
                    "rank_score": entry.get("rank_score")
                    if entry.get("rank_score") is not None
                    else impl.get("rank_score"),
                    "proxy_address": entry.get("address"),
                    "proxy_address_display": entry.get("address"),
                    "proxy_type_display": entry.get("proxy_type"),
                    "display_name": impl.get("contract_name") or _display_name(entry),
                }
            )
            merged_proxies.add(address)
            continue

        merged.append({**entry, "display_name": _display_name(entry)})

    for entry in entries:
        proxy_address = str(entry.get("proxy_address") or "").lower()
        if proxy_address and proxy_address not in merged_proxies:
            merged.append({**entry, "display_name": _display_name(entry)})

    return merged


@app.get("/")
def index():
    return _site_index_response()


@app.get("/api/health")
def health():
    """Liveness/readiness probe — verifies the database and object storage are reachable."""
    from fastapi.responses import JSONResponse

    from db.models import engine as _engine

    body: dict[str, Any] = {"status": "ok", "db": "ok", "storage": "inline"}
    failures: list[str] = []

    try:
        with SessionLocal() as session:
            # Cap the probe at 2s so a hung Postgres can't hang the health endpoint.
            # SET LOCAL statement_timeout is Postgres-specific syntax.
            session.execute(text("SET LOCAL statement_timeout = 2000"))
            session.execute(select(1))
    except Exception:
        logger.exception("Health check: db unreachable")
        body["db"] = "unavailable"
        failures.append("db")

    # NullPool (used in some test setups) lacks these counters.
    from sqlalchemy.pool import QueuePool

    if isinstance(_engine.pool, QueuePool):
        pool = _engine.pool
        body["pool"] = {
            "size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
        }

    storage_client = get_storage_client()
    if storage_client is not None:
        try:
            storage_client.health_check()
            body["storage"] = "ok"
        except StorageUnavailable:
            logger.exception("Health check: storage unreachable")
            body["storage"] = "unavailable"
            failures.append("storage")

    if failures:
        body["status"] = "unavailable"
        return JSONResponse(body, status_code=503)
    return body


@app.get("/api/version")
def version() -> dict[str, str]:
    """Returns the deployed git SHA. Used by post-deploy smoke checks to confirm
    the running image matches the commit that triggered the deploy."""
    return {"sha": os.environ.get("GIT_SHA", "unknown")}


@app.get("/api/config")
def config() -> dict[str, str]:
    return {"default_rpc_url": DEFAULT_RPC_URL}


@app.get("/api/stats")
def pipeline_stats() -> dict[str, Any]:
    """Quick stats: unique addresses stored, total jobs, etc."""
    from sqlalchemy import distinct, func

    with SessionLocal() as session:
        unique_addresses = (
            session.execute(select(func.count(distinct(Job.address))).where(Job.address.isnot(None))).scalar() or 0
        )
        total_jobs = session.execute(select(func.count(Job.id))).scalar() or 0
        completed_jobs = (
            session.execute(select(func.count(Job.id)).where(Job.status == JobStatus.completed)).scalar() or 0
        )
        failed_jobs = session.execute(select(func.count(Job.id)).where(Job.status == JobStatus.failed)).scalar() or 0
        return {
            "unique_addresses": unique_addresses,
            "total_jobs": total_jobs,
            "completed_jobs": completed_jobs,
            "failed_jobs": failed_jobs,
        }


@app.get("/api/jobs")
def list_jobs() -> list[dict[str, Any]]:
    with SessionLocal() as session:
        stmt = select(Job).order_by(Job.created_at.desc())
        jobs = session.execute(stmt).scalars().all()
        return [job.to_dict() for job in jobs]


@app.post("/api/analyze", dependencies=[Depends(require_admin_key)])
def analyze_address(request: AnalyzeRequest) -> dict[str, Any]:
    if request.address and not request.address.startswith("0x"):
        raise HTTPException(status_code=400, detail="Address must start with 0x")
    with SessionLocal() as session:
        req_dict = request.model_dump()
        if request.dapp_urls:
            job = create_job(session, req_dict, initial_stage=JobStage.dapp_crawl)
        elif request.defillama_protocol:
            job = create_job(session, req_dict, initial_stage=JobStage.defillama_scan)
        else:
            job = create_job(session, req_dict)
        return job.to_dict()


@app.post("/api/company/{company_name}/analyze-remaining", dependencies=[Depends(require_admin_key)])
def analyze_remaining(company_name: str) -> dict[str, Any]:
    """Queue analysis jobs for all discovered-but-not-analyzed contracts in a company."""
    with SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
            raise HTTPException(status_code=404, detail="Company not found")

        # Exclude backfilled historical impls — these rows exist only to
        # anchor audit-coverage matching, not to be re-analyzed. Analyzing
        # them would waste pipeline cycles on bytecode nobody's using.
        # ``discovery_sources.contains(['upgrade_history'])`` emits
        # Postgres ``@>``; NULL guard covers pre-array legacy rows.
        from sqlalchemy import not_ as sa_not_

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
            existing = find_existing_job_for_address(session, contract.address, chain=contract.chain)
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
            job = create_job(session, req_dict)
            contract.job_id = job.id
            session.commit()
            queued.append({"job_id": str(job.id), "address": contract.address})

        return {"queued": len(queued), "jobs": queued}


@app.delete(
    "/api/company/{company_name}/queued-jobs",
    dependencies=[Depends(require_admin_key)],
)
def cancel_queued_company_jobs(company_name: str) -> dict[str, Any]:
    """Cancel queued jobs for a company; leaves processing/completed/failed untouched."""
    with SessionLocal() as session:
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


@app.delete(
    "/api/company/{company_name}/addresses/{address}",
    dependencies=[Depends(require_admin_key)],
)
def delete_company_address(company_name: str, address: str) -> dict[str, Any]:
    """Remove a Contract row from a protocol.

    Scoped to the protocol so unrelated contracts sharing an address (very
    rare — addresses are chain-global but we key by address only) aren't
    affected. FK cascades on ``contracts.id`` clean up the audit coverage
    rows and any upgrade-event attribution.
    """
    if not _ADDRESS_RE.match(address):
        raise HTTPException(status_code=400, detail="Invalid address")
    with SessionLocal() as session:
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


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    with SessionLocal() as session:
        job = session.get(Job, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job.to_dict()


@app.get("/api/jobs/{job_id}/stage_timings", dependencies=[Depends(require_admin_key)])
def get_job_stage_timings(job_id: str) -> dict[str, Any]:
    """Return all per-stage timing artifacts the worker fleet wrote for
    this job, keyed by stage name. Schema-v2 layout (one
    ``stage_timing_<stage>`` artifact per stage). Used by the bench
    harness to populate ``worker_elapsed_seconds`` reliably without
    scraping Fly logs.

    Admin-protected because per-job timings expose internal worker_id /
    runtime metadata.
    """
    with SessionLocal() as session:
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
        client = get_storage_client()
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
                value = deserialize_artifact(body, content_type)
                if isinstance(value, dict):
                    timings[stage] = value

    return {"job_id": resolved_job_id, "stage_timings": timings}


@app.get("/api/analyses")
def analyses(response: Response) -> list[dict]:
    """List completed analyses with their available artifacts."""
    # Read-mostly listing — let the browser reuse it across navigations.
    # Short max-age + SWR keeps freshness while letting back/forward and
    # rapid re-renders avoid a network round-trip for the multi-MB payload.
    response.headers["Cache-Control"] = "private, max-age=15, stale-while-revalidate=60"
    with SessionLocal() as session:
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
        client = get_storage_client()
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
                resolved[cache_key] = deserialize_artifact(body, content_type)

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


@app.get("/api/analyses/{run_name:path}/artifact/{artifact_name:path}")
def analysis_artifact(run_name: str, artifact_name: str):
    """Get a specific artifact for an analysis.

    Storage-backed artifacts are fetched from object storage transparently;
    inline (legacy) artifacts are served from Postgres. Either way, the body
    is returned directly to the client.
    """
    from fastapi.responses import JSONResponse

    with SessionLocal() as session:
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

        # Try both with and without extension
        artifact = get_artifact(session, job.id, lookup_name)
        if artifact is None:
            artifact = get_artifact(session, job.id, artifact_name)
        if artifact is None:
            raise HTTPException(status_code=404, detail="Artifact not found")

        if isinstance(artifact, (dict, list)):
            return JSONResponse(content=artifact)
        return PlainTextResponse(str(artifact))


@app.get("/api/analyses/{run_name:path}")
def analysis_detail(run_name: str) -> dict:
    """Get analysis detail by job name (run_name) or job_id."""
    with SessionLocal() as session:
        # Try by name first, then by id, then by address
        stmt = select(Job).where(Job.name == run_name).order_by(Job.updated_at.desc()).limit(1)
        job = session.execute(stmt).scalar_one_or_none()
        if job is None:
            try:
                job = session.get(Job, run_name)
            except Exception:
                session.rollback()
        if job is None:
            # Try by address
            job = session.execute(
                select(Job)
                .where(Job.address == run_name, Job.status == JobStatus.completed)
                .order_by(Job.updated_at.desc())
                .limit(1)
            ).scalar_one_or_none()
        if job is None:
            raise HTTPException(status_code=404, detail="Analysis not found")

        # Load artifacts (for those still stored as artifacts)
        all_artifacts = get_all_artifacts(session, job.id)

        # Fall back to address lookup when copy_static_cache has reassigned
        # the Contract row to a newer job. Chain-scoped so we don't pick up
        # the same address on a different chain.
        contract_row = session.execute(select(Contract).where(Contract.job_id == job.id).limit(1)).scalar_one_or_none()
        if contract_row is None and job.address:
            fallback_stmt = select(Contract).where(Contract.address == job.address.lower())
            job_chain = job.request.get("chain") if isinstance(job.request, dict) else None
            if job_chain:
                fallback_stmt = fallback_stmt.where(Contract.chain == job_chain)
            contract_row = session.execute(fallback_stmt.limit(1)).scalar_one_or_none()

        # Walk the job tree — mirrors analyses().
        def _company_for(j: Job) -> str | None:
            seen: set[str] = set()
            current: Job | None = j
            while current is not None:
                if current.company:
                    return current.company
                request = current.request if isinstance(current.request, dict) else {}
                parent_id = request.get("parent_job_id")
                if not isinstance(parent_id, str) or parent_id in seen:
                    return None
                seen.add(parent_id)
                current = session.get(Job, parent_id)
            return None

        payload: dict[str, Any] = {
            "run_name": job.name or str(job.id),
            "job_id": str(job.id),
            "address": job.address,
            "contract_id": contract_row.id if contract_row else None,
            "company": _company_for(job),
            "deployer": contract_row.deployer if contract_row else None,
            "available_artifacts": sorted(all_artifacts.keys()),
        }

        # Inline artifacts that are still stored as artifacts
        for artifact_name in (
            "contract_analysis",
            "control_snapshot",
            "dependencies",
            "resolved_control_graph",
            "dependency_graph_viz",
            "upgrade_history",
            # Schema-v2: raw predicate trees per externally-callable
            # function. Existing consumers ignore the new key; v2
            # consumers read it directly OR fetch the resolved
            # ``v2_capabilities`` below.
            "predicate_trees",
        ):
            if artifact_name in all_artifacts and isinstance(all_artifacts[artifact_name], dict):
                payload[artifact_name] = all_artifacts[artifact_name]

        # Schema-v2 resolved capabilities. Computed lazily — the
        # raw predicate_trees lives on the artifact; resolving it to
        # the typed CapabilityExpr requires the AdapterRegistry +
        # repos. Defensive: a v2-resolution failure MUST NOT fail
        # the whole analysis_detail response (the v1 fields above
        # stay authoritative through the cutover).
        if "predicate_trees" in all_artifacts and job.address:
            try:
                from services.resolution.capability_resolver import (
                    resolve_contract_capabilities,
                )

                v2_caps = resolve_contract_capabilities(
                    session, address=job.address.lower()
                )
                if v2_caps is not None:
                    payload["v2_capabilities"] = v2_caps
            except Exception:
                logger.exception(
                    "v2 capability resolution failed for job %s; "
                    "v1 fields remain authoritative",
                    job.id,
                )

        if contract_row:
            ef_rows = list(
                session.execute(
                    select(EffectiveFunction)
                    .where(EffectiveFunction.contract_id == contract_row.id)
                    .options(selectinload(EffectiveFunction.principals))
                ).scalars()
            )

            ef_list = []
            for ef in ef_rows:
                direct_owner = None
                controller_principals = []
                for fp in ef.principals or []:
                    principal_dict = {
                        "address": fp.address,
                        "resolved_type": fp.resolved_type,
                        "source_controller_id": fp.origin,
                        "details": fp.details or {},
                    }
                    if fp.principal_type == "direct_owner" and direct_owner is None:
                        direct_owner = principal_dict
                    else:
                        controller_principals.append(principal_dict)
                ef_list.append(
                    {
                        "function": ef.abi_signature or ef.function_name,
                        "selector": ef.selector,
                        "effect_labels": list(ef.effect_labels or []),
                        "effect_targets": list(ef.effect_targets or []),
                        "action_summary": ef.action_summary,
                        "authority_public": ef.authority_public,
                        "controllers": [{"principals": controller_principals}] if controller_principals else [],
                        "authority_roles": ef.authority_roles or [],
                        "direct_owner": direct_owner,
                    }
                )
            if ef_list:
                payload["effective_permissions"] = {
                    "functions": ef_list,
                    "contract_name": contract_row.contract_name,
                    "contract_address": contract_row.address,
                }
                if "effective_permissions" not in payload.get("available_artifacts", []):
                    payload["available_artifacts"] = sorted(
                        set(payload.get("available_artifacts", [])) | {"effective_permissions"}
                    )

            # Build principal_labels from table
            from db.models import PrincipalLabel as PL

            pl_rows = session.execute(select(PL).where(PL.contract_id == contract_row.id)).scalars().all()
            if pl_rows:
                payload["principal_labels"] = {
                    "principals": [
                        {
                            "address": p.address,
                            "display_name": p.display_name,
                            "label": p.label,
                            "resolved_type": p.resolved_type,
                            "labels": list(p.labels or []),
                            "confidence": p.confidence,
                            "details": p.details or {},
                            "graph_context": list(p.graph_context or []),
                        }
                        for p in pl_rows
                    ],
                    "contract_name": contract_row.contract_name,
                    "contract_address": contract_row.address,
                }

            # Build control_snapshot from table if not in artifacts
            if "control_snapshot" not in payload:
                from db.models import ControllerValue as CV

                cv_rows = session.execute(select(CV).where(CV.contract_id == contract_row.id)).scalars().all()
                if cv_rows:
                    payload["control_snapshot"] = {
                        "contract_name": contract_row.contract_name,
                        "contract_address": contract_row.address,
                        "controller_values": {
                            cv.controller_id: {
                                "value": cv.value,
                                "resolved_type": cv.resolved_type,
                                "source": cv.source,
                                "block_number": cv.block_number,
                                "observed_via": cv.observed_via,
                                "details": cv.details or {},
                            }
                            for cv in cv_rows
                        },
                    }

            # Build resolved_control_graph from table if not in artifacts
            if "resolved_control_graph" not in payload:
                from db.models import ControlGraphEdge as CGE
                from db.models import ControlGraphNode as CGN

                cgn_rows = session.execute(select(CGN).where(CGN.contract_id == contract_row.id)).scalars().all()
                cge_rows = session.execute(select(CGE).where(CGE.contract_id == contract_row.id)).scalars().all()
                if cgn_rows:
                    payload["resolved_control_graph"] = {
                        "root_contract_address": contract_row.address,
                        "nodes": [
                            {
                                "id": f"address:{n.address}",
                                "address": n.address,
                                "node_type": n.node_type,
                                "resolved_type": n.resolved_type,
                                "label": n.label,
                                "contract_name": n.contract_name,
                                "depth": n.depth,
                                "analyzed": n.analyzed,
                                "details": n.details or {},
                            }
                            for n in cgn_rows
                        ],
                        "edges": [
                            {
                                "from_id": e.from_node_id,
                                "to_id": e.to_node_id,
                                "relation": e.relation,
                                "label": e.label,
                                "source_controller_id": e.source_controller_id,
                                "notes": list(e.notes or []),
                            }
                            for e in cge_rows
                        ],
                    }

        # For impl jobs, inherit proxy-specific artifacts from the proxy job
        request = job.request if isinstance(job.request, dict) else {}
        proxy_address = request.get("proxy_address")
        if proxy_address:
            proxy_stmt = select(Job).where(Job.address == proxy_address).order_by(Job.updated_at.desc()).limit(1)
            proxy_job = session.execute(proxy_stmt).scalar_one_or_none()
            if proxy_job:
                proxy_artifacts = get_all_artifacts(session, proxy_job.id)
                for fallback_name in ("upgrade_history", "dependency_graph_viz", "dependencies"):
                    if fallback_name in payload:
                        continue
                    fallback = proxy_artifacts.get(fallback_name)
                    if isinstance(fallback, dict):
                        payload[fallback_name] = fallback
        payload["proxy_address"] = proxy_address

        # For proxy jobs, inherit analysis from the impl child job
        is_proxy = contract_row.is_proxy if contract_row else False
        impl_addr = contract_row.implementation if contract_row else None
        if is_proxy and impl_addr:
            impl_stmt = select(Job).where(Job.address == impl_addr).order_by(Job.updated_at.desc()).limit(1)
            impl_job = session.execute(impl_stmt).scalar_one_or_none()
            if impl_job:
                impl_artifacts = get_all_artifacts(session, impl_job.id)
                for fallback_name in (
                    "contract_analysis",
                    "control_snapshot",
                    "resolved_control_graph",
                    "effective_permissions",
                    "principal_labels",
                ):
                    if fallback_name not in payload:
                        val = impl_artifacts.get(fallback_name)
                        if val is not None:
                            payload[fallback_name] = val

                # Inherit from impl's relational tables
                impl_c = session.execute(
                    select(Contract).where(Contract.job_id == impl_job.id).limit(1)
                ).scalar_one_or_none()
                if impl_c:
                    if "effective_permissions" not in payload:
                        impl_efs = list(
                            session.execute(
                                select(EffectiveFunction)
                                .where(EffectiveFunction.contract_id == impl_c.id)
                                .options(selectinload(EffectiveFunction.principals))
                            ).scalars()
                        )
                        if impl_efs:
                            ef_list = []
                            for ef in impl_efs:
                                direct_owner = None
                                controller_principals = []
                                for fp in ef.principals or []:
                                    principal_dict = {
                                        "address": fp.address,
                                        "resolved_type": fp.resolved_type,
                                        "source_controller_id": fp.origin,
                                        "details": fp.details or {},
                                    }
                                    if fp.principal_type == "direct_owner" and direct_owner is None:
                                        direct_owner = principal_dict
                                    else:
                                        controller_principals.append(principal_dict)
                                ef_list.append(
                                    {
                                        "function": ef.abi_signature or ef.function_name,
                                        "selector": ef.selector,
                                        "effect_labels": list(ef.effect_labels or []),
                                        "effect_targets": list(ef.effect_targets or []),
                                        "action_summary": ef.action_summary,
                                        "authority_public": ef.authority_public,
                                        "controllers": (
                                            [{"principals": controller_principals}] if controller_principals else []
                                        ),
                                        "authority_roles": ef.authority_roles or [],
                                        "direct_owner": direct_owner,
                                    }
                                )
                            payload["effective_permissions"] = {
                                "functions": ef_list,
                                "contract_name": impl_c.contract_name,
                                "contract_address": impl_c.address,
                            }

                    # control_snapshot from impl
                    if "control_snapshot" not in payload:
                        from db.models import ControllerValue as CV

                        impl_cvs = session.execute(select(CV).where(CV.contract_id == impl_c.id)).scalars().all()
                        if impl_cvs:
                            payload["control_snapshot"] = {
                                "contract_name": impl_c.contract_name,
                                "contract_address": impl_c.address,
                                "controller_values": {
                                    cv.controller_id: {
                                        "value": cv.value,
                                        "resolved_type": cv.resolved_type,
                                        "source": cv.source,
                                        "block_number": cv.block_number,
                                        "observed_via": cv.observed_via,
                                        "details": cv.details or {},
                                    }
                                    for cv in impl_cvs
                                },
                            }

                    # resolved_control_graph from impl
                    if "resolved_control_graph" not in payload:
                        from db.models import ControlGraphEdge as CGE
                        from db.models import ControlGraphNode as CGN

                        impl_cgn = session.execute(select(CGN).where(CGN.contract_id == impl_c.id)).scalars().all()
                        impl_cge = session.execute(select(CGE).where(CGE.contract_id == impl_c.id)).scalars().all()
                        if impl_cgn:
                            payload["resolved_control_graph"] = {
                                "root_contract_address": impl_c.address,
                                "nodes": [
                                    {
                                        "id": f"address:{n.address}",
                                        "address": n.address,
                                        "node_type": n.node_type,
                                        "resolved_type": n.resolved_type,
                                        "label": n.label,
                                        "contract_name": n.contract_name,
                                        "depth": n.depth,
                                        "analyzed": n.analyzed,
                                        "details": n.details or {},
                                    }
                                    for n in impl_cgn
                                ],
                                "edges": [
                                    {
                                        "from_id": e.from_node_id,
                                        "to_id": e.to_node_id,
                                        "relation": e.relation,
                                        "label": e.label,
                                        "source_controller_id": e.source_controller_id,
                                        "notes": list(e.notes or []),
                                    }
                                    for e in impl_cge
                                ],
                            }

                    # principal_labels from impl
                    if "principal_labels" not in payload:
                        from db.models import PrincipalLabel as PL

                        impl_pls = session.execute(select(PL).where(PL.contract_id == impl_c.id)).scalars().all()
                        if impl_pls:
                            payload["principal_labels"] = {
                                "principals": [
                                    {"address": p.address, "label": p.label, "resolved_type": p.resolved_type}
                                    for p in impl_pls
                                ],
                            }

                    # contract_name from impl
                    if "contract_name" not in payload and impl_c.contract_name:
                        payload["contract_name"] = impl_c.contract_name
                    if "summary" not in payload and impl_c.summary:
                        payload["summary"] = {
                            "control_model": impl_c.summary.control_model,
                            "is_upgradeable": impl_c.summary.is_upgradeable,
                            "is_pausable": impl_c.summary.is_pausable,
                            "has_timelock": impl_c.summary.has_timelock,
                            "static_risk_level": impl_c.summary.risk_level,
                            "standards": list(impl_c.summary.standards or []),
                        }

                payload["proxy_address"] = payload.get("proxy_address") or job.address
                payload["implementation_address"] = impl_addr

        # Add subject info from contract_analysis if available
        if isinstance(all_artifacts.get("contract_analysis"), dict):
            subject = all_artifacts["contract_analysis"].get("subject", {})
            payload["contract_name"] = subject.get("name", payload["run_name"])
            payload["summary"] = all_artifacts["contract_analysis"].get("summary")

        return payload


@app.get("/api/company/{company_name}")
def company_overview(company_name: str, response: Response) -> dict:
    """Aggregated governance overview for all contracts in a company."""
    # Largest payload on the site (1-3 MB). Letting the browser hold it for
    # 15s + serve-stale-while-revalidate makes back/forward navigation and
    # tab switches inside the company page instant — both CompanyOverview
    # and ProtocolSurface read this URL on mount.
    response.headers["Cache-Control"] = "private, max-age=15, stale-while-revalidate=60"
    with SessionLocal() as session:
        # Look up protocol — try by protocol table first, fall back to job.company
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()

        if protocol_row:
            # Get all jobs that belong to this protocol
            company_jobs = (
                session.execute(
                    select(Job).where(
                        Job.protocol_id == protocol_row.id,
                        Job.status == JobStatus.completed,
                        Job.address.isnot(None),
                    )
                )
                .scalars()
                .all()
            )
        else:
            # Fallback: find by company string (legacy data)
            company_job = session.execute(
                select(Job).where(Job.company == company_name).order_by(Job.updated_at.desc()).limit(1)
            ).scalar_one_or_none()
            if company_job is None:
                raise HTTPException(status_code=404, detail="Company not found")

            company_job_id = str(company_job.id)
            all_completed = session.execute(select(Job).where(Job.status == JobStatus.completed)).scalars().all()

            def belongs_to_company(job: Job) -> bool:
                seen: set[str] = set()
                current: Job | None = job
                jobs_by_id = {str(j.id): j for j in all_completed}
                jobs_by_id[company_job_id] = company_job
                while current is not None:
                    if current.company == company_name:
                        return True
                    request = current.request if isinstance(current.request, dict) else {}
                    parent_id = request.get("parent_job_id")
                    if not isinstance(parent_id, str) or parent_id in seen:
                        return False
                    seen.add(parent_id)
                    current = jobs_by_id.get(parent_id)
                return False

            company_jobs = [j for j in all_completed if j.address and belongs_to_company(j)]

        # Batch-prefetch every per-contract child table here so the loop
        # below is pure Python dict lookups instead of one SELECT per row
        # per table.
        company_job_ids = [j.id for j in company_jobs]
        contracts_by_job_id: dict[Any, Contract] = {}
        if company_job_ids:
            for c in session.execute(
                select(Contract).where(Contract.job_id.in_(company_job_ids)).options(selectinload(Contract.summary))
            ).scalars():
                contracts_by_job_id[c.job_id] = c

        # Address-keyed fallback for jobs whose Contract row was reassigned
        # by copy_static_cache.
        unresolved_addrs_by_chain: dict[str | None, set[str]] = {}
        for j in company_jobs:
            if contracts_by_job_id.get(j.id) is not None or not j.address:
                continue
            req = j.request if isinstance(j.request, dict) else {}
            unresolved_addrs_by_chain.setdefault(req.get("chain"), set()).add(j.address.lower())
        contracts_by_addr_chain: dict[tuple[str, str | None], Contract] = {}
        all_unresolved_addrs = {a for addrs in unresolved_addrs_by_chain.values() for a in addrs}
        if all_unresolved_addrs:
            for c in session.execute(
                select(Contract)
                .where(Contract.address.in_(list(all_unresolved_addrs)))
                .options(selectinload(Contract.summary))
            ).scalars():
                addr_lc = (c.address or "").lower()
                for chain_key, addrs in unresolved_addrs_by_chain.items():
                    if addr_lc in addrs and (chain_key is None or c.chain == chain_key):
                        contracts_by_addr_chain[(addr_lc, chain_key)] = c

        def _resolve_contract(j: Job) -> Contract | None:
            cr = contracts_by_job_id.get(j.id)
            if cr is not None or not j.address:
                return cr
            req = j.request if isinstance(j.request, dict) else {}
            return contracts_by_addr_chain.get((j.address.lower(), req.get("chain")))

        impl_addrs_needed: set[str] = set()
        for j in company_jobs:
            cr = _resolve_contract(j)
            if cr and cr.is_proxy and cr.implementation:
                impl_addrs_needed.add(cr.implementation.lower())
        impl_job_by_addr: dict[str, Job] = {}
        if impl_addrs_needed:
            impl_jobs_q = session.execute(
                select(Job).where(
                    Job.address.in_(list(impl_addrs_needed)),
                    Job.status == JobStatus.completed,
                )
            ).scalars()
            for ij in impl_jobs_q:
                key = (ij.address or "").lower()
                if key and key not in impl_job_by_addr:
                    impl_job_by_addr[key] = ij

        impl_job_ids_needed = [ij.id for ij in impl_job_by_addr.values()]
        if impl_job_ids_needed:
            for c in session.execute(
                select(Contract).where(Contract.job_id.in_(impl_job_ids_needed)).options(selectinload(Contract.summary))
            ).scalars():
                contracts_by_job_id[c.job_id] = c

        relevant_contract_ids: set[int] = {c.id for c in contracts_by_job_id.values() if c is not None} | {
            c.id for c in contracts_by_addr_chain.values() if c is not None
        }

        controller_values_by_cid: dict[int, list[ControllerValue]] = {}
        ef_rows_by_cid: dict[int, list[EffectiveFunction]] = {}
        upgrade_events_count_by_cid: dict[int, int] = {}
        balances_by_cid: dict[int, list[Any]] = {}
        cgn_by_cid: dict[int, list[ControlGraphNode]] = {}
        cge_by_cid: dict[int, list[ControlGraphEdge]] = {}
        from db.models import ContractBalance as _CB

        if relevant_contract_ids:
            id_list = list(relevant_contract_ids)
            for cv in session.execute(
                select(ControllerValue).where(ControllerValue.contract_id.in_(id_list))
            ).scalars():
                controller_values_by_cid.setdefault(cv.contract_id, []).append(cv)
            for ef in session.execute(
                select(EffectiveFunction)
                .where(EffectiveFunction.contract_id.in_(id_list))
                .options(selectinload(EffectiveFunction.principals))
            ).scalars():
                ef_rows_by_cid.setdefault(ef.contract_id, []).append(ef)
            for cid, count in session.execute(
                select(UpgradeEvent.contract_id, func.count(UpgradeEvent.id))
                .where(UpgradeEvent.contract_id.in_(id_list))
                .group_by(UpgradeEvent.contract_id)
            ).all():
                upgrade_events_count_by_cid[cid] = count
            for b in session.execute(select(_CB).where(_CB.contract_id.in_(id_list))).scalars():
                balances_by_cid.setdefault(b.contract_id, []).append(b)
            for n in session.execute(
                select(ControlGraphNode).where(ControlGraphNode.contract_id.in_(id_list))
            ).scalars():
                cgn_by_cid.setdefault(n.contract_id, []).append(n)
            for e in session.execute(
                select(ControlGraphEdge).where(ControlGraphEdge.contract_id.in_(id_list))
            ).scalars():
                cge_by_cid.setdefault(e.contract_id, []).append(e)

        # Build contract entries from relational tables
        contracts = []
        owner_groups: dict[str, list[dict]] = {}

        for job in company_jobs:
            request = job.request if isinstance(job.request, dict) else {}
            is_impl = bool(request.get("proxy_address"))
            if is_impl:
                continue

            # Read from contracts table — fall back to address lookup when
            # copy_static_cache has reassigned the Contract row to a newer job.
            contract_row = _resolve_contract(job)

            is_proxy = contract_row.is_proxy if contract_row else False
            proxy_type = contract_row.proxy_type if contract_row else None
            impl_addr = contract_row.implementation if contract_row else None

            impl_job = impl_job_by_addr.get(impl_addr.lower()) if impl_addr else None
            impl_job_id = str(impl_job.id) if impl_job else None
            impl_contract = contracts_by_job_id.get(impl_job.id) if impl_job else None

            # Read summary from contract_summaries table (prefer impl's summary for proxies)
            summary_row = impl_contract.summary if impl_contract else None
            if not summary_row and contract_row:
                summary_row = contract_row.summary

            # Prefer the impl's controller snapshot for proxies if it has any.
            lookup_contract = contract_row
            if is_proxy and impl_contract and controller_values_by_cid.get(impl_contract.id):
                lookup_contract = impl_contract

            owner = None
            controllers = {}
            if lookup_contract:
                for cv in controller_values_by_cid.get(lookup_contract.id, []):
                    controllers[cv.controller_id] = cv.value
                    if "owner" in cv.controller_id.lower() and cv.value and cv.value.startswith("0x"):
                        owner = cv.value.lower()

            upgrade_count = (upgrade_events_count_by_cid.get(contract_row.id) if contract_row else None) or None

            # Prefer impl for effect labels when available.
            ef_contract_id = (impl_contract.id if impl_contract else None) or (
                contract_row.id if contract_row else None
            )

            value_effects: list[str] = []
            all_effects: set[str] = set()
            ef_rows_for_contract = ef_rows_by_cid.get(ef_contract_id, []) if ef_contract_id else []
            for ef in ef_rows_for_contract:
                for label in ef.effect_labels or []:
                    all_effects.add(label)
                    if label in ("asset_pull", "asset_send", "mint", "burn"):
                        if label not in value_effects:
                            value_effects.append(label)

            # Build capabilities
            capabilities: list[str] = []
            if is_proxy:
                capabilities.append("upgradeable")
            if "implementation_update" in all_effects:
                capabilities.append("upgrade")
            if "pause_toggle" in all_effects or (summary_row and summary_row.is_pausable):
                capabilities.append("pause")
            if "ownership_transfer" in all_effects:
                capabilities.append("ownership")
            if "role_management" in all_effects:
                capabilities.append("roles")
            if "asset_pull" in all_effects or "mint" in all_effects:
                capabilities.append("value-in")
            if "asset_send" in all_effects or "burn" in all_effects:
                capabilities.append("value-out")
            if "delegatecall_execution" in all_effects:
                capabilities.append("delegatecall")
            if "arbitrary_external_call" in all_effects:
                capabilities.append("arbitrary-call")

            # For proxies, use the impl contract's name instead of the proxy name
            contract_name = None
            if is_proxy and impl_job:
                if impl_contract and impl_contract.contract_name:
                    contract_name = impl_contract.contract_name
                elif impl_job.name:
                    contract_name = impl_job.name
            if not contract_name:
                contract_name = (contract_row.contract_name if contract_row else None) or job.name or ""
            standards = list(summary_row.standards or []) if summary_row else []
            is_factory = summary_row.is_factory if summary_row else False
            has_timelock = summary_row.has_timelock if summary_row else False
            is_pausable = summary_row.is_pausable if summary_row else False
            control_model = summary_row.control_model if summary_row else None

            # Derive role
            name_lower = contract_name.lower()
            if "bridge" in name_lower or "gateway" in name_lower:
                role = "bridge"
            elif any(e in value_effects for e in ("asset_pull", "asset_send")):
                role = "value_handler"
            elif any(s in standards for s in ("ERC20", "ERC721", "ERC1155")):
                role = "token"
            elif has_timelock or control_model == "governance":
                role = "governance"
            elif is_factory:
                role = "factory"
            else:
                role = "utility"

            functions_list = []
            for ef in ef_rows_for_contract:
                functions_list.append(_build_company_function_entry(ef, ef.principals or []))

            balance_contract = lookup_contract or contract_row
            balances_list = []
            total_usd = 0.0
            if balance_contract:
                for b in balances_by_cid.get(balance_contract.id, []):
                    usd = float(b.usd_value) if b.usd_value is not None else None
                    balances_list.append(
                        {
                            "token_symbol": b.token_symbol,
                            "token_name": b.token_name,
                            "token_address": b.token_address,
                            "raw_balance": b.raw_balance,
                            "decimals": b.decimals,
                            "usd_value": usd,
                            "price_usd": float(b.price_usd) if b.price_usd is not None else None,
                        }
                    )
                    if usd:
                        total_usd += usd

            entry = {
                "address": job.address,
                "name": contract_name,
                "contract_id": contract_row.id if contract_row else None,
                "job_id": str(job.id),
                "impl_job_id": impl_job_id,
                "is_proxy": is_proxy,
                "proxy_type": proxy_type,
                "implementation": impl_addr,
                "deployer": contract_row.deployer if contract_row else None,
                "owner": owner,
                "controllers": controllers,
                "control_model": control_model,
                "risk_level": summary_row.risk_level if summary_row else None,
                "source_verified": summary_row.source_verified if summary_row else None,
                "upgrade_count": upgrade_count,
                "role": role,
                "standards": standards,
                "value_effects": value_effects,
                "is_pausable": is_pausable,
                "has_timelock": has_timelock,
                "capabilities": capabilities,
                "functions": functions_list,
                "balances": balances_list,
                "total_usd": round(total_usd, 2) if total_usd > 0 else None,
            }

            graph_contract = lookup_contract or contract_row
            if graph_contract:
                cg_nodes = cgn_by_cid.get(graph_contract.id, [])
                cg_edges = cge_by_cid.get(graph_contract.id, [])
                entry["control_graph"] = {
                    "nodes": [
                        {
                            "address": n.address,
                            "type": n.resolved_type,
                            "label": n.contract_name or n.label,
                            "details": n.details or {},
                        }
                        for n in cg_nodes
                    ],
                    "edges": [
                        {
                            "from": e.from_node_id.replace("address:", ""),
                            "to": e.to_node_id.replace("address:", ""),
                            "relation": e.relation,
                        }
                        for e in cg_edges
                    ],
                }
            contracts.append(entry)

            if owner:
                owner_groups.setdefault(owner, []).append(entry)

        # Deduplicate: remove standalone impl contracts already represented via a proxy
        impl_addresses = {c["implementation"].lower() for c in contracts if c.get("implementation")}
        contracts = [
            c for c in contracts if not c["address"] or c["address"].lower() not in impl_addresses or c["is_proxy"]
        ]

        # Rebuild owner_groups after dedup
        remaining_addrs = {c["address"] for c in contracts if c["address"]}
        for owner_addr in list(owner_groups):
            owner_groups[owner_addr] = [e for e in owner_groups[owner_addr] if e["address"] in remaining_addrs]
            if not owner_groups[owner_addr]:
                del owner_groups[owner_addr]

        # Build the ownership hierarchy
        hierarchy = []
        assigned = set()
        for owner_addr, owned in sorted(owner_groups.items(), key=lambda x: -len(x[1])):
            # Check if this owner is itself one of our contracts
            owner_contract = next((c for c in contracts if c["address"] and c["address"].lower() == owner_addr), None)
            hierarchy.append(
                {
                    "owner": owner_addr,
                    "owner_name": owner_contract["name"] if owner_contract else None,
                    "owner_is_contract": owner_contract is not None,
                    "contracts": [{"address": c["address"], "name": c["name"]} for c in owned],
                }
            )
            assigned.update(c["address"] for c in owned)

        # Add unowned contracts
        unowned = [c for c in contracts if c["address"] not in assigned]
        if unowned:
            hierarchy.append(
                {
                    "owner": None,
                    "owner_name": "No owner detected",
                    "owner_is_contract": False,
                    "contracts": [{"address": c["address"], "name": c["name"]} for c in unowned],
                }
            )

        # Build fund flow edges: connect contracts via resolved principal relationships
        contract_addrs = {c["address"].lower() for c in contracts if c["address"]}
        contract_by_addr = {c["address"].lower(): c for c in contracts if c["address"]}
        flow_seen: set[tuple[str, str]] = set()
        fund_flows: list[dict[str, Any]] = []

        def add_flow(from_addr: str, to_addr: str, flow_type: str, lane: str = "control") -> None:
            key = (from_addr, to_addr)
            if key in flow_seen:
                return
            flow_seen.add(key)
            target = contract_by_addr.get(to_addr, {})
            fund_flows.append(
                {
                    "from": from_addr,
                    "to": to_addr,
                    "type": flow_type,
                    "lane": lane,
                    "capabilities": target.get("capabilities", []),
                }
            )

        def _lookup_contract_for(entry: dict[str, Any]) -> Contract | None:
            lookup_job_id = entry.get("impl_job_id") or entry["job_id"]
            try:
                key_id = uuid.UUID(lookup_job_id) if isinstance(lookup_job_id, str) else lookup_job_id
            except (TypeError, ValueError):
                key_id = lookup_job_id
            return contracts_by_job_id.get(key_id)

        lookup_contract_by_entry: dict[str, Contract | None] = {}
        for entry in contracts:
            if entry.get("address"):
                lookup_contract_by_entry[entry["address"].lower()] = _lookup_contract_for(entry)

        for c in contracts:
            if not c["address"]:
                continue
            target = c["address"].lower()

            # Owner of this contract is another company contract
            if c.get("owner") and c["owner"] in contract_addrs:
                flow_type = (
                    "controls_value"
                    if any(e in c.get("value_effects", []) for e in ("asset_pull", "asset_send"))
                    else "controls"
                )
                add_flow(c["owner"], target, flow_type)

            # Controller storage values pointing to other company contracts
            for cid, val in c.get("controllers", {}).items():
                if isinstance(val, str) and val.startswith("0x"):
                    val_lower = val.lower()
                    if val_lower in contract_addrs and val_lower != (c.get("owner") or ""):
                        add_flow(val_lower, target, "controller")

            # Find company contracts that are principals of this one.
            lookup_c = lookup_contract_by_entry.get(target)
            if lookup_c:
                for cgn in cgn_by_cid.get(lookup_c.id, []):
                    node_addr = (cgn.address or "").lower()
                    if node_addr and node_addr in contract_addrs and node_addr != target:
                        add_flow(node_addr, target, "principal")

        # No transitive dedup — keep all direct principal edges as-is.

        # Collect non-contract principals from control graph.
        # Only show direct controllers (Safes, timelocks, EOAs that control contracts).
        # Safe owners are nested inside their Safe, not shown as standalone nodes.

        principal_map: dict[str, dict[str, Any]] = {}
        # First pass: find all safe_owner edges to build Safe→owners mapping
        safe_owners_map: dict[str, list[str]] = {}  # safe_addr → [owner_addrs]
        owner_of_safe: set[str] = set()  # addresses that are Safe owners

        for c in contracts:
            if not c["address"]:
                continue
            lookup_c = lookup_contract_by_entry.get(c["address"].lower())
            if not lookup_c:
                continue
            for edge in cge_by_cid.get(lookup_c.id, []):
                if edge.relation != "safe_owner":
                    continue
                safe_addr = edge.from_node_id.replace("address:", "").lower()
                owner_addr = edge.to_node_id.replace("address:", "").lower()
                safe_owners_map.setdefault(safe_addr, [])
                if owner_addr not in safe_owners_map[safe_addr]:
                    safe_owners_map[safe_addr].append(owner_addr)
                owner_of_safe.add(owner_addr)

        # Second pass: collect direct controllers (skip Safe owners — they're nested)
        for c in contracts:
            if not c["address"]:
                continue
            target = c["address"].lower()
            lookup_c = lookup_contract_by_entry.get(target)
            if not lookup_c:
                continue

            for cgn in cgn_by_cid.get(lookup_c.id, []):
                node_addr = (cgn.address or "").lower()
                if not node_addr or node_addr in contract_addrs:
                    continue
                if node_addr in owner_of_safe:
                    continue  # Skip — will be nested inside their Safe
                if cgn.resolved_type not in ("safe", "timelock", "proxy_admin", "eoa"):
                    continue
                # Skip zero addresses
                if node_addr == "0x0000000000000000000000000000000000000000":
                    continue

                if node_addr not in principal_map:
                    # Seed details with the CGN's own introspection result
                    # (getOwners/getThreshold for safes, getMinDelay for
                    # timelocks). This is the authoritative source for the
                    # principal's intrinsic config — ControllerValue rows
                    # describe the relationship FROM a consumer, not the
                    # Safe's own threshold, so prior code that only merged
                    # CV details missed the threshold and fell back to
                    # len(owners).
                    details: dict[str, Any] = {}
                    if isinstance(cgn.details, dict):
                        details.update(cgn.details)
                    for cv in controller_values_by_cid.get(lookup_c.id, []):
                        if (cv.value or "").lower() != node_addr:
                            continue
                        if cv.details and isinstance(cv.details, dict):
                            # Don't let consumer-side details overwrite the
                            # safe's own threshold / owners — only fill in
                            # keys the CGN didn't already establish.
                            for k, v in cv.details.items():
                                details.setdefault(k, v)

                    # For Safes, attach owners and threshold
                    if cgn.resolved_type == "safe":
                        # Prefer owners already persisted on the CGN's own
                        # details (from getOwners). Fall back to the
                        # edge-derived map if the CGN didn't include them.
                        if not details.get("owners"):
                            details["owners"] = safe_owners_map.get(node_addr, [])
                        if "threshold" not in details and details.get("owners"):
                            details["threshold"] = len(details["owners"])  # fallback

                    principal_map[node_addr] = {
                        "address": node_addr,
                        "type": cgn.resolved_type,
                        "label": cgn.contract_name or cgn.label or cgn.resolved_type,
                        "details": details,
                        "controls": [],
                    }

                principal_map[node_addr]["controls"].append(target)
                add_flow(node_addr, target, "principal")

        # Third pass: pull principals out of FunctionPrincipal rows. Some
        # role-gated functions (e.g. EtherFiTimelock.cancel / .execute) have
        # their controlling Safe/EOA stored *only* on the per-function
        # principal row — the Safe never gets a top-level ControlGraphNode
        # entry for that contract, so the prior CGN-only pass misses the
        # Safe→Contract edge entirely. This pass backfills.
        # (FunctionPrincipal + EffectiveFunction are module-level imports.)

        for c in contracts:
            if not c["address"]:
                continue
            target = c["address"].lower()
            lookup_c = lookup_contract_by_entry.get(target)
            if not lookup_c:
                continue
            fp_iter = (fp for ef in ef_rows_by_cid.get(lookup_c.id, []) for fp in (ef.principals or []))
            for fp in fp_iter:
                pa = (fp.address or "").lower()
                if not pa or pa == target:
                    continue
                if pa == "0x0000000000000000000000000000000000000000":
                    continue
                if pa in owner_of_safe:
                    # Signer on a Safe — already nested under the Safe node.
                    continue
                if fp.resolved_type not in ("safe", "timelock", "eoa", "proxy_admin"):
                    continue
                # Skip contract-principals — they're covered by the
                # controller/owner/CGN passes above as contract-to-contract
                # edges, not standalone principals.
                if pa in contract_addrs:
                    continue
                if pa not in principal_map:
                    fp_details = dict(fp.details or {})
                    if fp.resolved_type == "safe":
                        if not fp_details.get("owners"):
                            fp_details["owners"] = safe_owners_map.get(pa, [])
                        if "threshold" not in fp_details and fp_details.get("owners"):
                            fp_details["threshold"] = len(fp_details["owners"])  # fallback
                    principal_map[pa] = {
                        "address": pa,
                        "type": fp.resolved_type,
                        "label": fp.resolved_type,
                        "details": fp_details,
                        "controls": [],
                    }
                if target not in principal_map[pa]["controls"]:
                    principal_map[pa]["controls"].append(target)
                add_flow(pa, target, "principal")

        principals = list(principal_map.values())

        # Build all_addresses from contracts table (includes discovered + analyzed)
        if protocol_row:
            all_contract_rows = (
                session.execute(select(Contract).where(Contract.protocol_id == protocol_row.id)).scalars().all()
            )
        else:
            fallback_job_ids = [j.id for j in company_jobs]
            if fallback_job_ids:
                all_contract_rows = list(
                    session.execute(select(Contract).where(Contract.job_id.in_(fallback_job_ids))).scalars()
                )
            else:
                all_contract_rows = []

        # Prefetch impl-name lookup so proxy rows can expose the implementation
        # contract name alongside their own generic "UUPSProxy"/"ERC1967Proxy"
        # template name. Impl rows are already present in all_contract_rows
        # (the selection worker writes a Contract row for every discovered
        # impl), so no extra query is needed.
        impl_name_by_addr = {
            (c.address or "").lower(): c.contract_name for c in all_contract_rows if c.address and c.contract_name
        }
        job_ids = {cr.job_id for cr in all_contract_rows if cr.job_id is not None}
        completed_job_ids: set = set()
        if job_ids:
            completed_job_ids = set(
                session.execute(select(Job.id).where(Job.id.in_(job_ids), Job.status == JobStatus.completed))
                .scalars()
                .all()
            )
        all_addresses = sorted(
            [
                {
                    "address": cr.address,
                    "name": cr.contract_name,
                    "source_verified": cr.source_verified,
                    "is_proxy": cr.is_proxy,
                    "analyzed": cr.job_id is not None and cr.job_id in completed_job_ids,
                    "discovery_sources": list(cr.discovery_sources or []),
                    "discovery_url": cr.discovery_url,
                    "chain": cr.chain,
                    # Selection-worker rank — lets the Addresses modal sort
                    # highest-ranked first. NULL for rows the selection worker
                    # hasn't scored yet (e.g. freshly discovered).
                    "rank_score": (float(cr.rank_score) if cr.rank_score is not None else None),
                    "implementation_address": cr.implementation if cr.is_proxy else None,
                    "implementation_name": (
                        impl_name_by_addr.get((cr.implementation or "").lower()) if cr.is_proxy else None
                    ),
                }
                for cr in all_contract_rows
            ],
            key=lambda x: (not x["analyzed"], x["name"] or "zzz"),
        )

        # Latest TVL snapshot
        tvl_data: dict[str, Any] | None = None
        p_id = protocol_row.id if protocol_row else None
        if p_id is not None:
            latest_tvl = session.execute(
                select(TvlSnapshot)
                .where(TvlSnapshot.protocol_id == p_id)
                .order_by(TvlSnapshot.timestamp.desc())
                .limit(1)
            ).scalar_one_or_none()
            if latest_tvl:
                tvl_data = {
                    "total_usd": float(latest_tvl.total_usd) if latest_tvl.total_usd else None,
                    "defillama_tvl": float(latest_tvl.defillama_tvl) if latest_tvl.defillama_tvl else None,
                    "source": latest_tvl.source,
                    "timestamp": latest_tvl.timestamp.isoformat(),
                }

        # Audit reports from relational table
        from db.models import AuditReport

        audit_reports_list: list[dict[str, Any]] = []
        if protocol_row:
            audit_rows = (
                session.execute(
                    select(AuditReport)
                    .where(AuditReport.protocol_id == protocol_row.id)
                    .order_by(AuditReport.date.desc().nullslast())
                )
                .scalars()
                .all()
            )
            audit_reports_list = [
                {
                    "url": ar.url,
                    "pdf_url": ar.pdf_url,
                    "auditor": ar.auditor,
                    "title": ar.title,
                    "date": ar.date,
                    "confidence": float(ar.confidence) if ar.confidence is not None else None,
                }
                for ar in audit_rows
            ]

        return {
            "company": company_name,
            "protocol_id": protocol_row.id if protocol_row else None,
            "contract_count": len(contracts),
            "tvl": tvl_data,
            "audit_reports": audit_reports_list,
            "contracts": contracts,
            "principals": principals,
            "ownership_hierarchy": hierarchy,
            "fund_flows": fund_flows,
            "all_addresses": all_addresses,
        }


def _audit_report_to_dict(ar: Any) -> dict[str, Any]:
    """Serialize an AuditReport row, including text- and scope-extraction state."""
    from utils.github_urls import github_blob_to_raw

    scope_contracts = list(ar.scope_contracts or [])
    return {
        "id": ar.id,
        "url": ar.url,
        "pdf_url": github_blob_to_raw(ar.pdf_url) if ar.pdf_url else None,
        "auditor": ar.auditor,
        "title": ar.title,
        "date": ar.date,
        "confidence": float(ar.confidence) if ar.confidence is not None else None,
        "text_extraction_status": ar.text_extraction_status,
        "text_extracted_at": (ar.text_extracted_at.isoformat() if ar.text_extracted_at else None),
        "text_size_bytes": ar.text_size_bytes,
        "has_text": ar.text_extraction_status == "success",
        "scope_extraction_status": ar.scope_extraction_status,
        "scope_extracted_at": (ar.scope_extracted_at.isoformat() if ar.scope_extracted_at else None),
        "scope_contract_count": len(scope_contracts),
        "has_scope": ar.scope_extraction_status == "success",
        # Commit attribution: `reviewed_commits` is the flat list extracted
        # from the PDF via regex; `classified_commits` is the LLM-labeled
        # richer shape with {sha, label, context}. The frontend prefers the
        # classified list (filtered to label === "reviewed") and falls back
        # to reviewed_commits when the classification pass hasn't run.
        "reviewed_commits": list(ar.reviewed_commits or []),
        "classified_commits": list(ar.classified_commits or []),
        "referenced_repos": list(ar.referenced_repos or []),
    }


@app.get("/api/company/{company_name}/v2_capabilities")
def company_v2_capabilities(company_name: str) -> dict[str, Any]:
    """v2 capability map for every analyzed contract in a company.

    Returned as a separate endpoint (not embedded in the company-
    overview payload) because resolving capabilities requires
    running the AdapterRegistry over each contract's predicate
    trees + repo lookups — adds tens of milliseconds per contract,
    not free to include in the already-1-3MB overview response.
    UI consumers fetch this when they want to render guard
    details for the v2 cutover; otherwise they keep using the
    overview's v1 fields.

    Response shape::

        {
          "company": "<name>",
          "contracts": {
            "0xab...": {
              "guardedFn()": {
                "kind": "finite_set", "members": [...],
                "membership_quality": "exact",
                "confidence": "enumerable", ...
              },
              ...
            },
            "0xcd...": {...},
            "0xef...": null   // analyzed but no v2 artifact (legacy)
          },
          "missing_v2_count": <int>
        }

    A contract with no v2 artifact maps to ``null`` so consumers
    can distinguish "not yet v2-analyzed" from "v2-analyzed and
    has no guarded functions" (the latter maps to ``{}``).

    NOT admin-gated — read-only / idempotent, the same shape
    contract as ``/api/contract/{addr}/capabilities``.
    """
    from services.resolution.capability_resolver import resolve_contract_capabilities

    with SessionLocal() as session:
        protocol_row = session.execute(
            select(Protocol).where(Protocol.name == company_name)
        ).scalar_one_or_none()
        if protocol_row is None:
            raise HTTPException(status_code=404, detail="Company not found")

        # Distinct addresses with completed jobs in this company.
        # Same dedupe rule as cutover_dry_run: most-recent Job per
        # address wins via the resolver's own ordering.
        addresses = sorted(
            {
                (job.address or "").lower()
                for job in session.execute(
                    select(Job).where(
                        Job.protocol_id == protocol_row.id,
                        Job.status == JobStatus.completed,
                        Job.address.isnot(None),
                    )
                ).scalars()
                if job.address
            }
        )

        contracts: dict[str, Any] = {}
        missing = 0
        for addr in addresses:
            try:
                caps = resolve_contract_capabilities(session, address=addr)
            except Exception:
                logger.exception(
                    "v2 capabilities resolution failed for %s in company %s; "
                    "treating as missing",
                    addr,
                    company_name,
                )
                caps = None
            if caps is None:
                missing += 1
            contracts[addr] = caps

        return {
            "company": company_name,
            "contracts": contracts,
            "missing_v2_count": missing,
        }


@app.get("/api/company/{company_name}/audits")
def company_audits(company_name: str) -> dict[str, Any]:
    """List all known audit reports for a company."""
    from db.models import AuditReport

    with SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
            raise HTTPException(status_code=404, detail="Company not found")

        audit_rows = (
            session.execute(
                select(AuditReport)
                .where(AuditReport.protocol_id == protocol_row.id)
                .order_by(AuditReport.date.desc().nullslast())
            )
            .scalars()
            .all()
        )
        return {
            "company": company_name,
            "protocol_id": protocol_row.id,
            "audit_count": len(audit_rows),
            "audits": [_audit_report_to_dict(ar) for ar in audit_rows],
        }


# Failed-status lookback window for the pipeline endpoint. Keeps the
# "recent failures" panel from growing unbounded while still surfacing
# anything an on-call dev would want to see.
_PIPELINE_FAILED_LOOKBACK_HOURS = 24

# Hard cap per bucket so a pathological backlog can't wedge the monitor.
_PIPELINE_BUCKET_LIMIT = 50


def _pipeline_item(ar: Any, protocol_name: str | None, now: datetime) -> dict[str, Any]:
    """Shape one audit row for the monitor page's live timeline."""
    started = ar.text_extraction_started_at or ar.scope_extraction_started_at
    elapsed = int((now - started).total_seconds()) if started else None
    scope_contracts = list(ar.scope_contracts or [])
    reviewed_commits = list(ar.reviewed_commits or [])
    referenced_repos = list(ar.referenced_repos or [])
    scope_entries = list(ar.scope_entries or [])
    classified_commits = list(ar.classified_commits or [])
    return {
        "audit_id": ar.id,
        "protocol_id": ar.protocol_id,
        "company": protocol_name,
        "auditor": ar.auditor,
        "title": ar.title,
        "date": ar.date,
        "pdf_url": ar.pdf_url,
        "worker_id": (
            ar.text_extraction_worker if ar.text_extraction_status == "processing" else ar.scope_extraction_worker
        ),
        "started_at": started.isoformat() if started else None,
        "elapsed_seconds": elapsed,
        "text_extraction_status": ar.text_extraction_status,
        "text_extracted_at": (ar.text_extracted_at.isoformat() if ar.text_extracted_at else None),
        "text_size_bytes": ar.text_size_bytes,
        "scope_extraction_status": ar.scope_extraction_status,
        "scope_extracted_at": (ar.scope_extracted_at.isoformat() if ar.scope_extracted_at else None),
        "scope_contract_count": len(scope_contracts),
        "reviewed_commit_count": len(reviewed_commits),
        "referenced_repo_count": len(referenced_repos),
        "scope_entry_count": len(scope_entries),
        "classified_commit_count": len(classified_commits),
        "error": (
            ar.text_extraction_error
            if ar.text_extraction_status == "failed"
            else ar.scope_extraction_error
            if ar.scope_extraction_status == "failed"
            else None
        ),
    }


@app.get("/api/audits/pipeline")
def audits_pipeline() -> dict[str, Any]:
    """In-flight audit text + scope extraction, grouped by bucket.

    Feeds the monitor page's "Audit Extraction" shelf (parallel to
    ``/api/jobs`` for the job pipeline). Text and scope workers drive a
    column state machine on ``audit_reports`` rather than the ``jobs``
    queue, so they need their own endpoint.

    Response shape per worker:
        {
          "processing": [item, ...],  # currently being worked
          "pending":    [item, ...],  # ready to claim, not yet picked up
          "failed":     [item, ...],  # terminal failures in the last 24h
        }

    The scope ``pending`` list only includes rows whose text extraction has
    already succeeded — otherwise they aren't actually claimable.

    Each list is capped at ``_PIPELINE_BUCKET_LIMIT`` entries; callers
    should surface an overflow indicator when counts hit the cap.

    Route MUST stay registered before ``/api/audits/{audit_id}`` — FastAPI
    matches in declaration order and the param route would otherwise try to
    parse ``"pipeline"`` as an int and 422.
    """
    from db.models import AuditReport

    now = datetime.now(timezone.utc)
    failed_cutoff = now - timedelta(hours=_PIPELINE_FAILED_LOOKBACK_HOURS)

    with SessionLocal() as session:
        # Pre-load protocol names in one query so every row can be decorated
        # without N+1 lookups.
        protocol_names: dict[int, str] = {
            row.id: row.name for row in session.execute(select(Protocol.id, Protocol.name)).all()
        }

        def _fetch(stmt) -> list[Any]:
            return list(session.execute(stmt).scalars().all())

        text_processing = _fetch(
            select(AuditReport)
            .where(AuditReport.text_extraction_status == "processing")
            .order_by(AuditReport.text_extraction_started_at.asc().nullslast())
            .limit(_PIPELINE_BUCKET_LIMIT)
        )
        text_pending = _fetch(
            select(AuditReport)
            .where(AuditReport.text_extraction_status.is_(None))
            .order_by(AuditReport.discovered_at.asc().nullslast())
            .limit(_PIPELINE_BUCKET_LIMIT)
        )
        text_failed = _fetch(
            select(AuditReport)
            .where(
                AuditReport.text_extraction_status == "failed",
                AuditReport.text_extracted_at >= failed_cutoff,
            )
            .order_by(AuditReport.text_extracted_at.desc().nullslast())
            .limit(_PIPELINE_BUCKET_LIMIT)
        )

        # Scope is only reachable once text extraction succeeded. Filter on
        # that so the "pending" count reflects actually-claimable work.
        scope_processing = _fetch(
            select(AuditReport)
            .where(AuditReport.scope_extraction_status == "processing")
            .order_by(AuditReport.scope_extraction_started_at.asc().nullslast())
            .limit(_PIPELINE_BUCKET_LIMIT)
        )
        scope_pending = _fetch(
            select(AuditReport)
            .where(
                AuditReport.scope_extraction_status.is_(None),
                AuditReport.text_extraction_status == "success",
            )
            .order_by(AuditReport.text_extracted_at.asc().nullslast())
            .limit(_PIPELINE_BUCKET_LIMIT)
        )
        scope_failed = _fetch(
            select(AuditReport)
            .where(
                AuditReport.scope_extraction_status == "failed",
                AuditReport.scope_extracted_at >= failed_cutoff,
            )
            .order_by(AuditReport.scope_extracted_at.desc().nullslast())
            .limit(_PIPELINE_BUCKET_LIMIT)
        )

        def _shape(rows: list[Any]) -> list[dict[str, Any]]:
            return [_pipeline_item(ar, protocol_names.get(ar.protocol_id), now) for ar in rows]

        return {
            "text_extraction": {
                "processing": _shape(text_processing),
                "pending": _shape(text_pending),
                "failed": _shape(text_failed),
            },
            "scope_extraction": {
                "processing": _shape(scope_processing),
                "pending": _shape(scope_pending),
                "failed": _shape(scope_failed),
            },
            "generated_at": now.isoformat(),
        }


@app.get("/api/audits/{audit_id}")
def get_audit(audit_id: int) -> dict[str, Any]:
    """Fetch a single audit report's metadata, including text-extraction state."""
    from db.models import AuditReport

    with SessionLocal() as session:
        ar = session.get(AuditReport, audit_id)
        if ar is None:
            raise HTTPException(status_code=404, detail="Audit not found")
        return _audit_report_to_dict(ar)


@app.get("/api/audits/{audit_id}/pdf")
def get_audit_pdf(audit_id: int):
    """Proxy an audit's PDF through our origin so the frontend can embed it
    in an iframe. The typical source (GitHub raw content, auditor sites)
    serves PDFs with `X-Frame-Options: deny` and `Content-Type:
    application/octet-stream`, both of which prevent inline rendering — we
    need a passthrough that strips those headers and sets
    `Content-Type: application/pdf`.

    Only proxies URLs already stored in `AuditReport` rows (admin-curated),
    so this is not a generic fetch-any-url SSRF gadget.
    """
    import requests
    from fastapi.responses import Response

    from db.models import AuditReport
    from utils.github_urls import github_blob_to_raw

    with SessionLocal() as session:
        ar = session.get(AuditReport, audit_id)
        if ar is None:
            raise HTTPException(status_code=404, detail="Audit not found")
        url = ar.pdf_url or (ar.url if ar.url and ar.url.lower().endswith(".pdf") else None)
        if not url:
            raise HTTPException(status_code=404, detail="No PDF available for this audit")
        url = github_blob_to_raw(url)
        filename = f"audit-{audit_id}.pdf"

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch PDF: {exc}") from exc

    return Response(
        content=resp.content,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            "Cache-Control": "public, max-age=3600",
        },
    )


@app.get("/api/audits/{audit_id}/text", response_class=PlainTextResponse)
def get_audit_text(audit_id: int) -> str:
    """Return the extracted plain-text body of an audit report.

    Streams the text directly from object storage. Returns 404 for an
    unknown audit, 409 if extraction hasn't completed successfully yet
    (so the caller knows to retry later), and 503 if object storage is
    unreachable.
    """
    from db.models import AuditReport

    with SessionLocal() as session:
        ar = session.get(AuditReport, audit_id)
        if ar is None:
            raise HTTPException(status_code=404, detail="Audit not found")

        if ar.text_extraction_status != "success" or not ar.text_storage_key:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "text not available",
                    "status": ar.text_extraction_status,
                    "reason": ar.text_extraction_error,
                },
            )

        storage_key = ar.text_storage_key

    client = get_storage_client()
    if client is None:
        raise HTTPException(status_code=503, detail="object storage not configured")
    try:
        body = client.get(storage_key)
    except StorageUnavailable as exc:
        raise HTTPException(status_code=503, detail=f"storage error: {exc}") from exc
    except StorageError as exc:
        # Covers StorageKeyMissing — DB says text is available but the object
        # got deleted. Inconsistent state; surface as 500 so ops notice.
        raise HTTPException(
            status_code=500,
            detail=f"text record missing from storage: {exc}",
        ) from exc
    return body.decode("utf-8")


@app.get("/api/audits/{audit_id}/scope")
def get_audit_scope(audit_id: int) -> dict[str, Any]:
    """Return the list of in-scope contracts + date for a completed audit.

    Reads from the denormalized ``scope_contracts`` column — the JSON
    artifact in object storage is source-of-truth but not served here
    (that would be a debug-only endpoint). 404 for unknown audit, 409 if
    scope extraction hasn't completed successfully.
    """
    from db.models import AuditReport

    with SessionLocal() as session:
        ar = session.get(AuditReport, audit_id)
        if ar is None:
            raise HTTPException(status_code=404, detail="Audit not found")
        if ar.scope_extraction_status != "success":
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "scope not available",
                    "status": ar.scope_extraction_status,
                    "reason": ar.scope_extraction_error,
                },
            )
        return {
            "audit_id": audit_id,
            "auditor": ar.auditor,
            "title": ar.title,
            "date": ar.date,
            "contracts": list(ar.scope_contracts or []),
            "scope_extracted_at": (ar.scope_extracted_at.isoformat() if ar.scope_extracted_at else None),
        }


# --- Bytecode drift cache ----------------------------------------------
#
# Per-process TTL cache of eth_getCode keccak hashes keyed by address.
# The audit_timeline endpoint fetches these live to compare against the
# per-coverage-row ``bytecode_keccak_at_match`` — a rapid reload of the
# surface view shouldn't fire one RPC per audit row on every request.
# TTL is intentionally short (30s) so "just upgraded" reflects quickly
# in the UI when someone's actively debugging a drift.

_BYTECODE_KECCAK_CACHE: dict[str, tuple[float, str | None]] = {}
_BYTECODE_KECCAK_TTL_SECONDS: float = 30.0


def _bytecode_keccak_now_batch(addresses: set[str]) -> dict[str, str | None]:
    """Return ``{lower_address: keccak_hex_or_None}`` for a set of addresses.

    Uses a short TTL cache so the typical burst-of-requests pattern (UI
    loading and user flipping between contracts) only pays for one RPC
    per impl per 30s. A ``None`` result is cached too — a temporary RPC
    outage shouldn't cause a hot retry loop.
    """
    import time

    from services.audits.coverage import _fetch_bytecode_keccak

    now = time.monotonic()
    out: dict[str, str | None] = {}
    for raw in addresses:
        if not raw:
            continue
        addr = raw.lower()
        cached = _BYTECODE_KECCAK_CACHE.get(addr)
        if cached is not None and (now - cached[0]) < _BYTECODE_KECCAK_TTL_SECONDS:
            out[addr] = cached[1]
            continue
        keccak = _fetch_bytecode_keccak(addr)
        _BYTECODE_KECCAK_CACHE[addr] = (now, keccak)
        out[addr] = keccak
    return out


def _audit_brief(audit: Any, match: Any | None = None) -> dict[str, Any]:
    """Compact audit-report dict for the coverage/timeline endpoints."""
    out: dict[str, Any] = {
        "audit_id": audit.id,
        "auditor": audit.auditor,
        "title": audit.title,
        "date": audit.date,
    }
    if match is not None:
        out["match_type"] = match.match_type
        out["match_confidence"] = match.match_confidence
        out["covered_from_block"] = match.covered_from_block
        out["covered_to_block"] = match.covered_to_block
        # Source-equivalence verdict — see services.audits.source_equivalence
        # for the status vocabulary. ``proven`` means cryptographically
        # verified (file SHA-256 match between audit's GitHub commit and
        # Etherscan-verified source). Other values describe *why* the
        # check couldn't produce a proof so the UI can badge specifically.
        out["equivalence_status"] = getattr(match, "equivalence_status", None)
        out["equivalence_reason"] = getattr(match, "equivalence_reason", None)
        equivalence_checked_at = getattr(match, "equivalence_checked_at", None)
        out["equivalence_checked_at"] = equivalence_checked_at.isoformat() if equivalence_checked_at else None
        # Phase C: proof_kind is the strength subtype of ``proven`` rows.
        # 'pre_fix_unpatched' gets special treatment in the UI as a RED
        # FLAG — the audit reviewed exactly this code AND the protocol
        # knew of a fix but never shipped it.
        out["proof_kind"] = getattr(match, "proof_kind", None)
        # The specific commit SHA this contract's bytecode matched during
        # source-equivalence verification. NULL on heuristic matches and on
        # rows verified before the column existed; re-running
        # refresh_coverage repopulates.
        out["matched_commit_sha"] = getattr(match, "matched_commit_sha", None)
    return out


@app.get("/api/company/{company_name}/audit_coverage")
def company_audit_coverage(company_name: str) -> dict[str, Any]:
    """For each contract in the company's inventory, list audits covering it.

    Reads from the persisted ``audit_contract_coverage`` join table — rows
    are written proxy-aware by ``services.audits.coverage`` when scope
    extraction completes or a live upgrade event is detected. The
    ``last_audit`` pointer is the most recent matching audit by ``date``
    (nulls last, then id desc to break ties). Each audit entry carries
    ``match_type`` + ``match_confidence`` so the UI can flag low-confidence
    links differently.
    """
    from db.models import AuditContractCoverage, AuditReport

    with SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
            raise HTTPException(status_code=404, detail="Company not found")

        contracts = session.execute(select(Contract).where(Contract.protocol_id == protocol_row.id)).scalars().all()

        audit_rows = (
            session.execute(
                select(AuditReport)
                .where(
                    AuditReport.protocol_id == protocol_row.id,
                    AuditReport.scope_extraction_status == "success",
                )
                .order_by(AuditReport.date.desc().nullslast(), AuditReport.id.desc())
            )
            .scalars()
            .all()
        )
        audits_by_id = {a.id: a for a in audit_rows}

        # Pull every coverage row for the protocol in one query, then
        # bucket in Python — cheaper than N queries for N contracts.
        coverage_rows = (
            session.execute(
                select(AuditContractCoverage).where(
                    AuditContractCoverage.protocol_id == protocol_row.id,
                )
            )
            .scalars()
            .all()
        )
        coverage_by_contract: dict[int, list[Any]] = {}
        for row in coverage_rows:
            coverage_by_contract.setdefault(row.contract_id, []).append(row)

        def _sort_key(row: Any) -> tuple:
            audit = audits_by_id.get(row.audit_report_id)
            date = (audit.date if audit else None) or ""
            return (date, row.audit_report_id)

        # Proxy rows don't hold their own coverage rows — the scope
        # matcher writes against the impl Contract row. For the
        # company-level "is this contract audited?" view the user really
        # means "is the code this address is running audited?", so union
        # the proxy's entries with its current implementation's.
        contracts_by_addr = {c.address.lower(): c for c in contracts if c.address}

        coverage: list[dict[str, Any]] = []
        for c in contracts:
            entries = list(coverage_by_contract.get(c.id, []))
            seen_audit_ids = {e.audit_report_id for e in entries}
            if c.is_proxy and c.implementation:
                impl = contracts_by_addr.get(c.implementation.lower())
                if impl:
                    for e in coverage_by_contract.get(impl.id, []):
                        if e.audit_report_id not in seen_audit_ids:
                            entries.append(e)
                            seen_audit_ids.add(e.audit_report_id)
            entries = sorted(entries, key=_sort_key, reverse=True)
            matching = [
                _audit_brief(audits_by_id[e.audit_report_id], e) for e in entries if e.audit_report_id in audits_by_id
            ]
            coverage.append(
                {
                    "address": c.address,
                    "chain": c.chain,
                    "contract_name": c.contract_name,
                    "audit_count": len(matching),
                    "last_audit": matching[0] if matching else None,
                    "audits": matching,
                }
            )
        return {
            "company": company_name,
            "protocol_id": protocol_row.id,
            "contract_count": len(contracts),
            "audit_count": len(audit_rows),
            "coverage": coverage,
        }


@app.get("/api/contracts/{contract_id}/audit_timeline")
def contract_audit_timeline(contract_id: int) -> dict[str, Any]:
    """Per-impl audit timeline for a single contract, annotated with coverage.

    Response shape:
      - ``contract``: address, name, is_proxy, current_implementation
      - ``impl_windows``: list of (from_block, to_block, impl_address, ...)
        gathered from ``UpgradeEvent`` history; empty for non-proxies and
        for proxies whose first impl never emitted an upgrade event
      - ``coverage``: list of coverage rows with the audit + range
      - ``current_status``: one of
        ``'audited'`` — current impl is covered by ≥1 high/medium audit
        ``'unaudited_since_upgrade'`` — some prior impl was covered but
          the current impl isn't
        ``'never_audited'`` — proxy with impls, no coverage anywhere
        ``'non_proxy_audited'`` / ``'non_proxy_unaudited'`` — plain contracts
    """
    from db.models import AuditContractCoverage, AuditReport

    with SessionLocal() as session:
        contract = session.get(Contract, contract_id)
        if contract is None:
            raise HTTPException(status_code=404, detail="Contract not found")

        # Historical upgrade windows on this contract if it's a proxy.
        upgrade_rows = (
            session.execute(
                select(UpgradeEvent)
                .where(UpgradeEvent.contract_id == contract.id)
                .order_by(
                    UpgradeEvent.block_number.asc().nullslast(),
                    UpgradeEvent.id.asc(),
                )
            )
            .scalars()
            .all()
        )
        impl_windows: list[dict[str, Any]] = []
        for i, ev in enumerate(upgrade_rows):
            nxt = upgrade_rows[i + 1] if i + 1 < len(upgrade_rows) else None
            impl_windows.append(
                {
                    "impl_address": ev.new_impl,
                    "from_block": ev.block_number,
                    "to_block": nxt.block_number if nxt is not None else None,
                    "from_ts": ev.timestamp.isoformat() if ev.timestamp else None,
                    "to_ts": nxt.timestamp.isoformat() if (nxt and nxt.timestamp) else None,
                    "tx_hash": ev.tx_hash,
                }
            )

        # Coverage rows. For a proxy the timeline should show every audit
        # that covered ANY impl in its history — not just direct name
        # matches on the proxy row. We union:
        #   - rows keyed to the contract itself (direct matches OR, when
        #     the contract is an impl, its own impl_era coverage)
        #   - for proxies, rows keyed to every historical-impl Contract.id
        #     resolved from UpgradeEvent.new_impl
        scope_contract_ids = {contract.id}
        if contract.is_proxy:
            # Union coverage from every impl this proxy has referenced:
            # historical impls via UpgradeEvent.new_impl plus the current
            # pointer in Contract.implementation. The current-impl branch
            # matters for proxies whose UpgradeEvent rows haven't been
            # projected into the DB yet — without it, coverage for those
            # proxies stays empty even though _current_status can see it
            # via the separate Contract.implementation lookup.
            impl_addrs: set[str] = set()
            if upgrade_rows:
                impl_addrs.update(ev.new_impl.lower() for ev in upgrade_rows if ev.new_impl)
            if contract.implementation:
                impl_addrs.add(contract.implementation.lower())
            if impl_addrs:
                impl_contract_ids = (
                    session.execute(
                        select(Contract.id).where(
                            Contract.protocol_id == contract.protocol_id,
                            Contract.address.in_(impl_addrs),
                        )
                    )
                    .scalars()
                    .all()
                )
                scope_contract_ids.update(impl_contract_ids)

        cov_rows = (
            session.execute(
                select(AuditContractCoverage).where(
                    AuditContractCoverage.contract_id.in_(scope_contract_ids),
                )
            )
            .scalars()
            .all()
        )
        audit_ids = [r.audit_report_id for r in cov_rows]
        audits_by_id: dict[int, Any] = {}
        if audit_ids:
            audits_by_id = {
                a.id: a
                for a in session.execute(select(AuditReport).where(AuditReport.id.in_(audit_ids))).scalars().all()
            }

        # Dedupe: multiple impl rows can produce rows against the same
        # audit_id (the audit's scope name matched several historical
        # impls). Rank by (confidence, match_type) so cryptographic
        # source-equivalence proofs always beat heuristic temporal
        # matches at equal confidence. Without the type tiebreaker,
        # two ``high`` rows fell through to first-iterated-wins (no SQL
        # ORDER BY on cov_rows), which silently flipped audits off the
        # current impl in the UI even when the DB had a reviewed_commit
        # row pinning them there.
        from services.audits.coverage import _row_score

        best_by_audit: dict[int, Any] = {}
        for r in cov_rows:
            prev = best_by_audit.get(r.audit_report_id)
            if prev is None or _row_score(r) > _row_score(prev):
                best_by_audit[r.audit_report_id] = r

        # Address-by-contract_id lookup so the UI can attribute each
        # coverage row to a specific impl-era in the timeline (critical
        # for reviewed_commit matches, which carry no block range but
        # are proven against one specific Contract row's on-chain code).
        addr_by_cid: dict[int, str] = {
            cid: addr
            for cid, addr in session.execute(
                select(Contract.id, Contract.address).where(Contract.id.in_(scope_contract_ids))
            ).all()
        }

        # Live bytecode keccak for every impl referenced by a coverage row —
        # one RPC per distinct address, cached briefly so repeated hits to
        # this endpoint don't spam the provider. Compared against the
        # ``bytecode_keccak_at_match`` stored by ``services.audits.coverage``
        # to produce ``bytecode_drift``.
        live_keccaks = _bytecode_keccak_now_batch(
            {addr_by_cid[r.contract_id] for r in best_by_audit.values() if r.contract_id in addr_by_cid}
        )

        coverage_out: list[dict[str, Any]] = []
        for r in best_by_audit.values():
            audit = audits_by_id.get(r.audit_report_id)
            if not audit:
                continue
            brief = _audit_brief(audit, r)
            impl_addr = addr_by_cid.get(r.contract_id)
            brief["impl_address"] = impl_addr
            brief["bytecode_keccak_at_match"] = r.bytecode_keccak_at_match
            now_keccak = live_keccaks.get(impl_addr.lower()) if impl_addr else None
            brief["bytecode_keccak_now"] = now_keccak
            # Drift is only asserted when BOTH are known and differ. A NULL
            # on either side leaves drift=None so the UI can say
            # "unverified" rather than falsely flashing a drift warning.
            if r.bytecode_keccak_at_match and now_keccak:
                brief["bytecode_drift"] = r.bytecode_keccak_at_match.lower() != now_keccak.lower()
            else:
                brief["bytecode_drift"] = None
            brief["verified_at"] = r.verified_at.isoformat() if r.verified_at else None
            # live_findings: audit.findings filtered to non-'fixed' statuses.
            # Phase 3a seeds these manually; Phase 3b (deferred) fills them
            # from scope extraction. None/missing → empty list, not an error.
            findings = audit.findings or []
            brief["live_findings"] = [
                f for f in findings if isinstance(f, dict) and (f.get("status") or "").lower() != "fixed"
            ]
            coverage_out.append(brief)
        # Newest first by audit date (nulls last, id desc to break ties).
        coverage_out.sort(key=lambda e: (e.get("date") or "", e["audit_id"]), reverse=True)

        # --- current_status computation ---
        #
        # "audited" is a strong claim we only grant when the current impl
        # has a HIGH-confidence open-ended coverage row (audit dated inside
        # the impl's active window). A 'medium' match means the audit sits
        # in the grace zone on either side of the window boundary — e.g.
        # "audit published 10 days before this impl went live, so it might
        # have reviewed the new code, but also might have reviewed the
        # previous impl and been finalized right before the upgrade." Those
        # audits still appear in the ``coverage`` array; the UI can surface
        # them without the contract being badged as audited on their
        # strength alone.
        def _current_status(c: Contract) -> str:
            if not c.is_proxy:
                return "non_proxy_audited" if cov_rows else "non_proxy_unaudited"

            # Proxy: need to know if the current impl has live coverage.
            current_impl = c.implementation
            if not current_impl:
                return "never_audited" if not cov_rows else "unaudited_since_upgrade"

            impl_contract = session.execute(
                select(Contract).where(
                    Contract.address == current_impl.lower(),
                    Contract.protocol_id == c.protocol_id,
                )
            ).scalar_one_or_none()
            if impl_contract is None:
                # Current impl isn't in inventory. We can't tell.
                return "unaudited_since_upgrade" if cov_rows else "never_audited"

            # impl_contract.id is always in scope_contract_ids by the time we
            # get here, so cov_rows already covers it.
            current_cov = [r for r in cov_rows if r.contract_id == impl_contract.id]
            # 'audited' requires definitive coverage of the currently-open
            # impl window. Two paths:
            #   (a) any row on this impl has a cryptographic proof
            #       (equivalence_status='proven') with a non-coincidental
            #       proof kind — strongest evidence, overrides everything
            #       else on the impl;
            #   (b) a high-confidence open-ended temporal match AND no
            #       hash_mismatch anywhere on the impl. hash_mismatch
            #       is strong negative evidence — deployed code differs
            #       from what the auditor reviewed — so we don't let a
            #       heuristic temporal match paper over cryptographic
            #       disproof from a different audit. Weak
            #       ``proof_kind='cited_only'`` rows don't qualify here
            #       either just because their coverage row is
            #       ``reviewed_commit/high``.
            # Rows proven only via a 'cited_only' commit don't qualify:
            # the deployed code matched a SHA the PDF mentioned only for
            # context, not one the auditor actually reviewed.
            # Grace-medium temporal matches are intentionally NOT enough —
            # an audit 10 days before the impl went live is suggestive
            # but not proof the reviewed code is what shipped.
            has_proven = any(r.equivalence_status == "proven" and r.proof_kind != "cited_only" for r in current_cov)
            has_temporal_high = any(
                r.match_confidence == "high"
                and r.covered_to_block is None
                and not (r.equivalence_status == "proven" and r.proof_kind == "cited_only")
                for r in current_cov
            )
            has_hash_mismatch = any(r.equivalence_status == "hash_mismatch" for r in current_cov)
            if has_proven:
                return "audited"
            if has_temporal_high and not has_hash_mismatch:
                return "audited"
            if current_cov or cov_rows:
                return "unaudited_since_upgrade"
            return "never_audited"

        return {
            "contract": {
                "contract_id": contract.id,
                "address": contract.address,
                "chain": contract.chain,
                "contract_name": contract.contract_name,
                "is_proxy": contract.is_proxy,
                "current_implementation": contract.implementation,
            },
            "impl_windows": impl_windows,
            "coverage": coverage_out,
            "current_status": _current_status(contract),
        }


@app.post(
    "/api/company/{company_name}/refresh_coverage",
    dependencies=[Depends(require_admin_key)],
)
def refresh_company_coverage(
    company_name: str,
    verify_source_equivalence: bool = True,
) -> dict[str, Any]:
    """Rebuild ``audit_contract_coverage`` rows for every scoped audit in a protocol.

    Idempotent backfill. Useful when inventory is updated after audits are
    scoped (new Contract rows match pre-existing audit scope) or when a
    bulk data migration needs to re-seat links without waiting for the
    next scope re-extraction.

    ``verify_source_equivalence`` defaults to true: for each audit with
    reviewed_commits + source_repo, compare the byte content of each
    scope file against Etherscan's verified source. Proven matches
    upgrade to ``match_type='reviewed_commit'`` / ``match_confidence='high'``
    and every row gets an ``equivalence_status`` + ``equivalence_reason``
    stamp so the UI can surface failure modes (hash_mismatch, commit
    not in repo, etc.). Pass ``?verify_source_equivalence=false`` to
    skip the network pass for a fast heuristic-only refresh.
    """
    from services.audits.coverage import upsert_coverage_for_protocol

    with SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
            raise HTTPException(status_code=404, detail="Company not found")
        inserted = upsert_coverage_for_protocol(
            session,
            protocol_row.id,
            verify_source_equivalence=verify_source_equivalence,
        )
        session.commit()
        return {
            "company": company_name,
            "protocol_id": protocol_row.id,
            "coverage_rows": inserted,
            "verify_source_equivalence": verify_source_equivalence,
        }


@app.post(
    "/api/audits/{audit_id}/reextract_scope",
    dependencies=[Depends(require_admin_key)],
)
def reextract_audit_scope(audit_id: int) -> dict[str, Any]:
    """Reset scope-extraction state so the worker picks the row up again.

    Requires that text extraction already succeeded — without the stored
    text body there's nothing to re-scope. Idempotent: a fresh row with
    NULL status is a no-op reset.
    """
    from db.models import AuditReport

    with SessionLocal() as session:
        ar = session.get(AuditReport, audit_id)
        if ar is None:
            raise HTTPException(status_code=404, detail="Audit not found")
        if ar.text_extraction_status != "success":
            raise HTTPException(
                status_code=409,
                detail="text extraction has not succeeded for this audit",
            )
        ar.scope_extraction_status = None
        ar.scope_extraction_error = None
        ar.scope_extraction_worker = None
        ar.scope_extraction_started_at = None
        session.commit()
    return {"audit_id": audit_id, "reset": True}


class AddAuditRequest(BaseModel):
    url: str = Field(min_length=1)
    pdf_url: str | None = None
    auditor: str = Field(min_length=1)
    title: str = Field(min_length=1)
    date: str | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    source_repo: str | None = None


@app.post(
    "/api/company/{company_name}/audits",
    dependencies=[Depends(require_admin_key)],
)
def add_company_audit(company_name: str, req: AddAuditRequest) -> dict[str, Any]:
    """Register a new audit report for a protocol.

    The row is inserted with NULL text/scope extraction status, so the
    standing workers will claim it on their next poll: text extraction
    downloads the PDF, scope extraction parses the contracts + commits,
    and coverage matching wires it to deployed addresses. Duplicates
    (same url on the same protocol) are rejected with 409.
    """
    from db.models import AuditReport

    with SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
            raise HTTPException(status_code=404, detail="Company not found")

        existing = session.execute(
            select(AuditReport).where(
                AuditReport.protocol_id == protocol_row.id,
                AuditReport.url == req.url,
            )
        ).scalar_one_or_none()
        if existing is not None:
            raise HTTPException(
                status_code=409,
                detail=f"Audit with this url already exists (id={existing.id})",
            )

        ar = AuditReport(
            protocol_id=protocol_row.id,
            url=req.url,
            pdf_url=req.pdf_url or req.url,
            auditor=req.auditor,
            title=req.title,
            date=req.date,
            confidence=req.confidence,
            source_repo=req.source_repo,
        )
        session.add(ar)
        session.commit()
        session.refresh(ar)
        return _audit_report_to_dict(ar)


@app.delete(
    "/api/audits/{audit_id}",
    dependencies=[Depends(require_admin_key)],
)
def delete_audit(audit_id: int) -> dict[str, Any]:
    """Remove an audit report (cascades to coverage rows)."""
    from db.models import AuditReport

    with SessionLocal() as session:
        ar = session.get(AuditReport, audit_id)
        if ar is None:
            raise HTTPException(status_code=404, detail="Audit not found")
        session.delete(ar)
        session.commit()
    return {"audit_id": audit_id, "deleted": True}


def _watched_proxy_to_dict(proxy: WatchedProxy) -> dict[str, Any]:
    return {
        "id": str(proxy.id),
        "proxy_address": proxy.proxy_address,
        "chain": proxy.chain,
        "label": proxy.label,
        "proxy_type": proxy.proxy_type,
        "needs_polling": proxy.needs_polling,
        "last_known_implementation": proxy.last_known_implementation,
        "last_scanned_block": proxy.last_scanned_block,
        "created_at": proxy.created_at.isoformat(),
    }


@app.post("/api/watched-proxies", dependencies=[Depends(require_admin_key)])
def add_watched_proxy(request: WatchProxyRequest) -> dict[str, Any]:
    """Subscribe to proxy upgrade notifications."""
    if not request.address.startswith("0x"):
        raise HTTPException(status_code=400, detail="Address must start with 0x")
    address = request.address.lower()

    # Resolve RPC URL: explicit param > env default
    rpc_url = request.rpc_url or DEFAULT_RPC_URL

    # Block SSRF: reject non-http(s) schemes and private/internal URLs
    # Skip check when using the server's own default RPC (from ETH_RPC env var)
    if request.rpc_url:
        from urllib.parse import urlparse

        parsed = urlparse(rpc_url)
        if parsed.scheme not in ("http", "https"):
            raise HTTPException(status_code=400, detail="rpc_url must use http or https")
        hostname = parsed.hostname or ""
        if (
            hostname in ("localhost", "127.0.0.1", "0.0.0.0", "::1")
            or hostname.startswith("169.254.")
            or hostname.startswith("10.")
            or hostname.startswith("192.168.")
        ):
            raise HTTPException(status_code=400, detail="rpc_url must not point to internal addresses")

    # Classify the proxy to determine type, needs_polling, and current implementation
    from services.discovery.classifier import _KNOWN_EVENT_PROXY_TYPES, classify_single
    from services.monitoring.proxy_watcher import get_latest_block, resolve_current_implementation

    proxy_type = None
    needs_polling = False
    try:
        classification = classify_single(address, rpc_url)
        if classification.get("type") == "proxy":
            proxy_type = classification.get("proxy_type")
            needs_polling = proxy_type not in _KNOWN_EVENT_PROXY_TYPES
    except Exception:
        pass  # classification failure is non-fatal — watch with fallback resolution

    current_impl = resolve_current_implementation(address, rpc_url, proxy_type=proxy_type)

    # Starting scan point: explicit from_block > current block
    if request.from_block is not None:
        from_block = request.from_block
    else:
        try:
            from_block = get_latest_block(rpc_url)
        except Exception:
            raise HTTPException(
                status_code=502,
                detail="Could not determine current block. Provide from_block explicitly.",
            )

    with SessionLocal() as session:
        # Check if proxy is already watched — if so, just add a subscription
        existing = session.execute(
            select(WatchedProxy).where(
                WatchedProxy.proxy_address == address,
                WatchedProxy.chain == request.chain,
            )
        ).scalar_one_or_none()

        if existing:
            proxy = existing
        else:
            proxy = WatchedProxy(
                proxy_address=address,
                chain=request.chain,
                label=request.label,
                proxy_type=proxy_type,
                needs_polling=needs_polling,
                last_known_implementation=current_impl,
                last_scanned_block=from_block,
            )
            session.add(proxy)
            session.flush()

        # Create subscription if discord webhook provided
        subscription = None
        if request.discord_webhook_url:
            subscription = ProxySubscription(
                watched_proxy_id=proxy.id,
                discord_webhook_url=request.discord_webhook_url,
                label=request.label,
            )
            session.add(subscription)

        # Also create a MonitoredContract for unified monitoring
        existing_mc = session.execute(
            select(MonitoredContract).where(
                MonitoredContract.address == address,
                MonitoredContract.chain == request.chain,
            )
        ).scalar_one_or_none()
        if not existing_mc:
            mc = MonitoredContract(
                address=address,
                chain=request.chain,
                contract_type="proxy",
                watched_proxy_id=proxy.id,
                monitoring_config={"watch_upgrades": True, "watch_ownership": True},
                last_known_state={"implementation": current_impl} if current_impl else {},
                last_scanned_block=from_block,
                needs_polling=needs_polling,
                is_active=True,
                enrollment_source="proxy_watch",
            )
            session.add(mc)

        session.commit()
        session.refresh(proxy)
        result = _watched_proxy_to_dict(proxy)
        if subscription:
            session.refresh(subscription)
            result["subscription_id"] = str(subscription.id)
        return result


@app.get("/api/watched-proxies")
def list_watched_proxies() -> list[dict[str, Any]]:
    """List all watched proxy contracts."""
    with SessionLocal() as session:
        stmt = select(WatchedProxy).order_by(WatchedProxy.created_at.desc())
        proxies = session.execute(stmt).scalars().all()
        return [_watched_proxy_to_dict(p) for p in proxies]


@app.delete("/api/watched-proxies/{proxy_id}", dependencies=[Depends(require_admin_key)])
def remove_watched_proxy(proxy_id: str) -> dict[str, str]:
    """Stop watching a proxy contract."""
    with SessionLocal() as session:
        proxy = session.get(WatchedProxy, uuid.UUID(proxy_id))
        if proxy is None:
            raise HTTPException(status_code=404, detail="Watched proxy not found")
        session.delete(proxy)
        session.commit()
        return {"status": "removed"}


@app.get("/api/proxy-events")
def list_proxy_events(proxy_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    """List detected proxy upgrade events."""
    with SessionLocal() as session:
        stmt = select(ProxyUpgradeEvent).order_by(ProxyUpgradeEvent.detected_at.desc()).limit(limit)
        if proxy_id:
            stmt = stmt.where(ProxyUpgradeEvent.watched_proxy_id == proxy_id)
        events = session.execute(stmt).scalars().all()
        return [
            {
                "id": str(e.id),
                "watched_proxy_id": str(e.watched_proxy_id),
                "block_number": e.block_number,
                "tx_hash": e.tx_hash,
                "old_implementation": e.old_implementation,
                "new_implementation": e.new_implementation,
                "event_type": e.event_type,
                "detected_at": e.detected_at.isoformat(),
            }
            for e in events
        ]


@app.get("/api/watched-proxies/{proxy_id}/subscriptions")
def list_subscriptions(proxy_id: str) -> list[dict[str, Any]]:
    """List notification subscriptions for a watched proxy."""
    with SessionLocal() as session:
        proxy = session.get(WatchedProxy, uuid.UUID(proxy_id))
        if proxy is None:
            raise HTTPException(status_code=404, detail="Watched proxy not found")
        stmt = select(ProxySubscription).where(ProxySubscription.watched_proxy_id == proxy.id)
        subs = session.execute(stmt).scalars().all()
        return [
            {
                "id": str(s.id),
                "watched_proxy_id": str(s.watched_proxy_id),
                "discord_webhook_url": s.discord_webhook_url,
                "label": s.label,
                "created_at": s.created_at.isoformat(),
            }
            for s in subs
        ]


@app.post("/api/watched-proxies/{proxy_id}/subscriptions", dependencies=[Depends(require_admin_key)])
def add_subscription(proxy_id: str, request: SubscribeRequest) -> dict[str, Any]:
    """Add a notification subscription to an existing watched proxy."""
    with SessionLocal() as session:
        proxy = session.get(WatchedProxy, uuid.UUID(proxy_id))
        if proxy is None:
            raise HTTPException(status_code=404, detail="Watched proxy not found")
        sub = ProxySubscription(
            watched_proxy_id=proxy.id,
            discord_webhook_url=request.discord_webhook_url,
            label=request.label,
        )
        session.add(sub)
        session.commit()
        session.refresh(sub)
        return {
            "id": str(sub.id),
            "watched_proxy_id": str(sub.watched_proxy_id),
            "discord_webhook_url": sub.discord_webhook_url,
            "label": sub.label,
            "created_at": sub.created_at.isoformat(),
        }


@app.delete("/api/subscriptions/{subscription_id}", dependencies=[Depends(require_admin_key)])
def remove_subscription(subscription_id: str) -> dict[str, str]:
    """Remove a notification subscription."""
    with SessionLocal() as session:
        sub = session.get(ProxySubscription, uuid.UUID(subscription_id))
        if sub is None:
            raise HTTPException(status_code=404, detail="Subscription not found")
        session.delete(sub)
        session.commit()
        return {"status": "removed"}


# ---------------------------------------------------------------------------
# Unified protocol monitoring endpoints
# ---------------------------------------------------------------------------


@app.get("/api/protocols/{protocol_id}/monitoring")
def list_protocol_monitoring(protocol_id: int) -> list[dict[str, Any]]:
    """List all MonitoredContract rows for a protocol (including inactive)."""
    with SessionLocal() as session:
        stmt = select(MonitoredContract).where(
            MonitoredContract.protocol_id == protocol_id,
        )
        contracts = session.execute(stmt).scalars().all()
        return [
            {
                "id": str(c.id),
                "address": c.address,
                "chain": c.chain,
                "contract_type": c.contract_type,
                "monitoring_config": c.monitoring_config,
                "last_known_state": c.last_known_state,
                "last_scanned_block": c.last_scanned_block,
                "needs_polling": c.needs_polling,
                "is_active": c.is_active,
                "enrollment_source": c.enrollment_source,
                "created_at": c.created_at.isoformat() if c.created_at else None,
            }
            for c in contracts
        ]


@app.post("/api/protocols/{protocol_id}/re-enroll", dependencies=[Depends(require_admin_key)])
def re_enroll_protocol(protocol_id: int, chain: str = "ethereum") -> dict[str, Any]:
    """Manually trigger monitoring enrollment for a protocol.

    Calls enroll_protocol_contracts directly, bypassing the automatic
    in-flight job checks. Useful when enrollment produced wrong results
    or after manual DB changes.
    """
    rpc_url = DEFAULT_RPC_URL
    with SessionLocal() as session:
        protocol = session.get(Protocol, protocol_id)
        if protocol is None:
            raise HTTPException(status_code=404, detail="Protocol not found")

        from services.monitoring.enrollment import enroll_protocol_contracts

        enrolled = enroll_protocol_contracts(session, protocol_id, rpc_url, chain)
        return {
            "status": "enrolled",
            "protocol_id": protocol_id,
            "contracts_enrolled": len(enrolled),
            "contracts": [
                {
                    "id": str(mc.id),
                    "address": mc.address,
                    "contract_type": mc.contract_type,
                    "monitoring_config": mc.monitoring_config,
                    "needs_polling": mc.needs_polling,
                    "is_active": mc.is_active,
                }
                for mc in enrolled
            ],
        }


@app.post("/api/protocols/{protocol_id}/subscribe", dependencies=[Depends(require_admin_key)])
def subscribe_to_protocol(protocol_id: int, request: ProtocolSubscribeRequest) -> dict[str, Any]:
    """Create a ProtocolSubscription for governance event notifications."""
    with SessionLocal() as session:
        protocol = session.get(Protocol, protocol_id)
        if protocol is None:
            raise HTTPException(status_code=404, detail="Protocol not found")

        sub = ProtocolSubscription(
            protocol_id=protocol_id,
            discord_webhook_url=request.discord_webhook_url,
            label=request.label,
            event_filter=request.event_filter,
        )
        session.add(sub)
        session.commit()
        session.refresh(sub)
        return {
            "id": str(sub.id),
            "protocol_id": sub.protocol_id,
            "discord_webhook_url": sub.discord_webhook_url,
            "label": sub.label,
            "event_filter": sub.event_filter,
            "created_at": sub.created_at.isoformat() if sub.created_at else None,
        }


@app.get("/api/protocols/{protocol_id}/subscriptions")
def list_protocol_subscriptions(protocol_id: int) -> list[dict[str, Any]]:
    """List all ProtocolSubscription rows for a protocol."""
    with SessionLocal() as session:
        stmt = select(ProtocolSubscription).where(ProtocolSubscription.protocol_id == protocol_id)
        subs = session.execute(stmt).scalars().all()
        return [
            {
                "id": str(s.id),
                "protocol_id": s.protocol_id,
                "discord_webhook_url": s.discord_webhook_url,
                "label": s.label,
                "event_filter": s.event_filter,
                "created_at": s.created_at.isoformat() if s.created_at else None,
            }
            for s in subs
        ]


@app.delete("/api/protocol-subscriptions/{sub_id}", dependencies=[Depends(require_admin_key)])
def delete_protocol_subscription(sub_id: str) -> dict[str, str]:
    """Delete a ProtocolSubscription by id."""
    with SessionLocal() as session:
        sub = session.get(ProtocolSubscription, uuid.UUID(sub_id))
        if sub is None:
            raise HTTPException(status_code=404, detail="Subscription not found")
        session.delete(sub)
        session.commit()
        return {"status": "removed"}


@app.get("/api/protocols/{protocol_id}/events")
def list_protocol_events(protocol_id: int, limit: int = 50) -> list[dict[str, Any]]:
    """List MonitoredEvents for all contracts in a protocol."""
    with SessionLocal() as session:
        stmt = (
            select(MonitoredEvent, MonitoredContract)
            .join(MonitoredContract, MonitoredEvent.monitored_contract_id == MonitoredContract.id)
            .where(MonitoredContract.protocol_id == protocol_id)
            .order_by(MonitoredEvent.detected_at.desc())
            .limit(limit)
        )
        rows = session.execute(stmt).all()
        return [
            {
                "id": str(e.id),
                "monitored_contract_id": str(e.monitored_contract_id),
                "event_type": e.event_type,
                "block_number": e.block_number,
                "tx_hash": e.tx_hash,
                "data": {**(e.data or {}), "contract_address": mc.address},
                "detected_at": e.detected_at.isoformat() if e.detected_at else None,
            }
            for e, mc in rows
        ]


@app.get("/api/monitored-contracts")
def list_monitored_contracts(
    protocol_id: int | None = None,
    chain: str | None = None,
) -> list[dict[str, Any]]:
    """List all MonitoredContract rows, optionally filtered."""
    with SessionLocal() as session:
        stmt = select(MonitoredContract).order_by(MonitoredContract.created_at.desc())
        if protocol_id is not None:
            stmt = stmt.where(MonitoredContract.protocol_id == protocol_id)
        if chain is not None:
            stmt = stmt.where(MonitoredContract.chain == chain)
        contracts = session.execute(stmt).scalars().all()
        return [
            {
                "id": str(c.id),
                "address": c.address,
                "chain": c.chain,
                "protocol_id": c.protocol_id,
                "contract_type": c.contract_type,
                "monitoring_config": c.monitoring_config,
                "last_known_state": c.last_known_state,
                "last_scanned_block": c.last_scanned_block,
                "needs_polling": c.needs_polling,
                "is_active": c.is_active,
                "enrollment_source": c.enrollment_source,
                "created_at": c.created_at.isoformat() if c.created_at else None,
            }
            for c in contracts
        ]


class UpdateMonitoredContractRequest(BaseModel):
    monitoring_config: dict | None = Field(default=None, description="Updated monitoring config flags")
    is_active: bool | None = Field(default=None, description="Toggle monitoring on/off")
    needs_polling: bool | None = Field(default=None, description="Toggle storage-slot polling")


@app.patch("/api/monitored-contracts/{contract_id}", dependencies=[Depends(require_admin_key)])
def update_monitored_contract(contract_id: str, request: UpdateMonitoredContractRequest) -> dict[str, Any]:
    """Update monitoring_config, is_active, or needs_polling on a MonitoredContract."""
    with SessionLocal() as session:
        mc = session.get(MonitoredContract, uuid.UUID(contract_id))
        if mc is None:
            raise HTTPException(status_code=404, detail="MonitoredContract not found")

        if request.monitoring_config is not None:
            mc.monitoring_config = request.monitoring_config
        if request.is_active is not None:
            mc.is_active = request.is_active
        if request.needs_polling is not None:
            mc.needs_polling = request.needs_polling

        session.commit()
        session.refresh(mc)
        return {
            "id": str(mc.id),
            "address": mc.address,
            "chain": mc.chain,
            "protocol_id": mc.protocol_id,
            "contract_type": mc.contract_type,
            "monitoring_config": mc.monitoring_config,
            "last_known_state": mc.last_known_state,
            "last_scanned_block": mc.last_scanned_block,
            "needs_polling": mc.needs_polling,
            "is_active": mc.is_active,
            "enrollment_source": mc.enrollment_source,
            "created_at": mc.created_at.isoformat() if mc.created_at else None,
        }


@app.get("/api/monitored-events")
def list_monitored_events(
    contract_id: str | None = None,
    event_type: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """List all MonitoredEvent rows, optionally filtered."""
    with SessionLocal() as session:
        stmt = select(MonitoredEvent).order_by(MonitoredEvent.detected_at.desc()).limit(limit)
        if contract_id is not None:
            stmt = stmt.where(MonitoredEvent.monitored_contract_id == contract_id)
        if event_type is not None:
            stmt = stmt.where(MonitoredEvent.event_type == event_type)
        events = session.execute(stmt).scalars().all()
        return [
            {
                "id": str(e.id),
                "monitored_contract_id": str(e.monitored_contract_id),
                "event_type": e.event_type,
                "block_number": e.block_number,
                "tx_hash": e.tx_hash,
                "data": e.data,
                "detected_at": e.detected_at.isoformat() if e.detected_at else None,
            }
            for e in events
        ]


@app.get("/api/protocols/{protocol_id}/tvl")
def protocol_tvl(protocol_id: int, days: int = 30) -> dict[str, Any]:
    """Current TVL and historical snapshots for a protocol."""
    days = min(days, MAX_TVL_HISTORY_DAYS)

    with SessionLocal() as session:
        protocol = session.get(Protocol, protocol_id)
        if protocol is None:
            raise HTTPException(status_code=404, detail="Protocol not found")

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        stmt = (
            select(TvlSnapshot)
            .where(
                TvlSnapshot.protocol_id == protocol_id,
                TvlSnapshot.timestamp >= cutoff,
            )
            .order_by(TvlSnapshot.timestamp.desc())
        )
        snapshots = session.execute(stmt).scalars().all()

        latest = snapshots[0] if snapshots else None
        return {
            "protocol_id": protocol_id,
            "protocol_name": protocol.name,
            "current": {
                "total_usd": float(latest.total_usd) if latest and latest.total_usd else None,
                "defillama_tvl": float(latest.defillama_tvl) if latest and latest.defillama_tvl else None,
                "source": latest.source if latest else None,
                "timestamp": latest.timestamp.isoformat() if latest else None,
                "contract_breakdown": latest.contract_breakdown if latest else None,
                "chain_breakdown": latest.chain_breakdown if latest else None,
            },
            "history": [
                {
                    "timestamp": s.timestamp.isoformat(),
                    "total_usd": float(s.total_usd) if s.total_usd else None,
                    "defillama_tvl": float(s.defillama_tvl) if s.defillama_tvl else None,
                    "source": s.source,
                }
                for s in snapshots
            ],
        }


# ─── Address labels (admin-curated human names for arbitrary addresses) ────

_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def _normalize_address_or_400(address: str) -> str:
    a = (address or "").strip().lower()
    if not _ADDRESS_RE.match(a):
        raise HTTPException(status_code=400, detail="Invalid address format")
    return a


@app.get("/api/address_labels")
def list_address_labels() -> dict[str, Any]:
    """Return every stored address → name mapping as a flat dict.

    Public read endpoint so any page (principal detail, surface node, etc.)
    can decorate raw hex addresses with the admin-assigned name. The admin
    key is only required to mutate labels (PUT/DELETE below).
    """
    from db.models import AddressLabel

    with SessionLocal() as session:
        rows = session.execute(select(AddressLabel)).scalars().all()
        return {
            "labels": {
                row.address: {
                    "name": row.name,
                    "note": row.note,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                }
                for row in rows
            },
        }


class AddressLabelUpsert(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    note: str | None = Field(default=None, max_length=2000)


@app.put("/api/address_labels/{address}", dependencies=[Depends(require_admin_key)])
def upsert_address_label(address: str, payload: AddressLabelUpsert) -> dict[str, Any]:
    """Create or update the human-readable name for an address.

    Idempotent — repeated calls with the same body leave the row unchanged
    (aside from ``updated_at``). The frontend uses this to label Safe
    signers and EOA principals.
    """
    from db.models import AddressLabel

    a = _normalize_address_or_400(address)
    with SessionLocal() as session:
        row = session.get(AddressLabel, a)
        if row is None:
            row = AddressLabel(address=a, name=payload.name.strip(), note=payload.note)
            session.add(row)
        else:
            row.name = payload.name.strip()
            row.note = payload.note
        session.commit()
        return {
            "address": a,
            "name": row.name,
            "note": row.note,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }


@app.delete("/api/address_labels/{address}", dependencies=[Depends(require_admin_key)])
def delete_address_label(address: str) -> dict[str, Any]:
    from db.models import AddressLabel

    a = _normalize_address_or_400(address)
    with SessionLocal() as session:
        row = session.get(AddressLabel, a)
        if row is None:
            raise HTTPException(status_code=404, detail="Label not found")
        session.delete(row)
        session.commit()
        return {"address": a, "deleted": True}


class _ProbeMembershipRequest(BaseModel):
    function_signature: str = Field(..., description="Full signature, e.g. 'grantRole(bytes32,address)'")
    predicate_index: int = Field(..., ge=0, description="DFS-order leaf index in the function's predicate tree")
    member: str = Field(..., description="Address being tested for membership in the leaf's set")
    chain_id: int = Field(default=1, description="Chain id for repo lookups (defaults to ethereum mainnet)")
    block: int | None = Field(default=None, description="Optional block number for point-in-time probes")

    @field_validator("member")
    @classmethod
    def _check_member_address(cls, v: str) -> str:
        if not isinstance(v, str) or not v.startswith("0x") or len(v) != 42:
            raise ValueError("member must be a 0x-prefixed 20-byte address")
        return v.lower()


@app.post(
    "/api/contract/{address}/probe/membership",
    dependencies=[Depends(require_admin_key)],
)
def probe_contract_membership(address: str, req: _ProbeMembershipRequest) -> dict[str, Any]:
    """v2 schema probe: 'is ``member`` allowed by leaf
    ``predicate_index`` of ``function_signature`` on ``address``?'

    Resolves the predicate_trees artifact server-side from the most
    recent successful job for ``address``; the descriptor is NEVER
    client-supplied — clients only carry the leaf index they
    received from the v2 capability rendering.
    """
    addr = _normalize_address_or_400(address)

    # Lazy-import the resolver bits so the probe route doesn't
    # impose its dependency surface on the rest of the API.
    from services.resolution.adapters import AdapterRegistry, EvaluationContext
    from services.resolution.adapters.access_control import AccessControlAdapter
    from services.resolution.adapters.aragon_acl import (
        AragonACLAdapter,
        DSAuthAdapter,
        EIP1271Adapter,
    )
    from services.resolution.adapters.event_indexed import EventIndexedAdapter
    from services.resolution.adapters.safe import SafeAdapter
    from services.resolution.probe import probe_membership
    from services.resolution.repos import PostgresAragonACLRepo, PostgresRoleGrantsRepo

    with SessionLocal() as session:
        job = session.execute(
            select(Job)
            .where(Job.address == addr)
            .where(Job.status == JobStatus.completed)
            .order_by(Job.updated_at.desc(), Job.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if job is None:
            raise HTTPException(
                status_code=404, detail=f"No completed analysis job found for {addr}"
            )

        artifact = get_artifact(session, job.id, "predicate_trees")
        if artifact is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    "predicate_trees artifact missing for the latest analysis "
                    "(contract was analyzed before schema-v2 emit landed, or v2 emit failed)"
                ),
            )

        if not isinstance(artifact, dict) or "trees" not in artifact:
            # Either an error-path placeholder ({"error": "..."}) or
            # a malformed payload — surface the reason rather than
            # silently treating as no-tree.
            reason = (
                artifact.get("error")
                if isinstance(artifact, dict)
                else "predicate_trees payload was not a dict"
            )
            return {
                "result": "unknown",
                "reason": "predicate_trees_unavailable",
                "detail": reason,
            }

        tree = artifact["trees"].get(req.function_signature)
        if tree is None:
            # Resolver convention: absent function = unguarded
            # (publicly callable). For probe semantics, that means
            # anyone is in the set.
            return {
                "result": "yes",
                "reason": "function_unguarded",
                "function_signature": req.function_signature,
            }

        registry = AdapterRegistry()
        for cls in (
            AccessControlAdapter,
            SafeAdapter,
            AragonACLAdapter,
            DSAuthAdapter,
            EIP1271Adapter,
            EventIndexedAdapter,
        ):
            registry.register(cls)

        # Wire the Postgres-backed repos. AC adapter consumes
        # ``ctx.role_grants`` directly; Aragon adapter looks under
        # ``ctx.meta["aragon_acl_repo"]`` per its existing contract.
        # SafeRepo is RPC-backed (RpcSafeRepo) and would need an
        # rpc_url_for_chain map — not wired here yet because the
        # API process doesn't carry per-chain RPC configuration on
        # the request boundary.
        role_grants_repo = PostgresRoleGrantsRepo(session)
        aragon_repo = PostgresAragonACLRepo(session)
        ctx = EvaluationContext(
            chain_id=req.chain_id,
            contract_address=addr,
            block=req.block,
            role_grants=role_grants_repo,
            meta={"aragon_acl_repo": aragon_repo},
        )

        return probe_membership(
            tree,
            predicate_index=req.predicate_index,
            member=req.member,
            registry=registry,
            ctx=ctx,
        )


@app.get("/api/contract/{address}/capabilities")
def get_contract_capabilities(
    address: str,
    chain_id: int = 1,
    block: int | None = None,
) -> dict[str, Any]:
    """Return the v2 capability per externally-callable function on
    ``address``. Read path for the schema-v2 cutover (#18) — UI /
    external consumers query this and fall back to v1 endpoints
    when the response is 404 (legacy pre-v2 contract).

    Response shape:

        {
          "contract_address": "0x...",
          "chain_id": 1,
          "block": null,
          "capabilities": {
            "grantRole(bytes32,address)": {
              "kind": "finite_set",
              "members": ["0x..."],
              "membership_quality": "exact",
              "confidence": "enumerable",
              ...
            },
            ...
          }
        }

    Empty ``capabilities`` dict means every function on the
    contract is unguarded (publicly callable) per the resolver
    convention.

    Returns 404 if no completed analysis Job exists for the
    address, or no predicate_trees artifact has been written for
    the latest analysis (legacy pre-v2 contract).
    """
    from services.resolution.capability_resolver import resolve_contract_capabilities

    addr = _normalize_address_or_400(address)
    with SessionLocal() as session:
        capabilities = resolve_contract_capabilities(
            session, address=addr, chain_id=chain_id, block=block
        )
    if capabilities is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "No v2 capabilities for this address — either no completed "
                "analysis exists or it predates the schema-v2 emit. Fall "
                "back to /api/company/* or /api/jobs?address=..."
            ),
        )
    return {
        "contract_address": addr,
        "chain_id": chain_id,
        "block": block,
        "capabilities": capabilities,
    }


@app.get(
    "/api/contract/{address}/v1_v2_diff",
    dependencies=[Depends(require_admin_key)],
)
def get_contract_v1_v2_diff(address: str) -> dict[str, Any]:
    """Per-contract cutover-gate report. Loads BOTH the v1
    ``contract_analysis`` and v2 ``predicate_trees`` artifacts for
    the most recent completed analysis of ``address``, runs the
    diff harness, and returns a structured JSON report.

    Response shape:

        {
          "address": "0x...",
          "job_id": "<uuid>",
          "severity": "regression" | "new_coverage" | "role_drift" | "clean",
          "contract_name": "...",
          "agreed": [...],
          "v1_only": [...],
          "v2_only": [...],
          "role_disagreements": { fn: {v1_guard_kinds, v2_authority_roles} },
          "safe_to_cut_over": bool
        }

    Admin-gated because the diff exposes internal classifier
    detail not meant for external consumers. This is the human
    audit surface for #18.
    """
    from services.static.contract_analysis_pipeline.cutover_check import (
        cutover_check_for_address,
        is_safe_to_cut_over,
    )

    addr = _normalize_address_or_400(address)
    with SessionLocal() as session:
        report = cutover_check_for_address(session, address=addr)
    if report is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Cannot run cutover check for this address — either no "
                "completed analysis exists, the v1 contract_analysis "
                "artifact is missing, or the v2 predicate_trees artifact "
                "is missing (legacy pre-v2 contract). Re-analyze before "
                "evaluating."
            ),
        )
    return {**report, "safe_to_cut_over": is_safe_to_cut_over(report)}


@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    if full_path == "api" or full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")
    return _site_index_response()
