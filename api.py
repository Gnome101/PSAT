#!/usr/bin/env python3
"""FastAPI server for launching and browsing PSAT analyses."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select

from db.models import Artifact, Job, JobStatus, SessionLocal
from db.queue import create_job, get_all_artifacts, get_artifact

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent
SITE_DIR = ROOT_DIR / "site"
SITE_DIST_DIR = SITE_DIR / "dist"
SITE_ASSETS_DIR = SITE_DIST_DIR / "assets"

DEFAULT_RPC_URL = os.environ.get("ETH_RPC", "https://ethereum-rpc.publicnode.com")
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
    name: str | None = None
    chain: str | None = None
    discover_limit: int = Field(default=25, ge=1, le=200)
    analyze_limit: int = Field(default=5, ge=1, le=200)
    rpc_url: str | None = None

    @model_validator(mode="after")
    def _validate_target(self) -> "AnalyzeRequest":
        if bool(self.address) == bool(self.company):
            raise ValueError("Provide exactly one of address or company")
        return self


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


app = FastAPI(title="PSAT Demo", version="0.1.0", lifespan=lifespan)
if SITE_ASSETS_DIR.exists():
    app.mount("/assets", StaticFiles(directory=SITE_ASSETS_DIR), name="assets")


def _site_index_response():
    dist_index = SITE_DIST_DIR / "index.html"
    source_index = SITE_DIR / "index.html"
    if dist_index.exists():
        return FileResponse(dist_index)
    if source_index.exists():
        return FileResponse(source_index)
    return PlainTextResponse(
        "Frontend build not found. Run `cd site && npm run build` or start the "
        "Vite dev server with `cd site && npm run dev`.",
        status_code=503,
    )


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
                    "display_name": impl.get("contract_name") or impl.get("run_name") or _display_name(entry),
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
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/config")
def config() -> dict[str, str]:
    return {"default_rpc_url": DEFAULT_RPC_URL}


@app.get("/api/jobs")
def list_jobs() -> list[dict[str, Any]]:
    with SessionLocal() as session:
        stmt = select(Job).order_by(Job.created_at.desc())
        jobs = session.execute(stmt).scalars().all()
        return [job.to_dict() for job in jobs]


@app.post("/api/analyze")
def analyze_address(request: AnalyzeRequest) -> dict[str, Any]:
    if request.address and not request.address.startswith("0x"):
        raise HTTPException(status_code=400, detail="Address must start with 0x")
    with SessionLocal() as session:
        job = create_job(session, request.model_dump())
        return job.to_dict()


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    with SessionLocal() as session:
        job = session.get(Job, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job.to_dict()


@app.get("/api/analyses")
def analyses() -> list[dict]:
    """List completed analyses with their available artifacts."""
    with SessionLocal() as session:
        stmt = select(Job).where(Job.status == JobStatus.completed).order_by(Job.updated_at.desc())
        jobs = session.execute(stmt).scalars().all()

        jobs_by_id = {str(job.id): job for job in jobs}

        # Build lookups for company names and inventory rank scores
        rank_scores: dict[str, float] = {}  # address -> rank_score
        chains_by_address: dict[str, str] = {}  # address -> chain
        for job in jobs:
            if job.company:
                inventory = get_artifact(session, job.id, "contract_inventory")
                if isinstance(inventory, dict):
                    for contract in inventory.get("contracts", []):
                        addr = (contract.get("address") or "").lower()
                        if addr and "rank_score" in contract:
                            rank_scores[addr] = contract["rank_score"]
                        if addr and contract.get("chain"):
                            chains_by_address[addr] = str(contract["chain"])

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
            analysis_artifact = get_artifact(session, job.id, "contract_analysis")
            flags = get_artifact(session, job.id, "contract_flags")
            request = job.request if isinstance(job.request, dict) else {}
            parent_job_id = request.get("parent_job_id")
            company = company_for_job(job)
            addr_lower = (job.address or "").lower()
            entry: dict[str, Any] = {
                "run_name": run_name,
                "job_id": str(job.id),
                "address": job.address,
                "chain": request.get("chain") or chains_by_address.get(addr_lower),
                "company": company,
                "parent_job_id": parent_job_id,
                "rank_score": rank_scores.get(addr_lower),
                "is_proxy": bool(flags and flags.get("is_proxy")),
                "proxy_type": (flags or {}).get("proxy_type"),
                "implementation_address": (flags or {}).get("implementation"),
                "proxy_address": request.get("proxy_address"),
            }
            # List available artifact names
            stmt_artifacts = select(Artifact.name).where(Artifact.job_id == job.id)
            artifact_names = [row for row in session.execute(stmt_artifacts).scalars().all()]
            entry["available_artifacts"] = sorted(artifact_names)

            if isinstance(analysis_artifact, dict):
                subject = analysis_artifact.get("subject", {})
                entry["contract_name"] = subject.get("name", run_name)
                entry["summary"] = analysis_artifact.get("summary")
            results.append(entry)
        return _merge_proxy_impl_entries(results)


@app.get("/api/analyses/{run_name:path}/artifact/{artifact_name:path}")
def analysis_artifact(run_name: str, artifact_name: str):
    """Get a specific artifact for an analysis."""
    with SessionLocal() as session:
        # Find job by name or id
        stmt = select(Job).where(Job.name == run_name).order_by(Job.updated_at.desc()).limit(1)
        job = session.execute(stmt).scalar_one_or_none()
        if job is None:
            try:
                job = session.get(Job, run_name)
            except Exception:
                pass
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

        if isinstance(artifact, dict):
            from fastapi.responses import JSONResponse

            return JSONResponse(content=artifact)
        return PlainTextResponse(str(artifact))


@app.get("/api/analyses/{run_name:path}")
def analysis_detail(run_name: str) -> dict:
    """Get analysis detail by job name (run_name) or job_id."""
    with SessionLocal() as session:
        # Try by name first, then by id
        stmt = select(Job).where(Job.name == run_name).order_by(Job.updated_at.desc()).limit(1)
        job = session.execute(stmt).scalar_one_or_none()
        if job is None:
            try:
                job = session.get(Job, run_name)
            except Exception:
                pass
        if job is None:
            raise HTTPException(status_code=404, detail="Analysis not found")

        all_artifacts = get_all_artifacts(session, job.id)
        payload: dict[str, Any] = {
            "run_name": job.name or str(job.id),
            "job_id": str(job.id),
            "address": job.address,
            "available_artifacts": sorted(all_artifacts.keys()),
        }
        # Inline JSON artifacts directly
        for artifact_name in (
            "contract_analysis",
            "control_snapshot",
            "dependencies",
            "resolved_control_graph",
            "effective_permissions",
            "principal_labels",
            "dependency_graph_viz",
        ):
            if artifact_name in all_artifacts and isinstance(all_artifacts[artifact_name], dict):
                payload[artifact_name] = all_artifacts[artifact_name]

        # For impl jobs, inherit dependency viz from the proxy job if missing
        request = job.request if isinstance(job.request, dict) else {}
        proxy_address = request.get("proxy_address")
        if proxy_address and "dependency_graph_viz" not in payload:
            proxy_stmt = select(Job).where(Job.address == proxy_address).order_by(Job.updated_at.desc()).limit(1)
            proxy_job = session.execute(proxy_stmt).scalar_one_or_none()
            if proxy_job:
                for fallback_name in ("dependency_graph_viz", "dependencies"):
                    if fallback_name not in payload:
                        fallback = get_artifact(session, proxy_job.id, fallback_name)
                        if isinstance(fallback, dict):
                            payload[fallback_name] = fallback

        payload["proxy_address"] = proxy_address

        # Inline text artifacts
        if "analysis_report" in all_artifacts:
            payload["analysis_report"] = all_artifacts["analysis_report"]

        # Add subject info from contract_analysis if available
        if isinstance(all_artifacts.get("contract_analysis"), dict):
            subject = all_artifacts["contract_analysis"].get("subject", {})
            payload["contract_name"] = subject.get("name", payload["run_name"])
            payload["summary"] = all_artifacts["contract_analysis"].get("summary")

        return payload


@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    if full_path == "api" or full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")
    return _site_index_response()
