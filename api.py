#!/usr/bin/env python3
"""FastAPI server for launching and browsing PSAT analyses."""

from __future__ import annotations

import logging
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import select

from db.models import (
    Artifact,
    Contract,
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
    UpgradeEvent,
    WatchedProxy,
)
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
    dapp_urls: list[str] | None = None
    defillama_protocol: str | None = Field(default=None, min_length=1)
    name: str | None = None
    chain: str | None = None
    chain_id: int | None = Field(default=None, ge=1)
    wait: int | None = Field(default=None, ge=1, le=120)
    analyze_limit: int = Field(default=5, ge=1, le=200)
    rpc_url: str | None = None

    @model_validator(mode="after")
    def _validate_target(self) -> "AnalyzeRequest":
        targets = [self.address, self.company, self.dapp_urls, self.defillama_protocol]
        if sum(bool(t) for t in targets) != 1:
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


def _function_principal_payload(fp: FunctionPrincipal) -> dict[str, Any]:
    return {
        "address": fp.address,
        "resolved_type": fp.resolved_type,
        "source_controller_id": fp.origin,
        "details": fp.details or {},
    }


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

    return {
        "function": ef.abi_signature or ef.function_name,
        "selector": ef.selector,
        "effect_labels": list(ef.effect_labels or []),
        "effect_targets": list(ef.effect_targets or []),
        "action_summary": ef.action_summary,
        "authority_public": ef.authority_public,
        "controllers": list(controllers_by_label.values()),
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
def health() -> dict[str, str]:
    return {"status": "ok"}


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

        # Batch-fetch contract_flags to tag proxy jobs
        job_ids = [job.id for job in jobs]
        proxy_ids: set[str] = set()
        if job_ids:
            flags_stmt = select(Artifact.job_id, Artifact.data).where(
                Artifact.job_id.in_(job_ids), Artifact.name == "contract_flags"
            )
            for row in session.execute(flags_stmt):
                if isinstance(row.data, dict) and row.data.get("is_proxy"):
                    proxy_ids.add(str(row.job_id))

        result = []
        for job in jobs:
            d = job.to_dict()
            d["is_proxy"] = str(job.id) in proxy_ids
            result.append(d)
        return result


@app.post("/api/analyze")
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
                    contracts = inventory.get("contracts", [])
                    if not isinstance(contracts, list):
                        continue
                    for contract in contracts:
                        if not isinstance(contract, dict):
                            continue
                        addr = (contract.get("address") or "").lower()
                        if addr and "rank_score" in contract:
                            rank_scores[addr] = contract["rank_score"]
                        chains = contract.get("chains")
                        chain = contract.get("chain")
                        if addr and isinstance(chains, list) and chains:
                            chains_by_address[addr] = str(chains[0])
                        elif addr and chain:
                            chains_by_address[addr] = str(chain)

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
                "is_proxy": bool(flags.get("is_proxy")) if isinstance(flags, dict) else False,
                "proxy_type": flags.get("proxy_type") if isinstance(flags, dict) else None,
                "implementation_address": flags.get("implementation") if isinstance(flags, dict) else None,
                "proxy_address": request.get("proxy_address"),
            }
            # List available artifact names
            stmt_artifacts = select(Artifact.name).where(Artifact.job_id == job.id)
            artifact_names = [row for row in session.execute(stmt_artifacts).scalars().all()]
            entry["available_artifacts"] = sorted(artifact_names)

            # For proxy jobs, check if the impl child has completed.
            # If the impl is still running, hide the proxy — it'll appear
            # as a merged entry once the impl finishes.
            if isinstance(flags, dict) and flags.get("is_proxy") and flags.get("implementation"):
                impl_stmt = (
                    select(Job).where(Job.address == flags["implementation"]).order_by(Job.updated_at.desc()).limit(1)
                )
                impl_job = session.execute(impl_stmt).scalar_one_or_none()
                if impl_job and impl_job.status != JobStatus.completed:
                    continue
                if impl_job and not isinstance(analysis_artifact, dict):
                    analysis_artifact = get_artifact(session, impl_job.id, "contract_analysis")

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

        if isinstance(artifact, dict):
            from fastapi.responses import JSONResponse

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

        # Load from relational tables — fall back to address lookup when
        # copy_static_cache has reassigned the Contract row to a newer job.
        contract_row = session.execute(select(Contract).where(Contract.job_id == job.id).limit(1)).scalar_one_or_none()
        if contract_row is None and job.address:
            contract_row = session.execute(
                select(Contract).where(Contract.address == job.address.lower()).limit(1)
            ).scalar_one_or_none()

        payload: dict[str, Any] = {
            "run_name": job.name or str(job.id),
            "job_id": str(job.id),
            "address": job.address,
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
        ):
            if artifact_name in all_artifacts and isinstance(all_artifacts[artifact_name], dict):
                payload[artifact_name] = all_artifacts[artifact_name]

        # Build effective_permissions from relational tables
        if contract_row:
            ef_list = []
            for ef in (
                session.execute(select(EffectiveFunction).where(EffectiveFunction.contract_id == contract_row.id))
                .scalars()
                .all()
            ):
                direct_owner = None
                controller_principals = []
                for fp in (
                    session.execute(select(FunctionPrincipal).where(FunctionPrincipal.function_id == ef.id))
                    .scalars()
                    .all()
                ):
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
                for fallback_name in ("upgrade_history", "dependency_graph_viz", "dependencies"):
                    if fallback_name not in payload:
                        fallback = get_artifact(session, proxy_job.id, fallback_name)
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
                    "analysis_report",
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
                    # effective_permissions from impl
                    if "effective_permissions" not in payload:
                        impl_efs = (
                            session.execute(select(EffectiveFunction).where(EffectiveFunction.contract_id == impl_c.id))
                            .scalars()
                            .all()
                        )
                        if impl_efs:
                            ef_list = []
                            for ef in impl_efs:
                                direct_owner = None
                                controller_principals = []
                                for fp in (
                                    session.execute(
                                        select(FunctionPrincipal).where(FunctionPrincipal.function_id == ef.id)
                                    )
                                    .scalars()
                                    .all()
                                ):
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

        # Inline text artifacts
        if "analysis_report" in all_artifacts:
            payload["analysis_report"] = all_artifacts["analysis_report"]

        # Add subject info from contract_analysis if available
        if isinstance(all_artifacts.get("contract_analysis"), dict):
            subject = all_artifacts["contract_analysis"].get("subject", {})
            payload["contract_name"] = subject.get("name", payload["run_name"])
            payload["summary"] = all_artifacts["contract_analysis"].get("summary")

        return payload


@app.get("/api/company/{company_name}")
def company_overview(company_name: str) -> dict:
    """Aggregated governance overview for all contracts in a company."""
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
            contract_row = session.execute(
                select(Contract).where(Contract.job_id == job.id).limit(1)
            ).scalar_one_or_none()
            if contract_row is None and job.address:
                chain = request.get("chain")
                addr_stmt = select(Contract).where(
                    Contract.address == job.address.lower(),
                )
                if chain:
                    addr_stmt = addr_stmt.where(Contract.chain == chain)
                contract_row = session.execute(addr_stmt.limit(1)).scalar_one_or_none()

            is_proxy = contract_row.is_proxy if contract_row else False
            proxy_type = contract_row.proxy_type if contract_row else None
            impl_addr = contract_row.implementation if contract_row else None

            # For proxies, find impl job
            impl_job = None
            impl_job_id = None
            if impl_addr:
                impl_job = session.execute(
                    select(Job).where(Job.address == impl_addr, Job.status == JobStatus.completed).limit(1)
                ).scalar_one_or_none()
                if impl_job:
                    impl_job_id = str(impl_job.id)

            # Read summary from contract_summaries table (prefer impl's summary for proxies)
            summary_row = None
            if impl_job:
                impl_contract = session.execute(
                    select(Contract).where(Contract.job_id == impl_job.id).limit(1)
                ).scalar_one_or_none()
                if impl_contract:
                    summary_row = impl_contract.summary
            if not summary_row and contract_row:
                summary_row = contract_row.summary

            # Read controller values
            lookup_contract = contract_row
            if is_proxy and impl_job:
                impl_c = session.execute(
                    select(Contract).where(Contract.job_id == impl_job.id).limit(1)
                ).scalar_one_or_none()
                if (
                    impl_c
                    and session.execute(
                        select(ControllerValue).where(ControllerValue.contract_id == impl_c.id).limit(1)
                    ).scalar_one_or_none()
                ):
                    lookup_contract = impl_c

            owner = None
            controllers = {}
            if lookup_contract:
                for cv in (
                    session.execute(select(ControllerValue).where(ControllerValue.contract_id == lookup_contract.id))
                    .scalars()
                    .all()
                ):
                    controllers[cv.controller_id] = cv.value
                    if "owner" in cv.controller_id.lower() and cv.value and cv.value.startswith("0x"):
                        owner = cv.value.lower()

            # Get upgrade count
            upgrade_count = None
            if contract_row:
                ue_count = (
                    session.execute(select(UpgradeEvent).where(UpgradeEvent.contract_id == contract_row.id))
                    .scalars()
                    .all()
                )
                if ue_count:
                    upgrade_count = len(ue_count)

            # Read effect labels from effective_functions table
            ef_contract_id = None
            if impl_job:
                impl_c = session.execute(
                    select(Contract).where(Contract.job_id == impl_job.id).limit(1)
                ).scalar_one_or_none()
                if impl_c:
                    ef_contract_id = impl_c.id
            if not ef_contract_id and contract_row:
                ef_contract_id = contract_row.id

            value_effects: list[str] = []
            all_effects: set[str] = set()
            if ef_contract_id:
                for ef in (
                    session.execute(select(EffectiveFunction).where(EffectiveFunction.contract_id == ef_contract_id))
                    .scalars()
                    .all()
                ):
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
                impl_c = session.execute(
                    select(Contract).where(Contract.job_id == impl_job.id).limit(1)
                ).scalar_one_or_none()
                if impl_c and impl_c.contract_name:
                    contract_name = impl_c.contract_name
                elif impl_job.name:
                    contract_name = impl_job.name
            if not contract_name:
                contract_name = job.name or (contract_row.contract_name if contract_row else None) or ""
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

            # Build functions list from effective_functions table
            functions_list = []
            if ef_contract_id:
                for ef in (
                    session.execute(select(EffectiveFunction).where(EffectiveFunction.contract_id == ef_contract_id))
                    .scalars()
                    .all()
                ):
                    function_principals = (
                        session.execute(select(FunctionPrincipal).where(FunctionPrincipal.function_id == ef.id))
                        .scalars()
                        .all()
                    )
                    functions_list.append(_build_company_function_entry(ef, list(function_principals)))

            # Fetch balances
            from db.models import ContractBalance

            balance_contract = lookup_contract or contract_row
            balances_list = []
            total_usd = 0.0
            if balance_contract:
                for b in (
                    session.execute(select(ContractBalance).where(ContractBalance.contract_id == balance_contract.id))
                    .scalars()
                    .all()
                ):
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

            # Resolved control graph: find company contracts that are principals of this one
            lookup_job_id = c.get("impl_job_id") or c["job_id"]
            lookup_c = session.execute(
                select(Contract).where(Contract.job_id == lookup_job_id).limit(1)
            ).scalar_one_or_none()
            if lookup_c:
                for cgn in (
                    session.execute(select(ControlGraphNode).where(ControlGraphNode.contract_id == lookup_c.id))
                    .scalars()
                    .all()
                ):
                    node_addr = (cgn.address or "").lower()
                    if node_addr and node_addr in contract_addrs and node_addr != target:
                        add_flow(node_addr, target, "principal")

        # No transitive dedup — keep all direct principal edges as-is.

        # Collect non-contract principals from control graph.
        # Only show direct controllers (Safes, timelocks, EOAs that control contracts).
        # Safe owners are nested inside their Safe, not shown as standalone nodes.
        from db.models import ControlGraphEdge

        principal_map: dict[str, dict[str, Any]] = {}
        # First pass: find all safe_owner edges to build Safe→owners mapping
        safe_owners_map: dict[str, list[str]] = {}  # safe_addr → [owner_addrs]
        owner_of_safe: set[str] = set()  # addresses that are Safe owners

        for c in contracts:
            if not c["address"]:
                continue
            lookup_job_id = c.get("impl_job_id") or c["job_id"]
            lookup_c = session.execute(
                select(Contract).where(Contract.job_id == lookup_job_id).limit(1)
            ).scalar_one_or_none()
            if not lookup_c:
                continue
            for edge in (
                session.execute(
                    select(ControlGraphEdge).where(
                        ControlGraphEdge.contract_id == lookup_c.id,
                        ControlGraphEdge.relation == "safe_owner",
                    )
                )
                .scalars()
                .all()
            ):
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
            lookup_job_id = c.get("impl_job_id") or c["job_id"]
            lookup_c = session.execute(
                select(Contract).where(Contract.job_id == lookup_job_id).limit(1)
            ).scalar_one_or_none()
            if not lookup_c:
                continue

            for cgn in (
                session.execute(select(ControlGraphNode).where(ControlGraphNode.contract_id == lookup_c.id))
                .scalars()
                .all()
            ):
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
                    # Get details from controller_values
                    details: dict[str, Any] = {}
                    for cv in (
                        session.execute(
                            select(ControllerValue).where(
                                ControllerValue.contract_id == lookup_c.id,
                                ControllerValue.value == node_addr,
                            )
                        )
                        .scalars()
                        .all()
                    ):
                        if cv.details and isinstance(cv.details, dict):
                            details.update(cv.details)

                    # For Safes, attach owners and threshold
                    if cgn.resolved_type == "safe":
                        owners = safe_owners_map.get(node_addr, [])
                        details["owners"] = owners
                        # Try to get threshold from details
                        if "threshold" not in details and owners:
                            details["threshold"] = len(owners)  # fallback

                    principal_map[node_addr] = {
                        "address": node_addr,
                        "type": cgn.resolved_type,
                        "label": cgn.contract_name or cgn.label or cgn.resolved_type,
                        "details": details,
                        "controls": [],
                    }

                principal_map[node_addr]["controls"].append(target)
                add_flow(node_addr, target, "principal")

        principals = list(principal_map.values())

        # Build all_addresses from contracts table (includes discovered + analyzed)
        if protocol_row:
            all_contract_rows = (
                session.execute(select(Contract).where(Contract.protocol_id == protocol_row.id)).scalars().all()
            )
        else:
            # Fallback: collect from company_jobs
            all_contract_rows = []
            for j in company_jobs:
                cr = session.execute(select(Contract).where(Contract.job_id == j.id).limit(1)).scalar_one_or_none()
                if cr:
                    all_contract_rows.append(cr)

        all_addresses = sorted(
            [
                {
                    "address": cr.address,
                    "name": cr.contract_name,
                    "source_verified": cr.source_verified,
                    "is_proxy": cr.is_proxy,
                    "analyzed": cr.job_id is not None,
                    "discovery_source": cr.discovery_source,
                    "chain": cr.chain,
                }
                for cr in all_contract_rows
            ],
            key=lambda x: (not x["analyzed"], x["name"] or "zzz"),
        )

        return {
            "company": company_name,
            "protocol_id": protocol_row.id if protocol_row else None,
            "contract_count": len(contracts),
            "contracts": contracts,
            "principals": principals,
            "ownership_hierarchy": hierarchy,
            "fund_flows": fund_flows,
            "all_addresses": all_addresses,
        }


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


@app.post("/api/watched-proxies")
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


@app.delete("/api/watched-proxies/{proxy_id}")
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


@app.post("/api/watched-proxies/{proxy_id}/subscriptions")
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


@app.delete("/api/subscriptions/{subscription_id}")
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


@app.post("/api/protocols/{protocol_id}/re-enroll")
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


@app.post("/api/protocols/{protocol_id}/subscribe")
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


@app.delete("/api/protocol-subscriptions/{sub_id}")
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


@app.patch("/api/monitored-contracts/{contract_id}")
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


@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    if full_path == "api" or full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")
    return _site_index_response()
