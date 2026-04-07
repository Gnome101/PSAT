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
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select

from db.models import Artifact, Job, JobStatus, ProxySubscription, ProxyUpgradeEvent, SessionLocal, WatchedProxy
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
            "upgrade_history",
        ):
            if artifact_name in all_artifacts and isinstance(all_artifacts[artifact_name], dict):
                payload[artifact_name] = all_artifacts[artifact_name]

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

        # For proxy jobs, inherit analysis artifacts from the impl child job
        flags = get_artifact(session, job.id, "contract_flags")
        if isinstance(flags, dict) and flags.get("is_proxy") and flags.get("implementation"):
            impl_address = flags["implementation"]
            impl_stmt = select(Job).where(Job.address == impl_address).order_by(Job.updated_at.desc()).limit(1)
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
                # Use the impl's subject info if the proxy doesn't have its own
                if "contract_name" not in payload and isinstance(impl_artifacts.get("contract_analysis"), dict):
                    subject = impl_artifacts["contract_analysis"].get("subject", {})
                    payload["contract_name"] = subject.get("name", payload["run_name"])
                    payload["summary"] = impl_artifacts["contract_analysis"].get("summary")
                payload["proxy_address"] = payload.get("proxy_address") or job.address
                payload["implementation_address"] = impl_address

        # Inline text artifacts
        if "analysis_report" in all_artifacts:
            payload["analysis_report"] = all_artifacts["analysis_report"]

        # Add subject info from contract_analysis if available
        if isinstance(all_artifacts.get("contract_analysis"), dict):
            subject = all_artifacts["contract_analysis"].get("subject", {})
            payload["contract_name"] = subject.get("name", payload["run_name"])
            payload["summary"] = all_artifacts["contract_analysis"].get("summary")

        return payload


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


@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    if full_path == "api" or full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")
    return _site_index_response()
