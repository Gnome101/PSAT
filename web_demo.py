#!/usr/bin/env python3
"""FastAPI demo server for launching and browsing PSAT analyses."""

from __future__ import annotations

import threading
import traceback
import uuid
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from services.demo_runner import DEFAULT_DEMO_RPC_URL, artifact_path, list_analyses, read_analysis, run_demo_analysis

ROOT_DIR = Path(__file__).resolve().parent
SITE_DIR = ROOT_DIR / "site"
SITE_DIST_DIR = SITE_DIR / "dist"
SITE_ASSETS_DIR = SITE_DIST_DIR / "assets"


class AnalyzeRequest(BaseModel):
    address: str = Field(min_length=42, max_length=42)
    name: str | None = None
    rpc_url: str | None = None


JOBS: dict[str, dict[str, Any]] = {}
JOB_LOCK = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _update_job(job_id: str, **updates: Any) -> None:
    with JOB_LOCK:
        JOBS[job_id].update(updates)
        JOBS[job_id]["updated_at"] = _now_iso()


def _run_job(job_id: str, request: AnalyzeRequest) -> None:
    try:
        _update_job(job_id, status="running", stage="starting", detail="Preparing analysis")

        def progress(stage: str, detail: str) -> None:
            _update_job(job_id, stage=stage, detail=detail)

        result = run_demo_analysis(
            request.address,
            name=request.name,
            rpc_url=request.rpc_url or DEFAULT_DEMO_RPC_URL,
            progress=progress,
        )
        _update_job(
            job_id,
            status="completed",
            stage="completed",
            detail="Analysis complete",
            result=result,
            run_name=result["run_name"],
        )
    except Exception as exc:  # pragma: no cover - exercised via API tests with monkeypatch
        _update_job(
            job_id,
            status="failed",
            stage="failed",
            detail=str(exc),
            error=traceback.format_exc(),
        )


def start_demo_job(request: AnalyzeRequest) -> dict[str, Any]:
    job_id = uuid.uuid4().hex
    with JOB_LOCK:
        JOBS[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "stage": "queued",
            "detail": "Queued for analysis",
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "request": request.model_dump(),
            "result": None,
            "error": None,
            "run_name": None,
        }

    thread = threading.Thread(target=_run_job, args=(job_id, request), daemon=True)
    thread.start()
    return JOBS[job_id]


app = FastAPI(title="PSAT Demo", version="0.1.0")
if SITE_ASSETS_DIR.exists():
    app.mount("/assets", StaticFiles(directory=SITE_ASSETS_DIR), name="assets")


def _site_index_response():
    if (SITE_DIST_DIR / "index.html").exists():
        return FileResponse(SITE_DIST_DIR / "index.html")
    return PlainTextResponse(
        "Frontend build not found. Run `cd site && npm run build` or start the Vite dev server with `cd site && npm run dev`.",
        status_code=503,
    )


@app.get("/")
def index():
    return _site_index_response()


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/config")
def config() -> dict[str, str]:
    return {"default_rpc_url": DEFAULT_DEMO_RPC_URL}


@app.get("/api/jobs")
def list_jobs() -> list[dict[str, Any]]:
    with JOB_LOCK:
        return sorted(JOBS.values(), key=lambda item: item["created_at"], reverse=True)


@app.post("/api/analyze")
def analyze_address(request: AnalyzeRequest) -> dict[str, Any]:
    if not request.address.startswith("0x"):
        raise HTTPException(status_code=400, detail="Address must start with 0x")
    job = start_demo_job(request)
    return job


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    with JOB_LOCK:
        job = JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/api/analyses")
def analyses() -> list[dict]:
    return list_analyses()


@app.get("/api/analyses/{run_name}")
def analysis_detail(run_name: str) -> dict:
    try:
        return read_analysis(run_name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Analysis not found") from None


@app.get("/api/analyses/{run_name}/artifact/{artifact_name:path}")
def analysis_artifact(run_name: str, artifact_name: str):
    try:
        path = artifact_path(run_name, artifact_name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Artifact not found") from None

    if path.suffix == ".json":
        return JSONResponse(content=read_analysis(run_name).get(path.stem) or json.loads(path.read_text()))
    if path.suffix == ".txt" or path.suffix == ".jsonl":
        return PlainTextResponse(path.read_text())
    return FileResponse(path)


@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    if full_path == "api" or full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")
    return _site_index_response()
