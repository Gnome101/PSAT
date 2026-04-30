#!/usr/bin/env python3
"""FastAPI application: middleware, lifespan, and router registration.

The endpoint handlers live in ``routers/*``; aggregation logic lives in
``services/aggregations/*``. This file's only job is to wire them together.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from sqlalchemy import select

from routers import (
    address_labels,
    agent,
    analyses,
    audits,
    company,
    jobs,
    meta,
    monitored,
    protocols,
    spa,
    watched_proxies,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Verify DB is reachable on startup."""
    try:
        # Local import dodges a circular at module load (db.models indirectly
        # imports modules that read api during eager evaluation in some envs).
        from db.models import engine

        with engine.connect() as conn:
            conn.execute(select(1))
        logger.info("Database connection verified")
    except Exception:
        logger.warning("Database not reachable at startup - endpoints will fail until DB is available")
    yield


_raw_origins = os.environ.get("PSAT_SITE_ORIGIN", "")
ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]
if not ALLOWED_ORIGINS:
    logger.warning(
        "PSAT_SITE_ORIGIN is not set - CORS will deny all cross-origin requests. "
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

spa.mount_static_assets(app)

app.include_router(meta.router)
app.include_router(jobs.router)
app.include_router(analyses.router)
app.include_router(company.router)
app.include_router(audits.router)
app.include_router(watched_proxies.router)
app.include_router(protocols.router)
app.include_router(monitored.router)
app.include_router(address_labels.router)
app.include_router(agent.router)
# SPA catch-all MUST be last - its /{full_path:path} would otherwise
# swallow any /api/* route registered after it.
app.include_router(spa.router)
