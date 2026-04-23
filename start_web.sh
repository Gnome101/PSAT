#!/bin/bash
# `web` process group: FastAPI only.
set -e

cd "$(dirname "$0")"

# --limit-concurrency gives the event loop backpressure before Fly's
# hard_limit=100 piles connections on us. No --workers: uvicorn's
# preforking shares the SQLAlchemy engine across children, and
# psycopg2 sockets are not fork-safe — children crash on first DB
# access.
#
# --loop uvloop: Cython event loop, 2-4x faster than stdlib asyncio on
# the I/O-bound paths (DB query → JSON serialize → socket write) where
# per-call overhead dominates the per-request time.
#
# --http httptools: HTTP parser in C; ~15-25% less CPU per request vs
# uvicorn's pure-Python h11 fallback.
#
# --log-level warning: uvicorn's INFO access log is redundant with our
# structured api-logger output and measurable on hot paths.
exec uv run --no-sync uvicorn api:app --host 0.0.0.0 --port 8000 \
    --limit-concurrency 200 \
    --loop uvloop \
    --http httptools \
    --log-level warning
