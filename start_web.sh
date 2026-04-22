#!/bin/bash
# `web` process group: FastAPI only.
set -e

cd "$(dirname "$0")"

# --limit-concurrency gives the event loop backpressure before Fly's
# hard_limit=100 piles connections on us. No --workers: uvicorn's
# preforking shares the SQLAlchemy engine across children, and
# psycopg2 sockets are not fork-safe — children crash on first DB
# access.
exec uv run --no-sync uvicorn api:app --host 0.0.0.0 --port 8000 \
    --limit-concurrency 200
