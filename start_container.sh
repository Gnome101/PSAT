#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "[entrypoint] Running Alembic migrations (idempotent)..."
# Hard ceiling so a stuck schema migration can't keep uvicorn from
# binding port 8000; Fly restarts the machine and retries.
timeout 60s uv run --no-sync alembic upgrade head

echo "[entrypoint] Starting background workers..."
./start_workers.sh &

echo "[entrypoint] Starting API on 0.0.0.0:8000..."
# --limit-concurrency gives the event loop backpressure before Fly's
# hard_limit=100 piles connections on us. No --workers: uvicorn's
# preforking shares the SQLAlchemy engine across children, and
# psycopg2 sockets are not fork-safe — children crash on first DB
# access.
exec uv run --no-sync uvicorn api:app --host 0.0.0.0 --port 8000 \
    --limit-concurrency 200
