#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "[entrypoint] Initializing database schema (idempotent)..."
# Hard ceiling so a stuck schema migration can't keep uvicorn from
# binding port 8000; Fly restarts the machine and retries.
timeout 60s uv run --no-sync python -c "from db.models import create_tables; create_tables()"

echo "[entrypoint] Starting background workers..."
./start_workers.sh &

echo "[entrypoint] Starting API on 0.0.0.0:8000..."
exec uv run --no-sync uvicorn api:app --host 0.0.0.0 --port 8000 \
    --workers 2 --limit-concurrency 200
