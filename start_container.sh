#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "[entrypoint] Initializing database schema (idempotent)..."
uv run --no-sync python -c "from db.models import create_tables; create_tables()"

echo "[entrypoint] Starting background workers..."
./start_workers.sh &

echo "[entrypoint] Starting API on 0.0.0.0:8000..."
exec uv run --no-sync uvicorn api:app --host 0.0.0.0 --port 8000
