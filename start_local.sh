#!/bin/bash
set -e

cd "$(dirname "$0")"

# Load .env
if [ ! -f .env ]; then
  echo "ERROR: .env file not found. Copy .env.example to .env and fill in values."
  exit 1
fi
set -a
source .env
set +a

WORKER_PATTERN='workers\.(discovery|static_worker|resolution_worker|policy_worker|dapp_crawl_worker|defillama_worker)'
API_PID=""
WORKERS_PID=""
PROXY_SCANNER_PID=""
PROXY_POLLER_PID=""
TVL_TRACKER_PID=""

# Check required env vars
missing=()
[ -z "$DATABASE_URL" ]      && missing+=("DATABASE_URL")
[ -z "$ETHERSCAN_API_KEY" ] && missing+=("ETHERSCAN_API_KEY")
[ -z "$ETH_RPC" ]           && missing+=("ETH_RPC")
[ -z "$ENVIO_API_TOKEN" ]   && missing+=("ENVIO_API_TOKEN")
[ -z "$TAVILY_API_KEY" ]    && missing+=("TAVILY_API_KEY")
[ -z "$NVIDIA_API_KEY" ]    && missing+=("NVIDIA_API_KEY")

if [ ${#missing[@]} -gt 0 ]; then
  echo "ERROR: Missing required environment variables in .env:"
  for var in "${missing[@]}"; do
    echo "  - $var"
  done
  exit 1
fi

# Start postgres if not running
if ! docker compose ps postgres --status running -q 2>/dev/null | grep -q .; then
  echo "Starting postgres..."
  docker compose up postgres -d
  echo "Waiting for postgres to be healthy..."
  until docker compose ps postgres --status running -q 2>/dev/null | grep -q .; do
    sleep 1
  done
  sleep 3
fi

# Start minio (object storage) if not running.
# With ARTIFACT_STORAGE_* set, artifact bodies live in minio; without them,
# the app falls back to inline Postgres storage.
if ! docker compose ps minio --status running -q 2>/dev/null | grep -q .; then
  echo "Starting minio..."
  docker compose up minio minio-init -d
  echo "Waiting for minio to be healthy..."
  until docker compose ps minio --status running -q 2>/dev/null | grep -q .; do
    sleep 1
  done
  # minio-init creates the bucket then exits 0 — wait for that to complete.
  # Note: `docker compose ps` hides stopped containers by default, so pass -a.
  until [ "$(docker compose ps -a minio-init --format '{{.State}}' 2>/dev/null)" = "exited" ]; do
    sleep 1
  done
fi

# Report which artifact storage mode the app will use on boot.
if [ -n "$ARTIFACT_STORAGE_ENDPOINT" ] && [ -n "$ARTIFACT_STORAGE_BUCKET" ] \
   && [ -n "$ARTIFACT_STORAGE_ACCESS_KEY" ] && [ -n "$ARTIFACT_STORAGE_SECRET_KEY" ]; then
  echo "Artifact storage: minio ($ARTIFACT_STORAGE_ENDPOINT → $ARTIFACT_STORAGE_BUCKET)"
  echo "  Console: http://localhost:9001 (login: $ARTIFACT_STORAGE_ACCESS_KEY)"
else
  echo "WARNING: ARTIFACT_STORAGE_* not fully set in .env — app will use inline Postgres fallback."
  echo "  To use minio, add to .env:"
  echo "    ARTIFACT_STORAGE_ENDPOINT=http://localhost:9000"
  echo "    ARTIFACT_STORAGE_BUCKET=psat-artifacts"
  echo "    ARTIFACT_STORAGE_ACCESS_KEY=psat-minio"
  echo "    ARTIFACT_STORAGE_SECRET_KEY=psat-minio-secret"
fi

cleanup_stale_workers() {
  local stale
  stale=$(pgrep -af "$WORKER_PATTERN" || true)
  if [ -z "$stale" ]; then
    return
  fi

  echo "Stopping stale workers..."
  echo "$stale"
  pkill -f "$WORKER_PATTERN" 2>/dev/null || true

  for _ in $(seq 1 20); do
    if ! pgrep -f "$WORKER_PATTERN" >/dev/null 2>&1; then
      echo "Stale workers stopped."
      return
    fi
    sleep 0.5
  done

  echo "ERROR: Failed to stop stale workers."
  pgrep -af "$WORKER_PATTERN" || true
  exit 1
}

cleanup_stale_workers

echo "Initializing database tables..."
uv run python3 -c "from db.models import create_tables; create_tables(); print('Tables ready.')"

# Ensure Playwright browsers are installed (needed by dapp_crawl_worker)
if ! [ -d "$HOME/.cache/ms-playwright/chromium_headless_shell-1208" ]; then
  echo "Installing Playwright browsers..."
  uv run playwright install chromium
fi

# Trap to clean up background processes on exit
cleanup() {
  echo ""
  echo "Shutting down..."
  if [ -n "$API_PID" ]; then
    kill "$API_PID" 2>/dev/null || true
    wait "$API_PID" 2>/dev/null || true
  fi
  if [ -n "$WORKERS_PID" ]; then
    kill "$WORKERS_PID" 2>/dev/null || true
    wait "$WORKERS_PID" 2>/dev/null || true
  fi
  if [ -n "$PROXY_SCANNER_PID" ]; then
    kill "$PROXY_SCANNER_PID" 2>/dev/null || true
    wait "$PROXY_SCANNER_PID" 2>/dev/null || true
  fi
  if [ -n "$PROXY_POLLER_PID" ]; then
    kill "$PROXY_POLLER_PID" 2>/dev/null || true
    wait "$PROXY_POLLER_PID" 2>/dev/null || true
  fi
  if [ -n "$TVL_TRACKER_PID" ]; then
    kill "$TVL_TRACKER_PID" 2>/dev/null || true
    wait "$TVL_TRACKER_PID" 2>/dev/null || true
  fi
  echo "Done."
}
trap cleanup EXIT INT TERM

# Start API
echo "Starting API on http://127.0.0.1:8000 ..."
uv run uvicorn api:app --host 127.0.0.1 --port 8000 --reload &
API_PID=$!
sleep 2

# Start workers
echo "Starting workers..."
bash start_workers.sh &
WORKERS_PID=$!

# Start protocol monitor (unified event scanner + storage poller)
echo "Starting protocol monitor..."
uv run python -m workers.protocol_monitor &
PROXY_SCANNER_PID=$!
uv run python -m workers.protocol_monitor --poll &
PROXY_POLLER_PID=$!
uv run python -m workers.protocol_monitor --tvl &
TVL_TRACKER_PID=$!

echo ""
echo "=== PSAT running ==="
echo "  API:     http://127.0.0.1:8000"
echo "  Health:  http://127.0.0.1:8000/api/health"
echo ""
echo "Press Ctrl+C to stop."
wait
