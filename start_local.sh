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

WORKER_PATTERN='workers\.(discovery|static_worker|resolution_worker|policy_worker|proxy_monitor)'
API_PID=""
WORKERS_PID=""
PROXY_SCANNER_PID=""
PROXY_POLLER_PID=""

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

# Run migrations
cleanup_stale_workers

echo "Running migrations..."
uv run alembic upgrade head

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
  echo "Done."
}
trap cleanup EXIT INT TERM

# Start API
echo "Starting API on http://127.0.0.1:8000 ..."
uv run uvicorn api:app --host 127.0.0.1 --port 8000 &
API_PID=$!
sleep 2

# Start workers
echo "Starting workers..."
bash start_workers.sh &
WORKERS_PID=$!

# Start proxy monitor (event scanner + storage poller)
echo "Starting proxy monitor..."
uv run python -m workers.proxy_monitor &
PROXY_SCANNER_PID=$!
uv run python -m workers.proxy_monitor --poll &
PROXY_POLLER_PID=$!

echo ""
echo "=== PSAT running ==="
echo "  API:     http://127.0.0.1:8000"
echo "  Health:  http://127.0.0.1:8000/api/health"
echo ""
echo "Press Ctrl+C to stop."
wait
