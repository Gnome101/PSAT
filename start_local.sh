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

# Run migrations
echo "Running migrations..."
uv run alembic upgrade head

# Trap to clean up background processes on exit
cleanup() {
  echo ""
  echo "Shutting down..."
  kill $API_PID $WORKERS_PID 2>/dev/null
  wait $API_PID $WORKERS_PID 2>/dev/null
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

echo ""
echo "=== PSAT running ==="
echo "  API:     http://127.0.0.1:8000"
echo "  Health:  http://127.0.0.1:8000/api/health"
echo ""
echo "Press Ctrl+C to stop."
wait
