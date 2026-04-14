#!/bin/bash
set -e

cd "$(dirname "$0")"

export PYTHONUNBUFFERED=1

PIDS=()

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  if [ ${#PIDS[@]} -gt 0 ]; then
    kill "${PIDS[@]}" 2>/dev/null || true
    wait "${PIDS[@]}" 2>/dev/null || true
  fi
  exit $exit_code
}

trap cleanup EXIT INT TERM

if [ -x ".venv/bin/python" ]; then
  PYTHON_CMD=(./.venv/bin/python)
elif command -v uv >/dev/null 2>&1; then
  PYTHON_CMD=(uv run --no-sync python)
else
  PYTHON_CMD=(python3)
fi

echo "Starting protocol monitor (scanner + poller + tvl) with: ${PYTHON_CMD[*]}"

"${PYTHON_CMD[@]}" -m workers.protocol_monitor &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.protocol_monitor --poll &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.protocol_monitor --tvl &
PIDS+=($!)

echo "Protocol monitor started: ${PIDS[*]}"
wait "${PIDS[@]}"
