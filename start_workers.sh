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
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_CMD=(python3)
elif command -v python >/dev/null 2>&1; then
  PYTHON_CMD=(python)
else
  echo "ERROR: No Python interpreter found. Run 'uv sync' or activate the project virtualenv."
  exit 1
fi

echo "Starting PSAT workers with: ${PYTHON_CMD[*]}"

"${PYTHON_CMD[@]}" -m workers.discovery &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.static_worker &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.resolution_worker &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.policy_worker &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.proxy_monitor &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.proxy_monitor --poll &
PIDS+=($!)

echo "All workers started: ${PIDS[*]}"
wait "${PIDS[@]}"
