#!/bin/bash
# Process group: workers-heavy. Per codex review of the etherfi LP cascade
# bench, the sustained-work workers are static + resolution + policy
# (Slither + recursive control graph + classify_resolved_address fan-out).
# These benefit from dedicated CPU. Pair with workers-light on shared-cpu.
set -e

cd "$(dirname "$0")"

export PYTHONUNBUFFERED=1

# Match start_workers.sh: parallel fan-outs occasionally need a second
# session inside the same job, so bump base+overflow from 2+3 to 4+6.
export PSAT_DB_POOL_SIZE="${PSAT_DB_POOL_SIZE:-4}"
export PSAT_DB_MAX_OVERFLOW="${PSAT_DB_MAX_OVERFLOW:-6}"

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
  echo "ERROR: No Python interpreter found."
  exit 1
fi

echo "Starting PSAT workers-heavy with: ${PYTHON_CMD[*]}"

# 4 static workers — cascade test on shared-cpu-2x revealed 7-deep static queue
# behind only 2 workers when an etherfi-style proxy spawns ~20 child jobs.
# Doubling static parallelism halves the queue at the dominant chokepoint.
"${PYTHON_CMD[@]}" -m workers.static_worker &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.static_worker &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.static_worker &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.static_worker &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.resolution_worker &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.policy_worker &
PIDS+=($!)

echo "All heavy workers started: ${PIDS[*]}"
wait -n
