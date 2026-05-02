#!/bin/bash
# `workers` process group: queue consumers only. Scaled as a group via
# `fly scale count --process-group workers N`.
# dapp_crawl_worker lives in `browser`; protocol_monitor in `monitor`.
set -e

cd "$(dirname "$0")"

export PYTHONUNBUFFERED=1

# Worker DB pool sizing. Bumped from 2+3 to 4+6 alongside the threaded
# fan-outs: fan-out heartbeats + parallel artifact reads can grab a second
# session inside the same job. With ~10 worker procs per VM this caps total
# worker DB connections at ~100, still well under Neon's pool ceiling.
# Override per-process by exporting PSAT_DB_POOL_SIZE / PSAT_DB_MAX_OVERFLOW
# before this script.
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
  echo "ERROR: No Python interpreter found. Run 'uv sync' or activate the project virtualenv."
  exit 1
fi

echo "Starting PSAT workers with: ${PYTHON_CMD[*]}"

# Worker counts are env-tunable so the bench harness can sweep them
# without rebuilding the image. Defaults match the long-standing prod
# fleet shape; cascade benches found 1-4 to be the useful range per
# stage. Recommended values per VM size:
#   shared-cpu-1x:    static=1 resolution=1 policy=1
#   performance-2x:   static=2 resolution=2 policy=1   (current default)
#   performance-4x:   static=4 resolution=2 policy=2
STATIC_COUNT="${PSAT_STATIC_WORKERS:-2}"
RESOLUTION_COUNT="${PSAT_RESOLUTION_WORKERS:-1}"
POLICY_COUNT="${PSAT_POLICY_WORKERS:-1}"
echo "  static workers:     $STATIC_COUNT (set PSAT_STATIC_WORKERS to override)"
echo "  resolution workers: $RESOLUTION_COUNT (set PSAT_RESOLUTION_WORKERS to override)"
echo "  policy workers:     $POLICY_COUNT (set PSAT_POLICY_WORKERS to override)"

"${PYTHON_CMD[@]}" -m workers.discovery &
PIDS+=($!)
for _ in $(seq 1 "$STATIC_COUNT"); do
  "${PYTHON_CMD[@]}" -m workers.static_worker &
  PIDS+=($!)
done
for _ in $(seq 1 "$RESOLUTION_COUNT"); do
  "${PYTHON_CMD[@]}" -m workers.resolution_worker &
  PIDS+=($!)
done
for _ in $(seq 1 "$POLICY_COUNT"); do
  "${PYTHON_CMD[@]}" -m workers.policy_worker &
  PIDS+=($!)
done
"${PYTHON_CMD[@]}" -m workers.coverage_worker &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.defillama_worker &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.selection_worker &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.audit_text_extraction &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.audit_scope_extraction &
PIDS+=($!)

echo "All workers started: ${PIDS[*]}"
# Exit on first death — Fly restarts the machine so every worker
# relaunches. Silent-dead-worker is worse than a 30s restart.
wait -n
