#!/bin/bash
# `workers` process group: queue consumers only. Scaled as a group via
# `fly scale count --process-group workers N`.
# dapp_crawl_worker lives in `browser`; protocol_monitor in `monitor`.
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

# Static worker count is env-tunable so the bench harness can sweep it
# without rebuilding the image. Default 2 matches the long-standing prod
# fleet shape; cascade benches found 1-4 to be the useful range.
STATIC_COUNT="${PSAT_STATIC_WORKERS:-2}"
echo "  static workers: $STATIC_COUNT (set PSAT_STATIC_WORKERS to override)"

"${PYTHON_CMD[@]}" -m workers.discovery &
PIDS+=($!)
for _ in $(seq 1 "$STATIC_COUNT"); do
  "${PYTHON_CMD[@]}" -m workers.static_worker &
  PIDS+=($!)
done
"${PYTHON_CMD[@]}" -m workers.resolution_worker &
PIDS+=($!)
"${PYTHON_CMD[@]}" -m workers.policy_worker &
PIDS+=($!)
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
