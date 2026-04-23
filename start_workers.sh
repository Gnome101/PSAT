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

# Emit one spawn line per sub-worker (JSON so Loki `| json` indexes the
# module and pid). When a sub-worker OOM-dies before its first Python-side
# log, this is the only evidence it ever existed — invaluable for
# distinguishing "never spawned" from "died silently."
spawn() {
  local module="$1"
  "${PYTHON_CMD[@]}" -m "$module" &
  local pid=$!
  PIDS+=("$pid")
  printf '{"ts":"%s","level":"INFO","logger":"start_workers","msg":"spawned sub-worker","module":"%s","pid":%d}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" "$module" "$pid"
}

spawn workers.discovery
spawn workers.static_worker
spawn workers.static_worker
spawn workers.resolution_worker
spawn workers.policy_worker
spawn workers.coverage_worker
spawn workers.defillama_worker
spawn workers.selection_worker
spawn workers.audit_text_extraction
spawn workers.audit_scope_extraction

printf '{"ts":"%s","level":"INFO","logger":"start_workers","msg":"all sub-workers spawned","pids":"%s","count":%d}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" "${PIDS[*]}" "${#PIDS[@]}"
# Exit on first death — Fly restarts the machine so every worker
# relaunches. Silent-dead-worker is worse than a 30s restart.
wait -n
