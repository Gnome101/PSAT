#!/bin/bash
# `monitor` process group: three chain-scan singletons.
# DO NOT scale above 1 — running multiple instances races on scan
# state and duplicates writes.
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

PY=(uv run --no-sync python)

"${PY[@]}" -m workers.protocol_monitor &
PIDS+=($!)
"${PY[@]}" -m workers.protocol_monitor --poll &
PIDS+=($!)
"${PY[@]}" -m workers.protocol_monitor --tvl &
PIDS+=($!)

echo "Monitors started: ${PIDS[*]}"
# Exit on first death — Fly restarts the machine and relaunches all
# three. A silently-dead scanner is worse than a 30s restart.
wait -n
