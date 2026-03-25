#!/bin/bash
set -e

cd "$(dirname "$0")"

export PYTHONUNBUFFERED=1

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
"${PYTHON_CMD[@]}" -m workers.static_worker &
"${PYTHON_CMD[@]}" -m workers.resolution_worker &
"${PYTHON_CMD[@]}" -m workers.policy_worker &

echo "All workers started. Waiting..."
wait
