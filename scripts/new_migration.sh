#!/usr/bin/env bash
# Generate a new Alembic revision against TEST_DATABASE_URL.
#
# Why: `alembic revision --autogenerate` diffs Base.metadata against
# whatever DB the URL points at. Running it against the local dev DB
# (which may already have ad-hoc changes) produces noisy diffs; running
# it against $DATABASE_URL would target prod. We force TEST_DATABASE_URL
# so the diff is reproducible.
#
# Usage: scripts/new_migration.sh "short message"
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 \"short message\"" >&2
  exit 2
fi

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a; source .env; set +a
fi

if [[ -z "${TEST_DATABASE_URL:-}" ]]; then
  echo "TEST_DATABASE_URL not set (expected in .env)" >&2
  exit 1
fi

# Make sure the test DB is at head before diffing — otherwise autogenerate
# will pick up phantom changes from missing migrations.
DATABASE_URL="$TEST_DATABASE_URL" uv run alembic upgrade head

DATABASE_URL="$TEST_DATABASE_URL" uv run alembic revision --autogenerate -m "$1"
