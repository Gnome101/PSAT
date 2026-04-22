#!/bin/bash
# `browser` process group: Playwright + dapp_crawl_worker only.
# Isolated because Chromium's RAM profile is much heavier than the
# other queue workers — sharing a VM risks OOMing everything together.
set -e

cd "$(dirname "$0")"

export PYTHONUNBUFFERED=1

exec uv run --no-sync python -m workers.dapp_crawl_worker
