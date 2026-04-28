"""Unit tests for the within-cascade proxy-dedupe helper added to
``workers.selection_worker`` for Phase B Step 1.

Lives in its own file (not ``test_selection_worker.py``) because the
parent test module is gated by ``pytestmark = [requires_postgres]`` and
these helper tests use only ``MagicMock`` — they should run without
docker-compose Postgres up.

Integration coverage of the actual cascade behavior is the bench:
``cascade_job_count`` dropping from ~38 → ~22 after this fix is the
end-to-end signal.

Why the helper exists:

The bench observed 5× UUPSProxy duplicates per cascade because
``_queue_top_n`` had a "known proxy → re-queue for upgrade detection"
branch (``selection_worker.py:281``) that fired once per discovery
source surfacing the proxy. Under ``--force`` (bench mode) we want
exactly one job per (address, cascade); without ``--force`` (production)
the upgrade-detection re-queue stays useful.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from workers.selection_worker import _existing_in_same_cascade


def test_existing_in_same_cascade_returns_true_when_match():
    """Helper returns True when an existing job for the address is in
    the same cascade (root_job_id matches)."""
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = "some-existing-job-id"
    assert _existing_in_same_cascade(session, "0xabc", "ethereum", "root-1") is True


def test_existing_in_same_cascade_returns_false_when_no_match():
    """Helper returns False when no job for the address is in this
    cascade — fresh cascades always get fresh proxy jobs."""
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    assert _existing_in_same_cascade(session, "0xabc", "ethereum", "root-1") is False


def test_existing_in_same_cascade_handles_null_chain():
    """Some discovery sources don't carry a chain. The helper must not
    crash and must omit the chain filter when chain is None."""
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    assert _existing_in_same_cascade(session, "0xabc", None, "root-1") is False
    # SQL was issued (chain=None branch didn't short-circuit).
    assert session.execute.called
