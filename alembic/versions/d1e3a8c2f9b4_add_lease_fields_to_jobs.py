"""add lease_id and lease_expires_at to jobs

Revision ID: d1e3a8c2f9b4
Revises: c94ca0f7f0b2
Create Date: 2026-05-02 18:00:00.000000

Replace the implicit ``updated_at < NOW() - 900s`` heartbeat with an
explicit per-claim lease. ``claim_job`` mints a fresh ``lease_id``
(uuid4) on every successful claim and stamps ``lease_expires_at`` to
NOW() + ttl. ``_heartbeat`` extends ``lease_expires_at`` past the
current TTL window. ``reclaim_stuck_jobs`` keys on
``lease_expires_at < NOW()`` instead of ``updated_at``.

Why this matters: heartbeats run from inside parallel_map's per-task
callback. A single nested forge build that takes longer than the stale
timeout silently expires the implicit lease today, letting a sibling
worker claim the same row. With explicit ``lease_id``, every mutating
queue write (``advance_job``, ``complete_job``, ``requeue_job``,
``fail_job_terminal``) takes a ``lease_id`` argument and the row update
filters on it. A worker whose lease has rolled to a sibling raises
``LeaseLost`` instead of silently writing.

Both columns are nullable so pre-migration rows remain valid; the
claim path treats ``lease_id IS NULL`` as "no live holder" and the
sweep treats ``lease_expires_at IS NULL`` as "ineligible" (legacy rows
fall through to the existing ``updated_at`` predicate via the OR clause
inside reclaim_stuck_jobs).

The new index ``ix_jobs_lease_expires_at`` is partial on
``status='processing'`` so the sweep query stays a single index scan.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "d1e3a8c2f9b4"
down_revision: Union[str, Sequence[str], None] = "c94ca0f7f0b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("lease_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "jobs",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_jobs_lease_expires_at",
        "jobs",
        ["lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("status = 'processing'"),
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_lease_expires_at", table_name="jobs")
    op.drop_column("jobs", "lease_expires_at")
    op.drop_column("jobs", "lease_id")
