"""add retry fields and failed_terminal status to jobs

Revision ID: c94ca0f7f0b2
Revises: 8d2f47b1c3e9
Create Date: 2026-05-02 12:00:00.000000

Safety checklist (delete once reviewed):
  - All three new columns are nullable (``next_attempt_at``, ``last_failure_kind``)
    or carry a server_default (``retry_count``='0'). Adding them is a
    metadata-only operation in Postgres 11+: no rewrite, no row lock past the
    brief catalog flip. Safe inside the default lock_timeout=10s.
  - ``ALTER TYPE jobstatus ADD VALUE 'failed_terminal'`` MUST run inside an
    ``autocommit_block`` — Postgres rejects ``ADD VALUE`` inside a
    transaction. Idempotent via ``IF NOT EXISTS`` so repeat runs are safe.
  - Existing ``failed`` rows stay ``failed`` with ``retry_count=0``: we don't
    know if they were transient or terminal, so the conservative default is
    no auto-retry. Operators can promote individual rows back to ``queued``
    via ``POST /api/jobs/{id}/retry`` if they want a manual retry.
  - Reversibility verified: downgrade demotes any ``failed_terminal`` rows
    back to ``failed`` (closest equivalent), rebuilds the enum without that
    value (Postgres has no ``ALTER TYPE … DROP VALUE``), then drops the
    columns.
  - env.py sets lock_timeout=10s / statement_timeout=300s. The enum swap on
    downgrade rewrites the ``jobs.status`` column type, which takes a brief
    AccessExclusiveLock — fine for the dev-machine downgrade case but worth
    flagging if this is ever run against a live populated prod table.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c94ca0f7f0b2"
down_revision: Union[str, Sequence[str], None] = "8d2f47b1c3e9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("retry_count", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "jobs",
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "jobs",
        sa.Column("last_failure_kind", sa.String(length=20), nullable=True),
    )
    # ALTER TYPE … ADD VALUE cannot run inside a transaction. The
    # autocommit_block opts this single statement out of Alembic's
    # transaction_per_migration wrapper.
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'failed_terminal'")


def downgrade() -> None:
    # Demote any ``failed_terminal`` rows so the enum rebuild below doesn't
    # have orphan values to coerce. ``failed`` is the closest pre-migration
    # equivalent (transient-vs-terminal distinction collapses).
    op.execute("UPDATE jobs SET status = 'failed' WHERE status = 'failed_terminal'")
    # Postgres has no ALTER TYPE … DROP VALUE. Rebuild the enum:
    # rename current → _old, create the pre-migration version, swap the
    # ``jobs.status`` column over, drop the renamed type.
    op.execute("ALTER TYPE jobstatus RENAME TO jobstatus_old")
    op.execute("CREATE TYPE jobstatus AS ENUM ('queued', 'processing', 'completed', 'failed')")
    op.execute("ALTER TABLE jobs ALTER COLUMN status TYPE jobstatus USING status::text::jobstatus")
    op.execute("DROP TYPE jobstatus_old")
    op.drop_column("jobs", "last_failure_kind")
    op.drop_column("jobs", "next_attempt_at")
    op.drop_column("jobs", "retry_count")
