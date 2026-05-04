"""add trace_id to jobs

Revision ID: 8d2f47b1c3e9
Revises: ccfe335ed565
Create Date: 2026-05-02 12:00:00.000000

Safety checklist (delete once reviewed):
  - Adding a nullable column to ``jobs`` is a metadata-only operation in
    Postgres 11+: no rewrite, no row lock past the brief catalog flip.
    Safe to run inside the default lock_timeout=10s.
  - The accompanying btree index (``ix_jobs_trace_id``) is built inline
    here. ``jobs`` is small enough in practice that an inline build is
    fine; if the table ever grows past ~10M rows this should be moved
    to ``CREATE INDEX CONCURRENTLY`` inside an autocommit_block.
  - Old rows stay NULL on purpose. Workers and routers tolerate
    ``trace_id IS NULL`` (it just means the row predates correlation),
    so no backfill is required.
  - Reversibility verified: downgrade drops the index then the column.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "8d2f47b1c3e9"
down_revision: Union[str, Sequence[str], None] = "ccfe335ed565"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("trace_id", sa.String(length=32), nullable=True))
    op.create_index("ix_jobs_trace_id", "jobs", ["trace_id"])


def downgrade() -> None:
    op.drop_index("ix_jobs_trace_id", table_name="jobs")
    op.drop_column("jobs", "trace_id")
