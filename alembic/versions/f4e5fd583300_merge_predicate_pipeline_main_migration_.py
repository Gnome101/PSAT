"""Merge predicate-pipeline + main migration heads

Revision ID: f4e5fd583300
Revises: b2c3d4e5f6a7, f9c2a83d1e44
Create Date: 2026-05-04 00:50:56.197520

Safety checklist (delete once reviewed):
  - CREATE INDEX CONCURRENTLY / ALTER TYPE ADD VALUE must run in
    ``with op.get_context().autocommit_block():`` — they cannot run inside
    a transaction.
  - Adding a NOT NULL column to a populated table needs a server_default
    (or a 3-step add-nullable / backfill / set-not-null sequence).
  - Don't rename columns in one step on a live deploy — old code is still
    reading the old name. Add new column, dual-write, drop later.
  - env.py sets lock_timeout=10s / statement_timeout=300s. Override inside
    the migration if the operation legitimately needs longer.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'f4e5fd583300'
down_revision: Union[str, Sequence[str], None] = ('b2c3d4e5f6a7', 'f9c2a83d1e44')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
