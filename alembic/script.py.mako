"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

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
${imports if imports else ""}

revision: str = ${repr(up_revision)}
down_revision: Union[str, Sequence[str], None] = ${repr(down_revision)}
branch_labels: Union[str, Sequence[str], None] = ${repr(branch_labels)}
depends_on: Union[str, Sequence[str], None] = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
