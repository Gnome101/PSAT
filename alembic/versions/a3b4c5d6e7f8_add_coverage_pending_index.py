"""add partial index on audit_contract_coverage.equivalence_status='pending'

Revision ID: a3b4c5d6e7f8
Revises: f9c2a83d1e44
Create Date: 2026-05-08 12:00:00.000000

Source-equivalence verification was deferred from the inline coverage
write to a dedicated ``CoverageVerifyWorker`` (#82). The worker's claim
query is ``WHERE equivalence_status = 'pending' ORDER BY id LIMIT N``;
without an index that matches the predicate, the scan walks every row
in ``audit_contract_coverage`` once per poll. A partial btree index
gated on ``equivalence_status='pending'`` keeps the scan a single
index seek even after the table grows past tens of millions of rows
because terminal-status rows (``proven``, ``hash_mismatch``, ...) drop
out of the index entirely.

Safety checklist (delete once reviewed):
  - Index is partial on a small predicate set, so the index size
    tracks the queue depth not the table size — a healthy queue means
    a tiny index. No rewrite, no row lock past the brief catalog flip.
  - Built inline (not CONCURRENTLY) because the table is small at
    rollout time. Move to ``CREATE INDEX CONCURRENTLY`` inside an
    autocommit_block if the table ever grows past ~10M rows before
    the migration runs.
  - Reversibility verified: downgrade drops the index.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a3b4c5d6e7f8"
down_revision: Union[str, Sequence[str], None] = "f9c2a83d1e44"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_acc_equivalence_pending",
        "audit_contract_coverage",
        ["id"],
        unique=False,
        postgresql_where=sa.text("equivalence_status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index("ix_acc_equivalence_pending", table_name="audit_contract_coverage")
