"""cache predicate_trees, add builder_started_at for in-flight tracking

Revision ID: c1d2e3f4a5b6
Revises: b4d8e91a6c32
Create Date: 2026-05-12 15:00:00.000000

Two related changes to ``contract_materializations``:

1. ``predicate_trees`` (JSONB + paired ``predicate_trees_blob_key`` Text)
   so the recursive resolver's cache hits carry the semantic predicate
   trees produced by the predicate pipeline. Pre-this-migration the
   builder discarded them — every cache hit silently disabled
   mapping-writer enumeration downstream
   (``services/resolution/recursive.py:_mapping_writer_specs_from_predicate_trees``
   short-circuits on None).

   ``effects`` was considered for the same treatment but has no consumer
   in the materialization-cache path today: the policy stage reads the
   per-job ``effects`` artifact written by the static worker, and
   ``copy_static_cache`` propagates it across same-bytecode jobs.

2. ``builder_started_at`` so a ``status='building'`` row can be detected
   as in-flight vs. abandoned. Pre-this-migration ``materialize_or_wait``
   released the advisory lock between phase-1 ready-check and phase-2
   builder and stored *nothing* about in-flight builds; two callers
   reaching phase-1 within the build window both ran the (now 60-150s)
   builder and only the second's write was deduped. This column lets
   the second caller wait on the first instead of duplicating work.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "c1d2e3f4a5b6"
down_revision: Union[str, Sequence[str], None] = "b4d8e91a6c32"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "contract_materializations",
        sa.Column("predicate_trees", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "contract_materializations",
        sa.Column("predicate_trees_blob_key", sa.Text(), nullable=True),
    )
    op.add_column(
        "contract_materializations",
        sa.Column("builder_started_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("contract_materializations", "builder_started_at")
    op.drop_column("contract_materializations", "predicate_trees_blob_key")
    op.drop_column("contract_materializations", "predicate_trees")
