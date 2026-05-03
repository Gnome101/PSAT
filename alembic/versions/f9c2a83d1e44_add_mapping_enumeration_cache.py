"""add mapping_enumeration_cache table

Revision ID: f9c2a83d1e44
Revises: e7a4f1d63b29
Create Date: 2026-05-03 09:00:00.000000

Cross-process cache for mapping_enumerator hypersync scans. Workers
were split into separate OS processes by 9ce6fa3 ("perf: parallelize
worker pipeline"); the existing in-process module dict in
services/resolution/mapping_enumerator.py:_CACHE then stopped sharing
across the resolution/policy stage boundary. Same job, two stages,
two consecutive 60s hypersync timeouts on the same address — visible
in the LinkToken concurrent-test wedge.

Schema:
  - PRIMARY KEY (chain, address, specs_hash). specs_hash is sha256 of
    the normalized writer-spec list; a config change therefore yields
    a fresh row rather than a stale hit.
  - principals: JSONB list of EnumeratedPrincipal dicts.
  - status: complete | incomplete_timeout | incomplete_max_pages | error.
    Truncated/errored results are persisted intentionally — re-running
    inside the TTL would just hit the same bound; the caller sees the
    status field and decides.
  - materialized_at indexed for TTL-driven cleanup queries.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "f9c2a83d1e44"
down_revision: Union[str, Sequence[str], None] = "e7a4f1d63b29"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "mapping_enumeration_cache",
        sa.Column("chain", sa.String(length=100), nullable=False),
        sa.Column("address", sa.String(length=42), nullable=False),
        sa.Column("specs_hash", sa.String(length=64), nullable=False),
        sa.Column("principals", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("pages_fetched", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_block_scanned", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("materialized_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.PrimaryKeyConstraint("chain", "address", "specs_hash"),
    )
    op.create_index(
        "ix_mapping_enumeration_cache_materialized_at",
        "mapping_enumeration_cache",
        ["materialized_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_mapping_enumeration_cache_materialized_at", table_name="mapping_enumeration_cache")
    op.drop_table("mapping_enumeration_cache")
