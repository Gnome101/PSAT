"""add generic indexed event logs

Revision ID: b7e9c4a1d6f2
Revises: 8c1f4b9e7a23
Create Date: 2026-05-07
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "b7e9c4a1d6f2"
down_revision: Union[str, Sequence[str], None] = "8c1f4b9e7a23"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "indexed_event_logs",
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("event_address", sa.String(length=42), nullable=False),
        sa.Column("topic0", sa.String(length=66), nullable=False),
        sa.Column("tx_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("log_index", sa.Integer(), nullable=False),
        sa.Column("block_number", sa.BigInteger(), nullable=False),
        sa.Column("block_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("transaction_index", sa.Integer(), nullable=False),
        sa.Column("topics", postgresql.JSONB(), nullable=False),
        sa.Column("data_words", postgresql.JSONB(), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.PrimaryKeyConstraint("chain_id", "event_address", "topic0", "tx_hash", "log_index"),
    )
    op.create_index(
        "ix_indexed_event_logs_lookup",
        "indexed_event_logs",
        ["chain_id", "event_address", "topic0", "block_number", "transaction_index", "log_index"],
        unique=False,
    )
    op.create_index(
        "ix_indexed_event_logs_block",
        "indexed_event_logs",
        ["chain_id", "event_address", "block_number", "log_index"],
        unique=False,
    )
    op.create_table(
        "indexed_event_cursors",
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("event_address", sa.String(length=42), nullable=False),
        sa.Column("topic0", sa.String(length=66), nullable=False),
        sa.Column("last_indexed_block", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("last_indexed_block_hash", sa.LargeBinary(length=32), nullable=True),
        sa.Column("last_run_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.PrimaryKeyConstraint("chain_id", "event_address", "topic0"),
    )


def downgrade() -> None:
    op.drop_table("indexed_event_cursors")
    op.drop_index("ix_indexed_event_logs_block", table_name="indexed_event_logs")
    op.drop_index("ix_indexed_event_logs_lookup", table_name="indexed_event_logs")
    op.drop_table("indexed_event_logs")
