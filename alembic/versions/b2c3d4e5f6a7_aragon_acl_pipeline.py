"""aragon_acl pipeline: events log + cursor

Mirrors the role_grants_pipeline layout — same PK shape, same
two-index pattern, same cursor table for reorg detection. Aragon
permissions are ``(entity, app, role, allowed)`` tuples flipped
by ``SetPermission`` events on the ACL contract; the repo replays
those events to compute the current allowed-entity set per
``(app, role)``.

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-05-02
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "aragon_acl_events",
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("acl_contract_id", sa.Integer(), nullable=False),
        sa.Column("tx_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("log_index", sa.Integer(), nullable=False),
        sa.Column("app", sa.String(length=42), nullable=False),
        sa.Column("role", sa.LargeBinary(length=32), nullable=False),
        sa.Column("entity", sa.String(length=42), nullable=False),
        sa.Column("allowed", sa.Boolean(), nullable=False),
        sa.Column("block_number", sa.BigInteger(), nullable=False),
        sa.Column("block_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("transaction_index", sa.Integer(), nullable=False),
        sa.Column(
            "detected_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["acl_contract_id"], ["contracts.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("chain_id", "acl_contract_id", "tx_hash", "log_index"),
    )
    op.create_index(
        "ix_aragon_acl_events_lookup",
        "aragon_acl_events",
        [
            "chain_id",
            "acl_contract_id",
            "app",
            "role",
            "entity",
            "block_number",
            "log_index",
        ],
    )
    op.create_index(
        "ix_aragon_acl_events_block",
        "aragon_acl_events",
        ["chain_id", "acl_contract_id", "block_number", "log_index"],
    )

    op.create_table(
        "aragon_acl_cursors",
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("acl_contract_id", sa.Integer(), nullable=False),
        sa.Column(
            "last_indexed_block",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("last_indexed_block_hash", sa.LargeBinary(length=32), nullable=True),
        sa.Column(
            "last_run_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["acl_contract_id"], ["contracts.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("chain_id", "acl_contract_id"),
    )


def downgrade() -> None:
    op.drop_table("aragon_acl_cursors")
    op.drop_index("ix_aragon_acl_events_block", table_name="aragon_acl_events")
    op.drop_index("ix_aragon_acl_events_lookup", table_name="aragon_acl_events")
    op.drop_table("aragon_acl_events")
