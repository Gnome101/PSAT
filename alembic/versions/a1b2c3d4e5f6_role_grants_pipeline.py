"""role_grants pipeline: events log + cursor + chain_finality_config

Adds the read-side schema the AccessControl adapter needs to enumerate
role members exactly:

  * ``role_grants_events`` — append-only log of RoleGranted /
    RoleRevoked events. PK is (chain_id, contract_id, tx_hash,
    log_index) so the same log is at most one row even on
    re-scans. Lookup index on (chain_id, contract_id, role,
    member, block_number, log_index) so RoleGrantsRepo can scan
    a role's history cheaply.
  * ``role_grants_cursors`` — one row per (chain_id, contract_id),
    tracks the indexer's last-indexed block + block-hash for
    reorg detection. The indexer takes a Postgres advisory lock
    on (contract_id) to serialize work across worker replicas.
  * ``chain_finality_config`` — per-chain confirmation depth
    (mainnet=12, polygon=128, etc.). Seeded by this migration.

Revision ID: a1b2c3d4e5f6
Revises: 850ff90146d7
Create Date: 2026-05-02
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "850ff90146d7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Per-chain confirmation depth. Conservative defaults that keep
# reorg-induced false positives below 1 / 1e6 on each chain. Operators
# can tune by updating the row directly.
#
# Names match the convention used by ``MonitoredContract.chain`` /
# ``WatchedProxy.chain`` so cross-table joins on chain string don't
# need a translation layer.
_DEFAULT_FINALITY = (
    (1, "ethereum", 12),
    (10, "optimism", 24),
    (137, "polygon", 128),
    (8453, "base", 24),
    (42161, "arbitrum", 20),
    (59144, "linea", 24),
    (534352, "scroll", 24),
)


def upgrade() -> None:
    op.create_table(
        "role_grants_events",
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("contract_id", sa.Integer(), nullable=False),
        sa.Column("tx_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("log_index", sa.Integer(), nullable=False),
        sa.Column("role", sa.LargeBinary(length=32), nullable=False),
        sa.Column("member", sa.String(length=42), nullable=False),
        sa.Column("direction", sa.String(length=10), nullable=False),
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
            ["contract_id"], ["contracts.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("chain_id", "contract_id", "tx_hash", "log_index"),
    )
    op.create_index(
        "ix_role_grants_events_lookup",
        "role_grants_events",
        ["chain_id", "contract_id", "role", "member", "block_number", "log_index"],
    )
    op.create_index(
        "ix_role_grants_events_block",
        "role_grants_events",
        ["chain_id", "contract_id", "block_number", "log_index"],
    )

    op.create_table(
        "role_grants_cursors",
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("contract_id", sa.Integer(), nullable=False),
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
            ["contract_id"], ["contracts.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("chain_id", "contract_id"),
    )

    op.create_table(
        "chain_finality_config",
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=50), nullable=False),
        sa.Column("confirmation_depth", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("chain_id"),
    )

    op.bulk_insert(
        sa.table(
            "chain_finality_config",
            sa.column("chain_id", sa.Integer()),
            sa.column("name", sa.String()),
            sa.column("confirmation_depth", sa.Integer()),
        ),
        [
            {"chain_id": cid, "name": name, "confirmation_depth": depth}
            for cid, name, depth in _DEFAULT_FINALITY
        ],
    )


def downgrade() -> None:
    op.drop_table("chain_finality_config")
    op.drop_table("role_grants_cursors")
    op.drop_index("ix_role_grants_events_block", table_name="role_grants_events")
    op.drop_index("ix_role_grants_events_lookup", table_name="role_grants_events")
    op.drop_table("role_grants_events")
