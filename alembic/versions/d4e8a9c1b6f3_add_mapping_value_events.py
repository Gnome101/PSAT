"""mapping_value_events: durable backbone for ValuePredicate enumeration

Adds the read-side schema the EventIndexedAdapter's value-aware path
needs to compute "latest value per key" for ``mapping(K => V)``
predicates without an on-demand HyperSync scan:

  * ``mapping_value_events`` — append-only log of decoded set events
    (one row per ``OwnerSet(addr, val)``-style emission). PK is
    (chain_id, contract_id, mapping_name, tx_hash, log_index) so the
    same log can't double-insert across re-scans. Lookup index on
    (chain_id, contract_id, mapping_name, key_hex, block_number,
    log_index) makes the ``DISTINCT ON (key_hex) ORDER BY block DESC``
    latest-by query cheap.
  * ``mapping_value_cursors`` — one row per (chain_id, contract_id);
    mirrors ``role_grants_cursors`` in shape so the indexer's reorg-
    detect logic is verbatim from ``role_grants_indexer``.

Revision ID: d4e8a9c1b6f3
Revises: 8c1f4b9e7a23
Create Date: 2026-05-03
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d4e8a9c1b6f3"
down_revision: Union[str, Sequence[str], None] = "8c1f4b9e7a23"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "mapping_value_events",
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("contract_id", sa.Integer(), nullable=False),
        sa.Column("mapping_name", sa.String(120), nullable=False),
        sa.Column("tx_hash", sa.LargeBinary(32), nullable=False),
        sa.Column("log_index", sa.Integer(), nullable=False),
        sa.Column("key_hex", sa.String(66), nullable=False),
        sa.Column("value_hex", sa.String(66), nullable=False),
        sa.Column("block_number", sa.BigInteger(), nullable=False),
        sa.Column("block_hash", sa.LargeBinary(32), nullable=False),
        sa.Column("transaction_index", sa.Integer(), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["contract_id"], ["contracts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("chain_id", "contract_id", "mapping_name", "tx_hash", "log_index"),
    )
    op.create_index(
        "ix_mapping_value_events_lookup",
        "mapping_value_events",
        ["chain_id", "contract_id", "mapping_name", "key_hex", "block_number", "log_index"],
    )
    op.create_index(
        "ix_mapping_value_events_block",
        "mapping_value_events",
        ["chain_id", "contract_id", "block_number", "log_index"],
    )

    op.create_table(
        "mapping_value_cursors",
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("contract_id", sa.Integer(), nullable=False),
        sa.Column("last_indexed_block", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("last_indexed_block_hash", sa.LargeBinary(32), nullable=True),
        sa.Column(
            "last_run_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["contract_id"], ["contracts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("chain_id", "contract_id"),
    )


def downgrade() -> None:
    op.drop_table("mapping_value_cursors")
    op.drop_index("ix_mapping_value_events_block", table_name="mapping_value_events")
    op.drop_index("ix_mapping_value_events_lookup", table_name="mapping_value_events")
    op.drop_table("mapping_value_events")
