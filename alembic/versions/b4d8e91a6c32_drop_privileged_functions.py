"""drop privileged_functions

Revision ID: b4d8e91a6c32
Revises: a3f7b1c8d2e5
Create Date: 2026-05-08

The semantic predicate pipeline no longer writes the legacy
``privileged_functions`` table. Per-function control data now lives in
``effective_functions`` plus ``function_principals``.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "b4d8e91a6c32"
down_revision: Union[str, Sequence[str], None] = "a3f7b1c8d2e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("ix_privileged_functions_contract_id", table_name="privileged_functions")
    op.drop_table("privileged_functions")


def downgrade() -> None:
    op.create_table(
        "privileged_functions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("contract_id", sa.Integer(), nullable=False),
        sa.Column("function_name", sa.String(length=255), nullable=False),
        sa.Column("selector", sa.String(length=10), nullable=True),
        sa.Column("abi_signature", sa.Text(), nullable=True),
        sa.Column("effect_labels", postgresql.ARRAY(sa.String(length=100)), nullable=True),
        sa.Column("action_summary", sa.Text(), nullable=True),
        sa.Column("authority_public", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(["contract_id"], ["contracts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_privileged_functions_contract_id", "privileged_functions", ["contract_id"], unique=False)
