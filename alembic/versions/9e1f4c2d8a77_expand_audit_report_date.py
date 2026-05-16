"""Expand audit report date field.

Revision ID: 9e1f4c2d8a77
Revises: c1d2e3f4a5b6
Create Date: 2026-05-16 15:30:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "9e1f4c2d8a77"
down_revision: Union[str, Sequence[str], None] = "c1d2e3f4a5b6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("audit_reports", "date", existing_type=sa.String(length=20), type_=sa.Text(), nullable=True)


def downgrade() -> None:
    op.alter_column("audit_reports", "date", existing_type=sa.Text(), type_=sa.String(length=20), nullable=True)
