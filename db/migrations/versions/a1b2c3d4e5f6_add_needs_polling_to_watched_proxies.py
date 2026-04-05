"""add needs_polling to watched_proxies

Revision ID: a1b2c3d4e5f6
Revises: d357e4e8d46b
Create Date: 2026-04-04 16:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "d357e4e8d46b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add needs_polling column to watched_proxies table."""
    op.add_column(
        "watched_proxies",
        sa.Column("needs_polling", sa.Boolean(), nullable=False, server_default="false"),
    )


def downgrade() -> None:
    """Drop needs_polling column from watched_proxies table."""
    op.drop_column("watched_proxies", "needs_polling")
