"""add proxy_type to watched_proxies

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-04-05 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add proxy_type column to watched_proxies table."""
    op.add_column(
        "watched_proxies",
        sa.Column("proxy_type", sa.String(), nullable=True),
    )


def downgrade() -> None:
    """Drop proxy_type column from watched_proxies table."""
    op.drop_column("watched_proxies", "proxy_type")
