"""add proxy monitoring tables

Revision ID: d357e4e8d46b
Revises: 2004f600d83c
Create Date: 2026-04-04 15:37:33.914439

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "d357e4e8d46b"
down_revision: Union[str, Sequence[str], None] = "2004f600d83c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create watched_proxies and proxy_upgrade_events tables."""
    op.create_table(
        "watched_proxies",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("proxy_address", sa.String(42), nullable=False),
        sa.Column("chain", sa.String(), nullable=False, server_default="ethereum"),
        sa.Column("label", sa.String(), nullable=True),
        sa.Column("last_known_implementation", sa.String(42), nullable=True),
        sa.Column("last_scanned_block", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("proxy_address", "chain", name="uq_watched_proxy_address_chain"),
    )

    op.create_table(
        "proxy_upgrade_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column(
            "watched_proxy_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("watched_proxies.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("block_number", sa.Integer(), nullable=False),
        sa.Column("tx_hash", sa.String(66), nullable=False),
        sa.Column("old_implementation", sa.String(42), nullable=True),
        sa.Column("new_implementation", sa.String(42), nullable=False),
        sa.Column("event_type", sa.String(), nullable=False, server_default="upgraded"),
        sa.Column("detected_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )


def downgrade() -> None:
    """Drop proxy monitoring tables."""
    op.drop_table("proxy_upgrade_events")
    op.drop_table("watched_proxies")
