"""merge coverage index and predicate pipeline migration heads

Revision ID: d4e5f6a7b8c9
Revises: b4d8e91a6c32, a3b4c5d6e7f8
Create Date: 2026-05-09 21:15:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, Sequence[str], None] = ("b4d8e91a6c32", "a3b4c5d6e7f8")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
