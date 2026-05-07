"""add capability_expr columns and function_principals indexes

Revision ID: a3f7b1c8d2e5
Revises: d4e8a9c1b6f3
Create Date: 2026-05-05

B.1 of the v2 predicate pipeline cutover: introduce the row representation
for non-caller capability shapes that emit zero ``FunctionPrincipal`` rows
(cofinite_blacklist, external_check_only, conditional_universal,
unsupported, irreducible AND/OR, signature_witness with external signer).
That metadata moves onto ``effective_functions``:

  * ``capability_expr JSONB`` — the resolved CapabilityExpr verbatim from
    ``services/resolution/capability_resolver.py``.
  * ``conditions JSONB`` — list of ``Condition`` objects for the
    ``conditional_universal`` kind.
  * ``status TEXT`` — NULL by default, ``'unsupported'`` when classification
    failed, ``'public'`` for conditional_universal with no auth.

Q1 (under Option A): Safes get one synthetic FunctionPrincipal row carrying
``details.owners`` and ``details.threshold``. The "does Alice have
permission to call X?" lookup needs a two-hop UNION query, so we add:

  * ``ix_function_principals_safe_owners`` — partial GIN over
    ``(details->'owners')`` where ``resolved_type = 'safe'``, for the
    Safe-owner arm of the UNION.
  * ``ix_function_principals_lower_address`` — btree on ``lower(address)``
    for the direct (case-insensitive) arm of the same UNION.

This is a SCHEMA-ONLY change. Writers and consumers land in later phases.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "a3f7b1c8d2e5"
down_revision: Union[str, Sequence[str], None] = "d4e8a9c1b6f3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "effective_functions",
        sa.Column("capability_expr", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "effective_functions",
        sa.Column("conditions", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "effective_functions",
        sa.Column("status", sa.String(length=50), nullable=True),
    )

    # GIN index over the JSONB array under details.owners, partial on
    # the synthetic Safe rows. Raw SQL because Alembic's create_index does
    # not surface the JSON-arrow expression cleanly across versions.
    op.execute(
        sa.text(
            "CREATE INDEX ix_function_principals_safe_owners "
            "ON function_principals USING gin ((details->'owners')) "
            "WHERE resolved_type = 'safe'"
        )
    )
    op.execute(sa.text("CREATE INDEX ix_function_principals_lower_address ON function_principals (lower(address))"))


def downgrade() -> None:
    op.execute(sa.text("DROP INDEX IF EXISTS ix_function_principals_lower_address"))
    op.execute(sa.text("DROP INDEX IF EXISTS ix_function_principals_safe_owners"))
    op.drop_column("effective_functions", "status")
    op.drop_column("effective_functions", "conditions")
    op.drop_column("effective_functions", "capability_expr")
