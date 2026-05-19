"""Adopt structural-orphan contracts via parent dep edges.

Revision ID: 3a8f4d1c9b07
Revises: 9e1f4c2d8a77
Create Date: 2026-05-19 00:00:00.000000

PR #87 introduced a discovery-confidence gate that left some
legitimately protocol-owned contracts as orphans — specifically the
proxy shells and beacons that ether.fi's resolution cascade pulled in
for confirmed impls. This migration re-evaluates every orphan against
the now-extended ``asserts_ownership`` logic: if any HIGH-owned
contract has a structural dep edge (relationship_type IN
'implementation','proxy','beacon') to the orphan, adopt it.

Cross-protocol collisions — an orphan referenced by HIGH-owned
contracts of two different protocols — are skipped + logged rather
than silently assigned to one. Such cases are usually genuinely-shared
infrastructure and warrant manual review.
"""

from __future__ import annotations

import logging
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "3a8f4d1c9b07"
down_revision: Union[str, Sequence[str], None] = "9e1f4c2d8a77"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


logger = logging.getLogger("alembic.runtime.migration")


_SELECT_STRUCTURAL_ORPHANS = sa.text(
    """
    SELECT
        orphan.id              AS orphan_id,
        array_agg(DISTINCT parent.protocol_id) AS parent_protocols
    FROM contracts AS orphan
    JOIN contract_dependencies AS cd
      ON lower(cd.dependency_address) = orphan.address
    JOIN contracts AS parent
      ON parent.id = cd.contract_id
    WHERE orphan.protocol_id IS NULL
      AND parent.protocol_id IS NOT NULL
      AND cd.relationship_type IN ('implementation', 'proxy', 'beacon')
    GROUP BY orphan.id
    """
)


_ADOPT_ORPHAN = sa.text(
    """
    UPDATE contracts
    SET protocol_id = :pid,
        discovery_sources = CASE
            WHEN discovery_sources IS NULL THEN ARRAY['structural_adoption']::varchar[]
            WHEN 'structural_adoption' = ANY(discovery_sources) THEN discovery_sources
            ELSE array_append(discovery_sources, 'structural_adoption')
        END
    WHERE id = :id
    """
)


_REVERT_ADOPTIONS = sa.text(
    """
    UPDATE contracts
    SET protocol_id = NULL,
        discovery_sources = NULLIF(
            array_remove(discovery_sources, 'structural_adoption'),
            ARRAY[]::varchar[]
        )
    WHERE 'structural_adoption' = ANY(discovery_sources)
    """
)


def upgrade() -> None:
    bind = op.get_bind()
    rows = bind.execute(_SELECT_STRUCTURAL_ORPHANS).fetchall()

    adopted = 0
    skipped_multi = 0
    for orphan_id, parent_protocols in rows:
        unique = [pid for pid in (parent_protocols or []) if pid is not None]
        if not unique:
            continue
        if len(unique) > 1:
            skipped_multi += 1
            logger.warning(
                "structural-orphan adoption: contract_id=%s has structural edges from "
                "multiple HIGH-owned protocols (%s); leaving orphan for manual review",
                orphan_id,
                unique,
            )
            continue
        bind.execute(_ADOPT_ORPHAN, {"pid": unique[0], "id": orphan_id})
        adopted += 1

    if adopted or skipped_multi:
        logger.info(
            "structural-orphan adoption: adopted %d orphan contract(s); skipped %d cross-protocol case(s)",
            adopted,
            skipped_multi,
        )


def downgrade() -> None:
    # Revert any row this migration touched: protocol_id was NULL before
    # we adopted, and 'structural_adoption' is the sentinel that says
    # "this row's ownership came from this migration, not from a real
    # discovery source." Strip the tag and null the protocol_id together.
    op.get_bind().execute(_REVERT_ADOPTIONS)
