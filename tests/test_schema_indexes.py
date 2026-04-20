"""Schema-level regression tests for required indexes.

These exist because Postgres does NOT auto-create an index on a foreign-key
column, and several hot paths in the coverage / timeline code scan those
columns. If someone later drops an index, the matching assertion here fires.
"""

from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.conftest import requires_postgres  # noqa: E402


@requires_postgres
def test_upgrade_events_contract_id_index_exists(db_session):
    """``upgrade_events.contract_id`` must have an index.

    Hit by services.audits.coverage._compute_impl_windows* and by
    api.contract_audit_timeline on every request. The FK constraint alone
    does not create one; we rely on ``ix_upgrade_events_contract_id``.
    """
    row = db_session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename = 'upgrade_events' "
            "AND indexname = 'ix_upgrade_events_contract_id'"
        )
    ).scalar_one_or_none()
    assert row == "ix_upgrade_events_contract_id", (
        "Missing index ix_upgrade_events_contract_id on upgrade_events(contract_id) — "
        "this index is required by the coverage matcher and audit_timeline API; "
        "re-add it to apply_storage_migrations() in db/models.py"
    )
