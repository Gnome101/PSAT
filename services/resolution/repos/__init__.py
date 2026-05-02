"""Postgres-backed implementations of the SetAdapter Repo protocols.

Each module here implements a Protocol from ``services.resolution.adapters``
against the database tables maintained by the indexer workers.

  * ``role_grants_pg.PostgresRoleGrantsRepo`` — reads
    ``role_grants_events`` written by ``workers/role_grants_indexer.py``.

Tests provide in-memory fakes (``tests/test_adapters.py:FakeRoleGrantsRepo``);
this module is the production wiring."""

from .role_grants_pg import PostgresRoleGrantsRepo

__all__ = ["PostgresRoleGrantsRepo"]
