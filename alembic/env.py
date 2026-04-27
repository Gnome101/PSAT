"""Alembic environment.

Reads ``DATABASE_URL`` from the environment (same var the app uses) and
points autogenerate at ``db.models.Base.metadata``. ``transaction_per_migration``
is on so individual migrations can opt out of the transaction with
``with op.get_context().autocommit_block():`` for ``CREATE INDEX CONCURRENTLY``.
"""

from __future__ import annotations

import os

from sqlalchemy import engine_from_config, pool

from alembic import context
from db.models import Base

config = context.config

# Prefer a URL set programmatically (e.g. from a test helper) over $DATABASE_URL,
# so tests can target TEST_DATABASE_URL without mutating the process env.
DATABASE_URL = config.get_main_option("sqlalchemy.url") or os.environ.get(
    "DATABASE_URL", "postgresql://psat:psat@localhost:5433/psat"
)
config.set_main_option("sqlalchemy.url", DATABASE_URL)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            transaction_per_migration=True,
            compare_type=True,
            compare_server_default=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
