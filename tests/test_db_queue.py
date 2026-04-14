"""Unit tests for db/queue.py helpers."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.models import Protocol
from db.queue import get_or_create_protocol


class TestGetOrCreateProtocol:
    def test_creates_when_missing(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        row = get_or_create_protocol(session, "ether.fi", official_domain="ether.fi")

        assert isinstance(row, Protocol)
        assert row.name == "ether.fi"
        assert row.official_domain == "ether.fi"
        session.add.assert_called_once()
        session.flush.assert_called_once()

    def test_returns_existing_without_modifying(self):
        existing = Protocol(name="uniswap", official_domain="uniswap.org")
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = existing

        row = get_or_create_protocol(session, "uniswap", official_domain="uniswap.org")

        assert row is existing
        assert row.official_domain == "uniswap.org"
        session.add.assert_not_called()
        session.flush.assert_not_called()

    def test_backfills_official_domain_when_null(self):
        existing = Protocol(name="aave", official_domain=None)
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = existing

        row = get_or_create_protocol(session, "aave", official_domain="aave.com")

        assert row is existing
        assert row.official_domain == "aave.com"
        session.add.assert_not_called()
        session.flush.assert_called_once()

    def test_does_not_overwrite_existing_official_domain(self):
        existing = Protocol(name="aave", official_domain="aave-v3.com")
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = existing

        row = get_or_create_protocol(session, "aave", official_domain="different.com")

        assert row.official_domain == "aave-v3.com"
        session.flush.assert_not_called()

    def test_no_domain_provided_leaves_null_on_new_row(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        row = get_or_create_protocol(session, "some-slug")

        assert row.name == "some-slug"
        assert row.official_domain is None
