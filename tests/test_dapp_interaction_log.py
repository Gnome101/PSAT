"""Tests for the interaction capture log."""

import tempfile
from pathlib import Path

from services.crawlers.dapp.interaction_log import InteractionLog


def test_add_transaction():
    log = InteractionLog()
    log.add(
        {
            "type": "sendTransaction",
            "url": "https://evil-dapp.com",
            "timestamp": 1700000000,
            "to": "0xAbC123000000000000000000000000000000dEaD",
            "value": "0x0",
            "data": "0xa9059cbb0000000000000000000000001234",
        }
    )
    assert len(log.interactions) == 1
    assert log.interactions[0].to == "0xAbC123000000000000000000000000000000dEaD"
    assert log.interactions[0].method_selector == "0xa9059cbb"


def test_get_contract_addresses():
    log = InteractionLog()
    log.add({"type": "sendTransaction", "url": "a", "timestamp": 1, "to": "0xAAA"})
    log.add({"type": "sendTransaction", "url": "b", "timestamp": 2, "to": "0xBBB"})
    log.add({"type": "sendTransaction", "url": "c", "timestamp": 3, "to": "0xAAA"})

    addresses = log.get_contract_addresses()
    assert len(addresses) == 2
    assert "0xaaa" in addresses
    assert "0xbbb" in addresses


def test_get_permits():
    log = InteractionLog()
    log.add({"type": "signTypedData", "url": "a", "timestamp": 1, "isPermit": True})
    log.add({"type": "signTypedData", "url": "b", "timestamp": 2, "isPermit": False})

    permits = log.get_permits()
    assert len(permits) == 1


def test_save_and_load():
    log = InteractionLog()
    log.add({"type": "sendTransaction", "url": "x", "timestamp": 1, "to": "0xDEAD"})

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "interactions.json"
        log.save(path)

        loaded = InteractionLog.load(path)
        assert len(loaded.interactions) == 1
        assert loaded.interactions[0].to == "0xDEAD"
