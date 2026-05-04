"""Tests for ``RpcSafeRepo`` (RPC-backed Safe owner+threshold repo).

No live RPC — ``utils.rpc.rpc_request`` is monkeypatched. Covers:

  * happy path: getOwners + getThreshold decode correctly
  * returns lowercase addresses
  * unknown chain_id returns None
  * RPC error returns None (doesn't raise)
  * 0x response (function absent / revert) returns None
  * block parameter is forwarded as hex; default is "latest"
  * the right selectors are used for each call
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution.repos.safe_rpc import RpcSafeRepo  # noqa: E402

# Selectors the production code uses.
_GET_OWNERS = "0xa0e67e2b"
_GET_THRESHOLD = "0xe75235b8"


# ABI-encoded address[] for [a, b]. Layout:
#   offset (0x20)
#   length (2)
#   word(addr1)
#   word(addr2)
def _encode_address_list(addresses: list[str]) -> str:
    out = (32).to_bytes(32, "big")  # offset
    out += len(addresses).to_bytes(32, "big")
    for a in addresses:
        h = a.lower()[2:]
        out += bytes(12) + bytes.fromhex(h)
    return "0x" + out.hex()


def _encode_uint(value: int) -> str:
    return "0x" + value.to_bytes(32, "big").hex()


def test_happy_path_returns_owners_and_threshold(monkeypatch):
    owners = ["0x" + "11" * 20, "0x" + "22" * 20, "0x" + "33" * 20]
    expected_threshold = 2

    def fake_rpc(url, method, params, retries=1):
        assert method == "eth_call"
        call = params[0]
        if call["data"] == _GET_OWNERS:
            return _encode_address_list(owners)
        if call["data"] == _GET_THRESHOLD:
            return _encode_uint(expected_threshold)
        raise AssertionError(f"unexpected selector {call['data']!r}")

    monkeypatch.setattr("services.resolution.repos.safe_rpc.rpc_request", fake_rpc)
    repo = RpcSafeRepo({1: "http://eth.invalid"})
    result = repo.get_owners_threshold(chain_id=1, contract_address="0x" + "ab" * 20)
    assert result is not None
    got_owners, got_threshold = result
    assert got_owners == [a.lower() for a in owners]
    assert got_threshold == expected_threshold


def test_unknown_chain_returns_none():
    repo = RpcSafeRepo({1: "http://eth.invalid"})
    assert repo.get_owners_threshold(chain_id=999, contract_address="0x" + "ab" * 20) is None


def test_rpc_error_returns_none(monkeypatch):
    def fake_rpc(url, method, params, retries=1):
        raise RuntimeError("rate limited")

    monkeypatch.setattr("services.resolution.repos.safe_rpc.rpc_request", fake_rpc)
    repo = RpcSafeRepo({1: "http://eth.invalid"})
    assert repo.get_owners_threshold(chain_id=1, contract_address="0x" + "ab" * 20) is None


def test_revert_response_returns_none(monkeypatch):
    """A contract that doesn't implement getOwners() returns ``0x``;
    the repo treats this as 'not a Safe' rather than raising."""

    def fake_rpc(url, method, params, retries=1):
        return "0x"

    monkeypatch.setattr("services.resolution.repos.safe_rpc.rpc_request", fake_rpc)
    repo = RpcSafeRepo({1: "http://eth.invalid"})
    assert repo.get_owners_threshold(chain_id=1, contract_address="0x" + "ab" * 20) is None


def test_block_parameter_forwarded_as_hex(monkeypatch):
    captured: list[str] = []

    def fake_rpc(url, method, params, retries=1):
        captured.append(params[1])
        if params[0]["data"] == _GET_OWNERS:
            return _encode_address_list(["0x" + "11" * 20])
        return _encode_uint(1)

    monkeypatch.setattr("services.resolution.repos.safe_rpc.rpc_request", fake_rpc)
    repo = RpcSafeRepo({1: "http://eth.invalid"})
    repo.get_owners_threshold(chain_id=1, contract_address="0x" + "ab" * 20, block=18_000_000)
    # Both eth_call invocations use the same hex block tag.
    assert captured == [hex(18_000_000), hex(18_000_000)]


def test_block_default_is_latest(monkeypatch):
    captured: list[str] = []

    def fake_rpc(url, method, params, retries=1):
        captured.append(params[1])
        if params[0]["data"] == _GET_OWNERS:
            return _encode_address_list(["0x" + "11" * 20])
        return _encode_uint(1)

    monkeypatch.setattr("services.resolution.repos.safe_rpc.rpc_request", fake_rpc)
    repo = RpcSafeRepo({1: "http://eth.invalid"})
    repo.get_owners_threshold(chain_id=1, contract_address="0x" + "ab" * 20)
    assert captured == ["latest", "latest"]


def test_one_owner_threshold_one(monkeypatch):
    """Single-signer Safe — degenerate but valid configuration."""
    only_owner = "0x" + "ee" * 20

    def fake_rpc(url, method, params, retries=1):
        if params[0]["data"] == _GET_OWNERS:
            return _encode_address_list([only_owner])
        return _encode_uint(1)

    monkeypatch.setattr("services.resolution.repos.safe_rpc.rpc_request", fake_rpc)
    repo = RpcSafeRepo({1: "http://eth.invalid"})
    owners, threshold = repo.get_owners_threshold(chain_id=1, contract_address="0x" + "ab" * 20)  # type: ignore[misc]
    assert owners == [only_owner.lower()]
    assert threshold == 1


def test_owners_lowercased(monkeypatch):
    """Owners are returned lowercased so downstream comparisons
    don't need to normalize."""
    mixed_case = ["0x" + "Aa" * 20, "0x" + "Bb" * 20]

    def fake_rpc(url, method, params, retries=1):
        if params[0]["data"] == _GET_OWNERS:
            return _encode_address_list(mixed_case)
        return _encode_uint(2)

    monkeypatch.setattr("services.resolution.repos.safe_rpc.rpc_request", fake_rpc)
    repo = RpcSafeRepo({1: "http://eth.invalid"})
    owners, _ = repo.get_owners_threshold(chain_id=1, contract_address="0x" + "ab" * 20)  # type: ignore[misc]
    assert owners == [a.lower() for a in mixed_case]


def test_correct_selectors_used(monkeypatch):
    """Pin the selectors so a refactor doesn't accidentally swap to
    the wrong methods (a silent functional bug — wrong selector
    just returns 0x and the repo would report None)."""
    selectors_seen: list[str] = []

    def fake_rpc(url, method, params, retries=1):
        selectors_seen.append(params[0]["data"])
        if params[0]["data"] == _GET_OWNERS:
            return _encode_address_list(["0x" + "11" * 20])
        return _encode_uint(1)

    monkeypatch.setattr("services.resolution.repos.safe_rpc.rpc_request", fake_rpc)
    repo = RpcSafeRepo({1: "http://eth.invalid"})
    repo.get_owners_threshold(chain_id=1, contract_address="0x" + "ab" * 20)
    assert sorted(selectors_seen) == sorted([_GET_OWNERS, _GET_THRESHOLD])


def test_partial_failure_returns_none(monkeypatch):
    """If getOwners succeeds but getThreshold returns 0x, the repo
    returns None — partial state isn't actionable for the Safe
    adapter."""
    call_count = {"i": 0}

    def fake_rpc(url, method, params, retries=1):
        call_count["i"] += 1
        if params[0]["data"] == _GET_OWNERS:
            return _encode_address_list(["0x" + "11" * 20])
        return "0x"  # threshold call returns empty

    monkeypatch.setattr("services.resolution.repos.safe_rpc.rpc_request", fake_rpc)
    repo = RpcSafeRepo({1: "http://eth.invalid"})
    assert repo.get_owners_threshold(chain_id=1, contract_address="0x" + "ab" * 20) is None
