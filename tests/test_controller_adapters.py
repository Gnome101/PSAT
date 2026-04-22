"""Tests for services/resolution/controller_adapters.py.

Covers:
  - Pure utility functions (_encode_uint256, _decode_uint256, _decode_bytes32, _topic_address)
  - _eth_call_raw with mocked RPC
  - _try_enumerable_role_members success, zero count, RPC failure
  - _log_sort_key ordering
  - _get_logs with chunking fallback
  - _code_start_block binary search
  - _role_members_from_events grant/revoke replay
  - _try_aragon_acl_role_members
  - _try_aragon_app_details / _try_access_control_details
  - expand_role_identifier_principals top-level dispatch
  - type_authority_contract dispatch
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution.controller_adapters import (
    ANY_ENTITY,
    MAX_ENUMERABLE_ROLE_MEMBERS,
    MAX_LOG_BLOCK_RANGE,
    ROLE_GRANTED_TOPIC0,
    ROLE_REVOKED_TOPIC0,
    SET_PERMISSION_TOPIC0,
    _code_start_block,
    _decode_bytes32,
    _decode_uint256,
    _encode_uint256,
    _eth_call_raw,
    _get_logs,
    _log_sort_key,
    _logs_for_topic,
    _role_members_from_events,
    _topic_address,
    _try_access_control_details,
    _try_aragon_acl_role_members,
    _try_aragon_app_details,
    _try_enumerable_role_members,
    _try_role_holders_members,
    expand_role_identifier_principals,
    type_authority_contract,
)

RPC = "http://localhost:8545"
CONTRACT = "0x" + "ab" * 20
ROLE = "0x" + "cc" * 32


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _addr(n: int) -> str:
    return "0x" + hex(n)[2:].zfill(40)


def _padded_addr(addr: str) -> str:
    """32-byte left-padded address (66 chars total)."""
    return "0x" + "0" * 24 + addr[2:]


def _padded_int(v: int) -> str:
    return "0x" + hex(v)[2:].zfill(64)


# =========================================================================
# 1. Pure utility functions
# =========================================================================


class TestEncodeUint256:
    def test_zero(self):
        assert _encode_uint256(0) == "0" * 64

    def test_one(self):
        result = _encode_uint256(1)
        assert len(result) == 64
        assert result == "0" * 63 + "1"

    def test_large(self):
        result = _encode_uint256(2**256 - 1)
        assert result == "f" * 64

    def test_arbitrary(self):
        result = _encode_uint256(256)
        assert result == "0" * 61 + "100"


class TestDecodeUint256:
    def test_zero_0x(self):
        assert _decode_uint256("0x") == 0

    def test_zero_0x0(self):
        assert _decode_uint256("0x0") == 0

    def test_padded_zero(self):
        assert _decode_uint256("0x" + "0" * 64) == 0

    def test_one(self):
        assert _decode_uint256("0x" + "0" * 63 + "1") == 1

    def test_large(self):
        assert _decode_uint256("0x" + "f" * 64) == 2**256 - 1

    def test_non_hex_fallback(self):
        # normalize_hex returns "0x" for non-hex input -> 0
        assert _decode_uint256("not_hex") == 0


class TestDecodeBytes32:
    def test_valid_bytes32(self):
        val = "0x" + "ab" * 32
        assert _decode_bytes32(val) == val

    def test_too_short(self):
        assert _decode_bytes32("0xab") is None

    def test_too_long(self):
        assert _decode_bytes32("0x" + "ab" * 33) is None

    def test_empty(self):
        assert _decode_bytes32("0x") is None

    def test_non_hex(self):
        assert _decode_bytes32("garbage") is None


class TestTopicAddress:
    def test_valid_address(self):
        addr = "0x" + "ab" * 20
        result = _topic_address(addr)
        assert result.startswith("0x")
        assert len(result) == 66  # 0x + 64 hex chars
        assert result.endswith("ab" * 20)
        assert result[2:26] == "0" * 24

    def test_invalid_length_raises(self):
        with pytest.raises(RuntimeError, match="Invalid address"):
            _topic_address("0xshort")

    def test_non_hex_raises(self):
        with pytest.raises(RuntimeError, match="Invalid address"):
            _topic_address("not_an_address")


# =========================================================================
# 2. _eth_call_raw
# =========================================================================

MOD = "services.resolution.controller_adapters"


class TestEthCallRaw:
    @patch(f"{MOD}._rpc_request")
    def test_success(self, mock_rpc):
        mock_rpc.return_value = "0x" + "0" * 64
        result = _eth_call_raw(RPC, CONTRACT, "0xdeadbeef")
        assert result == "0x" + "0" * 64
        mock_rpc.assert_called_once()

    @patch(f"{MOD}._rpc_request")
    def test_non_string_raises(self, mock_rpc):
        mock_rpc.return_value = 42
        with pytest.raises(RuntimeError, match="Unexpected eth_call result"):
            _eth_call_raw(RPC, CONTRACT, "0xdeadbeef")

    @patch(f"{MOD}._rpc_request")
    def test_no_0x_prefix_raises(self, mock_rpc):
        mock_rpc.return_value = "deadbeef"
        with pytest.raises(RuntimeError, match="Unexpected eth_call result"):
            _eth_call_raw(RPC, CONTRACT, "0xdeadbeef")

    @patch(f"{MOD}._rpc_request")
    def test_none_raises(self, mock_rpc):
        mock_rpc.return_value = None
        with pytest.raises(RuntimeError, match="Unexpected eth_call result"):
            _eth_call_raw(RPC, CONTRACT, "0xdeadbeef")

    @patch(f"{MOD}._rpc_request")
    def test_block_tag_forwarded(self, mock_rpc):
        mock_rpc.return_value = "0x01"
        _eth_call_raw(RPC, CONTRACT, "0xaa", block_tag="0x100")
        args = mock_rpc.call_args
        assert args[0][2] == [{"to": CONTRACT, "data": "0xaa"}, "0x100"]


# =========================================================================
# 3. _try_enumerable_role_members
# =========================================================================


class TestTryEnumerableRoleMembers:
    @patch(f"{MOD}._eth_call_raw")
    def test_success_two_members(self, mock_call):
        addr1 = _addr(1)
        addr2 = _addr(2)

        def side_effect(rpc_url, contract, calldata, block_tag="latest"):
            # First call: getRoleMemberCount -> 2
            if "getRoleMemberCount" in str(calldata) or mock_call.call_count == 1:
                return _padded_int(2)
            # Second call: getRoleMember(role, 0) -> addr1
            if mock_call.call_count == 2:
                return _padded_addr(addr1)
            # Third call: getRoleMember(role, 1) -> addr2
            return _padded_addr(addr2)

        mock_call.side_effect = side_effect

        result = _try_enumerable_role_members(RPC, CONTRACT, ROLE)
        assert result is not None
        members, meta = result
        assert sorted(members) == sorted([addr1.lower(), addr2.lower()])
        assert meta["adapter"] == "access_control_enumerable"
        assert meta["member_count"] == 2

    @patch(f"{MOD}._eth_call_raw")
    def test_zero_count(self, mock_call):
        mock_call.return_value = "0x" + "0" * 64
        result = _try_enumerable_role_members(RPC, CONTRACT, ROLE)
        assert result is not None
        members, meta = result
        assert members == []
        assert meta["member_count"] == 0

    @patch(f"{MOD}._eth_call_raw")
    def test_rpc_failure_returns_none(self, mock_call):
        mock_call.side_effect = RuntimeError("RPC down")
        result = _try_enumerable_role_members(RPC, CONTRACT, ROLE)
        assert result is None

    @patch(f"{MOD}._eth_call_raw")
    def test_count_exceeds_max_returns_none(self, mock_call):
        mock_call.return_value = _padded_int(MAX_ENUMERABLE_ROLE_MEMBERS + 1)
        result = _try_enumerable_role_members(RPC, CONTRACT, ROLE)
        assert result is None

    @patch(f"{MOD}._eth_call_raw")
    def test_member_fetch_failure_returns_none(self, mock_call):
        """If fetching an individual member fails, the whole call returns None."""
        call_count = 0

        def side_effect(rpc_url, contract, calldata, block_tag="latest"):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _padded_int(1)  # count = 1
            raise RuntimeError("member fetch failed")

        mock_call.side_effect = side_effect
        result = _try_enumerable_role_members(RPC, CONTRACT, ROLE)
        assert result is None

    @patch(f"{MOD}._eth_call_raw")
    def test_deduplicates_members(self, mock_call):
        """Duplicate addresses are deduplicated in the result."""
        addr1 = _addr(1)
        call_count = 0

        def side_effect(rpc_url, contract, calldata, block_tag="latest"):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _padded_int(2)  # count = 2
            return _padded_addr(addr1)  # same addr both times

        mock_call.side_effect = side_effect
        result = _try_enumerable_role_members(RPC, CONTRACT, ROLE)
        assert result is not None
        members, meta = result
        assert len(members) == 1
        assert meta["member_count"] == 1


# =========================================================================
# 4. _log_sort_key
# =========================================================================


class TestLogSortKey:
    def test_basic_ordering(self):
        log1 = {"blockNumber": "0x1", "transactionIndex": "0x0", "logIndex": "0x0"}
        log2 = {"blockNumber": "0x1", "transactionIndex": "0x0", "logIndex": "0x1"}
        log3 = {"blockNumber": "0x2", "transactionIndex": "0x0", "logIndex": "0x0"}
        assert _log_sort_key(log1) < _log_sort_key(log2) < _log_sort_key(log3)

    def test_missing_fields_default_zero(self):
        assert _log_sort_key({}) == (0, 0, 0)

    def test_tx_index_matters(self):
        log_a = {"blockNumber": "0x5", "transactionIndex": "0x1", "logIndex": "0x0"}
        log_b = {"blockNumber": "0x5", "transactionIndex": "0x2", "logIndex": "0x0"}
        assert _log_sort_key(log_a) < _log_sort_key(log_b)


# =========================================================================
# 5. _get_logs -- normal + chunked fallback
# =========================================================================


class TestGetLogs:
    @patch(f"{MOD}._rpc_request")
    def test_simple_success(self, mock_rpc):
        mock_rpc.return_value = [{"log": 1}]
        result = _get_logs(RPC, {"address": CONTRACT, "fromBlock": "0x0", "toBlock": "latest"})
        assert result == [{"log": 1}]

    @patch(f"{MOD}._rpc_request")
    def test_non_list_returns_empty(self, mock_rpc):
        mock_rpc.return_value = "unexpected"
        result = _get_logs(RPC, {"address": CONTRACT, "fromBlock": "0x0", "toBlock": "latest"})
        assert result == []

    @patch(f"{MOD}._rpc_request")
    def test_chunked_fallback_with_latest(self, mock_rpc):
        """When max block range error, falls back to chunked fetching."""
        call_count = 0

        def side_effect(rpc_url, method, params):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("maximum block range exceeded")
            if method == "eth_blockNumber":
                return hex(MAX_LOG_BLOCK_RANGE + 100)
            # Return a log for each chunk
            return [{"log": call_count}]

        mock_rpc.side_effect = side_effect
        result = _get_logs(RPC, {"address": CONTRACT, "fromBlock": "0x0", "toBlock": "latest"})
        assert len(result) >= 1

    @patch(f"{MOD}._rpc_request")
    def test_chunked_fallback_with_explicit_to_block(self, mock_rpc):
        """Chunked fallback when toBlock is an explicit hex number."""
        call_count = 0

        def side_effect(rpc_url, method, params):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("maximum block range exceeded")
            return [{"log": call_count}]

        mock_rpc.side_effect = side_effect
        to_block = hex(MAX_LOG_BLOCK_RANGE + 50)
        result = _get_logs(RPC, {"address": CONTRACT, "fromBlock": "0x0", "toBlock": to_block})
        assert len(result) >= 1

    @patch(f"{MOD}._rpc_request")
    def test_non_range_error_reraises(self, mock_rpc):
        mock_rpc.side_effect = RuntimeError("some other error")
        with pytest.raises(RuntimeError, match="some other error"):
            _get_logs(RPC, {"address": CONTRACT, "fromBlock": "0x0", "toBlock": "latest"})

    @patch(f"{MOD}._rpc_request")
    def test_chunked_non_list_chunks_skipped(self, mock_rpc):
        """If a chunk returns non-list, it's silently skipped."""
        call_count = 0

        def side_effect(rpc_url, method, params):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("maximum block range exceeded")
            if method == "eth_blockNumber":
                return hex(10)
            return "not_a_list"  # non-list result for the chunk

        mock_rpc.side_effect = side_effect
        result = _get_logs(RPC, {"address": CONTRACT, "fromBlock": "0x0", "toBlock": "latest"})
        assert result == []

    @patch(f"{MOD}._rpc_request")
    def test_bad_block_number_in_chunked_raises(self, mock_rpc):
        """If eth_blockNumber returns garbage in chunked path, raises."""
        call_count = 0

        def side_effect(rpc_url, method, params):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("maximum block range exceeded")
            if method == "eth_blockNumber":
                return 42  # not a string
            return []

        mock_rpc.side_effect = side_effect
        with pytest.raises(RuntimeError, match="Unexpected eth_blockNumber"):
            _get_logs(RPC, {"address": CONTRACT, "fromBlock": "0x0", "toBlock": "latest"})


# =========================================================================
# 6. _code_start_block binary search
# =========================================================================


class TestCodeStartBlock:
    def setup_method(self):
        # Clear the LRU cache before each test
        _code_start_block.cache_clear()

    @patch(f"{MOD}._get_code_at_block")
    @patch(f"{MOD}._rpc_request")
    def test_contract_deployed_at_block_50(self, mock_rpc, mock_get_code):
        mock_rpc.return_value = "0x64"  # block 100

        def code_at(rpc_url, addr, block):
            return "0xdeadbeef" if block >= 50 else "0x"

        mock_get_code.side_effect = code_at
        result = _code_start_block(RPC, CONTRACT)
        assert result == 50

    @patch(f"{MOD}._get_code_at_block")
    @patch(f"{MOD}._rpc_request")
    def test_no_code_at_latest_returns_zero(self, mock_rpc, mock_get_code):
        mock_rpc.return_value = "0x64"  # block 100
        mock_get_code.return_value = "0x"
        result = _code_start_block(RPC, CONTRACT)
        assert result == 0

    @patch(f"{MOD}._rpc_request")
    def test_bad_block_number_returns_zero(self, mock_rpc):
        mock_rpc.return_value = 999  # not a string
        result = _code_start_block(RPC, CONTRACT)
        assert result == 0

    @patch(f"{MOD}._get_code_at_block")
    @patch(f"{MOD}._rpc_request")
    def test_code_at_block_zero(self, mock_rpc, mock_get_code):
        mock_rpc.return_value = "0x64"  # block 100
        mock_get_code.return_value = "0xcode"  # code everywhere
        result = _code_start_block(RPC, CONTRACT)
        assert result == 0

    @patch(f"{MOD}._get_code_at_block")
    @patch(f"{MOD}._rpc_request")
    def test_explicit_block_tag(self, mock_rpc, mock_get_code):
        mock_rpc.return_value = "0x64"  # doesn't matter, explicit tag used

        def code_at(rpc_url, addr, block):
            return "0xdeadbeef" if block >= 10 else "0x"

        mock_get_code.side_effect = code_at
        result = _code_start_block(RPC, _addr(99), block_tag="0x20")  # block 32
        assert result == 10


# =========================================================================
# 7. _role_members_from_events
# =========================================================================


class TestRoleMembersFromEvents:
    @patch(f"{MOD}._logs_for_topic")
    def test_grant_and_revoke(self, mock_logs):
        addr1 = _addr(1)
        addr2 = _addr(2)

        granted = [
            {
                "topics": [ROLE_GRANTED_TOPIC0, ROLE, _padded_addr(addr1)],
                "blockNumber": "0x1",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
            {
                "topics": [ROLE_GRANTED_TOPIC0, ROLE, _padded_addr(addr2)],
                "blockNumber": "0x2",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
        ]
        revoked = [
            {
                "topics": [ROLE_REVOKED_TOPIC0, ROLE, _padded_addr(addr1)],
                "blockNumber": "0x3",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
        ]

        mock_logs.side_effect = [granted, revoked]
        result = _role_members_from_events(RPC, CONTRACT, ROLE)
        assert result is not None
        members, meta = result
        assert addr1.lower() not in members  # revoked
        assert addr2.lower() in members
        assert meta["adapter"] == "access_control_events"
        assert meta["granted_events"] == 2
        assert meta["revoked_events"] == 1

    @patch(f"{MOD}._logs_for_topic")
    def test_no_events_returns_none(self, mock_logs):
        mock_logs.side_effect = [[], []]
        result = _role_members_from_events(RPC, CONTRACT, ROLE)
        assert result is None

    @patch(f"{MOD}._logs_for_topic")
    def test_exception_returns_none(self, mock_logs):
        mock_logs.side_effect = RuntimeError("network error")
        result = _role_members_from_events(RPC, CONTRACT, ROLE)
        assert result is None

    @patch(f"{MOD}._logs_for_topic")
    def test_log_with_too_few_topics_skipped(self, mock_logs):
        """Logs with fewer than 3 topics are silently skipped."""
        granted = [
            {
                "topics": [ROLE_GRANTED_TOPIC0, ROLE],  # only 2 topics
                "blockNumber": "0x1",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
        ]
        mock_logs.side_effect = [granted, []]
        result = _role_members_from_events(RPC, CONTRACT, ROLE)
        assert result is not None
        members, meta = result
        assert members == []


# =========================================================================
# 8. _try_aragon_acl_role_members
# =========================================================================


class TestTryAragonAclRoleMembers:
    @patch(f"{MOD}._aragon_permission_logs")
    @patch(f"{MOD}._eth_call_raw")
    def test_success_with_members(self, mock_call, mock_perm_logs):
        kernel = _addr(10)
        acl = _addr(11)
        member = _addr(20)

        mock_call.side_effect = [
            _padded_addr(kernel),  # kernel()
            _padded_addr(acl),  # acl()
        ]

        mock_perm_logs.return_value = [
            {
                "topics": [
                    SET_PERMISSION_TOPIC0,
                    _padded_addr(member),  # entity
                    _padded_addr(CONTRACT),  # app
                    ROLE,  # role
                ],
                "data": _padded_int(1),  # allowed = true
                "blockNumber": "0x1",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
        ]

        result = _try_aragon_acl_role_members(RPC, CONTRACT, ROLE)
        assert result is not None
        members, meta = result
        assert member.lower() in members
        assert meta["adapter"] == "aragon_acl"
        assert meta["kernel"] == kernel
        assert meta["acl"] == acl

    @patch(f"{MOD}._eth_call_raw")
    def test_no_kernel_zero_address(self, mock_call):
        """Zero-padded address is still decoded as a valid address, so this proceeds."""
        mock_call.return_value = "0x" + "0" * 64  # decode_address returns "0x" + "0"*40
        # This won't return None because decode_address("0x" + "0"*64) = "0x" + "0"*40 (truthy)
        # It will try acl() next and get same zero address; then proceed to logs.
        # We just verify it doesn't crash.
        _try_aragon_acl_role_members(RPC, CONTRACT, ROLE)

    @patch(f"{MOD}._eth_call_raw")
    def test_rpc_failure_returns_none(self, mock_call):
        mock_call.side_effect = RuntimeError("failed")
        result = _try_aragon_acl_role_members(RPC, CONTRACT, ROLE)
        assert result is None

    @patch(f"{MOD}._aragon_permission_logs")
    @patch(f"{MOD}._eth_call_raw")
    def test_any_entity_flagged(self, mock_call, mock_perm_logs):
        kernel = _addr(10)
        acl = _addr(11)

        mock_call.side_effect = [
            _padded_addr(kernel),
            _padded_addr(acl),
        ]

        mock_perm_logs.return_value = [
            {
                "topics": [
                    SET_PERMISSION_TOPIC0,
                    _padded_addr(ANY_ENTITY),  # ANY_ENTITY
                    _padded_addr(CONTRACT),
                    ROLE,
                ],
                "data": _padded_int(1),
                "blockNumber": "0x1",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
        ]

        result = _try_aragon_acl_role_members(RPC, CONTRACT, ROLE)
        assert result is not None
        members, meta = result
        assert meta["public_any_entity"] is True
        assert ANY_ENTITY.lower() not in members  # ANY_ENTITY not added to members list

    @patch(f"{MOD}._aragon_permission_logs")
    @patch(f"{MOD}._eth_call_raw")
    def test_revoke_removes_member(self, mock_call, mock_perm_logs):
        kernel = _addr(10)
        acl = _addr(11)
        member = _addr(20)

        mock_call.side_effect = [
            _padded_addr(kernel),
            _padded_addr(acl),
        ]

        mock_perm_logs.return_value = [
            {
                "topics": [SET_PERMISSION_TOPIC0, _padded_addr(member), _padded_addr(CONTRACT), ROLE],
                "data": _padded_int(1),
                "blockNumber": "0x1",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
            {
                "topics": [SET_PERMISSION_TOPIC0, _padded_addr(member), _padded_addr(CONTRACT), ROLE],
                "data": _padded_int(0),  # revoke
                "blockNumber": "0x2",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
        ]

        result = _try_aragon_acl_role_members(RPC, CONTRACT, ROLE)
        assert result is not None
        members, meta = result
        assert member.lower() not in members
        assert meta["member_count"] == 0

    @patch(f"{MOD}._aragon_permission_logs")
    @patch(f"{MOD}._eth_call_raw")
    def test_log_with_too_few_topics_skipped(self, mock_call, mock_perm_logs):
        kernel = _addr(10)
        acl = _addr(11)

        mock_call.side_effect = [_padded_addr(kernel), _padded_addr(acl)]
        mock_perm_logs.return_value = [
            {
                "topics": [SET_PERMISSION_TOPIC0, _padded_addr(_addr(20))],  # only 2 topics, need 4
                "data": _padded_int(1),
                "blockNumber": "0x1",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
        ]

        result = _try_aragon_acl_role_members(RPC, CONTRACT, ROLE)
        assert result is not None
        members, meta = result
        assert members == []

    @patch(f"{MOD}._aragon_permission_logs")
    @patch(f"{MOD}._eth_call_raw")
    def test_wrong_role_filtered(self, mock_call, mock_perm_logs):
        kernel = _addr(10)
        acl = _addr(11)
        other_role = "0x" + "dd" * 32

        mock_call.side_effect = [_padded_addr(kernel), _padded_addr(acl)]
        mock_perm_logs.return_value = [
            {
                "topics": [SET_PERMISSION_TOPIC0, _padded_addr(_addr(20)), _padded_addr(CONTRACT), other_role],
                "data": _padded_int(1),
                "blockNumber": "0x1",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
        ]

        result = _try_aragon_acl_role_members(RPC, CONTRACT, ROLE)
        assert result is not None
        members, meta = result
        assert members == []


# =========================================================================
# 9. _try_aragon_app_details / _try_access_control_details
# =========================================================================


class TestTryAragonAppDetails:
    @patch(f"{MOD}._eth_call_raw")
    def test_success(self, mock_call):
        kernel = _addr(10)
        acl = _addr(11)
        app_id = "0x" + "ff" * 32

        mock_call.side_effect = [
            _padded_addr(kernel),  # kernel()
            app_id,  # appId() -- already 66 chars
            _padded_addr(acl),  # acl()
        ]

        result = _try_aragon_app_details(RPC, CONTRACT)
        assert result is not None
        assert result["authority_kind"] == "aragon_app_like"
        assert result["kernel"] is not None
        assert result["app_id"] == app_id

    @patch(f"{MOD}._eth_call_raw")
    def test_no_kernel_returns_none(self, mock_call):
        # decode_address returns None for non-66 char values
        mock_call.return_value = "0x"
        result = _try_aragon_app_details(RPC, CONTRACT)
        assert result is None

    @patch(f"{MOD}._eth_call_raw")
    def test_rpc_error_returns_none(self, mock_call):
        mock_call.side_effect = RuntimeError("fail")
        result = _try_aragon_app_details(RPC, CONTRACT)
        assert result is None

    @patch(f"{MOD}._eth_call_raw")
    def test_no_app_id_returns_none(self, mock_call):
        kernel = _addr(10)
        mock_call.side_effect = [
            _padded_addr(kernel),  # kernel()
            "0xshort",  # appId() -> not 66 chars -> decode_bytes32 returns None
        ]
        result = _try_aragon_app_details(RPC, CONTRACT)
        assert result is None


class TestTryAccessControlDetails:
    @patch(f"{MOD}._eth_call_raw")
    def test_enumerable_like(self, mock_call):
        mock_call.return_value = _padded_int(3)  # getRoleMemberCount -> 3
        result = _try_access_control_details(RPC, CONTRACT)
        assert result is not None
        assert result["authority_kind"] == "access_control_enumerable_like"
        assert result["default_admin_member_count"] == 3

    @patch(f"{MOD}._eth_call_raw")
    def test_fallback_to_has_role(self, mock_call):
        call_count = 0

        def side_effect(rpc_url, contract, calldata, block_tag="latest"):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("no getRoleMemberCount")
            return "0x" + "0" * 64  # hasRole succeeds

        mock_call.side_effect = side_effect
        result = _try_access_control_details(RPC, CONTRACT)
        assert result is not None
        assert result["authority_kind"] == "access_control_like"

    @patch(f"{MOD}._eth_call_raw")
    def test_both_fail_returns_none(self, mock_call):
        mock_call.side_effect = RuntimeError("nothing works")
        result = _try_access_control_details(RPC, CONTRACT)
        assert result is None


class TestTryRoleHoldersMembers:
    @patch(f"{MOD}._eth_call_raw")
    @patch(f"{MOD}._decode_abi_value")
    def test_success(self, mock_decode, mock_call):
        a1 = _addr(1)
        a2 = _addr(2)
        mock_call.return_value = "0xdeadbeef"
        mock_decode.return_value = [a1, a2]

        members, meta = _try_role_holders_members(RPC, CONTRACT, ROLE)

        assert members == [a1.lower(), a2.lower()]
        assert meta["adapter"] == "role_holders"
        assert meta["member_count"] == 2

    @patch(f"{MOD}._eth_call_raw")
    def test_failure_returns_none(self, mock_call):
        mock_call.side_effect = RuntimeError("no roleHolders")
        assert _try_role_holders_members(RPC, CONTRACT, ROLE) is None


# =========================================================================
# 10. expand_role_identifier_principals (top-level dispatcher)
# =========================================================================


class TestExpandRoleIdentifierPrincipals:
    def test_invalid_role_id(self):
        members, meta = expand_role_identifier_principals(RPC, CONTRACT, "0xshort")
        assert members == []
        assert meta["reason"] == "role_id_not_bytes32"

    @patch(f"{MOD}._try_aragon_acl_role_members")
    @patch(f"{MOD}._role_members_from_events")
    @patch(f"{MOD}._try_enumerable_role_members")
    @patch(f"{MOD}._try_role_holders_members")
    def test_role_holders_wins(self, mock_role_holders, mock_enum, mock_events, mock_aragon):
        addr = _addr(5)
        mock_role_holders.return_value = ([addr.lower()], {"adapter": "role_holders", "member_count": 1})

        members, meta = expand_role_identifier_principals(RPC, CONTRACT, ROLE)

        assert members == [addr.lower()]
        assert meta["adapter"] == "role_holders"
        mock_enum.assert_not_called()
        mock_events.assert_not_called()
        mock_aragon.assert_not_called()

    @patch(f"{MOD}._try_aragon_acl_role_members")
    @patch(f"{MOD}._role_members_from_events")
    @patch(f"{MOD}._try_enumerable_role_members")
    @patch(f"{MOD}._try_role_holders_members")
    def test_enumerable_wins(self, mock_role_holders, mock_enum, mock_events, mock_aragon):
        addr = _addr(1)
        mock_role_holders.return_value = None
        mock_enum.return_value = ([addr.lower()], {"adapter": "access_control_enumerable", "member_count": 1})
        members, meta = expand_role_identifier_principals(RPC, CONTRACT, ROLE)
        assert members == [addr.lower()]
        assert meta["adapter"] == "access_control_enumerable"
        mock_events.assert_not_called()
        mock_aragon.assert_not_called()

    @patch(f"{MOD}._try_aragon_acl_role_members")
    @patch(f"{MOD}._role_members_from_events")
    @patch(f"{MOD}._try_enumerable_role_members")
    @patch(f"{MOD}._try_role_holders_members")
    def test_events_fallback(self, mock_role_holders, mock_enum, mock_events, mock_aragon):
        addr = _addr(2)
        mock_role_holders.return_value = None
        mock_enum.return_value = None
        mock_events.return_value = ([addr.lower()], {"adapter": "access_control_events", "member_count": 1})
        members, meta = expand_role_identifier_principals(RPC, CONTRACT, ROLE)
        assert members == [addr.lower()]
        assert meta["adapter"] == "access_control_events"
        mock_aragon.assert_not_called()

    @patch(f"{MOD}._try_aragon_acl_role_members")
    @patch(f"{MOD}._role_members_from_events")
    @patch(f"{MOD}._try_enumerable_role_members")
    @patch(f"{MOD}._try_role_holders_members")
    def test_aragon_fallback(self, mock_role_holders, mock_enum, mock_events, mock_aragon):
        addr = _addr(3)
        mock_role_holders.return_value = None
        mock_enum.return_value = None
        mock_events.return_value = None
        mock_aragon.return_value = ([addr.lower()], {"adapter": "aragon_acl", "member_count": 1})
        members, meta = expand_role_identifier_principals(RPC, CONTRACT, ROLE)
        assert members == [addr.lower()]
        assert meta["adapter"] == "aragon_acl"

    @patch(f"{MOD}._try_aragon_acl_role_members")
    @patch(f"{MOD}._role_members_from_events")
    @patch(f"{MOD}._try_enumerable_role_members")
    @patch(f"{MOD}._try_role_holders_members")
    def test_no_adapter_matched(self, mock_role_holders, mock_enum, mock_events, mock_aragon):
        mock_role_holders.return_value = None
        mock_enum.return_value = None
        mock_events.return_value = None
        mock_aragon.return_value = None
        members, meta = expand_role_identifier_principals(RPC, CONTRACT, ROLE)
        assert members == []
        assert meta["reason"] == "no_role_adapter_matched"


# =========================================================================
# 11. type_authority_contract
# =========================================================================


class TestTypeAuthorityContract:
    @patch(f"{MOD}._try_access_control_details")
    @patch(f"{MOD}._try_aragon_app_details")
    def test_aragon_wins(self, mock_aragon, mock_ac):
        mock_aragon.return_value = {"authority_kind": "aragon_app_like"}
        result = type_authority_contract(RPC, CONTRACT)
        assert result["authority_kind"] == "aragon_app_like"
        mock_ac.assert_not_called()

    @patch(f"{MOD}._try_access_control_details")
    @patch(f"{MOD}._try_aragon_app_details")
    def test_access_control_fallback(self, mock_aragon, mock_ac):
        mock_aragon.return_value = None
        mock_ac.return_value = {"authority_kind": "access_control_like"}
        result = type_authority_contract(RPC, CONTRACT)
        assert result["authority_kind"] == "access_control_like"

    @patch(f"{MOD}._try_access_control_details")
    @patch(f"{MOD}._try_aragon_app_details")
    def test_no_match_returns_empty(self, mock_aragon, mock_ac):
        mock_aragon.return_value = None
        mock_ac.return_value = None
        result = type_authority_contract(RPC, CONTRACT)
        assert result == {}


# =========================================================================
# 12. Module-level constants sanity checks
# =========================================================================


class TestConstants:
    def test_role_granted_topic0_format(self):
        assert ROLE_GRANTED_TOPIC0.startswith("0x")
        assert len(ROLE_GRANTED_TOPIC0) == 66

    def test_role_revoked_topic0_format(self):
        assert ROLE_REVOKED_TOPIC0.startswith("0x")
        assert len(ROLE_REVOKED_TOPIC0) == 66

    def test_set_permission_topic0_format(self):
        assert SET_PERMISSION_TOPIC0.startswith("0x")
        assert len(SET_PERMISSION_TOPIC0) == 66

    def test_any_entity(self):
        assert ANY_ENTITY == "0x" + "f" * 40

    def test_max_enumerable(self):
        assert MAX_ENUMERABLE_ROLE_MEMBERS == 256


# =========================================================================
# 13. _logs_for_topic (covers lines 108-109)
# =========================================================================


class TestLogsForTopic:
    def setup_method(self):
        _code_start_block.cache_clear()

    @patch(f"{MOD}._get_logs")
    @patch(f"{MOD}._code_start_block")
    def test_delegates_to_get_logs(self, mock_start, mock_logs):
        mock_start.return_value = 100
        mock_logs.return_value = [{"log": 1}]
        result = _logs_for_topic(RPC, CONTRACT, ROLE_GRANTED_TOPIC0, ROLE)
        assert result == [{"log": 1}]
        mock_logs.assert_called_once()
        call_args = mock_logs.call_args[0]
        assert call_args[1]["fromBlock"] == hex(100)


# =========================================================================
# 14. _role_members_from_events -- null member topic (covers line 210)
# =========================================================================


class TestRoleMembersFromEventsNullMember:
    @patch(f"{MOD}._logs_for_topic")
    def test_null_member_topic_skipped(self, mock_logs):
        """When _topic_to_address returns None (short topic), the log entry is skipped."""
        granted = [
            {
                "topics": [ROLE_GRANTED_TOPIC0, ROLE, "0xshort"],  # not 66 chars -> decode_address returns None
                "blockNumber": "0x1",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
        ]
        mock_logs.side_effect = [granted, []]
        result = _role_members_from_events(RPC, CONTRACT, ROLE)
        assert result is not None
        members, meta = result
        assert members == []
        assert meta["member_count"] == 0


# =========================================================================
# 15. _try_aragon_acl_role_members -- kernel/acl None paths (lines 233-234, 237-238)
#     and entity None path (line 252-253)
# =========================================================================


class TestTryAragonAclEdgeCases:
    @patch(f"{MOD}._eth_call_raw")
    def test_kernel_returns_none(self, mock_call):
        """When kernel() returns value that decode_address maps to None."""
        mock_call.return_value = "0xshort"  # decode_address returns None for non-66 char
        result = _try_aragon_acl_role_members(RPC, CONTRACT, ROLE)
        assert result is None

    @patch(f"{MOD}._eth_call_raw")
    def test_acl_returns_none(self, mock_call):
        """When acl() returns value that decode_address maps to None."""
        kernel = _addr(10)
        call_count = 0

        def side_effect(rpc_url, contract, calldata, block_tag="latest"):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _padded_addr(kernel)  # kernel() succeeds
            return "0xshort"  # acl() -> decode_address returns None

        mock_call.side_effect = side_effect
        result = _try_aragon_acl_role_members(RPC, CONTRACT, ROLE)
        assert result is None

    @patch(f"{MOD}._aragon_permission_logs")
    @patch(f"{MOD}._eth_call_raw")
    def test_entity_topic_null_skipped(self, mock_call, mock_perm_logs):
        """When entity topic cannot be decoded, the log is skipped (line 252-253)."""
        kernel = _addr(10)
        acl = _addr(11)

        mock_call.side_effect = [_padded_addr(kernel), _padded_addr(acl)]
        mock_perm_logs.return_value = [
            {
                "topics": [
                    SET_PERMISSION_TOPIC0,
                    "0xshort",  # entity topic -> decode_address returns None
                    _padded_addr(CONTRACT),
                    ROLE,
                ],
                "data": _padded_int(1),
                "blockNumber": "0x1",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
        ]

        result = _try_aragon_acl_role_members(RPC, CONTRACT, ROLE)
        assert result is not None
        members, meta = result
        assert members == []
        assert meta["member_count"] == 0
