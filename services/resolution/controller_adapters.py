"""Semantic controller adapters for expanding runtime principals."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import requests
from eth_utils.crypto import keccak

JSON_RPC_TIMEOUT_SECONDS = 10
MAX_ENUMERABLE_ROLE_MEMBERS = 256
ANY_ENTITY = "0xffffffffffffffffffffffffffffffffffffffff"
MAX_LOG_BLOCK_RANGE = 50000


def _rpc_request(rpc_url: str, method: str, params: list[Any]) -> Any:
    response = requests.post(
        rpc_url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=JSON_RPC_TIMEOUT_SECONDS,
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError(str(payload["error"]))
    return payload.get("result")


def _selector(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()[:8]


def _normalize_hex(value: str | None) -> str:
    if not isinstance(value, str) or not value.startswith("0x"):
        return "0x"
    return value.lower()


def _encode_uint256(value: int) -> str:
    return hex(value)[2:].zfill(64)


def _eth_call_raw(rpc_url: str, contract_address: str, calldata: str, block_tag: str = "latest") -> str:
    call = {"to": contract_address, "data": calldata}
    raw = _rpc_request(rpc_url, "eth_call", [call, block_tag])
    if not isinstance(raw, str) or not raw.startswith("0x"):
        raise RuntimeError(f"Unexpected eth_call result: {raw!r}")
    return raw


def _decode_uint256(raw_value: str) -> int:
    normalized = _normalize_hex(raw_value)
    if normalized in {"0x", "0x0"}:
        return 0
    return int(normalized, 16)


def _decode_address(raw_value: str) -> str | None:
    normalized = _normalize_hex(raw_value)
    if len(normalized) != 66:
        return None
    return "0x" + normalized[-40:]


def _decode_bytes32(raw_value: str) -> str | None:
    normalized = _normalize_hex(raw_value)
    if len(normalized) != 66:
        return None
    return normalized


def _topic_to_address(raw_value: str) -> str | None:
    normalized = _normalize_hex(raw_value)
    if len(normalized) != 66:
        return None
    return "0x" + normalized[-40:]


ROLE_GRANTED_TOPIC0 = "0x" + keccak(text="RoleGranted(bytes32,address,address)").hex()
ROLE_REVOKED_TOPIC0 = "0x" + keccak(text="RoleRevoked(bytes32,address,address)").hex()
SET_PERMISSION_TOPIC0 = "0x" + keccak(text="SetPermission(address,address,bytes32,bool)").hex()


def _topic_address(value: str) -> str:
    normalized = _normalize_hex(value)
    if len(normalized) != 42:
        raise RuntimeError(f"Invalid address for topic encoding: {value}")
    return "0x" + "0" * 24 + normalized[2:]


def _try_enumerable_role_members(
    rpc_url: str,
    contract_address: str,
    role_id: str,
    block_tag: str = "latest",
) -> tuple[list[str], dict[str, object]] | None:
    role_arg = _normalize_hex(role_id)[2:].zfill(64)
    count_calldata = _selector("getRoleMemberCount(bytes32)") + role_arg
    try:
        count_raw = _eth_call_raw(rpc_url, contract_address, count_calldata, block_tag)
        count = _decode_uint256(count_raw)
    except Exception:
        return None

    if count > MAX_ENUMERABLE_ROLE_MEMBERS:
        return None

    members: list[str] = []
    member_selector = _selector("getRoleMember(bytes32,uint256)")
    for index in range(count):
        calldata = member_selector + role_arg + _encode_uint256(index)
        try:
            member_raw = _eth_call_raw(rpc_url, contract_address, calldata, block_tag)
        except Exception:
            return None
        member = _decode_address(member_raw)
        if member:
            members.append(member.lower())

    return sorted(set(members)), {"adapter": "access_control_enumerable", "member_count": len(set(members))}


def _logs_for_topic(
    rpc_url: str,
    contract_address: str,
    topic0: str,
    role_id: str,
    block_tag: str = "latest",
) -> list[dict[str, Any]]:
    from_block = _code_start_block(rpc_url, contract_address, block_tag)
    return _get_logs(
        rpc_url,
        {
            "address": contract_address,
            "fromBlock": hex(from_block),
            "toBlock": block_tag,
            "topics": [topic0, _normalize_hex(role_id)],
        },
    )


def _get_logs(rpc_url: str, filter_params: dict[str, Any]) -> list[dict[str, Any]]:
    params = [filter_params]
    try:
        result = _rpc_request(rpc_url, "eth_getLogs", params)
        return result if isinstance(result, list) else []
    except RuntimeError as exc:
        if "maximum block range" not in str(exc):
            raise

    to_block_raw = filter_params.get("toBlock", "latest")
    if to_block_raw == "latest":
        current = _rpc_request(rpc_url, "eth_blockNumber", [])
        if not isinstance(current, str) or not current.startswith("0x"):
            raise RuntimeError(f"Unexpected eth_blockNumber result: {current!r}")
        to_block = int(current, 16)
    else:
        to_block = int(str(to_block_raw), 16)
    from_block = int(str(filter_params.get("fromBlock", "0x0")), 16)

    logs: list[dict[str, Any]] = []
    start = from_block
    while start <= to_block:
        end = min(start + MAX_LOG_BLOCK_RANGE - 1, to_block)
        chunk_filter = dict(filter_params)
        chunk_filter["fromBlock"] = hex(start)
        chunk_filter["toBlock"] = hex(end)
        result = _rpc_request(rpc_url, "eth_getLogs", [chunk_filter])
        if isinstance(result, list):
            logs.extend(result)
        start = end + 1
    return logs


def _get_code_at_block(rpc_url: str, address: str, block_number: int) -> str:
    result = _rpc_request(rpc_url, "eth_getCode", [address, hex(block_number)])
    return _normalize_hex(result if isinstance(result, str) else "0x")


@lru_cache(maxsize=256)
def _code_start_block(rpc_url: str, address: str, block_tag: str = "latest") -> int:
    current = _rpc_request(rpc_url, "eth_blockNumber", [])
    if not isinstance(current, str) or not current.startswith("0x"):
        return 0
    high = int(current, 16) if block_tag == "latest" else int(block_tag, 16)
    if _get_code_at_block(rpc_url, address, high) in {"0x", "0x0"}:
        return 0

    low = 0
    while low < high:
        mid = (low + high) // 2
        if _get_code_at_block(rpc_url, address, mid) in {"0x", "0x0"}:
            low = mid + 1
        else:
            high = mid
    return low


def _log_sort_key(log_entry: dict[str, Any]) -> tuple[int, int, int]:
    block_number = int(str(log_entry.get("blockNumber", "0x0")), 16)
    tx_index = int(str(log_entry.get("transactionIndex", "0x0")), 16)
    log_index = int(str(log_entry.get("logIndex", "0x0")), 16)
    return (block_number, tx_index, log_index)


def _role_members_from_events(
    rpc_url: str,
    contract_address: str,
    role_id: str,
    block_tag: str = "latest",
) -> tuple[list[str], dict[str, object]] | None:
    try:
        granted = _logs_for_topic(rpc_url, contract_address, ROLE_GRANTED_TOPIC0, role_id, block_tag)
        revoked = _logs_for_topic(rpc_url, contract_address, ROLE_REVOKED_TOPIC0, role_id, block_tag)
    except Exception:
        return None

    if not granted and not revoked:
        return None

    members: set[str] = set()
    combined = sorted(
        [("grant", item) for item in granted] + [("revoke", item) for item in revoked],
        key=lambda item: _log_sort_key(item[1]),
    )
    for action, log_entry in combined:
        topics = list(log_entry.get("topics", []))
        if len(topics) < 3:
            continue
        member = _topic_to_address(topics[2])
        if not member:
            continue
        if action == "grant":
            members.add(member.lower())
        else:
            members.discard(member.lower())

    return sorted(members), {
        "adapter": "access_control_events",
        "granted_events": len(granted),
        "revoked_events": len(revoked),
        "member_count": len(members),
    }


def _try_aragon_acl_role_members(
    rpc_url: str,
    contract_address: str,
    role_id: str,
    block_tag: str = "latest",
) -> tuple[list[str], dict[str, object]] | None:
    try:
        kernel_raw = _eth_call_raw(rpc_url, contract_address, _selector("kernel()"), block_tag)
        kernel = _decode_address(kernel_raw)
        if not kernel:
            return None
        acl_raw = _eth_call_raw(rpc_url, kernel, _selector("acl()"), block_tag)
        acl = _decode_address(acl_raw)
        if not acl:
            return None
        logs = _aragon_permission_logs(rpc_url, acl, contract_address, block_tag)
    except Exception:
        return None

    members: set[str] = set()
    public_any_entity = False
    for log_entry in sorted(logs, key=_log_sort_key):
        topics = list(log_entry.get("topics", []))
        if len(topics) < 4:
            continue
        if _normalize_hex(topics[3]) != _normalize_hex(role_id):
            continue
        entity = _topic_to_address(topics[1])
        if not entity:
            continue
        if entity.lower() == ANY_ENTITY:
            public_any_entity = True
            continue
        allowed = bool(_decode_uint256(str(log_entry.get("data", "0x0"))))
        if allowed:
            members.add(entity.lower())
        else:
            members.discard(entity.lower())

    return sorted(members), {
        "adapter": "aragon_acl",
        "kernel": kernel,
        "acl": acl,
        "member_count": len(members),
        "public_any_entity": public_any_entity,
        "permission_events": len(logs),
    }


@lru_cache(maxsize=256)
def _aragon_permission_logs(rpc_url: str, acl: str, contract_address: str, block_tag: str = "latest") -> list[dict[str, Any]]:
    return _get_logs(
        rpc_url,
        {
            "address": acl,
            "fromBlock": hex(_code_start_block(rpc_url, contract_address, block_tag)),
            "toBlock": block_tag,
            "topics": [SET_PERMISSION_TOPIC0, None, _topic_address(contract_address)],
        },
    )


def expand_role_identifier_principals(
    rpc_url: str,
    contract_address: str,
    role_id: str,
    block_tag: str = "latest",
) -> tuple[list[str], dict[str, object]]:
    """Expand a bytes32 role identifier into current member addresses when possible."""
    normalized_role = _normalize_hex(role_id)
    if len(normalized_role) != 66:
        return [], {"adapter": "none", "reason": "role_id_not_bytes32"}

    enumerable = _try_enumerable_role_members(rpc_url, contract_address, normalized_role, block_tag)
    if enumerable is not None:
        return enumerable

    event_backed = _role_members_from_events(rpc_url, contract_address, normalized_role, block_tag)
    if event_backed is not None:
        return event_backed

    aragon_acl = _try_aragon_acl_role_members(rpc_url, contract_address, normalized_role, block_tag)
    if aragon_acl is not None:
        return aragon_acl

    return [], {"adapter": "none", "reason": "no_role_adapter_matched"}


def _try_aragon_app_details(rpc_url: str, address: str, block_tag: str = "latest") -> dict[str, object] | None:
    try:
        kernel_raw = _eth_call_raw(rpc_url, address, _selector("kernel()"), block_tag)
        kernel = _decode_address(kernel_raw)
        if not kernel:
            return None
        app_id_raw = _eth_call_raw(rpc_url, address, _selector("appId()"), block_tag)
        app_id = _decode_bytes32(app_id_raw)
        if not app_id:
            return None
        acl_raw = _eth_call_raw(rpc_url, kernel, _selector("acl()"), block_tag)
        acl = _decode_address(acl_raw)
        return {
            "authority_kind": "aragon_app_like",
            "kernel": kernel,
            "acl": acl,
            "app_id": app_id,
        }
    except Exception:
        return None


def _try_access_control_details(rpc_url: str, address: str, block_tag: str = "latest") -> dict[str, object] | None:
    zero_role = "0x" + "00" * 32
    try:
        count_raw = _eth_call_raw(rpc_url, address, _selector("getRoleMemberCount(bytes32)") + zero_role[2:], block_tag)
        count = _decode_uint256(count_raw)
        return {"authority_kind": "access_control_enumerable_like", "default_admin_member_count": count}
    except Exception:
        pass
    try:
        calldata = _selector("hasRole(bytes32,address)") + zero_role[2:] + ("0" * 24) + ("0" * 40)
        _eth_call_raw(rpc_url, address, calldata, block_tag)
        return {"authority_kind": "access_control_like"}
    except Exception:
        return None


def type_authority_contract(rpc_url: str, address: str, block_tag: str = "latest") -> dict[str, object]:
    aragon = _try_aragon_app_details(rpc_url, address, block_tag)
    if aragon is not None:
        return aragon
    access_control = _try_access_control_details(rpc_url, address, block_tag)
    if access_control is not None:
        return access_control
    return {}
