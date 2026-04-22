"""Controller state snapshot builder and address classifier."""

from __future__ import annotations

from typing import Any

from eth_abi.abi import decode

from schemas.contract_analysis import AssociatedEvent, ControllerReadSpec
from schemas.control_tracking import ControlSnapshot, ControlTrackingPlan
from utils.rpc import (
    normalize_hex as _normalize_hex,
)
from utils.rpc import (
    rpc_request as _rpc_request,
)
from utils.rpc import (
    selector as _selector,
)

from .controller_adapters import expand_role_identifier_principals, type_authority_contract


def _decode_controller_value(raw_value: Any, controller_kind: str) -> str:
    value = _normalize_hex(raw_value if isinstance(raw_value, str) else "0x")
    if controller_kind in {"state_variable", "external_contract"} and len(value) == 66:
        return "0x" + value[-40:]
    return value


def _eth_call_raw(rpc_url: str, contract_address: str, signature: str, block_tag: str = "latest") -> str:
    call = {"to": contract_address, "data": _selector(signature)}
    raw = _rpc_request(rpc_url, "eth_call", [call, block_tag])
    if not isinstance(raw, str) or not raw.startswith("0x"):
        raise RuntimeError(f"Unexpected eth_call result for {signature}: {raw!r}")
    return raw


def _decode_abi_value(raw_value: str, abi_type: str):
    data = bytes.fromhex(_normalize_hex(raw_value)[2:])
    if not data:
        raise RuntimeError("Empty ABI data")
    value = decode([abi_type], data)[0]
    if abi_type == "address":
        return str(value).lower()
    if abi_type == "address[]":
        return [str(item).lower() for item in value]
    return value


def _decode_topic_value(raw_value: str, abi_type: str):
    normalized = _normalize_hex(raw_value)
    if abi_type == "address" and len(normalized) == 66:
        return "0x" + normalized[-40:]
    if abi_type == "bytes4" and len(normalized) == 66:
        return "0x" + normalized[2:10]
    if abi_type.startswith("uint"):
        return int(normalized, 16)
    if abi_type == "bool":
        return bool(int(normalized, 16))
    return normalized


def _try_eth_call_decoded(
    rpc_url: str, contract_address: str, signature: str, abi_type: str, block_tag: str = "latest"
) -> object | None:
    try:
        raw = _eth_call_raw(rpc_url, contract_address, signature, block_tag)
        if _normalize_hex(raw) in {"0x", "0x0"}:
            return None
        return _decode_abi_value(raw, abi_type)
    except Exception:
        return None


def _get_code(rpc_url: str, address: str, block_tag: str = "latest") -> str:
    raw = _rpc_request(rpc_url, "eth_getCode", [address, block_tag])
    return _normalize_hex(raw if isinstance(raw, str) else "0x")


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    raise RuntimeError(f"Unsupported integer value: {value!r}")


def classify_resolved_address(rpc_url: str, address: str, block_tag: str = "latest") -> tuple[str, dict[str, object]]:
    normalized = _normalize_hex(address)
    if normalized == "0x0000000000000000000000000000000000000000":
        return "zero", {"address": normalized}

    code = _get_code(rpc_url, normalized, block_tag)
    if code in {"0x", "0x0"}:
        return "eoa", {"address": normalized}

    safe_owners = _try_eth_call_decoded(rpc_url, normalized, "getOwners()", "address[]", block_tag)
    safe_threshold = _try_eth_call_decoded(rpc_url, normalized, "getThreshold()", "uint256", block_tag)
    if safe_owners is not None and safe_threshold is not None:
        return "safe", {
            "address": normalized,
            "owners": [str(item).lower() for item in safe_owners] if isinstance(safe_owners, list) else [],
            "threshold": _coerce_int(safe_threshold),
        }

    min_delay = _try_eth_call_decoded(rpc_url, normalized, "getMinDelay()", "uint256", block_tag)
    if min_delay is None:
        min_delay = _try_eth_call_decoded(rpc_url, normalized, "delay()", "uint256", block_tag)
    if min_delay is not None:
        owner = _try_eth_call_decoded(rpc_url, normalized, "owner()", "address", block_tag)
        details: dict[str, object] = {"address": normalized, "delay": _coerce_int(min_delay)}
        if owner is not None:
            details["owner"] = owner
        return "timelock", details

    upgrade_interface_version = _try_eth_call_decoded(
        rpc_url, normalized, "UPGRADE_INTERFACE_VERSION()", "string", block_tag
    )
    if upgrade_interface_version is not None:
        owner = _try_eth_call_decoded(rpc_url, normalized, "owner()", "address", block_tag)
        details = {
            "address": normalized,
            "upgrade_interface_version": str(upgrade_interface_version),
        }
        if owner is not None:
            details["owner"] = owner
        return "proxy_admin", details

    details = {"address": normalized}
    details.update(type_authority_contract(rpc_url, normalized, block_tag))
    return "contract", details


def _current_block_number(rpc_url: str) -> int:
    raw = _rpc_request(rpc_url, "eth_blockNumber", [])
    if not isinstance(raw, str) or not raw.startswith("0x"):
        raise RuntimeError(f"Unexpected eth_blockNumber result: {raw!r}")
    return int(raw, 16)


def _read_polling_source(
    rpc_url: str,
    contract_address: str,
    source: str,
    controller_kind: str,
    block_tag: str = "latest",
    read_spec: ControllerReadSpec | None = None,
) -> str:
    target = source
    if isinstance(read_spec, dict) and read_spec.get("strategy") == "getter_call":
        read_target = read_spec.get("target")
        if isinstance(read_target, str) and read_target:
            target = read_target
    raw = _eth_call_raw(rpc_url, contract_address, f"{target}()", block_tag)
    return _decode_controller_value(raw, controller_kind)


def build_control_snapshot(plan: ControlTrackingPlan, rpc_url: str, block_tag: str = "latest") -> ControlSnapshot:
    block_number = _current_block_number(rpc_url) if block_tag == "latest" else int(block_tag, 16)
    controller_values = {}
    # Cache classify_resolved_address results to avoid duplicate RPC calls
    # when multiple controllers resolve to the same address.
    _classification_cache: dict[str, tuple[str, dict[str, object]]] = {}

    def _cached_classify(address: str) -> tuple[str, dict[str, object]]:
        key = _normalize_hex(address)
        if key not in _classification_cache:
            _classification_cache[key] = classify_resolved_address(rpc_url, address, block_tag)
        return _classification_cache[key]

    for controller in plan["tracked_controllers"]:
        source = controller["source"]
        read_spec = controller.get("read_spec")
        try:
            value = _read_polling_source(
                rpc_url,
                plan["contract_address"],
                source,
                controller["kind"],
                block_tag,
                read_spec=read_spec if isinstance(read_spec, dict) else None,
            )
            if controller["kind"] == "role_identifier":
                member_addresses, adapter_meta = expand_role_identifier_principals(
                    rpc_url,
                    plan["contract_address"],
                    value,
                    block_tag,
                )
                resolved_principals = []
                for member_address in member_addresses:
                    resolved_type, details = _cached_classify(member_address)
                    resolved_principals.append(
                        {
                            "address": member_address,
                            "resolved_type": resolved_type,
                            "details": details,
                        }
                    )
                controller_values[controller["controller_id"]] = {
                    "source": source,
                    "value": value,
                    "block_number": block_number,
                    "observed_via": f"eth_call+{adapter_meta.get('adapter', 'none')}",
                    "resolved_type": "unknown",
                    "details": {
                        "source": source,
                        "role_id": value,
                        **adapter_meta,
                        "resolved_principals": resolved_principals,
                    },
                }
                continue
            controller_values[controller["controller_id"]] = {
                "source": source,
                "value": value,
                "block_number": block_number,
                "observed_via": "eth_call",
            }
            resolved_type, details = _cached_classify(value)
            controller_values[controller["controller_id"]]["resolved_type"] = resolved_type
            controller_values[controller["controller_id"]]["details"] = details
        except Exception as exc:
            controller_values[controller["controller_id"]] = {
                "source": source,
                "value": None,
                "block_number": block_number,
                "observed_via": "eth_call_error",
                "resolved_type": "unknown",
                "details": {
                    "source": source,
                    "error": str(exc),
                },
            }
    return {
        "schema_version": "0.1",
        "contract_address": plan["contract_address"],
        "contract_name": plan["contract_name"],
        "block_number": block_number,
        "controller_values": controller_values,
    }


def _decode_event_log_fields(event_ref: AssociatedEvent, log_entry: dict[str, Any]) -> dict[str, Any]:
    topics = list(log_entry.get("topics") or [])
    topic_index = 1
    non_indexed_types = [item["type"] for item in event_ref.get("inputs", []) if not item.get("indexed")]
    non_indexed_values = []
    data = _normalize_hex(log_entry.get("data"))
    if non_indexed_types and data not in {"0x", "0x0"}:
        non_indexed_values = list(decode(non_indexed_types, bytes.fromhex(data[2:])))

    decoded: dict[str, Any] = {}
    non_indexed_index = 0
    for item in event_ref.get("inputs", []):
        name = item.get("name") or item["type"]
        abi_type = item["type"]
        if item.get("indexed"):
            if topic_index >= len(topics):
                break
            decoded[name] = _decode_topic_value(topics[topic_index], abi_type)
            topic_index += 1
            continue

        if non_indexed_index >= len(non_indexed_values):
            break
        value = non_indexed_values[non_indexed_index]
        non_indexed_index += 1
        if abi_type == "address":
            decoded[name] = str(value).lower()
        elif abi_type == "bytes4":
            decoded[name] = "0x" + bytes(value).hex()
        elif abi_type == "bool":
            decoded[name] = bool(value)
        elif abi_type.startswith("uint"):
            decoded[name] = int(value)
        else:
            decoded[name] = value
    return decoded
