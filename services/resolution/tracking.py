"""Controller state snapshot builder and address classifier."""

from __future__ import annotations

import copy
import os
import threading
import time
from typing import Any

from eth_abi.abi import decode

from schemas.contract_analysis import AssociatedEvent, ControllerReadSpec
from schemas.control_tracking import ControlSnapshot, ControlTrackingPlan, TrackedController
from utils.rpc import (
    normalize_hex as _normalize_hex,
)
from utils.rpc import (
    rpc_batch_request_with_status as _rpc_batch_request_with_status,
)
from utils.rpc import (
    rpc_request as _rpc_request,
)
from utils.rpc import (
    selector as _selector,
)

from .controller_adapters import expand_role_identifier_principals, type_authority_contract

# Sentinel: distinguishes "RPC succeeded, function not present" (None) from
# "RPC raised an exception" (transient failure, throttling, network blip).
# Caching the latter would cement a misclassification process-wide.
_PROBE_ERROR = object()

# Process-wide cache for classify_resolved_address. Manual rather than
# functools.lru_cache so we can:
#   - skip caching when any underlying probe errored (lru_cache caches every return)
#   - apply a TTL for `latest`-block entries (otherwise a long-lived worker
#     could serve stale owner/threshold/delay readings indefinitely)
# Keyed on (rpc_url, address.lower(), block_tag). Value is
# (kind, deep-copied details, monotonic insertion time).
_CLASSIFY_CACHE: dict[tuple[str, str, str], tuple[str, dict[str, object], float]] = {}
_CLASSIFY_CACHE_LOCK = threading.Lock()
_CLASSIFY_CACHE_MAX = 4096
# Default TTL: 30 minutes. Longer than a single etherfi-scale cascade
# (~12 min) so sibling jobs share classifications, shorter than a worker
# process lifetime so we eventually re-probe upgraded contracts.
_CLASSIFY_CACHE_TTL_S = float(os.getenv("PSAT_CLASSIFY_CACHE_TTL_S", "1800"))

# Speculative single-batch classify probes (default ON). Saves 5 RTT per
# cold classify by sending all 6 probes in one JSON-RPC roundtrip.
# Whole-batch failure (e.g., a private RPC that rejects batches) falls
# back to the sequential path automatically — see commit 7f07e3e for the
# fallback regression test. Set PSAT_CLASSIFY_BATCH=0 to disable on a
# specific environment if needed.
_CLASSIFY_BATCH_ENABLED = os.getenv("PSAT_CLASSIFY_BATCH", "1").lower() in ("1", "true", "yes")


def clear_classify_cache() -> None:
    """Clear the process-wide classify_resolved_address cache.

    Use in tests, or call between top-level jobs to bound staleness for
    ``latest``-block entries (the worker base loop can call this on each
    new claim if max-correctness is needed).
    """
    with _CLASSIFY_CACHE_LOCK:
        _CLASSIFY_CACHE.clear()


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
    """Returns decoded value, None (function legitimately absent), or the
    `_PROBE_ERROR` sentinel (transient RPC issue — caller should not cache)."""
    try:
        raw = _eth_call_raw(rpc_url, contract_address, signature, block_tag)
        if _normalize_hex(raw) in {"0x", "0x0"}:
            return None
        try:
            return _decode_abi_value(raw, abi_type)
        except Exception:
            # RPC returned data but it didn't decode as the expected type —
            # treat as "function not present" (e.g. revert with reason data).
            return None
    except Exception:
        return _PROBE_ERROR


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


def classify_resolved_address_with_status(
    rpc_url: str, address: str, block_tag: str = "latest"
) -> tuple[str, dict[str, object], bool]:
    """Like ``classify_resolved_address`` but also reports whether the result
    is safe for callers to cache. False if any underlying probe RPC errored
    — caller-owned per-job/per-cascade caches must NOT persist such results,
    otherwise a transient throttle/timeout cements a wrong classification.

    The process-wide cache here already uses this signal internally; this
    helper exposes the same signal to upstream callers (resolve_control_graph
    BFS, principal labeling) that maintain their own short-lived caches.
    """
    normalized = _normalize_hex(address)
    cache_key = (rpc_url, normalized, block_tag)
    now = time.monotonic()

    with _CLASSIFY_CACHE_LOCK:
        cached = _CLASSIFY_CACHE.get(cache_key)
        if cached is not None:
            kind, cached_details, inserted_at = cached
            if now - inserted_at < _CLASSIFY_CACHE_TTL_S:
                # Cached values are by definition cacheable (we only insert
                # error-free results — see below).
                return kind, copy.deepcopy(cached_details), True
            del _CLASSIFY_CACHE[cache_key]

    if _CLASSIFY_BATCH_ENABLED:
        kind, details, had_error = _classify_uncached_batched(rpc_url, normalized, block_tag)
    else:
        kind, details, had_error = _classify_uncached(rpc_url, normalized, block_tag)

    if not had_error:
        with _CLASSIFY_CACHE_LOCK:
            if len(_CLASSIFY_CACHE) >= _CLASSIFY_CACHE_MAX:
                for old_key in list(_CLASSIFY_CACHE.keys())[: _CLASSIFY_CACHE_MAX // 2]:
                    del _CLASSIFY_CACHE[old_key]
            _CLASSIFY_CACHE[cache_key] = (kind, copy.deepcopy(details), now)

    return kind, details, not had_error


def classify_resolved_address(rpc_url: str, address: str, block_tag: str = "latest") -> tuple[str, dict[str, object]]:
    """Backwards-compatible wrapper: see ``classify_resolved_address_with_status``.

    Use this when you don't maintain a downstream cache (the process-wide
    cache here handles freshness/safety automatically). Use the
    ``_with_status`` form when you DO maintain a per-job dict that gets
    persisted as an artifact, so you can skip propagating
    transient-error-driven misclassifications.
    """
    kind, details, _cacheable = classify_resolved_address_with_status(rpc_url, address, block_tag)
    return kind, details


# Probe set used by the batched classifier. Order matters: the dispatch
# logic below indexes this list directly, so any reorder must update the
# unpacking in `_classify_uncached_batched`.
_CLASSIFY_PROBE_SIGS: tuple[tuple[str, str], ...] = (
    ("getOwners()", "address[]"),  # 0: Safe
    ("getThreshold()", "uint256"),  # 1: Safe
    ("getMinDelay()", "uint256"),  # 2: Timelock primary
    ("delay()", "uint256"),  # 3: Timelock fallback
    ("UPGRADE_INTERFACE_VERSION()", "string"),  # 4: ProxyAdmin
    ("owner()", "address"),  # 5: Timelock + ProxyAdmin secondary
)


def _decode_probe_result(raw: object, abi_type: str) -> object | None:
    """Apply the same per-probe decoding logic as ``_try_eth_call_decoded``
    but to a pre-fetched raw result. Returns the decoded value, or None
    when the call legitimately had no data (``"0x"`` / decode failure).

    Caller is responsible for translating ``had_error=True`` into
    ``_PROBE_ERROR`` BEFORE invoking this helper — this function does
    not see the error flag, only the raw result string.
    """
    if not isinstance(raw, str):
        return None
    if _normalize_hex(raw) in {"0x", "0x0"}:
        return None
    try:
        return _decode_abi_value(raw, abi_type)
    except Exception:
        return None


def _batch_probe(rpc_url: str, address: str, block_tag: str) -> list[object]:
    """Fire all 6 classify probes in one JSON-RPC batch.

    Returns a list aligned with ``_CLASSIFY_PROBE_SIGS``: each slot is
    the decoded value, ``None`` (function legitimately absent), or
    ``_PROBE_ERROR`` (per-call RPC error or whole-batch failure).

    Falling back to ``_PROBE_ERROR`` on whole-batch failure preserves the
    sequential path's semantics: caller (``_classify_uncached_batched``)
    sets ``had_error=True`` and skips caching, exactly as the sequential
    path does when ``_eth_call_raw`` raises.
    """
    calls = [
        ("eth_call", [{"to": address, "data": _selector(sig)}, block_tag])
        for sig, _abi in _CLASSIFY_PROBE_SIGS
    ]
    raw_results = _rpc_batch_request_with_status(rpc_url, calls)
    decoded: list[object] = []
    for (raw, had_err), (_sig, abi_type) in zip(raw_results, _CLASSIFY_PROBE_SIGS):
        if had_err:
            decoded.append(_PROBE_ERROR)
            continue
        decoded.append(_decode_probe_result(raw, abi_type))
    return decoded


def _classify_uncached_batched(
    rpc_url: str, normalized: str, block_tag: str
) -> tuple[str, dict[str, object], bool]:
    """Same contract as ``_classify_uncached`` but batches the 6 probes
    upfront. Saves 5 RTT for any contract that falls through to the
    generic-contract branch (most of them, in DeFi).

    Wasteful for early-terminating cases (Safes, Timelocks, ProxyAdmins
    don't need every probe), but the wasted calls are cheap on a single
    socket and providers handle them fine. The bigger cost would be
    rebuilding all this dispatch logic to do a two-phase batch — not
    worth the complexity for a minor RPC bandwidth saving.
    """
    if normalized == "0x0000000000000000000000000000000000000000":
        return "zero", {"address": normalized}, False

    try:
        code = _get_code(rpc_url, normalized, block_tag)
    except Exception:
        return "contract", {"address": normalized}, True
    if code in {"0x", "0x0"}:
        return "eoa", {"address": normalized}, False

    probes = _batch_probe(rpc_url, normalized, block_tag)
    # Whole-batch failure (provider rejects JSON-RPC batches, transport
    # error) marks every slot as _PROBE_ERROR. Without a fallback, the
    # batched classifier would dump out as ("contract", ..., had_error=True)
    # — but the SEQUENTIAL path may well have succeeded on individual
    # eth_calls and produced the right Safe/Timelock/ProxyAdmin
    # classification. Enabling the flag must not silently degrade
    # resolution accuracy on providers that don't support batches.
    if all(p is _PROBE_ERROR for p in probes):
        return _classify_uncached(rpc_url, normalized, block_tag)
    safe_owners_raw, safe_threshold_raw, min_delay_a, min_delay_b, upgrade_iv, owner_raw = probes

    def _ok(v: object) -> object | None:
        """Coerce probe result into the same shape ``_probe()`` returns
        in the sequential path: error → None + sets had_error; legit
        absent → None; success → value."""
        if v is _PROBE_ERROR:
            return None
        return v

    had_error = any(p is _PROBE_ERROR for p in probes)
    safe_owners = _ok(safe_owners_raw)
    safe_threshold = _ok(safe_threshold_raw)
    if safe_owners is not None and safe_threshold is not None:
        return (
            "safe",
            {
                "address": normalized,
                "owners": [str(item).lower() for item in safe_owners] if isinstance(safe_owners, list) else [],
                "threshold": _coerce_int(safe_threshold),
            },
            had_error,
        )

    min_delay = _ok(min_delay_a)
    if min_delay is None:
        min_delay = _ok(min_delay_b)
    if min_delay is not None:
        owner = _ok(owner_raw)
        details: dict[str, object] = {"address": normalized, "delay": _coerce_int(min_delay)}
        if owner is not None:
            details["owner"] = owner
        return "timelock", details, had_error

    upgrade_interface_version = _ok(upgrade_iv)
    if upgrade_interface_version is not None:
        owner = _ok(owner_raw)
        details = {
            "address": normalized,
            "upgrade_interface_version": str(upgrade_interface_version),
        }
        if owner is not None:
            details["owner"] = owner
        return "proxy_admin", details, had_error

    details = {"address": normalized}
    try:
        details.update(type_authority_contract(rpc_url, normalized, block_tag))
    except Exception:
        had_error = True
    return "contract", details, had_error


def _classify_uncached(rpc_url: str, normalized: str, block_tag: str) -> tuple[str, dict[str, object], bool]:
    """The actual classifier. Returns (kind, details, had_rpc_error).

    `had_rpc_error` is True if any underlying probe returned the
    `_PROBE_ERROR` sentinel — caller should skip caching to avoid cementing
    a transient-failure-driven misclassification.
    """
    if normalized == "0x0000000000000000000000000000000000000000":
        return "zero", {"address": normalized}, False

    try:
        code = _get_code(rpc_url, normalized, block_tag)
    except Exception:
        # Even the basic getCode failed — return generic contract but flag
        # the error so the caller doesn't cache.
        return "contract", {"address": normalized}, True
    if code in {"0x", "0x0"}:
        return "eoa", {"address": normalized}, False

    had_error = False

    def _probe(signature: str, abi_type: str) -> object | None:
        nonlocal had_error
        result = _try_eth_call_decoded(rpc_url, normalized, signature, abi_type, block_tag)
        if result is _PROBE_ERROR:
            had_error = True
            return None
        return result

    safe_owners = _probe("getOwners()", "address[]")
    safe_threshold = _probe("getThreshold()", "uint256")
    if safe_owners is not None and safe_threshold is not None:
        return (
            "safe",
            {
                "address": normalized,
                "owners": [str(item).lower() for item in safe_owners] if isinstance(safe_owners, list) else [],
                "threshold": _coerce_int(safe_threshold),
            },
            had_error,
        )

    min_delay = _probe("getMinDelay()", "uint256")
    if min_delay is None:
        min_delay = _probe("delay()", "uint256")
    if min_delay is not None:
        owner = _probe("owner()", "address")
        details: dict[str, object] = {"address": normalized, "delay": _coerce_int(min_delay)}
        if owner is not None:
            details["owner"] = owner
        return "timelock", details, had_error

    upgrade_interface_version = _probe("UPGRADE_INTERFACE_VERSION()", "string")
    if upgrade_interface_version is not None:
        owner = _probe("owner()", "address")
        details = {
            "address": normalized,
            "upgrade_interface_version": str(upgrade_interface_version),
        }
        if owner is not None:
            details["owner"] = owner
        return "proxy_admin", details, had_error

    details = {"address": normalized}
    try:
        details.update(type_authority_contract(rpc_url, normalized, block_tag))
    except Exception:
        had_error = True
    return "contract", details, had_error


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


def _controller_address_from_value(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = _normalize_hex(value)
    if len(normalized) != 42 or normalized == "0x0000000000000000000000000000000000000000":
        return None
    return normalized


def build_control_snapshot(plan: ControlTrackingPlan, rpc_url: str, block_tag: str = "latest") -> ControlSnapshot:
    block_number = _current_block_number(rpc_url) if block_tag == "latest" else int(block_tag, 16)
    controller_values = {}
    controllers_by_source: dict[str, list[TrackedController]] = {}
    for controller in plan["tracked_controllers"]:
        controllers_by_source.setdefault(controller["source"], []).append(controller)
    in_progress: set[str] = set()
    # Cache classify_resolved_address results to avoid duplicate RPC calls
    # when multiple controllers resolve to the same address.
    _classification_cache: dict[str, tuple[str, dict[str, object]]] = {}

    def _cached_classify(address: str) -> tuple[str, dict[str, object]]:
        key = _normalize_hex(address)
        if key not in _classification_cache:
            _classification_cache[key] = classify_resolved_address(rpc_url, address, block_tag)
        return _classification_cache[key]

    def _resolve_read_contract_address(controller: TrackedController) -> str:
        read_spec = controller.get("read_spec")
        if isinstance(read_spec, dict):
            contract_source = read_spec.get("contract_source")
            if isinstance(contract_source, str) and contract_source:
                for dependency in controllers_by_source.get(contract_source, []):
                    _read_controller_value(dependency)
                    value = controller_values.get(dependency["controller_id"], {}).get("value")
                    resolved = _controller_address_from_value(value)
                    if resolved:
                        return resolved
        return plan["contract_address"]

    def _read_controller_value(controller: TrackedController) -> None:
        controller_id = controller["controller_id"]
        if controller_id in controller_values or controller_id in in_progress:
            return
        in_progress.add(controller_id)
        source = controller["source"]
        read_spec = controller.get("read_spec")
        try:
            read_contract_address = _resolve_read_contract_address(controller)
            value = _read_polling_source(
                rpc_url,
                read_contract_address,
                source,
                controller["kind"],
                block_tag,
                read_spec=read_spec if isinstance(read_spec, dict) else None,
            )
            if controller["kind"] == "role_identifier":
                member_addresses, adapter_meta = expand_role_identifier_principals(
                    rpc_url,
                    read_contract_address,
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
                        "authority_contract": read_contract_address,
                        **adapter_meta,
                        "resolved_principals": resolved_principals,
                    },
                }
                return
            controller_values[controller_id] = {
                "source": source,
                "value": value,
                "block_number": block_number,
                "observed_via": "eth_call",
            }
            resolved_type, details = _cached_classify(value)
            controller_values[controller_id]["resolved_type"] = resolved_type
            controller_values[controller_id]["details"] = details
        except Exception as exc:
            controller_values[controller_id] = {
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
        finally:
            in_progress.discard(controller_id)

    for controller in plan["tracked_controllers"]:
        _read_controller_value(controller)
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
