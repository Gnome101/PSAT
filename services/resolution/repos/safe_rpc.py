"""RPC-backed SafeRepo.

Reads Safe owner set + threshold via two view calls:

  * ``getOwners()`` -> ``address[]`` (selector ``0xa0e67e2b``)
  * ``getThreshold()`` -> ``uint256`` (selector ``0xe75235b8``)

Safe state is small (typically <50 owners) and these are pure
view calls, so unlike RoleGrants we don't need a persistent index;
the RPC is the source of truth and fetching it on demand is
sub-100ms in practice.

Returns ``None`` (rather than raising) on any of:
  * Unknown chain_id (no RPC URL configured)
  * RPC error / timeout
  * Empty / malformed response
  * ABI decode failure

The Safe adapter treats ``None`` as "I don't know" → external_check_only
fallback. Raising would push the failure up the resolver stack and
cascade.

A future on-disk cache layer (e.g. cached via ``ContractBalance``-style
table keyed on (chain_id, address, block) with a short TTL) would
fit cleanly behind the same interface — wrap this class and cache
``get_owners_threshold`` calls.
"""

from __future__ import annotations

from typing import Any

from utils.rpc import rpc_request


# Function selectors for the Safe ABI.
_GET_OWNERS_SELECTOR = "0xa0e67e2b"
_GET_THRESHOLD_SELECTOR = "0xe75235b8"


class RpcSafeRepo:
    """Implements ``SafeRepo`` against an RPC URL per chain.

    Args:
      rpc_url_for_chain: ``{chain_id: rpc_url}`` map. Unmapped
        chains return ``None``.
    """

    def __init__(self, rpc_url_for_chain: dict[int, str]) -> None:
        self.rpc_url_for_chain = rpc_url_for_chain

    def get_owners_threshold(
        self,
        *,
        chain_id: int,
        contract_address: str,
        block: int | None = None,
    ) -> tuple[list[str], int] | None:
        url = self.rpc_url_for_chain.get(chain_id)
        if url is None:
            return None
        block_tag = hex(block) if block is not None else "latest"

        try:
            owners_raw = _eth_call(url, contract_address, _GET_OWNERS_SELECTOR, block_tag)
            threshold_raw = _eth_call(
                url, contract_address, _GET_THRESHOLD_SELECTOR, block_tag
            )
        except Exception:
            return None
        if owners_raw is None or threshold_raw is None:
            return None

        owners = _decode_address_list(owners_raw)
        threshold = _decode_uint(threshold_raw)
        if owners is None or threshold is None:
            return None
        return (owners, threshold)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _eth_call(
    rpc_url: str, contract_address: str, calldata: str, block_tag: str
) -> str | None:
    """Single ``eth_call`` round-trip. Returns the hex response or
    ``None`` if the call returned ``0x`` (function-absent / revert)."""
    payload: dict[str, Any] = {"to": contract_address, "data": calldata}
    result = rpc_request(rpc_url, "eth_call", [payload, block_tag])
    if not isinstance(result, str) or not result.startswith("0x"):
        return None
    if result in ("0x", "0x0"):
        return None
    return result


def _decode_address_list(raw_hex: str) -> list[str] | None:
    """Decode dynamic ``address[]`` ABI output. Returns lowercase
    addresses or ``None`` on malformed input."""
    try:
        from eth_abi import decode  # type: ignore[import]
    except Exception:
        return None
    try:
        data = bytes.fromhex(raw_hex[2:])
        result = decode(["address[]"], data)[0]
    except Exception:
        return None
    if not isinstance(result, (list, tuple)):
        return None
    return [str(addr).lower() for addr in result]


def _decode_uint(raw_hex: str) -> int | None:
    """Decode ``uint256`` ABI output. Single 32-byte slot."""
    try:
        data = bytes.fromhex(raw_hex[2:])
    except ValueError:
        return None
    if len(data) < 32:
        return None
    return int.from_bytes(data[:32], "big")
