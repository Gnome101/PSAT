"""RPC-backed implementations of ``LogFetcher`` and
``BlockHashFetcher`` for the role_grants indexer.

Wraps ``utils.rpc.rpc_request`` (the project's standard JSON-RPC
helper). The fetcher batches ``eth_getLogs`` requests with the
``RoleGranted`` and ``RoleRevoked`` topic0 filters AND the contract
address filter, decoding raw log dicts into ``FetchedLog``.
``BlockHashFetcher`` calls ``eth_getBlockByNumber`` with the
``False`` (no-tx-detail) flag to keep the response small.

This module is a thin adapter — the indexer's algorithm lives in
``workers/role_grants_indexer.py``."""

from __future__ import annotations

from typing import Any

from utils.rpc import rpc_request
from workers.role_grants_indexer import (
    ROLE_GRANTED_TOPIC0,
    ROLE_REVOKED_TOPIC0,
    FetchedLog,
    event_direction,
)


# Free-tier RPC limit used by the existing watchers. Keeps requests
# safe across providers (Alchemy free tier caps at 10k blocks; some
# providers cap lower).
DEFAULT_LOG_RANGE = 2000


class RpcLogFetcher:
    """Implements ``LogFetcher`` against an RPC URL.

    The fetcher chunks the requested range in
    ``DEFAULT_LOG_RANGE`` block windows so requests stay under the
    common 2k-block cap free-tier providers enforce. Larger ranges
    can be passed if the operator's RPC supports it (the indexer's
    ``batch_size`` is the outer chunk; this class chunks again
    internally only when forced by RPC error responses)."""

    def __init__(self, rpc_url: str, *, max_block_range: int = DEFAULT_LOG_RANGE) -> None:
        self.rpc_url = rpc_url
        self.max_block_range = max_block_range

    def fetch_logs(
        self,
        *,
        chain_id: int,
        contract_address: str,
        from_block: int,
        to_block: int,
    ) -> list[FetchedLog]:
        out: list[FetchedLog] = []
        cursor = from_block
        while cursor <= to_block:
            chunk_end = min(cursor + self.max_block_range - 1, to_block)
            params: dict[str, Any] = {
                "address": contract_address,
                "fromBlock": hex(cursor),
                "toBlock": hex(chunk_end),
                "topics": [
                    # OR over both RoleGranted + RoleRevoked
                    [
                        "0x" + ROLE_GRANTED_TOPIC0.hex(),
                        "0x" + ROLE_REVOKED_TOPIC0.hex(),
                    ]
                ],
            }
            raw_logs = rpc_request(self.rpc_url, "eth_getLogs", [params])
            if isinstance(raw_logs, list):
                for log in raw_logs:
                    decoded = _decode_role_event_log(log)
                    if decoded is not None:
                        out.append(decoded)
            cursor = chunk_end + 1
        return out


class RpcBlockHashFetcher:
    """Implements ``BlockHashFetcher`` against an RPC URL via
    ``eth_getBlockByNumber(hex_block, False)``. Returns ``None`` for
    blocks the node hasn't seen (pruned or beyond head)."""

    def __init__(self, rpc_url: str) -> None:
        self.rpc_url = rpc_url

    def block_hash(self, *, chain_id: int, block_number: int) -> bytes | None:
        try:
            block = rpc_request(
                self.rpc_url, "eth_getBlockByNumber", [hex(block_number), False]
            )
        except Exception:
            return None
        if not isinstance(block, dict):
            return None
        h = block.get("hash")
        if not isinstance(h, str) or not h.startswith("0x"):
            return None
        try:
            raw = bytes.fromhex(h[2:])
        except ValueError:
            return None
        if len(raw) != 32:
            return None
        return raw


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------


def _decode_role_event_log(log: dict[str, Any]) -> FetchedLog | None:
    """Convert a raw eth_getLogs result entry into a ``FetchedLog``.

    Returns ``None`` if the entry is malformed, missing fields, or
    its topic0 isn't a RoleGranted/Revoked. Defensive on every
    decode step so a single bad log doesn't bring down the indexer.
    """
    topics = log.get("topics") or []
    if len(topics) < 3:
        # RoleGranted/Revoked: topic0 + indexed role + indexed account.
        return None
    topic0 = _hex_to_bytes(topics[0], 32)
    role = _hex_to_bytes(topics[1], 32)
    account_topic = _hex_to_bytes(topics[2], 32)
    if topic0 is None or role is None or account_topic is None:
        return None
    direction = event_direction(topic0)
    if direction is None:
        return None
    member = "0x" + account_topic[-20:].hex()

    block_hash = _hex_to_bytes(log.get("blockHash"), 32)
    tx_hash = _hex_to_bytes(log.get("transactionHash"), 32)
    if block_hash is None or tx_hash is None:
        return None
    block_number = _hex_to_int(log.get("blockNumber"))
    log_index = _hex_to_int(log.get("logIndex"))
    transaction_index = _hex_to_int(log.get("transactionIndex"))
    if block_number is None or log_index is None or transaction_index is None:
        return None

    return FetchedLog(
        block_number=block_number,
        block_hash=block_hash,
        tx_hash=tx_hash,
        log_index=log_index,
        transaction_index=transaction_index,
        role=role,
        member=member,
        direction=direction,
    )


def _hex_to_bytes(value: Any, expected_len: int) -> bytes | None:
    if not isinstance(value, str) or not value.startswith("0x"):
        return None
    try:
        raw = bytes.fromhex(value[2:])
    except ValueError:
        return None
    if len(raw) != expected_len:
        return None
    return raw


def _hex_to_int(value: Any) -> int | None:
    if not isinstance(value, str) or not value.startswith("0x"):
        return None
    try:
        return int(value, 16)
    except ValueError:
        return None
