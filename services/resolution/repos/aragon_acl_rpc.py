"""RPC-backed Aragon ACL log fetcher.

Wraps ``utils.rpc.rpc_request`` to fetch ``SetPermission`` logs
from a node's ``eth_getLogs`` and decode them into
``FetchedAragonLog`` for the indexer step.

Same chunking + defensive-decode contract as
``role_grants_rpc.RpcLogFetcher`` — bad log entries are silently
skipped so a single malformed log doesn't poison the batch.

Aragon ``SetPermission`` event:

    event SetPermission(
        address indexed entity,
        address indexed app,
        bytes32 indexed role,
        bool allowed
    );

The ``allowed`` flag lives in the data region (last 32-byte word
== 1 for true, 0 for false). Aragon revokes by emitting a new
SetPermission with allowed=false — no separate revoke topic.
"""

from __future__ import annotations

from typing import Any

from utils.rpc import rpc_request
from workers.aragon_acl_indexer import (
    SET_PERMISSION_TOPIC0,
    FetchedAragonLog,
)

# Same free-tier-safe block range cap as the role_grants fetcher.
DEFAULT_LOG_RANGE = 2000


class RpcAragonACLLogFetcher:
    """Implements the Aragon indexer's LogFetcher Protocol against
    an RPC URL.

    Internal chunking by ``max_block_range`` keeps each
    ``eth_getLogs`` request under the common 2k-block free-tier
    cap. The indexer's outer ``batch_size`` controls the number of
    blocks per insert flush; this fetcher chunks again only as
    needed by the wire-level limit."""

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
    ) -> list[FetchedAragonLog]:
        out: list[FetchedAragonLog] = []
        cursor = from_block
        while cursor <= to_block:
            chunk_end = min(cursor + self.max_block_range - 1, to_block)
            params: dict[str, Any] = {
                "address": contract_address,
                "fromBlock": hex(cursor),
                "toBlock": hex(chunk_end),
                # Single-topic filter — only SetPermission has this topic0.
                "topics": ["0x" + SET_PERMISSION_TOPIC0.hex()],
            }
            raw_logs = rpc_request(self.rpc_url, "eth_getLogs", [params])
            if isinstance(raw_logs, list):
                for log in raw_logs:
                    decoded = _decode_set_permission_log(log)
                    if decoded is not None:
                        out.append(decoded)
            cursor = chunk_end + 1
        return out


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------


def _decode_set_permission_log(log: dict[str, Any]) -> FetchedAragonLog | None:
    """Convert a raw eth_getLogs entry into a FetchedAragonLog.
    Returns None if any field is malformed."""
    topics = log.get("topics") or []
    # SetPermission carries 4 topics: topic0 + 3 indexed args.
    if len(topics) < 4:
        return None
    topic0 = _hex_to_bytes(topics[0], 32)
    if topic0 != SET_PERMISSION_TOPIC0:
        return None

    entity_topic = _hex_to_bytes(topics[1], 32)
    app_topic = _hex_to_bytes(topics[2], 32)
    role = _hex_to_bytes(topics[3], 32)
    if entity_topic is None or app_topic is None or role is None:
        return None
    entity = "0x" + entity_topic[-20:].hex()
    app = "0x" + app_topic[-20:].hex()

    allowed = _decode_bool_data(log.get("data"))
    if allowed is None:
        return None

    block_hash = _hex_to_bytes(log.get("blockHash"), 32)
    tx_hash = _hex_to_bytes(log.get("transactionHash"), 32)
    if block_hash is None or tx_hash is None:
        return None
    block_number = _hex_to_int(log.get("blockNumber"))
    log_index = _hex_to_int(log.get("logIndex"))
    transaction_index = _hex_to_int(log.get("transactionIndex"))
    if block_number is None or log_index is None or transaction_index is None:
        return None

    return FetchedAragonLog(
        block_number=block_number,
        block_hash=block_hash,
        tx_hash=tx_hash,
        log_index=log_index,
        transaction_index=transaction_index,
        entity=entity,
        app=app,
        role=role,
        allowed=allowed,
    )


def _decode_bool_data(value: Any) -> bool | None:
    """The data region for SetPermission is a single 32-byte
    ABI-encoded bool. Last byte == 1 → True; == 0 → False; any
    other shape → None (malformed)."""
    if not isinstance(value, str) or not value.startswith("0x"):
        return None
    try:
        raw = bytes.fromhex(value[2:])
    except ValueError:
        return None
    if len(raw) < 32:
        return None
    word = raw[:32]
    if word[-1] == 1 and all(b == 0 for b in word[:-1]):
        return True
    if all(b == 0 for b in word):
        return False
    # Defensive: garbage data isn't a clean true/false.
    return None


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
