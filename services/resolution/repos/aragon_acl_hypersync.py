"""HyperSync-backed Aragon ACL log fetcher.

Mirror of ``role_grants_hypersync.HyperSyncLogFetcher`` but for
``SetPermission`` events — same async-to-sync wrapper, same
per-chain URL map, same fail-on-missing-token contract, same
defensive-decode skip on malformed log entries.

Use HyperSync for genesis-from-scratch backfill of an Aragon ACL
contract; fall back to ``RpcAragonACLLogFetcher`` for the recent
tail where HyperSync's data lags head by ~5–60s.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from workers.aragon_acl_indexer import (
    SET_PERMISSION_TOPIC0,
    FetchedAragonLog,
)

# Per-chain HyperSync hostnames. Aligned with role_grants_hypersync
# and chain_finality_config.
_DEFAULT_URL_FOR_CHAIN = {
    1: "https://eth.hypersync.xyz",
    10: "https://optimism.hypersync.xyz",
    137: "https://polygon.hypersync.xyz",
    8453: "https://base.hypersync.xyz",
    42161: "https://arbitrum.hypersync.xyz",
    59144: "https://linea.hypersync.xyz",
    534352: "https://scroll.hypersync.xyz",
}


class HyperSyncAragonACLLogFetcher:
    """Implements the Aragon indexer's LogFetcher Protocol against
    HyperSync (envio.dev). Same async-to-sync wrapper as the
    role_grants HyperSync fetcher — the indexer's sync API is
    unchanged.

    Args:
      bearer_token: HyperSync API token. Falls back to
        ``ENVIO_API_TOKEN``.
      url_for_chain: optional override map; unmapped chains fall
        back to the module default.
      hypersync_module: dependency-injection seam for tests.
    """

    def __init__(
        self,
        *,
        bearer_token: str | None = None,
        url_for_chain: dict[int, str] | None = None,
        hypersync_module: Any = None,
    ) -> None:
        self.bearer_token = bearer_token or os.getenv("ENVIO_API_TOKEN")
        self.url_for_chain = {**_DEFAULT_URL_FOR_CHAIN, **(url_for_chain or {})}
        self._hypersync_module = hypersync_module

    def fetch_logs(
        self,
        *,
        chain_id: int,
        contract_address: str,
        from_block: int,
        to_block: int,
    ) -> list[FetchedAragonLog]:
        if from_block > to_block:
            return []
        return asyncio.run(
            self._async_fetch_logs(
                chain_id=chain_id,
                contract_address=contract_address,
                from_block=from_block,
                to_block=to_block,
            )
        )

    async def _async_fetch_logs(
        self,
        *,
        chain_id: int,
        contract_address: str,
        from_block: int,
        to_block: int,
    ) -> list[FetchedAragonLog]:
        hs = self._resolve_module()
        if not self.bearer_token:
            raise RuntimeError(
                "HyperSyncAragonACLLogFetcher requires an API token. Set ENVIO_API_TOKEN or pass bearer_token=."
            )
        url = self.url_for_chain.get(chain_id)
        if url is None:
            raise RuntimeError(
                f"HyperSync URL not configured for chain_id={chain_id}; "
                "pass url_for_chain={...} to extend the default map."
            )
        client = hs.HypersyncClient(hs.ClientConfig(url=url, bearer_token=self.bearer_token))

        topic0_hex = "0x" + SET_PERMISSION_TOPIC0.hex()
        out: list[FetchedAragonLog] = []
        current_from = from_block
        target_exclusive = to_block + 1
        while current_from < target_exclusive:
            query = hs.Query(
                from_block=current_from,
                to_block=target_exclusive,
                logs=[
                    hs.LogSelection(
                        address=[contract_address],
                        topics=[[topic0_hex]],
                    )
                ],
                field_selection=hs.FieldSelection(
                    log=[field.value for field in hs.LogField],
                ),
            )
            response = await client.get(query)
            page_logs = list(getattr(getattr(response, "data", None), "logs", []) or [])
            for log in page_logs:
                decoded = _decode_hypersync_log(log)
                if decoded is not None:
                    out.append(decoded)

            next_block = getattr(response, "next_block", None)
            if next_block is None or next_block <= current_from:
                break
            if next_block >= target_exclusive:
                break
            current_from = int(next_block)
        return out

    def _resolve_module(self):
        if self._hypersync_module is not None:
            return self._hypersync_module
        import hypersync as hs  # type: ignore

        return hs


# ---------------------------------------------------------------------------
# Decode
# ---------------------------------------------------------------------------


def _decode_hypersync_log(log: Any) -> FetchedAragonLog | None:
    topics = list(getattr(log, "topics", None) or [])
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

    allowed = _decode_bool_data(getattr(log, "data", None))
    if allowed is None:
        return None

    block_hash = _hex_to_bytes(getattr(log, "block_hash", None), 32)
    tx_hash = _hex_to_bytes(getattr(log, "transaction_hash", None), 32)
    if block_hash is None or tx_hash is None:
        return None
    block_number = _coerce_int(getattr(log, "block_number", None))
    log_index = _coerce_int(getattr(log, "log_index", None))
    transaction_index = _coerce_int(getattr(log, "transaction_index", None))
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


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith("0x") else int(value)
        except ValueError:
            return None
    return None
