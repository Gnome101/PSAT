"""HyperSync-backed implementation of ``LogFetcher``.

HyperSync (envio.dev) returns logs from a columnar archive much
faster than ``eth_getLogs`` — useful for genesis-from-scratch
backfill of role_grants. The query API takes a single
``[from_block, to_block)`` range; the returned response carries a
``next_block`` cursor for pagination across very large ranges.

The block-hash check the indexer uses for reorg detection stays on
``RpcBlockHashFetcher`` — that's a single per-pass call where RPC
is cheaper than spinning up a HyperSync client.

This adapter keeps the same ``LogFetcher`` Protocol the indexer
already consumes, so swapping between RPC and HyperSync is a
``LogFetcher`` injection at the call site. Hybrid use (HyperSync
for the historical scan, RPC for the tail) is just two fetchers
behind a small dispatcher.

Async-to-sync: the underlying ``hypersync.HypersyncClient.get`` is
async; ``fetch_logs`` runs it via ``asyncio.run`` so the indexer's
sync API is unchanged. Don't call ``fetch_logs`` from a running
event loop — make a fresh fetcher per worker process.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from workers.role_grants_indexer import (
    ROLE_GRANTED_TOPIC0,
    ROLE_REVOKED_TOPIC0,
    FetchedLog,
    event_direction,
)


# Per-chain HyperSync hostnames. Keep aligned with
# ``chain_finality_config`` seed and the existing
# ``services/policy/hypersync_backfill.py`` default.
_DEFAULT_URL_FOR_CHAIN = {
    1: "https://eth.hypersync.xyz",
    10: "https://optimism.hypersync.xyz",
    137: "https://polygon.hypersync.xyz",
    8453: "https://base.hypersync.xyz",
    42161: "https://arbitrum.hypersync.xyz",
    59144: "https://linea.hypersync.xyz",
    534352: "https://scroll.hypersync.xyz",
}


class HyperSyncLogFetcher:
    """Implements ``LogFetcher`` against HyperSync.

    Args:
      bearer_token: HyperSync API token. Falls back to
        ``ENVIO_API_TOKEN`` env var.
      url_for_chain: optional override map ``{chain_id: url}``;
        unmapped chains fall through to the module default.
      hypersync_module: dependency-injection seam for tests — pass
        a stub module exposing ``HypersyncClient``, ``ClientConfig``,
        ``Query``, ``LogSelection``, ``FieldSelection``, ``LogField``.
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
    ) -> list[FetchedLog]:
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
    ) -> list[FetchedLog]:
        hs = self._resolve_module()
        if not self.bearer_token:
            raise RuntimeError(
                "HyperSyncLogFetcher requires an API token. "
                "Set ENVIO_API_TOKEN or pass bearer_token=."
            )
        url = self.url_for_chain.get(chain_id)
        if url is None:
            raise RuntimeError(
                f"HyperSync URL not configured for chain_id={chain_id}; "
                "pass url_for_chain={...} to extend the default map."
            )
        client = hs.HypersyncClient(
            hs.ClientConfig(url=url, bearer_token=self.bearer_token)
        )

        # HyperSync's to_block is exclusive; the indexer hands us
        # an inclusive bound, so add 1 to keep the boundary block.
        # next_block in the response is also exclusive of the page,
        # so it slots naturally into ``current_from`` on the next
        # iteration.
        topic_or = [
            "0x" + ROLE_GRANTED_TOPIC0.hex(),
            "0x" + ROLE_REVOKED_TOPIC0.hex(),
        ]
        out: list[FetchedLog] = []
        current_from = from_block
        target_exclusive = to_block + 1
        while current_from < target_exclusive:
            query = hs.Query(
                from_block=current_from,
                to_block=target_exclusive,
                logs=[
                    hs.LogSelection(
                        address=[contract_address],
                        topics=[topic_or],
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


def _decode_hypersync_log(log: Any) -> FetchedLog | None:
    """HyperSync log objects use snake_case attrs (block_number,
    block_hash, transaction_hash, log_index, transaction_index,
    topics, data, address). Defensive on every step so a malformed
    entry doesn't poison the batch."""
    topics = list(getattr(log, "topics", None) or [])
    if len(topics) < 3:
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

    block_hash = _hex_to_bytes(getattr(log, "block_hash", None), 32)
    tx_hash = _hex_to_bytes(getattr(log, "transaction_hash", None), 32)
    if block_hash is None or tx_hash is None:
        return None
    block_number = _coerce_int(getattr(log, "block_number", None))
    log_index = _coerce_int(getattr(log, "log_index", None))
    transaction_index = _coerce_int(getattr(log, "transaction_index", None))
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


def _coerce_int(value: Any) -> int | None:
    """HyperSync returns ints as ``int`` directly OR as 0x-hex
    strings depending on the field. Accept both."""
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
