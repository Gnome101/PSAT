"""HyperSync-backed ``TraceFetcher`` for the ``MappingTraceAdapter``
(D.4).

HyperSync exposes call-trace data alongside logs via its
``TraceField`` selection — first usage in PSAT. The fetcher mirrors
``role_grants_hypersync.HyperSyncLogFetcher`` in shape:
async-to-sync wrapping, per-chain URL map, dependency-injection
seam for tests.

Production wiring is gated on ``ENVIO_API_TOKEN`` + a leaf actually
carrying ``writer_selectors``; without either, the
``MappingTraceAdapter`` doesn't match and this fetcher is never
constructed. For the unit-test pass we use ``HyperSyncTraceFetcher``
with a stubbed ``hypersync_module`` so we exercise the shape
without an Envio round-trip.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from services.resolution.adapters.mapping_trace import FetchedTrace

_DEFAULT_URL_FOR_CHAIN = {
    1: "https://eth.hypersync.xyz",
    10: "https://optimism.hypersync.xyz",
    137: "https://polygon.hypersync.xyz",
    8453: "https://base.hypersync.xyz",
    42161: "https://arbitrum.hypersync.xyz",
    59144: "https://linea.hypersync.xyz",
    534352: "https://scroll.hypersync.xyz",
}


class HyperSyncTraceFetcher:
    """Implements the ``TraceFetcher`` Protocol against HyperSync.

    The query selects internal calls where ``to == subject_address``,
    drops ``staticcall`` (read-only — can't mutate state), and the
    adapter further filters by selector + non-error.
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

    def fetch_traces(
        self,
        *,
        chain_id: int,
        contract_address: str,
        from_block: int = 0,
        to_block: int | None = None,
    ) -> list[FetchedTrace]:
        if to_block is not None and from_block > to_block:
            return []
        return asyncio.run(
            self._async_fetch(
                chain_id=chain_id,
                contract_address=contract_address,
                from_block=from_block,
                to_block=to_block,
            )
        )

    async def _async_fetch(
        self,
        *,
        chain_id: int,
        contract_address: str,
        from_block: int,
        to_block: int | None,
    ) -> list[FetchedTrace]:
        hs = self._hypersync_module
        if hs is None:
            import hypersync as hs  # type: ignore

        if not self.bearer_token:
            raise RuntimeError("HyperSync trace fetcher requires ENVIO_API_TOKEN.")
        url = self.url_for_chain.get(chain_id) or _DEFAULT_URL_FOR_CHAIN[1]
        client = hs.HypersyncClient(hs.ClientConfig(url=url, bearer_token=self.bearer_token))

        # The trace selection filters on the subject contract being the
        # callee. ``call_type`` filtering happens client-side in the
        # adapter so a single query stays cheap.
        query = hs.Query(
            from_block=from_block,
            to_block=to_block,
            traces=[hs.TraceSelection(to=[contract_address.lower()])],
            field_selection=hs.FieldSelection(
                trace=[field.value for field in hs.TraceField],
            ),
        )

        out: list[FetchedTrace] = []
        current_from = from_block
        while True:
            result = await client.get(query)
            data = getattr(result, "data", None)
            traces = list(getattr(data, "traces", None) or [])
            for raw in traces:
                trace_address_raw = getattr(raw, "trace_address", None) or []
                tx_pos_raw = getattr(raw, "transaction_position", None) or getattr(raw, "transaction_index", 0) or 0
                call_type_raw = getattr(raw, "call_type", None) or getattr(raw, "type", "") or ""
                out.append(
                    FetchedTrace(
                        block_number=int(getattr(raw, "block_number", 0) or 0),
                        transaction_index=int(tx_pos_raw),
                        trace_address=tuple(int(x) for x in trace_address_raw),
                        input_data=str(getattr(raw, "input", "") or ""),
                        call_type=str(call_type_raw),
                        error=getattr(raw, "error", None),
                    )
                )
            next_from = getattr(result, "next_block", None)
            if next_from is None or next_from <= current_from:
                break
            current_from = next_from
            query = hs.Query(
                from_block=current_from,
                to_block=to_block,
                traces=[hs.TraceSelection(to=[contract_address.lower()])],
                field_selection=hs.FieldSelection(
                    trace=[field.value for field in hs.TraceField],
                ),
            )
        return out
