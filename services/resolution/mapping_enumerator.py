"""Generic mapping-allowlist principal enumeration via Hypersync event replay.

Consumes `WriterEventSpec` records from the static pipeline and applies
set-union/set-difference semantics to produce the current allowlist.
Dispatches on `(event_signature, direction)` — no hardcoded event names.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, TypedDict

from eth_utils.crypto import keccak

from services.static.contract_analysis_pipeline.mapping_events import WriterEventSpec
from utils.rpc import normalize_hex as _normalize_hex

logger = logging.getLogger(__name__)

DEFAULT_HYPERSYNC_URL = "https://eth.hypersync.xyz"


class EnumeratedPrincipal(TypedDict):
    address: str
    mapping_name: str
    direction_history: list[str]
    last_seen_block: int


def _event_topic0(signature: str) -> str:
    digest = keccak(text=signature).hex()
    return _normalize_hex("0x" + digest)


def _build_query(hypersync_module, contract_address: str, topic0s: list[str], from_block: int, to_block: int | None):
    return hypersync_module.Query(
        from_block=from_block,
        to_block=to_block,
        logs=[
            hypersync_module.LogSelection(
                address=[contract_address.lower()],
                topics=[topic0s],
            )
        ],
        field_selection=hypersync_module.FieldSelection(
            log=[field.value for field in hypersync_module.LogField],
        ),
    )


def _topics_from_log(log: Any) -> list[str]:
    topics = getattr(log, "topics", None)
    if isinstance(topics, (list, tuple)):
        return [_normalize_hex(t) for t in topics if isinstance(t, str) and t.startswith("0x")]
    extracted: list[str] = []
    for attr in ("topic0", "topic1", "topic2", "topic3"):
        value = getattr(log, attr, None)
        if isinstance(value, str) and value.startswith("0x") and value not in {"0x", "0x0"}:
            extracted.append(_normalize_hex(value))
    return extracted


def _decode_address_topic(topic: str) -> str:
    t = _normalize_hex(topic)
    if len(t) != 66:
        return ""
    return _normalize_hex("0x" + t[-40:])


def _decode_address_arg_from_data(data: str, position: int) -> str:
    hex_body = data[2:] if data.startswith("0x") else data
    start = 64 * position
    end = start + 64
    if end > len(hex_body):
        return ""
    slot = hex_body[start:end]
    return _normalize_hex("0x" + slot[-40:])


def _extract_key_address(
    log: Any,
    key_position: int,
    *,
    indexed_positions: list[int] | None = None,
) -> str:
    """Pull the mapping-key address from an event log.

    Indexed parameters live in `topics[1..N]` in declaration order (topic0
    is the signature). Non-indexed parameters are packed into `data` at
    32-byte slots by their non-indexed rank.
    """
    topics = _topics_from_log(log)
    indexed_positions = indexed_positions or [key_position]
    if key_position in indexed_positions:
        indexed_rank = sorted(indexed_positions).index(key_position)
        topic_index = 1 + indexed_rank
        if topic_index < len(topics):
            return _decode_address_topic(topics[topic_index])
        return ""
    non_indexed = [p for p in range(key_position + 1) if p not in indexed_positions]
    if non_indexed:
        data_rank = len(non_indexed) - 1
        return _decode_address_arg_from_data(getattr(log, "data", "0x") or "0x", data_rank)
    return ""


async def enumerate_mapping_allowlist(
    contract_address: str,
    writer_specs: list[WriterEventSpec],
    *,
    hypersync_url: str = DEFAULT_HYPERSYNC_URL,
    bearer_token: str | None = None,
    from_block: int = 0,
    to_block: int | None = None,
    client: Any = None,
    hypersync_module: Any = None,
) -> list[EnumeratedPrincipal]:
    """Enumerate the current members of the writer-spec mappings on
    `contract_address` by replaying every writer event's history.

    Multiple specs on the same mapping collapse into one allowlist
    (e.g. Rely=add + Deny=remove → current wards)."""
    if not writer_specs:
        return []

    if hypersync_module is None:
        import hypersync as hypersync_module  # type: ignore
    if client is None:
        if not bearer_token:
            raise RuntimeError("Hypersync requires an API token; pass bearer_token= or set ENVIO_API_TOKEN.")
        client = hypersync_module.HypersyncClient(
            hypersync_module.ClientConfig(url=hypersync_url, bearer_token=bearer_token)
        )

    topic0_to_specs: dict[str, list[WriterEventSpec]] = {}
    for spec in writer_specs:
        topic0 = _event_topic0(spec["event_signature"])
        topic0_to_specs.setdefault(topic0, []).append(spec)

    topic0s = sorted(topic0_to_specs.keys())
    logger.info(
        "mapping_enumerator: address=%s from_block=%d to_block=%s topic0s=%s specs=%s",
        contract_address,
        from_block,
        to_block,
        topic0s,
        [(s["event_signature"], s["direction"], s.get("key_position")) for s in writer_specs],
    )
    query = _build_query(hypersync_module, contract_address, topic0s, from_block, to_block)

    state: dict[tuple[str, str], dict[str, Any]] = {}
    current_from = from_block
    page_count = 0
    while True:
        result = await client.get(query)
        page_count += 1
        # Real Hypersync response nests logs under `result.data.logs`;
        # test fakes use a flatter `result.logs`. Accept both.
        data_obj = getattr(result, "data", None)
        if data_obj is not None and hasattr(data_obj, "logs"):
            logs = list(getattr(data_obj, "logs", None) or [])
        elif isinstance(data_obj, list):
            logs = data_obj
        else:
            logs = list(getattr(result, "logs", None) or [])
        logger.info(
            "mapping_enumerator page %d: %d logs at from_block=%d, next_block=%s",
            page_count,
            len(logs),
            current_from,
            getattr(result, "next_block", None),
        )
        for raw_log in logs:
            topics = _topics_from_log(raw_log)
            if not topics:
                continue
            topic0 = topics[0]
            matching_specs = topic0_to_specs.get(topic0)
            if not matching_specs:
                continue
            for spec in matching_specs:
                key_address = _extract_key_address(raw_log, spec["key_position"])
                if not key_address.startswith("0x") or len(key_address) != 42:
                    continue
                block = int(getattr(raw_log, "block_number", 0) or 0)
                entry = state.setdefault(
                    (spec["mapping_name"], key_address),
                    {"present": False, "history": [], "last_block": 0},
                )
                if spec["direction"] == "add":
                    entry["present"] = True
                else:
                    entry["present"] = False
                entry["history"].append(spec["direction"])
                entry["last_block"] = max(entry["last_block"], block)

        next_from = getattr(result, "next_block", None)
        if next_from is None or next_from <= current_from:
            break
        current_from = next_from
        query = _build_query(hypersync_module, contract_address, topic0s, current_from, to_block)

    out: list[EnumeratedPrincipal] = []
    for (mapping_name, addr), entry in state.items():
        if not entry["present"]:
            continue
        out.append(
            {
                "address": addr,
                "mapping_name": mapping_name,
                "direction_history": list(entry["history"]),
                "last_seen_block": int(entry["last_block"]),
            }
        )
    return out


def enumerate_mapping_allowlist_sync(
    contract_address: str,
    writer_specs: list[WriterEventSpec],
    **kwargs: Any,
) -> list[EnumeratedPrincipal]:
    """Sync wrapper for callers (like `recursive.py`) that don't want
    to manage an event loop. Uses `asyncio.run` — safe only when
    called from a non-async context."""
    return asyncio.run(enumerate_mapping_allowlist(contract_address, writer_specs, **kwargs))
