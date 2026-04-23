"""Generic mapping-allowlist principal enumeration via event replay.

Consumes `WriterEventSpec` records from the static pipeline (see
`services/static/contract_analysis_pipeline/mapping_events.py`), pulls
the corresponding event history from Hypersync, and applies
set-union/set-difference semantics to produce the current allowlist.

Replaces protocol-specific enumerators (wards via Rely/Deny, OZ roles
via RoleGranted/RoleRevoked, custom whitelists via SetWhitelisted)
with one dispatcher keyed on `(event_signature, direction)`. No
hardcoded event names — the writer-event discovery in the static
stage tells us exactly which events to scrape.

Shape:

    enumerate_mapping_allowlist(
        contract_address,
        writer_specs,  # list[WriterEventSpec]
        *,
        hypersync_url="https://eth.hypersync.xyz",
        bearer_token=...,
        from_block=0,
        to_block=None,
        hypersync_module=None,  # injectable for tests
        client=None,             # injectable for tests
    ) -> list[EnumeratedPrincipal]

Returns a list of `{address, directions_seen, last_block}` dicts
representing the current set — addresses whose net state is "present"
(at least one add event without a matching later remove).
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
    """One entry in the final allowlist — an address that's currently
    a member, with enough evidence for the downstream resolver to
    write `control_graph_nodes` rows."""

    address: str
    mapping_name: str
    # For operator inspection: did we see adds only, or add-then-
    # remove-then-re-add? "add" / "remove" / mix thereof.
    direction_history: list[str]
    # Block of the LAST event that kept the address in the set.
    # Useful for "when was this granted?" audit questions.
    last_seen_block: int


def _event_topic0(signature: str) -> str:
    """keccak256 of the canonical event signature. Slither gives us
    `Rely(address)`; Ethereum's log topic0 is `keccak(b"Rely(address)")`."""
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
    """Address-typed indexed event parameters are padded to 32 bytes.
    The original address is the low-order 20 bytes."""
    t = _normalize_hex(topic)
    if len(t) != 66:
        return ""
    return _normalize_hex("0x" + t[-40:])


def _decode_address_arg_from_data(data: str, position: int) -> str:
    """Non-indexed address arg from the log's `data` field.

    ABI packs each non-indexed arg into a 32-byte slot; the k-th
    non-indexed arg lives at bytes [32*k, 32*(k+1)). We return the
    low-order 20 bytes of that slot.
    """
    hex_body = data[2:] if data.startswith("0x") else data
    start = 64 * position  # 32 bytes = 64 hex chars
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
    """Pull the mapping-key address from an event log, picking the
    right slot based on whether the key parameter was declared
    `indexed` in the event.

    Phase 3a records `key_position` as the event's parameter index (0,
    1, …) but doesn't know which are indexed — slither's `AssociatedEvent`
    carries that, but we haven't plumbed it through yet. We try the
    obvious mappings: indexed → topicN (N = 1 + indexed_count_before_key);
    not indexed → data slot [key_position among non-indexed].

    For MakerDAO wards (`Rely(address indexed usr)`), the single arg
    is indexed, so topic[1] holds the address. We treat `key_position=0`
    + 1 indexed param as the common case; operators with multi-arg
    events can refine via `indexed_positions`.
    """
    topics = _topics_from_log(log)
    indexed_positions = indexed_positions or [key_position]
    # If the key parameter is indexed, it lives in topics[1+...].
    if key_position in indexed_positions:
        # Rank of the key among indexed params == its index in the
        # ordered indexed_positions list — topic[0] is the signature,
        # topic[1..N] are indexed args in declaration order.
        indexed_rank = sorted(indexed_positions).index(key_position)
        topic_index = 1 + indexed_rank
        if topic_index < len(topics):
            return _decode_address_topic(topics[topic_index])
        return ""
    # Otherwise it's in the `data` field at its non-indexed rank.
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
    """Enumerate the current members of one or more mapping allowlists
    on `contract_address` by replaying every writer event's history
    and applying set-semantics.

    Multiple `writer_specs` on the same mapping collapse into one
    allowlist — e.g. `(wards, Rely, add)` and `(wards, Deny, remove)`
    fold into one set of "current wards". We keep the `mapping_name`
    on each output record so callers can group if multiple allowlists
    are being enumerated in parallel.

    `client` and `hypersync_module` are injectable so unit tests can
    substitute fakes without reaching out to the real Hypersync
    endpoint.
    """
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

    # Topic0 → spec lookup so we can route each log back to its spec.
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

    # State: (mapping_name, address) → {present: bool, history: [...], last_block: int}
    state: dict[tuple[str, str], dict[str, Any]] = {}
    current_from = from_block
    page_count = 0
    while True:
        result = await client.get(query)
        page_count += 1
        # Hypersync's real response is `QueryResponseData` at
        # `result.data` with `.logs` inside. We also accept `result.logs`
        # directly for the convenience of test fakes that want a flatter
        # shape. Both paths end with a list we can iterate.
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
