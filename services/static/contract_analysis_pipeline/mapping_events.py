"""Discover writer-events paired with mapping writes.

Purpose: for an `allowlist` pattern like MakerDAO's
`wards[guy] = 1; emit Rely(guy);`, we need the downstream resolution
stage to scrape `Rely(address)` (and its `Deny` counterpart) to
enumerate the current members of `wards`. This module walks the
contract's source and extracts those `(mapping, event_signature,
direction)` triples generically — no hardcoded event names.

The rule we're encoding:

> A function that writes to a `mapping(address => T)` AND emits an
> event whose argument list contains the same address key is the
> writer for that mapping. The event-write direction (add vs remove)
> is inferred from the mapping value being written.

This is protocol-convention-level information that the AST carries:
the correspondence between `emit X(guy)` and `wards[guy] = 1` is a
syntactic fact. The resolver downstream uses it to pull the event
log history via Hypersync and materialize the current allowlist.

Phase 3 scope:
- Direction inference covers `= 1` / `= 0` (MakerDAO wards), `= true`
  / `= false` (OZ-style bool mappings), and `delete x[k]` (which is
  equivalent to `= 0`).
- Multi-arg events where the address key isn't the first parameter
  still match as long as one arg is the mapping key.
- We don't attempt to handle event emissions whose argument is a
  computed expression (e.g. `emit Updated(hashOf(guy))`); these
  slither lowers through a TMP and we'd need deeper taint to follow.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict

from .shared import _contract_functions


class WriterEventSpec(TypedDict):
    """One mapping-write + co-emitted event pairing."""

    # Mapping state variable name, e.g. "wards".
    mapping_name: str
    # Fully-qualified event name as slither exposes it ("Rely(address)").
    event_signature: str
    # Event name alone ("Rely"), for human-readable labeling.
    event_name: str
    # Position (0-indexed) of the address argument in the event's
    # parameter list that corresponds to the mapping key. Used by the
    # resolver to pull the right indexed topic from the log.
    key_position: int
    # Set-semantics for the event: does it ADD the key to the allowlist
    # (writing a truthy value) or REMOVE it (writing zero / false /
    # via `delete`)?
    direction: Literal["add", "remove"]
    # Source function that does the write — for evidence attribution.
    writer_function: str


def _ir_name(ir: Any) -> str:
    return type(ir).__name__ if ir is not None else ""


def _var_name(item: Any) -> str:
    if item is None:
        return ""
    return getattr(item, "name", None) or str(item)


def _is_mapping_address_keyed(variable: Any) -> bool:
    """True when `variable` is a state mapping whose key type is
    `address`. The downstream enumerator only handles address
    allowlists — integer-keyed mappings (e.g. role IDs) are a
    different shape and out of scope here."""
    type_str = str(getattr(variable, "type", "") or "")
    return type_str.startswith("mapping(address =>") or type_str.startswith("mapping(address=>")


def _written_mappings(function: Any) -> list[Any]:
    return [v for v in function.all_state_variables_written() if _is_mapping_address_keyed(v)]


def _extract_index_writes(function: Any) -> list[tuple[str, Any, Any]]:
    """Return `(mapping_name, key_var, value_var)` triples for every
    `X[key] = value` assignment in the function body.

    Slither lowers these into an Index IR (read of lvalue handle) +
    a subsequent assignment / store of the value into the index
    lvalue. We pattern-match both shapes conservatively — anything we
    can't attribute cleanly gets skipped rather than misattributed.
    """
    triples: list[tuple[str, Any, Any]] = []
    for node in getattr(function, "nodes", []) or []:
        # Track Index IRs by their lvalue name so we can join a later
        # Assignment that writes `X[key] = value` back in.
        index_map: dict[str, tuple[Any, Any]] = {}
        for ir in getattr(node, "irs", []) or []:
            kind = _ir_name(ir)
            if kind == "Index":
                base = getattr(ir, "variable_left", None)
                if not _is_mapping_address_keyed(base):
                    continue
                key = getattr(ir, "variable_right", None)
                lvalue_name = _var_name(getattr(ir, "lvalue", None))
                if lvalue_name:
                    index_map[lvalue_name] = (base, key)
            elif kind == "Assignment":
                lvalue_name = _var_name(getattr(ir, "lvalue", None))
                if lvalue_name in index_map:
                    base, key = index_map[lvalue_name]
                    triples.append((_var_name(base), key, getattr(ir, "rvalue", None)))
            elif kind == "Delete":
                # `delete X[key]` — slither emits a Delete IR whose
                # operand is the index lvalue. Treat as a zero-write.
                operand_name = _var_name(getattr(ir, "lvalue", None))
                if operand_name in index_map:
                    base, key = index_map[operand_name]
                    triples.append((_var_name(base), key, None))
    return triples


def _direction_of_write(value_var: Any) -> Literal["add", "remove"] | None:
    """Infer set-semantics from the value being written.

    - `= 1`, `= true`, non-zero int literal → add
    - `= 0`, `= false`, `delete` (value_var is None) → remove
    - anything else (variable, expression) → ambiguous; return None
      and skip the triple.
    """
    if value_var is None:
        return "remove"  # delete
    value = getattr(value_var, "value", None)
    type_str = str(getattr(value_var, "type", "") or "")
    if value is None:
        # Not a constant — we can't tell. Skip rather than misattribute.
        return None
    # Boolean literals: python `True`/`False` OR slither string "true"/"false".
    if isinstance(value, bool):
        return "add" if value else "remove"
    # Numeric literal: non-zero → add, zero → remove.
    try:
        numeric = int(value, 16) if isinstance(value, str) and str(value).startswith("0x") else int(value)
        return "add" if numeric != 0 else "remove"
    except (ValueError, TypeError):
        pass
    # String-typed "true"/"false" via slither constant representation.
    if type_str == "bool":
        if str(value).lower() in ("true", "1"):
            return "add"
        if str(value).lower() in ("false", "0"):
            return "remove"
    return None


def _event_signature_map(contract: Any) -> dict[str, str]:
    """Map bare event name → canonical signature for every event
    visible on `contract` (including inherited). Slither's EventCall
    IR only exposes the bare name (`"Rely"`); the on-chain topic0
    is `keccak(canonical)` where canonical is `"Rely(address)"`, so
    we need the Event declaration's `full_name` to hash correctly.

    If two inherited events collide on name, the first one wins —
    that's rare enough in practice that we accept the ambiguity."""
    mapping: dict[str, str] = {}
    for event in getattr(contract, "events", []) or []:
        name = getattr(event, "name", "") or ""
        full = getattr(event, "full_name", "") or name
        if name and name not in mapping:
            mapping[name] = full
    return mapping


def _extract_event_emissions(function: Any, sig_map: dict[str, str]) -> list[tuple[str, list[Any]]]:
    """Every `emit X(args)` in the function body.

    Returns `(event_signature, arguments)` — the signature is the
    canonical form like "Rely(address)" resolved via `sig_map`, for
    matching against on-chain topic0 hashes in the enumerator.
    """
    emissions: list[tuple[str, list[Any]]] = []
    for node in getattr(function, "nodes", []) or []:
        for ir in getattr(node, "irs", []) or []:
            if _ir_name(ir) != "EventCall":
                continue
            name = getattr(ir, "name", "") or ""
            arguments = list(getattr(ir, "arguments", []) or [])
            if not name:
                continue
            signature = sig_map.get(name, name)
            emissions.append((signature, arguments))
    return emissions


def _match_event_to_key(
    emissions: list[tuple[str, list[Any]]],
    key_var: Any,
) -> tuple[str, int] | None:
    """Given the event emissions in a function and the mapping key
    variable, find the event whose argument list contains that key
    variable, returning `(event_signature, key_position)`.

    If multiple events emit the key, return the first — the downstream
    resolver can scrape multiple events per mapping if the writer emits
    more than one, but for allowlist-style mappings there's typically
    one event per direction."""
    key_name = _var_name(key_var)
    if not key_name:
        return None
    for signature, arguments in emissions:
        for i, arg in enumerate(arguments):
            if _var_name(arg) == key_name:
                return signature, i
    return None


def discover_mapping_writer_events(contract: Any) -> list[WriterEventSpec]:
    """Walk every function on `contract` and emit one `WriterEventSpec`
    per `(mapping write, co-emitted event)` pairing found.

    Returns a list because a single mapping can have multiple writers
    in different directions — MakerDAO `wards` typically has `rely`
    (add) and `deny` (remove), emitting `Rely` and `Deny` events
    respectively. The enumerator folds both event streams into one
    set-with-removals view downstream.

    Output is deduplicated on `(mapping_name, event_signature,
    direction)` so the resolver doesn't double-query the same event.
    """
    specs: list[WriterEventSpec] = []
    seen: set[tuple[str, str, str]] = set()
    sig_map = _event_signature_map(contract)
    for function in _contract_functions(contract):
        if getattr(function, "is_constructor", False):
            continue
        written = _written_mappings(function)
        if not written:
            continue
        index_writes = _extract_index_writes(function)
        if not index_writes:
            continue
        emissions = _extract_event_emissions(function, sig_map)
        if not emissions:
            continue
        for mapping_name, key_var, value_var in index_writes:
            direction = _direction_of_write(value_var)
            if direction is None:
                continue
            match = _match_event_to_key(emissions, key_var)
            if match is None:
                continue
            event_signature, key_position = match
            # Event signatures from slither look like "Rely(address)";
            # strip down to the name for human-readable labels.
            event_name = event_signature.split("(", 1)[0]
            key = (mapping_name, event_signature, direction)
            if key in seen:
                continue
            seen.add(key)
            specs.append(
                {
                    "mapping_name": mapping_name,
                    "event_signature": event_signature,
                    "event_name": event_name,
                    "key_position": key_position,
                    "direction": direction,
                    "writer_function": getattr(function, "full_name", getattr(function, "name", "")),
                }
            )
    return specs
