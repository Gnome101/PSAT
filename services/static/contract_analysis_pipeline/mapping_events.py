"""Discover writer-event / mapping-write pairings.

For an `allowlist` pattern like `wards[guy] = 1; emit Rely(guy);`, pairs the
`Rely(address)` event with `wards` so the resolver can enumerate current
members via Hypersync. Direction (add/remove) is inferred from the written
value: `= 1`/`= true` is add, `= 0`/`= false`/`delete x[k]` is remove.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict

from .shared import _contract_functions


class WriterEventSpec(TypedDict):
    mapping_name: str
    event_signature: str
    event_name: str
    # 0-indexed position of the address argument in the event that matches
    # the mapping key — used to pick the right topic/data slot when decoding.
    key_position: int
    direction: Literal["add", "remove"]
    writer_function: str


def _ir_name(ir: Any) -> str:
    return type(ir).__name__ if ir is not None else ""


def _var_name(item: Any) -> str:
    if item is None:
        return ""
    return getattr(item, "name", None) or str(item)


def _is_mapping_address_keyed(variable: Any) -> bool:
    type_str = str(getattr(variable, "type", "") or "")
    return type_str.startswith("mapping(address =>") or type_str.startswith("mapping(address=>")


def _written_mappings(function: Any) -> list[Any]:
    return [v for v in function.all_state_variables_written() if _is_mapping_address_keyed(v)]


def _extract_index_writes(function: Any) -> list[tuple[str, Any, Any]]:
    """Return `(mapping_name, key_var, value_var)` for every `X[key] = value`
    (or `delete X[key]`, with `value_var=None`) in the function body."""
    triples: list[tuple[str, Any, Any]] = []
    for node in getattr(function, "nodes", []) or []:
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
    # EventCall IR only carries the bare name, but topic0 is keccak of
    # the canonical signature — so we resolve via the Event declaration's
    # full_name. First-declared wins on name collisions.
    mapping: dict[str, str] = {}
    for event in getattr(contract, "events", []) or []:
        name = getattr(event, "name", "") or ""
        full = getattr(event, "full_name", "") or name
        if name and name not in mapping:
            mapping[name] = full
    return mapping


def _extract_event_emissions(function: Any, sig_map: dict[str, str]) -> list[tuple[str, list[Any]]]:
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
    """Return `(event_signature, key_position)` for the first event whose
    argument list contains `key_var`."""
    key_name = _var_name(key_var)
    if not key_name:
        return None
    for signature, arguments in emissions:
        for i, arg in enumerate(arguments):
            if _var_name(arg) == key_name:
                return signature, i
    return None


def discover_mapping_writer_events(contract: Any) -> list[WriterEventSpec]:
    """Emit one `WriterEventSpec` per `(mapping write, co-emitted event)`
    pairing found on `contract`. Deduplicated on
    `(mapping_name, event_signature, direction)`."""
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
