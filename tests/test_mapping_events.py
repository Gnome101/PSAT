"""Phase 3a — writer-event discovery for mapping allowlist patterns.

Covers `discover_mapping_writer_events` on the MakerDAO `wards` family,
OZ-style bool whitelists, `delete`-based removals, and multi-arg
events where the key isn't the first parameter. All mocks use slither's
IR class-name convention (`type(ir).__name__`) so the extractor's
filters match.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static.contract_analysis_pipeline.mapping_events import (  # noqa: E402
    discover_mapping_writer_events,
)


def _named(cls_name: str, **attrs: Any) -> SimpleNamespace:
    subclass = type(cls_name, (SimpleNamespace,), {})
    subclass.__name__ = cls_name
    return subclass(**attrs)


def _mapping(name: str, value_type: str = "uint256") -> SimpleNamespace:
    subclass = type("StateVariable", (SimpleNamespace,), {})
    subclass.__name__ = "StateVariable"
    return subclass(name=name, type=f"mapping(address => {value_type})")


def _local(name: str, type_str: str = "address") -> SimpleNamespace:
    subclass = type("LocalVariable", (SimpleNamespace,), {})
    subclass.__name__ = "LocalVariable"
    return subclass(name=name, type=type_str)


def _tmp(name: str, type_str: str = "uint256") -> SimpleNamespace:
    subclass = type("TemporaryVariable", (SimpleNamespace,), {})
    subclass.__name__ = "TemporaryVariable"
    return subclass(name=name, type=type_str)


def _constant(value: Any, type_str: str = "uint256") -> SimpleNamespace:
    subclass = type("Constant", (SimpleNamespace,), {})
    subclass.__name__ = "Constant"
    return subclass(name=str(value), type=type_str, value=value)


def _index(base: Any, key: Any, lvalue: Any) -> SimpleNamespace:
    return _named("Index", variable_left=base, variable_right=key, lvalue=lvalue)


def _assignment(lvalue: Any, rvalue: Any) -> SimpleNamespace:
    return _named("Assignment", lvalue=lvalue, rvalue=rvalue)


def _delete(lvalue: Any) -> SimpleNamespace:
    return _named("Delete", lvalue=lvalue)


def _event_call(signature: str, arguments: list[Any]) -> SimpleNamespace:
    return _named("EventCall", name=signature, arguments=arguments)


def _node(irs: list[Any]) -> SimpleNamespace:
    return SimpleNamespace(irs=irs, node_id=0)


def _function(
    name: str,
    nodes: list[Any],
    *,
    written: list[Any] | None = None,
    is_constructor: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        full_name=f"{name}(address)",
        nodes=nodes,
        all_state_variables_written=lambda w=(written or []): w,
        is_constructor=is_constructor,
    )


def _contract(functions: list[Any]) -> SimpleNamespace:
    return SimpleNamespace(name="TestContract", functions=functions, inheritance=[])


# ---------------------------------------------------------------------------
# MakerDAO wards pattern
# ---------------------------------------------------------------------------


def test_makerdao_rely_adds_to_wards():
    """`rely(address guy) auth { wards[guy] = 1; emit Rely(guy); }` —
    yields one WriterEventSpec with direction=add."""
    wards = _mapping("wards")
    guy = _local("guy")
    index_lv = _tmp("TMP_0")
    rely_fn = _function(
        "rely",
        nodes=[
            _node(
                [
                    _index(wards, guy, index_lv),
                    _assignment(index_lv, _constant(1)),
                    _event_call("Rely(address)", [guy]),
                ]
            )
        ],
        written=[wards],
    )
    specs = discover_mapping_writer_events(_contract([rely_fn]))
    assert len(specs) == 1
    s = specs[0]
    assert s["mapping_name"] == "wards"
    assert s["event_signature"] == "Rely(address)"
    assert s["event_name"] == "Rely"
    assert s["direction"] == "add"
    assert s["key_position"] == 0


def test_makerdao_deny_removes_via_zero_write():
    """`deny(address guy) auth { wards[guy] = 0; emit Deny(guy); }` —
    direction=remove."""
    wards = _mapping("wards")
    guy = _local("guy")
    index_lv = _tmp("TMP_0")
    deny_fn = _function(
        "deny",
        nodes=[
            _node(
                [
                    _index(wards, guy, index_lv),
                    _assignment(index_lv, _constant(0)),
                    _event_call("Deny(address)", [guy]),
                ]
            )
        ],
        written=[wards],
    )
    specs = discover_mapping_writer_events(_contract([deny_fn]))
    assert len(specs) == 1
    assert specs[0]["direction"] == "remove"


def test_rely_and_deny_together_produce_two_specs():
    """Both writers present — we get one spec per direction."""
    wards = _mapping("wards")
    guy_a = _local("guy_a")
    guy_b = _local("guy_b")
    rely_fn = _function(
        "rely",
        nodes=[
            _node(
                [
                    _index(wards, guy_a, _tmp("T1")),
                    _assignment(_tmp("T1"), _constant(1)),
                    _event_call("Rely(address)", [guy_a]),
                ]
            )
        ],
        written=[wards],
    )
    deny_fn = _function(
        "deny",
        nodes=[
            _node(
                [
                    _index(wards, guy_b, _tmp("T2")),
                    _assignment(_tmp("T2"), _constant(0)),
                    _event_call("Deny(address)", [guy_b]),
                ]
            )
        ],
        written=[wards],
    )
    specs = discover_mapping_writer_events(_contract([rely_fn, deny_fn]))
    directions = {s["direction"] for s in specs}
    assert directions == {"add", "remove"}
    assert all(s["mapping_name"] == "wards" for s in specs)


# ---------------------------------------------------------------------------
# OZ-style bool whitelist
# ---------------------------------------------------------------------------


def test_bool_mapping_true_is_add():
    """`whitelist[x] = true` + `emit Whitelisted(x)` — direction=add."""
    whitelist = _mapping("whitelist", value_type="bool")
    x = _local("x")
    lv = _tmp("T0", type_str="bool")
    fn = _function(
        "whitelist_user",
        nodes=[
            _node(
                [
                    _index(whitelist, x, lv),
                    _assignment(lv, _constant(True, type_str="bool")),
                    _event_call("Whitelisted(address)", [x]),
                ]
            )
        ],
        written=[whitelist],
    )
    specs = discover_mapping_writer_events(_contract([fn]))
    assert len(specs) == 1
    assert specs[0]["direction"] == "add"


def test_bool_mapping_false_is_remove():
    whitelist = _mapping("whitelist", value_type="bool")
    x = _local("x")
    lv = _tmp("T0", type_str="bool")
    fn = _function(
        "unwhitelist",
        nodes=[
            _node(
                [
                    _index(whitelist, x, lv),
                    _assignment(lv, _constant(False, type_str="bool")),
                    _event_call("Unwhitelisted(address)", [x]),
                ]
            )
        ],
        written=[whitelist],
    )
    specs = discover_mapping_writer_events(_contract([fn]))
    assert len(specs) == 1
    assert specs[0]["direction"] == "remove"


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete_is_remove():
    """`delete wards[guy]; emit Deny(guy);` — treated as remove even
    though there's no explicit assignment. Slither emits a Delete IR."""
    wards = _mapping("wards")
    guy = _local("guy")
    lv = _tmp("T0")
    fn = _function(
        "denyViaDelete",
        nodes=[
            _node(
                [
                    _index(wards, guy, lv),
                    _delete(lv),
                    _event_call("Deny(address)", [guy]),
                ]
            )
        ],
        written=[wards],
    )
    specs = discover_mapping_writer_events(_contract([fn]))
    assert len(specs) == 1
    assert specs[0]["direction"] == "remove"


# ---------------------------------------------------------------------------
# Negative cases
# ---------------------------------------------------------------------------


def test_write_with_no_emit_is_skipped():
    """A mapping write without a co-emitted event is un-enumerable —
    skipped rather than emitting a spec the enumerator can't use."""
    wards = _mapping("wards")
    guy = _local("guy")
    lv = _tmp("T0")
    fn = _function(
        "relyQuiet",
        nodes=[_node([_index(wards, guy, lv), _assignment(lv, _constant(1))])],
        written=[wards],
    )
    assert discover_mapping_writer_events(_contract([fn])) == []


def test_emit_without_matching_key_arg_is_skipped():
    """The function emits an event but the event's args don't include
    the mapping key. Couldn't enumerate from the event, so skip."""
    wards = _mapping("wards")
    guy = _local("guy")
    other = _local("other")
    lv = _tmp("T0")
    fn = _function(
        "rely_odd",
        nodes=[
            _node(
                [
                    _index(wards, guy, lv),
                    _assignment(lv, _constant(1)),
                    _event_call("SomeOther(address)", [other]),
                ]
            )
        ],
        written=[wards],
    )
    assert discover_mapping_writer_events(_contract([fn])) == []


def test_non_address_keyed_mapping_skipped():
    """`mapping(uint256 => bool) roleByIndex` — not an allowlist we
    can enumerate via address-keyed events. Skip."""
    subclass = type("StateVariable", (SimpleNamespace,), {})
    subclass.__name__ = "StateVariable"
    role_mapping = subclass(name="roleByIndex", type="mapping(uint256 => bool)")
    key = _local("key", type_str="uint256")
    lv = _tmp("T0", type_str="bool")
    fn = _function(
        "setRole",
        nodes=[
            _node(
                [
                    _index(role_mapping, key, lv),
                    _assignment(lv, _constant(True, type_str="bool")),
                    _event_call("RoleSet(uint256)", [key]),
                ]
            )
        ],
        written=[role_mapping],
    )
    assert discover_mapping_writer_events(_contract([fn])) == []


def test_constructor_skipped():
    """Writes in the constructor are initial state, not enumerable —
    skip (the enumerator scrapes events from deploy-block onward
    separately, not via constructor writes)."""
    wards = _mapping("wards")
    guy = _local("guy")
    lv = _tmp("T0")
    fn = _function(
        "constructor",
        nodes=[
            _node(
                [
                    _index(wards, guy, lv),
                    _assignment(lv, _constant(1)),
                    _event_call("Rely(address)", [guy]),
                ]
            )
        ],
        written=[wards],
        is_constructor=True,
    )
    assert discover_mapping_writer_events(_contract([fn])) == []


def test_non_literal_value_skipped():
    """`wards[guy] = someVariable;` — we can't statically tell if
    it's add or remove. Skip rather than emit an ambiguous spec."""
    wards = _mapping("wards")
    guy = _local("guy")
    some = _local("someValue", "uint256")
    lv = _tmp("T0")
    fn = _function(
        "setWard",
        nodes=[
            _node(
                [
                    _index(wards, guy, lv),
                    _assignment(lv, some),
                    _event_call("WardSet(address)", [guy]),
                ]
            )
        ],
        written=[wards],
    )
    assert discover_mapping_writer_events(_contract([fn])) == []


def test_multi_arg_event_with_key_not_first():
    """`emit UserSet(uint256 id, address guy)` where `guy` is arg 1 —
    key_position must be 1."""
    wards = _mapping("wards")
    id_var = _local("id", "uint256")
    guy = _local("guy", "address")
    lv = _tmp("T0")
    fn = _function(
        "setWard",
        nodes=[
            _node(
                [
                    _index(wards, guy, lv),
                    _assignment(lv, _constant(1)),
                    _event_call("UserSet(uint256,address)", [id_var, guy]),
                ]
            )
        ],
        written=[wards],
    )
    specs = discover_mapping_writer_events(_contract([fn]))
    assert len(specs) == 1
    assert specs[0]["key_position"] == 1


def test_dedupes_on_mapping_event_direction():
    """Two different writer functions that emit the same `(mapping,
    event, direction)` triple shouldn't produce two specs — the
    enumerator only queries each event stream once."""
    wards = _mapping("wards")
    guy_a = _local("guy_a")
    guy_b = _local("guy_b")
    lv1 = _tmp("T1")
    lv2 = _tmp("T2")
    fn_a = _function(
        "relyFromAdmin",
        nodes=[
            _node(
                [
                    _index(wards, guy_a, lv1),
                    _assignment(lv1, _constant(1)),
                    _event_call("Rely(address)", [guy_a]),
                ]
            )
        ],
        written=[wards],
    )
    fn_b = _function(
        "relyFromGovernor",
        nodes=[
            _node(
                [
                    _index(wards, guy_b, lv2),
                    _assignment(lv2, _constant(1)),
                    _event_call("Rely(address)", [guy_b]),
                ]
            )
        ],
        written=[wards],
    )
    specs = discover_mapping_writer_events(_contract([fn_a, fn_b]))
    assert len(specs) == 1


def test_empty_contract_returns_empty():
    assert discover_mapping_writer_events(_contract([])) == []


def test_bare_event_name_in_ir_resolves_to_canonical_signature():
    """Regression: slither's EventCall IR exposes `.name` as the bare
    event name ("Rely"), not the canonical signature ("Rely(address)").
    We must look the full name up on `contract.events` so the downstream
    keccak(topic0) matches the on-chain hash — otherwise Hypersync
    returns zero logs.

    Pre-fix: the stored `event_signature` was "Rely", which keccak'd to
    the wrong topic0 and produced 0 principals for Sky.
    """
    wards = _mapping("wards")
    guy = _local("guy")
    index_lv = _tmp("TMP_0")
    # EventCall IR name is the bare event name — what slither really emits.
    rely_fn = _function(
        "rely",
        nodes=[
            _node(
                [
                    _index(wards, guy, index_lv),
                    _assignment(index_lv, _constant(1)),
                    _event_call("Rely", [guy]),  # bare name, as slither emits it
                ]
            )
        ],
        written=[wards],
    )
    rely_event_decl = SimpleNamespace(name="Rely", full_name="Rely(address)")
    contract = SimpleNamespace(
        name="TestContract", functions=[rely_fn], inheritance=[], events=[rely_event_decl]
    )
    specs = discover_mapping_writer_events(contract)
    assert len(specs) == 1
    assert specs[0]["event_signature"] == "Rely(address)"
    assert specs[0]["event_name"] == "Rely"
