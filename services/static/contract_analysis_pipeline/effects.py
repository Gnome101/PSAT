"""Build the v2 ``effects`` artifact for a contract.

Walks Slither IR for every externally-callable function on a contract
and emits a typed record describing the function's *effects*: state
writes, external calls, delegatecalls, contract creations, and
selfdestructs — including those reached transitively through internal
calls. The artifact is the v2 carrier replacing
``permission_graph["sinks"]`` for downstream consumers
(``cross_contract.py``, ``tracking.py``, ``effective_permissions.py``).

Why a separate artifact (vs. extending ``predicate_trees``):
``predicate_trees`` deliberately omits *unguarded* functions
(``predicate_artifacts.py:44``) — the resolver treats absence as
"public / unguarded". For sink/effect discovery we want a record per
externally-callable function regardless of guard structure, so a
publicly callable sensitive action (e.g. unprotected ``mint``) is
still surfaced to the policy stage.

Function inclusion (matches v1 graph behavior + a deliberate addition):
  * external/public functions: included.
  * constructor: skipped (matches ``predicate_artifacts._is_externally_callable``;
    constructor effects are tracked elsewhere).
  * fallback / receive: INCLUDED. They have real effect semantics —
    receive can hold ETH; fallback often delegatecalls. The
    predicate-tree builder skips them because their "guard" semantics
    are unusual, but that's not a reason to drop them from sink
    discovery.
  * internal / private: never appear directly; their effects are
    surfaced through their external callers via transitive walk.
"""

from __future__ import annotations

from typing import Any, TypedDict

from eth_utils.crypto import keccak

from .shared import (
    _function_effects,
)
from .summaries import (
    _action_summary,
    _effect_labels,
)

SCHEMA_VERSION = "v2"


class SinkRecord(TypedDict):
    """One sink reachable from a given external function. ``id`` is a
    stable cross-reference; ``function`` is the *originating* external
    function (the entry-point), not the unit where the IR lives — that
    way consumers can group sinks by entry without re-walking
    internal calls."""

    id: str
    function: str
    kind: str  # state_write | external_call | delegatecall | contract_creation | selfdestruct
    target: str
    selector: str | None


class EffectInfo(TypedDict):
    function: str
    selector: str
    abi_signature: str
    sinks: list[SinkRecord]
    effects: list[str]
    effect_labels: list[str]
    effect_targets: list[str]
    action_summary: str
    writer_selectors: list[str]


class EffectsArtifact(TypedDict):
    schema_version: str
    contract_name: str | None
    functions: dict[str, EffectInfo]


# ---------------------------------------------------------------------------
# Function inclusion (mirrors predicate_artifacts._is_externally_callable but
# keeps fallback/receive — see module docstring).
# ---------------------------------------------------------------------------


def _is_externally_observable(fn: Any) -> bool:
    """External/public OR fallback/receive. Skips constructor and
    internal/private functions."""
    if getattr(fn, "is_constructor", False):
        return False
    if getattr(fn, "is_fallback", False) or getattr(fn, "is_receive", False):
        return True
    name = getattr(fn, "name", "") or ""
    if name == "constructor":
        return False
    if name in ("fallback", "receive"):
        return True
    visibility = getattr(fn, "visibility", None)
    return visibility in ("external", "public")


# ---------------------------------------------------------------------------
# Sink discovery (transitive across internal calls).
# ---------------------------------------------------------------------------


def _node_irs(node: Any) -> list[Any]:
    return list(getattr(node, "irs", []) or [])


def _function_full_name(fn: Any) -> str:
    name = getattr(fn, "full_name", None) or getattr(fn, "name", None) or "<anonymous>"
    return str(name)


def _selector_for(signature: str) -> str | None:
    """Compute keccak256[:4] of a canonical ``name(types)`` signature.
    Returns ``None`` if the signature isn't in canonical form (e.g.
    fallback/receive, which have no selector)."""
    if not signature or "(" not in signature or ")" not in signature:
        return None
    return "0x" + keccak(text=signature).hex()[:8]


def _sink_id(function_name: str, kind: str, target: str, idx: int) -> str:
    """Stable, idx-disambiguated ID. The ``idx`` keeps multiple sinks
    of the same (kind, target) on one function distinct (e.g. two
    state_write sinks to the same var from different branches).

    Format mirrors the v1 graph format ``<function>:<idx>:<kind>:<target>``
    so legacy tests asserting ``endswith(":<kind>:<target>")`` keep
    passing through Phase A.6 / D.3."""
    return f"{function_name}:sink{idx}:{kind}:{target}"


def _node_kind_state_writes(node: Any) -> list[str]:
    """Return the names of state variables written at this node."""
    names: list[str] = []
    for variable in getattr(node, "state_variables_written", []) or []:
        name = getattr(variable, "name", "") or ""
        if name:
            names.append(name)
    return names


def _classify_node_irs(node: Any) -> list[tuple[str, str]]:
    """Classify the non-state-write sinks at a node. Returns a list of
    ``(kind, target)`` pairs.

    State writes are handled separately — Slither's
    ``node.state_variables_written`` is more reliable than walking IR
    assignments by hand."""
    out: list[tuple[str, str]] = []
    for ir in _node_irs(node):
        op = type(ir).__name__
        if op == "NewContract":
            target = getattr(ir, "contract_name", None) or str(getattr(ir, "contract_created", "")) or "unknown"
            out.append(("contract_creation", str(target)))
        elif op in ("HighLevelCall", "LibraryCall"):
            destination = getattr(ir, "destination", None)
            destination_name = getattr(destination, "name", None) or str(destination) or "unknown"
            function_name = getattr(ir, "function_name", None) or "call"
            # LibraryCall's "destination" is in its first argument.
            if op == "LibraryCall":
                arguments = list(getattr(ir, "arguments", []) or [])
                if arguments:
                    arg = arguments[0]
                    destination_name = getattr(arg, "name", None) or str(arg) or destination_name
            out.append(("external_call", f"{destination_name}.{function_name}"))
        elif op == "LowLevelCall":
            target = getattr(getattr(ir, "destination", None), "name", None) or str(
                getattr(ir, "destination", None) or "unknown"
            )
            function_name = str(getattr(ir, "function_name", "") or "")
            if function_name == "delegatecall":
                out.append(("delegatecall", str(target)))
            else:
                out.append(("external_call", f"{target}.{function_name or 'call'}"))
        elif op == "SolidityCall":
            function_name = getattr(getattr(ir, "function", None), "name", "") or ""
            if function_name.startswith("selfdestruct("):
                out.append(("selfdestruct", "selfdestruct"))
    return out


def _walk_unit_for_sinks(
    unit: Any,
    visited: set[Any],
) -> list[tuple[str, str]]:
    """Recursively gather (kind, target) pairs from ``unit`` and any
    internal/library callees. Returns a flat list (de-dup happens at
    the caller level so we can keep distinct indices)."""
    unit_key = getattr(unit, "canonical_name", None) or getattr(unit, "full_name", None) or id(unit)
    if unit_key in visited:
        return []
    visited.add(unit_key)

    found: list[tuple[str, str]] = []
    for node in getattr(unit, "nodes", []) or []:
        for var_name in _node_kind_state_writes(node):
            found.append(("state_write", var_name))
        found.extend(_classify_node_irs(node))
        # Recurse into internal/library callees so transitive writes
        # surface on the entry-point's record.
        for ir in _node_irs(node):
            op = type(ir).__name__
            if op not in ("InternalCall", "LibraryCall"):
                continue
            callee = getattr(ir, "function", None)
            if callee is None or not getattr(callee, "nodes", None):
                continue
            found.extend(_walk_unit_for_sinks(callee, visited))
    return found


def _build_sink_records(function: Any) -> list[SinkRecord]:
    """One sink per (kind, target) pair we discover, transitively
    deduped while preserving order. The selector field on
    ``SinkRecord`` is per-sink, not per-function: only ``external_call``
    sinks carry one (the called function's selector when it can be
    formed; we only have the function name from IR, not its full
    canonical signature, so this is best-effort)."""
    function_name = _function_full_name(function)
    pairs = _walk_unit_for_sinks(function, set())

    out: list[SinkRecord] = []
    seen: set[tuple[str, str]] = set()
    for kind, target in pairs:
        key = (kind, target)
        if key in seen:
            continue
        seen.add(key)
        idx = len(out)
        record: SinkRecord = {
            "id": _sink_id(function_name, kind, target, idx),
            "function": function_name,
            "kind": kind,
            "target": target,
            "selector": None,
        }
        out.append(record)
    return out


# ---------------------------------------------------------------------------
# Effects + labels + writer selectors per function.
# ---------------------------------------------------------------------------


def _effect_targets_from_sinks(sinks: list[SinkRecord]) -> list[str]:
    """The state-var name(s) this function writes — sourced from the
    sink list. Mirrors the v1 graph's ``effect_targets`` for state-writes;
    consumers of effect_targets only key on writes (see _effect_labels'
    targets_lower lookups for ".transfer", ".mint" etc., which we
    augment from external_call sinks)."""
    seen: list[str] = []
    seen_set: set[str] = set()
    for sink in sinks:
        if sink["kind"] == "state_write" and sink["target"] not in seen_set:
            seen.append(sink["target"])
            seen_set.add(sink["target"])
        elif sink["kind"] == "external_call" and sink["target"] not in seen_set:
            # ``_effect_labels`` checks targets ending in ``.mint`` /
            # ``.transfer`` etc., so we include external-call dotted
            # targets here for compatibility with that label path.
            seen.append(sink["target"])
            seen_set.add(sink["target"])
    return seen


def _writer_selectors_for(function: Any, sinks: list[SinkRecord]) -> list[str]:
    """For a state-write function, its own selector is the relevant
    writer selector (HyperSync replays this function to attribute the
    write). Returns a list because some pipelines accumulate multiple
    selectors per logical writer (overloads)."""
    has_state_write = any(s["kind"] == "state_write" for s in sinks)
    if not has_state_write:
        return []
    signature = _function_full_name(function)
    selector = _selector_for(signature)
    if selector is None:
        return []
    return [selector]


def _effect_info_for_function(function: Any) -> EffectInfo:
    sinks = _build_sink_records(function)
    effects = _function_effects(function)

    # ``effect_targets`` for label inference includes both state-write
    # var names and external-call dotted targets (so ``.mint``-style
    # detection still fires). The downstream "what state does this
    # write" consumer (tracking.py) keys only on state_write sinks.
    effect_targets = _effect_targets_from_sinks(sinks)

    # _effect_labels takes a synthetic graph-entry analog so its
    # ``sink_kinds`` layer still triggers (delegatecall_execution,
    # selfdestruct_capability, contract_deployment).
    sink_kinds = sorted({s["kind"] for s in sinks})
    effect_context = {
        "effects": list(effects),
        "effect_targets": list(effect_targets),
        "sink_kinds": sink_kinds,
    }
    labels = _effect_labels(function, list(effects), list(effect_targets), effect_context)
    # Functions with external_call sinks but no specific (mint/burn/asset/etc)
    # label get ``external_contract_call`` — the v1 graph emitted this via
    # the ``privileged_external_call`` effect when the sink was guarded.
    has_external_call = any(s["kind"] == "external_call" for s in sinks)
    if has_external_call and not any(
        lbl
        in (
            "external_contract_call",
            "arbitrary_external_call",
            "asset_send",
            "asset_pull",
            "mint",
            "burn",
            "authority_update",
            "hook_update",
            "ownership_transfer",
            "role_management",
            "pause_toggle",
            "implementation_update",
            "timelock_operation",
            "contract_deployment",
            "delegatecall_execution",
            "selfdestruct_capability",
        )
        for lbl in labels
    ):
        labels.append("external_contract_call")
    summary = _action_summary(labels, list(effect_targets))

    signature = _function_full_name(function)
    selector = _selector_for(signature) or ""
    return {
        "function": signature,
        "selector": selector,
        "abi_signature": signature,
        "sinks": sinks,
        "effects": list(effects),
        "effect_labels": list(labels),
        # Includes both state-write var names and external-call dotted
        # targets (mirrors v1 graph behavior + the inputs ``_effect_labels``
        # / ``_action_summary`` expect). Tracking.py reads ``sinks``
        # directly to enumerate state_write writers.
        "effect_targets": list(effect_targets),
        "action_summary": summary,
        "writer_selectors": _writer_selectors_for(function, sinks),
    }


# ---------------------------------------------------------------------------
# Top-level entry.
# ---------------------------------------------------------------------------


def build_effects(contract: Any) -> EffectsArtifact:
    """Return the ``effects`` artifact for ``contract``: one
    ``EffectInfo`` per externally-observable function (external,
    public, fallback, receive)."""
    functions: dict[str, EffectInfo] = {}
    for fn in getattr(contract, "functions", []) or []:
        if not _is_externally_observable(fn):
            continue
        info = _effect_info_for_function(fn)
        functions[info["function"]] = info

    return {
        "schema_version": SCHEMA_VERSION,
        "contract_name": getattr(contract, "name", None),
        "functions": functions,
    }
