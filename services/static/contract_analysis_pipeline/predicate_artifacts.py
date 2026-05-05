"""Build the v2 predicate-tree artifact for a contract.

Runs the full predicate pipeline (``build_predicate_tree`` per
function + ``apply_writer_gate_pass`` + ``apply_reentrancy_pause_pass``
across the contract) and returns a JSON-ready dict keyed on each
externally-callable function's full name.

The artifact is emitted alongside the existing
``contract_analysis.json`` and ``semantic_guards.json`` during
the schema-v2 rollout; downstream consumers (resolver, UI) start
consuming it incrementally without breaking the v1 path.

Convention:
  * present + tree → function is guarded by the tree's predicate.
  * absent → function is unguarded (publicly callable). The
    resolver maps unguarded to ``CapabilityExpr.public`` /
    ``conditional_universal`` per its own rules.

External/public visibility is the boundary we report on — internal/
private functions never appear in the output. We also skip
constructors and fallback/receive functions (their guard semantics
are different and the v1 pipeline excludes them too).
"""

from __future__ import annotations

from typing import Any

from eth_utils.crypto import keccak

from .mapping_events import discover_mapping_writer_events
from .predicate_types import PredicateTree
from .predicates import _helper_engine_cache, build_predicate_tree
from .reentrancy_pause import apply_reentrancy_pause_pass
from .writer_gate import apply_writer_gate_pass

SCHEMA_VERSION = "v2"


def build_predicate_artifacts(contract: Any) -> dict[str, Any]:
    """Return a JSON-serializable dict of predicate trees for every
    external/public function on ``contract``.

    Functions whose tree is ``None`` (no revert paths) are omitted
    from the output. The resolver treats absent entries as
    unguarded.
    """
    # Scope a per-contract helper-engine cache for the cross-fn
    # build path. Multiple functions on the same AC contract
    # often share helpers (grantRole / revokeRole / renounceRole
    # all funnel through onlyRole→_checkRole); this cache makes
    # the second + third cross-fn build effectively free.
    cache_token = _helper_engine_cache.set({})
    try:
        trees: dict[str, PredicateTree] = {}
        for fn in getattr(contract, "functions", []) or []:
            if not _is_externally_callable(fn):
                continue
            tree = build_predicate_tree(fn)
            if tree is None:
                continue
            trees[fn.full_name] = tree
    finally:
        _helper_engine_cache.reset(cache_token)

    # Cross-contract passes mutate trees in place: writer-gate's
    # writer-side analysis can promote 1-key membership leaves to
    # caller_authority once it sees the full set of writers, and
    # reentrancy/pause analyzers cross-reference state-vars across
    # the contract's functions.
    if trees:
        apply_writer_gate_pass(contract, trees)
        apply_reentrancy_pause_pass(contract, trees)
        # D.4 — annotate value-predicate descriptors with
        # ``writer_selectors`` so the MappingTraceAdapter can replay
        # calldata. Selectors are keccak256[:4] of the writer
        # function's canonical signature; the trace adapter expects
        # ``"<selector>|<arg_types>"`` form so it can ABI-decode.
        _annotate_writer_selectors(contract, trees)

    return {
        "schema_version": SCHEMA_VERSION,
        "contract_name": getattr(contract, "name", None),
        "trees": trees,
    }


def _annotate_writer_selectors(contract: Any, trees: dict[str, PredicateTree]) -> None:
    """For every ``mapping_membership`` set descriptor with a
    ``value_predicate``, attach the selectors of the contract's
    writer functions targeting that mapping. ``writer_selectors``
    flows from the static pipeline through to
    ``MappingTraceAdapter`` (D.4).

    No-op when the contract has no writer-event records — the
    adapter simply doesn't match.
    """
    writer_events = discover_mapping_writer_events(contract)
    if not writer_events:
        return
    # Build {mapping_name: [selector_with_argtypes, ...]}.
    by_mapping: dict[str, list[str]] = {}
    seen_per_mapping: dict[str, set[str]] = {}
    for spec in writer_events:
        mapping = spec.get("mapping_name") or ""
        writer_fn_signature = spec.get("writer_function") or ""
        if not mapping or not writer_fn_signature or "(" not in writer_fn_signature:
            continue
        # signature = "name(types)" canonical form, suitable for keccak256.
        selector = "0x" + keccak(text=writer_fn_signature).hex()[:8]
        arg_blob = writer_fn_signature.partition("(")[2].rstrip(")")
        entry = f"{selector}|{arg_blob}" if arg_blob else selector
        bucket = by_mapping.setdefault(mapping, [])
        seen = seen_per_mapping.setdefault(mapping, set())
        if entry not in seen:
            bucket.append(entry)
            seen.add(entry)
    if not by_mapping:
        return

    for tree in trees.values():
        _walk_and_annotate(tree, by_mapping)


def _walk_and_annotate(node: Any, by_mapping: dict[str, list[str]]) -> None:
    if isinstance(node, dict):
        if node.get("kind") == "leaf":
            leaf = node.get("leaf") or {}
            descriptor = leaf.get("set_descriptor") or {}
            mapping = descriptor.get("storage_var")
            if descriptor.get("value_predicate") and mapping in by_mapping and not descriptor.get("writer_selectors"):
                descriptor["writer_selectors"] = list(by_mapping[mapping])
        for child in node.get("children") or []:
            _walk_and_annotate(child, by_mapping)


def _is_externally_callable(fn: Any) -> bool:
    """External or public visibility, AND not a constructor /
    fallback / receive special function. Modifiers are not
    functions in this sense."""
    visibility = getattr(fn, "visibility", None)
    if visibility not in ("external", "public"):
        return False
    if getattr(fn, "is_constructor", False):
        return False
    # Slither tags special functions via name; receive/fallback also
    # have non-standard signatures.
    name = getattr(fn, "name", "") or ""
    if name in ("constructor", "fallback", "receive"):
        return False
    if getattr(fn, "is_fallback", False) or getattr(fn, "is_receive", False):
        return False
    return True
