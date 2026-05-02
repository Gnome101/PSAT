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

from .predicate_types import PredicateTree
from .predicates import build_predicate_tree
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
    trees: dict[str, PredicateTree] = {}
    for fn in getattr(contract, "functions", []) or []:
        if not _is_externally_callable(fn):
            continue
        tree = build_predicate_tree(fn)
        if tree is None:
            continue
        trees[fn.full_name] = tree

    # Cross-contract passes mutate trees in place: writer-gate's
    # writer-side analysis can promote 1-key membership leaves to
    # caller_authority once it sees the full set of writers, and
    # reentrancy/pause analyzers cross-reference state-vars across
    # the contract's functions.
    if trees:
        apply_writer_gate_pass(contract, trees)
        apply_reentrancy_pause_pass(contract, trees)

    return {
        "schema_version": SCHEMA_VERSION,
        "contract_name": getattr(contract, "name", None),
        "trees": trees,
    }


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
