"""Writer-gate analyzer — pass 2 of the predicate pipeline.

For 1-key caller-keyed bool/uint mappings, the predicate builder's
pass 1 conservatively classifies them as ``authority_role="business"``
because the same structural shape covers both:
  - ``claimed[msg.sender]``        (personal flag, business)
  - ``_blacklist[msg.sender]``     (auth, set by an admin)
  - ``wards[msg.sender]``          (auth, self-administered Maker style)

Pass 2 disambiguates by inspecting how the underlying storage var is
*written*. The discriminator both auth and personal-flag shapes share:
**how the writer keys the mapping at write time.**

Rules (v7, simplified initial cut):
  a. If ALL writers are self_keyed (``map[msg.sender] = ...``) →
     personal flag → leave as business.
  b.i. If at least one writer is external_keyed (``map[arg] = ...``)
       AND every external_keyed writer's predicate tree contains
       a ``caller_authority`` or ``delegated_authority`` leaf
       → promote the read leaf to ``caller_authority``.
  b.ii. (Self-administered, like Maker wards): the writer reads the
        same map M as its own gate. Detect by checking the writer's
        predicate tree for a membership leaf reading M. Promote if
        present.
  c. (Open registration / public assignment) — at least one writer
     is external_keyed AND ungated. Leaf stays business; future UI
     can surface the open writer.
  d. (Constructor-only init) — TODO.

Implementation lives outside the per-function builder so it can see
the whole contract.
"""

from __future__ import annotations

from typing import Any

try:
    from slither.slithir.operations import (  # type: ignore[import]
        Assignment,
        Index,
    )
    from slither.slithir.variables import Constant  # type: ignore[import]

    SLITHER_AVAILABLE = True
except Exception:  # pragma: no cover
    SLITHER_AVAILABLE = False

from .predicate_types import LeafPredicate, PredicateTree


def apply_writer_gate_pass(
    contract: Any,
    predicate_trees: dict[str, PredicateTree],
) -> None:
    """Mutates ``predicate_trees`` in place. Promotes 1-key caller-
    keyed membership leaves from authority_role="business" to
    "caller_authority" when the underlying storage var's writers
    are themselves authority-gated.

    Args:
        contract: a Slither Contract object.
        predicate_trees: mapping function full_name → PredicateTree
            from build_predicate_tree's pass 1.
    """
    if not SLITHER_AVAILABLE:
        raise RuntimeError("writer-gate analyzer requires slither")

    # Build storage_var → set of writer functions index.
    writers_by_var: dict[str, list[Any]] = {}
    for fn in contract.functions:
        for sv in fn.state_variables_written:
            writers_by_var.setdefault(sv.name, []).append(fn)

    # For each function, walk its tree and inspect 1-key caller-keyed
    # membership leaves.
    for fn_name, tree in predicate_trees.items():
        if tree is None:
            continue
        _walk_and_promote(tree, writers_by_var, predicate_trees, contract)


# ---------------------------------------------------------------------------
# Tree walk
# ---------------------------------------------------------------------------


def _walk_and_promote(
    tree: PredicateTree,
    writers_by_var: dict[str, list[Any]],
    all_trees: dict[str, PredicateTree],
    contract: Any,
) -> None:
    if tree.get("op") == "LEAF":
        leaf = tree.get("leaf")
        if leaf is not None:
            _maybe_promote_leaf(leaf, writers_by_var, all_trees, contract)
        return
    for child in tree.get("children") or []:
        _walk_and_promote(child, writers_by_var, all_trees, contract)


def _maybe_promote_leaf(
    leaf: LeafPredicate,
    writers_by_var: dict[str, list[Any]],
    all_trees: dict[str, PredicateTree],
    contract: Any,
) -> None:
    if leaf.get("authority_role") != "business":
        return  # already classified
    if leaf.get("kind") != "membership":
        return
    descriptor = leaf.get("set_descriptor")
    if not descriptor:
        return
    keys = descriptor.get("key_sources") or []
    if len(keys) != 1:
        return
    if keys[0]["source"] not in ("msg_sender", "tx_origin", "signature_recovery"):
        return
    storage_var = descriptor.get("storage_var")
    if not storage_var:
        return
    writers = writers_by_var.get(storage_var, [])
    if not writers:
        return
    classification = _classify_writers(storage_var, writers, all_trees)
    if classification == "promote":
        leaf["authority_role"] = "caller_authority"
        leaf["basis"] = list(leaf.get("basis", [])) + [
            f"writer-gate promoted: {storage_var} writers are authority-gated",
        ]


# ---------------------------------------------------------------------------
# Writer classification
# ---------------------------------------------------------------------------


def _classify_writers(
    storage_var: str,
    writers: list[Any],
    all_trees: dict[str, PredicateTree],
) -> str:
    """Returns one of:
    - "promote"  — at least one external_keyed writer is gated
      (rule b.i / b.ii) → leaf gets caller_authority
    - "keep_business" — rule a (all self-keyed) or rule c (open
      registration)
    """
    write_kinds: list[str] = []  # per writer
    has_external_keyed_gated = False
    for fn in writers:
        kinds = _classify_writer_keys(fn, storage_var)
        write_kinds.extend(kinds)
        if "external_keyed" in kinds:
            if _writer_is_gated(fn, storage_var, all_trees):
                has_external_keyed_gated = True

    # Rule a: ALL self_keyed → business.
    if write_kinds and all(k == "self_keyed" for k in write_kinds):
        return "keep_business"

    if has_external_keyed_gated:
        return "promote"

    return "keep_business"


def _classify_writer_keys(fn: Any, storage_var: str) -> list[str]:
    """For each Index+Assignment pair targeting ``storage_var``,
    classify how the index key is sourced. Returns a list of
    self_keyed / external_keyed / constant_keyed classifications,
    one per write site in the function.

    Self_keyed: key is msg.sender (or aliased to it).
    External_keyed: key is a function parameter or computed value.
    Constant_keyed: key is a literal constant.
    """
    classifications: list[str] = []
    # Find Index IRs whose base is the target storage var, and
    # check whether the Index's lvalue is later assigned to. This is
    # a coarse but adequate detection — we don't need full data-flow
    # for the classification, just the immediate write site.
    write_index_lvalues: set[str] = set()
    indexes_by_ref: dict[str, Any] = {}
    for node in fn.nodes:
        for ir in node.irs_ssa or []:
            if isinstance(ir, Index):
                base = ir.variable_left
                base_name = getattr(base, "name", None)
                if base_name == storage_var:
                    indexes_by_ref[ir.lvalue.name] = ir
            elif isinstance(ir, Assignment):
                lv_name = getattr(ir.lvalue, "name", None)
                if lv_name in indexes_by_ref:
                    write_index_lvalues.add(lv_name)

    for ref_name, ix in indexes_by_ref.items():
        if ref_name not in write_index_lvalues:
            continue  # not actually written — pure read
        key = ix.variable_right
        kind = _classify_key(key)
        classifications.append(kind)
    return classifications


def _classify_key(key: Any) -> str:
    """Determine whether the key is msg.sender, a parameter, or a
    constant. Falls back to external_keyed when uncertain."""
    if isinstance(key, Constant):
        return "constant_keyed"
    name = getattr(key, "name", "")
    if name == "msg.sender" or name == "tx.origin":
        return "self_keyed"
    # Heuristic: check the Slither variable type. LocalIRVariable
    # / TemporaryVariable from a parameter or computation maps to
    # external_keyed. ProvenanceEngine could give a more precise
    # answer; for now, anything not msg.sender and not Constant is
    # external_keyed (parameter / computed / view-call).
    return "external_keyed"


def _writer_is_gated(
    fn: Any,
    storage_var: str,
    all_trees: dict[str, PredicateTree],
) -> bool:
    """A writer is "gated" if its own predicate tree contains a
    caller_authority or delegated_authority leaf, OR a membership
    leaf reading the same storage_var (rule b.ii self-administered)."""
    tree = all_trees.get(fn.full_name)
    if tree is None:
        return False
    return _tree_has_authority_or_self_admin(tree, storage_var)


def _tree_has_authority_or_self_admin(tree: PredicateTree, storage_var: str) -> bool:
    if tree.get("op") == "LEAF":
        leaf = tree.get("leaf")
        if leaf is None:
            return False
        if leaf.get("authority_role") in ("caller_authority", "delegated_authority"):
            return True
        # Self-administered (rule b.ii): writer's gate reads the same map.
        if leaf.get("kind") == "membership":
            sd = leaf.get("set_descriptor") or {}
            if sd.get("storage_var") == storage_var:
                return True
        return False
    for child in tree.get("children") or []:
        if _tree_has_authority_or_self_admin(child, storage_var):
            return True
    return False
