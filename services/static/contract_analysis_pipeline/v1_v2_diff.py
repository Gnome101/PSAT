"""Compare v1 ``contract_analysis.json`` against v2
``predicate_trees.json`` for the same contract.

The schema-v2 cutover (#18) flips downstream readers from the v1
output to the v2 artifact. This module produces the comparison
harness that validates the flip is safe per contract:

  * ``agreed``: functions both schemas flag as guarded.
  * ``v1_only``: functions v1 marked privileged but v2 produced
    no tree for. These are REGRESSION candidates — the v2
    pipeline isn't seeing a gate the v1 heuristics did.
  * ``v2_only``: functions v2 produced a tree for but v1 didn't
    flag privileged. Could be a v1 false-negative the new
    pipeline catches, or a v2 false-positive (the predicate
    builder over-emitted on a non-auth control flow).
  * ``role_disagreements``: for agreed functions, where the v2
    classification differs from what v1's guard_kinds suggest.

Use ``classify_diff_severity`` to decide whether to gate the
cutover on a per-contract diff.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DiffReport:
    contract_name: str
    agreed: set[str] = field(default_factory=set)
    v1_only: set[str] = field(default_factory=set)
    v2_only: set[str] = field(default_factory=set)
    # function_full_name -> (v1_guard_kinds, v2_authority_roles)
    role_disagreements: dict[str, tuple[list[str], list[str]]] = field(default_factory=dict)


def diff_artifacts(v1_analysis: dict[str, Any], v2_artifact: dict[str, Any]) -> DiffReport:
    """Compute the per-function diff between v1 + v2 outputs.

    ``v1_analysis`` is the dict produced by ``collect_contract_analysis``
    (the contract_analysis.json content). ``v2_artifact`` is the
    output of ``build_predicate_artifacts`` (the predicate_trees.json
    content).
    """
    contract_name = v2_artifact.get("contract_name") or _v1_subject_name(v1_analysis)

    v1_funcs = _v1_privileged_function_names(v1_analysis)
    v2_funcs = set((v2_artifact.get("trees") or {}).keys())

    agreed = v1_funcs & v2_funcs
    v1_only = v1_funcs - v2_funcs
    v2_only = v2_funcs - v1_funcs

    # Role disagreements: for agreed functions, surface where the
    # v2 authority_roles diverge from the v1 guard_kinds. v1's guard_
    # kinds are coarse strings ("access_control", "ownership", etc.);
    # v2's authority_roles are the typed enum. We don't try to map
    # them 1:1 — we just report both sides so a reviewer can decide.
    v1_kinds_by_fn = _v1_guard_kinds_by_function(v1_analysis)
    role_disagreements: dict[str, tuple[list[str], list[str]]] = {}
    for fn in agreed:
        v1_kinds = v1_kinds_by_fn.get(fn, [])
        v2_roles = _v2_authority_roles(v2_artifact["trees"][fn])
        if not _kinds_compatible(v1_kinds, v2_roles):
            role_disagreements[fn] = (v1_kinds, v2_roles)

    return DiffReport(
        contract_name=contract_name or "<unknown>",
        agreed=agreed,
        v1_only=v1_only,
        v2_only=v2_only,
        role_disagreements=role_disagreements,
    )


def classify_diff_severity(report: DiffReport) -> str:
    """Cutover gate-keeper.

    * ``regression`` — v1 flagged guard(s) v2 missed; cutover
      must be blocked or the v2 pipeline must extend before flip.
    * ``new_coverage`` — v2 catches gates v1 missed, no
      regressions. Safe to cut over; v1 was the loose schema.
    * ``role_drift`` — only role-classification differs on
      agreed functions; review case-by-case.
    * ``clean`` — exact agreement.
    """
    if report.v1_only:
        return "regression"
    if report.v2_only and not report.role_disagreements:
        return "new_coverage"
    if report.role_disagreements:
        return "role_drift"
    return "clean"


# ---------------------------------------------------------------------------
# v1 readers
# ---------------------------------------------------------------------------


def _v1_privileged_function_names(v1: dict[str, Any]) -> set[str]:
    """Pull the set of full_name strings v1 marked privileged."""
    ac = v1.get("access_control") or {}
    funcs = ac.get("privileged_functions") or []
    return {entry["function"] for entry in funcs if isinstance(entry, dict) and "function" in entry}


def _v1_guard_kinds_by_function(v1: dict[str, Any]) -> dict[str, list[str]]:
    ac = v1.get("access_control") or {}
    funcs = ac.get("privileged_functions") or []
    return {
        entry["function"]: list(entry.get("guard_kinds") or [])
        for entry in funcs
        if isinstance(entry, dict) and "function" in entry
    }


def _v1_subject_name(v1: dict[str, Any]) -> str | None:
    subj = v1.get("subject") or {}
    return subj.get("name")


# ---------------------------------------------------------------------------
# v2 readers
# ---------------------------------------------------------------------------


def _v2_authority_roles(tree: dict[str, Any] | None) -> list[str]:
    """Collect the distinct authority_role strings across every
    LEAF in the tree."""
    if not tree:
        return []
    roles: set[str] = set()
    _walk_roles(tree, roles)
    return sorted(roles)


def _walk_roles(node: dict[str, Any], out: set[str]) -> None:
    if node.get("op") == "LEAF":
        leaf = node.get("leaf") or {}
        role = leaf.get("authority_role")
        if role:
            out.add(role)
        return
    for child in node.get("children") or []:
        _walk_roles(child, out)


# ---------------------------------------------------------------------------
# Mapping: do v1 guard_kinds and v2 authority_roles agree?
# ---------------------------------------------------------------------------


# v1 guard_kinds (loose strings the v1 pipeline emits) -> set of v2
# authority_roles that are compatible. The mapping is intentionally
# permissive — v1 conflates many shapes under a single label, so we
# accept any of the typed roles that v1 might cover under that label.
#
# Keys are the ACTUAL guard kinds v1's graph.py/build_permission_graph
# appends to ``privileged_functions[].guard_kinds`` (verified empirically
# against oz_ownable / oz_ac_inline / oz_pausable fixtures). Earlier
# versions of this map used aspirational labels like "access_control" /
# "ownership" / "roles" that v1 NEVER emits — every lookup returned the
# default empty set, then the not-expected branch returned True
# unconditionally, meaning role_drift was never flagged at the cutover
# gate. Without this fix the cutover_dry_run couldn't surface real
# v1↔v2 classification disagreements.
_KIND_COMPAT = {
    # graph.py guard kinds (the canonical v1 emits)
    "caller_equals_storage": {"caller_authority"},
    "caller_in_mapping": {"caller_authority"},
    "role_membership_check": {"caller_authority"},
    "caller_via_helper_function": {"caller_authority", "delegated_authority"},
    "external_authority_check": {"caller_authority", "delegated_authority"},
    "inline_assembly_check": {"caller_authority"},
    # caller_sinks.py kinds (when piped through to graph entries —
    # rare but possible on synthetic/legacy pipelines)
    "caller_equals": {"caller_authority"},
    "caller_internal_call": {"caller_authority", "delegated_authority"},
    "caller_signature": {"caller_authority"},
    "caller_merkle": {"caller_authority"},
    # Legacy labels — kept defensively in case any v1 emit path uses
    # these older strings (they appear in older snapshots in the wild).
    "access_control": {"caller_authority", "delegated_authority"},
    "ownership": {"caller_authority"},
    "roles": {"caller_authority"},
    "pause": {"pause"},
    "reentrancy": {"reentrancy"},
    "time": {"time"},
    "self_permission": {"caller_authority"},
}


def _kinds_compatible(v1_kinds: list[str], v2_roles: list[str]) -> bool:
    """``True`` when the v2 roles are explainable by the v1 kinds.

    If v1 emitted no kinds (rare — usually means the function was
    privileged for some external-call reason) we treat any v2 set
    as compatible.
    """
    if not v1_kinds:
        return True
    expected: set[str] = set()
    for k in v1_kinds:
        expected |= _KIND_COMPAT.get(k, set())
    if not expected:
        # v1 used a kind we don't have a mapping for — don't flag
        # role_drift on unrecognized labels.
        return True
    return any(role in expected for role in v2_roles)
