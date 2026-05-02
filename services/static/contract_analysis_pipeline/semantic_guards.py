"""``semantic_guards`` artifact emitter.

Schema-v2 cutover state: this module now produces the artifact
exclusively from the v2 ``predicate_trees`` shape via the shim. The
v1 native emit (which derived guards from ``permission_graph`` +
name-heuristic ``_classify_caller_*`` / ``_looks_like_*`` paths) is
gone — every consumer of ``semantic_guards`` reads a v2-shim-derived
dict identical in structure to what the legacy emit produced, but
sourced from the typed ``PredicateTree`` per function.

``build_semantic_guards(analysis)`` keeps its v1 signature so existing
call sites (``core.analyze_contract``, ``workers/static_worker``) stay
unchanged. The function reads ``analysis["_v2_predicate_trees"]`` —
populated by ``collect_contract_analysis`` from
``build_predicate_artifacts(subject_contract)`` — and feeds it through
the shim. If the v2 emit failed (``error`` key set) the result is the
empty-functions envelope; consumers treat it as "no guards detected".
"""

from __future__ import annotations

from typing import Any

from schemas.contract_analysis import ContractAnalysis

from .v2_to_v1_shim import synthesize_semantic_guards_from_predicate_trees


def build_semantic_guards(analysis: ContractAnalysis) -> dict[str, Any]:
    """Translate ``analysis``'s embedded v2 ``predicate_trees`` into
    the legacy ``semantic_guards`` artifact shape.

    Drop-in for v1-native callers: the returned dict carries the
    same ``functions`` list with ``predicates`` typed kinds
    (``mapping_membership`` / ``caller_equals_controller`` /
    ``policy_check`` / ``external_helper``) so downstream
    ``effective_permissions`` and the artifact-store consumers stay
    on the same contract.
    """
    subject = analysis.get("subject") or {}
    contract_address = subject.get("address") or ""
    contract_name = subject.get("name") or ""

    predicate_trees = analysis.get("_v2_predicate_trees")
    if not isinstance(predicate_trees, dict) or "trees" not in predicate_trees:
        # collect_contract_analysis didn't populate v2 trees (Vyper
        # project, predicate emit error, or schema mismatch). Return
        # the empty envelope; consumers treat as "no guards".
        return {
            "schema_version": "0.1",
            "contract_address": contract_address,
            "contract_name": contract_name,
            "functions": [],
            "_synthetic_from": "v2_predicate_trees",
        }
    return synthesize_semantic_guards_from_predicate_trees(
        predicate_trees,
        contract_address=contract_address,
        contract_name=contract_name,
    )
