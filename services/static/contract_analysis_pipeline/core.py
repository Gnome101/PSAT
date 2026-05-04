"""Top-level orchestration for contract analysis."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from slither.slither import Slither

from schemas.contract_analysis import AuditAlignment, ContractAnalysis, Summary
from services.static.vyper_analysis import collect_vyper_contract_analysis, is_vyper_project

from .graph import build_permission_graph
from .predicate_artifacts import build_predicate_artifacts
from .semantic_guards import build_semantic_guards
from .shared import _load_json, _select_subject_contract
from .summaries import (
    _build_tracking_hints,
    _derive_static_risk_level,
    _detect_access_control,
    _detect_contract_classification,
    _detect_pausability,
    _detect_timelock,
    _detect_upgradeability,
    _determine_control_model,
    _summarize_slither,
)
from .tracking import build_controller_tracking, build_policy_tracking

logger = logging.getLogger(__name__)


def analyze_contract(project_dir: Path) -> Path:
    """Generate contract_analysis.json for a scaffolded project."""
    analysis = collect_contract_analysis(project_dir)
    output_path = project_dir / "contract_analysis.json"
    output_path.write_text(json.dumps(analysis, indent=2) + "\n")
    semantic_guards_path = project_dir / "semantic_guards.json"
    semantic_guards_path.write_text(json.dumps(build_semantic_guards(analysis), indent=2) + "\n")

    # Schema-v2 shadow artifact — emit predicate_trees.json alongside
    # the existing v1 outputs. Defensive try/except: a failure here
    # MUST NOT fail the whole analysis (the v1 path is still
    # authoritative; v2 is opt-in for downstream consumers during
    # the rollout).
    try:
        predicate_trees_path = project_dir / "predicate_trees.json"
        predicate_artifact = _build_predicate_trees_for_project(project_dir)
        predicate_trees_path.write_text(json.dumps(predicate_artifact, indent=2) + "\n")
    except Exception:
        logger.exception("predicate_trees.json emit failed for %s", project_dir)

    return output_path


def _build_predicate_trees_for_project(project_dir: Path) -> dict:
    """Re-parse with Slither and run the predicate pipeline. Vyper
    projects skip the artifact (the predicate pipeline operates on
    Slither IR; Vyper has its own static path)."""
    meta = _load_json(project_dir / "contract_meta.json", {})
    if is_vyper_project(project_dir, meta):
        return {"schema_version": "v2", "skipped": "vyper"}
    slither = Slither(str(project_dir))
    subject = _select_subject_contract(slither, meta.get("contract_name"))
    if subject is None:
        return {"schema_version": "v2", "skipped": "no_subject_contract"}
    return build_predicate_artifacts(subject)


def collect_contract_analysis(project_dir: Path) -> ContractAnalysis:
    """Collect a structured static analysis for the project."""
    meta = _load_json(project_dir / "contract_meta.json", {})
    if is_vyper_project(project_dir, meta):
        return collect_vyper_contract_analysis(project_dir)

    slither = Slither(str(project_dir))
    slither_output = _load_json(project_dir / "slither_results.json", {})

    subject_contract = _select_subject_contract(slither, meta.get("contract_name"))
    if subject_contract is None:
        raise RuntimeError(f"No analyzable contracts found in {project_dir}")

    # Schema-v2 cutover: build predicate_trees FIRST so _detect_access_control
    # can use it as the privileged-function inclusion signal. Defensive:
    # a v2 emit failure must not fail the v1 path — fall back to the empty
    # trees dict so downstream gating sees no v2 evidence (functions are
    # then gated purely on permission_graph evidence, same as before).
    v2_predicate_trees: dict
    try:
        v2_predicate_trees = build_predicate_artifacts(subject_contract)
    except Exception as exc:
        logger.exception("v2 predicate_trees emit failed for %s", project_dir)
        v2_predicate_trees = {"schema_version": "v2", "error": str(exc)}

    permission_graph = build_permission_graph(subject_contract, project_dir)
    classification = _detect_contract_classification(subject_contract, project_dir)
    access_control = _detect_access_control(subject_contract, project_dir, permission_graph, v2_predicate_trees)
    controller_tracking = build_controller_tracking(subject_contract, project_dir, permission_graph, access_control)
    policy_tracking = build_policy_tracking(subject_contract, project_dir, permission_graph)
    upgradeability = _detect_upgradeability(subject_contract, project_dir)
    pausability = _detect_pausability(subject_contract, project_dir)
    timelock = _detect_timelock(subject_contract, project_dir, access_control["role_definitions"])
    slither_summary = _summarize_slither(slither_output)
    audit_alignment: AuditAlignment = {
        "status": "not_checked",
        "bytecode_match": "not_checked",
        "notes": [],
    }

    summary: Summary = {
        "control_model": _determine_control_model(subject_contract, access_control, timelock),
        "is_upgradeable": upgradeability["is_upgradeable"],
        "is_pausable": pausability["is_pausable"],
        "has_timelock": timelock["has_timelock"],
        "static_risk_level": _derive_static_risk_level(slither_summary["detector_counts"]),
        "standards": classification["standards"],
        "is_factory": classification["is_factory"],
        "is_nft": classification["is_nft"],
    }

    return {
        "schema_version": "0.1",
        "subject": {
            "address": meta.get("address", ""),
            "name": subject_contract.name,
            "compiler_version": meta.get("compiler_version", ""),
            "source_verified": bool(list(project_dir.rglob("src/**/*.sol"))),
        },
        "analysis_status": {
            "static_analysis_completed": True,
            "slither_completed": bool(slither_output),
            "errors": [],
        },
        "summary": summary,
        "permission_graph": permission_graph,
        "contract_classification": classification,
        "access_control": access_control,
        "upgradeability": upgradeability,
        "pausability": pausability,
        "timelock": timelock,
        "audit_alignment": audit_alignment,
        "slither": slither_summary,
        "tracking_hints": _build_tracking_hints(access_control, upgradeability, pausability, timelock),
        "controller_tracking": controller_tracking,
        "policy_tracking": policy_tracking,
        "_v2_predicate_trees": v2_predicate_trees,
    }
