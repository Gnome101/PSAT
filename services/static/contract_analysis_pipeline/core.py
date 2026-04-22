"""Top-level orchestration for contract analysis."""

from __future__ import annotations

from pathlib import Path

from slither.slither import Slither

from schemas.contract_analysis import AuditAlignment, ContractAnalysis, Summary
from services.static.vyper_analysis import collect_vyper_contract_analysis, is_vyper_project

from .graph import build_permission_graph
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

    permission_graph = build_permission_graph(subject_contract, project_dir)
    classification = _detect_contract_classification(subject_contract, project_dir)
    access_control = _detect_access_control(subject_contract, project_dir, permission_graph)
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
    }
