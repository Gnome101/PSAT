"""Top-level orchestration for contract analysis."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from slither.slither import Slither

from schemas.contract_analysis import AuditAlignment, ContractAnalysis, Summary

from .effects import EffectsArtifact, build_effects
from .predicate_artifacts import (
    build_predicate_artifacts_with_pause_info,
)
from .reentrancy_pause import PauseInfo
from .shared import _load_json, _select_subject_contract
from .summaries import (
    _build_semantic_control_summary,
    _build_tracking_hints,
    _derive_static_risk_level,
    _detect_contract_classification,
    _detect_pausability,
    _detect_timelock,
    _detect_upgradeability,
    _determine_control_model,
    _summarize_slither,
)
from .tracking import build_controller_tracking

logger = logging.getLogger(__name__)

_VYPER_PRAGMA_RE = re.compile(r"^\s*#\s*(?:@version|pragma\s+version)\s+([^\s]+)", re.MULTILINE)


def _detect_vyper_version(project_dir: Path, meta: dict) -> str | None:
    """Best-effort Vyper version detection.

    Reads ``contract_meta.json`` first (``compiler_version`` like
    ``"vyper:0.3.10"``), falls back to scanning ``*.vy`` files for a
    ``# @version X`` or ``# pragma version X`` line. Returns the bare
    version string (``"0.3.10"``) or ``None`` if not a Vyper project.
    """
    raw = str(meta.get("compiler_version", ""))
    if "vyper" in raw.lower():
        version = raw.split(":", 1)[-1].strip().lstrip("v")
        if version:
            return version
    for path in project_dir.rglob("*.vy"):
        try:
            match = _VYPER_PRAGMA_RE.search(path.read_text())
        except OSError:
            continue
        if match:
            return match.group(1).strip().lstrip("v").lstrip("^~>=<")
    return None


def _guard_vyper_version(project_dir: Path, meta: dict) -> None:
    """Hard-fail unsupported Vyper versions before invoking Slither.

    Vyper 0.4.x triggers an upstream crytic-compile bug
    (``platform/vyper.py:101`` splits a dict the new sourceMap format
    returns as an object). Raise a clear error instead of crashing
    deeper inside Slither/crytic-compile.
    """
    version = _detect_vyper_version(project_dir, meta)
    if version and version.startswith("0.4."):
        raise RuntimeError(
            f"Vyper {version} is not supported (upstream crytic-compile sourceMap bug). "
            "Pin the contract to Vyper 0.3.x."
        )


def _slither_target(project_dir: Path, meta: dict) -> str:
    """For Vyper projects, hand Slither the main ``.vy`` file path
    instead of the project directory. The scaffolder writes a
    ``foundry.toml`` even for Vyper projects, which would otherwise
    route the project through crytic-compile's Foundry platform and
    crash on missing ``out/build-info``. Solidity projects fall through
    to the directory path."""
    if _detect_vyper_version(project_dir, meta) is None:
        return str(project_dir)
    contract_name = str(meta.get("contract_name", "")).strip()
    candidates = sorted(project_dir.rglob("*.vy"))
    if not candidates:
        return str(project_dir)
    if contract_name:
        for path in candidates:
            if path.stem == contract_name:
                return str(path)
    return str(candidates[0])


def analyze_contract(project_dir: Path) -> Path:
    """Generate contract_analysis.json + predicate_trees.json + effects.json
    for a scaffolded project. ``predicate_trees`` + ``effects`` are the
    semantic source of truth."""
    analysis, predicate_trees, effects = collect_contract_analysis_with_artifacts(project_dir)
    output_path = project_dir / "contract_analysis.json"
    output_path.write_text(json.dumps(analysis, indent=2) + "\n")

    if predicate_trees is not None:
        (project_dir / "predicate_trees.json").write_text(json.dumps(predicate_trees, indent=2) + "\n")
    if effects is not None:
        (project_dir / "effects.json").write_text(json.dumps(effects, indent=2) + "\n")

    return output_path


def collect_contract_analysis(project_dir: Path) -> ContractAnalysis:
    """Backwards-compatible accessor for the analysis dict only.

    Most callers (tests, resolution.recursive) only need the analysis
    dict. The static worker uses
    :func:`collect_contract_analysis_with_artifacts` to also receive
    the semantic ``predicate_trees`` and ``effects`` artifacts so it can
    persist them off a single Slither parse.
    """
    analysis, _trees, _effects = collect_contract_analysis_with_artifacts(project_dir)
    return analysis


def collect_contract_analysis_with_artifacts(
    project_dir: Path,
) -> tuple[ContractAnalysis, dict[str, Any] | None, Mapping[str, Any] | None]:
    """Collect the analysis dict plus semantic predicate/effect artifacts in
    a single pass. Returns ``(analysis, predicate_trees, effects)``.

    Vyper projects flow through the same Slither path as Solidity.
    """
    meta = _load_json(project_dir / "contract_meta.json", {})
    _guard_vyper_version(project_dir, meta)
    slither = Slither(_slither_target(project_dir, meta))
    slither_output = _load_json(project_dir / "slither_results.json", {})

    subject_contract = _select_subject_contract(slither, meta.get("contract_name"))
    if subject_contract is None:
        raise RuntimeError(f"No analyzable contracts found in {project_dir}")

    # Predicate trees + effects are the only source of truth for the static
    # stage's controller-tracking / semantic-control / pausability outputs.
    predicate_trees_artifact: dict[str, Any]
    pause_info: PauseInfo
    try:
        predicate_trees_artifact, pause_info = build_predicate_artifacts_with_pause_info(subject_contract)
    except Exception as exc:
        logger.exception("semantic predicate_trees emit failed for %s", project_dir)
        predicate_trees_artifact = {"schema_version": "semantic", "error": str(exc)}
        pause_info = {
            "pause_state_vars": [],
            "pause_toggle_functions": [],
            "reentrancy_state_vars": [],
            "reentrancy_guarded_functions": [],
        }

    effects_artifact: EffectsArtifact | dict[str, Any]
    try:
        effects_artifact = build_effects(subject_contract)
    except Exception as exc:
        logger.exception("semantic effects emit failed for %s", project_dir)
        effects_artifact = {"schema_version": "semantic", "error": str(exc)}

    classification = _detect_contract_classification(subject_contract, project_dir, effects_artifact)
    semantic_control = _build_semantic_control_summary(
        subject_contract,
        project_dir,
        predicate_trees_artifact,
        effects_artifact,
    )
    controller_tracking = build_controller_tracking(
        subject_contract,
        project_dir,
        predicate_trees_artifact,
        effects_artifact,
        semantic_control,
    )
    upgradeability = _detect_upgradeability(subject_contract, project_dir, effects_artifact)
    pausability = _detect_pausability(subject_contract, project_dir, pause_info)
    timelock = _detect_timelock(subject_contract, project_dir, semantic_control["role_definitions"])
    slither_summary = _summarize_slither(slither_output)
    audit_alignment: AuditAlignment = {
        "status": "not_checked",
        "bytecode_match": "not_checked",
        "notes": [],
    }

    summary: Summary = {
        "control_model": _determine_control_model(subject_contract, semantic_control, timelock),
        "is_upgradeable": upgradeability["is_upgradeable"],
        "is_pausable": pausability["is_pausable"],
        "has_timelock": timelock["has_timelock"],
        "static_risk_level": _derive_static_risk_level(slither_summary["detector_counts"]),
        "standards": classification["standards"],
        "is_factory": classification["is_factory"],
        "is_nft": classification["is_nft"],
    }

    analysis: ContractAnalysis = {
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
        "contract_classification": classification,
        "semantic_control": semantic_control,
        "upgradeability": upgradeability,
        "pausability": pausability,
        "timelock": timelock,
        "audit_alignment": audit_alignment,
        "slither": slither_summary,
        "tracking_hints": _build_tracking_hints(semantic_control, upgradeability, pausability, timelock),
        "controller_tracking": controller_tracking,
    }
    return analysis, predicate_trees_artifact, effects_artifact
