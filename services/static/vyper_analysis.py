"""Lightweight semantic analysis fallback for verified Vyper contracts."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

from schemas.contract_analysis import (
    AccessControlAnalysis,
    AnalysisStatus,
    AuditAlignment,
    ContractAnalysis,
    ContractClassification,
    ControllerReadSpec,
    ControllerTrackingTarget,
    CurrentHolders,
    PausabilityAnalysis,
    PolicyTrackingTarget,
    PrivilegedFunction,
    SlitherSummary,
    Subject,
    Summary,
    TimelockAnalysis,
    TrackingHint,
    UpgradeabilityAnalysis,
)
from services.static.contract_analysis_pipeline.shared import _load_json


def is_vyper_project(project_dir: Path, meta: dict[str, Any] | None = None) -> bool:
    meta = meta or {}
    if "vyper" in str(meta.get("compiler_version", "")).lower():
        return True
    if str(meta.get("language", "")).lower() == "vyper":
        return True
    if list(project_dir.rglob("*.vy")):
        return True
    for path in project_dir.rglob("*"):
        if not path.is_file() or path.suffix in {".json", ".txt", ".toml"}:
            continue
        try:
            if path.read_text().lstrip().startswith("# @version"):
                return True
        except OSError:
            continue
    return False


def _vyper_source_files(project_dir: Path) -> list[Path]:
    files = sorted(project_dir.rglob("*.vy"))
    if files:
        return files
    fallback: list[Path] = []
    for path in sorted(project_dir.rglob("*")):
        if not path.is_file() or path.suffix in {".json", ".txt", ".toml"}:
            continue
        try:
            if path.read_text().lstrip().startswith("# @version"):
                fallback.append(path)
        except OSError:
            continue
    return fallback


def _authish_name(value: str) -> bool:
    lowered = value.lower()
    return any(
        token in lowered
        for token in ("owner", "admin", "govern", "auth", "committee", "timelock", "guardian", "signer")
    )


def _split_args(arg_string: str) -> list[str]:
    args: list[str] = []
    current: list[str] = []
    depth = 0
    for char in arg_string:
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth = max(depth - 1, 0)
        if char == "," and depth == 0:
            piece = "".join(current).strip()
            if piece:
                args.append(piece)
            current = []
            continue
        current.append(char)
    piece = "".join(current).strip()
    if piece:
        args.append(piece)
    return args


def _abi_signature(name: str, arg_string: str) -> str:
    raw_args = _split_args(arg_string)
    abi_types: list[str] = []
    for raw in raw_args:
        _, _, type_name = raw.partition(":")
        abi_types.append(type_name.strip().replace(" ", "") if type_name else raw.replace(" ", ""))
    return f"{name}({','.join(abi_types)})"


def _parse_vyper_functions(source: str) -> list[dict[str, Any]]:
    lines = source.splitlines()
    functions: list[dict[str, Any]] = []
    decorators: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if stripped.startswith("@") and not line.startswith(" "):
            decorators.append(stripped)
            i += 1
            continue
        if not stripped.startswith("def ") or line.startswith(" "):
            decorators = []
            i += 1
            continue

        header_lines = [stripped]
        while not header_lines[-1].endswith(":") and i + 1 < len(lines):
            i += 1
            header_lines.append(lines[i].strip())
        header = " ".join(header_lines)
        match = re.match(r"def\s+([A-Za-z_]\w*)\((.*)\)(?:\s*->\s*([^:]+))?:", header)
        if not match:
            decorators = []
            i += 1
            continue

        name = match.group(1)
        args = match.group(2)
        return_type = (match.group(3) or "").strip()
        start_line = i + 1 - (len(header_lines) - 1)
        i += 1
        body_lines: list[str] = []
        while i < len(lines):
            candidate = lines[i]
            candidate_stripped = candidate.strip()
            if candidate_stripped and not candidate.startswith(" "):
                break
            body_lines.append(candidate)
            i += 1

        functions.append(
            {
                "name": name,
                "signature": (
                    "constructor" + _abi_signature("", args)[1:] if name == "__init__" else _abi_signature(name, args)
                ),
                "args": args,
                "return_type": return_type,
                "decorators": decorators,
                "body": body_lines,
                "body_text": "\n".join(body_lines),
                "line": start_line,
            }
        )
        decorators = []
    return functions


def _parse_state_variables(source: str) -> dict[str, dict[str, Any]]:
    state_vars: dict[str, dict[str, Any]] = {}
    for lineno, line in enumerate(source.splitlines(), 1):
        if line.startswith(" ") or not line.strip() or line.strip().startswith(("@", "def ", "interface ", "event ")):
            continue
        match = re.match(r"([A-Za-z_]\w*)\s*:\s*([^\n#]+)", line.strip())
        if not match:
            continue
        name = match.group(1)
        type_name = match.group(2).strip()
        state_vars[name] = {
            "name": name,
            "type": type_name,
            "line": lineno,
            "public": "public(" in type_name,
            "immutable": "immutable(" in type_name,
        }
    return state_vars


def _explicit_getters(functions: list[dict[str, Any]], state_vars: dict[str, dict[str, Any]]) -> dict[str, str]:
    getters: dict[str, str] = {}
    for name, state_var in state_vars.items():
        if state_var.get("public"):
            getters[name] = name

    for function in functions:
        decorators = set(function["decorators"])
        if "@external" not in decorators or not decorators.intersection({"@view", "@pure"}):
            continue
        if _split_args(function["args"]):
            continue
        body_lines = [line.strip() for line in function["body"] if line.strip() and not line.strip().startswith("#")]
        if len(body_lines) != 1 or not body_lines[0].startswith("return "):
            continue
        expr = body_lines[0][7:].strip()
        if expr.startswith("self."):
            getters[expr[5:]] = function["name"]
        elif re.fullmatch(r"[A-Z_][A-Z0-9_]*", expr) or expr in state_vars:
            getters[expr] = function["name"]
    return getters


def _controller_from_expr(expr: str) -> str | None:
    candidate = expr.strip().strip("()")
    if candidate.startswith("self."):
        candidate = candidate[5:]
    if "." in candidate:
        candidate = candidate.split(".", 1)[0]
    candidate = candidate.strip()
    if not re.fullmatch(r"[A-Za-z_]\w*", candidate):
        return None
    return candidate


def _effect_labels(function: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    name = function["name"].lower()
    body_text = function["body_text"]
    if "raw_call(" in body_text:
        labels.append("external_contract_call")
    if re.search(r"self\.[A-Za-z_]\w*\s*=", body_text):
        labels.append("state_update")
    if "seal" in name or "pause" in name:
        labels.append("pause_toggle")
    return sorted(set(labels))


def _action_summary(effect_labels: list[str]) -> str:
    if "external_contract_call" in effect_labels:
        return "Calls an external contract from the contract context."
    if "pause_toggle" in effect_labels:
        return "Changes the contract pause state."
    if "state_update" in effect_labels:
        return "Updates contract state."
    return "Requires privileged access."


def collect_vyper_contract_analysis(project_dir: Path) -> ContractAnalysis:
    meta = _load_json(project_dir / "contract_meta.json", {})
    source_files = _vyper_source_files(project_dir)
    if not source_files:
        raise RuntimeError(f"No Vyper source files found in {project_dir}")

    contract_name = str(meta.get("contract_name") or source_files[0].stem)
    main_source = next((path for path in source_files if path.stem == contract_name), source_files[0])
    source = main_source.read_text()
    functions = _parse_vyper_functions(source)
    state_vars = _parse_state_variables(source)
    getters = _explicit_getters(functions, state_vars)

    privileged_functions: list[PrivilegedFunction] = []
    controller_tracking: list[ControllerTrackingTarget] = []
    tracking_hints: list[TrackingHint] = []
    controller_refs_seen: set[str] = set()

    for function in functions:
        decorators = set(function["decorators"])
        if "@external" not in decorators or decorators.intersection({"@view", "@pure"}):
            continue
        if function["name"] == "__init__":
            continue

        body_lines = [line.strip() for line in function["body"] if line.strip()]
        controller_refs: list[str] = []
        for line in body_lines:
            match = re.search(r"assert\s+msg\.sender\s*==\s*(.+?)(?:,|$)", line)
            reverse = re.search(r"assert\s+(.+?)\s*==\s*msg\.sender(?:,|$)", line)
            expr = match.group(1) if match else reverse.group(1) if reverse else None
            if not expr:
                continue
            controller = _controller_from_expr(expr)
            if controller:
                controller_refs.append(controller)

        effect_labels = _effect_labels(function)
        if not controller_refs and not effect_labels:
            continue

        deduped_refs = sorted(set(controller_refs))
        privileged_functions.append(
            {
                "contract": contract_name,
                "function": function["signature"],
                "visibility": "external",
                "guards": deduped_refs,
                "guard_kinds": ["caller_equals_storage"] if deduped_refs else [],
                "controller_refs": deduped_refs,
                "sink_ids": [],
                "effects": effect_labels,
                "effect_targets": [],
                "effect_labels": effect_labels,
                "action_summary": _action_summary(effect_labels),
            }
        )

        for ref in deduped_refs:
            if ref in controller_refs_seen:
                continue
            read_target = getters.get(ref)
            read_spec: ControllerReadSpec | None = None
            if read_target:
                read_spec = {"strategy": "getter_call", "target": read_target}
            controller_tracking.append(
                {
                    "controller_id": f"state_variable:{ref}",
                    "label": ref,
                    "source": ref,
                    "kind": "state_variable",
                    "read_spec": read_spec,
                    "confidence": None,
                    "tracking_mode": "state_only",
                    "writer_functions": [],
                    "associated_events": [],
                    "polling_sources": [ref],
                    "notes": [
                        "Vyper semantic fallback inferred this controller from caller "
                        "equality checks in verified source."
                    ],
                }
            )
            tracking_hints.append({"kind": "controller", "label": ref, "source": ref})
            controller_refs_seen.add(ref)

    lower_refs = {ref.lower() for ref in controller_refs_seen}
    pattern = "custom"
    if any("owner" in ref for ref in lower_refs):
        pattern = "ownable"
    elif any(token in ref for ref in lower_refs for token in ("govern", "timelock", "committee")):
        pattern = "governance"
    elif any(token in ref for ref in lower_refs for token in ("auth", "authority")):
        pattern = "auth"

    access_control: AccessControlAnalysis = {
        "pattern": pattern,
        "owner_variables": sorted(ref for ref in controller_refs_seen if "owner" in ref.lower()),
        "admin_variables": sorted(ref for ref in controller_refs_seen if _authish_name(ref)),
        "role_definitions": [],
        "privileged_functions": privileged_functions,
        "current_holders": cast(CurrentHolders, {"status": "unknown_static_only"}),
    }
    classification: ContractClassification = {
        "standards": [],
        "is_erc20": False,
        "is_erc721": False,
        "is_erc1155": False,
        "is_nft": False,
        "is_factory": False,
        "factory_functions": [],
        "evidence": [],
    }
    upgradeability: UpgradeabilityAnalysis = {
        "is_upgradeable": False,
        "is_upgradeable_proxy": False,
        "pattern": "none",
        "upgradeable_version": None,
        "implementation_slots": [],
        "admin_paths": [],
        "evidence": [],
    }
    pausability: PausabilityAnalysis = {
        "is_pausable": any(function["name"].lower() in {"pause", "unpause"} for function in functions),
        "pause_functions": [function["signature"] for function in functions if function["name"].lower() == "pause"],
        "unpause_functions": [function["signature"] for function in functions if function["name"].lower() == "unpause"],
        "gating_modifiers": [],
        "pause_variables": [],
        "authorized_roles": sorted(controller_refs_seen),
        "evidence": [],
    }
    timelock: TimelockAnalysis = {
        "has_timelock": any("timelock" in ref.lower() for ref in controller_refs_seen),
        "pattern": "custom" if any("timelock" in ref.lower() for ref in controller_refs_seen) else "none",
        "delay_variables": [],
        "queue_execute_functions": [],
        "authorized_roles": sorted(ref for ref in controller_refs_seen if "timelock" in ref.lower()),
        "evidence": [],
    }
    slither: SlitherSummary = {"detector_counts": {}, "key_findings": []}
    audit_alignment: AuditAlignment = {
        "status": "not_checked",
        "bytecode_match": "not_checked",
        "notes": [],
    }
    subject: Subject = {
        "address": str(meta.get("address", "")),
        "name": contract_name,
        "compiler_version": str(meta.get("compiler_version", "")),
        "source_verified": bool(source_files),
    }
    status: AnalysisStatus = {
        "static_analysis_completed": True,
        "slither_completed": False,
        "errors": [],
    }
    summary: Summary = {
        "control_model": pattern,
        "is_upgradeable": False,
        "is_pausable": pausability["is_pausable"],
        "has_timelock": timelock["has_timelock"],
        "static_risk_level": "medium" if privileged_functions else "unknown",
        "standards": [],
        "is_factory": False,
        "is_nft": False,
    }
    return {
        "schema_version": "0.1",
        "subject": subject,
        "analysis_status": status,
        "summary": summary,
        "permission_graph": {"controllers": [], "guards": [], "sinks": []},
        "contract_classification": classification,
        "access_control": access_control,
        "upgradeability": upgradeability,
        "pausability": pausability,
        "timelock": timelock,
        "audit_alignment": audit_alignment,
        "slither": slither,
        "tracking_hints": tracking_hints,
        "controller_tracking": controller_tracking,
        "policy_tracking": cast(list[PolicyTrackingTarget], []),
    }
