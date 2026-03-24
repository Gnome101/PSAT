"""Helper functions for the FastAPI demo server."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from dotenv import load_dotenv

from services.discovery import CONTRACTS_DIR, fetch, scaffold
from services.policy import (
    run_hypersync_policy_backfill,
    write_effective_permissions_from_files,
    write_principal_labels_from_files,
)
from services.resolution import (
    build_control_snapshot,
    load_control_tracking_plan,
    write_control_snapshot,
    write_control_tracking_plan,
    write_resolved_control_graph,
)
from services.static import analyze, analyze_contract

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DEFAULT_DEMO_RPC_URL = os.getenv("PSAT_DEMO_RPC_URL", "https://ethereum-rpc.publicnode.com")
DEFAULT_HYPERSYNC_URL = os.getenv("PSAT_HYPERSYNC_URL", "https://eth.hypersync.xyz")
JSON_ARTIFACTS = (
    "contract_analysis.json",
    "contract_meta.json",
    "control_snapshot.json",
    "control_tracking_plan.json",
    "effective_permissions.json",
    "principal_labels.json",
    "resolved_control_graph.json",
    "slither_results.json",
)
TEXT_ARTIFACTS = (
    "analysis_report.txt",
    "policy_event_history.jsonl",
)
CORE_ARTIFACTS = (
    "contract_analysis.json",
    "control_snapshot.json",
    "control_tracking_plan.json",
    "resolved_control_graph.json",
    "effective_permissions.json",
    "principal_labels.json",
    "analysis_report.txt",
)


def _slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", value).strip("_") or "analysis"


def _unique_run_name(base: str) -> str:
    candidate = _slug(base)
    if not (CONTRACTS_DIR / candidate).exists():
        return candidate
    suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{candidate}_{suffix}"


def _analysis_summary(project_dir: Path) -> dict:
    analysis_path = project_dir / "contract_analysis.json"
    summary = {
        "run_name": project_dir.name,
        "path": str(project_dir),
        "available_artifacts": sorted(
            [name for name in (*JSON_ARTIFACTS, *TEXT_ARTIFACTS) if (project_dir / name).exists()]
        ),
    }
    if analysis_path.exists():
        analysis = json_load(analysis_path)
        summary.update(
            {
                "contract_name": analysis["subject"]["name"],
                "address": analysis["subject"]["address"],
                "summary": analysis["summary"],
            }
        )
    else:
        meta_path = project_dir / "contract_meta.json"
        if meta_path.exists():
            meta = json_load(meta_path)
            summary.update(
                {
                    "contract_name": meta.get("contract_name", project_dir.name),
                    "address": meta.get("address"),
                }
            )
    return summary


def list_analyses(include_nested: bool = False) -> list[dict]:
    runs = []
    for child in sorted(CONTRACTS_DIR.iterdir(), key=lambda path: path.stat().st_mtime, reverse=True):
        if not child.is_dir():
            continue
        if not include_nested and child.name.startswith("recursive_"):
            continue
        if not any((child / artifact).exists() for artifact in CORE_ARTIFACTS):
            continue
        runs.append(_analysis_summary(child))
    return runs


def json_load(path: Path) -> dict:
    import json

    return json.loads(path.read_text())


def read_analysis(run_name: str) -> dict:
    project_dir = CONTRACTS_DIR / run_name
    if not project_dir.exists() or not project_dir.is_dir():
        raise FileNotFoundError(run_name)

    payload = _analysis_summary(project_dir)
    for artifact in (
        "contract_analysis.json",
        "control_snapshot.json",
        "resolved_control_graph.json",
        "effective_permissions.json",
        "principal_labels.json",
    ):
        path = project_dir / artifact
        if path.exists():
            payload[artifact.removesuffix(".json")] = json_load(path)
    report_path = project_dir / "analysis_report.txt"
    if report_path.exists():
        payload["analysis_report"] = report_path.read_text()
    return payload


def artifact_path(run_name: str, artifact_name: str) -> Path:
    allowed = set(JSON_ARTIFACTS) | set(TEXT_ARTIFACTS)
    if artifact_name not in allowed:
        raise FileNotFoundError(artifact_name)
    path = CONTRACTS_DIR / run_name / artifact_name
    if not path.exists():
        raise FileNotFoundError(artifact_name)
    return path


def _find_controller_snapshot_from_graph(resolved_graph_path: Path, controller_address: str) -> Path | None:
    graph = json_load(resolved_graph_path)
    target = controller_address.lower()
    for node in graph.get("nodes", []):
        if node.get("address", "").lower() != target:
            continue
        snapshot = (node.get("artifacts") or {}).get("snapshot")
        if snapshot:
            candidate = Path(snapshot)
            if candidate.exists():
                return candidate
    return None


def _authority_snapshot_and_policy(
    project_dir: Path,
    resolved_graph_path: Path,
    target_snapshot_path: Path,
) -> tuple[Path | None, Path | None, dict]:
    target_snapshot = json_load(target_snapshot_path)
    authority_address = None
    for controller_id, value in target_snapshot.get("controller_values", {}).items():
        if controller_id.endswith(":authority"):
            authority_address = str(value.get("value", "")).lower()
            break
    if not authority_address or authority_address == "0x0000000000000000000000000000000000000000":
        return (
            None,
            None,
            {
                "status": "no_authority",
                "reason": "No non-zero authority controller was resolved for this contract.",
            },
        )

    authority_snapshot_path = _find_controller_snapshot_from_graph(resolved_graph_path, authority_address)
    if authority_snapshot_path is None:
        return (
            None,
            None,
            {
                "status": "no_authority_snapshot",
                "reason": "The authority contract was found, but its recursive snapshot artifact is missing.",
            },
        )

    authority_project_dir = authority_snapshot_path.parent
    authority_plan_path = authority_project_dir / "control_tracking_plan.json"
    authority_analysis_path = authority_project_dir / "contract_analysis.json"
    policy_state_path = authority_project_dir / "policy_state.json"

    if authority_plan_path.exists() and authority_analysis_path.exists():
        authority_analysis = json_load(authority_analysis_path)
        if authority_analysis.get("policy_tracking"):
            if policy_state_path.exists():
                return (
                    authority_snapshot_path,
                    policy_state_path,
                    {
                        "status": "complete",
                        "reason": "Existing authority policy state was joined into the permission view.",
                    },
                )
            if os.getenv("ENVIO_API_TOKEN"):
                run_hypersync_policy_backfill(
                    authority_plan_path,
                    url=DEFAULT_HYPERSYNC_URL,
                    state_out=policy_state_path,
                    events_out=authority_project_dir / "policy_event_history.jsonl",
                )
                if policy_state_path.exists():
                    return (
                        authority_snapshot_path,
                        policy_state_path,
                        {
                            "status": "complete",
                            "reason": (
                                "Authority policy backfill completed and principals "
                                "were joined from current policy state."
                            ),
                        },
                    )
            return (
                authority_snapshot_path,
                None,
                {
                    "status": "missing_hypersync_token",
                    "reason": (
                        "Authority policy tracking exists, but ENVIO_API_TOKEN is "
                        "not set, so HyperSync backfill was skipped."
                    ),
                },
            )
        return (
            authority_snapshot_path,
            None,
            {
                "status": "no_policy_tracking",
                "reason": "The resolved authority contract does not expose policy tracking metadata.",
            },
        )
    return (
        authority_snapshot_path,
        None,
        {
            "status": "missing_policy_state",
            "reason": "Authority artifacts are incomplete, so policy state could not be reconstructed.",
        },
    )


def run_demo_analysis(
    address: str,
    *,
    name: str | None = None,
    rpc_url: str = DEFAULT_DEMO_RPC_URL,
    progress: Callable[[str, str], None] | None = None,
) -> dict:
    def update(stage: str, detail: str) -> None:
        if progress:
            progress(stage, detail)

    update("fetch", f"Fetching verified source for {address}")
    result = fetch(address)
    contract_name = result.get("ContractName", "Contract")

    run_name = _unique_run_name(name or f"{contract_name}_{address[2:10]}")
    update("scaffold", f"Scaffolding {run_name}")
    project_dir = scaffold(address, run_name, result)

    update("slither", "Running Slither")
    analyze(project_dir, contract_name, address)

    update("analysis", "Building structured contract analysis")
    contract_analysis_path = analyze_contract(project_dir)
    tracking_plan_path = write_control_tracking_plan(contract_analysis_path)

    update("snapshot", "Reading current controller state")
    plan = load_control_tracking_plan(tracking_plan_path)
    snapshot = build_control_snapshot(plan, rpc_url)
    snapshot_path = project_dir / "control_snapshot.json"
    write_control_snapshot(snapshot, snapshot_path)

    update("graph", "Resolving recursive control graph")
    resolved_graph_path = write_resolved_control_graph(
        contract_analysis_path,
        rpc_url=rpc_url,
        output_path=project_dir / "resolved_control_graph.json",
        max_depth=4,
        workspace_prefix="recursive",
        refresh_snapshots=True,
    )

    update("permissions", "Resolving effective permissions")
    authority_snapshot_path, policy_state_path, principal_resolution = _authority_snapshot_and_policy(
        project_dir,
        resolved_graph_path,
        snapshot_path,
    )
    effective_permissions_path = write_effective_permissions_from_files(
        contract_analysis_path,
        target_snapshot_path=snapshot_path,
        authority_snapshot_path=authority_snapshot_path,
        policy_state_path=policy_state_path,
        output_path=project_dir / "effective_permissions.json",
        principal_resolution=principal_resolution,
    )

    update("graph", "Refreshing control graph with role-holder recursion")
    resolved_graph_path = write_resolved_control_graph(
        contract_analysis_path,
        rpc_url=rpc_url,
        output_path=project_dir / "resolved_control_graph.json",
        max_depth=4,
        workspace_prefix="recursive",
        refresh_snapshots=True,
    )

    update("labels", "Labeling principals")
    principal_labels_path = write_principal_labels_from_files(
        effective_permissions_path,
        resolved_control_graph_path=resolved_graph_path,
        rpc_url=rpc_url,
        output_path=project_dir / "principal_labels.json",
    )

    return {
        "run_name": run_name,
        "project_dir": str(project_dir),
        "contract_name": contract_name,
        "address": address,
        "artifacts": {
            "contract_analysis": str(contract_analysis_path),
            "tracking_plan": str(tracking_plan_path),
            "control_snapshot": str(snapshot_path),
            "resolved_control_graph": str(resolved_graph_path),
            "effective_permissions": str(effective_permissions_path),
            "principal_labels": str(principal_labels_path),
        },
    }
