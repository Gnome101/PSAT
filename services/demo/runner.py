"""Helper functions for the FastAPI demo server."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv

from schemas.effective_permissions import PrincipalResolution
from services.discovery import (
    CONTRACTS_DIR,
    fetch,
    find_dependencies,
    find_dynamic_dependencies,
    scaffold,
    search_protocol_inventory,
)
from services.discovery.classifier import classify_contracts
from services.discovery.dependency_graph_builder import write_dependency_visualization
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
from utils.etherscan import get_contract_info

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

DEFAULT_DEMO_RPC_URL = os.getenv("PSAT_DEMO_RPC_URL") or os.getenv("ETH_RPC") or "https://ethereum-rpc.publicnode.com"
DEFAULT_HYPERSYNC_URL = os.getenv("PSAT_HYPERSYNC_URL", "https://eth.hypersync.xyz")
RECURSION_MAX_DEPTH = int(os.getenv("PSAT_RECURSION_MAX_DEPTH", "6"))
PROTOCOLS_DIR = Path(__file__).resolve().parents[2] / "protocols"
JSON_ARTIFACTS = (
    "contract_analysis.json",
    "contract_meta.json",
    "control_snapshot.json",
    "control_tracking_plan.json",
    "dependency_graph_viz.json",
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


def _protocol_inventory_path(company: str) -> Path:
    protocol_dir = PROTOCOLS_DIR / _slug(company)
    protocol_dir.mkdir(parents=True, exist_ok=True)
    return protocol_dir / "contract_inventory.json"


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
        "dependencies.json",
        "dependency_graph_viz.json",
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
) -> tuple[Path | None, Path | None, PrincipalResolution]:
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

    # --- Dependency discovery pipeline ---
    update("dependencies", "Discovering dependencies")
    try:
        from main import _build_unified_deps

        deps_output = None
        dyn_output = None
        cls_output = None
        resolved_rpc = rpc_url

        try:
            deps_output = find_dependencies(address, rpc_url)
            resolved_rpc = deps_output.get("rpc") or resolved_rpc
        except RuntimeError:
            pass

        try:
            dyn_output = find_dynamic_dependencies(address, rpc_url=rpc_url, tx_limit=10)
        except RuntimeError:
            pass

        if deps_output or dyn_output:
            unique_deps = sorted(
                set((deps_output or {}).get("dependencies", []) + (dyn_output or {}).get("dependencies", []))
            )
            try:
                cls_output = classify_contracts(
                    address,
                    unique_deps,
                    resolved_rpc,
                    dynamic_edges=(dyn_output or {}).get("dependency_graph"),
                )
            except RuntimeError:
                pass

            unified = _build_unified_deps(address, deps_output, dyn_output, cls_output)

            # Enrich with contract names and function selectors
            deps = unified.get("dependencies", {})
            keyed_graph = unified.get("dependency_graph", {})

            addrs_to_fetch: set[str] = set(deps.keys())
            for info in deps.values():
                impl = info.get("implementation")
                if isinstance(impl, dict):
                    addrs_to_fetch.add(impl["address"])

            info_cache: dict[str, tuple[str | None, dict[str, str]]] = {}
            for addr in sorted(addrs_to_fetch):
                info_cache[addr] = get_contract_info(addr)

            for addr, dep_info in deps.items():
                cname = info_cache.get(addr, (None, {}))[0]
                if cname:
                    dep_info["contract_name"] = cname
                impl = dep_info.get("implementation")
                if isinstance(impl, dict):
                    impl_name = info_cache.get(impl["address"], (None, {}))[0]
                    if impl_name:
                        impl["contract_name"] = impl_name

            for key, edges in keyed_graph.items():
                to_addr = key.split("|")[1]
                for edge in edges:
                    sel = edge.get("selector")
                    if not sel or sel == "0x":
                        continue
                    fn_name = info_cache.get(to_addr, (None, {}))[1].get(sel)
                    if not fn_name:
                        impl = deps.get(to_addr, {}).get("implementation")
                        impl_addr = impl["address"] if isinstance(impl, dict) else impl
                        if impl_addr:
                            fn_name = info_cache.get(impl_addr, (None, {}))[1].get(sel)
                    if fn_name:
                        edge["function_name"] = fn_name

            deps_path = project_dir / "dependencies.json"
            deps_path.write_text(json.dumps(unified, indent=2) + "\n")
            write_dependency_visualization(project_dir)
    except Exception:
        pass  # Dependency discovery is best-effort; don't block the rest of the pipeline

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
        max_depth=RECURSION_MAX_DEPTH,
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
        max_depth=RECURSION_MAX_DEPTH,
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


def run_protocol_analysis(
    company: str,
    *,
    chain: str | None = None,
    discover_limit: int = 25,
    analyze_limit: int = 5,
    rpc_url: str = DEFAULT_DEMO_RPC_URL,
    progress: Callable[[str, str], None] | None = None,
) -> dict[str, Any]:
    def update(stage: str, detail: str) -> None:
        if progress:
            progress(stage, detail)

    clean_company = company.strip()
    if not clean_company:
        raise ValueError("Company must not be empty")
    if discover_limit < 1:
        raise ValueError("Discover limit must be >= 1")
    if analyze_limit < 1:
        raise ValueError("Analyze limit must be >= 1")

    update("discovery", f"Discovering official contracts for {clean_company}")
    inventory = search_protocol_inventory(clean_company, chain=chain, limit=discover_limit)
    inventory_path = _protocol_inventory_path(clean_company)
    inventory_path.write_text(json.dumps(inventory, indent=2) + "\n")

    discovered_contracts = [entry for entry in inventory.get("contracts", []) if entry.get("address")]
    selected_contracts = discovered_contracts[:analyze_limit]
    analyzed_runs: list[dict[str, Any]] = []

    if not selected_contracts:
        update("completed", "Discovery finished but no contracts were available to analyze")
        return {
            "mode": "company",
            "company": clean_company,
            "chain": chain or "any",
            "inventory_path": str(inventory_path),
            "official_domain": inventory.get("official_domain"),
            "discovered_contract_count": len(discovered_contracts),
            "analyzed_contract_count": 0,
            "run_name": None,
            "run_names": [],
            "runs": [],
        }

    total = len(selected_contracts)
    for index, contract in enumerate(selected_contracts, start=1):
        contract_name = str(contract.get("name") or f"{clean_company}_{str(contract['address'])[2:10]}")

        def child_progress(stage: str, detail: str, *, current: int = index, overall: int = total) -> None:
            update(f"{stage} {current}/{overall}", f"{contract_name}: {detail}")

        result = run_demo_analysis(
            str(contract["address"]),
            name=contract_name,
            rpc_url=rpc_url,
            progress=child_progress,
        )
        analyzed_runs.append(
            {
                "run_name": result["run_name"],
                "contract_name": result["contract_name"],
                "address": result["address"],
                "chain": contract.get("chain"),
                "confidence": contract.get("confidence"),
                "rank_score": contract.get("rank_score"),
                "activity": contract.get("activity"),
            }
        )

    update("completed", f"Discovery complete. Analyzed {len(analyzed_runs)} contract(s)")
    return {
        "mode": "company",
        "company": clean_company,
        "chain": chain or "any",
        "inventory_path": str(inventory_path),
        "official_domain": inventory.get("official_domain"),
        "discovered_contract_count": len(discovered_contracts),
        "analyzed_contract_count": len(analyzed_runs),
        "run_name": analyzed_runs[0]["run_name"] if analyzed_runs else None,
        "run_names": [run["run_name"] for run in analyzed_runs],
        "runs": analyzed_runs,
    }
