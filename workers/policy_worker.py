"""Policy worker — computes effective permissions and labels principals."""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import Contract, EffectiveFunction, FunctionPrincipal, Job, JobStage, JobStatus, PrincipalLabel
from db.queue import get_artifact, store_artifact
from schemas.effective_permissions import PrincipalResolution
from services.policy.hypersync_backfill import run_hypersync_policy_backfill
from workers.base import BaseWorker

logger = logging.getLogger("workers.policy_worker")

DEFAULT_RPC_URL = os.getenv("ETH_RPC", "https://ethereum-rpc.publicnode.com")
DEFAULT_HYPERSYNC_URL = "https://eth.hypersync.xyz"
RECURSION_MAX_DEPTH = int(os.getenv("PSAT_RECURSION_MAX_DEPTH", "6"))


class PolicyWorker(BaseWorker):
    stage = JobStage.policy
    next_stage = JobStage.coverage

    def process(self, session: Session, job: Job) -> None:
        logger.info(
            "Policy stage started for job %s address=%s name=%s",
            job.id,
            job.address or "0x0",
            job.name or "Contract",
        )
        rpc_url = DEFAULT_RPC_URL
        if job.request and isinstance(job.request, dict):
            rpc_url = job.request.get("rpc_url") or rpc_url

        # Load required artifacts from DB
        contract_analysis = get_artifact(session, job.id, "contract_analysis")
        control_snapshot = get_artifact(session, job.id, "control_snapshot")
        resolved_control_graph = get_artifact(session, job.id, "resolved_control_graph")
        semantic_guards = get_artifact(session, job.id, "semantic_guards")

        if not isinstance(contract_analysis, dict):
            raise RuntimeError("contract_analysis artifact not found")
        if not isinstance(control_snapshot, dict):
            raise RuntimeError("control_snapshot artifact not found")

        # We need temp files for functions that read from filesystem
        tmp_dir = tempfile.mkdtemp(prefix="psat_policy_")
        project_dir = Path(tmp_dir)
        try:
            contract_row = None
            ep_data: dict | None = None
            analysis_path = project_dir / "contract_analysis.json"
            analysis_path.write_text(json.dumps(contract_analysis, indent=2) + "\n")

            snapshot_path = project_dir / "control_snapshot.json"
            snapshot_path.write_text(json.dumps(control_snapshot, indent=2) + "\n")

            resolved_graph_path = None
            if isinstance(resolved_control_graph, dict):
                resolved_graph_path = project_dir / "resolved_control_graph.json"
                resolved_graph_path.write_text(json.dumps(resolved_control_graph, indent=2) + "\n")
            semantic_guards_path = None
            if isinstance(semantic_guards, dict):
                semantic_guards_path = project_dir / "semantic_guards.json"
                semantic_guards_path.write_text(json.dumps(semantic_guards, indent=2) + "\n")

            # Determine authority snapshot and policy state
            authority_snapshot_path = None
            policy_state_path = None
            principal_resolution: PrincipalResolution = {
                "status": "no_authority",
                "reason": "Worker-mode authority resolution",
            }

            if resolved_graph_path:
                authority_result = self._resolve_authority(
                    project_dir, resolved_graph_path, snapshot_path, contract_analysis
                )
                authority_snapshot_path = authority_result.get("authority_snapshot_path")
                policy_state_path = authority_result.get("policy_state_path")
                principal_resolution = authority_result.get("principal_resolution", principal_resolution)
                logger.info(
                    "Policy stage authority resolution for job %s address=%s status=%s",
                    job.id,
                    job.address or "0x0",
                    principal_resolution.get("status", "unknown"),
                )

            # Build effective permissions
            self.update_detail(session, job, "Computing effective permissions")
            from services.policy import write_effective_permissions_from_files

            effective_permissions_path = write_effective_permissions_from_files(
                analysis_path,
                target_snapshot_path=snapshot_path,
                authority_snapshot_path=authority_snapshot_path,
                policy_state_path=policy_state_path,
                semantic_guards_path=semantic_guards_path,
                resolved_control_graph_path=resolved_graph_path,
                output_path=project_dir / "effective_permissions.json",
                principal_resolution=principal_resolution,
            )

            if effective_permissions_path.exists():
                ep_data = json.loads(effective_permissions_path.read_text())

                # Write to effective_functions and function_principals tables
                contract_row = session.execute(
                    select(Contract).where(Contract.job_id == job.id).limit(1)
                ).scalar_one_or_none()
                if contract_row and isinstance(ep_data, dict):
                    session.query(EffectiveFunction).filter(EffectiveFunction.contract_id == contract_row.id).delete()
                    for fn in ep_data.get("functions", []):
                        ef = EffectiveFunction(
                            contract_id=contract_row.id,
                            function_name=fn.get("function", "").split("(")[0],
                            selector=fn.get("selector"),
                            abi_signature=fn.get("function") or fn.get("abi_signature"),
                            effect_labels=fn.get("effect_labels", []),
                            effect_targets=fn.get("effect_targets", []),
                            action_summary=fn.get("action_summary"),
                            authority_public=fn.get("authority_public", False),
                            authority_roles=fn.get("authority_roles"),
                        )
                        session.add(ef)
                        session.flush()
                        # Add principals from all sources
                        do = fn.get("direct_owner")
                        if isinstance(do, dict) and do.get("address"):
                            session.add(
                                FunctionPrincipal(
                                    function_id=ef.id,
                                    address=do["address"].lower(),
                                    resolved_type=do.get("resolved_type"),
                                    origin="direct owner",
                                    principal_type="direct_owner",
                                    details=do.get("details"),
                                )
                            )
                        for ctrl in fn.get("controllers") or []:
                            for p in ctrl.get("principals") or []:
                                if isinstance(p, dict) and p.get("address"):
                                    session.add(
                                        FunctionPrincipal(
                                            function_id=ef.id,
                                            address=p["address"].lower(),
                                            resolved_type=p.get("resolved_type"),
                                            origin=ctrl.get("label") or ctrl.get("controller_id", "controller"),
                                            principal_type="controller",
                                            details=p.get("details"),
                                        )
                                    )
                        for role in fn.get("authority_roles") or []:
                            for p in role.get("principals") or []:
                                if isinstance(p, dict) and p.get("address"):
                                    session.add(
                                        FunctionPrincipal(
                                            function_id=ef.id,
                                            address=p["address"].lower(),
                                            resolved_type=p.get("resolved_type"),
                                            origin=f"role {role.get('role', '?')}",
                                            principal_type="authority_role",
                                            details=p.get("details"),
                                        )
                                    )
                    session.commit()

                store_artifact(session, job.id, "effective_permissions", data=ep_data)

                logger.info(
                    "Policy stage effective permissions complete for job %s address=%s name=%s",
                    job.id,
                    job.address or "0x0",
                    job.name or "Contract",
                )

            # Rebuild the resolved graph now that effective_permissions.json exists,
            # so semantic role/controller principals can be projected into the graph.
            self.update_detail(session, job, "Refreshing resolved control graph")
            from services.resolution.recursive import write_resolved_control_graph

            refreshed_graph_path = write_resolved_control_graph(
                analysis_path,
                rpc_url=rpc_url,
                output_path=project_dir / "resolved_control_graph.json",
                max_depth=RECURSION_MAX_DEPTH,
                workspace_prefix="recursive",
                refresh_snapshots=False,
            )
            if refreshed_graph_path.exists():
                resolved_graph_path = refreshed_graph_path
                rg_data = json.loads(refreshed_graph_path.read_text())
                store_artifact(session, job.id, "resolved_control_graph", data=rg_data)

            # Label principals
            self.update_detail(session, job, "Labeling principals")
            from services.policy import write_principal_labels_from_files

            principal_labels_path = write_principal_labels_from_files(
                effective_permissions_path,
                resolved_control_graph_path=resolved_graph_path,
                rpc_url=rpc_url,
                output_path=project_dir / "principal_labels.json",
            )

            if principal_labels_path.exists():
                pl_data = json.loads(principal_labels_path.read_text())

                # Write to principal_labels table
                if contract_row:
                    session.query(PrincipalLabel).filter(PrincipalLabel.contract_id == contract_row.id).delete()
                    for p in pl_data.get("principals", []):
                        if p.get("address"):
                            session.add(
                                PrincipalLabel(
                                    contract_id=contract_row.id,
                                    address=p["address"].lower(),
                                    label=p.get("display_name"),
                                    display_name=p.get("display_name"),
                                    resolved_type=p.get("resolved_type"),
                                    labels=p.get("labels"),
                                    confidence=p.get("confidence"),
                                    details=p.get("details"),
                                    graph_context=p.get("graph_context"),
                                )
                            )
                    session.commit()

                store_artifact(session, job.id, "principal_labels", data=pl_data)

                logger.info(
                    "Policy stage principal labels complete for job %s address=%s name=%s",
                    job.id,
                    job.address or "0x0",
                    job.name or "Contract",
                )

            # Cross-contract effect enrichment: propagate labels across contract boundaries
            enriched = self._enrich_cross_contract(session, job, contract_analysis, control_snapshot)
            if enriched and ep_data is not None:
                self._apply_effect_label_updates(ep_data, enriched)
                effective_permissions_path.write_text(json.dumps(ep_data, indent=2) + "\n")
                store_artifact(session, job.id, "effective_permissions", data=ep_data)

            self.update_detail(session, job, "Policy analysis complete")
            logger.info(
                "Policy stage complete for job %s address=%s name=%s",
                job.id,
                job.address or "0x0",
                job.name or "Contract",
            )

            # Auto-enroll protocol contracts into unified monitoring
            if job.protocol_id:
                try:
                    from services.monitoring.enrollment import maybe_enroll_protocol

                    enrolled = maybe_enroll_protocol(
                        session,
                        job.protocol_id,
                        rpc_url,
                        chain="ethereum",
                        exclude_job_id=job.id,
                    )
                    if enrolled:
                        logger.info(
                            "Auto-enrolled protocol %s contracts into monitoring",
                            job.protocol_id,
                        )
                        # Fetch DeFiLlama TVL so the protocol has a number immediately.
                        # Per-contract tracked value is already in contract_balances
                        # from the resolution stage — the hourly loop will create
                        # a full snapshot combining both.
                        try:
                            from db.models import Protocol, TvlSnapshot
                            from services.monitoring.tvl import fetch_defillama_tvl

                            proto = session.get(Protocol, job.protocol_id)
                            dl = fetch_defillama_tvl(proto.name) if proto else None
                            if dl:
                                session.add(
                                    TvlSnapshot(
                                        protocol_id=job.protocol_id,
                                        defillama_tvl=round(dl["tvl"], 2) if dl["tvl"] else None,
                                        chain_breakdown=dl["chain_breakdown"],
                                        source="defillama",
                                    )
                                )
                                session.commit()
                        except Exception:
                            logger.exception("Initial TVL snapshot failed for protocol %s", job.protocol_id)
                except Exception:
                    logger.exception("Auto-enrollment failed for protocol %s", job.protocol_id)

            # Send completion webhook for re-analysis jobs
            request = job.request if isinstance(job.request, dict) else {}
            if request.get("reanalysis_trigger"):
                try:
                    from services.monitoring.notifier import notify_reanalysis_complete

                    notify_reanalysis_complete(session, job)
                except Exception:
                    logger.exception(
                        "Reanalysis completion notification failed for job %s",
                        job.id,
                    )

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def _apply_effect_label_updates(self, payload: dict, enriched: dict[str, list[str]]) -> None:
        for fn in payload.get("functions", []):
            fn_sig = fn.get("function") or fn.get("abi_signature")
            if not fn_sig:
                continue
            new_labels = enriched.get(fn_sig)
            if not new_labels:
                continue
            existing = set(fn.get("effect_labels") or [])
            fn["effect_labels"] = sorted(existing | set(new_labels))

    def _enrich_cross_contract(
        self, session, job: Job, contract_analysis: dict, control_snapshot: dict
    ) -> dict[str, list[str]]:
        """Propagate effect labels across contract boundaries.

        For each external call this contract makes, look up the callee's analysis
        and propagate its effect labels to the calling function.
        """
        from services.static.cross_contract import build_callee_effect_map, enrich_cross_contract_effects

        # Find sibling jobs (same company / same parent)
        request = job.request if isinstance(job.request, dict) else {}
        parent_job_id = request.get("parent_job_id")
        company = job.company

        # Collect analyses of all completed sibling contracts
        sibling_analyses: dict[str, dict] = {}
        completed_jobs = (
            session.execute(select(Job).where(Job.status == JobStatus.completed, Job.address.isnot(None)))
            .scalars()
            .all()
        )

        for sj in completed_jobs:
            if sj.id == job.id or not sj.address:
                continue
            # Check if sibling: same company or same parent
            sj_req = sj.request if isinstance(sj.request, dict) else {}
            is_sibling = (
                (company and sj.company == company)
                or (parent_job_id and sj_req.get("parent_job_id") == parent_job_id)
                or (parent_job_id and str(sj.id) == parent_job_id)
            )
            if not is_sibling:
                continue

            sj_analysis = get_artifact(session, sj.id, "contract_analysis")
            if isinstance(sj_analysis, dict):
                sibling_analyses[sj.address.lower()] = sj_analysis

        if not sibling_analyses:
            return {}

        callee_map = build_callee_effect_map(sibling_analyses)
        controller_values = control_snapshot.get("controller_values", {})

        enriched = enrich_cross_contract_effects(contract_analysis, controller_values, callee_map)
        if enriched:
            logger.info(
                "Job %s: cross-contract enrichment added labels: %s",
                job.id,
                enriched,
            )
            # Update the effective_functions table with new labels
            contract_row = session.execute(
                select(Contract).where(Contract.job_id == job.id).limit(1)
            ).scalar_one_or_none()
            if contract_row:
                for fn_sig, new_labels in enriched.items():
                    ef = session.execute(
                        select(EffectiveFunction).where(
                            EffectiveFunction.contract_id == contract_row.id,
                            EffectiveFunction.abi_signature == fn_sig,
                        )
                    ).scalar_one_or_none()
                    if ef is None:
                        fn_name = fn_sig.split("(")[0]
                        ef = session.execute(
                            select(EffectiveFunction).where(
                                EffectiveFunction.contract_id == contract_row.id,
                                EffectiveFunction.function_name == fn_name,
                            )
                        ).scalar_one_or_none()
                    if ef:
                        existing = set(ef.effect_labels or [])
                        ef.effect_labels = sorted(existing | set(new_labels))
                session.commit()
        return enriched

    def _resolve_authority(
        self, project_dir: Path, resolved_graph_path: Path, snapshot_path: Path, contract_analysis: dict
    ) -> dict:
        """Attempt to find authority snapshot and policy state from the resolved graph."""
        snapshot = json.loads(snapshot_path.read_text())
        graph = json.loads(resolved_graph_path.read_text())

        # Find authority address from snapshot
        authority_address = None
        for controller_id, value in snapshot.get("controller_values", {}).items():
            if controller_id.endswith(":authority"):
                authority_address = str(value.get("value", "")).lower()
                break

        if not authority_address or authority_address == "0x0000000000000000000000000000000000000000":
            return {"principal_resolution": {"status": "no_authority", "reason": "No non-zero authority found"}}

        # Find authority node in graph
        authority_snapshot_path = None
        for node in graph.get("nodes", []):
            if node.get("address", "").lower() != authority_address:
                continue
            snapshot_artifact = (node.get("artifacts") or {}).get("snapshot")
            if snapshot_artifact:
                candidate = Path(snapshot_artifact)
                if candidate.exists():
                    authority_snapshot_path = candidate
            break

        if authority_snapshot_path is None:
            return {
                "principal_resolution": {
                    "status": "no_authority_snapshot",
                    "reason": "Authority contract found but snapshot artifact missing",
                }
            }

        # Check for policy tracking and HyperSync backfill
        authority_project_dir = authority_snapshot_path.parent
        authority_plan_path = authority_project_dir / "control_tracking_plan.json"
        authority_analysis_path = authority_project_dir / "contract_analysis.json"
        policy_state_path = authority_project_dir / "policy_state.json"

        if authority_plan_path.exists() and authority_analysis_path.exists():
            authority_analysis = json.loads(authority_analysis_path.read_text())
            if authority_analysis.get("policy_tracking"):
                if policy_state_path.exists():
                    return {
                        "authority_snapshot_path": authority_snapshot_path,
                        "policy_state_path": policy_state_path,
                        "principal_resolution": {
                            "status": "complete",
                            "reason": "Existing authority policy state joined into permission view",
                        },
                    }
                if os.getenv("ENVIO_API_TOKEN"):
                    run_hypersync_policy_backfill(
                        authority_plan_path,
                        url=DEFAULT_HYPERSYNC_URL,
                        state_out=policy_state_path,
                        events_out=authority_project_dir / "policy_event_history.jsonl",
                    )
                    if policy_state_path.exists():
                        return {
                            "authority_snapshot_path": authority_snapshot_path,
                            "policy_state_path": policy_state_path,
                            "principal_resolution": {
                                "status": "complete",
                                "reason": "Authority policy backfill completed",
                            },
                        }

        return {
            "authority_snapshot_path": authority_snapshot_path,
            "principal_resolution": {
                "status": "missing_policy_state",
                "reason": "Authority artifacts incomplete or ENVIO_API_TOKEN not set",
            },
        }


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        force=True,
    )
    PolicyWorker().run_loop()


if __name__ == "__main__":
    main()
