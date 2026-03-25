"""Policy worker — computes effective permissions and labels principals."""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from pathlib import Path

from db.models import JobStage
from db.queue import get_artifact, store_artifact
from services.policy.hypersync_backfill import run_hypersync_policy_backfill
from workers.base import BaseWorker

logger = logging.getLogger(__name__)

DEFAULT_RPC_URL = os.getenv("ETH_RPC", "https://ethereum-rpc.publicnode.com")
DEFAULT_HYPERSYNC_URL = "https://eth.hypersync.xyz"


class PolicyWorker(BaseWorker):
    stage = JobStage.policy
    next_stage = JobStage.done

    def process(self, session, job):
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

        if not isinstance(contract_analysis, dict):
            raise RuntimeError("contract_analysis artifact not found")
        if not isinstance(control_snapshot, dict):
            raise RuntimeError("control_snapshot artifact not found")

        # We need temp files for functions that read from filesystem
        tmp_dir = tempfile.mkdtemp(prefix="psat_policy_")
        project_dir = Path(tmp_dir)
        try:
            analysis_path = project_dir / "contract_analysis.json"
            analysis_path.write_text(json.dumps(contract_analysis, indent=2) + "\n")

            snapshot_path = project_dir / "control_snapshot.json"
            snapshot_path.write_text(json.dumps(control_snapshot, indent=2) + "\n")

            resolved_graph_path = None
            if isinstance(resolved_control_graph, dict):
                resolved_graph_path = project_dir / "resolved_control_graph.json"
                resolved_graph_path.write_text(json.dumps(resolved_control_graph, indent=2) + "\n")

            # Determine authority snapshot and policy state
            authority_snapshot_path = None
            policy_state_path = None
            principal_resolution = {"status": "no_authority", "reason": "Worker-mode authority resolution"}

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
                output_path=project_dir / "effective_permissions.json",
                principal_resolution=principal_resolution,
            )

            if effective_permissions_path.exists():
                store_artifact(
                    session, job.id, "effective_permissions", data=json.loads(effective_permissions_path.read_text())
                )
                logger.info(
                    "Policy stage effective permissions complete for job %s address=%s name=%s",
                    job.id,
                    job.address or "0x0",
                    job.name or "Contract",
                )

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
                store_artifact(
                    session, job.id, "principal_labels", data=json.loads(principal_labels_path.read_text())
                )
                logger.info(
                    "Policy stage principal labels complete for job %s address=%s name=%s",
                    job.id,
                    job.address or "0x0",
                    job.name or "Contract",
                )

            self.update_detail(session, job, "Policy analysis complete")
            logger.info(
                "Policy stage complete for job %s address=%s name=%s",
                job.id,
                job.address or "0x0",
                job.name or "Contract",
            )

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    PolicyWorker().run_loop()


if __name__ == "__main__":
    main()
