"""Resolution worker — builds control snapshot and resolves control graph."""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import cast

from sqlalchemy.orm import Session

from db.models import Job, JobStage
from db.queue import get_artifact, store_artifact
from schemas.control_tracking import ControlTrackingPlan
from services.discovery.upgrade_history import write_upgrade_history
from services.resolution.recursive import write_resolved_control_graph
from services.resolution.tracking import build_control_snapshot
from workers.base import BaseWorker

logger = logging.getLogger("workers.resolution_worker")

DEFAULT_RPC_URL = os.getenv("ETH_RPC", "https://ethereum-rpc.publicnode.com")
RECURSION_MAX_DEPTH = int(os.getenv("PSAT_RECURSION_MAX_DEPTH", "6"))


class ResolutionWorker(BaseWorker):
    stage = JobStage.resolution
    next_stage = JobStage.policy

    def process(self, session: Session, job: Job) -> None:
        logger.info(
            "Resolution stage started for job %s address=%s name=%s",
            job.id,
            job.address or "0x0",
            job.name or "Contract",
        )
        rpc_url = DEFAULT_RPC_URL
        if job.request and isinstance(job.request, dict):
            rpc_url = job.request.get("rpc_url") or rpc_url

        # Read control_tracking_plan from DB
        tracking_plan = get_artifact(session, job.id, "control_tracking_plan")
        if not isinstance(tracking_plan, dict):
            raise RuntimeError("control_tracking_plan artifact not found")

        # Read contract_analysis from DB (needed for recursive resolution)
        contract_analysis = get_artifact(session, job.id, "contract_analysis")
        if not isinstance(contract_analysis, dict):
            raise RuntimeError("contract_analysis artifact not found")

        # For impl jobs, read storage from the proxy address (where state lives)
        request = job.request if isinstance(job.request, dict) else {}
        proxy_address = request.get("proxy_address")
        if proxy_address:
            tracking_plan = {**tracking_plan, "contract_address": proxy_address}
            contract_analysis = {
                **contract_analysis,
                "subject": {**contract_analysis.get("subject", {}), "address": proxy_address},
            }
            logger.info(
                "Job %s: impl contract — reading state from proxy %s",
                job.id,
                proxy_address,
            )

        # Build control snapshot via RPC calls
        self.update_detail(session, job, "Reading current controller state")
        snapshot = build_control_snapshot(cast(ControlTrackingPlan, tracking_plan), rpc_url)
        store_artifact(session, job.id, "control_snapshot", data=snapshot)
        logger.info(
            "Resolution stage control snapshot complete for job %s address=%s name=%s",
            job.id,
            job.address or "0x0",
            job.name or "Contract",
        )

        # For recursive resolution, we need a temp workspace with the contract_analysis.json
        # because write_resolved_control_graph reads from filesystem
        tmp_dir = tempfile.mkdtemp(prefix="psat_resolution_")
        project_dir = Path(tmp_dir)
        try:
            analysis_path = project_dir / "contract_analysis.json"
            analysis_path.write_text(json.dumps(contract_analysis, indent=2) + "\n")

            # Write tracking plan and snapshot so _load_or_build_artifacts finds them
            (project_dir / "control_tracking_plan.json").write_text(json.dumps(tracking_plan, indent=2) + "\n")
            (project_dir / "control_snapshot.json").write_text(json.dumps(snapshot, indent=2) + "\n")

            self.update_detail(session, job, "Resolving recursive control graph")
            resolved_graph_path = write_resolved_control_graph(
                analysis_path,
                rpc_url=rpc_url,
                output_path=project_dir / "resolved_control_graph.json",
                max_depth=RECURSION_MAX_DEPTH,
                workspace_prefix="recursive",
                refresh_snapshots=True,
            )

            if resolved_graph_path.exists():
                store_artifact(
                    session, job.id, "resolved_control_graph", data=json.loads(resolved_graph_path.read_text())
                )
                logger.info(
                    "Resolution stage graph complete for job %s address=%s name=%s",
                    job.id,
                    job.address or "0x0",
                    job.name or "Contract",
                )

            # Phase: Upgrade history for proxy contracts (non-fatal)
            self._run_upgrade_history(session, job, project_dir)

            self.update_detail(session, job, "Resolution complete")
            logger.info(
                "Resolution stage complete for job %s address=%s name=%s",
                job.id,
                job.address or "0x0",
                job.name or "Contract",
            )

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


    def _run_upgrade_history(self, session: Session, job: Job, project_dir: Path) -> None:
        """Fetch upgrade history for proxy contracts found in dependencies. Non-fatal."""
        dependencies = get_artifact(session, job.id, "dependencies")
        if not isinstance(dependencies, dict):
            logger.info("Job %s: skipping upgrade history — no dependencies artifact", job.id)
            return

        self.update_detail(session, job, "Fetching proxy upgrade history")
        deps_path = project_dir / "dependencies.json"
        deps_path.write_text(json.dumps(dependencies, indent=2) + "\n")

        try:
            uh_path = write_upgrade_history(deps_path)
            if uh_path and uh_path.exists():
                store_artifact(session, job.id, "upgrade_history", data=json.loads(uh_path.read_text()))
                logger.info(
                    "Resolution stage upgrade history complete for job %s address=%s",
                    job.id,
                    job.address or "0x0",
                )
            else:
                logger.info(
                    "Resolution stage upgrade history skipped for job %s — no proxies found",
                    job.id,
                )
        except Exception as exc:
            logger.warning(
                "Resolution stage upgrade history failed for job %s address=%s: %s",
                job.id,
                job.address or "0x0",
                exc,
            )
            store_artifact(session, job.id, "upgrade_history_error", data={"error": str(exc)})


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        force=True,
    )
    ResolutionWorker().run_loop()


if __name__ == "__main__":
    main()
