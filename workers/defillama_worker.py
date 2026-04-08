"""DefiLlama worker — discovers contract addresses from DefiLlama adapter source code.

Invokes the defillama-crawler repo as a subprocess, parses discovered addresses
with chain context, and creates child jobs for each.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import Job, JobStage
from db.queue import complete_job, count_analysis_children, create_job, store_artifact
from workers.base import BaseWorker, JobHandledDirectly

logger = logging.getLogger("workers.defillama")

PSAT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CRAWLER_PATH = str(PSAT_ROOT.parent / "defillama-crawler")
CRAWLER_PATH = os.getenv("DEFILLAMA_CRAWLER_PATH", DEFAULT_CRAWLER_PATH)
SUBPROCESS_TIMEOUT = int(os.getenv("DEFILLAMA_CRAWL_TIMEOUT", "300"))


def _find_python(crawler_dir: str) -> list[str]:
    """Find the best Python command to run the crawler."""
    venv_python = Path(crawler_dir) / ".venv" / "bin" / "python"
    if venv_python.exists():
        return [str(venv_python)]
    return ["uv", "run", "--no-sync", "python"]


class DefiLlamaWorker(BaseWorker):
    stage = JobStage.defillama_scan
    next_stage = JobStage.done

    def process(self, session: Session, job: Job) -> None:
        request = job.request if isinstance(job.request, dict) else {}
        protocol = request.get("defillama_protocol")
        if not protocol:
            raise ValueError("defillama_scan job missing defillama_protocol in request")

        analyze_limit = request.get("analyze_limit", 5)
        no_clone = os.getenv("DEFILLAMA_NO_CLONE", "").lower() in ("1", "true", "yes")

        self.update_detail(session, job, f"Scanning DefiLlama adapters for {protocol}")
        logger.info("DefiLlama scan started for job %s: protocol=%s", job.id, protocol)

        with tempfile.TemporaryDirectory(prefix="psat_defillama_") as tmp:
            psat_export = Path(tmp) / "psat_addresses.json"
            full_output = Path(tmp) / "protocols.json"

            python_cmd = _find_python(CRAWLER_PATH)
            cmd = [
                *python_cmd,
                str(Path(CRAWLER_PATH) / "main.py"),
                "--protocol", protocol,
                "--output", str(full_output),
                "--psat-export", str(psat_export),
            ]
            if no_clone:
                cmd.append("--no-clone")

            logger.info("Running: %s", " ".join(cmd))
            result = subprocess.run(
                cmd,
                cwd=CRAWLER_PATH,
                capture_output=True,
                text=True,
                timeout=SUBPROCESS_TIMEOUT,
            )

            if result.returncode != 0:
                error_msg = result.stderr[-2000:] if result.stderr else "Unknown error"
                logger.error("DefiLlama crawler failed for job %s: %s", job.id, error_msg)
                raise RuntimeError(f"defillama-crawler exited {result.returncode}: {error_msg}")

            # Parse discovered addresses
            if not psat_export.exists():
                logger.warning("Job %s: defillama-crawler produced no PSAT export", job.id)
                addresses = []
            else:
                export_data = json.loads(psat_export.read_text())
                addresses = export_data.get("addresses", [])

            # Parse full output for chain context
            chain_by_address: dict[str, str | None] = {}
            if full_output.exists():
                full_data = json.loads(full_output.read_text())
                store_artifact(session, job.id, "defillama_full_scan", data=full_data)
                for proto in full_data.get("protocols", []):
                    for entry in proto.get("addresses", []):
                        addr = entry.get("address", "").lower()
                        chain = entry.get("chain")
                        if addr and chain:
                            chain_by_address[addr] = chain

        logger.info("DefiLlama scan found %d addresses for job %s", len(addresses), job.id)

        # Store raw results
        store_artifact(session, job.id, "defillama_scan_results", data={
            "protocol": protocol,
            "addresses_found": len(addresses),
            "addresses": addresses,
        })

        # Deduplicate against existing jobs and create children (shared global cap)
        root_job_id = request.get("root_job_id", str(job.id))
        already_used = count_analysis_children(session, root_job_id)
        remaining = max(0, analyze_limit - already_used)
        selected = addresses[:remaining]
        child_ids = []
        for addr in selected:
            existing = session.execute(
                select(Job).where(Job.address == addr).limit(1)
            ).scalar_one_or_none()
            if existing:
                logger.info("Job %s: address %s already has job %s, skipping", job.id, addr, existing.id)
                continue

            child_request = {
                "address": addr,
                "name": f"{protocol}_{addr[2:10]}",
                "parent_job_id": str(job.id),
                "root_job_id": root_job_id,
                "rpc_url": request.get("rpc_url"),
            }
            chain = chain_by_address.get(addr.lower()) or request.get("chain")
            if chain:
                child_request["chain"] = chain

            child_job = create_job(session, child_request)
            child_ids.append({"job_id": str(child_job.id), "address": addr, "chain": chain})
            logger.info("Created child job %s for %s (chain=%s)", child_job.id, addr, chain)

        store_artifact(session, job.id, "discovery_summary", data={
            "mode": "defillama_scan",
            "protocol": protocol,
            "discovered_count": len(addresses),
            "analyzed_count": len(child_ids),
            "child_jobs": child_ids,
        })

        if not job.name:
            job.name = f"DefiLlama: {protocol}"
            session.commit()

        complete_job(
            session, job.id,
            f"DefiLlama scan complete for {protocol}: {len(addresses)} addresses found, {len(child_ids)} queued",
        )
        raise JobHandledDirectly()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        force=True,
    )
    DefiLlamaWorker().run_loop()


if __name__ == "__main__":
    main()
