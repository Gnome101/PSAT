"""DApp Crawl worker — discovers contract addresses by crawling DApp frontends.

Invokes the dapp-crawler repo as a subprocess, parses discovered addresses,
and creates child jobs for each so they enter the normal PSAT pipeline.
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

logger = logging.getLogger("workers.dapp_crawl")

PSAT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CRAWLER_PATH = str(PSAT_ROOT.parent / "dapp-crawler")
CRAWLER_PATH = os.getenv("DAPP_CRAWLER_PATH", DEFAULT_CRAWLER_PATH)
SUBPROCESS_TIMEOUT = int(os.getenv("DAPP_CRAWL_TIMEOUT", "600"))


def _find_python(crawler_dir: str) -> list[str]:
    """Find the best Python command to run the crawler."""
    venv_python = Path(crawler_dir) / ".venv" / "bin" / "python"
    if venv_python.exists():
        return [str(venv_python)]
    return ["uv", "run", "--no-sync", "python"]


class DAppCrawlWorker(BaseWorker):
    stage = JobStage.dapp_crawl
    next_stage = JobStage.done

    def process(self, session: Session, job: Job) -> None:
        request = job.request if isinstance(job.request, dict) else {}
        urls = request.get("dapp_urls", [])
        if not urls:
            raise ValueError("dapp_crawl job missing dapp_urls in request")

        chain_id = request.get("chain_id") or 1
        wait = request.get("wait") or 10
        analyze_limit = request.get("analyze_limit", 25)

        self.update_detail(session, job, f"Crawling {len(urls)} DApp URL(s)")
        logger.info("DApp crawl started for job %s: %d URLs", job.id, len(urls))

        with tempfile.TemporaryDirectory(prefix="psat_dapp_crawl_") as tmp:
            url_file = Path(tmp) / "urls.json"
            url_file.write_text(json.dumps(urls))

            psat_export = Path(tmp) / "psat_addresses.json"
            interactions_out = Path(tmp) / "interactions.json"

            python_cmd = _find_python(CRAWLER_PATH)
            cmd = [
                *python_cmd,
                str(Path(CRAWLER_PATH) / "main.py"),
                "--url-file", str(url_file),
                "--output", str(interactions_out),
                "--psat-export", str(psat_export),
                "--chain-id", str(chain_id),
                "--wait", str(wait),
            ]

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
                logger.error("DApp crawler failed for job %s: %s", job.id, error_msg)
                raise RuntimeError(f"dapp-crawler exited {result.returncode}: {error_msg}")

            # Parse discovered addresses
            if not psat_export.exists():
                logger.warning("Job %s: dapp-crawler produced no PSAT export", job.id)
                addresses = []
            else:
                export_data = json.loads(psat_export.read_text())
                addresses = export_data.get("addresses", [])

            # Store full interaction log as artifact if available
            if interactions_out.exists():
                interactions = json.loads(interactions_out.read_text())
                store_artifact(session, job.id, "dapp_crawl_interactions", data=interactions)

        logger.info("DApp crawl found %d addresses for job %s", len(addresses), job.id)

        # Store raw results
        store_artifact(session, job.id, "dapp_crawl_results", data={
            "urls_crawled": urls,
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
                "name": f"dapp_{addr[2:10]}",
                "parent_job_id": str(job.id),
                "root_job_id": root_job_id,
                "rpc_url": request.get("rpc_url"),
            }
            if request.get("chain"):
                child_request["chain"] = request["chain"]

            child_job = create_job(session, child_request)
            child_ids.append({"job_id": str(child_job.id), "address": addr})
            logger.info("Created child job %s for %s", child_job.id, addr)

        store_artifact(session, job.id, "discovery_summary", data={
            "mode": "dapp_crawl",
            "urls": urls,
            "discovered_count": len(addresses),
            "analyzed_count": len(child_ids),
            "child_jobs": child_ids,
        })

        if not job.name:
            job.name = f"DApp crawl ({len(urls)} URLs)"
            session.commit()

        complete_job(
            session, job.id,
            f"DApp crawl complete: {len(addresses)} addresses found, {len(child_ids)} queued",
        )
        raise JobHandledDirectly()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        force=True,
    )
    DAppCrawlWorker().run_loop()


if __name__ == "__main__":
    main()
