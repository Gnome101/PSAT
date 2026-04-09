"""DApp Crawl worker — discovers contract addresses by crawling DApp frontends.

Calls the integrated dapp crawler directly (no subprocess) to visit DApp URLs
with a spoofed wallet and capture contract interactions, then creates child
jobs for each discovered address.
"""

from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import Job, JobStage
from db.queue import complete_job, count_analysis_children, create_job, store_artifact
from services.crawlers.dapp.crawl import crawl_dapp
from workers.base import BaseWorker, JobHandledDirectly

logger = logging.getLogger("workers.dapp_crawl")


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

        # Call crawler directly — no subprocess
        result = crawl_dapp(
            urls,
            chain_id=chain_id,
            wait=wait,
        )

        addresses = result["addresses"]
        logger.info("DApp crawl found %d addresses for job %s", len(addresses), job.id)

        # Store raw results
        store_artifact(
            session,
            job.id,
            "dapp_crawl_results",
            data={
                "urls_crawled": urls,
                "addresses_found": len(addresses),
                "addresses": addresses,
                "interaction_count": result.get("interaction_count", 0),
            },
        )

        # Deduplicate against existing jobs and create children (shared global cap)
        root_job_id = request.get("root_job_id", str(job.id))
        already_used = count_analysis_children(session, root_job_id)
        remaining = max(0, analyze_limit - already_used)
        selected = addresses[:remaining]
        child_ids = []
        for addr in selected:
            existing = session.execute(select(Job).where(Job.address == addr).limit(1)).scalar_one_or_none()
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

        store_artifact(
            session,
            job.id,
            "discovery_summary",
            data={
                "mode": "dapp_crawl",
                "urls": urls,
                "discovered_count": len(addresses),
                "analyzed_count": len(child_ids),
                "child_jobs": child_ids,
            },
        )

        if not job.name:
            job.name = f"DApp crawl ({len(urls)} URLs)"
            session.commit()

        complete_job(
            session,
            job.id,
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
