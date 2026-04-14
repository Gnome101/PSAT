"""DApp Crawl worker — discovers contract addresses by crawling DApp frontends.

Calls the integrated dapp crawler directly (no subprocess) to visit DApp URLs
with a spoofed wallet and capture contract interactions, then creates child
jobs for each discovered address.
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import Contract, DAppInteraction, Job, JobStage
from db.queue import (
    complete_job,
    count_analysis_children,
    create_job,
    get_or_create_protocol,
    store_artifact,
)
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
        chain_id_to_name = {
            1: "ethereum",
            10: "optimism",
            56: "bsc",
            137: "polygon",
            8453: "base",
            42161: "arbitrum",
            43114: "avalanche",
            534352: "scroll",
        }
        wait = request.get("wait") or 10
        analyze_limit = request.get("analyze_limit", 5)

        # Derive / create Protocol row from URL hostname if no company context exists
        first_host = (urlparse(urls[0]).hostname or "").lstrip(".")
        if first_host.startswith("www."):
            first_host = first_host[4:]
        protocol_name = job.company or first_host or f"dapp_{str(job.id)[:8]}"
        official_domain = first_host or None
        protocol_row = get_or_create_protocol(session, protocol_name, official_domain=official_domain)
        job.protocol_id = protocol_row.id
        if not job.company:
            job.company = protocol_row.name
        session.commit()

        self.update_detail(session, job, f"Preparing crawl for {len(urls)} DApp URL(s)")
        logger.info("DApp crawl started for job %s: %d URLs", job.id, len(urls))

        def report(detail: str) -> None:
            self.update_detail(session, job, detail)

        # Call crawler directly — no subprocess
        result = crawl_dapp(
            urls,
            chain_id=chain_id,
            wait=wait,
            progress=report,
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

        # Persist full interaction log for later audit / analytics
        for entry in result.get("interactions", []):
            to_raw = entry.get("to") or ""
            session.add(
                DAppInteraction(
                    job_id=job.id,
                    protocol_id=protocol_row.id,
                    type=str(entry.get("type") or "unknown"),
                    page_url=entry.get("url"),
                    to_address=to_raw.lower() if to_raw else None,
                    value=entry.get("value"),
                    data=entry.get("data"),
                    method_selector=entry.get("method_selector"),
                    typed_data=entry.get("typed_data"),
                    is_permit=bool(entry.get("is_permit")),
                    message=entry.get("message"),
                    captured_at=entry.get("timestamp"),
                )
            )
        session.commit()

        # Write ALL discovered addresses to contracts table
        protocol_id = protocol_row.id
        default_chain = request.get("chain") or chain_id_to_name.get(chain_id)
        # Build per-address context from address_details
        detail_by_addr: dict[str, dict] = {}
        for detail in result.get("address_details", []):
            addr = detail.get("address", "").lower()
            if addr:
                detail_by_addr[addr] = detail

        for addr in addresses:
            normalized = addr.lower()
            info = detail_by_addr.get(normalized, {})
            addr_chain = info.get("chain") or default_chain
            source_urls = info.get("source_urls", [])
            existing_contract = session.execute(
                select(Contract).where(Contract.address == normalized, Contract.chain == addr_chain)
            ).scalar_one_or_none()
            if existing_contract is None:
                session.add(
                    Contract(
                        address=normalized,
                        chain=addr_chain,
                        protocol_id=protocol_id,
                        discovery_source="dapp_crawl",
                        discovery_url=source_urls[0] if source_urls else None,
                    )
                )
            elif existing_contract.protocol_id is None and protocol_id:
                existing_contract.protocol_id = protocol_id
        session.commit()

        # Deduplicate against existing jobs and create children (shared global cap)
        root_job_id = request.get("root_job_id", str(job.id))
        already_used = count_analysis_children(session, root_job_id)
        remaining = max(0, analyze_limit - already_used)
        child_ids = []
        seen_addresses: set[str] = set()
        for addr in addresses:
            if len(child_ids) >= remaining:
                break

            normalized = addr.lower()
            if normalized in seen_addresses:
                logger.info("Job %s: address %s already seen in crawl results, skipping", job.id, addr)
                continue
            seen_addresses.add(normalized)

            existing = session.execute(
                select(Job)
                .where(
                    Job.address == addr,
                    Job.request["root_job_id"].as_string() == root_job_id,
                )
                .limit(1)
            ).scalar_one_or_none()
            if existing:
                logger.info("Job %s: address %s already has job %s, skipping", job.id, addr, existing.id)
                continue

            addr_info = detail_by_addr.get(normalized, {})
            child_request = {
                "address": addr,
                "name": f"dapp_{addr[2:10]}",
                "company": protocol_row.name,
                "parent_job_id": str(job.id),
                "root_job_id": root_job_id,
                "rpc_url": request.get("rpc_url"),
                "discovery_source": "dapp_crawl",
                "protocol_id": protocol_id,
                "chain": addr_info.get("chain") or request.get("chain") or default_chain,
            }

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
