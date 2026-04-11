"""DefiLlama worker — discovers contract addresses from DefiLlama adapter source code.

Calls the integrated defillama crawler directly (no subprocess) to scan
adapter source for contract addresses, then creates child jobs for each.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import Contract, Job, JobStage
from db.queue import complete_job, count_analysis_children, create_job, store_artifact
from services.crawlers.defillama.scan import scan_protocol
from workers.base import BaseWorker, JobHandledDirectly

logger = logging.getLogger("workers.defillama")

DEFAULT_REPO_PATH = Path(__file__).resolve().parents[1] / "repo" / "DefiLlama-Adapters"
REPO_PATH = Path(os.getenv("DEFILLAMA_REPO_PATH", str(DEFAULT_REPO_PATH)))


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

        self.update_detail(session, job, f"Preparing DefiLlama scan for {protocol}")
        logger.info("DefiLlama scan started for job %s: protocol=%s", job.id, protocol)

        def report(detail: str) -> None:
            self.update_detail(session, job, detail)

        # Call crawler directly — no subprocess
        result = scan_protocol(
            protocol_name=protocol,
            repo_path=REPO_PATH,
            no_clone=no_clone,
            progress=report,
        )

        addresses = result["addresses"]
        logger.info("DefiLlama scan found %d addresses for job %s", len(addresses), job.id)

        # Store full scan details as artifact
        store_artifact(
            session,
            job.id,
            "defillama_full_scan",
            data={
                "protocol": protocol,
                "scan_time": result["scan_time"],
                "address_details": result["address_details"],
            },
        )

        # Store raw results
        store_artifact(
            session,
            job.id,
            "defillama_scan_results",
            data={
                "protocol": protocol,
                "addresses_found": len(addresses),
                "addresses": addresses,
            },
        )

        # Build chain lookup from detailed results
        chain_by_address: dict[str, str | None] = {}
        for entry in result.get("address_details", []):
            addr = entry.get("address", "").lower()
            chain = entry.get("chain")
            if addr and chain:
                chain_by_address[addr] = chain

        # Write ALL discovered addresses to contracts table
        protocol_id = job.protocol_id
        for addr in addresses:
            normalized = addr.lower()
            chain = chain_by_address.get(normalized)
            existing_contract = session.execute(
                select(Contract).where(Contract.address == normalized, Contract.chain == chain)
            ).scalar_one_or_none()
            if existing_contract is None:
                session.add(Contract(
                    address=normalized,
                    chain=chain,
                    protocol_id=protocol_id,
                    discovery_source="defillama",
                ))
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
                logger.info("Job %s: address %s already seen in scan results, skipping", job.id, addr)
                continue
            seen_addresses.add(normalized)

            existing = session.execute(select(Job).where(Job.address == addr).limit(1)).scalar_one_or_none()
            if existing:
                logger.info("Job %s: address %s already has job %s, skipping", job.id, addr, existing.id)
                continue

            child_request = {
                "address": addr,
                "name": f"{protocol}_{addr[2:10]}",
                "parent_job_id": str(job.id),
                "root_job_id": root_job_id,
                "rpc_url": request.get("rpc_url"),
                "discovery_source": "defillama",
                "protocol_id": protocol_id,
            }
            chain = chain_by_address.get(addr.lower()) or request.get("chain")
            if chain:
                child_request["chain"] = chain

            child_job = create_job(session, child_request)
            child_ids.append({"job_id": str(child_job.id), "address": addr, "chain": chain})
            logger.info("Created child job %s for %s (chain=%s)", child_job.id, addr, chain)

        store_artifact(
            session,
            job.id,
            "discovery_summary",
            data={
                "mode": "defillama_scan",
                "protocol": protocol,
                "discovered_count": len(addresses),
                "analyzed_count": len(child_ids),
                "child_jobs": child_ids,
            },
        )

        if not job.name:
            job.name = f"DefiLlama: {protocol}"
            session.commit()

        complete_job(
            session,
            job.id,
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
