"""Discovery worker — fetches verified source from Etherscan and stores in DB.

For address-mode jobs: fetches source, stores files + metadata, advances to static.
For company-mode jobs: discovers contracts via protocol inventory, creates child jobs for each.
"""

from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from db.models import Job, JobStage
from db.queue import create_job, store_artifact, store_source_files
from services.discovery.fetch import fetch, is_vyper_result, parse_remappings, parse_sources
from services.discovery.inventory import search_protocol_inventory
from workers.base import BaseWorker, JobHandledDirectly

logger = logging.getLogger("workers.discovery")


class DiscoveryWorker(BaseWorker):
    stage = JobStage.discovery
    next_stage = JobStage.static

    def process(self, session: Session, job: Job) -> None:
        if job.company and not job.address:
            self._process_company(session, job)
        elif job.address:
            self._process_address(session, job)
        else:
            raise ValueError("Job has neither address nor company")

    def _process_company(self, session: Session, job: Job) -> None:
        """Discover contracts for a company and create child jobs for each."""
        company = job.company
        if company is None:
            raise ValueError("Company job missing company name")
        request = job.request if isinstance(job.request, dict) else {}
        chain = request.get("chain")
        discover_limit = request.get("discover_limit", 25)
        analyze_limit = request.get("analyze_limit", 5)

        self.update_detail(session, job, f"Discovering contracts for {company}")
        inventory = search_protocol_inventory(company, chain=chain, limit=discover_limit)

        store_artifact(session, job.id, "contract_inventory", data=inventory)

        discovered = [e for e in inventory.get("contracts", []) if e.get("address")]
        selected = discovered[:analyze_limit]

        if not selected:
            self.update_detail(session, job, "No contracts found to analyze")
            # Store summary and let base worker complete the job
            store_artifact(session, job.id, "discovery_summary", data={
                "mode": "company",
                "company": company,
                "discovered_count": len(discovered),
                "analyzed_count": 0,
            })
            # Override next_stage to done since there's nothing to process
            from db.queue import complete_job
            complete_job(session, job.id, f"Discovery complete for {company}: no contracts found")
            # Signal base worker to skip the normal advance
            raise JobHandledDirectly()

        child_ids = []
        for contract in selected:
            addr = str(contract["address"])
            child_name = str(contract.get("name") or f"{company}_{addr[2:10]}")
            child_chains = contract.get("chains")
            child_chain = child_chains[0] if isinstance(child_chains, list) and child_chains else contract.get("chain")
            child_request = {
                "address": addr,
                "name": child_name,
                "chain": child_chain,
                "rpc_url": request.get("rpc_url"),
                "parent_job_id": str(job.id),
            }
            child_job = create_job(session, child_request)
            child_ids.append({"job_id": str(child_job.id), "address": addr, "name": child_name, "chain": child_chain})
            logger.info("Created child job %s for %s (%s)", child_job.id, addr, child_name)

        store_artifact(session, job.id, "discovery_summary", data={
            "mode": "company",
            "company": company,
            "official_domain": inventory.get("official_domain"),
            "discovered_count": len(discovered),
            "analyzed_count": len(child_ids),
            "child_jobs": child_ids,
        })

        if not job.name:
            job.name = company
            session.commit()

        self.update_detail(
            session, job,
            f"Discovered {len(discovered)} contracts, queued {len(child_ids)} for analysis",
        )

        # Complete the parent job — children will run through the pipeline independently
        from db.queue import complete_job
        complete_job(session, job.id, f"Discovery complete: {len(child_ids)} contracts queued")
        raise JobHandledDirectly()

    def _process_address(self, session: Session, job: Job) -> None:
        """Fetch verified source for a single address."""
        address = job.address
        if address is None:
            raise ValueError("Address job missing address")

        self.update_detail(session, job, f"Fetching verified source for {address}")
        result = fetch(address)

        contract_name = result.get("ContractName", "Contract")

        sources = parse_sources(result)
        remappings = parse_remappings(result)

        self.update_detail(session, job, "Storing source files")
        store_source_files(session, job.id, sources)

        meta = {
            "address": address,
            "contract_name": contract_name,
            "compiler_version": result.get("CompilerVersion", ""),
            "language": "vyper" if is_vyper_result(result) else "solidity",
            "optimization_used": result.get("OptimizationUsed", ""),
            "runs": result.get("Runs", ""),
            "evm_version": result.get("EVMVersion", ""),
            "license": result.get("LicenseType", ""),
            "source_format": "standard_json" if "sources" in str(result.get("SourceCode", ""))[:10] else "flat",
            "source_file_count": len(sources),
            "remappings": remappings,
        }
        store_artifact(session, job.id, "contract_meta", data=meta)

        raw_evm = result.get("EVMVersion", "") or ""
        evm_version = raw_evm if raw_evm.lower() not in ("", "default") else "shanghai"
        build_settings = {
            "evm_version": evm_version,
            "optimization_used": result.get("OptimizationUsed", "1") == "1",
            "runs": int(result.get("Runs", "200") or 200),
        }
        store_artifact(session, job.id, "build_settings", data=build_settings)

        if not job.name:
            job.name = f"{contract_name}_{address[2:10]}"
            session.commit()

        self.update_detail(session, job, f"Discovery complete: {contract_name}")
        logger.info("Discovery complete for %s (%s)", address, contract_name)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        force=True,
    )
    DiscoveryWorker().run_loop()


if __name__ == "__main__":
    main()
