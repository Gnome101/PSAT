"""Discovery worker — fetches verified source from Etherscan and stores in DB.

For address-mode jobs: fetches source, stores files + metadata, advances to static.
For company-mode jobs: discovers contracts via protocol inventory, creates child jobs for each.
"""

from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from db.models import Contract, Job, JobStage
from db.queue import count_analysis_children, create_job, store_artifact, store_source_files
from services.discovery.deployer import _batch_get_creators
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
        analyze_limit = request.get("analyze_limit", 5)
        root_job_id = str(job.id)

        self.update_detail(session, job, f"Discovering contracts for {company}")
        inventory = search_protocol_inventory(company, chain=chain)

        store_artifact(session, job.id, "contract_inventory", data=inventory)

        discovered = [e for e in inventory.get("contracts", []) if e.get("address")]
        already_used = count_analysis_children(session, root_job_id)
        remaining = max(0, analyze_limit - already_used)
        selected = discovered[:remaining]

        if not selected:
            self.update_detail(session, job, "No contracts found to analyze")
            # Store summary and let base worker complete the job
            store_artifact(
                session,
                job.id,
                "discovery_summary",
                data={
                    "mode": "company",
                    "company": company,
                    "discovered_count": len(discovered),
                    "analyzed_count": 0,
                },
            )
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
                "parent_job_id": root_job_id,
                "root_job_id": root_job_id,
                "rank_score": contract.get("rank_score"),
                "confidence": contract.get("confidence"),
                "discovery_source": contract.get("discovery_source"),
                "chains": contract.get("chains"),
            }
            child_job = create_job(session, child_request)
            child_ids.append({"job_id": str(child_job.id), "address": addr, "name": child_name, "chain": child_chain})
            logger.info("Created child job %s for %s (%s)", child_job.id, addr, child_name)

        store_artifact(
            session,
            job.id,
            "discovery_summary",
            data={
                "mode": "company",
                "company": company,
                "official_domain": inventory.get("official_domain"),
                "discovered_count": len(discovered),
                "analyzed_count": len(child_ids),
                "child_jobs": child_ids,
            },
        )

        if not job.name:
            job.name = company
            session.commit()

        self._spawn_parallel_discovery(session, job, company, request, root_job_id)

        self.update_detail(
            session,
            job,
            f"Discovered {len(discovered)} contracts, queued {len(child_ids)} for analysis",
        )

        # Complete the parent job — children will run through the pipeline independently
        from db.queue import complete_job

        complete_job(session, job.id, f"Discovery complete: {len(child_ids)} contracts queued")
        raise JobHandledDirectly()

    def _spawn_parallel_discovery(
        self,
        session: Session,
        job: Job,
        company: str,
        request: dict,
        root_job_id: str,
    ) -> None:
        """Spawn DApp crawl and DefiLlama scan jobs if we can resolve the protocol."""
        from services.discovery.protocol_resolver import resolve_protocol

        protocol = resolve_protocol(company)
        if not protocol.get("slug") and not protocol.get("url"):
            logger.info("Job %s: no DefiLlama match for '%s', skipping parallel discovery", job.id, company)
            return

        logger.info(
            "Job %s: resolved '%s' → slug=%s url=%s",
            job.id,
            company,
            protocol.get("slug"),
            protocol.get("url"),
        )

        # Spawn DefiLlama adapter scans — one per sub-protocol
        all_slugs = protocol.get("all_slugs", [])
        if not all_slugs and protocol.get("slug"):
            all_slugs = [protocol["slug"]]
        for slug in all_slugs:
            defillama_request = {
                "defillama_protocol": slug,
                "name": f"{company}_defillama_{slug}",
                "parent_job_id": str(job.id),
                "root_job_id": root_job_id,
                "analyze_limit": request.get("analyze_limit", 5),
                "rpc_url": request.get("rpc_url"),
            }
            dl_job = create_job(session, defillama_request, initial_stage=JobStage.defillama_scan)
            dl_job.company = company
            session.commit()
            logger.info("Job %s: spawned DefiLlama scan job %s (slug=%s)", job.id, dl_job.id, slug)

        # Spawn DApp crawl
        dapp_url = protocol.get("url")
        if dapp_url:
            dapp_request = {
                "dapp_urls": [dapp_url],
                "name": f"{company}_dapp_crawl",
                "parent_job_id": str(job.id),
                "root_job_id": root_job_id,
                "analyze_limit": request.get("analyze_limit", 5),
                "chain_id": request.get("chain_id") or 1,
                "wait": request.get("wait", 10),
                "rpc_url": request.get("rpc_url"),
            }
            crawl_job = create_job(session, dapp_request, initial_stage=JobStage.dapp_crawl)
            crawl_job.company = company
            session.commit()
            logger.info("Job %s: spawned DApp crawl job %s (url=%s)", job.id, crawl_job.id, dapp_url)

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

        raw_evm = result.get("EVMVersion", "") or ""
        evm_version = raw_evm if raw_evm.lower() not in ("", "default") else "shanghai"

        # Look up deployer wallet via Etherscan
        deployer = None
        try:
            creators = _batch_get_creators([address])
            deployer = creators.get(address.lower())
        except Exception:
            logger.debug("Could not fetch deployer for %s", address)

        # Write to contracts table (replaces contract_meta + build_settings artifacts)
        request = job.request if isinstance(job.request, dict) else {}
        contract = Contract(
            job_id=job.id,
            address=address,
            chain=request.get("chain"),
            contract_name=contract_name,
            compiler_version=result.get("CompilerVersion", ""),
            language="vyper" if is_vyper_result(result) else "solidity",
            evm_version=evm_version,
            optimization=result.get("OptimizationUsed", "1") == "1",
            optimization_runs=int(result.get("Runs", "200") or 200),
            source_format="standard_json" if "sources" in str(result.get("SourceCode", ""))[:10] else "flat",
            source_file_count=len(sources),
            license=result.get("LicenseType", ""),
            deployer=deployer,
            remappings=remappings or [],
            rank_score=request.get("rank_score"),
            confidence=request.get("confidence"),
            discovery_source=request.get("discovery_source"),
            chains=request.get("chains"),
        )
        session.merge(contract)
        session.commit()

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
