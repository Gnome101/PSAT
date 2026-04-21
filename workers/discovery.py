"""Discovery worker — fetches verified source from Etherscan and stores in DB.

For address-mode jobs: fetches source, stores files + metadata, advances to static.
For company-mode jobs: discovers contracts via protocol inventory, writes them
to the ``contracts`` table, spawns DApp / DefiLlama sibling jobs, then advances
to the ``selection`` stage. The ``SelectionWorker`` ranks the unified contract
set and creates the top-N analysis child jobs once the siblings settle.
"""

from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import Contract, Job, JobStage
from db.queue import (
    advance_job,
    copy_static_cache,
    create_job,
    find_completed_static_cache,
    find_previous_company_inventory,
    get_artifact,
    get_or_create_protocol,
    store_artifact,
    store_source_files,
    upsert_discovered_contract,
)
from services.discovery.audit_reports import merge_audit_reports, search_audit_reports
from services.discovery.deployer import _batch_get_creators
from services.discovery.fetch import fetch, is_vyper_result, parse_remappings, parse_sources
from services.discovery.inventory import merge_inventory, search_protocol_inventory
from workers.base import BaseWorker, JobHandledDirectly

logger = logging.getLogger("workers.discovery")


def _sync_audit_reports_to_db(session: Session, protocol_id: int, reports: list[dict]) -> None:
    """Upsert audit report rows into the relational table."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from db.models import AuditReport

    for report in reports:
        auditor = str(report.get("auditor") or "").strip()
        title = str(report.get("title") or "").strip()
        url = str(report.get("url") or "").strip()
        if not url or not auditor or not title:
            continue

        stmt = pg_insert(AuditReport).values(
            protocol_id=protocol_id,
            url=url,
            pdf_url=report.get("pdf_url"),
            auditor=auditor,
            title=title,
            date=report.get("date"),
            confidence=report.get("confidence"),
            source_url=report.get("source_url"),
            # Needed by services/audits/source_equivalence for GitHub lookup.
            source_repo=report.get("source_repo"),
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_audit_report_protocol_url",
            set_={
                "pdf_url": stmt.excluded.pdf_url,
                "auditor": stmt.excluded.auditor,
                "title": stmt.excluded.title,
                "date": stmt.excluded.date,
                "confidence": stmt.excluded.confidence,
                "source_url": stmt.excluded.source_url,
                "source_repo": stmt.excluded.source_repo,
            },
        )
        session.execute(stmt)
    session.commit()


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
        """Discover contracts for a company and advance to the selection stage.

        All three discovery sources (this inventory pass, plus the DApp
        and DefiLlama siblings spawned below) write into ``contracts``
        without queuing analysis jobs. The ``SelectionWorker`` ranks the
        unified set and spends the ``analyze_limit`` budget in one pass.
        """
        company = job.company
        if company is None:
            raise ValueError("Company job missing company name")
        request = job.request if isinstance(job.request, dict) else {}
        chain = request.get("chain")
        root_job_id = str(job.id)

        # Load previous inventory from a prior completed company job (same chain)
        prev_inventory: dict | None = None
        prev_job = find_previous_company_inventory(session, company, exclude_job_id=job.id, chain=chain)
        if prev_job:
            _raw = get_artifact(session, prev_job.id, "contract_inventory")
            if isinstance(_raw, dict):
                prev_inventory = _raw

        self.update_detail(session, job, f"Discovering contracts for {company}")
        logger.info("Discovery started for job %s: company=%s, chain=%s", job.id, company, chain)
        inventory = search_protocol_inventory(company, chain=chain)

        # Merge with previous inventory if available
        if prev_inventory and isinstance(prev_inventory, dict):
            inventory = merge_inventory(prev_inventory, inventory)

        store_artifact(session, job.id, "contract_inventory", data=inventory)

        # Create or look up Protocol row
        protocol_row = get_or_create_protocol(session, company, official_domain=inventory.get("official_domain"))
        job.protocol_id = protocol_row.id
        session.commit()

        # --- Audit report discovery ---
        self.update_detail(session, job, f"Discovering audit reports for {company}")
        prev_audits: dict | None = None
        if prev_job:
            _raw_audits = get_artifact(session, prev_job.id, "audit_reports")
            if isinstance(_raw_audits, dict):
                prev_audits = _raw_audits

        try:
            audit_result = search_audit_reports(
                company,
                official_domain=inventory.get("official_domain"),
            )
            if prev_audits:
                audit_result = merge_audit_reports(prev_audits, audit_result)
            store_artifact(session, job.id, "audit_reports", data=audit_result)
            _sync_audit_reports_to_db(session, protocol_row.id, audit_result.get("reports", []))
            audit_count = len(audit_result.get("reports", []))
            if audit_count:
                logger.info("Job %s: found %d audit report(s) for %s", job.id, audit_count, company)
        except Exception as exc:
            logger.warning("Job %s: audit report discovery failed: %s", job.id, exc)

        discovered = [e for e in inventory.get("contracts", []) if e.get("address")]

        # Write ALL discovered addresses to contracts table. Ranking and
        # job creation happen later in the selection stage, once DApp
        # crawl and DefiLlama results are also in the table — that way
        # every source competes for the analyze_limit budget on equal
        # footing instead of the first-to-arrive claiming everything.
        # The upsert unions ``discovery_sources`` so a contract that's
        # already in the table from a prior source gains this one as
        # corroboration rather than being dropped.
        for entry in discovered:
            entry_chains = entry.get("chains")
            entry_chain = entry_chains[0] if isinstance(entry_chains, list) and entry_chains else entry.get("chain")
            # Inventory entries carry their own ``source`` list (e.g.
            # ``["tavily_ai_inventory", "deployer_expansion"]``) when
            # multiple inventory signals agreed. Preserve that granularity
            # so ranking sees the richer corroboration story; fall back
            # to the generic ``inventory`` tag when the entry didn't
            # expose one (legacy cached entries).
            entry_sources = entry.get("source") or ["inventory"]
            if not isinstance(entry_sources, list):
                entry_sources = [str(entry_sources)]
            upsert_discovered_contract(
                session,
                address=str(entry["address"]),
                chain=entry_chain,
                protocol_id=protocol_row.id,
                new_sources=entry_sources,
                contract_name=entry.get("name"),
                confidence=entry.get("confidence"),
                chains=entry.get("chains"),
            )
        session.commit()

        store_artifact(
            session,
            job.id,
            "discovery_summary",
            data={
                "mode": "company",
                "company": company,
                "official_domain": inventory.get("official_domain"),
                "discovered_count": len(discovered),
            },
        )

        if not job.name:
            job.name = company
            session.commit()

        # Run unconditionally: DApp crawl + DefiLlama scans are independent
        # sources, and empty primary inventory is the case that most needs them.
        self._spawn_parallel_discovery(session, job, company, request, root_job_id)

        self.update_detail(
            session,
            job,
            f"Discovered {len(discovered)} contracts; awaiting parallel discovery before ranking",
        )

        # Hand off to the selection stage. The SelectionWorker waits for
        # DApp/DefiLlama siblings to settle, then ranks the full set of
        # unanalyzed contracts for this protocol and creates the top-N
        # analysis child jobs under the shared analyze_limit budget.
        advance_job(
            session,
            job.id,
            JobStage.selection,
            f"Discovery complete for {company}: {len(discovered)} contracts; ranking pending",
        )
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
                "company": company,
                "parent_job_id": str(job.id),
                "root_job_id": root_job_id,
                "analyze_limit": request.get("analyze_limit", 5),
                "rpc_url": request.get("rpc_url"),
                "protocol_id": job.protocol_id,
            }
            dl_job = create_job(session, defillama_request, initial_stage=JobStage.defillama_scan)
            logger.info("Job %s: spawned DefiLlama scan job %s (slug=%s)", job.id, dl_job.id, slug)

        # Spawn DApp crawl
        dapp_url = protocol.get("url")
        if dapp_url:
            dapp_request = {
                "dapp_urls": [dapp_url],
                "name": f"{company}_dapp_crawl",
                "company": company,
                "parent_job_id": str(job.id),
                "root_job_id": root_job_id,
                "analyze_limit": request.get("analyze_limit", 5),
                "chain_id": request.get("chain_id") or 1,
                "wait": request.get("wait", 10),
                "rpc_url": request.get("rpc_url"),
                "protocol_id": job.protocol_id,
            }
            crawl_job = create_job(session, dapp_request, initial_stage=JobStage.dapp_crawl)
            logger.info("Job %s: spawned DApp crawl job %s (url=%s)", job.id, crawl_job.id, dapp_url)

    def _process_address(self, session: Session, job: Job) -> None:
        """Fetch verified source for a single address."""
        address = job.address
        if address is None:
            raise ValueError("Address job missing address")

        # Check for cached static data from a previously completed job (same chain)
        request = job.request if isinstance(job.request, dict) else {}
        cached_job = find_completed_static_cache(session, address, chain=request.get("chain"))
        if cached_job is not None:
            self.update_detail(session, job, f"Reusing cached static data for {address}")
            new_contract_id = copy_static_cache(session, cached_job.id, job.id)
            if new_contract_id is not None:
                # Mark the job so downstream workers know static data was cached
                req = job.request if isinstance(job.request, dict) else {}
                job.request = {**req, "static_cached": True, "cache_source_job_id": str(cached_job.id)}
                session.commit()

                # Set job name from the cached contract if not already set
                if not job.name:
                    from sqlalchemy import select as sa_select

                    contract_row = session.execute(
                        sa_select(Contract).where(Contract.job_id == job.id).limit(1)
                    ).scalar_one_or_none()
                    if contract_row and contract_row.contract_name:
                        job.name = f"{contract_row.contract_name}_{address[2:10]}"
                        session.commit()

                logger.info(
                    "Discovery cache hit for %s — reused data from job %s",
                    address,
                    cached_job.id,
                )
                self.update_detail(session, job, f"Discovery complete (cached): {address}")
                return

            logger.warning(
                "Discovery cache copy failed for %s from job %s — falling back to fetch",
                address,
                cached_job.id,
            )

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

        # Write to contracts table — upsert to handle pre-existing discovered rows
        request = job.request if isinstance(job.request, dict) else {}
        existing = session.execute(
            select(Contract).where(
                Contract.address == address.lower(),
                Contract.chain == request.get("chain"),
            )
        ).scalar_one_or_none()

        if existing:
            existing.job_id = job.id
            existing.contract_name = contract_name
            existing.compiler_version = result.get("CompilerVersion", "")
            existing.language = "vyper" if is_vyper_result(result) else "solidity"
            existing.evm_version = evm_version
            existing.optimization = result.get("OptimizationUsed", "1") == "1"
            existing.optimization_runs = int(result.get("Runs", "200") or 200)
            existing.source_format = "standard_json" if "sources" in str(result.get("SourceCode", ""))[:10] else "flat"
            existing.source_file_count = len(sources)
            existing.license = result.get("LicenseType", "")
            existing.deployer = deployer
            existing.remappings = remappings or []
            existing.source_verified = True
            if not existing.protocol_id and job.protocol_id:
                existing.protocol_id = job.protocol_id
        else:
            contract = Contract(
                job_id=job.id,
                address=address.lower(),
                chain=request.get("chain"),
                protocol_id=job.protocol_id,
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
                discovery_sources=request.get("discovery_sources"),
                chains=request.get("chains"),
                source_verified=True,
            )
            session.add(contract)
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
