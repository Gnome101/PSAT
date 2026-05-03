"""Resolution worker — builds control snapshot and resolves control graph."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from db.models import (
    Contract,
    ContractBalance,
    ControlGraphEdge,
    ControlGraphNode,
    ControllerValue,
    Job,
    JobStage,
    UpgradeEvent,
)
from db.nested_artifacts import store_bundle as store_nested_artifacts
from db.queue import create_job, get_artifact, get_contract_for_job, store_artifact
from schemas.control_tracking import ControlSnapshot, ControlTrackingPlan
from services.resolution.recursive import LoadedArtifacts, resolve_control_graph
from services.resolution.tracking import build_control_snapshot
from utils.logging import record_degraded
from workers.base import BaseWorker

logger = logging.getLogger("workers.resolution_worker")

DEFAULT_RPC_URL = os.getenv("ETH_RPC", "https://ethereum-rpc.publicnode.com")
RECURSION_MAX_DEPTH = int(os.getenv("PSAT_RECURSION_MAX_DEPTH", "6"))


def _build_root_artifacts(
    contract_analysis: dict,
    tracking_plan: dict,
    snapshot: ControlSnapshot,
) -> LoadedArtifacts:
    """Package the root job's in-memory artifacts for the recursive resolver."""
    return {
        "analysis": contract_analysis,
        "tracking_plan": tracking_plan,
        "snapshot": snapshot,
    }


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
        t0 = time.monotonic()
        snapshot = build_control_snapshot(cast(ControlTrackingPlan, tracking_plan), rpc_url)
        logger.info(
            "resolution phase complete: control snapshot",
            extra={"duration_ms": int((time.monotonic() - t0) * 1000), "phase": "control_snapshot"},
        )
        # Keep as artifact — policy stage reads it as JSON
        store_artifact(session, job.id, "control_snapshot", data=snapshot)

        # Write to controller_values table
        contract_row = get_contract_for_job(session, job)
        if contract_row:
            session.query(ControllerValue).filter(ControllerValue.contract_id == contract_row.id).delete()
            for cid, cv in snapshot.get("controller_values", {}).items():
                session.add(
                    ControllerValue(
                        contract_id=contract_row.id,
                        controller_id=cid,
                        value=cv.get("value"),
                        resolved_type=cv.get("resolved_type"),
                        source=cv.get("source"),
                        block_number=snapshot.get("block_number"),
                        details=cv.get("details"),
                        observed_via=cv.get("observed_via"),
                    )
                )
            session.commit()

        logger.info(
            "Resolution stage control snapshot complete for job %s address=%s name=%s",
            job.id,
            job.address or "0x0",
            job.name or "Contract",
        )

        # Fetch token balances
        self._fetch_balances(session, job, contract_row)

        root_artifacts = _build_root_artifacts(contract_analysis, tracking_plan, snapshot)

        self.update_detail(session, job, "Resolving recursive control graph")
        t0 = time.monotonic()
        # Cache classify_resolved_address results so the policy stage can
        # short-circuit its refresh + labeling passes (the dominant cost
        # on cascade workloads — see PSAT_BENCH_NOTES in
        # services/resolution/recursive.py).
        classify_cache: dict[str, tuple[str, dict[str, object]]] = {}
        resolved_graph, nested_artifacts = resolve_control_graph(
            root_artifacts=root_artifacts,
            rpc_url=rpc_url,
            max_depth=RECURSION_MAX_DEPTH,
            workspace_prefix="recursive",
            classify_cache=classify_cache,
            heartbeat=lambda: self._heartbeat(session, job),
        )

        logger.info(
            "resolution phase complete: recursive graph",
            extra={"duration_ms": int((time.monotonic() - t0) * 1000), "phase": "recursive_graph"},
        )

        if resolved_graph:
            # Persist each nested contract's artifacts so the policy stage can
            # read them back by address (no local filesystem).
            store_nested_artifacts(session, job.id, nested_artifacts)
            # Keep as artifact — policy stage reads it as JSON
            store_artifact(session, job.id, "resolved_control_graph", data=resolved_graph)
            # Persist the classify cache so the policy stage skips re-running
            # the 6-10 RPC fan-out per address. dict[str, tuple] → JSON-friendly
            # dict[str, list] for storage.
            if classify_cache:
                store_artifact(
                    session,
                    job.id,
                    "classified_addresses",
                    data={addr: list(v) for addr, v in classify_cache.items()},
                )
            logger.info(
                "Resolution stage graph complete for job %s address=%s name=%s",
                job.id,
                job.address or "0x0",
                job.name or "Contract",
            )

            # Write to control_graph_nodes and control_graph_edges tables
            if contract_row:
                session.query(ControlGraphNode).filter(ControlGraphNode.contract_id == contract_row.id).delete()
                session.query(ControlGraphEdge).filter(ControlGraphEdge.contract_id == contract_row.id).delete()
                for node in resolved_graph.get("nodes", []):
                    session.add(
                        ControlGraphNode(
                            contract_id=contract_row.id,
                            address=(node.get("address") or "").lower(),
                            node_type=node.get("node_type"),
                            resolved_type=node.get("resolved_type"),
                            label=node.get("label"),
                            contract_name=node.get("contract_name"),
                            depth=node.get("depth"),
                            analyzed=node.get("analyzed", False),
                            details=node.get("details"),
                        )
                    )
                for edge in resolved_graph.get("edges", []):
                    session.add(
                        ControlGraphEdge(
                            contract_id=contract_row.id,
                            from_node_id=edge.get("from_id", ""),
                            to_node_id=edge.get("to_id", ""),
                            relation=edge.get("relation"),
                            label=edge.get("label"),
                            source_controller_id=edge.get("source_controller_id"),
                            notes=edge.get("notes"),
                        )
                    )
                session.commit()

            # Queue analysis jobs for contracts discovered during resolution
            self._queue_discovered_contracts(session, job, cast(dict, resolved_graph), rpc_url)

        # Phase: Upgrade history for proxy contracts (non-fatal)
        self._run_upgrade_history(session, job)

        self.update_detail(session, job, "Resolution complete")
        logger.info(
            "Resolution stage complete for job %s address=%s name=%s",
            job.id,
            job.address or "0x0",
            job.name or "Contract",
        )

    def _fetch_balances(self, session: Session, job: Job, contract_row: Contract | None) -> None:
        """Fetch ETH + token balances and store in contract_balances table."""
        from utils.etherscan import get_eth_balance, get_eth_price, get_token_balances, parallel_get

        address = job.address
        if not address or not contract_row:
            return

        request = job.request if isinstance(job.request, dict) else {}
        target_address = request.get("proxy_address") or address

        self.update_detail(session, job, "Fetching token balances")
        # Fan out the three Etherscan calls (eth balance, token balances, eth
        # price). All three serialise on the global rate lock, so threading
        # only stacks RTTs — the limiter is preserved.
        results = parallel_get(
            {
                "eth_wei": (lambda: get_eth_balance(target_address)),
                "tokens": (lambda: get_token_balances(target_address)),
                "eth_price": get_eth_price,
            }
        )

        eth_wei_raw = results.get("eth_wei")
        tokens_raw = results.get("tokens")
        if isinstance(eth_wei_raw, BaseException) or isinstance(tokens_raw, BaseException):
            primary_exc = eth_wei_raw if isinstance(eth_wei_raw, BaseException) else tokens_raw
            assert isinstance(primary_exc, BaseException)
            record_degraded(
                phase="balance_fetch",
                exc=primary_exc,
                context={
                    "address": target_address,
                    "eth_failed": isinstance(eth_wei_raw, BaseException),
                    "tokens_failed": isinstance(tokens_raw, BaseException),
                },
            )
            logger.warning(
                "Job %s: balance fetch failed: eth=%r tokens=%r",
                job.id,
                eth_wei_raw,
                tokens_raw,
            )
            return
        eth_wei = cast(int, eth_wei_raw)
        tokens = cast(list, tokens_raw)

        # Clear old balances
        session.query(ContractBalance).filter(ContractBalance.contract_id == contract_row.id).delete()

        # Native ETH balance
        if eth_wei > 0:
            eth_price_raw = results.get("eth_price")
            eth_price: float | None
            eth_usd: float | None = None
            if isinstance(eth_price_raw, BaseException):
                record_degraded(
                    phase="eth_price_fetch",
                    exc=eth_price_raw,
                    context={"address": target_address},
                )
                logger.warning("Job %s: ETH price fetch failed: %s", job.id, eth_price_raw)
                eth_price = None
            else:
                eth_price = cast(float, eth_price_raw)
                eth_usd = (eth_wei / 1e18) * eth_price
            session.add(
                ContractBalance(
                    contract_id=contract_row.id,
                    token_address=None,
                    token_name="Ether",
                    token_symbol="ETH",
                    decimals=18,
                    raw_balance=str(eth_wei),
                    price_usd=eth_price,
                    usd_value=round(eth_usd, 2) if eth_usd else None,
                )
            )

        # ERC-20 token balances
        for tok in tokens:
            session.add(
                ContractBalance(
                    contract_id=contract_row.id,
                    token_address=tok["token_address"],
                    token_name=tok["token_name"],
                    token_symbol=tok["token_symbol"],
                    decimals=tok["decimals"],
                    raw_balance=str(tok["balance"]),
                    price_usd=tok.get("price_usd"),
                    usd_value=tok.get("usd_value"),
                )
            )

        session.commit()
        total = len(tokens) + (1 if eth_wei > 0 else 0)
        logger.info("Job %s: stored %d balance(s) for %s", job.id, total, target_address)

    def _queue_discovered_contracts(self, session: Session, job: Job, resolved_graph: dict, rpc_url: str) -> None:
        """Queue analysis jobs for contracts found during resolution that have no existing job."""
        request = job.request if isinstance(job.request, dict) else {}
        parent_company = job.company

        # Walk up parent chain to find company if not set on this job
        if not parent_company:
            seen: set[str] = set()
            current_req = request
            while not parent_company:
                parent_id = current_req.get("parent_job_id")
                if not isinstance(parent_id, str) or parent_id in seen:
                    break
                seen.add(parent_id)
                parent_job = session.get(Job, parent_id)
                if parent_job is None:
                    break
                if parent_job.company:
                    parent_company = parent_job.company
                    break
                current_req = parent_job.request if isinstance(parent_job.request, dict) else {}

        nodes = resolved_graph.get("nodes", [])
        root_address = resolved_graph.get("root_contract_address", "").lower()
        queued_count = 0

        for node in nodes:
            addr = (node.get("address") or "").lower()
            if not addr or not addr.startswith("0x") or len(addr) != 42:
                continue
            if addr == root_address:
                continue
            # Only queue contracts that were analyzed during resolution
            if not node.get("analyzed"):
                continue
            if node.get("node_type") != "contract":
                continue

            # Skip if a job already exists for this address
            existing = session.execute(select(Job).where(Job.address == addr).limit(1)).scalar_one_or_none()
            if existing:
                continue

            contract_name = node.get("contract_name") or node.get("label") or addr
            child_request = {
                "address": addr,
                "name": contract_name,
                "rpc_url": rpc_url,
                "parent_job_id": str(job.id),
                "discovered_by": "resolution",
            }
            if request.get("chain"):
                child_request["chain"] = request["chain"]

            child_job = create_job(session, child_request, initial_stage=JobStage.discovery)
            if parent_company:
                child_job.company = parent_company
            if job.protocol_id:
                child_job.protocol_id = job.protocol_id
            session.commit()

            queued_count += 1
            logger.info(
                "Job %s: queued discovered contract %s (%s) as job %s",
                job.id,
                contract_name,
                addr,
                child_job.id,
            )

        if queued_count:
            logger.info(
                "Job %s: queued %d contracts discovered during resolution",
                job.id,
                queued_count,
            )

    def _run_upgrade_history(self, session: Session, job: Job) -> None:
        """Project the cached ``upgrade_history`` artifact into relational rows.

        Two outputs from one artifact read:
          1. ``UpgradeEvent`` rows — deleted + re-inserted each run so the
             stored history always reflects the artifact exactly.
          2. ``Contract`` rows for every unique historical impl address —
             backfilled via ``_backfill_historical_impls`` so the audit
             coverage matcher can link audits whose scope names a past
             impl. Tagged ``discovery_source='upgrade_history'`` so the
             inventory endpoints and ``analyze-remaining`` can filter
             them out of "current contracts" views.
        """
        uh_data = get_artifact(session, job.id, "upgrade_history")
        if not isinstance(uh_data, dict) or not uh_data.get("proxies"):
            logger.info("Job %s: skipping upgrade history — no upgrade_history artifact", job.id)
            return

        self.update_detail(session, job, "Writing upgrade events")
        try:
            contract_row = get_contract_for_job(session, job)
            if contract_row is None:
                return

            # Legacy cleanup: older versions of this worker keyed every
            # event to the subject's id regardless of which proxy the
            # event described. Drop those so re-run is idempotent for
            # non-proxy subjects.
            session.query(UpgradeEvent).filter(UpgradeEvent.contract_id == contract_row.id).delete()

            impl_addrs: set[str] = set()
            for proxy_info in uh_data["proxies"].values():
                proxy_addr = proxy_info.get("proxy_address", "")
                if not proxy_addr:
                    continue
                # UpgradeEvent.contract_id must point at the PROXY's row,
                # not the subject's — the artifact can describe any proxy
                # in the dependency graph, not just the subject's own.
                chain_filter = (
                    Contract.chain == contract_row.chain if contract_row.chain is not None else Contract.chain.is_(None)
                )
                proxy_contract = session.execute(
                    select(Contract).where(
                        func.lower(Contract.address) == proxy_addr.lower(),
                        chain_filter,
                    )
                ).scalar_one_or_none()
                if proxy_contract is None:
                    # Proxy not yet in inventory — skip. It'll get picked
                    # up on a later run once discovery surfaces the address.
                    continue
                session.query(UpgradeEvent).filter(UpgradeEvent.contract_id == proxy_contract.id).delete()
                for evt in proxy_info.get("events", []):
                    if evt.get("event_type") != "upgraded":
                        continue
                    impl = evt.get("implementation")
                    # Artifact carries ``timestamp`` as unix seconds (int | None);
                    # the DB column is DateTime(timezone=True). Dropping this
                    # was the root cause of ImplWindow.from_ts=None downstream,
                    # which collapsed every post-upgrade audit to low confidence.
                    ts_raw = evt.get("timestamp")
                    ts_val = datetime.fromtimestamp(ts_raw, tz=timezone.utc) if ts_raw is not None else None
                    session.add(
                        UpgradeEvent(
                            contract_id=proxy_contract.id,
                            proxy_address=proxy_addr,
                            old_impl=None,
                            new_impl=impl,
                            block_number=evt.get("block_number"),
                            timestamp=ts_val,
                            tx_hash=evt.get("tx_hash"),
                        )
                    )
                    if impl:
                        impl_addrs.add(impl.lower())
            session.commit()

            if contract_row.protocol_id is not None and impl_addrs:
                self._backfill_historical_impls(
                    session,
                    protocol_id=contract_row.protocol_id,
                    chain=contract_row.chain,
                    impl_addrs=impl_addrs,
                )

            logger.info(
                "Resolution stage upgrade history complete for job %s address=%s",
                job.id,
                job.address or "0x0",
            )
        except Exception as exc:
            record_degraded(
                phase="resolution_upgrade_history",
                exc=exc,
                context={"address": job.address or "0x0"},
            )
            logger.warning(
                "Resolution stage upgrade history failed for job %s address=%s: %s",
                job.id,
                job.address or "0x0",
                exc,
            )

    def _backfill_historical_impls(
        self,
        session: Session,
        *,
        protocol_id: int,
        chain: str | None,
        impl_addrs: set[str],
    ) -> None:
        """Ensure a Contract row exists for each historical impl address.

        For each address, three cases:
          1. No Contract row exists → create one tagged
             ``discovery_source='upgrade_history'`` with Etherscan-resolved
             name. This is the normal path for newly-surfaced impls.
          2. Row exists with ``protocol_id`` NULL or equal to ours → adopt.
             Sets the upgrade_history tag if empty, sets protocol_id. Keeps
             existing name/analysis fields intact.
          3. Row exists in a DIFFERENT protocol → leave alone, log a warning.
             Rare (implementation bytecode is usually protocol-specific) but
             possible; silently stomping another protocol's inventory would
             be worse than an unresolved coverage link.

        Etherscan name resolution uses the shared ``get_contract_info``
        cache, so re-analyzing a protocol re-hits only new impls. On name
        fetch failure the row is still created with ``'UnknownImpl'`` —
        coverage matching against it will fail (no scope says "UnknownImpl")
        but the row remains adoptable on a later run when Etherscan is
        reachable. Swallows per-address exceptions so one flaky lookup
        doesn't wreck the whole backfill.
        """
        from utils.etherscan import get_contract_info, parallel_get

        # Match the natural (address, chain) uniqueness grain. Cross-chain
        # protocols (rare but real — CREATE2 / deterministic deployments can
        # put the same impl address on Ethereum and Polygon) would otherwise
        # look like cross-protocol collisions and get skipped incorrectly.
        chain_filter = Contract.chain == chain if chain is not None else Contract.chain.is_(None)
        existing_rows = {
            row.address.lower(): row
            for row in session.execute(select(Contract).where(Contract.address.in_(impl_addrs), chain_filter))
            .scalars()
            .all()
        }

        # Fan out the Etherscan name lookups for the addresses that need a
        # fresh Contract row. Existing rows (handled in the loop below) don't
        # need a name fetch — adoption keeps whatever is already stored.
        new_addrs = sorted(addr for addr in impl_addrs if addr not in existing_rows)
        name_results: dict[str, str | None] = {}
        if new_addrs:
            calls = {addr: (lambda a=addr: get_contract_info(a)) for addr in new_addrs}
            fetched = parallel_get(calls)
            for addr in new_addrs:
                value = fetched.get(addr)
                if isinstance(value, tuple) and len(value) == 2:
                    name_results[addr] = cast(str | None, value[0])
                else:
                    if isinstance(value, BaseException):
                        logger.warning(
                            "Etherscan name fetch failed for historical impl %s: %s",
                            addr,
                            value,
                        )
                    name_results[addr] = None

        created = 0
        adopted = 0
        # New/adopted rows that need a coverage refresh — scope extraction
        # ran before these existed, so the audit ↔ impl join is empty
        # until we re-run the matcher per contract.
        refresh_ids: list[int] = []
        for addr in impl_addrs:
            existing = existing_rows.get(addr)
            if existing is not None:
                if existing.protocol_id is None or existing.protocol_id == protocol_id:
                    was_orphan = existing.protocol_id is None
                    had_no_tag = "upgrade_history" not in (existing.discovery_sources or [])
                    if was_orphan:
                        existing.protocol_id = protocol_id
                    if had_no_tag:
                        existing.discovery_sources = list(existing.discovery_sources or []) + ["upgrade_history"]
                    adopted += 1
                    # No-op adoption (same protocol, tag already set) wouldn't
                    # change matcher output — skip the refresh.
                    if was_orphan or had_no_tag:
                        refresh_ids.append(existing.id)
                else:
                    logger.warning(
                        "Job protocol %s: historical impl %s already owned by protocol %s — "
                        "coverage link will not be created against this impl",
                        protocol_id,
                        addr,
                        existing.protocol_id,
                    )
                continue

            name = name_results.get(addr)

            new_row = Contract(
                protocol_id=protocol_id,
                address=addr,
                chain=chain,
                contract_name=name or "UnknownImpl",
                is_proxy=False,
                job_id=None,
                discovery_sources=["upgrade_history"],
                source_verified=bool(name),
            )
            session.add(new_row)
            created += 1
            session.flush()  # materialize new_row.id for the refresh below
            refresh_ids.append(new_row.id)

        if created or adopted:
            session.commit()
            logger.info(
                "Protocol %s: backfilled %d historical impl Contract row(s) (%d created, %d adopted)",
                protocol_id,
                created + adopted,
                created,
                adopted,
            )

        if refresh_ids:
            # Lazy import keeps the worker boot path free of audits-service deps.
            from services.audits.coverage import upsert_coverage_for_contract

            refreshed = 0
            for contract_id in refresh_ids:
                try:
                    # Source-equivalence ON: historical impls have no Job,
                    # so the coverage_worker path never runs for them. This
                    # inline call is the only chance to promote matches to
                    # reviewed_commit/high when an audit's reviewed_commits
                    # byte-equal the deployed impl's Etherscan source.
                    refreshed += upsert_coverage_for_contract(
                        session,
                        contract_id,
                        verify_source_equivalence=True,
                    )
                except Exception as exc:
                    # One flaky match shouldn't poison the rest; admin
                    # refresh_coverage can fill in what we missed.
                    logger.warning(
                        "Coverage refresh failed for backfilled impl contract_id=%s: %s",
                        contract_id,
                        exc,
                        extra={"exc_type": type(exc).__name__},
                    )
            session.commit()
            if refreshed:
                logger.info(
                    "Protocol %s: linked %d audit coverage row(s) to %d backfilled impl(s)",
                    protocol_id,
                    refreshed,
                    len(refresh_ids),
                )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        force=True,
    )
    ResolutionWorker().run_loop()


if __name__ == "__main__":
    main()
