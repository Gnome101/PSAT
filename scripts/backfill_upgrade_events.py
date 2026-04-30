"""Backfill UpgradeEvent rows from existing upgrade_history artifacts.

The resolution worker (workers/resolution_worker.py:_run_upgrade_history)
projects ``upgrade_history`` artifacts into ``UpgradeEvent`` rows during the
normal pipeline. For some older jobs, that projection never landed —
typically because the proxy's Contract row didn't exist in inventory yet
when resolution ran (see the ``proxy_contract is None: continue`` path).
The artifact stays correct, but the relational rows that drive
``/api/company/{name}.upgrade_count`` and ``last_upgrade_*`` are missing,
so the UI shows null.

This script walks every Job whose ``upgrade_history`` artifact has a
non-empty ``proxies`` dict but whose proxies have zero UpgradeEvent rows,
and re-runs the projection for them. Idempotent — the projection itself
deletes-and-reinserts per proxy, so running this twice is harmless.

Usage:
    uv run python scripts/backfill_upgrade_events.py --dry-run
    uv run python scripts/backfill_upgrade_events.py
    uv run python scripts/backfill_upgrade_events.py --company "ether fi"
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Iterable

from sqlalchemy import func, select

from db.models import Artifact, Contract, Job, SessionLocal, UpgradeEvent
from db.queue import get_artifact
from workers.resolution_worker import project_upgrade_history

logger = logging.getLogger("scripts.backfill_upgrade_events")


def jobs_with_upgrade_history(session) -> Iterable[Job]:
    """Yield Jobs that have an ``upgrade_history`` artifact row.

    We don't pre-load the artifact body here — for jobs whose body lives in
    object storage the body fetch is lazy via get_artifact() per row, which
    keeps memory bounded when the dataset is large.
    """
    job_ids = (
        session.execute(select(Artifact.job_id).where(Artifact.name == "upgrade_history").distinct()).scalars().all()
    )
    if not job_ids:
        return []
    return session.execute(select(Job).where(Job.id.in_(job_ids))).scalars().all()


def proxy_contract_ids_for_artifact(session, *, subject_chain: str | None, uh_data: dict) -> list[int]:
    """Resolve the proxy Contract ids referenced by this artifact.

    Mirrors the lookup in project_upgrade_history so we can ask "do all of
    these already have UpgradeEvent rows?" without re-running the full
    projection.
    """
    proxy_addrs = [
        (proxy_info.get("proxy_address") or "").lower()
        for proxy_info in (uh_data.get("proxies") or {}).values()
        if proxy_info.get("proxy_address")
    ]
    if not proxy_addrs:
        return []
    chain_filter = Contract.chain == subject_chain if subject_chain is not None else Contract.chain.is_(None)
    rows = (
        session.execute(
            select(Contract.id).where(
                func.lower(Contract.address).in_(proxy_addrs),
                chain_filter,
            )
        )
        .scalars()
        .all()
    )
    return list(rows)


def needs_backfill(session, *, contract_ids: list[int]) -> bool:
    """True iff at least one of these contracts has zero UpgradeEvent rows.

    We don't require ALL to be empty — even a single missing proxy is
    enough to warrant re-running the projection (which is idempotent).
    """
    if not contract_ids:
        return False
    counts = dict(
        session.execute(
            select(UpgradeEvent.contract_id, func.count(UpgradeEvent.id))
            .where(UpgradeEvent.contract_id.in_(contract_ids))
            .group_by(UpgradeEvent.contract_id)
        ).all()
    )
    return any(counts.get(cid, 0) == 0 for cid in contract_ids)


def run(*, dry_run: bool, company_filter: str | None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    seen = projected = skipped_no_artifact = skipped_already_full = errored = 0

    with SessionLocal() as session:
        jobs = list(jobs_with_upgrade_history(session))
        if company_filter:
            jobs = [j for j in jobs if (j.company or "") == company_filter]
        logger.info("Considering %d jobs with upgrade_history artifacts", len(jobs))

        for job in jobs:
            seen += 1
            uh_data = get_artifact(session, job.id, "upgrade_history")
            if not isinstance(uh_data, dict) or not uh_data.get("proxies"):
                skipped_no_artifact += 1
                continue

            contract_row = session.execute(
                select(Contract).where(Contract.job_id == job.id).limit(1)
            ).scalar_one_or_none()
            if contract_row is None:
                skipped_no_artifact += 1
                continue

            proxy_cids = proxy_contract_ids_for_artifact(session, subject_chain=contract_row.chain, uh_data=uh_data)
            if not needs_backfill(session, contract_ids=proxy_cids):
                skipped_already_full += 1
                continue

            if dry_run:
                logger.info(
                    "DRY: would project job=%s company=%s address=%s proxies=%d",
                    job.id,
                    job.company,
                    job.address,
                    len(proxy_cids),
                )
                projected += 1
                continue

            try:
                stats = project_upgrade_history(
                    session,
                    subject_contract_id=contract_row.id,
                    subject_chain=contract_row.chain,
                    artifact_data=uh_data,
                )
                session.commit()
                logger.info(
                    "projected job=%s company=%s address=%s (proxies %d/%d, events %d, skipped %d)",
                    job.id,
                    job.company,
                    job.address,
                    stats["proxies_projected"],
                    stats["proxies_seen"],
                    stats["events_written"],
                    stats["proxies_skipped_no_contract"],
                )
                projected += 1
            except Exception as exc:
                session.rollback()
                logger.warning("FAILED job=%s: %s", job.id, exc)
                errored += 1

    logger.info(
        "Done. considered=%d projected=%d skipped_no_artifact=%d skipped_already_full=%d errored=%d",
        seen,
        projected,
        skipped_no_artifact,
        skipped_already_full,
        errored,
    )
    return 0 if errored == 0 else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report what would be projected without writing.")
    parser.add_argument("--company", help="Limit to a single company name (e.g. 'ether fi').")
    args = parser.parse_args(argv)
    return run(dry_run=args.dry_run, company_filter=args.company)


if __name__ == "__main__":
    sys.exit(main())
