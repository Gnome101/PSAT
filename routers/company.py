"""Company / protocol overview + audit views."""

from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Response
from sqlalchemy import select

from db.models import AuditContractCoverage, AuditReport, Contract, Protocol
from services.aggregations import CompanyNotFound, build_company_overview
from services.aggregations.company_overview import (
    all_addresses_for_protocol,
    build_functions_for_protocol,
    resolve_company_jobs,
)
from services.audits.serializers import _audit_brief, _audit_report_to_dict

from . import deps

router = APIRouter()
logger = logging.getLogger("routers.company")


def _log_endpoint(route: str, *, company: str, started: float, **extras: Any) -> None:
    """Emit one structured log line per endpoint hit with elapsed time.

    Pairs with the ``x-psat-trace-id`` middleware so each line is grepable
    by trace_id in Loki. Matches the ``duration_ms`` field already used by
    ``workers/base.py`` for stage timings.
    """
    elapsed_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        "%s elapsed_ms=%d company=%s",
        route,
        elapsed_ms,
        company,
        extra={"phase": "http_endpoint", "route": route, "duration_ms": elapsed_ms, "company": company, **extras},
    )


@router.get("/api/company/{company_name}")
def company_overview(company_name: str, response: Response) -> dict:
    """Aggregated governance overview for all contracts in a company."""
    # Largest payload on the site (1-3 MB). Letting the browser hold it for
    # 15s + serve-stale-while-revalidate makes back/forward navigation and
    # tab switches inside the company page instant — both CompanyOverview
    # and ProtocolSurface read this URL on mount.
    response.headers["Cache-Control"] = "private, max-age=15, stale-while-revalidate=60"
    started = time.monotonic()
    with deps.SessionLocal() as session:
        try:
            payload = build_company_overview(session, company_name)
        except CompanyNotFound:
            _log_endpoint("/api/company/{name}", company=company_name, started=started, outcome="not_found")
            raise HTTPException(status_code=404, detail="Company not found")
    _log_endpoint(
        "/api/company/{name}",
        company=company_name,
        started=started,
        outcome="success",
        contract_count=len(payload.get("contracts") or []),
    )
    return payload


@router.get("/api/company/{company_name}/addresses")
def company_addresses(company_name: str, response: Response) -> dict[str, Any]:
    """Full inventory of contract addresses for a protocol.

    Split out from the main ``/api/company/{name}`` payload so the
    167 KB list isn't shipped on every page-load — ``AddressesModal``
    fetches this lazily when the user opens it.
    """
    response.headers["Cache-Control"] = "private, max-age=15, stale-while-revalidate=60"
    started = time.monotonic()
    with deps.SessionLocal() as session:
        protocol_row, jobs = resolve_company_jobs(session, company_name)
        if protocol_row is None and not jobs:
            _log_endpoint("/api/company/{name}/addresses", company=company_name, started=started, outcome="not_found")
            raise HTTPException(status_code=404, detail="Company not found")
        addresses = all_addresses_for_protocol(session, protocol_row, jobs)
    _log_endpoint(
        "/api/company/{name}/addresses",
        company=company_name,
        started=started,
        outcome="success",
        address_count=len(addresses),
    )
    return {"all_addresses": addresses}


@router.get("/api/company/{company_name}/functions")
def company_functions(company_name: str, response: Response) -> dict[str, Any]:
    """Per-contract function entries for a protocol, keyed by address.

    Split out of the main ``/api/company/{name}`` payload — the
    ``EffectiveFunction`` table accounts for ~2.13 MB of payload and
    120-290ms of TTFB on ether.fi, neither of which the Surface canvas
    needs to render. ``ProtocolSurface`` fetches this in parallel with
    the main payload and populates the function inspector when it
    arrives.
    """
    response.headers["Cache-Control"] = "private, max-age=15, stale-while-revalidate=60"
    started = time.monotonic()
    with deps.SessionLocal() as session:
        try:
            functions_by_address = build_functions_for_protocol(session, company_name)
        except CompanyNotFound:
            _log_endpoint("/api/company/{name}/functions", company=company_name, started=started, outcome="not_found")
            raise HTTPException(status_code=404, detail="Company not found")
    _log_endpoint(
        "/api/company/{name}/functions",
        company=company_name,
        started=started,
        outcome="success",
        contract_count=len(functions_by_address),
        function_count=sum(len(v) for v in functions_by_address.values()),
    )
    return {"functions": functions_by_address}


@router.get("/api/company/{company_name}/audits")
def company_audits(company_name: str) -> dict[str, Any]:
    """List all known audit reports for a company."""
    started = time.monotonic()
    with deps.SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
            _log_endpoint("/api/company/{name}/audits", company=company_name, started=started, outcome="not_found")
            raise HTTPException(status_code=404, detail="Company not found")

        audit_rows = (
            session.execute(
                select(AuditReport)
                .where(AuditReport.protocol_id == protocol_row.id)
                .order_by(AuditReport.date.desc().nullslast())
            )
            .scalars()
            .all()
        )
        result = {
            "company": company_name,
            "protocol_id": protocol_row.id,
            "audit_count": len(audit_rows),
            "audits": [_audit_report_to_dict(ar) for ar in audit_rows],
        }
    _log_endpoint(
        "/api/company/{name}/audits",
        company=company_name,
        started=started,
        outcome="success",
        audit_count=len(audit_rows),
    )
    return result


@router.get("/api/company/{company_name}/audit_coverage")
def company_audit_coverage(company_name: str) -> dict[str, Any]:
    """For each contract in the company's inventory, list audits covering it.

    Reads from the persisted ``audit_contract_coverage`` join table — rows
    are written proxy-aware by ``services.audits.coverage`` when scope
    extraction completes or a live upgrade event is detected. The
    ``last_audit`` pointer is the most recent matching audit by ``date``
    (nulls last, then id desc to break ties). Each audit entry carries
    ``match_type`` + ``match_confidence`` so the UI can flag low-confidence
    links differently.
    """
    started = time.monotonic()
    with deps.SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
            _log_endpoint(
                "/api/company/{name}/audit_coverage",
                company=company_name,
                started=started,
                outcome="not_found",
            )
            raise HTTPException(status_code=404, detail="Company not found")

        contracts = session.execute(select(Contract).where(Contract.protocol_id == protocol_row.id)).scalars().all()

        audit_rows = (
            session.execute(
                select(AuditReport)
                .where(
                    AuditReport.protocol_id == protocol_row.id,
                    AuditReport.scope_extraction_status == "success",
                )
                .order_by(AuditReport.date.desc().nullslast(), AuditReport.id.desc())
            )
            .scalars()
            .all()
        )
        audits_by_id = {a.id: a for a in audit_rows}

        # Pull every coverage row for the protocol in one query, then
        # bucket in Python — cheaper than N queries for N contracts.
        coverage_rows = (
            session.execute(
                select(AuditContractCoverage).where(
                    AuditContractCoverage.protocol_id == protocol_row.id,
                )
            )
            .scalars()
            .all()
        )
        coverage_by_contract: dict[int, list[Any]] = {}
        for row in coverage_rows:
            coverage_by_contract.setdefault(row.contract_id, []).append(row)

        def _sort_key(row: Any) -> tuple:
            audit = audits_by_id.get(row.audit_report_id)
            date = (audit.date if audit else None) or ""
            return (date, row.audit_report_id)

        # Proxy rows don't hold their own coverage rows — the scope
        # matcher writes against the impl Contract row. For the
        # company-level "is this contract audited?" view the user really
        # means "is the code this address is running audited?", so union
        # the proxy's entries with its current implementation's.
        contracts_by_addr = {c.address.lower(): c for c in contracts if c.address}

        coverage: list[dict[str, Any]] = []
        for c in contracts:
            entries = list(coverage_by_contract.get(c.id, []))
            seen_audit_ids = {e.audit_report_id for e in entries}
            if c.is_proxy and c.implementation:
                impl = contracts_by_addr.get(c.implementation.lower())
                if impl:
                    for e in coverage_by_contract.get(impl.id, []):
                        if e.audit_report_id not in seen_audit_ids:
                            entries.append(e)
                            seen_audit_ids.add(e.audit_report_id)
            entries = sorted(entries, key=_sort_key, reverse=True)
            matching = [
                _audit_brief(audits_by_id[e.audit_report_id], e) for e in entries if e.audit_report_id in audits_by_id
            ]
            # Inventory-only entries (discovered but never analyzed) have no
            # name and no audits — they contribute nothing to the coverage
            # view and otherwise inflate the payload (~67% of rows for a
            # mature protocol). Drop them at the serializer rather than at
            # the query, so analyzed contracts without audits still surface.
            if not c.contract_name and not matching:
                continue
            coverage.append(
                {
                    "address": c.address,
                    "chain": c.chain,
                    "contract_name": c.contract_name,
                    "audit_count": len(matching),
                    "last_audit": matching[0] if matching else None,
                    "audits": matching,
                }
            )
        result = {
            "company": company_name,
            "protocol_id": protocol_row.id,
            "contract_count": len(coverage),
            "audit_count": len(audit_rows),
            "coverage": coverage,
        }
    _log_endpoint(
        "/api/company/{name}/audit_coverage",
        company=company_name,
        started=started,
        outcome="success",
        contract_count=len(coverage),
        audit_count=len(audit_rows),
        coverage_row_count=len(coverage_rows),
    )
    return result
