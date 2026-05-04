"""Company / protocol overview + audit views."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Response
from sqlalchemy import select

from db.models import AuditContractCoverage, AuditReport, Contract, Protocol
from services.aggregations import CompanyNotFound, build_company_overview
from services.audits.serializers import _audit_brief, _audit_report_to_dict

from . import deps

router = APIRouter()


@router.get("/api/company/{company_name}")
def company_overview(company_name: str, response: Response) -> dict:
    """Aggregated governance overview for all contracts in a company."""
    # Largest payload on the site (1-3 MB). Letting the browser hold it for
    # 15s + serve-stale-while-revalidate makes back/forward navigation and
    # tab switches inside the company page instant — both CompanyOverview
    # and ProtocolSurface read this URL on mount.
    response.headers["Cache-Control"] = "private, max-age=15, stale-while-revalidate=60"
    with deps.SessionLocal() as session:
        try:
            return build_company_overview(session, company_name)
        except CompanyNotFound:
            raise HTTPException(status_code=404, detail="Company not found")


@router.get("/api/company/{company_name}/audits")
def company_audits(company_name: str) -> dict[str, Any]:
    """List all known audit reports for a company."""
    with deps.SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
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
        return {
            "company": company_name,
            "protocol_id": protocol_row.id,
            "audit_count": len(audit_rows),
            "audits": [_audit_report_to_dict(ar) for ar in audit_rows],
        }


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
    with deps.SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
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
        return {
            "company": company_name,
            "protocol_id": protocol_row.id,
            "contract_count": len(coverage),
            "audit_count": len(audit_rows),
            "coverage": coverage,
        }
