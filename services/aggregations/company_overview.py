"""Company-level governance overview.

Decomposed from a single ~700-line endpoint into stages so each step is
testable on its own. ``build_company_overview`` is the orchestrator
called by the router.

Stages (each returns plain Python data, not ORM rows that pin a session):

1. ``resolve_company_jobs`` — protocol lookup with legacy-company fallback
   that walks ``parent_job_id`` chains for older jobs that don't carry a
   protocol_id.
2. ``prefetch_contracts`` — batch fetch ``Contract`` rows by ``job_id``,
   with an address+chain fallback for jobs whose Contract row was
   reassigned by ``copy_static_cache`` to a newer job.
3. ``resolve_implementation_contracts`` — for proxy contracts in the
   inventory, locate the impl Contract row keyed by impl address.
4. ``build_governance_view`` — merges the above with prefetched child
   tables to produce the contract entries, ownership hierarchy,
   non-contract principals, and inter-contract fund-flow edges.
5. ``assemble_company_payload`` — adds the protocol-wide views
   (all_addresses, latest TVL, audit reports) and shapes the final dict.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from db.models import (
    AuditReport,
    Contract,
    ContractBalance,
    ControlGraphEdge,
    ControlGraphNode,
    ControllerValue,
    EffectiveFunction,
    Job,
    JobStatus,
    Protocol,
    TvlSnapshot,
    UpgradeEvent,
)
from services.governance.principals import _build_company_function_entry


class CompanyNotFound(Exception):
    """Raised when no jobs / protocol match the given company name."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.name = name


@dataclass
class GovernanceView:
    contracts: list[dict[str, Any]] = field(default_factory=list)
    principals: list[dict[str, Any]] = field(default_factory=list)
    hierarchy: list[dict[str, Any]] = field(default_factory=list)
    fund_flows: list[dict[str, Any]] = field(default_factory=list)


def resolve_company_jobs(session: Session, name: str) -> tuple[Protocol | None, list[Job]]:
    """Find the protocol row + jobs that belong to ``name``.

    Modern data: ``Protocol`` row exists, every job carries ``protocol_id``.
    Legacy fallback: no Protocol row but a Job has ``company == name``;
    we walk ``request.parent_job_id`` chains across all completed jobs to
    backfill the company graph.
    """
    protocol_row = session.execute(select(Protocol).where(Protocol.name == name)).scalar_one_or_none()

    if protocol_row:
        company_jobs = (
            session.execute(
                select(Job).where(
                    Job.protocol_id == protocol_row.id,
                    Job.status == JobStatus.completed,
                    Job.address.isnot(None),
                )
            )
            .scalars()
            .all()
        )
        return protocol_row, list(company_jobs)

    company_job = session.execute(
        select(Job).where(Job.company == name).order_by(Job.updated_at.desc()).limit(1)
    ).scalar_one_or_none()
    if company_job is None:
        return None, []

    company_job_id = str(company_job.id)
    all_completed = session.execute(select(Job).where(Job.status == JobStatus.completed)).scalars().all()
    jobs_by_id = {str(j.id): j for j in all_completed}
    jobs_by_id[company_job_id] = company_job

    def belongs_to_company(job: Job) -> bool:
        seen: set[str] = set()
        current: Job | None = job
        while current is not None:
            if current.company == name:
                return True
            request = current.request if isinstance(current.request, dict) else {}
            parent_id = request.get("parent_job_id")
            if not isinstance(parent_id, str) or parent_id in seen:
                return False
            seen.add(parent_id)
            current = jobs_by_id.get(parent_id)
        return False

    return None, [j for j in all_completed if j.address and belongs_to_company(j)]


def prefetch_contracts(session: Session, jobs: list[Job]) -> dict[Any, Contract]:
    """Return ``{job_id: Contract}``, with address/chain fallback.

    Jobs whose Contract row was reassigned to a newer job by
    ``copy_static_cache`` are matched by ``(address, chain)``.
    """
    company_job_ids = [j.id for j in jobs]
    contracts_by_job_id: dict[Any, Contract] = {}
    if company_job_ids:
        for c in session.execute(
            select(Contract).where(Contract.job_id.in_(company_job_ids)).options(selectinload(Contract.summary))
        ).scalars():
            contracts_by_job_id[c.job_id] = c

    unresolved_addrs_by_chain: dict[str | None, set[str]] = {}
    for j in jobs:
        if contracts_by_job_id.get(j.id) is not None or not j.address:
            continue
        req = j.request if isinstance(j.request, dict) else {}
        unresolved_addrs_by_chain.setdefault(req.get("chain"), set()).add(j.address.lower())
    contracts_by_addr_chain: dict[tuple[str, str | None], Contract] = {}
    all_unresolved_addrs = {a for addrs in unresolved_addrs_by_chain.values() for a in addrs}
    if all_unresolved_addrs:
        for c in session.execute(
            select(Contract)
            .where(Contract.address.in_(list(all_unresolved_addrs)))
            .options(selectinload(Contract.summary))
        ).scalars():
            addr_lc = (c.address or "").lower()
            for chain_key, addrs in unresolved_addrs_by_chain.items():
                if addr_lc in addrs and (chain_key is None or c.chain == chain_key):
                    contracts_by_addr_chain[(addr_lc, chain_key)] = c

    # Combine — fallback contracts get keyed by job_id too so the rest of
    # the pipeline can pretend it always had a job_id match.
    out = dict(contracts_by_job_id)
    for j in jobs:
        if out.get(j.id) is not None or not j.address:
            continue
        req = j.request if isinstance(j.request, dict) else {}
        fallback = contracts_by_addr_chain.get((j.address.lower(), req.get("chain")))
        if fallback is not None:
            # Don't overwrite the source-of-truth dict when another job's row
            # legitimately points at this Contract; key by the job_id we want
            # the resolver to find under.
            out[j.id] = fallback
    return out


def resolve_implementation_contracts(
    session: Session, jobs: list[Job], contracts_by_job_id: dict[Any, Contract]
) -> tuple[dict[str, Job], dict[Any, Contract]]:
    """Return ``(impl_job_by_addr, contracts_by_job_id)`` with impls resolved.

    Mutates the contracts_by_job_id dict to also include impl-contract rows
    keyed by their own job_id, so downstream code can look up impl
    contracts directly.
    """
    impl_addrs_needed: set[str] = set()
    for j in jobs:
        cr = contracts_by_job_id.get(j.id)
        if cr and cr.is_proxy and cr.implementation:
            impl_addrs_needed.add(cr.implementation.lower())

    impl_job_by_addr: dict[str, Job] = {}
    if impl_addrs_needed:
        for ij in session.execute(
            select(Job).where(
                Job.address.in_(list(impl_addrs_needed)),
                Job.status == JobStatus.completed,
            )
        ).scalars():
            key = (ij.address or "").lower()
            if key and key not in impl_job_by_addr:
                impl_job_by_addr[key] = ij

    impl_job_ids_needed = [ij.id for ij in impl_job_by_addr.values()]
    if impl_job_ids_needed:
        for c in session.execute(
            select(Contract).where(Contract.job_id.in_(impl_job_ids_needed)).options(selectinload(Contract.summary))
        ).scalars():
            contracts_by_job_id[c.job_id] = c

    return impl_job_by_addr, contracts_by_job_id


def _prefetch_child_tables(session: Session, contract_ids: set[int]) -> dict[str, dict[int, Any]]:
    """Pre-load every per-contract child row used downstream."""
    out: dict[str, dict[int, Any]] = {
        "controller_values": {},
        "ef_rows": {},
        "upgrade_events_count": {},
        "upgrade_events_last": {},
        "balances": {},
        "cgn": {},
        "cge": {},
    }
    if not contract_ids:
        return out

    id_list = list(contract_ids)
    for cv in session.execute(select(ControllerValue).where(ControllerValue.contract_id.in_(id_list))).scalars():
        out["controller_values"].setdefault(cv.contract_id, []).append(cv)
    for ef in session.execute(
        select(EffectiveFunction)
        .where(EffectiveFunction.contract_id.in_(id_list))
        .options(selectinload(EffectiveFunction.principals))
    ).scalars():
        out["ef_rows"].setdefault(ef.contract_id, []).append(ef)
    for cid, count in session.execute(
        select(UpgradeEvent.contract_id, func.count(UpgradeEvent.id))
        .where(UpgradeEvent.contract_id.in_(id_list))
        .group_by(UpgradeEvent.contract_id)
    ).all():
        out["upgrade_events_count"][cid] = count
    for cid, last_block, last_ts in session.execute(
        select(
            UpgradeEvent.contract_id,
            func.max(UpgradeEvent.block_number),
            func.max(UpgradeEvent.timestamp),
        )
        .where(UpgradeEvent.contract_id.in_(id_list))
        .group_by(UpgradeEvent.contract_id)
    ).all():
        out["upgrade_events_last"][cid] = {"block": last_block, "timestamp": last_ts}
    for b in session.execute(select(ContractBalance).where(ContractBalance.contract_id.in_(id_list))).scalars():
        out["balances"].setdefault(b.contract_id, []).append(b)
    for n in session.execute(select(ControlGraphNode).where(ControlGraphNode.contract_id.in_(id_list))).scalars():
        out["cgn"].setdefault(n.contract_id, []).append(n)
    for e in session.execute(select(ControlGraphEdge).where(ControlGraphEdge.contract_id.in_(id_list))).scalars():
        out["cge"].setdefault(e.contract_id, []).append(e)
    return out


def build_governance_view(
    session: Session,
    jobs: list[Job],
    contracts_by_job_id: dict[Any, Contract],
    impl_job_by_addr: dict[str, Job],
) -> GovernanceView:
    """Build the contracts list + ownership hierarchy + fund flows + principals."""
    relevant_contract_ids: set[int] = {c.id for c in contracts_by_job_id.values() if c is not None}
    children = _prefetch_child_tables(session, relevant_contract_ids)
    controller_values_by_cid: dict[int, list[ControllerValue]] = children["controller_values"]
    ef_rows_by_cid: dict[int, list[EffectiveFunction]] = children["ef_rows"]
    upgrade_events_count_by_cid: dict[int, int] = children["upgrade_events_count"]
    last_upgrade_by_cid: dict[int, dict[str, Any]] = children["upgrade_events_last"]
    balances_by_cid: dict[int, list[Any]] = children["balances"]
    cgn_by_cid: dict[int, list[ControlGraphNode]] = children["cgn"]
    cge_by_cid: dict[int, list[ControlGraphEdge]] = children["cge"]

    contracts: list[dict[str, Any]] = []
    owner_groups: dict[str, list[dict]] = {}

    for job in jobs:
        request = job.request if isinstance(job.request, dict) else {}
        if request.get("proxy_address"):
            continue

        contract_row = contracts_by_job_id.get(job.id)
        is_proxy = contract_row.is_proxy if contract_row else False
        proxy_type = contract_row.proxy_type if contract_row else None
        impl_addr = contract_row.implementation if contract_row else None

        impl_job = impl_job_by_addr.get(impl_addr.lower()) if impl_addr else None
        impl_job_id = str(impl_job.id) if impl_job else None
        impl_contract = contracts_by_job_id.get(impl_job.id) if impl_job else None

        summary_row = impl_contract.summary if impl_contract else None
        if not summary_row and contract_row:
            summary_row = contract_row.summary

        # Prefer the impl's controller snapshot for proxies if it has any.
        lookup_contract = contract_row
        if is_proxy and impl_contract and controller_values_by_cid.get(impl_contract.id):
            lookup_contract = impl_contract

        owner = None
        controllers: dict[str, Any] = {}
        if lookup_contract:
            for cv in controller_values_by_cid.get(lookup_contract.id, []):
                controllers[cv.controller_id] = cv.value
                if "owner" in cv.controller_id.lower() and cv.value and cv.value.startswith("0x"):
                    owner = cv.value.lower()

        upgrade_count = upgrade_events_count_by_cid.get(contract_row.id) if contract_row else None
        last_upgrade_entry = (last_upgrade_by_cid.get(contract_row.id) if contract_row else None) or {}
        last_upgrade_block = last_upgrade_entry.get("block")
        last_ts = last_upgrade_entry.get("timestamp")
        last_upgrade_timestamp = last_ts.isoformat() if last_ts is not None else None

        ef_contract_id = (impl_contract.id if impl_contract else None) or (contract_row.id if contract_row else None)

        value_effects: list[str] = []
        all_effects: set[str] = set()
        ef_rows_for_contract = ef_rows_by_cid.get(ef_contract_id, []) if ef_contract_id else []
        for ef in ef_rows_for_contract:
            for label in ef.effect_labels or []:
                all_effects.add(label)
                if label in ("asset_pull", "asset_send", "mint", "burn"):
                    if label not in value_effects:
                        value_effects.append(label)

        capabilities: list[str] = []
        if is_proxy:
            capabilities.append("upgradeable")
        if "implementation_update" in all_effects:
            capabilities.append("upgrade")
        if "pause_toggle" in all_effects or (summary_row and summary_row.is_pausable):
            capabilities.append("pause")
        if "ownership_transfer" in all_effects:
            capabilities.append("ownership")
        if "role_management" in all_effects:
            capabilities.append("roles")
        if "asset_pull" in all_effects or "mint" in all_effects:
            capabilities.append("value-in")
        if "asset_send" in all_effects or "burn" in all_effects:
            capabilities.append("value-out")
        if "delegatecall_execution" in all_effects:
            capabilities.append("delegatecall")
        if "arbitrary_external_call" in all_effects:
            capabilities.append("arbitrary-call")

        contract_name = None
        if is_proxy and impl_job:
            if impl_contract and impl_contract.contract_name:
                contract_name = impl_contract.contract_name
            elif impl_job.name:
                contract_name = impl_job.name
        if not contract_name:
            contract_name = (contract_row.contract_name if contract_row else None) or job.name or ""
        standards = list(summary_row.standards or []) if summary_row else []
        is_factory = summary_row.is_factory if summary_row else False
        has_timelock = summary_row.has_timelock if summary_row else False
        is_pausable = summary_row.is_pausable if summary_row else False
        control_model = summary_row.control_model if summary_row else None

        name_lower = contract_name.lower()
        if "bridge" in name_lower or "gateway" in name_lower:
            role = "bridge"
        elif any(e in value_effects for e in ("asset_pull", "asset_send")):
            role = "value_handler"
        elif any(s in standards for s in ("ERC20", "ERC721", "ERC1155")):
            role = "token"
        elif has_timelock or control_model == "governance":
            role = "governance"
        elif is_factory:
            role = "factory"
        else:
            role = "utility"

        functions_list = [_build_company_function_entry(ef, ef.principals or []) for ef in ef_rows_for_contract]

        balance_contract = lookup_contract or contract_row
        balances_list = []
        total_usd = 0.0
        if balance_contract:
            for b in balances_by_cid.get(balance_contract.id, []):
                usd = float(b.usd_value) if b.usd_value is not None else None
                balances_list.append(
                    {
                        "token_symbol": b.token_symbol,
                        "token_name": b.token_name,
                        "token_address": b.token_address,
                        "raw_balance": b.raw_balance,
                        "decimals": b.decimals,
                        "usd_value": usd,
                        "price_usd": float(b.price_usd) if b.price_usd is not None else None,
                    }
                )
                if usd:
                    total_usd += usd

        entry: dict[str, Any] = {
            "address": job.address,
            "name": contract_name,
            "contract_id": contract_row.id if contract_row else None,
            "job_id": str(job.id),
            "impl_job_id": impl_job_id,
            "is_proxy": is_proxy,
            "proxy_type": proxy_type,
            "implementation": impl_addr,
            "deployer": contract_row.deployer if contract_row else None,
            "owner": owner,
            "controllers": controllers,
            "control_model": control_model,
            "risk_level": summary_row.risk_level if summary_row else None,
            "source_verified": summary_row.source_verified if summary_row else None,
            "chain": contract_row.chain if contract_row else None,
            "upgrade_count": upgrade_count,
            "last_upgrade_block": last_upgrade_block,
            "last_upgrade_timestamp": last_upgrade_timestamp,
            "role": role,
            "standards": standards,
            "value_effects": value_effects,
            "is_pausable": is_pausable,
            "has_timelock": has_timelock,
            "capabilities": capabilities,
            "functions": functions_list,
            "balances": balances_list,
            "total_usd": round(total_usd, 2) if total_usd > 0 else None,
        }

        graph_contract = lookup_contract or contract_row
        if graph_contract:
            cg_nodes = cgn_by_cid.get(graph_contract.id, [])
            cg_edges = cge_by_cid.get(graph_contract.id, [])
            entry["control_graph"] = {
                "nodes": [
                    {
                        "address": n.address,
                        "type": n.resolved_type,
                        "label": n.contract_name or n.label,
                        "details": n.details or {},
                    }
                    for n in cg_nodes
                ],
                "edges": [
                    {
                        "from": e.from_node_id.replace("address:", ""),
                        "to": e.to_node_id.replace("address:", ""),
                        "relation": e.relation,
                    }
                    for e in cg_edges
                ],
            }
        contracts.append(entry)

        if owner:
            owner_groups.setdefault(owner, []).append(entry)

    # Deduplicate: remove standalone impl contracts already represented via a proxy
    impl_addresses = {c["implementation"].lower() for c in contracts if c.get("implementation")}
    contracts = [
        c for c in contracts if not c["address"] or c["address"].lower() not in impl_addresses or c["is_proxy"]
    ]

    remaining_addrs = {c["address"] for c in contracts if c["address"]}
    for owner_addr in list(owner_groups):
        owner_groups[owner_addr] = [e for e in owner_groups[owner_addr] if e["address"] in remaining_addrs]
        if not owner_groups[owner_addr]:
            del owner_groups[owner_addr]

    hierarchy = _build_ownership_hierarchy(contracts, owner_groups)
    fund_flows, principals = _build_flows_and_principals(
        contracts, contracts_by_job_id, controller_values_by_cid, ef_rows_by_cid, cgn_by_cid, cge_by_cid
    )

    return GovernanceView(
        contracts=contracts,
        principals=principals,
        hierarchy=hierarchy,
        fund_flows=fund_flows,
    )


def _build_ownership_hierarchy(
    contracts: list[dict[str, Any]], owner_groups: dict[str, list[dict]]
) -> list[dict[str, Any]]:
    hierarchy: list[dict[str, Any]] = []
    assigned: set[str | None] = set()
    for owner_addr, owned in sorted(owner_groups.items(), key=lambda x: -len(x[1])):
        owner_contract = next((c for c in contracts if c["address"] and c["address"].lower() == owner_addr), None)
        hierarchy.append(
            {
                "owner": owner_addr,
                "owner_name": owner_contract["name"] if owner_contract else None,
                "owner_is_contract": owner_contract is not None,
                "contracts": [{"address": c["address"], "name": c["name"]} for c in owned],
            }
        )
        assigned.update(c["address"] for c in owned)

    unowned = [c for c in contracts if c["address"] not in assigned]
    if unowned:
        hierarchy.append(
            {
                "owner": None,
                "owner_name": "No owner detected",
                "owner_is_contract": False,
                "contracts": [{"address": c["address"], "name": c["name"]} for c in unowned],
            }
        )
    return hierarchy


def _build_flows_and_principals(
    contracts: list[dict[str, Any]],
    contracts_by_job_id: dict[Any, Contract],
    controller_values_by_cid: dict[int, list[ControllerValue]],
    ef_rows_by_cid: dict[int, list[EffectiveFunction]],
    cgn_by_cid: dict[int, list[ControlGraphNode]],
    cge_by_cid: dict[int, list[ControlGraphEdge]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    contract_addrs = {c["address"].lower() for c in contracts if c["address"]}
    contract_by_addr = {c["address"].lower(): c for c in contracts if c["address"]}
    flow_seen: set[tuple[str, str]] = set()
    fund_flows: list[dict[str, Any]] = []

    def add_flow(from_addr: str, to_addr: str, flow_type: str, lane: str = "control") -> None:
        key = (from_addr, to_addr)
        if key in flow_seen:
            return
        flow_seen.add(key)
        target = contract_by_addr.get(to_addr, {})
        fund_flows.append(
            {
                "from": from_addr,
                "to": to_addr,
                "type": flow_type,
                "lane": lane,
                "capabilities": target.get("capabilities", []),
            }
        )

    def _lookup_contract_for(entry: dict[str, Any]) -> Contract | None:
        import uuid as _uuid

        lookup_job_id = entry.get("impl_job_id") or entry["job_id"]
        try:
            key_id = _uuid.UUID(lookup_job_id) if isinstance(lookup_job_id, str) else lookup_job_id
        except (TypeError, ValueError):
            key_id = lookup_job_id
        return contracts_by_job_id.get(key_id)

    lookup_contract_by_entry: dict[str, Contract | None] = {}
    for entry in contracts:
        if entry.get("address"):
            lookup_contract_by_entry[entry["address"].lower()] = _lookup_contract_for(entry)

    for c in contracts:
        if not c["address"]:
            continue
        target = c["address"].lower()

        if c.get("owner") and c["owner"] in contract_addrs:
            flow_type = (
                "controls_value"
                if any(e in c.get("value_effects", []) for e in ("asset_pull", "asset_send"))
                else "controls"
            )
            add_flow(c["owner"], target, flow_type)

        for cid, val in c.get("controllers", {}).items():
            if isinstance(val, str) and val.startswith("0x"):
                val_lower = val.lower()
                if val_lower in contract_addrs and val_lower != (c.get("owner") or ""):
                    add_flow(val_lower, target, "controller")

        lookup_c = lookup_contract_by_entry.get(target)
        if lookup_c:
            for cgn in cgn_by_cid.get(lookup_c.id, []):
                node_addr = (cgn.address or "").lower()
                if node_addr and node_addr in contract_addrs and node_addr != target:
                    add_flow(node_addr, target, "principal")

    # Collect non-contract principals from control graph + function principals.
    # First pass: find safe_owner edges so we can nest Safe owners later.
    principal_map: dict[str, dict[str, Any]] = {}
    safe_owners_map: dict[str, list[str]] = {}
    owner_of_safe: set[str] = set()

    for c in contracts:
        if not c["address"]:
            continue
        lookup_c = lookup_contract_by_entry.get(c["address"].lower())
        if not lookup_c:
            continue
        for edge in cge_by_cid.get(lookup_c.id, []):
            if edge.relation != "safe_owner":
                continue
            safe_addr = edge.from_node_id.replace("address:", "").lower()
            owner_addr = edge.to_node_id.replace("address:", "").lower()
            safe_owners_map.setdefault(safe_addr, [])
            if owner_addr not in safe_owners_map[safe_addr]:
                safe_owners_map[safe_addr].append(owner_addr)
            owner_of_safe.add(owner_addr)

    # Second pass: collect direct controllers (skip Safe owners — they're nested)
    for c in contracts:
        if not c["address"]:
            continue
        target = c["address"].lower()
        lookup_c = lookup_contract_by_entry.get(target)
        if not lookup_c:
            continue

        for cgn in cgn_by_cid.get(lookup_c.id, []):
            node_addr = (cgn.address or "").lower()
            if not node_addr or node_addr in contract_addrs:
                continue
            if node_addr in owner_of_safe:
                continue
            if cgn.resolved_type not in ("safe", "timelock", "proxy_admin", "eoa"):
                continue
            if node_addr == "0x0000000000000000000000000000000000000000":
                continue

            if node_addr not in principal_map:
                # Seed details with the CGN's own introspection result
                # (getOwners/getThreshold for safes, getMinDelay for
                # timelocks). This is the authoritative source for the
                # principal's intrinsic config — ControllerValue rows
                # describe the relationship FROM a consumer, not the
                # Safe's own threshold, so prior code that only merged
                # CV details missed the threshold and fell back to
                # len(owners).
                details: dict[str, Any] = {}
                if isinstance(cgn.details, dict):
                    details.update(cgn.details)
                for cv in controller_values_by_cid.get(lookup_c.id, []):
                    if (cv.value or "").lower() != node_addr:
                        continue
                    if cv.details and isinstance(cv.details, dict):
                        for k, v in cv.details.items():
                            details.setdefault(k, v)

                if cgn.resolved_type == "safe":
                    if not details.get("owners"):
                        details["owners"] = safe_owners_map.get(node_addr, [])
                    if "threshold" not in details and details.get("owners"):
                        details["threshold"] = len(details["owners"])

                principal_map[node_addr] = {
                    "address": node_addr,
                    "type": cgn.resolved_type,
                    "label": cgn.contract_name or cgn.label or cgn.resolved_type,
                    "details": details,
                    "controls": [],
                }

            principal_map[node_addr]["controls"].append(target)
            add_flow(node_addr, target, "principal")

    # Third pass: pull principals out of FunctionPrincipal rows. Some
    # role-gated functions (e.g. EtherFiTimelock.cancel / .execute) have
    # their controlling Safe/EOA stored *only* on the per-function
    # principal row — the Safe never gets a top-level ControlGraphNode
    # entry for that contract, so the prior CGN-only pass misses the
    # Safe→Contract edge entirely. This pass backfills.
    for c in contracts:
        if not c["address"]:
            continue
        target = c["address"].lower()
        lookup_c = lookup_contract_by_entry.get(target)
        if not lookup_c:
            continue
        fp_iter = (fp for ef in ef_rows_by_cid.get(lookup_c.id, []) for fp in (ef.principals or []))
        for fp in fp_iter:
            pa = (fp.address or "").lower()
            if not pa or pa == target:
                continue
            if pa == "0x0000000000000000000000000000000000000000":
                continue
            if pa in owner_of_safe:
                continue
            if fp.resolved_type not in ("safe", "timelock", "eoa", "proxy_admin"):
                continue
            if pa in contract_addrs:
                continue
            if pa not in principal_map:
                fp_details = dict(fp.details or {})
                if fp.resolved_type == "safe":
                    if not fp_details.get("owners"):
                        fp_details["owners"] = safe_owners_map.get(pa, [])
                    if "threshold" not in fp_details and fp_details.get("owners"):
                        fp_details["threshold"] = len(fp_details["owners"])
                principal_map[pa] = {
                    "address": pa,
                    "type": fp.resolved_type,
                    "label": fp.resolved_type,
                    "details": fp_details,
                    "controls": [],
                }
            if target not in principal_map[pa]["controls"]:
                principal_map[pa]["controls"].append(target)
            add_flow(pa, target, "principal")

    return fund_flows, list(principal_map.values())


def _all_addresses_for_protocol(
    session: Session, protocol_row: Protocol | None, jobs: list[Job]
) -> list[dict[str, Any]]:
    if protocol_row:
        all_contract_rows = (
            session.execute(select(Contract).where(Contract.protocol_id == protocol_row.id)).scalars().all()
        )
    else:
        fallback_job_ids = [j.id for j in jobs]
        if fallback_job_ids:
            all_contract_rows = list(
                session.execute(select(Contract).where(Contract.job_id.in_(fallback_job_ids))).scalars()
            )
        else:
            all_contract_rows = []

    # Prefetch impl-name lookup so proxy rows can expose the implementation
    # contract name alongside their own generic "UUPSProxy"/"ERC1967Proxy"
    # template name.
    impl_name_by_addr = {
        (c.address or "").lower(): c.contract_name for c in all_contract_rows if c.address and c.contract_name
    }
    job_ids = {cr.job_id for cr in all_contract_rows if cr.job_id is not None}
    completed_job_ids: set = set()
    if job_ids:
        completed_job_ids = set(
            session.execute(select(Job.id).where(Job.id.in_(job_ids), Job.status == JobStatus.completed))
            .scalars()
            .all()
        )

    return sorted(
        [
            {
                "address": cr.address,
                "name": cr.contract_name,
                "source_verified": cr.source_verified,
                "is_proxy": cr.is_proxy,
                "analyzed": cr.job_id is not None and cr.job_id in completed_job_ids,
                "discovery_sources": list(cr.discovery_sources or []),
                "discovery_url": cr.discovery_url,
                "chain": cr.chain,
                "rank_score": (float(cr.rank_score) if cr.rank_score is not None else None),
                "implementation_address": cr.implementation if cr.is_proxy else None,
                "implementation_name": (
                    impl_name_by_addr.get((cr.implementation or "").lower()) if cr.is_proxy else None
                ),
            }
            for cr in all_contract_rows
        ],
        key=lambda x: (not x["analyzed"], x["name"] or "zzz"),
    )


def _latest_tvl(session: Session, protocol_row: Protocol | None) -> dict[str, Any] | None:
    if protocol_row is None:
        return None
    latest_tvl = session.execute(
        select(TvlSnapshot)
        .where(TvlSnapshot.protocol_id == protocol_row.id)
        .order_by(TvlSnapshot.timestamp.desc())
        .limit(1)
    ).scalar_one_or_none()
    if latest_tvl is None:
        return None
    return {
        "total_usd": float(latest_tvl.total_usd) if latest_tvl.total_usd else None,
        "defillama_tvl": float(latest_tvl.defillama_tvl) if latest_tvl.defillama_tvl else None,
        "source": latest_tvl.source,
        "timestamp": latest_tvl.timestamp.isoformat(),
    }


def _audit_reports(session: Session, protocol_row: Protocol | None) -> list[dict[str, Any]]:
    if protocol_row is None:
        return []
    audit_rows = (
        session.execute(
            select(AuditReport)
            .where(AuditReport.protocol_id == protocol_row.id)
            .order_by(AuditReport.date.desc().nullslast())
        )
        .scalars()
        .all()
    )
    return [
        {
            "url": ar.url,
            "pdf_url": ar.pdf_url,
            "auditor": ar.auditor,
            "title": ar.title,
            "date": ar.date,
            "confidence": float(ar.confidence) if ar.confidence is not None else None,
        }
        for ar in audit_rows
    ]


def assemble_company_payload(
    session: Session,
    name: str,
    protocol_row: Protocol | None,
    jobs: list[Job],
    governance: GovernanceView,
) -> dict[str, Any]:
    return {
        "company": name,
        "protocol_id": protocol_row.id if protocol_row else None,
        "contract_count": len(governance.contracts),
        "tvl": _latest_tvl(session, protocol_row),
        "audit_reports": _audit_reports(session, protocol_row),
        "contracts": governance.contracts,
        "principals": governance.principals,
        "ownership_hierarchy": governance.hierarchy,
        "fund_flows": governance.fund_flows,
        "all_addresses": _all_addresses_for_protocol(session, protocol_row, jobs),
    }


def build_company_overview(session: Session, name: str) -> dict[str, Any]:
    protocol_row, jobs = resolve_company_jobs(session, name)
    if not jobs:
        raise CompanyNotFound(name)
    contracts_by_job_id = prefetch_contracts(session, jobs)
    impl_job_by_addr, contracts_by_job_id = resolve_implementation_contracts(session, jobs, contracts_by_job_id)
    governance = build_governance_view(session, jobs, contracts_by_job_id, impl_job_by_addr)
    return assemble_company_payload(session, name, protocol_row, jobs, governance)
