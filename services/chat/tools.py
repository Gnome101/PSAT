"""Tool implementations for the protocol-analyst chatbot.

Each tool is a plain Python function that takes keyword args and returns
a JSON-serializable dict or list. Schemas are OpenAI-compatible and
passed to the LLM's ``tools=`` parameter; the LLM then emits
``tool_calls`` which the orchestrator in ``api.py`` dispatches back to
these functions.

Why hand-rolled instead of LangChain/Pydantic AI: the codebase already
has ~100 LOC hand-rolled HTTP clients for Tavily/Exa/OpenRouter, so
adding one more tiny module of plain functions matches the existing
style and avoids pulling in a dependency we'd then have to upgrade
forever.

Contract: every tool ALWAYS returns a dict. On error, returns
``{"error": "<human-readable message>"}``. The LLM then sees that and
can either retry with different args or explain to the user that the
data wasn't available. This keeps the loop robust to bad LLM
tool-calls (e.g. asking for a function that doesn't exist) without
crashing the chat.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import or_, select

from db.models import (
    AuditReport,
    Contract,
    ContractSummary,
    ControlGraphEdge,
    ControlGraphNode,
    EffectiveFunction,
    FunctionPrincipal,
    Protocol,
    SessionLocal,
    SourceFile,
)


def _protocol_id_for(company: str, session) -> int | None:
    row = session.execute(select(Protocol).where(Protocol.name == company)).scalar_one_or_none()
    return row.id if row else None


def _contract_by_address(address: str, session) -> Contract | None:
    addr = (address or "").lower()
    return session.execute(select(Contract).where(Contract.address == addr)).scalar_one_or_none()


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


def list_contracts(
    company: str,
    limit: int = 50,
    min_usd: float | None = None,
    is_proxy: bool | None = None,
    order_by: str = "tvl",
) -> dict[str, Any]:
    """List contracts for a company with basic metadata.

    Replaces the "top 30 by TVL" text dump the old prompt used —
    the LLM can page through deliberately when it needs more.
    """
    with SessionLocal() as session:
        protocol_id = _protocol_id_for(company, session)
        if protocol_id is None:
            return {"error": f"company {company!r} not found"}
        stmt = select(Contract).where(Contract.protocol_id == protocol_id)
        if is_proxy is not None:
            stmt = stmt.where(Contract.is_proxy == is_proxy)
        rows = list(session.execute(stmt).scalars().all())
        # Attach ContractSummary so we can filter/sort by USD.
        summaries = {
            s.contract_id: s
            for s in session.execute(
                select(ContractSummary).where(ContractSummary.contract_id.in_([r.id for r in rows]))
            ).scalars()
        }
        items = []
        for r in rows:
            s = summaries.get(r.id)
            usd = getattr(s, "total_usd", None) if s else None
            if min_usd is not None and (usd or 0) < min_usd:
                continue
            items.append(
                {
                    "name": r.contract_name,
                    "address": r.address,
                    "is_proxy": r.is_proxy,
                    "proxy_type": r.proxy_type,
                    "implementation": r.implementation,
                    "source_verified": r.source_verified,
                    "total_usd": usd,
                    "has_timelock": getattr(s, "has_timelock", None) if s else None,
                    "is_pausable": getattr(s, "is_pausable", None) if s else None,
                    "risk_level": getattr(s, "risk_level", None) if s else None,
                }
            )
        if order_by == "tvl":
            items.sort(key=lambda x: x.get("total_usd") or 0, reverse=True)
        elif order_by == "name":
            items.sort(key=lambda x: (x.get("name") or "").lower())
        items = items[:limit]
        return {"company": company, "total_matches": len(items), "contracts": items}


def get_contract(address: str) -> dict[str, Any]:
    """Full detail for one contract: identity, ownership, functions count."""
    with SessionLocal() as session:
        c = _contract_by_address(address, session)
        if c is None:
            return {"error": f"contract {address!r} not found"}
        s = session.execute(select(ContractSummary).where(ContractSummary.contract_id == c.id)).scalar_one_or_none()
        fn_count = (
            session.execute(select(EffectiveFunction).where(EffectiveFunction.contract_id == c.id)).scalars().all()
        )
        return {
            "address": c.address,
            "name": c.contract_name,
            "is_proxy": c.is_proxy,
            "proxy_type": c.proxy_type,
            "implementation": c.implementation,
            "deployer": c.deployer,
            "source_verified": c.source_verified,
            "compiler_version": c.compiler_version,
            "total_usd": getattr(s, "total_usd", None) if s else None,
            "control_model": getattr(s, "control_model", None) if s else None,
            "has_timelock": getattr(s, "has_timelock", None) if s else None,
            "is_pausable": getattr(s, "is_pausable", None) if s else None,
            "is_factory": getattr(s, "is_factory", None) if s else None,
            "risk_level": getattr(s, "risk_level", None) if s else None,
            "standards": list(getattr(s, "standards", None) or []) if s else [],
            "effective_function_count": len(fn_count),
        }


def list_functions(address: str, limit: int = 100) -> dict[str, Any]:
    """Privileged functions for a contract with their resolved principals."""
    with SessionLocal() as session:
        c = _contract_by_address(address, session)
        if c is None:
            return {"error": f"contract {address!r} not found"}
        fns = list(
            session.execute(select(EffectiveFunction).where(EffectiveFunction.contract_id == c.id).limit(limit))
            .scalars()
            .all()
        )
        fn_ids = [f.id for f in fns]
        principals_by_fn: dict[int, list[dict]] = {}
        if fn_ids:
            for p in session.execute(
                select(FunctionPrincipal).where(FunctionPrincipal.function_id.in_(fn_ids))
            ).scalars():
                principals_by_fn.setdefault(p.function_id, []).append(
                    {
                        "address": p.address,
                        "origin": p.origin,
                        "principal_type": p.principal_type,
                        "resolved_type": p.resolved_type,
                    }
                )
        items = []
        for f in fns:
            items.append(
                {
                    "function": f.function_name,
                    "abi_signature": f.abi_signature,
                    "selector": f.selector,
                    "authority_public": f.authority_public,
                    "effect_labels": list(f.effect_labels or []),
                    "effect_targets": list(f.effect_targets or []),
                    "action_summary": f.action_summary,
                    "principals": principals_by_fn.get(f.id, []),
                }
            )
        return {"contract": c.address, "contract_name": c.contract_name, "functions": items}


def list_audits(company: str, auditor: str | None = None, limit: int = 100) -> dict[str, Any]:
    """Full audit-report list for a company (not truncated to top-20)."""
    with SessionLocal() as session:
        protocol_id = _protocol_id_for(company, session)
        if protocol_id is None:
            return {"error": f"company {company!r} not found"}
        stmt = select(AuditReport).where(AuditReport.protocol_id == protocol_id).limit(limit)
        if auditor:
            needle = f"%{auditor.lower()}%"
            stmt = stmt.where(AuditReport.auditor.ilike(needle))
        rows = list(session.execute(stmt).scalars().all())
        items = [
            {
                "title": r.title,
                "auditor": r.auditor,
                "date": str(r.date) if r.date else None,
                "pdf_url": r.pdf_url,
                "source_repo": r.source_repo,
                "scope_contract_count": len(r.scope_contracts or []) if r.scope_contracts else 0,
            }
            for r in rows
        ]
        return {"company": company, "audit_count": len(items), "audits": items}


def search_contracts(company: str, query: str, limit: int = 15) -> dict[str, Any]:
    """Substring search over contract names + address prefix match."""
    with SessionLocal() as session:
        protocol_id = _protocol_id_for(company, session)
        if protocol_id is None:
            return {"error": f"company {company!r} not found"}
        q = (query or "").strip()
        if not q:
            return {"error": "query must not be empty"}
        name_pat = f"%{q}%"
        addr_pat = f"{q.lower()}%"
        stmt = (
            select(Contract)
            .where(Contract.protocol_id == protocol_id)
            .where(or_(Contract.contract_name.ilike(name_pat), Contract.address.ilike(addr_pat)))
            .limit(limit)
        )
        rows = list(session.execute(stmt).scalars().all())
        return {
            "company": company,
            "query": query,
            "matches": [{"name": r.contract_name, "address": r.address, "is_proxy": r.is_proxy} for r in rows],
        }


def get_control_graph(address: str, direction: str = "out", max_edges: int = 30) -> dict[str, Any]:
    """Nodes + edges around a contract — who it controls, who controls it.

    ``direction``: ``"out"`` = contracts this address is an owner/admin of,
    ``"in"`` = contracts/EOAs that control this address, ``"both"``.
    """
    with SessionLocal() as session:
        c = _contract_by_address(address, session)
        if c is None:
            return {"error": f"contract {address!r} not found"}
        node_id_out = f"address:{c.address}"
        stmt = select(ControlGraphEdge).where(ControlGraphEdge.contract_id == c.id).limit(max_edges)
        edges = list(session.execute(stmt).scalars().all())
        if direction == "out":
            edges = [e for e in edges if e.from_node_id == node_id_out]
        elif direction == "in":
            edges = [e for e in edges if e.to_node_id == node_id_out]
        nodes = list(
            session.execute(select(ControlGraphNode).where(ControlGraphNode.contract_id == c.id)).scalars().all()
        )
        node_by_id = {f"address:{n.address}": n for n in nodes}
        edge_items = []
        for e in edges[:max_edges]:
            src = node_by_id.get(e.from_node_id)
            dst = node_by_id.get(e.to_node_id)
            edge_items.append(
                {
                    "from": e.from_node_id.split(":", 1)[-1],
                    "from_label": getattr(src, "label", None) if src else None,
                    "to": e.to_node_id.split(":", 1)[-1],
                    "to_label": getattr(dst, "label", None) if dst else None,
                    "relation": e.relation,
                    "label": e.label,
                }
            )
        return {"root": c.address, "direction": direction, "edges": edge_items}


def get_source(address: str, path_substring: str | None = None, max_chars: int = 6000) -> dict[str, Any]:
    """Source code for a contract. Pass ``path_substring`` to narrow to
    a specific file; otherwise the primary source file is returned.
    """
    with SessionLocal() as session:
        c = _contract_by_address(address, session)
        if c is None or c.job_id is None:
            return {"error": f"contract {address!r} has no source / job"}
        stmt = select(SourceFile).where(SourceFile.job_id == c.job_id)
        if path_substring:
            stmt = stmt.where(SourceFile.path.ilike(f"%{path_substring}%"))
        rows = list(session.execute(stmt.limit(10)).scalars().all())
        if not rows:
            return {"error": "no source files matched", "contract": address}
        # If multiple files matched, just list their paths and let the
        # LLM pick the one it wants by calling again with a narrower
        # path_substring.
        if len(rows) > 1 and not path_substring:
            return {
                "contract": address,
                "multiple": True,
                "files": [{"path": r.path, "bytes": len(r.content or "")} for r in rows],
                "hint": "call get_source again with path_substring set to one of these paths",
            }
        f = rows[0]
        content = (f.content or "")[:max_chars]
        return {
            "contract": address,
            "path": f.path,
            "truncated": len(f.content or "") > max_chars,
            "content": content,
        }


# ---------------------------------------------------------------------------
# Schemas — OpenAI function-calling format
# ---------------------------------------------------------------------------

TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "list_contracts",
            "description": (
                "List contracts belonging to a company (protocol) with basic metadata. "
                "Use this first to get an overview of what's in scope before drilling "
                "into specific contracts. Supports filtering by min USD value and "
                "proxy status, ordered by TVL by default."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company": {"type": "string", "description": "Company/protocol slug, e.g. 'ether fi'"},
                    "limit": {"type": "integer", "default": 50},
                    "min_usd": {"type": "number", "description": "Minimum TVL in USD", "default": None},
                    "is_proxy": {"type": "boolean", "description": "Filter to only proxies when true"},
                    "order_by": {"type": "string", "enum": ["tvl", "name"], "default": "tvl"},
                },
                "required": ["company"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_contract",
            "description": (
                "Full detail for one contract by address: name, proxy status, "
                "implementation, owner, timelock, pausability, risk level, "
                "and standard tags."
            ),
            "parameters": {
                "type": "object",
                "properties": {"address": {"type": "string", "description": "0x-prefixed 20-byte address"}},
                "required": ["address"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_functions",
            "description": (
                "List privileged functions of a contract with their resolved "
                "on-chain principals (addresses that can call the function). "
                "Essential for blast-radius analysis."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "address": {"type": "string"},
                    "limit": {"type": "integer", "default": 100},
                },
                "required": ["address"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_audits",
            "description": (
                "Full list of third-party security audit reports for a company. "
                "Use this when the user asks about audit coverage — the "
                "context's top-20 may not include the one they're asking about."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company": {"type": "string"},
                    "auditor": {"type": "string", "description": "Filter by auditor firm name (substring match)"},
                    "limit": {"type": "integer", "default": 100},
                },
                "required": ["company"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_contracts",
            "description": (
                "Fuzzy search contracts by name or address prefix within a "
                "company. Use when the user names a contract that wasn't in "
                "the context dump."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company": {"type": "string"},
                    "query": {"type": "string", "description": "Contract name or address prefix"},
                    "limit": {"type": "integer", "default": 15},
                },
                "required": ["company", "query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_control_graph",
            "description": (
                "Returns who owns this contract and what it owns "
                "(outgoing/incoming control edges). Use for 'what does X "
                "control' or 'who can change Y' questions."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "address": {"type": "string"},
                    "direction": {"type": "string", "enum": ["out", "in", "both"], "default": "out"},
                    "max_edges": {"type": "integer", "default": 30},
                },
                "required": ["address"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_source",
            "description": (
                "Read source code for a contract. If path_substring is "
                "omitted, returns the list of source files available; call "
                "again with path_substring to fetch one. Content is "
                "truncated at max_chars."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "address": {"type": "string"},
                    "path_substring": {"type": "string"},
                    "max_chars": {"type": "integer", "default": 6000},
                },
                "required": ["address"],
            },
        },
    },
]


TOOL_IMPLS = {
    "list_contracts": list_contracts,
    "get_contract": get_contract,
    "list_functions": list_functions,
    "list_audits": list_audits,
    "search_contracts": search_contracts,
    "get_control_graph": get_control_graph,
    "get_source": get_source,
}
