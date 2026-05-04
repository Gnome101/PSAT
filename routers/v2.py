"""Schema-v2 capability + probe endpoints (predicate-pipeline cutover).

Hosts the read path that consumes the v2 ``predicate_trees`` artifact:
 - per-contract / per-company capability resolution
 - membership and signature probes against individual leaves
 - v1↔v2 diff harness + fleet-wide migration status

These shipped on the predicate-pipeline branch while ``api.py`` was still
a single-file monolith; under the routers refactor they live here so the
existing handler is untouched and the registration is one line in
``api.py``.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select

from db.models import Job, JobStatus, Protocol

from . import deps

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Probe rate limiter
# ---------------------------------------------------------------------------
# v4 plan §15 spec is "10/min/key/contract" — sliding-window per
# (admin_key, address). PSAT_PROBE_RATE_LIMIT and PSAT_PROBE_RATE_WINDOW_S
# override. Each worker has its own state so a multi-worker deployment
# allows up to N×limit requests in aggregate; that's an acceptable first
# cut. Long-term: shared store (Redis) for fleet-wide accounting.

_PROBE_RATE_LIMIT = int(os.environ.get("PSAT_PROBE_RATE_LIMIT", "10"))
_PROBE_RATE_WINDOW_S = float(os.environ.get("PSAT_PROBE_RATE_WINDOW_S", "60"))
_probe_rate_state: dict[tuple[str, str], Any] = {}


def _probe_rate_check(admin_key: str | None, address: str) -> None:
    """Raise HTTPException(429) when the (admin_key, address) sliding
    window has hit its limit. No-op when the limit is 0 (env override
    for testing / disabled-by-default flag use)."""
    if _PROBE_RATE_LIMIT <= 0:
        return
    import collections as _collections
    import time as _time

    state = _probe_rate_state.setdefault((admin_key or "<no-key>", address.lower()), _collections.deque())
    now = _time.time()
    while state and state[0] + _PROBE_RATE_WINDOW_S < now:
        state.popleft()
    if len(state) >= _PROBE_RATE_LIMIT:
        retry_after = max(0, int(state[0] + _PROBE_RATE_WINDOW_S - now)) + 1
        raise HTTPException(
            status_code=429,
            detail=(
                f"Probe rate limit exceeded for this admin key + contract "
                f"({_PROBE_RATE_LIMIT} requests / "
                f"{int(_PROBE_RATE_WINDOW_S)}s). Retry in ~{retry_after}s."
            ),
            headers={"Retry-After": str(retry_after)},
        )
    state.append(now)


# ---------------------------------------------------------------------------
# Capabilities response cache
# ---------------------------------------------------------------------------
# In-process TTL cache for /api/contract/{addr}/capabilities. Per the v4
# plan §15 ("Response cache 60 blocks") — at 12s/block on mainnet that's
# ~12 minutes; we accept seconds for chain-agnostic simplicity.
# PSAT_CAPABILITIES_CACHE_TTL_S overrides; 0 disables. Each worker process
# has its own cache; that's fine — the resolver is read-only and the cache
# is best-effort.
_CAPABILITIES_CACHE_TTL_S = float(os.environ.get("PSAT_CAPABILITIES_CACHE_TTL_S", "60"))
_capabilities_cache: dict[tuple[str, int, int | None], tuple[float, dict[str, Any]]] = {}


def _capabilities_cache_get(key: tuple[str, int, int | None]) -> dict[str, Any] | None:
    if _CAPABILITIES_CACHE_TTL_S <= 0:
        return None
    import time as _time

    entry = _capabilities_cache.get(key)
    if entry is None:
        return None
    expires_at, value = entry
    if expires_at < _time.time():
        _capabilities_cache.pop(key, None)
        return None
    return value


def _capabilities_cache_put(key: tuple[str, int, int | None], value: dict[str, Any]) -> None:
    if _CAPABILITIES_CACHE_TTL_S <= 0:
        return
    import time as _time

    _capabilities_cache[key] = (_time.time() + _CAPABILITIES_CACHE_TTL_S, value)


# ---------------------------------------------------------------------------
# Probe request models
# ---------------------------------------------------------------------------


class _ProbeMembershipRequest(BaseModel):
    function_signature: str = Field(..., description="Full signature, e.g. 'grantRole(bytes32,address)'")
    predicate_index: int = Field(..., ge=0, description="DFS-order leaf index in the function's predicate tree")
    member: str = Field(..., description="Address being tested for membership in the leaf's set")
    chain_id: int = Field(default=1, description="Chain id for repo lookups (defaults to ethereum mainnet)")
    block: int | None = Field(default=None, description="Optional block number for point-in-time probes")

    @field_validator("member")
    @classmethod
    def _check_member_address(cls, v: str) -> str:
        if not isinstance(v, str) or not v.startswith("0x") or len(v) != 42:
            raise ValueError("member must be a 0x-prefixed 20-byte address")
        return v.lower()


class _ProbeSignatureRequest(BaseModel):
    function_signature: str = Field(..., description="Full signature, e.g. 'execute(bytes32,bytes)'")
    predicate_index: int = Field(..., ge=0, description="DFS-order leaf index for the signature_auth leaf")
    recovered_signer: str = Field(
        ...,
        description=(
            "Address ECDSA-recovered from (hash, sig) by the caller, OR the address "
            "approving the signature via EIP-1271 isValidSignature. The route checks "
            "whether this address is in the leaf's allowed-signer set."
        ),
    )
    chain_id: int = Field(default=1)
    block: int | None = Field(default=None)

    @field_validator("recovered_signer")
    @classmethod
    def _check_signer_address(cls, v: str) -> str:
        if not isinstance(v, str) or not v.startswith("0x") or len(v) != 42:
            raise ValueError("recovered_signer must be a 0x-prefixed 20-byte address")
        return v.lower()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compute_data_freshness(session, address: str, chain_id: int) -> dict[str, Any]:
    """Look up the role_grants_cursors row for ``(chain_id, address)``
    and return a freshness summary the UI can render as
    'data current as of block X, ~5 min ago'.

    Returns:
        {
          "role_grants": {
            "last_indexed_block": int | None,
            "last_run_at": ISO8601 string | None,
          } | None
        }

    ``role_grants`` is None when no cursor exists yet (legacy pre-v2
    contract or AC contract not yet enrolled in the indexer). Other
    indexers (aragon_acl) follow the same pattern; surfaced lazily as
    we wire them.
    """
    from db.models import Contract, RoleGrantsCursor

    chain_name_map = {1: "ethereum", 10: "optimism", 137: "polygon", 8453: "base", 42161: "arbitrum"}
    chain_name = chain_name_map.get(chain_id, "ethereum")
    contract_row = session.execute(
        select(Contract.id).where(
            func.lower(Contract.address) == address.lower(),
            Contract.chain == chain_name,
        )
    ).first()
    if contract_row is None:
        return {"role_grants": None}
    cid = contract_row[0]
    cursor = session.execute(
        select(RoleGrantsCursor).where(
            RoleGrantsCursor.chain_id == chain_id,
            RoleGrantsCursor.contract_id == cid,
        )
    ).scalar_one_or_none()
    if cursor is None:
        return {"role_grants": None}
    return {
        "role_grants": {
            "last_indexed_block": cursor.last_indexed_block,
            "last_run_at": cursor.last_run_at.isoformat() if cursor.last_run_at else None,
        }
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/api/contract/{address}/probe/membership",
    dependencies=[Depends(deps.require_admin_key)],
)
def probe_contract_membership(
    address: str,
    req: _ProbeMembershipRequest,
    x_psat_admin_key: str | None = Header(default=None),
) -> dict[str, Any]:
    """v2 schema probe: 'is ``member`` allowed by leaf ``predicate_index``
    of ``function_signature`` on ``address``?'

    Resolves the predicate_trees artifact server-side from the most
    recent successful job for ``address``; the descriptor is NEVER
    client-supplied — clients only carry the leaf index they received
    from the v2 capability rendering.
    """
    addr = deps._normalize_address_or_400(address)
    _probe_rate_check(x_psat_admin_key, addr)

    # Lazy-import the resolver bits so the probe route doesn't impose
    # its dependency surface on the rest of the API.
    from services.resolution.adapters import AdapterRegistry, EvaluationContext
    from services.resolution.adapters.access_control import AccessControlAdapter
    from services.resolution.adapters.aragon_acl import (
        AragonACLAdapter,
        DSAuthAdapter,
        EIP1271Adapter,
    )
    from services.resolution.adapters.event_indexed import EventIndexedAdapter
    from services.resolution.adapters.safe import SafeAdapter
    from services.resolution.probe import probe_membership
    from services.resolution.repos import PostgresAragonACLRepo, PostgresRoleGrantsRepo

    with deps.SessionLocal() as session:
        job = session.execute(
            select(Job)
            .where(Job.address == addr)
            .where(Job.status == JobStatus.completed)
            .order_by(Job.updated_at.desc(), Job.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if job is None:
            raise HTTPException(status_code=404, detail=f"No completed analysis job found for {addr}")

        artifact = deps.get_artifact(session, job.id, "predicate_trees")
        if artifact is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    "predicate_trees artifact missing for the latest analysis "
                    "(contract was analyzed before schema-v2 emit landed, or v2 emit failed)"
                ),
            )

        if not isinstance(artifact, dict) or "trees" not in artifact:
            # Either an error-path placeholder ({"error": "..."}) or a
            # malformed payload — surface the reason rather than silently
            # treating as no-tree.
            reason = artifact.get("error") if isinstance(artifact, dict) else "predicate_trees payload was not a dict"
            return {
                "result": "unknown",
                "reason": "predicate_trees_unavailable",
                "detail": reason,
            }

        tree = artifact["trees"].get(req.function_signature)
        if tree is None:
            # Resolver convention: absent function = unguarded (publicly
            # callable). For probe semantics, that means anyone is in
            # the set.
            return {
                "result": "yes",
                "reason": "function_unguarded",
                "function_signature": req.function_signature,
            }

        registry = AdapterRegistry()
        for cls in (
            AccessControlAdapter,
            SafeAdapter,
            AragonACLAdapter,
            DSAuthAdapter,
            EIP1271Adapter,
            EventIndexedAdapter,
        ):
            registry.register(cls)

        # Wire the Postgres-backed repos. AC adapter consumes
        # ``ctx.role_grants`` directly; Aragon adapter looks under
        # ``ctx.meta["aragon_acl_repo"]`` per its existing contract.
        # SafeRepo is RPC-backed (RpcSafeRepo) and would need an
        # rpc_url_for_chain map — not wired here yet because the API
        # process doesn't carry per-chain RPC configuration on the
        # request boundary.
        role_grants_repo = PostgresRoleGrantsRepo(session)
        aragon_repo = PostgresAragonACLRepo(session)
        ctx = EvaluationContext(
            chain_id=req.chain_id,
            contract_address=addr,
            block=req.block,
            role_grants=role_grants_repo,
            meta={"aragon_acl_repo": aragon_repo},
        )

        return probe_membership(
            tree,
            predicate_index=req.predicate_index,
            member=req.member,
            registry=registry,
            ctx=ctx,
        )


@router.post(
    "/api/contract/{address}/probe/signature",
    dependencies=[Depends(deps.require_admin_key)],
)
def probe_contract_signature(
    address: str,
    req: _ProbeSignatureRequest,
    x_psat_admin_key: str | None = Header(default=None),
) -> dict[str, Any]:
    """Counterpart to /probe/membership for signature_auth leaves.
    Caller already did ECDSA recovery (or EIP-1271 verification); we
    check whether the recovered signer is in the leaf's allowed-signer
    set."""
    from services.resolution.adapters import AdapterRegistry, EvaluationContext
    from services.resolution.adapters.access_control import AccessControlAdapter
    from services.resolution.adapters.aragon_acl import (
        AragonACLAdapter,
        DSAuthAdapter,
        EIP1271Adapter,
    )
    from services.resolution.adapters.event_indexed import EventIndexedAdapter
    from services.resolution.adapters.safe import SafeAdapter
    from services.resolution.probe import probe_signature
    from services.resolution.repos import PostgresAragonACLRepo, PostgresRoleGrantsRepo

    addr = deps._normalize_address_or_400(address)
    _probe_rate_check(x_psat_admin_key, addr)
    with deps.SessionLocal() as session:
        job = session.execute(
            select(Job)
            .where(Job.address == addr)
            .where(Job.status == JobStatus.completed)
            .order_by(Job.updated_at.desc(), Job.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if job is None:
            raise HTTPException(status_code=404, detail=f"No completed analysis job found for {addr}")
        artifact = deps.get_artifact(session, job.id, "predicate_trees")
        if artifact is None:
            raise HTTPException(
                status_code=404,
                detail="predicate_trees artifact missing for the latest analysis",
            )
        if not isinstance(artifact, dict) or "trees" not in artifact:
            return {
                "result": "unknown",
                "reason": "predicate_trees_unavailable",
                "detail": (artifact.get("error") if isinstance(artifact, dict) else "malformed"),
            }
        tree = artifact["trees"].get(req.function_signature)
        if tree is None:
            return {
                "result": "yes",
                "reason": "function_unguarded",
                "function_signature": req.function_signature,
            }

        registry = AdapterRegistry()
        for cls in (
            AccessControlAdapter,
            SafeAdapter,
            AragonACLAdapter,
            DSAuthAdapter,
            EIP1271Adapter,
            EventIndexedAdapter,
        ):
            registry.register(cls)
        ctx = EvaluationContext(
            chain_id=req.chain_id,
            contract_address=addr,
            block=req.block,
            role_grants=PostgresRoleGrantsRepo(session),
            meta={"aragon_acl_repo": PostgresAragonACLRepo(session)},
        )

        return probe_signature(
            tree,
            predicate_index=req.predicate_index,
            recovered_signer=req.recovered_signer,
            registry=registry,
            ctx=ctx,
        )


@router.get("/api/contract/{address}/capabilities")
def get_contract_capabilities(
    address: str,
    chain_id: int = 1,
    block: int | None = None,
) -> dict[str, Any]:
    """Return the v2 capability per externally-callable function on
    ``address``. Read path for the schema-v2 cutover (#18) — UI /
    external consumers query this and fall back to v1 endpoints when
    the response is 404 (legacy pre-v2 contract).

    Response shape::

        {
          "contract_address": "0x...",
          "chain_id": 1,
          "block": null,
          "capabilities": {
            "grantRole(bytes32,address)": {
              "kind": "finite_set",
              "members": ["0x..."],
              "membership_quality": "exact",
              "confidence": "enumerable",
              ...
            },
            ...
          }
        }

    Empty ``capabilities`` dict means every function on the contract is
    unguarded (publicly callable) per the resolver convention.

    Returns 404 if no completed analysis Job exists for the address, or
    no predicate_trees artifact has been written for the latest
    analysis (legacy pre-v2 contract).
    """
    from services.resolution.capability_resolver import resolve_contract_capabilities

    addr = deps._normalize_address_or_400(address)
    cache_key = (addr, chain_id, block)
    cached = _capabilities_cache_get(cache_key)
    if cached is not None:
        return cached

    with deps.SessionLocal() as session:
        capabilities = resolve_contract_capabilities(session, address=addr, chain_id=chain_id, block=block)
        if capabilities is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    "No v2 capabilities for this address — either no completed "
                    "analysis exists or it predates the schema-v2 emit. Fall "
                    "back to /api/company/* or /api/jobs?address=..."
                ),
            )
        freshness = _compute_data_freshness(session, addr, chain_id)
    response = {
        "contract_address": addr,
        "chain_id": chain_id,
        "block": block,
        "capabilities": capabilities,
        "data_freshness": freshness,
    }
    _capabilities_cache_put(cache_key, response)
    return response


@router.get(
    "/api/contract/{address}/v1_v2_diff",
    dependencies=[Depends(deps.require_admin_key)],
)
def get_contract_v1_v2_diff(address: str) -> dict[str, Any]:
    """Per-contract cutover-gate report. Loads BOTH the v1
    ``contract_analysis`` and v2 ``predicate_trees`` artifacts for the
    most recent completed analysis of ``address``, runs the diff
    harness, and returns a structured JSON report.

    Response shape::

        {
          "address": "0x...",
          "job_id": "<uuid>",
          "severity": "regression" | "new_coverage" | "role_drift" | "clean",
          "contract_name": "...",
          "agreed": [...],
          "v1_only": [...],
          "v2_only": [...],
          "role_disagreements": { fn: {v1_guard_kinds, v2_authority_roles} },
          "safe_to_cut_over": bool
        }

    Admin-gated because the diff exposes internal classifier detail not
    meant for external consumers. This is the human audit surface for
    #18.
    """
    from services.static.contract_analysis_pipeline.cutover_check import (
        cutover_check_for_address,
        is_safe_to_cut_over,
    )

    addr = deps._normalize_address_or_400(address)
    with deps.SessionLocal() as session:
        report = cutover_check_for_address(session, address=addr)
    if report is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Cannot run cutover check for this address — either no "
                "completed analysis exists, the v1 contract_analysis "
                "artifact is missing, or the v2 predicate_trees artifact "
                "is missing (legacy pre-v2 contract). Re-analyze before "
                "evaluating."
            ),
        )
    return {**report, "safe_to_cut_over": is_safe_to_cut_over(report)}


@router.get(
    "/api/v2/migration_status",
    dependencies=[Depends(deps.require_admin_key)],
)
def get_v2_migration_status(address_prefix: str | None = None, max_regressions: int = 50) -> dict[str, Any]:
    """Fleet-wide v2 cutover-readiness snapshot. Operationally
    equivalent to scripts/cutover_dry_run.py but live over HTTP so an
    operator dashboard can poll it without DB access.

    Same severity buckets as the per-contract /v1_v2_diff: clean,
    new_coverage, role_drift, regression, not_eligible. Returns counts
    + the regression / role_drift address lists + safe_to_cut_count +
    safe_pct.

    Admin-gated because the report leaks per-contract classifier
    detail.
    """
    from scripts.cutover_dry_run import run_dry_run

    with deps.SessionLocal() as session:
        report = run_dry_run(
            session,
            address_prefix=address_prefix,
            max_regressions=max_regressions,
        )
    return report


@router.get("/api/company/{company_name}/v2_capabilities")
def company_v2_capabilities(company_name: str) -> dict[str, Any]:
    """v2 capability map for every analyzed contract in a company.

    Returned as a separate endpoint (not embedded in the company-
    overview payload) because resolving capabilities requires running
    the AdapterRegistry over each contract's predicate trees + repo
    lookups — adds tens of milliseconds per contract, not free to
    include in the already-1-3MB overview response. UI consumers fetch
    this when they want to render guard details for the v2 cutover;
    otherwise they keep using the overview's v1 fields.

    Response shape::

        {
          "company": "<name>",
          "contracts": {
            "0xab...": {
              "guardedFn()": {
                "kind": "finite_set", "members": [...],
                "membership_quality": "exact",
                "confidence": "enumerable", ...
              },
              ...
            },
            "0xcd...": {...},
            "0xef...": null
          },
          "missing_v2_count": <int>
        }

    A contract with no v2 artifact maps to ``null`` so consumers can
    distinguish "not yet v2-analyzed" from "v2-analyzed and has no
    guarded functions" (the latter maps to ``{}``).

    NOT admin-gated — read-only / idempotent, the same shape contract
    as ``/api/contract/{addr}/capabilities``.
    """
    from services.resolution.capability_resolver import resolve_contract_capabilities

    with deps.SessionLocal() as session:
        protocol_row = session.execute(select(Protocol).where(Protocol.name == company_name)).scalar_one_or_none()
        if protocol_row is None:
            raise HTTPException(status_code=404, detail="Company not found")

        addresses = sorted(
            {
                (job.address or "").lower()
                for job in session.execute(
                    select(Job).where(
                        Job.protocol_id == protocol_row.id,
                        Job.status == JobStatus.completed,
                        Job.address.isnot(None),
                    )
                ).scalars()
                if job.address
            }
        )

        contracts: dict[str, Any] = {}
        missing = 0
        for addr in addresses:
            try:
                caps = resolve_contract_capabilities(session, address=addr)
            except Exception as exc:
                logger.warning(
                    "v2 capabilities resolution failed for %s in company %s: %s",
                    addr,
                    company_name,
                    exc,
                    extra={"exc_type": type(exc).__name__},
                )
                caps = None
            if caps is None:
                missing += 1
            contracts[addr] = caps

        return {
            "company": company_name,
            "contracts": contracts,
            "missing_v2_count": missing,
        }
