"""Resolver-side counterpart to ``predicate_artifacts``: given a
contract address, return the v2 capability per externally-callable
function.

This is the v2 read path the API/UI consumes during the
schema-v2 cutover (#18). It loads the persisted ``predicate_trees``
artifact (written by the static stage's
``build_predicate_artifacts`` + ``store_artifact``), wires the
Postgres-backed ``RoleGrantsRepo`` + ``AragonACLRepo`` into an
``EvaluationContext``, evaluates each function's PredicateTree
through ``evaluate_tree_with_registry`` to a ``CapabilityExpr``,
and serializes the result to a JSON-ready dict per function.

The output is the structured replacement for v1's
``access_control.privileged_functions`` — every external/public
function appears (or doesn't, if unguarded), with a typed
capability shape the resolver/UI can reason about uniformly.

Usage:

    with SessionLocal() as session:
        result = resolve_contract_capabilities(
            session, address="0x...", chain_id=1
        )
    # {
    #   "grantRole(bytes32,address)": {
    #       "kind": "finite_set",
    #       "members": ["0x..."],
    #       "membership_quality": "exact",
    #       "confidence": "enumerable",
    #       ...
    #   },
    #   ...
    # }

Returns ``None`` when the contract has no completed analysis or
no v2 artifact yet (legacy pre-v2 contract). Callers fall back to
v1 in that case.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import Contract, ControllerValue, Job, JobStatus
from db.queue import get_artifact

from .adapters import AdapterRegistry, EvaluationContext
from .adapters.access_control import AccessControlAdapter
from .adapters.aragon_acl import AragonACLAdapter, DSAuthAdapter, EIP1271Adapter
from .adapters.event_indexed import EventIndexedAdapter
from .adapters.mapping_trace import MappingTraceAdapter
from .adapters.safe import SafeAdapter
from .capabilities import CapabilityExpr
from .predicate_evaluator import evaluate_tree_with_registry
from .repos import PostgresAragonACLRepo, PostgresMappingValueRepo, PostgresRoleGrantsRepo


def resolve_contract_capabilities(
    session: Session,
    *,
    address: str,
    chain_id: int = 1,
    block: int | None = None,
    job_id: Any = None,
) -> dict[str, dict[str, Any]] | None:
    """Return ``{function_signature: capability_dict}`` for the most
    recent completed analysis of ``address``, or ``None`` if there's
    no analysis / no v2 artifact yet.

    The caller MUST keep ``session`` open for the duration of the
    call — adapters consume the repos lazily inside
    ``evaluate_tree_with_registry``.

    ``job_id`` lets in-pipeline callers (e.g. the policy worker's v2
    enrichment pass) target the job they're currently processing. The
    default ``Job.status == completed`` filter would otherwise skip
    the in-progress job and return None or stale prior artifacts.
    """
    addr = address.lower()
    if job_id is not None:
        job = session.get(Job, job_id)
        if job is None or (job.address or "").lower() != addr:
            return None
    else:
        job = session.execute(
            select(Job)
            .where(Job.address == addr)
            .where(Job.status == JobStatus.completed)
            .order_by(Job.updated_at.desc(), Job.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if job is None:
            return None

    artifact = get_artifact(session, job.id, "predicate_trees")
    if not isinstance(artifact, dict) or "trees" not in artifact:
        return None

    registry = AdapterRegistry()
    for cls in (
        AccessControlAdapter,
        SafeAdapter,
        AragonACLAdapter,
        DSAuthAdapter,
        EIP1271Adapter,
        EventIndexedAdapter,
        MappingTraceAdapter,
    ):
        registry.register(cls)

    role_grants_repo = PostgresRoleGrantsRepo(session)
    aragon_repo = PostgresAragonACLRepo(session)
    mapping_value_repo = PostgresMappingValueRepo(session)
    state_var_values = _load_state_var_values(session, addr)
    ctx = EvaluationContext(
        chain_id=chain_id,
        contract_address=addr,
        block=block,
        role_grants=role_grants_repo,
        mapping_value_repo=mapping_value_repo,
        trace_fetcher=_maybe_trace_fetcher(),
        state_var_values=state_var_values,
        session=session,
        meta={"aragon_acl_repo": aragon_repo},
    )

    out: dict[str, dict[str, Any]] = {}
    for fn_signature, tree in (artifact["trees"] or {}).items():
        cap = evaluate_tree_with_registry(tree, registry, ctx)
        out[fn_signature] = capability_to_dict(cap)
    return out


def _maybe_trace_fetcher() -> Any:
    """Lazy-construct the HyperSync trace fetcher when ``ENVIO_API_TOKEN``
    is set. Returns ``None`` otherwise so the ``MappingTraceAdapter``
    falls through. Spinning up a client just to no-op would burn a
    descriptor every time the resolver runs in a dev DB without an
    Envio token.
    """
    import os

    if not os.getenv("ENVIO_API_TOKEN"):
        return None
    try:
        from .repos.mapping_value_hypersync import HyperSyncTraceFetcher

        return HyperSyncTraceFetcher()
    except Exception:
        return None


def _load_state_var_values(session: Session, address: str) -> dict[str, str]:
    """Read persisted ``controller_values`` rows for ``address`` and key
    them by the bare state-variable name the predicate evaluator looks
    up (e.g. ``"_owner"``, ``"roleRegistry"``).

    The static pipeline writes ``controller_id`` with a
    ``"<kind>:<name>"`` prefix (e.g. ``"state_variable:_owner"``,
    ``"external_contract:roleRegistry"``). The predicate evaluator
    queries ``ctx.state_var_values[<name>]`` without the prefix, so we
    strip it on read and prefer ``state_variable:`` rows when both
    a state-variable and an external-contract row exist for the same
    name.

    Returns an empty dict when the contract has no row — the evaluator
    falls back to the lower_bound/partial placeholder."""
    contract = session.execute(select(Contract).where(Contract.address == address).limit(1)).scalar_one_or_none()
    if contract is None:
        return {}
    rows = session.execute(select(ControllerValue).where(ControllerValue.contract_id == contract.id)).scalars()
    state_var: dict[str, str] = {}
    other: dict[str, str] = {}
    for row in rows:
        cid = row.controller_id or ""
        value = row.value
        if not cid or not value:
            continue
        if ":" in cid:
            kind, _, name = cid.partition(":")
        else:
            kind, name = "", cid
        if not name:
            continue
        if kind == "state_variable":
            state_var[name] = value
        else:
            other.setdefault(name, value)
    # state_variable rows win; external_contract / role_identifier rows
    # fill in only when there's no direct state-variable value.
    return {**other, **state_var}


def capability_to_dict(cap: CapabilityExpr) -> dict[str, Any]:
    """Serialize a ``CapabilityExpr`` to a JSON-ready dict.

    Recurses through ``children`` (AND / OR composition) and
    ``signer`` (signature_witness wraps another CapabilityExpr).
    Drops keys whose value is the dataclass default (None / empty
    list) so the wire shape is compact.
    """
    if not is_dataclass(cap):
        return {}
    out: dict[str, Any] = {"kind": cap.kind}
    if cap.members is not None:
        out["members"] = list(cap.members)
    if cap.threshold is not None:
        m, signers = cap.threshold
        out["threshold"] = {"m": m, "signers": list(signers)}
    if cap.blacklist is not None:
        out["blacklist"] = list(cap.blacklist)
    if cap.signer is not None:
        out["signer"] = capability_to_dict(cap.signer)
    if cap.check is not None:
        out["check"] = asdict(cap.check)
    if cap.conditions:
        out["conditions"] = [asdict(c) if is_dataclass(c) else dict(c) for c in cap.conditions]
    if cap.unsupported_reason is not None:
        out["unsupported_reason"] = cap.unsupported_reason
    if cap.children:
        out["children"] = [capability_to_dict(c) for c in cap.children]
    out["membership_quality"] = cap.membership_quality
    out["confidence"] = cap.confidence
    if cap.last_indexed_block is not None:
        out["last_indexed_block"] = cap.last_indexed_block
    return out
