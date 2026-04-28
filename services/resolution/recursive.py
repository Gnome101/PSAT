"""Recursively resolve contract control chains into a reusable graph artifact."""

from __future__ import annotations

import copy
import logging
import os
import re
import tempfile
import threading
import time as _time
from collections import deque
from pathlib import Path
from typing import Any, TypedDict, cast

from typing_extensions import NotRequired

from schemas.contract_analysis import ContractAnalysis
from schemas.control_tracking import ControlSnapshot
from schemas.resolved_control_graph import ResolvedControlGraph, ResolvedGraphEdge, ResolvedGraphNode
from services.discovery.fetch import fetch, scaffold
from services.policy.effective_permissions import build_effective_permissions
from services.static import collect_contract_analysis

from .tracking import (
    build_control_snapshot,
    classify_resolved_address,
    classify_resolved_address_with_status,
)
from .tracking_plan import build_control_tracking_plan

logger = logging.getLogger(__name__)

ANALYZABLE_TYPES = {"contract", "timelock", "proxy_admin"}
DEFAULT_RECURSION_MAX_DEPTH = int(os.getenv("PSAT_RECURSION_MAX_DEPTH", "6"))

# Process-wide cache for the static parts of _materialize_contract_artifacts (analysis + plan + contract_name).
# Address index keyed by (rpc_url, address) for chain isolation; keccak fallback enables cross-chain reuse on byte-exact
# bytecode matches.
_ARTIFACT_CACHE: dict[tuple[str, str], tuple[str, dict[str, Any], dict[str, Any], float]] = {}
_ARTIFACT_CACHE_BY_KECCAK: dict[str, tuple[str, dict[str, Any], dict[str, Any], float]] = {}
_ARTIFACT_CACHE_LOCK = threading.Lock()
_ARTIFACT_CACHE_MAX = 1024
_ARTIFACT_CACHE_TTL_S = float(os.getenv("PSAT_RESOLUTION_ARTIFACT_CACHE_TTL_S", "1800"))


def clear_artifact_cache() -> None:
    """Clear the process-wide static-artifact cache. For tests + manual reset."""
    from utils.memory import reset_cache_pressure_state

    with _ARTIFACT_CACHE_LOCK:
        _ARTIFACT_CACHE.clear()
        _ARTIFACT_CACHE_BY_KECCAK.clear()
    reset_cache_pressure_state("artifact_addr")
    reset_cache_pressure_state("artifact_keccak")


def _log_artifact_pressure() -> None:
    """Log when either artifact cache crosses 50/75/95% (caller holds the lock)."""
    from utils.memory import cache_pressure_message

    msg = cache_pressure_message("artifact_addr", len(_ARTIFACT_CACHE), _ARTIFACT_CACHE_MAX)
    if msg:
        logger.info("[CACHE_PRESSURE] %s", msg)
    msg = cache_pressure_message("artifact_keccak", len(_ARTIFACT_CACHE_BY_KECCAK), _ARTIFACT_CACHE_MAX)
    if msg:
        logger.info("[CACHE_PRESSURE] %s", msg)


def _get_cached_static_artifacts(
    effective_address: str, rpc_url: str = "", bytecode_keccak: str | None = None
) -> tuple[str, dict[str, Any], dict[str, Any]] | None:
    """Cached ``(contract_name, analysis, plan)`` deepcopies; chain-isolated by addr, keccak for cross-chain reuse."""
    addr_key = (rpc_url, effective_address.lower())
    now = _time.monotonic()
    with _ARTIFACT_CACHE_LOCK:
        entry = _ARTIFACT_CACHE.get(addr_key)
        if entry is None and bytecode_keccak:
            entry = _ARTIFACT_CACHE_BY_KECCAK.get(bytecode_keccak)
    if entry is None:
        return None
    contract_name, analysis, plan, inserted_at = entry
    if now - inserted_at > _ARTIFACT_CACHE_TTL_S:
        with _ARTIFACT_CACHE_LOCK:
            _ARTIFACT_CACHE.pop(addr_key, None)
            if bytecode_keccak:
                _ARTIFACT_CACHE_BY_KECCAK.pop(bytecode_keccak, None)
        return None
    return contract_name, copy.deepcopy(analysis), copy.deepcopy(plan)


def _store_cached_static_artifacts(
    effective_address: str,
    contract_name: str,
    analysis: dict[str, Any],
    plan: dict[str, Any],
    rpc_url: str = "",
    bytecode_keccak: str | None = None,
) -> None:
    addr_key = (rpc_url, effective_address.lower())
    with _ARTIFACT_CACHE_LOCK:
        if len(_ARTIFACT_CACHE) >= _ARTIFACT_CACHE_MAX:
            oldest_key = min(_ARTIFACT_CACHE, key=lambda k: _ARTIFACT_CACHE[k][3])
            _ARTIFACT_CACHE.pop(oldest_key, None)
        payload = (
            contract_name,
            copy.deepcopy(analysis),
            copy.deepcopy(plan),
            _time.monotonic(),
        )
        _ARTIFACT_CACHE[addr_key] = payload
        if bytecode_keccak:
            if len(_ARTIFACT_CACHE_BY_KECCAK) >= _ARTIFACT_CACHE_MAX:
                oldest = min(_ARTIFACT_CACHE_BY_KECCAK, key=lambda k: _ARTIFACT_CACHE_BY_KECCAK[k][3])
                _ARTIFACT_CACHE_BY_KECCAK.pop(oldest, None)
            _ARTIFACT_CACHE_BY_KECCAK[bytecode_keccak] = payload
        _log_artifact_pressure()


class LoadedArtifacts(TypedDict):
    """Per-contract artifact bundle emitted by ``resolve_control_graph`` and persisted by the worker as DB artifacts."""

    analysis: dict[str, Any]
    tracking_plan: dict[str, Any]
    snapshot: ControlSnapshot
    effective_permissions: NotRequired[dict[str, Any] | None]


class PendingContract(TypedDict):
    address: str
    depth: int
    artifacts: NotRequired[LoadedArtifacts]


class RolePrincipalAccumulator(TypedDict):
    address: str
    resolved_type: str
    details: dict[str, object]
    roles: set[int]
    functions: set[str]


class RolePrincipal(TypedDict):
    address: str
    resolved_type: str
    details: dict[str, object]
    roles: list[int]
    functions: list[str]


def _address_node_id(address: str) -> str:
    return f"address:{address.lower()}"


def _sanitize_name(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "_", value).strip("_")
    return cleaned or "contract"


def _workspace_name(contract_name: str, address: str, prefix: str) -> str:
    return f"{_sanitize_name(prefix)}_{_sanitize_name(contract_name)}_{address.lower()[2:10]}"


def _contract_name_for_address(address: str) -> str | None:
    try:
        result = fetch(address)
    except Exception:
        return None
    if not isinstance(result, dict):
        return None
    name = str(result.get("ContractName", "")).strip()
    return name or None


def _build_effective_permissions(
    analysis: dict[str, Any],
    snapshot: ControlSnapshot,
) -> dict[str, Any] | None:
    """Compute the effective-permissions payload (matches the legacy ``effective_permissions.json`` shape)."""
    try:
        return cast(
            dict,
            build_effective_permissions(
                analysis,
                target_snapshot=cast(dict, snapshot),
                principal_resolution={"status": "no_authority", "reason": "No non-zero authority found"},
            ),
        )
    except Exception as exc:
        logger.debug("Recursive resolve: effective_permissions build failed: %s", exc)
        return None


def _materialize_contract_artifacts(
    address: str,
    rpc_url: str,
    *,
    workspace_prefix: str,
) -> LoadedArtifacts:
    """Build analysis + plan + snapshot + effective permissions in memory (tempdir cleaned up before return)."""
    # Proxy check — analyze the implementation but read storage from the proxy.
    effective_address = address
    snapshot_address = address
    try:
        from services.discovery.classifier import classify_single

        classification = classify_single(address, rpc_url)
        if classification.get("type") == "proxy":
            impl = classification.get("implementation")
            if impl:
                logger.info("Recursive resolve: %s is a proxy, using impl %s", address, impl)
                effective_address = impl
    except Exception as exc:
        logger.debug("Recursive resolve: proxy check failed for %s: %s", address, exc)

    # Cross-cascade reuse of static artifacts; indexed by address AND bytecode keccak so identical-bytecode contracts
    # share one cache slot.
    bytecode_keccak: str | None = None
    try:
        from utils.rpc import get_code_with_keccak

        _code, bytecode_keccak = get_code_with_keccak(rpc_url, effective_address)
    except Exception as exc:
        logger.debug("Recursive resolve: get_code_with_keccak failed for %s: %s", effective_address, exc)
    cached = _get_cached_static_artifacts(effective_address, rpc_url=rpc_url, bytecode_keccak=bytecode_keccak)
    if cached is not None:
        contract_name, analysis, plan = cached
        # Retarget address fields on a keccak hit so build_control_snapshot reads from THIS contract.
        analysis = copy.deepcopy(analysis)
        plan = copy.deepcopy(plan)
        if isinstance(analysis.get("subject"), dict):
            analysis["subject"]["address"] = effective_address
        plan["contract_address"] = effective_address
    else:
        result = fetch(effective_address)
        contract_name = str(result.get("ContractName", "Contract"))
        project_name = _workspace_name(contract_name, effective_address, workspace_prefix)

        with tempfile.TemporaryDirectory(prefix=f"psat_{workspace_prefix}_") as tmp:
            project_dir = Path(tmp) / project_name
            scaffold(effective_address, result, project_dir)
            analysis = cast(dict, collect_contract_analysis(project_dir))

        plan = cast(dict, build_control_tracking_plan(cast(ContractAnalysis, analysis)))
        _store_cached_static_artifacts(
            effective_address, contract_name, analysis, plan, rpc_url=rpc_url, bytecode_keccak=bytecode_keccak
        )
    if snapshot_address != effective_address:
        plan = {**plan, "contract_address": snapshot_address}

    snapshot = build_control_snapshot(cast(Any, plan), rpc_url)
    effective_permissions = _build_effective_permissions(cast(dict, analysis), snapshot)

    return {
        "analysis": cast(dict, analysis),
        "tracking_plan": plan,
        "snapshot": snapshot,
        "effective_permissions": effective_permissions,
    }


def _ensure_node(
    nodes: dict[str, ResolvedGraphNode],
    *,
    address: str,
    resolved_type: str,
    label: str,
    depth: int,
    node_type: str,
    contract_name: str | None = None,
    analyzed: bool = False,
    details: dict[str, object] | None = None,
    artifacts: dict[str, str] | None = None,
) -> str:
    normalized = address.lower()
    node_id = _address_node_id(normalized)
    current = nodes.get(node_id)
    payload: ResolvedGraphNode = {
        "id": node_id,
        "address": normalized,
        "node_type": node_type,
        "resolved_type": resolved_type,  # type: ignore[typeddict-item]
        "label": label,
        "contract_name": contract_name,
        "depth": depth,
        "analyzed": analyzed,
        "details": details or {},
        "artifacts": artifacts or {},
    }
    if current is None:
        nodes[node_id] = payload
        return node_id

    current["depth"] = min(current.get("depth", depth), depth)
    if contract_name:
        current["contract_name"] = contract_name
    if analyzed:
        current["analyzed"] = True
        current["node_type"] = "contract"
    if resolved_type != "unknown" or not current.get("resolved_type"):
        current["resolved_type"] = resolved_type  # type: ignore[typeddict-item]
    if label:
        current["label"] = label
    if details:
        merged_details = dict(current.get("details", {}))
        merged_details.update(details)
        current["details"] = merged_details
    if artifacts:
        merged_artifacts = dict(current.get("artifacts", {}))
        merged_artifacts.update(artifacts)
        current["artifacts"] = merged_artifacts
    return node_id


def _edge_key(edge: ResolvedGraphEdge) -> tuple:
    relation = edge["relation"]
    # Nested holder edges often appear via multiple upstream controller paths; keep one edge and merge notes.
    if relation in {"safe_owner", "timelock_owner", "proxy_admin_owner"}:
        return (
            edge["from_id"],
            edge["to_id"],
            relation,
            edge.get("label"),
        )
    return (
        edge["from_id"],
        edge["to_id"],
        relation,
        edge.get("label"),
        edge.get("source_controller_id"),
    )


def _add_edge(edges: dict[tuple, ResolvedGraphEdge], edge: ResolvedGraphEdge) -> None:
    key = _edge_key(edge)
    if key in edges:
        existing_notes = set(edges[key].get("notes", []))
        existing_notes.update(edge.get("notes", []))
        edges[key]["notes"] = sorted(existing_notes)
        return
    edges[key] = edge


def _nested_principals_for_details(resolved_type: str, details: dict[str, object]) -> list[tuple[str, str, str]]:
    principals: list[tuple[str, str, str]] = []
    if resolved_type == "safe":
        owners = details.get("owners")
        for owner in owners if isinstance(owners, list) else []:
            if isinstance(owner, str) and owner.startswith("0x"):
                principals.append((owner.lower(), "safe_owner", "safe owner"))
    elif resolved_type == "timelock":
        owner = details.get("owner")
        if isinstance(owner, str) and owner.startswith("0x"):
            principals.append((owner.lower(), "timelock_owner", "timelock owner"))
    elif resolved_type == "proxy_admin":
        owner = details.get("owner")
        if isinstance(owner, str) and owner.startswith("0x"):
            principals.append((owner.lower(), "proxy_admin_owner", "proxy admin owner"))
    return principals


def _role_principals_from_effective_permissions(effective_permissions: dict[str, Any]) -> list[RolePrincipal]:
    principals: dict[str, RolePrincipalAccumulator] = {}
    for function in effective_permissions.get("functions", []):
        if not isinstance(function, dict):
            continue
        function_signature = str(function.get("function", ""))
        for role_grant in function.get("authority_roles", []):
            if not isinstance(role_grant, dict):
                continue
            role = int(role_grant["role"])
            for principal in role_grant.get("principals", []):
                if not isinstance(principal, dict):
                    continue
                address = str(principal.get("address", "")).lower()
                if not address.startswith("0x"):
                    continue
                details_raw = principal.get("details", {})
                details = dict(details_raw) if isinstance(details_raw, dict) else {}
                payload = principals.setdefault(
                    address,
                    {
                        "address": address,
                        "resolved_type": str(principal.get("resolved_type", "unknown")),
                        "details": details,
                        "roles": set(),
                        "functions": set(),
                    },
                )
                payload["roles"].add(role)
                if function_signature:
                    payload["functions"].add(function_signature)
                if payload.get("resolved_type") in {None, "", "unknown"} and principal.get("resolved_type"):
                    payload["resolved_type"] = str(principal.get("resolved_type"))
                merged_details = dict(payload["details"])
                merged_details.update(details)
                payload["details"] = merged_details

        for controller in function.get("controllers", []):
            if not isinstance(controller, dict):
                continue
            controller_label = str(controller.get("label") or controller.get("source") or "controller")
            for principal in controller.get("principals", []):
                if not isinstance(principal, dict):
                    continue
                address = str(principal.get("address", "")).lower()
                if not address.startswith("0x"):
                    continue
                details_raw = principal.get("details", {})
                details = dict(details_raw) if isinstance(details_raw, dict) else {}
                payload = principals.setdefault(
                    address,
                    {
                        "address": address,
                        "resolved_type": str(principal.get("resolved_type", "unknown")),
                        "details": details,
                        "roles": set(),
                        "functions": set(),
                    },
                )
                if function_signature:
                    payload["functions"].add(function_signature)
                if payload.get("resolved_type") in {None, "", "unknown"} and principal.get("resolved_type"):
                    payload["resolved_type"] = str(principal.get("resolved_type"))
                merged_details = dict(payload["details"])
                merged_details.update(details)
                merged_details.setdefault("controller_label", controller_label)
                payload["details"] = merged_details

    serialized: list[RolePrincipal] = []
    for payload in principals.values():
        serialized.append(
            {
                "address": payload["address"],
                "resolved_type": payload["resolved_type"],
                "details": dict(payload["details"]),
                "roles": sorted(payload["roles"]),
                "functions": sorted(payload["functions"]),
            }
        )
    return sorted(serialized, key=lambda item: str(item["address"]))


def _maybe_queue_address(
    queue: deque[PendingContract], queued: set[str], address: str, depth: int, max_depth: int
) -> None:
    if address in queued or depth > max_depth:
        return
    queue.append({"address": address, "depth": depth})
    queued.add(address)


def _add_nested_principals(
    *,
    nodes: dict[str, ResolvedGraphNode],
    edges: dict[tuple, ResolvedGraphEdge],
    queue: deque[PendingContract],
    queued: set[str],
    rpc_url: str,
    from_node_id: str,
    source_controller_id: str | None,
    resolved_type: str,
    details: dict[str, object],
    depth: int,
    max_depth: int,
    classify_fn: Any | None = None,
) -> None:
    for nested_address, relation, label in _nested_principals_for_details(resolved_type, details):
        classify = classify_fn or (lambda addr: classify_resolved_address(rpc_url, addr))
        nested_type, nested_details = classify(nested_address)
        nested_node_type = "contract" if nested_type in ANALYZABLE_TYPES else "principal"
        nested_node_id = _ensure_node(
            nodes,
            address=nested_address,
            resolved_type=nested_type,
            label=label,
            depth=depth + 1,
            node_type=nested_node_type,
            details=nested_details,
        )
        _add_edge(
            edges,
            {
                "from_id": from_node_id,
                "to_id": nested_node_id,
                "relation": relation,  # type: ignore[typeddict-item]
                "label": label,
                "source_controller_id": source_controller_id,
                "notes": [],
            },
        )
        if nested_type in ANALYZABLE_TYPES:
            _maybe_queue_address(queue, queued, nested_address, depth + 1, max_depth)


def resolve_control_graph(
    *,
    root_artifacts: LoadedArtifacts,
    rpc_url: str,
    max_depth: int = DEFAULT_RECURSION_MAX_DEPTH,
    workspace_prefix: str = "recursive",
    nested_artifacts_override: dict[str, LoadedArtifacts] | None = None,
    classify_cache: dict[str, tuple[str, dict[str, object]]] | None = None,
    initial_graph: ResolvedControlGraph | None = None,
) -> tuple[ResolvedControlGraph, dict[str, LoadedArtifacts]]:
    """BFS the control chain. Returns ``(graph, nested_artifacts_by_address)``; classify_cache is mutated in place."""
    root_analysis = root_artifacts["analysis"]
    root_subject = root_analysis.get("subject", {})
    root_address = str(root_subject.get("address", "")).lower()

    queue: deque[PendingContract] = deque(
        [
            {
                "address": root_address,
                "depth": 0,
                "artifacts": root_artifacts,
            }
        ]
    )
    queued = {root_address}
    processed: set[str] = set()
    _classify_cache: dict[str, tuple[str, dict[str, object]]] = classify_cache if classify_cache is not None else {}
    nested_artifacts: dict[str, LoadedArtifacts] = dict(nested_artifacts_override or {})

    def _cached_classify(addr: str) -> tuple[str, dict[str, object]]:
        key = addr.lower()
        if key in _classify_cache:
            return _classify_cache[key]
        kind, details, cacheable = classify_resolved_address_with_status(rpc_url, addr)
        # Skip caching transient RPC errors — otherwise a "contract" fallback gets cemented in the persisted
        # classified_addresses artifact.
        if cacheable:
            _classify_cache[key] = (kind, details)
        return kind, details

    nodes: dict[str, ResolvedGraphNode] = {}
    edges: dict[tuple, ResolvedGraphEdge] = {}

    # Pre-seed the graph from a prior walk so the policy refresh path skips re-analyzing already-processed nested
    # contracts.
    if initial_graph is not None:
        for node in initial_graph.get("nodes", []):
            if not isinstance(node, dict):
                continue
            node_id = node.get("id")
            if isinstance(node_id, str):
                nodes[node_id] = cast(ResolvedGraphNode, dict(node))
        for edge in initial_graph.get("edges", []):
            if not isinstance(edge, dict):
                continue
            edges[_edge_key(cast(ResolvedGraphEdge, edge))] = cast(ResolvedGraphEdge, dict(edge))
        # Mark already-analyzed nested contracts as processed; the root must re-walk so freshly-computed role principals
        # get projected.
        for node in initial_graph.get("nodes", []):
            if not isinstance(node, dict) or not node.get("analyzed"):
                continue
            node_address = (node.get("details") or {}).get("address")
            if isinstance(node_address, str):
                addr = node_address.lower()
                if addr and addr != root_address:
                    processed.add(addr)

    while queue:
        pending = queue.popleft()
        address = pending["address"]
        depth = pending["depth"]
        if address in processed or depth > max_depth:
            continue

        preloaded = pending.get("artifacts")
        if preloaded is not None:
            artifacts = preloaded
        elif address in nested_artifacts:
            artifacts = nested_artifacts[address]
        else:
            try:
                artifacts = _materialize_contract_artifacts(
                    address,
                    rpc_url,
                    workspace_prefix=workspace_prefix,
                )
            except Exception as exc:
                contract_name = _contract_name_for_address(address)
                logger.warning(
                    "Recursive resolve: failed to materialize nested contract %s at depth %s: %s",
                    address,
                    depth,
                    exc,
                )
                _ensure_node(
                    nodes,
                    address=address,
                    resolved_type="contract",
                    label=contract_name or address,
                    depth=depth,
                    node_type="contract",
                    analyzed=False,
                    contract_name=contract_name,
                    details={"address": address, "materialize_error": str(exc)},
                )
                processed.add(address)
                continue
            nested_artifacts[address] = artifacts

        processed.add(address)
        analysis = artifacts["analysis"]
        snapshot = artifacts["snapshot"]
        effective_permissions = artifacts.get("effective_permissions")
        subject = analysis.get("subject", {})
        contract_name = str(subject.get("name", address))
        # Carry `method_to_role` onto the graph node so the policy stage can resolve authority method names without
        # keyword heuristics.
        access_control_block = analysis.get("access_control") or {}
        method_to_role = access_control_block.get("method_to_role") or {}
        node_details: dict[str, object] = {"address": address}
        if method_to_role:
            node_details["method_to_role"] = dict(method_to_role)
        contract_node_id = _ensure_node(
            nodes,
            address=address,
            resolved_type="contract",
            label=contract_name,
            depth=depth,
            node_type="contract",
            contract_name=contract_name,
            analyzed=True,
            details=node_details,
            artifacts={"data_key": f"recursive:{address.lower()}"},
        )

        # Replay mapping-allowlist writer events into principal nodes; bounded enumeration surfaces truncation via the
        # `status` field.
        mapping_specs = list(access_control_block.get("mapping_writer_events") or [])
        enumerated: list[Any] = []
        enumeration_status = "skipped"
        if mapping_specs:
            hypersync_token = os.getenv("ENVIO_API_TOKEN") or ""
            logger.info(
                "mapping_enumerator: %s has %d writer-event specs, token=%s",
                address,
                len(mapping_specs),
                "present" if hypersync_token else "missing",
            )
            if hypersync_token:
                from services.resolution.mapping_enumerator import enumerate_mapping_allowlist_sync

                try:
                    result = enumerate_mapping_allowlist_sync(
                        address,
                        mapping_specs,
                        bearer_token=hypersync_token,
                    )
                except Exception as exc:
                    # Bounds are inside enumerate_mapping_allowlist; raises here are unexpected (auth, hypersync load,
                    # etc).
                    logger.warning(
                        "mapping_enumerator UNEXPECTED FAILURE for %s: %s — treating as truncated",
                        address,
                        exc,
                    )
                    enumeration_status = "error"
                else:
                    enumerated = list(result["principals"])
                    enumeration_status = result["status"]
                    if enumeration_status != "complete":
                        logger.warning(
                            "mapping_enumerator: %s INCOMPLETE status=%s pages=%d last_block=%d "
                            "(principal set may be missing entries)",
                            address,
                            enumeration_status,
                            result["pages_fetched"],
                            result["last_block_scanned"],
                        )
                    logger.info(
                        "mapping_enumerator: %s returned %d principals (status=%s)",
                        address,
                        len(enumerated),
                        enumeration_status,
                    )
            for principal in enumerated:
                member_addr = principal["address"]
                _ensure_node(
                    nodes,
                    address=member_addr,
                    resolved_type="unknown",
                    label=principal["mapping_name"],
                    depth=depth + 1,
                    node_type="principal",
                    analyzed=False,
                    details={
                        "address": member_addr,
                        "controller_label": principal["mapping_name"],
                        "mapping_name": principal["mapping_name"],
                        "last_seen_block": principal["last_seen_block"],
                        "direction_history": principal["direction_history"],
                    },
                )
                _add_edge(
                    edges,
                    {
                        "from_id": contract_node_id,
                        "to_id": _address_node_id(member_addr),
                        "relation": "mapping_member",
                        "label": principal["mapping_name"],
                        "source_controller_id": f"mapping:{principal['mapping_name']}",
                        "notes": [],
                    },
                )
            # Surface enumeration status on the node so downstream stages can flag incomplete allowlists.
            if mapping_specs and contract_node_id in nodes:
                nodes[contract_node_id]["details"]["mapping_enumeration_status"] = enumeration_status

        for controller_id, controller_value in snapshot.get("controller_values", {}).items():
            controller_address = str(controller_value.get("value", "")).lower()
            if not controller_address.startswith("0x") or len(controller_address) != 42:
                continue
            resolved_type = str(controller_value.get("resolved_type", "unknown"))
            details = dict(controller_value.get("details", {}))
            controller_label = str(controller_value.get("source", controller_id))
            controller_node_type = "contract" if resolved_type in ANALYZABLE_TYPES else "principal"
            controller_node_id = _ensure_node(
                nodes,
                address=controller_address,
                resolved_type=resolved_type,
                label=controller_label,
                depth=depth + 1,
                node_type=controller_node_type,
                details=details,
            )
            _add_edge(
                edges,
                {
                    "from_id": contract_node_id,
                    "to_id": controller_node_id,
                    "relation": "controller_value",
                    "label": controller_label,
                    "source_controller_id": controller_id,
                    "notes": [f"resolved_type={resolved_type}"],
                },
            )

            if resolved_type in ANALYZABLE_TYPES:
                _maybe_queue_address(queue, queued, controller_address, depth + 1, max_depth)

            _add_nested_principals(
                nodes=nodes,
                edges=edges,
                queue=queue,
                queued=queued,
                rpc_url=rpc_url,
                from_node_id=controller_node_id,
                source_controller_id=controller_id,
                resolved_type=resolved_type,
                details=details,
                depth=depth + 1,
                max_depth=max_depth,
                classify_fn=_cached_classify,
            )

        for principal_value in _role_principals_from_effective_permissions(effective_permissions or {}):
            principal_address = str(principal_value["address"]).lower()
            if principal_address == address:
                continue
            resolved_type = str(principal_value.get("resolved_type", "unknown"))
            details = dict(principal_value["details"])
            if resolved_type == "unknown":
                resolved_type, classified_details = _cached_classify(principal_address)
                merged_details = dict(details)
                merged_details.update(classified_details)
                details = merged_details

            node_type = "contract" if resolved_type in ANALYZABLE_TYPES else "principal"
            principal_node_id = _ensure_node(
                nodes,
                address=principal_address,
                resolved_type=resolved_type,
                label="role principal",
                depth=depth + 1,
                node_type=node_type,
                details=details,
            )
            roles = principal_value["roles"]
            functions = principal_value["functions"]
            _add_edge(
                edges,
                {
                    "from_id": contract_node_id,
                    "to_id": principal_node_id,
                    "relation": "role_principal",
                    "label": f"roles {','.join(str(role) for role in roles)}" if roles else "role principal",
                    "source_controller_id": None,
                    "notes": [f"functions={len(functions)}", *(f"role={role}" for role in roles)],
                },
            )
            if resolved_type in ANALYZABLE_TYPES:
                _maybe_queue_address(queue, queued, principal_address, depth + 1, max_depth)
            _add_nested_principals(
                nodes=nodes,
                edges=edges,
                queue=queue,
                queued=queued,
                rpc_url=rpc_url,
                from_node_id=principal_node_id,
                source_controller_id=None,
                resolved_type=resolved_type,
                details=details,
                depth=depth + 1,
                max_depth=max_depth,
                classify_fn=_cached_classify,
            )

    graph: ResolvedControlGraph = {
        "schema_version": "0.1",
        "root_contract_address": root_address,
        "max_depth": max_depth,
        "nodes": sorted(nodes.values(), key=lambda item: item["id"]),
        "edges": sorted(edges.values(), key=lambda item: (item["from_id"], item["relation"], item["to_id"])),
    }
    return graph, nested_artifacts
