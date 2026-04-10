"""Static analysis worker — runs Slither and contract analysis in a temp directory."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import tempfile
import textwrap
import time
from pathlib import Path

from sqlalchemy import select

from db.models import Contract, ContractSummary, Job, JobStage, PrivilegedFunction, RoleDefinition, SlitherFinding
from db.queue import create_job, get_artifact, get_source_files, store_artifact
from services.monitoring.proxy_watcher import resolve_current_implementation
from utils.rpc import normalize_hex  # used for address comparison
from services.discovery import (
    build_unified_dependencies,
    classify_contracts,
    enrich_dependency_metadata,
    find_dependencies,
    find_dynamic_dependencies,
    write_dependency_visualization,
)
from services.discovery.dynamic_dependencies import NoNewTransactionsError
from services.resolution.tracking_plan import build_control_tracking_plan_from_file
from services.static import analyze, analyze_contract
from services.static.vyper_analysis import is_vyper_project
from workers.base import DEBUG_TIMING, BaseWorker, JobHandledDirectly

logger = logging.getLogger("workers.static_worker")

# ---------------------------------------------------------------------------
# Error logging template
# ---------------------------------------------------------------------------
_ERROR_TEMPLATE = """
================== STATIC WORKER ERROR ==================
Job:      {job_id}
Address:  {address}
Contract: {contract_name}
Phase:    {phase}
----------------------------------------------------------
{error}
==========================================================
""".strip()


def _log_phase_error(job_id: str, address: str, contract_name: str, phase: str, error: str) -> None:
    logger.error(
        _ERROR_TEMPLATE.format(
            job_id=job_id,
            address=address,
            contract_name=contract_name,
            phase=phase,
            error=error,
        )
    )


# ---------------------------------------------------------------------------
# Dynamic dependency merge helper
# ---------------------------------------------------------------------------


def _merge_dynamic_deps(prev: dict, new: dict) -> dict:
    """Merge previous and new dynamic dependency results (append-only).

    Unions dependencies, provenance, edges, transactions, trace methods,
    and trace errors — deduplicating where appropriate.
    """
    # Union dependencies (sorted, deduplicated)
    prev_deps = set(prev.get("dependencies", []))
    new_deps = set(new.get("dependencies", []))
    merged_deps = sorted(prev_deps | new_deps)

    # Union provenance dicts (merge per-address lists, deduplicate)
    merged_provenance: dict[str, list[dict]] = {}
    for prov_dict in [prev.get("provenance", {}), new.get("provenance", {})]:
        for addr, records in prov_dict.items():
            existing = merged_provenance.setdefault(addr, [])
            for record in records:
                if record not in existing:
                    existing.append(record)

    # Union dependency_graph edges (deduplicate by from+to+op+selector)
    seen_edges: set[tuple[str, str, str, str]] = set()
    merged_graph: list[dict] = []
    for graph_list in [prev.get("dependency_graph", []), new.get("dependency_graph", [])]:
        for edge in graph_list:
            key = (edge["from"], edge["to"], edge["op"], edge.get("selector", ""))
            if key in seen_edges:
                # Merge provenance into existing edge
                for existing_edge in merged_graph:
                    existing_key = (
                        existing_edge["from"],
                        existing_edge["to"],
                        existing_edge["op"],
                        existing_edge.get("selector", ""),
                    )
                    if existing_key == key:
                        for prov in edge.get("provenance", []):
                            if prov not in existing_edge.get("provenance", []):
                                existing_edge.setdefault("provenance", []).append(prov)
                        break
                continue
            seen_edges.add(key)
            merged_graph.append(dict(edge))

    # Concatenate transactions_analyzed (deduplicate by tx_hash)
    seen_tx_hashes: set[str] = set()
    merged_txs: list[dict] = []
    for tx_list in [prev.get("transactions_analyzed", []), new.get("transactions_analyzed", [])]:
        for tx in tx_list:
            tx_hash = tx.get("tx_hash", "")
            if tx_hash not in seen_tx_hashes:
                seen_tx_hashes.add(tx_hash)
                merged_txs.append(tx)

    # Union trace_methods
    merged_methods = sorted(set(prev.get("trace_methods", [])) | set(new.get("trace_methods", [])))

    # Concatenate trace_errors (deduplicate by tx_hash)
    seen_error_hashes: set[str] = set()
    merged_errors: list[dict] = []
    for err_list in [prev.get("trace_errors", []), new.get("trace_errors", [])]:
        for err in err_list:
            err_hash = err.get("tx_hash", "")
            if err_hash not in seen_error_hashes:
                seen_error_hashes.add(err_hash)
                merged_errors.append(err)

    return {
        "address": new.get("address") or prev.get("address"),
        "rpc": new.get("rpc") or prev.get("rpc"),
        "transactions_analyzed": merged_txs,
        "trace_methods": merged_methods,
        "dependencies": merged_deps,
        "provenance": merged_provenance,
        "dependency_graph": merged_graph,
        "trace_errors": merged_errors,
    }


def _resolve_dynamic_deps(
    session, job, address: str, dynamic_rpc: str | None, tx_limit: int,
    tx_hashes: list[str] | None, proxy_addr: str | None,
    code_cache: dict[str, str],
) -> tuple[dict | None, str | None]:
    """Load cached dynamic deps, discover new ones, merge, and persist.

    Returns ``(dyn_output, error_string)``.  On success *error_string* is
    ``None``.  When previous deps exist and no new transactions are found,
    the previous output is returned as-is (not an error).
    """
    request = job.request if isinstance(job.request, dict) else {}

    # --- Load previous dynamic deps for append-only merge ---
    # The artifact is either on this job already (copied by copy_static_cache
    # as a seed artifact, or stored by a previous attempt of this job).
    prev_dyn: dict | None = None
    if not tx_hashes:
        prev_dyn = get_artifact(session, job.id, "dynamic_dependencies")
        if prev_dyn is not None and not isinstance(prev_dyn, dict):
            prev_dyn = None

    # --- Compute start_block for incremental fetch ---
    start_block: int | None = None
    if prev_dyn:
        prev_txs = prev_dyn.get("transactions_analyzed", [])
        last_block = max((tx.get("block_number") or 0 for tx in prev_txs), default=0)
        if last_block > 0:
            start_block = last_block + 1

    # --- Discover ---
    try:
        dyn_output = find_dynamic_dependencies(
            address,
            rpc_url=dynamic_rpc,
            tx_limit=tx_limit,
            tx_hashes=tx_hashes,
            proxy_address=proxy_addr,
            code_cache=code_cache,
            start_block=start_block,
        )
        if prev_dyn and not tx_hashes:
            dyn_output = _merge_dynamic_deps(prev_dyn, dyn_output)
        store_artifact(session, job.id, "dynamic_dependencies", data=dyn_output)
        return dyn_output, None
    except NoNewTransactionsError:
        if prev_dyn:
            store_artifact(session, job.id, "dynamic_dependencies", data=prev_dyn)
            return prev_dyn, None
        return None, "No representative transactions found"
    except Exception as exc:
        return None, str(exc)


# ---------------------------------------------------------------------------
# Upgrade history merge helper
# ---------------------------------------------------------------------------


def _merge_upgrade_history(prev: dict, new: dict) -> dict:
    """Merge previous and new upgrade history results (append-only).

    For each proxy present in both, events are unioned (deduplicated by
    block_number + tx_hash + event_type) and timelines rebuilt.  Proxies
    appearing in only one side are kept as-is.
    """
    from services.discovery.upgrade_history import _build_implementation_timeline

    merged_proxies: dict[str, dict] = {}

    all_proxy_addrs = set(prev.get("proxies", {}).keys()) | set(new.get("proxies", {}).keys())

    total_upgrades = 0
    for addr in all_proxy_addrs:
        prev_proxy = prev.get("proxies", {}).get(addr)
        new_proxy = new.get("proxies", {}).get(addr)

        if prev_proxy and not new_proxy:
            merged_proxies[addr] = prev_proxy
            total_upgrades += prev_proxy.get("upgrade_count", 0)
            continue
        if new_proxy and not prev_proxy:
            merged_proxies[addr] = new_proxy
            total_upgrades += new_proxy.get("upgrade_count", 0)
            continue

        # Both exist — merge events
        prev_events = prev_proxy.get("events", [])
        new_events = new_proxy.get("events", [])

        # Deduplicate by (block_number, tx_hash, event_type)
        seen: set[tuple[int, str, str]] = set()
        merged_events: list[dict] = []
        for event in prev_events + new_events:
            key = (event.get("block_number", 0), event.get("tx_hash", ""), event.get("event_type", ""))
            if key not in seen:
                seen.add(key)
                merged_events.append(event)

        merged_events.sort(key=lambda e: (e.get("block_number", 0), e.get("log_index", 0)))

        # Rebuild timeline from merged events
        current_impl = new_proxy.get("current_implementation") or prev_proxy.get("current_implementation")
        implementations = _build_implementation_timeline(merged_events, current_impl)
        upgrade_events = [e for e in merged_events if e["event_type"] == "upgraded"]

        merged_proxies[addr] = {
            "proxy_address": addr,
            "proxy_type": new_proxy.get("proxy_type") or prev_proxy.get("proxy_type"),
            "current_implementation": current_impl,
            "upgrade_count": len(upgrade_events),
            "first_upgrade_block": upgrade_events[0]["block_number"] if upgrade_events else None,
            "last_upgrade_block": upgrade_events[-1]["block_number"] if upgrade_events else None,
            "implementations": implementations,
            "events": merged_events,
        }
        total_upgrades += len(upgrade_events)

    return {
        "schema_version": new.get("schema_version") or prev.get("schema_version", "0.1"),
        "target_address": new.get("target_address") or prev.get("target_address"),
        "proxies": merged_proxies,
        "total_upgrades": total_upgrades,
    }


def _resolve_upgrade_history(
    session, job, dependencies_path, prev_uh: dict | None,
) -> dict | None:
    """Load cached upgrade history, fetch incrementally, merge, and persist.

    Returns the (possibly merged) upgrade history dict, or None on failure.
    """
    from services.discovery.upgrade_history import build_upgrade_history

    # Compute from_block for incremental fetch
    from_block = 0
    if prev_uh and isinstance(prev_uh, dict) and prev_uh.get("proxies"):
        max_block = 0
        for proxy_info in prev_uh["proxies"].values():
            for event in proxy_info.get("events", []):
                block = event.get("block_number", 0)
                if block > max_block:
                    max_block = block
        if max_block > 0:
            from_block = max_block + 1

    uh = build_upgrade_history(dependencies_path, from_block=from_block)

    if prev_uh and isinstance(prev_uh, dict) and prev_uh.get("proxies"):
        if uh.get("proxies"):
            uh = _merge_upgrade_history(prev_uh, uh)
        else:
            # No new events — use previous as-is
            uh = prev_uh

    if uh.get("proxies"):
        store_artifact(session, job.id, "upgrade_history", data=uh)

    return uh if uh.get("proxies") else None


# ---------------------------------------------------------------------------
# Source / project helpers
# ---------------------------------------------------------------------------
# Minimum solc version to avoid known compiler bugs (e.g. Natspec.cpp assertion in 0.8.21)
_MIN_SOLC = "0.8.24"  # 0.8.21-0.8.23 have Natspec.cpp internal compiler errors on some OZ contracts


def _detect_solc_version(sources: dict[str, str]) -> str:
    min_tuple = tuple(int(x) for x in _MIN_SOLC.split("."))
    versions = []
    for content in sources.values():
        for m in re.finditer(r"pragma\s+solidity\s+[\^~>=<]*\s*(0\.\d+\.\d+)", content):
            versions.append(m.group(1))
    if not versions:
        return _MIN_SOLC
    detected = max(versions, key=lambda v: tuple(int(x) for x in v.split(".")))
    detected_tuple = tuple(int(x) for x in detected.split("."))
    # Enforce the minimum only for the 0.8.x line that is affected by the bug.
    if detected_tuple[:2] == min_tuple[:2] and detected_tuple < min_tuple:
        return _MIN_SOLC
    return detected


def _relax_pragmas(sources: dict[str, str]) -> dict[str, str]:
    """Rewrite exact pragma constraints to '^X.Y.Z'.

    Foundry nightly validates pragma constraints against solc_version even with
    auto_detect_solc = false. Both bare '0.8.28' and '=0.8.28' are exact
    constraints that prevent using a newer patch-level compiler.
    """
    relaxed = {}
    for path, content in sources.items():
        # Match 'pragma solidity =0.8.28' or bare 'pragma solidity 0.8.28'
        relaxed[path] = re.sub(
            r"(pragma\s+solidity\s+)=?\s*(0\.\d+\.\d+)",
            r"\1^\2",
            content,
        )
    return relaxed


def _detect_src_dir(sources: dict[str, str]) -> str:
    """Pick the foundry `src` directory based on where source files live.

    Priority:
      1. "src" if any file starts with src/
      2. "contracts" if any file starts with contracts/
      3. "." to catch files at root or under lib/
    """
    for path in sources:
        if path.startswith("src/"):
            return "src"
    for path in sources:
        if path.startswith("contracts/"):
            return "contracts"
    return "."


def _prune_remappings(remappings: list[str], source_paths: set[str]) -> list[str]:
    """Keep only remappings whose target directory actually contains files in the source bundle.

    A remapping like ``@openzeppelin/contracts/=lib/openzeppelin-contracts/contracts/``
    is only useful if we have files under ``lib/openzeppelin-contracts/contracts/``.
    Remappings pointing to dirs with zero files just confuse solc/Slither.
    """
    kept: list[str] = []
    dropped: list[str] = []
    for entry in remappings:
        # Parse "prefix=target" (Foundry remapping format)
        if "=" not in entry:
            kept.append(entry)
            continue
        _prefix, target = entry.split("=", 1)
        target = target.rstrip("/")
        # Check if any source file lives under this target path
        if any(p == target or p.startswith(target + "/") for p in source_paths):
            kept.append(entry)
        else:
            dropped.append(entry)
    if dropped:
        logger.info(
            "Pruned %d/%d remappings with no matching source files: %s",
            len(dropped),
            len(remappings),
            ", ".join(d.split("=")[0] for d in dropped),
        )
    return kept


# Proxy types where the implementation is baked into bytecode (immutable) —
# safe to reuse from cache without an RPC slot check.
_IMMUTABLE_PROXY_TYPES = frozenset({"eip1167"})

# Proxy types with multiple facets that can't be verified with a single slot check.
_MULTI_FACET_PROXY_TYPES = frozenset({"eip2535"})

# The proxy fields that get copied between contract rows.  Kept in sync
# with _MUTABLE_CONTRACT_FIELDS in db/queue.py (the cache-copy exclusion set).
_PROXY_FIELDS = ("is_proxy", "proxy_type", "implementation", "beacon", "admin")


def _apply_proxy_cache(session, src_contract, contract_row) -> dict:
    """Copy proxy fields from *src_contract* to *contract_row* and return a
    ``classify_single``-style dict for downstream consumers."""
    for field in _PROXY_FIELDS:
        setattr(contract_row, field, getattr(src_contract, field))
    session.commit()

    if not src_contract.is_proxy:
        return {"type": "regular"}
    return {
        "type": "proxy",
        **{f: getattr(src_contract, f) for f in _PROXY_FIELDS if f != "is_proxy"},
    }


def _check_proxy_cache(session, job, contract_row) -> dict | None:
    """Check whether proxy classification can be reused from a cached source job.

    Returns a ``classify_single``-style dict if the cached proxy state is still
    valid, or ``None`` when full ``_resolve_proxy`` must run.
    """
    request = job.request if isinstance(job.request, dict) else {}
    if not request.get("static_cached"):
        return None

    source_job_id = request.get("cache_source_job_id")
    if not source_job_id:
        return None

    try:
        src_contract = session.execute(
            select(Contract).where(Contract.job_id == source_job_id).limit(1)
        ).scalar_one_or_none()
    except Exception:
        return None

    if src_contract is None:
        return None

    # Non-proxy source: non-proxies don't become proxies.
    if not src_contract.is_proxy:
        return _apply_proxy_cache(session, src_contract, contract_row)

    proxy_type = src_contract.proxy_type

    # Diamond proxies have multiple facets — can't verify with a single call.
    if proxy_type in _MULTI_FACET_PROXY_TYPES:
        return None

    # Immutable proxy types (e.g. EIP-1167): impl is baked into bytecode.
    if proxy_type in _IMMUTABLE_PROXY_TYPES:
        return _apply_proxy_cache(session, src_contract, contract_row)

    cached_impl = src_contract.implementation
    if not cached_impl:
        return None

    rpc_url = request.get("rpc_url") or os.getenv("ETH_RPC")
    if not rpc_url:
        return None

    # Single RPC call — resolve_current_implementation handles all proxy types
    # (slot reads, getter calls, fallback discovery).
    try:
        current_impl = resolve_current_implementation(
            contract_row.address, rpc_url, proxy_type=proxy_type
        )
        if not current_impl:
            return None
        current_impl = normalize_hex(current_impl)
    except Exception:
        return None

    if current_impl != normalize_hex(cached_impl):
        return None  # upgraded — need full re-classification

    return _apply_proxy_cache(session, src_contract, contract_row)


class StaticWorker(BaseWorker):
    stage = JobStage.static
    next_stage = JobStage.resolution

    def process(self, session, job):
        from sqlalchemy import select as sa_select

        sources = get_source_files(session, job.id)
        if not sources:
            raise RuntimeError("No source files found in DB for this job")

        # Read from contracts table instead of artifacts
        contract_row = session.execute(
            sa_select(Contract).where(Contract.job_id == job.id).limit(1)
        ).scalar_one_or_none()
        if not contract_row:
            raise RuntimeError("Contract row not found for this job")

        contract_name = contract_row.contract_name or "Contract"
        address = contract_row.address or job.address or "0x0"
        job_id_str = str(job.id)

        # Build meta dict for downstream tools that still expect it
        meta = {
            "address": address,
            "contract_name": contract_name,
            "compiler_version": contract_row.compiler_version or "",
            "language": contract_row.language or "solidity",
            "evm_version": contract_row.evm_version or "shanghai",
            "source_format": contract_row.source_format or "flat",
            "source_file_count": contract_row.source_file_count or len(sources),
            "remappings": list(contract_row.remappings or []),
        }
        build_settings = {
            "evm_version": contract_row.evm_version or "shanghai",
            "optimization_used": contract_row.optimization or False,
            "runs": contract_row.optimization_runs or 200,
        }
        remappings = meta.get("remappings", [])

        # Attach the job's display name so downstream tools (e.g. graph builder)
        # can use it instead of the Etherscan contract name for proxy contracts.
        if job.name:
            meta["display_name"] = job.name

        request = job.request if isinstance(job.request, dict) else {}

        logger.info(
            "Static stage started for job %s address=%s contract=%s",
            job_id_str,
            address,
            contract_name,
        )

        # Attempt to reuse proxy classification from a cached source job.
        # This avoids 3-8 RPC calls when the proxy hasn't been upgraded.
        cached_proxy = _check_proxy_cache(session, job, contract_row)
        if cached_proxy is not None:
            target_classification = cached_proxy
            # Store contract_flags artifact to match what _resolve_proxy would produce
            cached_type = cached_proxy.get("type", "regular")
            flags = {
                "is_proxy": cached_type == "proxy",
                "classification_type": cached_type,
                "cached_from_job": str(request.get("cache_source_job_id", "")),
                **{f: cached_proxy.get(f) for f in _PROXY_FIELDS if f != "is_proxy"},
            }
            store_artifact(session, job.id, "contract_flags", data=flags)
            logger.info(
                "Job %s: proxy classification reused from cache (type=%s)",
                job.id,
                cached_type,
            )
        else:
            # Always attempt semantic proxy classification when RPC is available.
            # Hidden proxies often won't match name-based heuristics so we run
            # this unconditionally.  The result is reused by classify_contracts()
            # in the dependency phase to avoid duplicate RPC calls.
            target_classification = self._resolve_proxy(session, job, address, contract_name)

        # Check if proxy classification marked this as a proxy — if so,
        # skip Slither/analysis on the proxy source (it's just a thin wrapper).
        # Dependency discovery still runs because proxy-address deps are useful.
        session.refresh(contract_row)
        is_proxy = contract_row.is_proxy

        # Check if the discovery worker flagged this job as using cached static
        # data.  When set, we skip the expensive Slither / contract-analysis /
        # tracking-plan phases but still run the dependency phase (resolution
        # needs it).
        has_cached_static = bool(request.get("static_cached"))

        # Create temp directory and write source files
        tmp_dir = tempfile.mkdtemp(prefix="psat_static_")
        project_dir = Path(tmp_dir)
        try:
            self._scaffold_project(project_dir, sources, meta, build_settings, remappings)

            # Phase 0: Dependency artifacts (always runs — proxy deps are useful)
            self._run_dependency_phase(session, job, project_dir, contract_name, address, target_classification)

            if is_proxy:
                self.update_detail(session, job, "Proxy detected — impl job handles analysis")
                logger.info(
                    "Static stage skipping analysis for proxy job %s (%s) — impl child job will analyze",
                    job_id_str,
                    contract_name,
                )
                # Proxy jobs skip resolution/policy — complete directly
                from db.queue import complete_job

                complete_job(session, job.id, f"Proxy {contract_name} — impl child job queued for full analysis")
                raise JobHandledDirectly()
            elif has_cached_static:
                # Static artifacts already present from cache — skip analysis phases.
                logger.info(
                    "Static stage cache hit for job %s (%s) — skipping Slither/analysis/tracking plan",
                    job_id_str,
                    contract_name,
                )
                self.update_detail(session, job, "Static analysis complete (cached)")
            else:
                # Phase 1: Slither
                t0 = time.monotonic()
                slither_ok = self._run_slither_phase(session, job, project_dir, contract_name, address)
                if DEBUG_TIMING:
                    logger.info("[TIMING] slither: %.1fs", time.monotonic() - t0)

                # Phase 2: Contract analysis
                t0 = time.monotonic()
                analysis_ok = self._run_analysis_phase(session, job, project_dir, contract_name, address)
                if DEBUG_TIMING:
                    logger.info("[TIMING] contract analysis: %.1fs", time.monotonic() - t0)

                if not analysis_ok:
                    raise RuntimeError(
                        f"Contract analysis failed for {contract_name} ({address}). "
                        f"Slither CLI {'succeeded' if slither_ok else 'also failed'}."
                    )

                # Phase 3: Control tracking plan
                t0 = time.monotonic()
                self._run_tracking_plan_phase(session, job, project_dir, contract_name, address)
                if DEBUG_TIMING:
                    logger.info("[TIMING] tracking plan: %.1fs", time.monotonic() - t0)

            self.update_detail(session, job, "Static analysis complete")
            logger.info("Static analysis complete for job %s (%s)", job_id_str, contract_name)

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def _resolve_proxy(self, session, job, address: str, contract_name: str) -> dict | None:
        """Use on-chain classification to detect proxy type and resolve implementation.

        If an implementation is found, creates a linked child job for it so the
        real business logic gets analyzed.

        Returns the raw ``classify_single`` result dict so callers can pass it
        to ``classify_contracts(pre_classified=...)`` and avoid duplicate RPC calls.
        Returns ``None`` when classification was skipped or failed.
        """
        from services.discovery.classifier import classify_single

        rpc_url = None
        request = job.request if isinstance(job.request, dict) else {}
        if request:
            rpc_url = request.get("rpc_url")
        if not rpc_url:
            rpc_url = os.getenv("ETH_RPC")
        if not rpc_url:
            logger.info("Job %s: no RPC available for proxy classification", job.id)
            store_artifact(
                session,
                job.id,
                "contract_flags",
                data={"is_proxy": False, "classification_type": "unknown", "classification_skipped": "no_rpc"},
            )
            return None

        try:
            classification = classify_single(address, rpc_url)
        except Exception as exc:
            logger.warning("Job %s: proxy classification failed: %s", job.id, exc)
            store_artifact(
                session,
                job.id,
                "contract_flags",
                data={"is_proxy": False, "classification_type": "unknown", "classification_error": str(exc)},
            )
            return None

        classification_type = classification.get("type", "regular")
        if classification_type != "proxy":
            store_artifact(
                session,
                job.id,
                "contract_flags",
                data={"is_proxy": False, "classification_type": classification_type},
            )
            logger.info(
                "Job %s: semantic proxy classification result=%s for %s",
                job.id,
                classification_type,
                contract_name,
            )
            return classification

        proxy_type = classification.get("proxy_type", "unknown")
        impl_address = classification.get("implementation")
        beacon = classification.get("beacon")
        admin = classification.get("admin")
        facets = classification.get("facets")

        # Update contracts table with proxy info
        from sqlalchemy import select as sa_select

        contract_row = session.execute(
            sa_select(Contract).where(Contract.job_id == job.id).limit(1)
        ).scalar_one_or_none()
        if contract_row:
            contract_row.is_proxy = True
            contract_row.proxy_type = proxy_type
            contract_row.implementation = impl_address
            contract_row.beacon = beacon
            contract_row.admin = admin
            session.commit()

        store_artifact(
            session,
            job.id,
            "contract_flags",
            data={
                "is_proxy": True,
                "classification_type": classification_type,
                "proxy_type": proxy_type,
                "implementation": impl_address,
                "beacon": beacon,
                "admin": admin,
                "facets": facets,
            },
        )

        logger.info(
            "Job %s: proxy classified as %s, implementation=%s",
            job.id,
            proxy_type,
            impl_address or "unknown",
        )

        # Queue implementation addresses for analysis
        impl_entries: list[tuple[str, str]] = []  # (address, label)
        if impl_address:
            impl_entries.append((impl_address, "impl"))
        if facets:
            for i, facet in enumerate(facets):
                if facet != impl_address:  # avoid duplicates
                    impl_entries.append((facet, f"facet {i + 1}"))

        base_name = job.name or contract_name
        for impl_addr, label in impl_entries:
            # Check if we already have a job for this implementation
            existing = session.execute(select(Job).where(Job.address == impl_addr).limit(1)).scalar_one_or_none()
            if existing:
                logger.info(
                    "Job %s: %s %s already has job %s, skipping",
                    job.id,
                    label,
                    impl_addr,
                    existing.id,
                )
                continue

            impl_name = f"{base_name}: ({label})"
            child_request = {
                "address": impl_addr,
                "name": impl_name,
                "rpc_url": rpc_url,
                "parent_job_id": str(job.id),
                "proxy_address": address,
                "proxy_type": proxy_type,
            }
            if request.get("chain") is not None:
                child_request["chain"] = request.get("chain")
            child_job = create_job(session, child_request)
            logger.info(
                "Job %s: created %s job %s for %s (%s)",
                job.id,
                label,
                child_job.id,
                impl_addr,
                impl_name,
            )

        return classification

    def _scaffold_project(
        self,
        project_dir: Path,
        sources: dict[str, str],
        meta: dict,
        build_settings: dict,
        remappings: list[str],
    ) -> None:
        """Write source files, foundry.toml, remappings to the temp project."""
        sources = _relax_pragmas(sources)
        for filepath, content in sources.items():
            full_path = project_dir / filepath
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(content)

        solc_version = _detect_solc_version(sources)
        src_dir = _detect_src_dir(sources)
        evm_version = build_settings.get("evm_version", "shanghai")
        optimizer = str(build_settings.get("optimization_used", True)).lower()
        optimizer_runs = build_settings.get("runs", 200)

        (project_dir / "foundry.toml").write_text(
            textwrap.dedent(
                f"""\
                [profile.default]
                src = "{src_dir}"
                out = "out"
                libs = ["lib"]
                solc_version = "{solc_version}"
                evm_version = "{evm_version}"
                optimizer = {optimizer}
                optimizer_runs = {optimizer_runs}
                auto_detect_solc = false
            """
            )
        )

        # Prune remappings to only those whose target dirs have actual source files
        pruned = _prune_remappings(remappings, set(sources.keys()))
        if pruned:
            (project_dir / "remappings.txt").write_text("\n".join(pruned) + "\n")

        (project_dir / "contract_meta.json").write_text(json.dumps(meta, indent=2) + "\n")

    def _run_dependency_phase(
        self,
        session,
        job,
        project_dir: Path,
        contract_name: str,
        address: str,
        target_classification: dict | None = None,
    ) -> None:
        """Build dependency artifacts before compile-dependent analysis starts."""
        self.update_detail(session, job, "Discovering dependencies")

        request = job.request if isinstance(job.request, dict) else {}
        deps_rpc = request.get("rpc_url")
        dynamic_rpc = request.get("dynamic_rpc") or deps_rpc
        dynamic_tx_limit = request.get("dynamic_tx_limit", 10)
        dynamic_tx_hashes = request.get("dynamic_tx_hashes")
        dependency_errors: dict[str, str] = {}
        # Shared bytecode cache across all dependency stages to avoid duplicate
        # eth_getCode RPC calls for the same addresses.
        code_cache: dict[str, str] = {}

        logger.info(
            "Static stage dependency discovery started for job %s address=%s contract=%s",
            job.id,
            address,
            contract_name,
        )

        deps_output = None

        # Check for cached static dependencies (bytecode is immutable, so these
        # never change and can be reused permanently).  The artifact is
        # self-describing — if it exists on this job it's valid, whether it
        # was copied from a prior completed job or stored on a previous
        # attempt of this same job that failed later in the pipeline.
        cached_static_deps = get_artifact(session, job.id, "static_dependencies")

        if cached_static_deps is not None:
            deps_output = cached_static_deps
            logger.info(
                "Static stage static dependencies loaded from cache for job %s address=%s count=%d",
                job.id,
                address,
                len(deps_output.get("dependencies", [])),
            )
        else:
            try:
                t0 = time.monotonic()
                deps_output = find_dependencies(address, deps_rpc, code_cache=code_cache)
                if DEBUG_TIMING:
                    n = len(deps_output.get("dependencies", []))
                    logger.info("[TIMING] static deps: %.1fs (%d deps)", time.monotonic() - t0, n)
                logger.info(
                    "Static stage static dependencies complete for job %s address=%s count=%d",
                    job.id,
                    address,
                    len(deps_output.get("dependencies", [])),
                )
                # Persist for future cache hits
                store_artifact(session, job.id, "static_dependencies", data=deps_output)
            except Exception as exc:
                dependency_errors["static"] = str(exc)
                logger.warning(
                    "Static stage static dependency discovery failed for job %s address=%s: %s",
                    job.id,
                    address,
                    exc,
                )

        t0 = time.monotonic()
        tx_hashes = dynamic_tx_hashes if isinstance(dynamic_tx_hashes, list) else None
        dyn_output, dyn_error = _resolve_dynamic_deps(
            session, job, address, dynamic_rpc, int(dynamic_tx_limit),
            tx_hashes, request.get("proxy_address"), code_cache,
        )
        if DEBUG_TIMING and dyn_output:
            logger.info("[TIMING] dynamic deps: %.1fs (%d deps)",
                        time.monotonic() - t0, len(dyn_output.get("dependencies", [])))
        if dyn_error:
            dependency_errors["dynamic"] = dyn_error
            logger.warning(
                "Static stage dynamic dependency discovery failed for job %s address=%s: %s",
                job.id, address, dyn_error,
            )
        elif dyn_output:
            logger.info(
                "Static stage dynamic dependencies complete for job %s address=%s count=%d",
                job.id, address, len(dyn_output.get("dependencies", [])),
            )

        resolved_rpc = None
        if isinstance(deps_output, dict):
            resolved_rpc = deps_output.get("rpc")
        if not resolved_rpc and isinstance(dyn_output, dict):
            resolved_rpc = dyn_output.get("rpc")

        cls_output = None
        if resolved_rpc:
            unique_deps = sorted(
                set((deps_output or {}).get("dependencies", []) + (dyn_output or {}).get("dependencies", []))
            )
            try:
                t0 = time.monotonic()
                from services.discovery.static_dependencies import normalize_address

                pre_classified = {}
                if target_classification:
                    pre_classified[normalize_address(address)] = target_classification

                # Load previous classifications to skip already-classified addresses
                prev_cls = get_artifact(session, job.id, "classifications")
                if isinstance(prev_cls, dict):
                    for cls_addr, cls_info in prev_cls.get("classifications", {}).items():
                        if cls_addr not in pre_classified:
                            pre_classified[cls_addr] = cls_info

                cls_output = classify_contracts(
                    address,
                    unique_deps,
                    resolved_rpc,
                    dynamic_edges=(dyn_output or {}).get("dependency_graph"),
                    code_cache=code_cache,
                    pre_classified=pre_classified or None,
                )
                # Store classifications artifact for future cache hits
                store_artifact(session, job.id, "classifications", data=cls_output)
                if DEBUG_TIMING:
                    logger.info("[TIMING] classification: %.1fs (%d deps)", time.monotonic() - t0, len(unique_deps))
                logger.info(
                    "Static stage dependency classification complete for job %s address=%s discovered=%d",
                    job.id,
                    address,
                    len(cls_output.get("discovered_addresses", [])),
                )
            except Exception as exc:
                dependency_errors["classification"] = str(exc)
                logger.warning(
                    "Static stage dependency classification failed for job %s address=%s: %s",
                    job.id,
                    address,
                    exc,
                )
        else:
            logger.info(
                "Static stage dependency classification skipped for job %s address=%s (no resolved RPC)",
                job.id,
                address,
            )

        if deps_output or dyn_output:
            unified = build_unified_dependencies(
                address, deps_output, dyn_output, cls_output, target_classification=target_classification
            )
            # Load cached enrichment data (contract names + selectors are immutable)
            prev_enrichment = get_artifact(session, job.id, "enrichment_cache")
            info_cache: dict[str, tuple[str | None, dict[str, str]]] = {}
            if isinstance(prev_enrichment, dict):
                for _addr, _data in prev_enrichment.items():
                    if isinstance(_data, dict):
                        info_cache[_addr] = (_data.get("name"), _data.get("selectors", {}))

            t0 = time.monotonic()
            enrich_dependency_metadata(unified, info_cache=info_cache)
            if DEBUG_TIMING:
                logger.info("[TIMING] enrichment: %.1fs", time.monotonic() - t0)

            # Store updated enrichment cache (includes any newly fetched entries)
            enrichment_data = {
                addr: {"name": name, "selectors": selectors}
                for addr, (name, selectors) in info_cache.items()
            }
            store_artifact(session, job.id, "enrichment_cache", data=enrichment_data)

            # Write to contract_dependencies table
            from sqlalchemy import select as sa_select

            contract_row = session.execute(
                sa_select(Contract).where(Contract.job_id == job.id).limit(1)
            ).scalar_one_or_none()
            if contract_row:
                from db.models import ContractDependency

                session.query(ContractDependency).filter(ContractDependency.contract_id == contract_row.id).delete()
                for dep_addr, dep_info in unified.get("dependencies", {}).items():
                    if not isinstance(dep_info, dict):
                        continue
                    impl = dep_info.get("implementation")
                    if isinstance(impl, dict):
                        impl_addr = impl.get("address")
                    elif isinstance(impl, str):
                        impl_addr = impl
                    else:
                        impl_addr = None
                    session.add(
                        ContractDependency(
                            contract_id=contract_row.id,
                            dependency_address=dep_addr.lower(),
                            dependency_name=dep_info.get("contract_name"),
                            relationship_type=dep_info.get("type", "regular"),
                            source=dep_info.get("source"),
                            proxy_type=dep_info.get("proxy_type"),
                            implementation=impl_addr,
                            admin=dep_info.get("admin"),
                        )
                    )
                session.commit()

            dependencies_path = project_dir / "dependencies.json"
            dependencies_path.write_text(json.dumps(unified, indent=2) + "\n")
            store_artifact(session, job.id, "dependencies", data=unified)

            proxy_addr = request.get("proxy_address")
            proxy_name = (job.name or "").split(":")[0].strip() if proxy_addr else None
            proxy_type = request.get("proxy_type") if proxy_addr else None
            viz_path = write_dependency_visualization(project_dir, proxy_addr, proxy_name, proxy_type)
            if viz_path and viz_path.exists():
                dependency_graph = json.loads(viz_path.read_text())
                store_artifact(session, job.id, "dependency_graph_viz", data=dependency_graph)
                logger.info(
                    "Static stage dependency graph complete for job %s address=%s nodes=%d edges=%d",
                    job.id,
                    address,
                    len(dependency_graph.get("nodes", [])),
                    len(dependency_graph.get("edges", [])),
                )
            else:
                logger.info(
                    "Static stage dependencies complete for job %s address=%s (no graph nodes)",
                    job.id,
                    address,
                )

            # Upgrade history for proxy contracts (incremental / append-only)
            try:
                prev_uh = get_artifact(session, job.id, "upgrade_history")
                if prev_uh is not None and not isinstance(prev_uh, dict):
                    prev_uh = None
                uh = _resolve_upgrade_history(session, job, dependencies_path, prev_uh)
                if uh:
                    logger.info(
                        "Static stage upgrade history complete for job %s address=%s upgrades=%d",
                        job.id,
                        address,
                        uh.get("total_upgrades", 0),
                    )
            except Exception as exc:
                logger.warning(
                    "Static stage upgrade history failed for job %s address=%s: %s",
                    job.id,
                    address,
                    exc,
                )
        else:
            logger.warning(
                "Static stage dependency artifacts skipped for job %s address=%s (no dependency outputs)",
                job.id,
                address,
            )

        if dependency_errors:
            store_artifact(session, job.id, "dependency_errors", data=dependency_errors)

    def _run_slither_phase(self, session, job, project_dir: Path, contract_name: str, address: str) -> bool:
        """Run Slither CLI. Returns True on success, False on failure (non-fatal)."""
        if is_vyper_project(project_dir):
            self.update_detail(session, job, "Skipping Slither for Vyper source")
            store_artifact(
                session,
                job.id,
                "slither_error",
                data={"error": "Skipped Slither for Vyper source"},
            )
            logger.info(
                "Static stage skipped Slither for Vyper job %s address=%s contract=%s",
                job.id,
                address,
                contract_name,
            )
            return False
        self.update_detail(session, job, "Running Slither")
        try:
            analyze(project_dir, contract_name, address)
        except Exception as exc:
            _log_phase_error(str(job.id), address, contract_name, "slither_cli", str(exc))
            store_artifact(session, job.id, "slither_error", data={"error": str(exc)})
            return False

        slither_path = project_dir / "slither_results.json"
        if slither_path.exists():
            slither_data = json.loads(slither_path.read_text())

            # Write to slither_findings table
            from sqlalchemy import select as sa_select

            contract_row = session.execute(
                sa_select(Contract).where(Contract.job_id == job.id).limit(1)
            ).scalar_one_or_none()
            if contract_row:
                session.query(SlitherFinding).filter(SlitherFinding.contract_id == contract_row.id).delete()
                for finding in slither_data.get("results", {}).get("detectors", []):
                    session.add(
                        SlitherFinding(
                            contract_id=contract_row.id,
                            detector=finding.get("check"),
                            severity=finding.get("impact"),
                            description=finding.get("description"),
                            elements=finding.get("elements"),
                        )
                    )
                session.commit()

            store_artifact(session, job.id, "slither_results", data=slither_data)

        report_path = project_dir / "analysis_report.txt"
        if report_path.exists():
            store_artifact(session, job.id, "analysis_report", text_data=report_path.read_text())

        logger.info(
            "Static stage slither complete for job %s address=%s contract=%s",
            job.id,
            address,
            contract_name,
        )
        return True

    def _run_analysis_phase(self, session, job, project_dir: Path, contract_name: str, address: str) -> bool:
        """Run structured contract analysis. Returns True on success."""
        self.update_detail(session, job, "Building structured contract analysis")
        try:
            contract_analysis_path = analyze_contract(project_dir)
        except Exception as exc:
            _log_phase_error(str(job.id), address, contract_name, "contract_analysis", str(exc))
            store_artifact(session, job.id, "analysis_error", data={"error": str(exc)})
            return False

        if contract_analysis_path.exists():
            analysis_data = json.loads(contract_analysis_path.read_text())
            # Keep as artifact — resolution/policy stages read it as JSON
            store_artifact(session, job.id, "contract_analysis", data=analysis_data)
            self._write_analysis_tables(session, job, analysis_data)
        logger.info(
            "Static stage contract analysis complete for job %s address=%s contract=%s",
            job.id,
            address,
            contract_name,
        )
        return True

    def _write_analysis_tables(self, session, job: Job, analysis: dict) -> None:
        """Extract structured data from contract_analysis JSON into relational tables."""
        from sqlalchemy import select as sa_select

        contract_row = session.execute(
            sa_select(Contract).where(Contract.job_id == job.id).limit(1)
        ).scalar_one_or_none()
        if not contract_row:
            return

        summary = analysis.get("summary", {})
        subject = analysis.get("subject", {})

        # Update contract name from analysis if available
        if subject.get("name"):
            contract_row.contract_name = subject["name"]

        # Write contract_summary
        existing_summary = session.execute(
            sa_select(ContractSummary).where(ContractSummary.contract_id == contract_row.id)
        ).scalar_one_or_none()
        if existing_summary:
            session.delete(existing_summary)
            session.flush()

        session.add(
            ContractSummary(
                contract_id=contract_row.id,
                control_model=summary.get("control_model"),
                is_upgradeable=summary.get("is_upgradeable"),
                is_pausable=summary.get("is_pausable"),
                has_timelock=summary.get("has_timelock"),
                risk_level=summary.get("static_risk_level"),
                is_factory=summary.get("is_factory"),
                is_nft=summary.get("is_nft"),
                standards=summary.get("standards", []),
                source_verified=subject.get("source_verified"),
            )
        )

        # Write privileged_functions
        session.query(PrivilegedFunction).filter(PrivilegedFunction.contract_id == contract_row.id).delete()
        ac = analysis.get("access_control", {})
        for pf in ac.get("privileged_functions", []):
            session.add(
                PrivilegedFunction(
                    contract_id=contract_row.id,
                    function_name=pf.get("function", ""),
                    selector=pf.get("selector"),
                    abi_signature=pf.get("abi_signature"),
                    effect_labels=pf.get("effect_labels", []),
                    action_summary=pf.get("action_summary"),
                    authority_public=False,
                )
            )

        # Write role_definitions
        session.query(RoleDefinition).filter(RoleDefinition.contract_id == contract_row.id).delete()
        for rd in ac.get("role_definitions", []):
            session.add(
                RoleDefinition(
                    contract_id=contract_row.id,
                    role_name=rd.get("role", ""),
                    declared_in=rd.get("declared_in"),
                )
            )

        session.commit()

    def _run_tracking_plan_phase(self, session, job, project_dir: Path, contract_name: str, address: str) -> None:
        """Build control tracking plan. Non-fatal on failure."""
        self.update_detail(session, job, "Building control tracking plan")
        analysis_path = project_dir / "contract_analysis.json"
        if not analysis_path.exists():
            logger.warning("Job %s: skipping tracking plan — no contract_analysis.json", job.id)
            return
        try:
            tracking_plan = build_control_tracking_plan_from_file(analysis_path)
            store_artifact(session, job.id, "control_tracking_plan", data=tracking_plan)
            logger.info(
                "Static stage tracking plan complete for job %s address=%s contract=%s",
                job.id,
                address,
                contract_name,
            )
        except Exception as exc:
            _log_phase_error(str(job.id), address, contract_name, "tracking_plan", str(exc))
            store_artifact(session, job.id, "tracking_plan_error", data={"error": str(exc)})


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        force=True,
    )
    StaticWorker().run_loop()


if __name__ == "__main__":
    main()
