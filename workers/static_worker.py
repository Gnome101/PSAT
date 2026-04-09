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
from db.queue import create_job, get_source_files, store_artifact
from services.discovery import (
    build_unified_dependencies,
    classify_contracts,
    enrich_dependency_metadata,
    find_dependencies,
    find_dynamic_dependencies,
    write_dependency_visualization,
)
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

        logger.info(
            "Static stage started for job %s address=%s contract=%s",
            job_id_str,
            address,
            contract_name,
        )

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
        except Exception as exc:
            dependency_errors["static"] = str(exc)
            logger.warning(
                "Static stage static dependency discovery failed for job %s address=%s: %s",
                job.id,
                address,
                exc,
            )

        dyn_output = None
        try:
            t0 = time.monotonic()
            tx_limit = int(dynamic_tx_limit)
            tx_hashes = dynamic_tx_hashes if isinstance(dynamic_tx_hashes, list) else None
            proxy_addr = request.get("proxy_address")
            dyn_output = find_dynamic_dependencies(
                address,
                rpc_url=dynamic_rpc,
                tx_limit=tx_limit,
                tx_hashes=tx_hashes,
                proxy_address=proxy_addr,
                code_cache=code_cache,
            )
            if DEBUG_TIMING:
                n = len(dyn_output.get("dependencies", []))
                logger.info("[TIMING] dynamic deps: %.1fs (%d deps)", time.monotonic() - t0, n)
            logger.info(
                "Static stage dynamic dependencies complete for job %s address=%s count=%d",
                job.id,
                address,
                len(dyn_output.get("dependencies", [])),
            )
        except Exception as exc:
            dependency_errors["dynamic"] = str(exc)
            logger.warning(
                "Static stage dynamic dependency discovery failed for job %s address=%s: %s",
                job.id,
                address,
                exc,
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
                pre_classified = None
                if target_classification:
                    from services.discovery.static_dependencies import normalize_address

                    pre_classified = {normalize_address(address): target_classification}
                cls_output = classify_contracts(
                    address,
                    unique_deps,
                    resolved_rpc,
                    dynamic_edges=(dyn_output or {}).get("dependency_graph"),
                    code_cache=code_cache,
                    pre_classified=pre_classified,
                )
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
            t0 = time.monotonic()
            enrich_dependency_metadata(unified)
            if DEBUG_TIMING:
                logger.info("[TIMING] enrichment: %.1fs", time.monotonic() - t0)

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

            # Upgrade history for proxy contracts
            try:
                from services.discovery.upgrade_history import build_upgrade_history

                uh = build_upgrade_history(dependencies_path)
                if uh.get("proxies"):
                    store_artifact(session, job.id, "upgrade_history", data=uh)
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
