"""Static analysis worker — runs Slither and contract analysis in a temp directory."""

from __future__ import annotations

import json
import logging
import re
import shutil
import tempfile
import textwrap
from pathlib import Path

from db.models import JobStage
from db.queue import get_artifact, get_source_files, store_artifact
from services.resolution.tracking_plan import build_control_tracking_plan_from_file
from services.static import analyze, analyze_contract
from workers.base import BaseWorker

logger = logging.getLogger(__name__)

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
def _detect_solc_version(sources: dict[str, str]) -> str:
    versions = []
    for content in sources.values():
        for m in re.finditer(r"pragma\s+solidity\s+[\^~>=<]*\s*(0\.\d+\.\d+)", content):
            versions.append(m.group(1))
    if not versions:
        return "0.8.19"
    return max(versions, key=lambda v: tuple(int(x) for x in v.split(".")))


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


def _find_main_contract_file(sources: dict[str, str], contract_name: str) -> str | None:
    """Find the source file most likely to contain the main contract."""
    target = f"{contract_name}.sol"
    # Prefer src/ or contracts/ over lib/
    for path in sources:
        if path.endswith(target) and not path.startswith("lib/"):
            return path
    for path in sources:
        if path.endswith(target):
            return path
    return None


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


def _is_proxy_contract(contract_name: str, sources: dict[str, str]) -> bool:
    """Detect if this is a proxy contract based on name and source content."""
    proxy_names = {"proxy", "erc1967proxy", "transparentupgradeableproxy", "beaconproxy", "uupsproxy"}
    if contract_name.lower().replace("_", "") in proxy_names:
        return True
    main_file = _find_main_contract_file(sources, contract_name)
    if main_file and main_file in sources:
        content = sources[main_file].lower()
        if "delegatecall" in content and ("_implementation" in content or "fallback" in content):
            return True
    return False


class StaticWorker(BaseWorker):
    stage = JobStage.static
    next_stage = JobStage.resolution

    def process(self, session, job):
        sources = get_source_files(session, job.id)
        if not sources:
            raise RuntimeError("No source files found in DB for this job")

        meta = get_artifact(session, job.id, "contract_meta")
        if not isinstance(meta, dict):
            raise RuntimeError("contract_meta artifact not found or invalid")

        build_settings = get_artifact(session, job.id, "build_settings")
        if not isinstance(build_settings, dict):
            build_settings = {}

        contract_name = meta.get("contract_name", "Contract")
        address = meta.get("address", job.address or "0x0")
        remappings = meta.get("remappings", [])
        job_id_str = str(job.id)

        logger.info(
            "Static stage started for job %s address=%s contract=%s",
            job_id_str,
            address,
            contract_name,
        )

        # Detect proxy contracts early
        is_proxy = _is_proxy_contract(contract_name, sources)
        if is_proxy:
            logger.info("Job %s: %s detected as proxy contract", job_id_str, contract_name)
            store_artifact(session, job.id, "contract_flags", data={"is_proxy": True})

        # Create temp directory and write source files
        tmp_dir = tempfile.mkdtemp(prefix="psat_static_")
        project_dir = Path(tmp_dir)
        try:
            self._scaffold_project(project_dir, sources, meta, build_settings, remappings)

            # Phase 1: Slither
            slither_ok = self._run_slither_phase(session, job, project_dir, contract_name, address)

            # Phase 2: Contract analysis (uses Slither library directly, may succeed even if CLI failed)
            analysis_ok = self._run_analysis_phase(session, job, project_dir, contract_name, address)

            if not analysis_ok:
                raise RuntimeError(
                    f"Contract analysis failed for {contract_name} ({address}). "
                    f"Slither CLI {'succeeded' if slither_ok else 'also failed'}."
                )

            # Phase 3: Control tracking plan
            self._run_tracking_plan_phase(session, job, project_dir, contract_name, address)

            self.update_detail(session, job, "Static analysis complete")
            logger.info("Static analysis complete for job %s (%s)", job_id_str, contract_name)

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

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

    def _run_slither_phase(self, session, job, project_dir: Path, contract_name: str, address: str) -> bool:
        """Run Slither CLI. Returns True on success, False on failure (non-fatal)."""
        self.update_detail(session, job, "Running Slither")
        try:
            analyze(project_dir, contract_name, address)
        except Exception as exc:
            _log_phase_error(str(job.id), address, contract_name, "slither_cli", str(exc))
            store_artifact(session, job.id, "slither_error", data={"error": str(exc)})
            return False

        slither_path = project_dir / "slither_results.json"
        if slither_path.exists():
            store_artifact(session, job.id, "slither_results", data=json.loads(slither_path.read_text()))

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
            store_artifact(
                session, job.id, "contract_analysis", data=json.loads(contract_analysis_path.read_text())
            )
        logger.info(
            "Static stage contract analysis complete for job %s address=%s contract=%s",
            job.id,
            address,
            contract_name,
        )
        return True

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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    StaticWorker().run_loop()


if __name__ == "__main__":
    main()
