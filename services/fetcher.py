"""Fetch verified smart contract source code from Etherscan and scaffold a Foundry project."""

from __future__ import annotations

import json
import re
import textwrap
from pathlib import Path

from utils.etherscan import get_source

CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"


def fetch(address: str) -> dict:
    """Fetch verified source from Etherscan. Returns the raw result dict."""
    return get_source(address)


def _parse_source_code(raw: str) -> dict | None:
    """Parse Etherscan's SourceCode field into a dict when possible."""
    if not isinstance(raw, str):
        return None

    candidate = raw
    if candidate.startswith("{{") and candidate.endswith("}}"):
        candidate = candidate[1:-1]

    try:
        parsed = json.loads(candidate)
    except (json.JSONDecodeError, TypeError):
        return None

    return parsed if isinstance(parsed, dict) else None


def parse_verification_bundle(result: dict) -> dict | None:
    """Return the parsed standard-json verification bundle when present."""
    parsed = _parse_source_code(result.get("SourceCode", ""))
    if not parsed or "sources" not in parsed:
        return None
    return parsed


def parse_sources(result: dict) -> dict[str, str]:
    """Parse Etherscan response into {filepath: source_code} mapping."""
    bundle = parse_verification_bundle(result)
    contract_name = result.get("ContractName", "Contract")

    if bundle:
        sources = {}
        for filename, obj in bundle["sources"].items():
            content = obj["content"] if isinstance(obj, dict) else obj
            normalized = filename.lstrip("./")
            sources[normalized] = content
        return sources

    raw = result["SourceCode"]
    return {f"src/{contract_name}.sol": raw}


def parse_remappings(result: dict) -> list[str]:
    """Extract remappings from a standard-json verification payload."""
    bundle = parse_verification_bundle(result)
    settings = bundle.get("settings", {}) if bundle else {}
    remappings = settings.get("remappings", [])
    return [entry.strip() for entry in remappings if isinstance(entry, str) and entry.strip()]


def _detect_solc_version(sources: dict[str, str]) -> str:
    for content in sources.values():
        match = re.search(r"pragma\s+solidity\s+[\^~>=<]*\s*(0\.\d+\.\d+)", content)
        if match:
            return match.group(1)
    return "0.8.19"


def _project_src_dir(sources: dict[str, str]) -> str:
    if any(filename.startswith("src/") for filename in sources):
        return "src"
    return "."


def scaffold(address: str, name: str, result: dict) -> Path:
    """Write source files into a Foundry project and return the project path."""
    sources = parse_sources(result)
    remappings = parse_remappings(result)
    bundle = parse_verification_bundle(result)
    solc_version = _detect_solc_version(sources)
    src_dir = _project_src_dir(sources)

    project_dir = CONTRACTS_DIR / name
    project_dir.mkdir(parents=True, exist_ok=True)

    # foundry.toml
    (project_dir / "foundry.toml").write_text(
        textwrap.dedent(
            f"""\
            [profile.default]
            src = "{src_dir}"
            out = "out"
            libs = ["lib"]
            solc_version = "{solc_version}"
            evm_version = "{result.get('EVMVersion', 'shanghai') or 'shanghai'}"
            optimizer = {str(result.get("OptimizationUsed", "1") == "1").lower()}
            optimizer_runs = {int(result.get("Runs", "200") or 200)}
        """
        )
    )

    if remappings:
        (project_dir / "remappings.txt").write_text("\n".join(remappings) + "\n")

    if bundle:
        (project_dir / "etherscan_standard_input.json").write_text(json.dumps(bundle, indent=2) + "\n")

    # source files
    for filename, content in sources.items():
        filepath = project_dir / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(content)

    # metadata
    meta = {
        "address": address,
        "contract_name": result.get("ContractName", ""),
        "compiler_version": result.get("CompilerVersion", ""),
        "optimization_used": result.get("OptimizationUsed", ""),
        "runs": result.get("Runs", ""),
        "evm_version": result.get("EVMVersion", ""),
        "license": result.get("LicenseType", ""),
        "source_format": "standard_json" if bundle else "flat",
        "source_file_count": len(sources),
        "remappings": remappings,
    }
    (project_dir / "contract_meta.json").write_text(json.dumps(meta, indent=2) + "\n")

    return project_dir
