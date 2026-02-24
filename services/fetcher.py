"""Fetch verified smart contract source code from Etherscan and scaffold a Foundry project."""

import json
import re
import textwrap
from pathlib import Path

from utils.etherscan import get_source

CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"


def fetch(address: str) -> dict:
    """Fetch verified source from Etherscan. Returns the raw result dict."""
    return get_source(address)


def parse_sources(result: dict) -> dict[str, str]:
    """Parse Etherscan response into {filepath: source_code} mapping."""
    raw = result["SourceCode"]
    contract_name = result.get("ContractName", "Contract")

    # Double-brace wrapped JSON
    if raw.startswith("{{"):
        raw = raw[1:-1]

    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {f"src/{contract_name}.sol": raw}

    if isinstance(parsed, dict) and "sources" in parsed:
        parsed = parsed["sources"]

    sources = {}
    for filename, obj in parsed.items():
        content = obj["content"] if isinstance(obj, dict) else obj
        filename = filename.lstrip("./")
        if not filename.startswith("src/"):
            filename = f"src/{filename}"
        sources[filename] = content

    return sources


def _detect_solc_version(sources: dict[str, str]) -> str:
    for content in sources.values():
        match = re.search(r"pragma\s+solidity\s+[\^~>=<]*\s*(0\.\d+\.\d+)", content)
        if match:
            return match.group(1)
    return "0.8.19"


def scaffold(address: str, name: str, result: dict) -> Path:
    """Write source files into a Foundry project and return the project path."""
    sources = parse_sources(result)
    solc_version = _detect_solc_version(sources)

    project_dir = CONTRACTS_DIR / name
    project_dir.mkdir(parents=True, exist_ok=True)

    # foundry.toml
    (project_dir / "foundry.toml").write_text(textwrap.dedent(f"""\
        [profile.default]
        src = "src"
        out = "out"
        libs = ["lib"]
        solc_version = "{solc_version}"
        evm_version = "shanghai"
        optimizer = true
        optimizer_runs = 200
    """))

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
    }
    (project_dir / "contract_meta.json").write_text(json.dumps(meta, indent=2) + "\n")

    return project_dir
