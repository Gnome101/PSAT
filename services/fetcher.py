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


def _detect_remappings(sources: dict[str, str]) -> list[str]:
    """Scan source files for import prefixes and match them to existing source paths."""
    # Collect all import paths with @ prefix
    import_prefixes: set[str] = set()
    for content in sources.values():
        for m in re.finditer(r'import\s+.*?"(@[^/]+/[^"]*)"', content):
            prefix = m.group(1).split("/")[0]  # e.g. "@solmate", "@openzeppelin"
            import_prefixes.add(prefix)
        for m in re.finditer(r'import\s+.*?\'(@[^/]+/[^\']*)', content):
            prefix = m.group(1).split("/")[0]
            import_prefixes.add(prefix)

    if not import_prefixes:
        return []

    # Build a map of known source directories (relative paths under src/)
    source_dirs: dict[str, str] = {}
    for filepath in sources:
        # e.g. "src/lib/solmate/src/tokens/ERC20.sol" -> we want to find
        # that "@solmate/" maps to "src/lib/solmate/src/"
        parts = filepath.split("/")
        for i, part in enumerate(parts):
            if part.startswith("lib") or part in ("node_modules",):
                # Look at the next directory as the package name
                if i + 1 < len(parts):
                    source_dirs[parts[i + 1]] = "/".join(parts[: i + 2])

    remappings = []
    for prefix in sorted(import_prefixes):
        clean = prefix.lstrip("@")  # "solmate", "openzeppelin"
        # Try to match against known source directories
        for dir_name, dir_path in source_dirs.items():
            if clean in dir_name or dir_name in clean:
                # Check if there's a src/ subdirectory
                src_subdir = dir_path + "/src/"
                contracts_subdir = dir_path + "/contracts/"
                # Determine the actual mapping by checking what exists in sources
                has_src = any(f.startswith(src_subdir) for f in sources)
                has_contracts = any(f.startswith(contracts_subdir) for f in sources)

                if clean == "openzeppelin" and has_contracts:
                    # @openzeppelin/contracts/... -> src/lib/openzeppelin-contracts/contracts/...
                    remappings.append(f"@openzeppelin/={dir_path}/")
                elif has_src:
                    remappings.append(f"{prefix}/={dir_path}/src/")
                else:
                    remappings.append(f"{prefix}/={dir_path}/")
                break

    return remappings


def scaffold(address: str, name: str, result: dict) -> Path:
    """Write source files into a Foundry project and return the project path."""
    sources = parse_sources(result)
    solc_version = _detect_solc_version(sources)
    remappings = _detect_remappings(sources)

    project_dir = CONTRACTS_DIR / name
    project_dir.mkdir(parents=True, exist_ok=True)

    # foundry.toml
    toml_lines = [
        "[profile.default]",
        'src = "src"',
        'out = "out"',
        'libs = ["lib"]',
        f'solc_version = "{solc_version}"',
        'evm_version = "shanghai"',
        "optimizer = true",
        "optimizer_runs = 200",
    ]
    if remappings:
        remap_entries = ", ".join(f'"{r}"' for r in remappings)
        toml_lines.append(f"remappings = [{remap_entries}]")
    toml_lines.append("")  # trailing newline
    (project_dir / "foundry.toml").write_text("\n".join(toml_lines) + "\n")

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
