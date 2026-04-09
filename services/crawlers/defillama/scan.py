"""
Callable entry point for DefiLlama adapter scanning.

Extracts the core logic from the CLI main.py into importable functions
that the PSAT worker can call directly.
"""

from __future__ import annotations

import logging
import subprocess
import time
from pathlib import Path

from services.crawlers.defillama.core_assets import build_address_to_chain_map, load_core_assets
from services.crawlers.defillama.extract import extract_addresses_from_file, extract_protocol

logger = logging.getLogger(__name__)

DEFAULT_REPO_PATH = Path(__file__).resolve().parents[4] / "repo" / "DefiLlama-Adapters"


def clone_or_update_repo(repo_path: Path) -> None:
    """Clone the DefiLlama-Adapters repo, or pull latest if it exists."""
    if (repo_path / ".git").exists():
        logger.info("Updating existing repo at %s", repo_path)
        subprocess.run(
            ["git", "-C", str(repo_path), "pull", "--ff-only"],
            capture_output=True,
        )
    else:
        logger.info("Cloning DefiLlama-Adapters (shallow)...")
        repo_path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["git", "clone", "--depth", "1",
             "https://github.com/DefiLlama/DefiLlama-Adapters.git",
             str(repo_path)],
            check=True,
        )


def _discover_protocols(projects_dir: Path) -> list[Path]:
    """Find all protocol directories under projects/."""
    protocols = []
    for entry in sorted(projects_dir.iterdir()):
        if entry.is_dir() and entry.name != "helper":
            protocols.append(entry)
        elif entry.is_file() and entry.suffix in (".js", ".ts"):
            protocols.append(entry)
    return protocols


def scan_protocol(
    protocol_name: str,
    repo_path: Path | None = None,
    no_clone: bool = False,
) -> dict:
    """Scan a single protocol's adapters and return discovered addresses with chain context.

    Returns:
        {
            "protocol": "aave",
            "addresses": ["0x...", ...],
            "address_details": [{"address": "0x...", "chain": "ethereum", "source": "..."}],
            "scan_time": 1.2,
        }
    """
    repo_path = repo_path or DEFAULT_REPO_PATH

    if not no_clone:
        clone_or_update_repo(repo_path)

    projects_dir = repo_path / "projects"
    if not projects_dir.exists():
        raise FileNotFoundError(f"Projects directory not found: {projects_dir}")

    core_assets = load_core_assets(repo_path)
    addr_to_chain = build_address_to_chain_map(core_assets)

    # Find the matching protocol directory
    protocol_dirs = _discover_protocols(projects_dir)
    matching = [
        p for p in protocol_dirs
        if p.name == protocol_name or p.stem == protocol_name
    ]
    if not matching:
        logger.warning("Protocol '%s' not found in DefiLlama-Adapters", protocol_name)
        return {
            "protocol": protocol_name,
            "addresses": [],
            "address_details": [],
            "scan_time": 0,
        }

    start = time.time()
    proto_path = matching[0]

    if proto_path.is_file():
        addrs = extract_addresses_from_file(proto_path)
        result = {
            "protocol": proto_path.stem,
            "files_scanned": 1,
            "addresses": [
                {"address": a, "chain": None, "source": proto_path.name}
                for a in addrs
            ],
        }
    else:
        result = extract_protocol(proto_path)

    # Enrich chain info from core assets
    for entry in result["addresses"]:
        if not entry["chain"] and entry["address"] in addr_to_chain:
            entry["chain"] = addr_to_chain[entry["address"]]

    elapsed = time.time() - start
    unique_addrs = sorted({e["address"] for e in result["addresses"]})

    return {
        "protocol": protocol_name,
        "addresses": unique_addrs,
        "address_details": result["addresses"],
        "scan_time": elapsed,
    }


def scan_all_protocols(
    repo_path: Path | None = None,
    no_clone: bool = False,
) -> dict:
    """Scan all protocols in the DefiLlama-Adapters repo.

    Returns the full scan results dict with protocols, chain_summary, etc.
    """
    repo_path = repo_path or DEFAULT_REPO_PATH

    if not no_clone:
        clone_or_update_repo(repo_path)

    projects_dir = repo_path / "projects"
    if not projects_dir.exists():
        raise FileNotFoundError(f"Projects directory not found: {projects_dir}")

    core_assets = load_core_assets(repo_path)
    addr_to_chain = build_address_to_chain_map(core_assets)

    protocol_dirs = _discover_protocols(projects_dir)
    logger.info("Found %d protocols to scan", len(protocol_dirs))

    start = time.time()
    all_protocols = []
    all_unique: set[str] = set()

    for i, proto_path in enumerate(protocol_dirs):
        if proto_path.is_file():
            addrs = extract_addresses_from_file(proto_path)
            result = {
                "protocol": proto_path.stem,
                "files_scanned": 1,
                "addresses": [
                    {"address": a, "chain": None, "source": proto_path.name}
                    for a in addrs
                ],
            }
        else:
            result = extract_protocol(proto_path)

        for entry in result["addresses"]:
            if not entry["chain"] and entry["address"] in addr_to_chain:
                entry["chain"] = addr_to_chain[entry["address"]]
            all_unique.add(entry["address"])

        if result["addresses"]:
            all_protocols.append(result)

        if (i + 1) % 500 == 0:
            logger.info("Progress: %d/%d protocols", i + 1, len(protocol_dirs))

    elapsed = time.time() - start

    chain_counts: dict[str, int] = {}
    for proto in all_protocols:
        for entry in proto["addresses"]:
            chain = entry.get("chain") or "unknown"
            chain_counts[chain] = chain_counts.get(chain, 0) + 1

    return {
        "scan_time": elapsed,
        "protocols_scanned": len(protocol_dirs),
        "protocols_with_addresses": len(all_protocols),
        "unique_addresses": len(all_unique),
        "chain_summary": chain_counts,
        "protocols": all_protocols,
    }
