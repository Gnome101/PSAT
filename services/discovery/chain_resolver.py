"""Multi-chain resolution for discovered contracts.

After the inventory pipeline builds contracts, some entries have
``chain="unknown"``.  This module probes ``eth_getCode`` via JSON-RPC
batch requests to Alchemy endpoints to determine where each contract
is actually deployed.

Requires ``ETH_RPC`` to be set to an Alchemy URL
(``https://<network>.g.alchemy.com/v2/<key>``).  The API key is
extracted and used to derive per-chain endpoints so all chains can be
probed **in parallel** (~1–2 seconds for hundreds of addresses across
10+ chains).

Strategy
--------
1. Extract the Alchemy API key from ``ETH_RPC``.
2. **Phase 1** — probe every unknown address on every known chain in
   parallel using JSON-RPC batch requests.
3. **Phase 2** — for addresses that matched nothing in phase 1, probe
   the remaining supported chains (also in parallel).
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from .activity import CHAIN_IDS
from .inventory_domain import _debug_log
from .static_dependencies import (
    batch_get_code,
    has_deployed_code,
)

# Alchemy network slugs matching CHAIN_IDS keys.
_ALCHEMY_CHAIN_SLUGS: dict[str, str] = {
    "ethereum": "eth-mainnet",
    "arbitrum": "arb-mainnet",
    "optimism": "opt-mainnet",
    "polygon": "polygon-mainnet",
    "base": "base-mainnet",
    "avalanche": "avax-mainnet",
    "bsc": "bnb-mainnet",
    "linea": "linea-mainnet",
    "scroll": "scroll-mainnet",
    "zksync": "zksync-mainnet",
    "blast": "blast-mainnet",
}


def _get_alchemy_key() -> str:
    """Extract the Alchemy API key from ETH_RPC."""
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    rpc = os.getenv("ETH_RPC", "")
    # Key is the last path segment: https://<slug>.g.alchemy.com/v2/<key>
    key = rpc.rstrip("/").rsplit("/", 1)[-1] if "/v2/" in rpc else ""
    if not key:
        raise RuntimeError(
            "Chain resolution requires ETH_RPC set to an Alchemy URL "
            "(https://<network>.g.alchemy.com/v2/<key>)"
        )
    return key


def _alchemy_rpc(chain_name: str, api_key: str) -> str | None:
    """Build an Alchemy RPC URL for a given chain."""
    slug = _ALCHEMY_CHAIN_SLUGS.get(chain_name)
    if not slug:
        return None
    return f"https://{slug}.g.alchemy.com/v2/{api_key}"


def _probe_chain_batch(
    addresses: list[str],
    chain_name: str,
    api_key: str,
    debug: bool = False,
) -> set[str]:
    """Probe all *addresses* on a single chain via Alchemy JSON-RPC batch."""
    rpc_url = _alchemy_rpc(chain_name, api_key)
    if not rpc_url:
        _debug_log(debug, f"  {chain_name}: no Alchemy slug configured, skipping")
        return set()

    try:
        code_map = batch_get_code(rpc_url, addresses)
        return {addr for addr, code in code_map.items() if has_deployed_code(code)}
    except Exception as exc:
        _debug_log(debug, f"  {chain_name}: probe failed: {exc!r}")
        return set()


def _probe_chains(
    addresses: list[str],
    chains: list[str],
    api_key: str,
    matched: dict[str, list[str]],
    debug: bool = False,
) -> None:
    """Probe multiple chains in parallel using Alchemy batch endpoints."""
    with ThreadPoolExecutor(max_workers=min(len(chains), 10)) as executor:
        future_to_chain = {
            executor.submit(_probe_chain_batch, addresses, chain_name, api_key, debug): chain_name
            for chain_name in chains
        }
        for future in as_completed(future_to_chain):
            chain_name = future_to_chain[future]
            try:
                hits = future.result()
                for addr in hits:
                    matched[addr].append(chain_name)
                _debug_log(debug, f"  {chain_name}: {len(hits)} hit(s)")
            except Exception as exc:
                _debug_log(debug, f"  {chain_name}: probe failed: {exc!r}")


def resolve_unknown_chains(
    contracts: list[dict[str, Any]],
    debug: bool = False,
) -> list[dict[str, Any]]:
    """Resolve ``chain="unknown"`` entries by probing ``eth_getCode`` across chains.

    Mutates the contract dicts in-place (updates ``chain`` and ``chains``
    fields) and returns the same list.
    """
    if not contracts:
        return contracts

    unknowns = [c for c in contracts if c.get("chain") == "unknown"]
    if not unknowns:
        _debug_log(debug, "Chain resolution: no unknown-chain contracts to resolve")
        return contracts

    api_key = _get_alchemy_key()

    # Warn about chains in CHAIN_IDS that have no Alchemy slug configured.
    uncovered = sorted(ch for ch in CHAIN_IDS if ch not in _ALCHEMY_CHAIN_SLUGS)
    if uncovered:
        _debug_log(debug, f"WARNING: no Alchemy slug for chain(s): {uncovered} — these will be skipped")

    # Determine which chains this protocol is known to use — probe these first.
    known_chains: list[str] = []
    seen: set[str] = set()
    for c in contracts:
        for ch in c.get("chains", []):
            if ch not in seen and ch != "unknown" and ch in CHAIN_IDS:
                known_chains.append(ch)
                seen.add(ch)

    if not known_chains:
        known_chains = list(CHAIN_IDS.keys())

    remaining_chains = [ch for ch in CHAIN_IDS if ch not in seen]

    _debug_log(
        debug,
        f"Chain resolution: {len(unknowns)} unknown contract(s), "
        f"probing {len(known_chains)} known chain(s): {known_chains}",
    )

    # address → list of chains where it has code
    matched: dict[str, list[str]] = {c["address"]: [] for c in unknowns}
    all_addrs = list(matched.keys())

    # Phase 1: probe ALL unknowns on ALL known chains in parallel.
    _probe_chains(all_addrs, known_chains, api_key, matched, debug)

    # Phase 2: for addresses that matched NOTHING on known chains, probe the
    # remaining chains in parallel.
    unresolved = [addr for addr, chains in matched.items() if not chains]
    if unresolved and remaining_chains:
        _debug_log(debug, f"Probing {len(remaining_chains)} remaining chain(s) for {len(unresolved)} address(es)")
        _probe_chains(unresolved, remaining_chains, api_key, matched, debug)

    # Apply results.
    resolved_count = 0
    for contract in unknowns:
        chains = matched.get(contract["address"], [])
        if chains:
            contract["chain"] = chains[0] if len(chains) == 1 else "multiple"
            contract["chains"] = chains
            resolved_count += 1
            _debug_log(debug, f"  {contract['address']}: resolved to {chains}")

    _debug_log(debug, f"Chain resolution: resolved {resolved_count}/{len(unknowns)} contract(s)")
    return contracts
