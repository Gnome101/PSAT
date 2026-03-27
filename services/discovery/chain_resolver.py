"""Multi-chain resolution for discovered contracts.

After the inventory pipeline builds contracts, some entries have
``chain="unknown"`` — typically from deployer expansion which only queries
Ethereum Etherscan.  This module probes the Etherscan V2 API across multiple
chains to determine the actual chain for each unknown-chain contract.

Strategy
--------
1. Collect all known chains from the inventory (the protocol is active on these).
2. For each unknown-chain contract, probe those chains first (cheap — usually
   hits on the first try), then fall back to a broader set.
3. Uses ``getsourcecode`` which returns verified source if the contract exists
   on that chain.  A non-error response means the address has code on that chain.
"""

from __future__ import annotations

import time
from typing import Any

from utils import etherscan

from .activity import CHAIN_IDS
from .inventory_domain import _debug_log

# Etherscan free-tier rate limit buffer.
_RATE_LIMIT_DELAY = 0.22


def _probe_chain(address: str, chain_id: int) -> bool:
    """Return True if *address* has a contract on the chain with *chain_id*."""
    try:
        etherscan.get("contract", "getsourcecode", chain_id=chain_id, address=address)
        return True
    except RuntimeError:
        return False


def resolve_unknown_chains(
    contracts: list[dict[str, Any]],
    debug: bool = False,
) -> list[dict[str, Any]]:
    """Resolve ``chain="unknown"`` entries by probing Etherscan V2 multi-chain.

    Mutates the contract dicts in-place (updates ``chain`` and ``chains``
    fields) and returns the same list.
    """
    if not contracts:
        return contracts

    unknowns = [c for c in contracts if c.get("chain") == "unknown"]
    if not unknowns:
        _debug_log(debug, "Chain resolution: no unknown-chain contracts to resolve")
        return contracts

    # Determine which chains this protocol is known to use — probe these first.
    known_chains: list[str] = []
    seen = set()
    for c in contracts:
        for ch in c.get("chains", []):
            if ch not in seen and ch != "unknown" and ch in CHAIN_IDS:
                known_chains.append(ch)
                seen.add(ch)

    # Fall back to all supported chains if no known chains.
    if not known_chains:
        known_chains = list(CHAIN_IDS.keys())

    # Remaining chains to try after the known set.
    remaining_chains = [ch for ch in CHAIN_IDS if ch not in seen]

    _debug_log(
        debug,
        f"Chain resolution: {len(unknowns)} unknown contract(s), "
        f"probing {len(known_chains)} known chain(s) first: {known_chains}",
    )

    resolved_count = 0
    for contract in unknowns:
        address = contract["address"]
        matched_chains: list[str] = []

        # Probe known protocol chains first.
        for chain_name in known_chains:
            chain_id = CHAIN_IDS[chain_name]
            if _probe_chain(address, chain_id):
                matched_chains.append(chain_name)
            time.sleep(_RATE_LIMIT_DELAY)

        # If nothing matched on known chains, try the rest.
        if not matched_chains:
            for chain_name in remaining_chains:
                chain_id = CHAIN_IDS[chain_name]
                if _probe_chain(address, chain_id):
                    matched_chains.append(chain_name)
                time.sleep(_RATE_LIMIT_DELAY)

        if matched_chains:
            if len(matched_chains) == 1:
                contract["chain"] = matched_chains[0]
            else:
                contract["chain"] = "multiple"
            contract["chains"] = matched_chains
            resolved_count += 1
            _debug_log(debug, f"  {address}: resolved to {matched_chains}")
        else:
            _debug_log(debug, f"  {address}: no chain match found")

    _debug_log(debug, f"Chain resolution: resolved {resolved_count}/{len(unknowns)} contract(s)")
    return contracts
