"""Small chain registry used by bridge runtime resolvers."""

from __future__ import annotations

import os
from typing import TypedDict

from utils.rpc import PUBLIC_ETH_RPC_URL, chain_id_for_chain_name, erpc_url_for_chain_id


class LayerZeroEidEntry(TypedDict):
    eid: int
    network: str
    chain: str
    chain_type: str


_DISPLAY_NAMES = {
    "ethereum": "Ethereum",
    "mainnet": "Ethereum",
    "arbitrum": "Arbitrum",
    "optimism": "Optimism",
    "polygon": "Polygon",
    "base": "Base",
    "avalanche": "Avalanche",
    "bsc": "BNB Chain",
    "linea": "Linea",
    "scroll": "Scroll",
    "zksync": "zkSync Era",
    "blast": "Blast",
    "mode": "Mode",
    "bera": "Berachain",
    "berachain": "Berachain",
}

_ALIASES = {
    "eth": "ethereum",
    "mainnet": "ethereum",
    "bnb": "bsc",
    "bnbchain": "bsc",
    "zkconsensys": "linea",
    "zksyncera": "zksync",
    "berachain": "bera",
}

_LAYERZERO_EIDS: tuple[LayerZeroEidEntry, ...] = (
    {"eid": 30101, "network": "ethereum-mainnet", "chain": "ethereum", "chain_type": "evm"},
    {"eid": 30102, "network": "bsc-mainnet", "chain": "bsc", "chain_type": "evm"},
    {"eid": 30106, "network": "avalanche-mainnet", "chain": "avalanche", "chain_type": "evm"},
    {"eid": 30109, "network": "polygon-mainnet", "chain": "polygon", "chain_type": "evm"},
    {"eid": 30110, "network": "arbitrum-mainnet", "chain": "arbitrum", "chain_type": "evm"},
    {"eid": 30111, "network": "optimism-mainnet", "chain": "optimism", "chain_type": "evm"},
    {"eid": 30165, "network": "zksync-mainnet", "chain": "zksync", "chain_type": "evm"},
    {"eid": 30183, "network": "zkconsensys-mainnet", "chain": "linea", "chain_type": "evm"},
    {"eid": 30184, "network": "base-mainnet", "chain": "base", "chain_type": "evm"},
    {"eid": 30214, "network": "scroll-mainnet", "chain": "scroll", "chain_type": "evm"},
    {"eid": 30243, "network": "blast-mainnet", "chain": "blast", "chain_type": "evm"},
    {"eid": 30260, "network": "mode-mainnet", "chain": "mode", "chain_type": "evm"},
    {"eid": 30362, "network": "bera-mainnet", "chain": "bera", "chain_type": "evm"},
)


def normalize_chain_name(chain: str | None) -> str | None:
    if not isinstance(chain, str) or not chain.strip():
        return None
    key = "".join(ch for ch in chain.lower().strip() if ch.isalnum())
    return _ALIASES.get(key) or chain.lower().strip()


def chain_id_for_chain(chain: str | None) -> int | None:
    return chain_id_for_chain_name(normalize_chain_name(chain))


def display_name_for_chain(chain: str | None) -> str | None:
    normalized = normalize_chain_name(chain)
    if not normalized:
        return None
    return _DISPLAY_NAMES.get(normalized, normalized)


def layerzero_eid_entries() -> tuple[LayerZeroEidEntry, ...]:
    return _LAYERZERO_EIDS


def chain_name_for_layerzero_eid(eid: int | None) -> str | None:
    if eid is None:
        return None
    for entry in _LAYERZERO_EIDS:
        if entry["eid"] == eid:
            return entry["chain"]
    return None


def rpc_url_for_chain(chain: str | None, default_rpc_url: str | None = None) -> str | None:
    normalized = normalize_chain_name(chain)
    if normalized is None:
        return None
    chain_id = chain_id_for_chain(normalized)
    if chain_id is None:
        return None

    override_key = f"PSAT_RPC_{normalized.upper().replace('-', '_')}"
    override = os.getenv(override_key)
    if override:
        return override

    erpc_url = erpc_url_for_chain_id(chain_id)
    if erpc_url:
        return erpc_url

    if normalized == "ethereum":
        return default_rpc_url or os.getenv("ETH_RPC") or PUBLIC_ETH_RPC_URL
    return None
