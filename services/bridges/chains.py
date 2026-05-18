"""Shared chain metadata for bridge runtime resolution.

Bridge protocols identify destinations with different namespaces:
LayerZero EIDs, Hyperlane domains, EVM chain IDs, or protocol-specific
labels. Keep the common EVM chain facts in one place so runtime
resolvers, source fetching, and peer queueing agree on chain names.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ChainInfo:
    name: str
    display_name: str
    chain_id: int | None = None
    alchemy_slug: str | None = None
    hyperlane_domain: int | None = None
    public_rpc_url: str | None = None


CHAIN_REGISTRY: dict[str, ChainInfo] = {
    "ethereum": ChainInfo("ethereum", "Ethereum", 1, "eth-mainnet", 1),
    "mainnet": ChainInfo("ethereum", "Ethereum", 1, "eth-mainnet", 1),
    "arbitrum": ChainInfo("arbitrum", "Arbitrum", 42161, "arb-mainnet", 42161),
    "optimism": ChainInfo("optimism", "Optimism", 10, "opt-mainnet", 10),
    "polygon": ChainInfo("polygon", "Polygon", 137, "polygon-mainnet", 137),
    "base": ChainInfo("base", "Base", 8453, "base-mainnet", 8453),
    "avalanche": ChainInfo("avalanche", "Avalanche", 43114, "avax-mainnet", 43114),
    "bsc": ChainInfo("bsc", "BNB Chain", 56, "bnb-mainnet", 56),
    "linea": ChainInfo("linea", "Linea", 59144, "linea-mainnet", 59144),
    "zkconsensys": ChainInfo("linea", "Linea", 59144, "linea-mainnet", 59144),
    "scroll": ChainInfo("scroll", "Scroll", 534352, "scroll-mainnet", 534352),
    "zksync": ChainInfo("zksync", "zkSync Era", 324, "zksync-mainnet", 324),
    "blast": ChainInfo("blast", "Blast", 81457, "blast-mainnet", 81457),
    "mode": ChainInfo("mode", "Mode", 34443, None, 34443, "https://mainnet.mode.network"),
    "bera": ChainInfo("bera", "Berachain", 80094, None, 80094, "https://rpc.berachain.com"),
    "berachain": ChainInfo("bera", "Berachain", 80094, None, 80094, "https://rpc.berachain.com"),
}


def normalize_chain_name(chain: str | None) -> str | None:
    if not isinstance(chain, str) or not chain.strip():
        return None
    key = chain.lower().strip()
    return CHAIN_REGISTRY.get(key, ChainInfo(key, chain)).name


def chain_info(chain: str | None) -> ChainInfo | None:
    name = normalize_chain_name(chain)
    if not name:
        return None
    return CHAIN_REGISTRY.get(name) or CHAIN_REGISTRY.get(str(chain).lower().strip()) or ChainInfo(name, name)


def chain_id_for_chain(chain: str | None, default: int | None = None) -> int | None:
    info = chain_info(chain)
    return info.chain_id if info and info.chain_id is not None else default


def chain_name_for_chain_id(chain_id: int | None) -> str | None:
    if chain_id is None:
        return None
    seen: set[str] = set()
    for info in CHAIN_REGISTRY.values():
        if info.name in seen:
            continue
        seen.add(info.name)
        if info.chain_id == chain_id:
            return info.name
    return None


def display_name_for_chain(chain: str | None) -> str | None:
    info = chain_info(chain)
    return info.display_name if info else chain


def hyperlane_domain_entries() -> tuple[dict[str, int | str], ...]:
    seen: set[int] = set()
    entries: list[dict[str, int | str]] = []
    for info in CHAIN_REGISTRY.values():
        if info.hyperlane_domain is None or info.hyperlane_domain in seen:
            continue
        seen.add(info.hyperlane_domain)
        entries.append(
            {
                "domain": info.hyperlane_domain,
                "chain": info.name,
                "display_name": info.display_name,
                "chain_id": info.chain_id or info.hyperlane_domain,
            }
        )
    return tuple(sorted(entries, key=lambda item: int(item["domain"])))


def rpc_url_for_chain(chain: str | None, default_rpc_url: str | None = None) -> str | None:
    """Return a chain-specific RPC URL when the configured default supports it.

    The existing deployment convention uses an Alchemy URL in ``ETH_RPC``.
    When that is available we can derive sibling mainnet URLs; otherwise we
    derive sibling mainnet URLs; otherwise known public RPC fallbacks are
    used for chains that need them.
    """
    info = chain_info(chain)
    if info is None:
        return None
    configured = default_rpc_url or os.getenv("ETH_RPC")
    override_key = f"PSAT_RPC_{info.name.upper().replace('-', '_')}"
    if os.getenv(override_key):
        return os.getenv(override_key)
    if info.name == "ethereum":
        return configured
    if not configured or "/v2/" not in configured or not info.alchemy_slug:
        return info.public_rpc_url
    api_key = configured.rstrip("/").rsplit("/", 1)[-1]
    if not api_key:
        return None
    return f"https://{info.alchemy_slug}.g.alchemy.com/v2/{api_key}"


def rpc_url_for_runtime_chain(chain: str | None, default_rpc_url: str | None = None) -> str | None:
    """Return an RPC for live reads without inventing Ethereum for explicit chains."""
    if isinstance(chain, str) and chain.strip():
        return rpc_url_for_chain(chain, default_rpc_url)
    return default_rpc_url or os.getenv("ETH_RPC")
