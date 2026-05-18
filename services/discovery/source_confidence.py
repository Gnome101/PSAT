"""Tier discovery sources by whether they assert protocol ownership.

A "discovery" signal proves we saw an address while looking at protocol X.
An "ownership" signal proves protocol X actually deploys or controls the
contract. Many sources do the former — page scraping, LLM extraction,
proxy upgrade traversal — and most callers conflate the two by stamping
``Contract.protocol_id`` on first sight. That conflation is how widely-
held collateral tokens (WETH, stETH, OP-Stack predeploys) end up in a
protocol's inventory just because the protocol's UI mentions them.

Only HIGH_CONFIDENCE sources may stamp ``protocol_id``. Low-confidence
sources still create / annotate Contract rows so the audit trail of
"who saw this" survives, but the row stays orphan (``protocol_id=NULL``)
until a high-confidence corroboration arrives. See ``db/queue.py`` and
``services/discovery/upgrade_history.py`` for the gates that consume
this classification.

Adding a new source: default to LOW_CONFIDENCE and only promote it after
confirming, on real data, that it doesn't pull in third-party
infrastructure. Sources not listed in either set are treated as low-
confidence — safer to require an explicit promotion than to opt new
sources in by default.
"""

from __future__ import annotations

# Sources that prove the address is deployed or controlled by the
# protocol — deployer-verified addresses, curated lists, or LLM-extracted
# entries from the protocol's own docs. These may stamp
# ``Contract.protocol_id``.
HIGH_CONFIDENCE_SOURCES: frozenset[str] = frozenset(
    {
        "deployer_expansion",  # known deployer + CREATE/CREATE2 lineage
        "defillama",  # curated public list of protocol contracts
        "ai_inventory",  # LLM extraction from official docs
        "exa_deep_research",  # research scan over official sources
        "inventory",  # admin/discovery-pipeline curated entry
        "spa_override",  # admin override entry
        "dependency_two_pass",  # static dependency from a confirmed contract
    }
)

# Sources that only prove the address was encountered while looking at
# the protocol — scraping every 0x... on a DApp UI, or walking a proxy's
# upgrade history when the proxy itself isn't confirmed-owned. They
# populate ``Contract.discovery_sources`` but must not stamp
# ``Contract.protocol_id`` on their own.
LOW_CONFIDENCE_SOURCES: frozenset[str] = frozenset(
    {
        "dapp_crawl",  # UI scrape — picks up every collateral / router / bridge
        "upgrade_history",  # only as strong as the parent proxy's own confirmation
    }
)


def asserts_ownership(sources: list[str] | None) -> bool:
    """True iff ``sources`` contains at least one HIGH_CONFIDENCE tag.

    Empty / None counts as low-confidence (no positive assertion). An
    unknown source also counts as low-confidence — see module docstring
    for why we don't opt new sources in by default.
    """
    if not sources:
        return False
    return any(s in HIGH_CONFIDENCE_SOURCES for s in sources)
