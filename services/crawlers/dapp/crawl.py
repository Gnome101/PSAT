"""
Callable entry point for DApp crawling.

Importable function used by ``workers.dapp_crawl_worker``.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import asdict
from typing import Callable

from services.crawlers.dapp.browser import DAppCrawler
from services.crawlers.dapp.interaction_log import InteractionLog
from services.crawlers.dapp.wallet import HoneypotWallet

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str], None]

# Hard cap on a single ``crawl_dapp`` call. A stuck wallet-connect flow
# (SDK hung on MetaMask modal, stalled network request, etc.) used to
# wedge the DApp crawl worker until the 15-minute stale-job sweep ran —
# and even then, the worker's blocked ``asyncio.run`` didn't release
# playwright, so the next attempt sat on the re-queued row with nobody
# polling. Enforcing a global timeout inside the async wrapper lets the
# worker raise cleanly and mark the job failed.
_DAPP_CRAWL_TIMEOUT_SECONDS = int(os.environ.get("PSAT_DAPP_CRAWL_TIMEOUT", "300"))


async def _crawl_async(
    urls: list[str],
    *,
    chain_id: int = 1,
    eth_balance: str = "0x3635C9ADC5DEA00000",
    token_balance: str = "0x84595161401484A000000",
    wait: int = 10,
    progress: ProgressCallback | None = None,
) -> InteractionLog:
    wallet = HoneypotWallet()
    logger.info("Honeypot wallet: %s", wallet.address)
    if progress:
        progress(f"Launching browser with wallet {wallet.address[:8]}...")

    crawler = DAppCrawler(
        wallet=wallet,
        chain_id=chain_id,
        eth_balance=eth_balance,
        token_balance=token_balance,
        headless=True,
    )

    logger.info("Crawling %d URLs...", len(urls))
    interaction_log = await crawler.crawl(urls, wait_seconds=wait, progress=progress)
    return interaction_log


def crawl_dapp(
    urls: list[str],
    *,
    chain_id: int = 1,
    wait: int = 10,
    progress: ProgressCallback | None = None,
) -> dict:
    """Crawl DApp URLs and return discovered contract addresses.

    This is the main entry point for the PSAT worker. Runs the async
    crawler synchronously and returns results as a dict.

    Returns:
        {
            "addresses": ["0x...", ...],
            "interactions": [...],
            "session_start": "...",
        }
    """
    async def _bounded() -> InteractionLog:
        return await asyncio.wait_for(
            _crawl_async(
                urls,
                chain_id=chain_id,
                wait=wait,
                progress=progress,
            ),
            timeout=_DAPP_CRAWL_TIMEOUT_SECONDS,
        )

    try:
        interaction_log = asyncio.run(_bounded())
    except asyncio.TimeoutError as exc:
        logger.error(
            "DApp crawl exceeded %ds limit — aborting so the worker can mark the job failed",
            _DAPP_CRAWL_TIMEOUT_SECONDS,
        )
        raise RuntimeError(
            f"DApp crawl exceeded {_DAPP_CRAWL_TIMEOUT_SECONDS}s — likely a hung wallet-connect "
            f"or stalled page load on {urls!r}"
        ) from exc

    addresses = interaction_log.get_contract_addresses()
    logger.info("Discovered %d unique contract addresses", len(addresses))

    return {
        "addresses": addresses,
        "address_details": interaction_log.get_address_details(),
        "interactions": [asdict(i) for i in interaction_log.interactions],
        "interaction_count": len(interaction_log.interactions),
        "session_start": interaction_log.session_start,
    }
