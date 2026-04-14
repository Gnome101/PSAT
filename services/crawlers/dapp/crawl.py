"""
Callable entry point for DApp crawling.

Extracts the core logic from the CLI main.py into an importable function
that the PSAT worker can call directly.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Callable

from services.crawlers.dapp.browser import DAppCrawler
from services.crawlers.dapp.interaction_log import InteractionLog
from services.crawlers.dapp.wallet import HoneypotWallet

logger = logging.getLogger(__name__)

WALLET_PATH = Path(__file__).resolve().parents[3] / ".wallet.json"
ProgressCallback = Callable[[str], None]


async def _crawl_async(
    urls: list[str],
    *,
    chain_id: int = 1,
    eth_balance: str = "0x3635C9ADC5DEA00000",
    token_balance: str = "0x84595161401484A000000",
    wait: int = 10,
    wallet_path: Path | None = None,
    progress: ProgressCallback | None = None,
) -> InteractionLog:
    wallet = HoneypotWallet.load_or_create(wallet_path or WALLET_PATH)
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
    wallet_path: Path | None = None,
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
    interaction_log = asyncio.run(
        _crawl_async(
            urls,
            chain_id=chain_id,
            wait=wait,
            wallet_path=wallet_path,
            progress=progress,
        )
    )

    addresses = interaction_log.get_contract_addresses()
    logger.info("Discovered %d unique contract addresses", len(addresses))

    return {
        "addresses": addresses,
        "address_details": interaction_log.get_address_details(),
        "interactions": [asdict(i) for i in interaction_log.interactions],
        "interaction_count": len(interaction_log.interactions),
        "session_start": interaction_log.session_start,
    }
