"""DApp crawler — honeypot wallet that discovers smart contract interactions."""

from services.crawlers.dapp.interaction_log import CapturedInteraction, InteractionLog


def __getattr__(name: str):
    if name == "HoneypotWallet":
        from services.crawlers.dapp.wallet import HoneypotWallet

        return HoneypotWallet
    if name == "DAppCrawler":
        from services.crawlers.dapp.browser import DAppCrawler

        return DAppCrawler
    if name == "crawl_dapp":
        from services.crawlers.dapp.crawl import crawl_dapp

        return crawl_dapp
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "InteractionLog",
    "CapturedInteraction",
    "HoneypotWallet",
    "DAppCrawler",
    "crawl_dapp",
]
