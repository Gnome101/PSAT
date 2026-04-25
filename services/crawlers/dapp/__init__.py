"""DApp crawler — honeypot wallet that discovers smart contract interactions."""

from services.crawlers.dapp.interaction_log import CapturedInteraction, InteractionLog

__all__ = ["InteractionLog", "CapturedInteraction"]
