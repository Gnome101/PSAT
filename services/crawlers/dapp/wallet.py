"""
Honeypot wallet management.

Generates a throwaway private key used to legitimately sign auth messages
during DApp crawls. The wallet never holds real funds - balances are spoofed
at the RPC level. A fresh key is generated per crawl session (in-memory only).
"""

import json

from eth_account import Account
from eth_account.messages import encode_defunct


class HoneypotWallet:
    """A real wallet with a real private key, but no real funds."""

    def __init__(self, private_key: str | None = None):
        if private_key:
            self.account = Account.from_key(private_key)
        else:
            self.account = Account.create()

        self.address = self.account.address
        self.private_key = self.account.key.hex()

    def sign_message(self, message: str) -> str:
        """Sign a personal_sign message (used for SIWE, auth, etc.)."""
        if message.startswith("0x"):
            msg_bytes = bytes.fromhex(message[2:])
            signable = encode_defunct(primitive=msg_bytes)
        else:
            signable = encode_defunct(text=message)

        signed = self.account.sign_message(signable)
        return signed.signature.hex()

    def sign_typed_data(self, typed_data: str | dict) -> str:
        """Sign EIP-712 typed data (permits, auth, etc.)."""
        data: dict = json.loads(typed_data) if isinstance(typed_data, str) else typed_data

        signed = self.account.sign_typed_data(
            data.get("domain", {}),
            data.get("types", {}),
            data.get("message", {}),
        )
        return signed.signature.hex()
