"""
Honeypot wallet management.

Generates or loads a throwaway private key used to legitimately sign
auth messages. The wallet never holds real funds - balances are spoofed
at the RPC level.
"""

import json
import os
from pathlib import Path

from eth_account import Account
from eth_account.messages import encode_defunct, encode_typed_data


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
        if isinstance(typed_data, str):
            typed_data = json.loads(typed_data)

        signed = self.account.sign_typed_data(
            typed_data.get("domain", {}),
            typed_data.get("types", {}),
            typed_data.get("message", {}),
        )
        return signed.signature.hex()

    def save(self, path: str | Path):
        """Persist wallet to disk for reuse across sessions."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {"address": self.address, "private_key": self.private_key},
                indent=2,
            )
        )

    @classmethod
    def load(cls, path: str | Path) -> "HoneypotWallet":
        """Load a previously saved wallet."""
        data = json.loads(Path(path).read_text())
        return cls(private_key=data["private_key"])

    @classmethod
    def load_or_create(cls, path: str | Path) -> "HoneypotWallet":
        """Load existing wallet or create and save a new one."""
        path = Path(path)
        if path.exists():
            return cls.load(path)
        wallet = cls()
        wallet.save(path)
        return wallet
