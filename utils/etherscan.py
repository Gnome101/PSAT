"""Etherscan API client."""

import os
from pathlib import Path

import requests
from dotenv import load_dotenv

ETHERSCAN_API = "https://api.etherscan.io/v2/api"


def _get_api_key() -> str:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    key = os.getenv("ETHERSCAN_API_KEY")
    if not key:
        raise RuntimeError("ETHERSCAN_API_KEY not set in .env")
    return key


def get(module: str, action: str, **params) -> dict:
    """Make an Etherscan API call. Returns the parsed JSON response."""
    api_key = _get_api_key()
    resp = requests.get(
        ETHERSCAN_API,
        params={
            "chainid": "1",
            "module": module,
            "action": action,
            "apikey": api_key,
            **params,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "1":
        raise RuntimeError(f"Etherscan error: {data.get('message', 'unknown')} - {data.get('result', '')}")

    return data


def get_source(address: str) -> dict:
    """Fetch verified source code for a contract address. Returns the first result."""
    data = get("contract", "getsourcecode", address=address)
    result = data["result"][0]

    if not result.get("SourceCode"):
        raise RuntimeError(f"No verified source code for {address}")

    return result
