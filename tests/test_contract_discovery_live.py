import os
import re
import sys
from pathlib import Path
from typing import Any

import pytest
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.contract_discovery import search_contract_name
from services.contract_discovery_ai import search_contract_name_ai

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

ONEINCH_ROUTER_V5 = "0x1111111254eeb25477b68fb85ed929f73a960582"
ETHERFI_KING_DISTRIBUTOR = "0x6db24ee656843e3fe03eb8762a54d86186ba6b64"
ETHEREUM = "ethereum"


def _candidate_addresses(result: dict[str, Any]) -> set[str]:
    return {
        str(candidate.get("address", "")).lower()
        for candidate in result.get("candidates", [])
        if isinstance(candidate, dict)
    }


def _error_blob(errors: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for err in errors:
        if not isinstance(err, dict):
            continue
        parts.append(str(err.get("error", "")))
        parts.append(str(err.get("detail", "")))
    return " ".join(parts).lower()


def _looks_like_network_or_provider_outage(errors: list[dict[str, Any]]) -> bool:
    blob = _error_blob(errors)
    if not blob:
        return False
    needles = (
        "failed to resolve",
        "name or service not known",
        "temporary failure in name resolution",
        "max retries exceeded",
        "connection",
        "timed out",
        "timeout",
        "service unavailable",
        "http 5",
        "http 429",
        "rate limit",
    )
    return any(needle in blob for needle in needles)


def _parse_tavily_budget(notes: list[str]) -> tuple[int, int] | None:
    for note in notes:
        match = re.search(r"Tavily queries used:\s*(\d+)\s*/\s*(\d+)", note)
        if match:
            return int(match.group(1)), int(match.group(2))
    return None


def test_live_contract_discovery_oneinch_v5_router():
    query = "1inch Aggregation Router V5"
    result = search_contract_name(query, chain=ETHEREUM, limit=10)

    # Shape + contract-discovery pipeline surface assertions.
    assert result["query"] == query
    assert result["chain"] == ETHEREUM
    assert isinstance(result.get("errors"), list)
    assert isinstance(result.get("candidates"), list)

    if not result["candidates"] and _looks_like_network_or_provider_outage(result["errors"]):
        pytest.skip(f"Live explorer dependency unavailable: {result['errors']}")

    assert result["candidates"], f"No candidates returned: {result}"

    # Rank order should be descending confidence.
    confidences = [float(c["confidence"]) for c in result["candidates"]]
    assert confidences == sorted(confidences, reverse=True)

    # All candidates should be ethereum when chain filter is set.
    for candidate in result["candidates"]:
        assert candidate["chain"] == ETHEREUM
        assert candidate["source"] == "blockscout"
        assert "blockscout" in candidate["links"]
        assert str(candidate["address"]).startswith("0x")

    # Live correctness check for the known 1inch V5 router.
    addresses = _candidate_addresses(result)
    assert ONEINCH_ROUTER_V5 in addresses, f"Expected {ONEINCH_ROUTER_V5}, got {sorted(addresses)}"

    # If auto-selected, best_candidate must be a member of ranked candidates.
    if result.get("best_candidate"):
        best = str(result["best_candidate"].get("address", "")).lower()
        assert best in addresses


def test_live_contract_discovery_ai_etherfi_king_distributor():
    if not os.environ.get("TAVILY_API_KEY", "").strip():
        pytest.skip("Set TAVILY_API_KEY before running this live test.")
    if not os.environ.get("OPEN_ROUTER_KEY", "").strip():
        pytest.skip("Set OPEN_ROUTER_KEY before running this live test.")

    result = search_contract_name_ai(
        company="etherfi",
        contract_name="KING Distributor",
        chain=None,
        limit=10,
        max_queries=4,
    )

    # Shape + step-surface assertions (domain selection, query budget, ranking output).
    assert result["query"] == "KING Distributor"
    assert result["company"] == "etherfi"
    assert result["chain"] == "any"
    assert isinstance(result.get("notes"), list)
    assert isinstance(result.get("errors"), list)
    assert isinstance(result.get("domain_candidates"), list)
    assert isinstance(result.get("candidates"), list)

    if not result["candidates"] and _looks_like_network_or_provider_outage(result["errors"]):
        pytest.skip(f"Live AI dependencies unavailable: {result['errors']}")

    # Domain resolution should succeed when API dependencies are available.
    assert result.get("official_domain"), f"Official domain not resolved: {result}"
    assert result["domain_candidates"], f"No domain candidates resolved: {result}"
    assert result["official_domain"] in result["domain_candidates"]

    budget = _parse_tavily_budget(result["notes"])
    assert budget is not None, f"Missing Tavily budget note in {result['notes']}"
    used, cap = budget
    assert cap == 4
    assert 0 <= used <= cap

    # Live correctness check for the expected Ether.fi KING Distributor contract.
    addresses = _candidate_addresses(result)
    assert addresses, f"No AI candidates returned: {result}"
    assert ETHERFI_KING_DISTRIBUTOR in addresses, (
        f"Expected {ETHERFI_KING_DISTRIBUTOR}, got {sorted(addresses)}"
    )

    target = next(
        c for c in result["candidates"] if str(c.get("address", "")).lower() == ETHERFI_KING_DISTRIBUTOR
    )
    assert target["chain"] in {"ethereum", "arbitrum", "optimism", "polygon", "base", "unknown"}
    assert target["source"] == "tavily_ai"
    assert isinstance(target.get("reasons"), list) and target["reasons"]
    assert isinstance(target.get("links"), dict) and target["links"]

    if result.get("best_candidate"):
        best = str(result["best_candidate"].get("address", "")).lower()
        assert best in addresses
