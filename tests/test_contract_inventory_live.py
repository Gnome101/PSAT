#run live test with accuracy metrics with uv run python -m pytest -s tests/test_contract_inventory_live.py
from collections import Counter
import os
import re
import sys
from pathlib import Path
from typing import Any

import pytest
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.contract_inventory_ai import search_protocol_inventory

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

ETHERFI_EXPECTED_ADDRESSES = {
    "0x00c452affee3a17d9cecc1bcd2b8d5c7635c4cb9",
    "0x01f0a31698c4d065659b9bdc21b3610292a1c506",
    "0x04c0599ae5a44757c0af6f9ec3b93da8976c150a",
    "0x0c1e1a20566321de81841e61a75b2b949610cb39",
    "0x0ef8fa4760db8f5cd4d993f3e3416f30f942d705",
    "0x18fa07df94b4e9f09844e1128483801b24fe8a27",
    "0x1b7a4c3797236a1c37f8741c0be35c2c72736fff",
    "0x1baa2146e5b258a2cc516166a095dbc22caacfe6",
    "0x1bf74c010e6320bab11e2e5a532b5ac15e0b8aa6",
    "0x2093bbb221f1d8c7c932c32ee28be6dee4a37a6a",
    "0x227dd729c7ca1eb91c22dac0c4b1abad75b8365a",
    "0x25e821b7197b146f7713c3b89b6a4d83516b912d",
    "0x2882f978460d1229f1d0414ce91d0061b81adc2c",
    "0x2c999fd1543dd5a228acd0173092af10e3a8eeda",
    "0x2ec90ef34e312a855becf74762d198d8369eece1",
    "0x308861a430be4cce5502d0a12724771fc6daf216",
    "0x333321a783f765bfd4c22fbbc5b2d02b97efb44c",
    "0x35751007a407ca6feffe80b3cb397736d2cf4dbe",
    "0x35fa164735182de50811e8e2e824cfb9b6118ac2",
    "0x390624d61f03075d7e14d909d6c3f46ecac8b984",
    "0x3d320286e014c3e1ce99af6d6b00f0c1d63e3000",
    "0x44c00821f0e70f00b7af74235981eb30beb3577f",
    "0x4dca5093e0bb450d7f7961b5df0a9d4c24b24786",
    "0x4dea1271dfae80f4f3324b3a50c33abdbea89a57",
    "0x4e9fa586862183a944aa8a6e158af47ccae544e2",
    "0x5423885b376ebb4e6104b8ab1a908d350f6a162e",
    "0x57aaf0004c716388b21795431cd7d5f9d3bb6a41",
    "0x5a7facb970d094b6c7ff1df0ea68d99e6e73cbff",
    "0x62247d29b4b9becf4bb73e0c722cf6445cfc7ce9",
    "0x6329004e903b7f420245e7af3f355186f2432466",
    "0x637ee65658cb8f6524c051f76677e791ddc10bd4",
    "0x64776b0907b839e759f91a5a328ea143067ddcd7",
    "0x657e8c867d8b37dcc18fa4caead9c45eb088c642",
    "0x6599861e55abd28b91dd9d86a826ec0cc8d72c2c",
    "0x6c240dda6b5c336df09a4d011139beaaa1ea2aa2",
    "0x6db24ee656843e3fe03eb8762a54d86186ba6b64",
    "0x7189fb5b6504bbff6a852b13b7b82a3c118fdc27",
    "0x73f7b1184b5cd361cc0f7654998953e2a251dd58",
    "0x7623e9dc0da6ff821ddb9ebaba794054e078f8c4",
    "0x79ed14b41a30215e168d58fe9b6c2266a89765b0",
    "0x7aec93210fd857bfb1e7919cb9ef30731494c003",
    "0x7b5ae07e2af1c861bcc4736d23f5f66a61e0ca5e",
    "0x7b6a67f1031c1d8c7bab1cf001bdaf83271241fb",
    "0x7d372c3ca903ca2b6ecd8600d567eb6bafc5e6c9",
    "0x7d5706f6ef3f89b3951e23e557cdfbc3239d4e2c",
    "0x7dcc39b4d1c53cb31e1abc0e358b43987fef80f7",
    "0x8487c5f8550e3c3e7734fe7dcf77db2b72e4a848",
    "0x86b5780b606940eb59a062aa85a07959518c0161",
    "0x87eb1c0f3827cb91bef234f61e4a5594280754e9",
    "0x8b4c8c403fc015c46061a8702799490fd616e3bf",
    "0x8b71140ad2e5d1e7018d2a7f8a288bd3cd38916f",
    "0x8f9d2cd33551ce06dd0564ba147513f715c2f4a0",
    "0x90102473a816a01a9fb0809f2289438b2e294f76",
    "0x93fff4028927f53f708534397ed349b9cd4e2f9f",
    "0x9893989433e7a383cb313953e4c2365107dc19a7",
    "0x9d0b0877b9f2204cf414ca7862e4f03506822538",
    "0x9f26d4c958fd811a1f59b01b86be7dffc9d20761",
    "0x9ffdf407cde9a93c47611799da23924af3ef764f",
    "0xa24af73eadd17997eeedbed36672e996544d2de4",
    "0xa3d68b74bf0528fdd07263c60d6488749044914b",
    "0xa6cb988942610f6731e664379d15ffcfbf282b44",
    "0xa8037a13f5b0d6dc91a1c4c75b31a79a2986e24b",
    "0xb1f5bbc3e4de0c767ace41eab8a28b837fba966f",
    "0xb49e4420ea6e35f98060cd133842dbea9c27e479",
    "0xbd456973dfd5b00b07ce5307110c77e3f228ca3c",
    "0xc1fa6e2e8667d9be0ca938a54c7e0285e9df924a",
    "0xc5fde679f52e9bd896ab1dee5265f9a80c672512",
    "0xc85276fec421d0ca3c0efd4be2b7f569bc7b5b99",
    "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee",
    "0xce6460b8e97d7be72a9f525fa5b49c62d06d2b46",
    "0xcead893b162d38e714d82d06a7fe0b0dc3c38e0b",
    "0xcfc6d9bd7411962bfe7145451a7ef71a24b6a7a2",
    "0xd5edf7730abad812247f6f54d7bd31a52554e35e",
    "0xd8fc8f0b03eba61f64d08b0bef69d80916e5dda9",
    "0xdadef1ffbfeaab4f68a9fd181395f68b4e4e7ae0",
    "0xeb61c16a60ab1b4a9a1f8e92305808f949f4ea9b",
    "0xf0164d48b308c42ae028e3379b2fb620e70f8780",
    "0xf5a6d0642c7e02b250a5ada440f901d211b40506",
    "0xfe0c30065b384f05761f15d0cc899d4f9f9cc0eb",
}
ETHERFI_LIVE_SPOTCHECKS = [
    ("DepositAdapter", "ethereum", "0xcfc6d9bd7411962bfe7145451a7ef71a24b6a7a2"),
    ("EtherFiRestaker", "ethereum", "0x1b7a4c3797236a1c37f8741c0be35c2c72736fff"),
    ("KING Distributor", "ethereum", "0x6db24ee656843e3fe03eb8762a54d86186ba6b64"),
    ("User Safe Lens", "scroll", "0x333321a783f765bfd4c22fbbc5b2d02b97efb44c"),
    ("User Safe Factory", "scroll", "0x18fa07df94b4e9f09844e1128483801b24fe8a27"),
]


def _contract_address_rows(result: dict[str, Any]) -> list[str]:
    return [
        str(contract.get("address", "")).lower()
        for contract in result.get("contracts", [])
        if isinstance(contract, dict) and str(contract.get("address", "")).strip()
    ]


def _contract_addresses(result: dict[str, Any]) -> set[str]:
    return set(_contract_address_rows(result))


def _contracts_by_name(result: dict[str, Any], name: str) -> list[dict[str, Any]]:
    return [
        contract
        for contract in result.get("contracts", [])
        if isinstance(contract, dict) and contract.get("name") == name
    ]


def _assert_contract_present(
    result: dict[str, Any],
    *,
    name: str,
    chain: str,
    address: str,
) -> None:
    normalized = address.lower()
    matches = [
        contract
        for contract in _contracts_by_name(result, name)
        if str(contract.get("chain")) == chain and str(contract.get("address", "")).lower() == normalized
    ]
    assert matches, f"Missing expected contract {name} on {chain} at {address}"


def _duplicate_address_sample(counts: Counter[str], limit: int = 5) -> str:
    duplicates = [
        (address, count)
        for address, count in counts.items()
        if count > 1
    ]
    if not duplicates:
        return ""
    duplicates.sort(key=lambda item: (-item[1], item[0]))
    return ", ".join(f"{address}x{count}" for address, count in duplicates[:limit])


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


def test_live_protocol_inventory_etherfi():
    if not os.environ.get("TAVILY_API_KEY", "").strip():
        pytest.skip("Set TAVILY_API_KEY before running this live test.")
    if not os.environ.get("OPEN_ROUTER_KEY", "").strip():
        pytest.skip("Set OPEN_ROUTER_KEY before running this live test.")

    result = search_protocol_inventory("etherfi", limit=200, max_queries=4)

    assert result["company"] == "etherfi"
    assert result["chain"] == "any"
    assert isinstance(result.get("domain_candidates"), list)
    assert isinstance(result.get("pages_selected"), list)
    assert isinstance(result.get("contracts"), list)
    assert isinstance(result.get("errors"), list)
    assert isinstance(result.get("notes"), list)

    if not result["contracts"] and _looks_like_network_or_provider_outage(result["errors"]):
        pytest.skip(f"Live inventory dependencies unavailable: {result['errors']}")

    assert result.get("official_domain"), f"Official domain not resolved: {result}"
    assert result["pages_selected"], f"No inventory pages selected: {result}"

    budget = _parse_tavily_budget(result["notes"])
    assert budget is not None, f"Missing Tavily budget note in {result['notes']}"
    used, cap = budget
    assert cap == 4
    assert 0 < used <= cap

    raw_address_rows = _contract_address_rows(result)
    actual_addresses = set(raw_address_rows)
    assert actual_addresses, f"No contracts extracted: {result}"

    matched_addresses = ETHERFI_EXPECTED_ADDRESSES & actual_addresses
    extra_addresses = actual_addresses - ETHERFI_EXPECTED_ADDRESSES
    missing_addresses = ETHERFI_EXPECTED_ADDRESSES - actual_addresses
    raw_rows = len(raw_address_rows)
    actual_total = len(actual_addresses)
    expected_total = len(ETHERFI_EXPECTED_ADDRESSES)
    matched = len(matched_addresses)
    missing = len(missing_addresses)
    extra = len(extra_addresses)
    counts = Counter(raw_address_rows)
    duplicate_rows = raw_rows - actual_total
    duplicate_addresses = sum(1 for count in counts.values() if count > 1)
    coverage = matched / expected_total
    precision = matched / actual_total
    duplicate_sample = _duplicate_address_sample(counts)
    print(
        "Ether.fi raw-address reliability: "
        f"raw_rows={raw_rows} unique_found={actual_total} duplicate_rows={duplicate_rows} "
        f"duplicate_addresses={duplicate_addresses}"
    )
    if duplicate_sample:
        print(f"Ether.fi duplicate address sample: {duplicate_sample}")
    print(
        "Ether.fi expected-address metrics: "
        f"expected={expected_total} matched={matched} missing={missing} "
        f"extra={extra} coverage={coverage:.1%} precision={precision:.1%}"
    )

    for name, chain, address in ETHERFI_LIVE_SPOTCHECKS:
        _assert_contract_present(
            result,
            name=name,
            chain=chain,
            address=address,
        )

    assert matched > 0, (
        "Expected the live Ether.fi inventory to contain at least one known address; "
        f"got {actual_total} addresses and 0/{expected_total} matches"
    )
