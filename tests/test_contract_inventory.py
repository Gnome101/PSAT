"""Offline integration tests for the contract inventory pipeline.

Exercises the full scoring, dedup, chain-resolution, and HTML extraction
logic end-to-end with realistic inputs.  No network calls — runs in CI
without API keys.
"""

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.discovery.deployer import expand_from_deployers
from services.discovery.inventory import (
    _build_contracts,
    search_protocol_inventory,
)
from services.discovery.inventory_extract import (
    extract_inventory_entries_from_page_text,
)

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _entry(
    address: str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    chain: str = "ethereum",
    name: str | None = "TestContract",
    kind: str = "official_inventory_table",
    url: str = "https://docs.example.com/contracts",
    explorer_url: str | None = "https://etherscan.io/address/0xaaaa",
) -> dict[str, Any]:
    return {
        "name": name,
        "address": address,
        "chain": chain,
        "kind": kind,
        "url": url,
        "explorer_url": explorer_url,
        "chain_from_hint": False,
    }


# ---------------------------------------------------------------------------
# _build_contracts — realistic multi-entry scenarios
# ---------------------------------------------------------------------------


class TestBuildContracts:
    def test_multi_source_merge_and_scoring(self):
        """Multiple entries for the same address from different pages/kinds
        merge into one contract with boosted confidence."""
        addr = "0x" + "a" * 40
        entries = [
            _entry(address=addr, name="Vault", chain="ethereum", kind="official_inventory_table", url="https://a.com"),
            _entry(address=addr, name="Vault", chain="ethereum", kind="official_inventory_link", url="https://b.com"),
        ]
        contracts = _build_contracts(entries, limit=10)

        assert len(contracts) == 1
        c = contracts[0]
        assert c["name"] == "Vault"
        assert c["address"] == addr
        assert c["chain"] == "ethereum"
        assert c["confidence"] > 0.7
        assert c["source"] == ["tavily_ai_inventory"]
        assert "source_1" in c["links"]
        assert "source_2" in c["links"]

    def test_unknown_chain_remapped_and_multi_chain(self):
        """Unknown-chain entry remaps when one specific chain exists;
        multiple specific chains produce chain='multiple'."""
        addr_a = "0x" + "a" * 40
        addr_b = "0x" + "b" * 40
        entries = [
            _entry(address=addr_a, chain="ethereum"),
            _entry(address=addr_a, chain="unknown", url="https://other.com"),
            _entry(address=addr_b, chain="ethereum"),
            _entry(address=addr_b, chain="arbitrum"),
        ]
        contracts = _build_contracts(entries, limit=10)
        by_addr = {c["address"]: c for c in contracts}

        assert by_addr[addr_a]["chain"] == "ethereum"
        assert by_addr[addr_b]["chain"] == "multiple"
        assert set(by_addr[addr_b]["chains"]) == {"ethereum", "arbitrum"}

    def test_limit_and_sort_order(self):
        """Higher-confidence contracts sort first; limit is respected."""
        entries = [
            _entry(address=f"0x{i:040x}", name=None, kind="official_inventory_text", explorer_url=None)
            for i in range(5)
        ] + [
            _entry(address="0x" + "f" * 40, name="Best", kind="official_inventory_table"),
        ]
        contracts = _build_contracts(entries, limit=3)

        assert len(contracts) == 3
        assert contracts[0]["address"] == "0x" + "f" * 40
        assert contracts[0]["confidence"] > contracts[-1]["confidence"]

    def test_entries_without_links_excluded(self):
        entries = [_entry(url="", explorer_url=None)]
        assert _build_contracts(entries, limit=10) == []

    def test_name_voting_and_aliases(self):
        addr = "0x" + "c" * 40
        entries = [
            _entry(address=addr, name="Alpha", url="https://a.com"),
            _entry(address=addr, name="Alpha", url="https://b.com"),
            _entry(address=addr, name="Beta", url="https://c.com"),
        ]
        contracts = _build_contracts(entries, limit=10)
        assert contracts[0]["name"] == "Alpha"
        assert "Beta" in contracts[0]["aliases"]


# ---------------------------------------------------------------------------
# extract_inventory_entries_from_page_text — realistic HTML
# ---------------------------------------------------------------------------


class TestExtractFromPageText:
    def test_table_with_chain_headings_and_explorer_links(self):
        """Realistic docs page with chain headings, a table, and explorer links."""
        html = """
        <h2>Ethereum</h2>
        <table>
            <tr><th>Contract</th><th>Address</th></tr>
            <tr>
                <td>StakingPool</td>
                <td><a href="https://etherscan.io/address/0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa">
                    0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa</a></td>
            </tr>
        </table>

        <h2>Arbitrum</h2>
        <p>Router: <a href="https://arbiscan.io/address/0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb">view</a></p>
        """
        entries = extract_inventory_entries_from_page_text(
            "https://docs.example.com/contracts", html, requested_chain=None
        )

        by_addr = {e["address"]: e for e in entries}
        assert "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" in by_addr
        assert "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" in by_addr

        eth_entry = by_addr["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
        assert eth_entry["chain"] == "ethereum"
        assert eth_entry["name"] is not None

        arb_entry = by_addr["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
        assert arb_entry["chain"] == "arbitrum"

    def test_requested_chain_filters_entries(self):
        html = """
        <h2>Ethereum</h2>
        <p>0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa</p>
        <h2>Arbitrum</h2>
        <p>0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb</p>
        """
        entries = extract_inventory_entries_from_page_text("https://docs.example.com", html, requested_chain="ethereum")
        assert all(e["chain"] == "ethereum" for e in entries)
        assert not any(e["address"] == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" for e in entries)

    def test_dedup_and_no_addresses(self):
        html_dup = "<p>0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa</p>"
        entries = extract_inventory_entries_from_page_text("https://x.com", html_dup, requested_chain=None)
        assert len(entries) == 1

        entries_empty = extract_inventory_entries_from_page_text(
            "https://x.com", "<p>Nothing</p>", requested_chain=None
        )
        assert entries_empty == []


# ---------------------------------------------------------------------------
# search_protocol_inventory — orchestrator with mocked externals
# ---------------------------------------------------------------------------


class TestSearchProtocolInventoryOffline:
    def test_validation_errors(self):
        with pytest.raises(ValueError, match="must not be empty"):
            search_protocol_inventory("")
        with pytest.raises(ValueError, match="limit must be >= 1"):
            search_protocol_inventory("test", limit=0)

    def test_no_domain_returns_valid_structure(self, monkeypatch):
        monkeypatch.setattr("services.discovery.inventory._tavily_search", lambda *_a, **_kw: [])
        monkeypatch.setattr("services.discovery.inventory._llm_select_domain", lambda *_a, **_kw: None)

        result = search_protocol_inventory("nonexistent_xyz")
        assert result["official_domain"] is None
        assert result["contracts"] == []
        assert any("Could not identify" in n for n in result["notes"])

    def test_full_pipeline_with_mocked_pages(self, monkeypatch):
        """Mocked page extraction feeds through scoring and produces correct output shape."""
        fake_entries = [
            _entry(address="0x" + "a" * 40, name="Vault", chain="ethereum"),
            _entry(address="0x" + "b" * 40, name="Router", chain="arbitrum", kind="official_inventory_link"),
        ]
        monkeypatch.setattr(
            "services.discovery.inventory._discover_contract_inventory_pages",
            lambda *_a, **_kw: ([{"url": "https://docs.example.com"}], ["https://docs.example.com"]),
        )
        monkeypatch.setattr(
            "services.discovery.inventory.extract_inventory_entries_from_pages",
            lambda *_a, **_kw: fake_entries,
        )
        monkeypatch.setattr(
            "services.discovery.inventory.expand_from_deployers",
            lambda *_a, **_kw: [],
        )

        result = search_protocol_inventory("docs.example.com", limit=10)
        assert result["official_domain"] == "docs.example.com"
        assert result["chain"] == "any"
        assert len(result["contracts"]) == 2

        for contract in result["contracts"]:
            for field in ("name", "address", "chain", "confidence", "source", "reasons", "links"):
                assert field in contract
            assert 0 < contract["confidence"] <= 0.99
            assert isinstance(contract["source"], list)

    def test_full_pipeline_with_deployer_expansion(self, monkeypatch):
        """Deployer entries merge with Tavily entries and boost confidence."""
        addr_both = "0x" + "a" * 40
        addr_deployer_only = "0x" + "d" * 40
        tavily_entries = [
            _entry(address=addr_both, name="Vault", chain="ethereum"),
        ]
        deployer_entries = [
            _entry(
                address=addr_both,
                name=None,
                chain="unknown",
                kind="deployer_expansion",
                url="https://etherscan.io/address/0xdeployer",
                explorer_url=f"https://etherscan.io/address/{addr_both}",
            ),
            _entry(
                address=addr_deployer_only,
                name="NewContract",
                chain="unknown",
                kind="deployer_expansion",
                url="https://etherscan.io/address/0xdeployer",
                explorer_url=f"https://etherscan.io/address/{addr_deployer_only}",
            ),
        ]
        monkeypatch.setattr(
            "services.discovery.inventory._discover_contract_inventory_pages",
            lambda *_a, **_kw: ([{"url": "https://docs.example.com"}], ["https://docs.example.com"]),
        )
        monkeypatch.setattr(
            "services.discovery.inventory.extract_inventory_entries_from_pages",
            lambda *_a, **_kw: tavily_entries,
        )
        monkeypatch.setattr(
            "services.discovery.inventory.expand_from_deployers",
            lambda *_a, **_kw: deployer_entries,
        )

        result = search_protocol_inventory("docs.example.com", limit=10)
        contracts = result["contracts"]
        by_addr = {c["address"]: c for c in contracts}

        # Corroborated address gets both sources and deployer confidence boost
        assert addr_both in by_addr
        corroborated = by_addr[addr_both]
        assert "tavily_ai_inventory" in corroborated["source"]
        assert "deployer_expansion" in corroborated["source"]
        assert any("Deployer evidence" in r for r in corroborated["reasons"])

        # Deployer-only address appears with deployer source
        assert addr_deployer_only in by_addr
        deployer_only = by_addr[addr_deployer_only]
        assert deployer_only["source"] == ["deployer_expansion"]
        assert deployer_only["name"] == "NewContract"


# ---------------------------------------------------------------------------
# _build_contracts — deployer merge scenarios
# ---------------------------------------------------------------------------


class TestBuildContractsDeployerMerge:
    def test_deployer_unknown_chain_remapped_by_tavily(self):
        """Deployer entries with chain=unknown get remapped when Tavily provides a chain."""
        addr = "0x" + "a" * 40
        entries = [
            _entry(address=addr, name="Vault", chain="ethereum", kind="official_inventory_table"),
            _entry(
                address=addr,
                name=None,
                chain="unknown",
                kind="deployer_expansion",
                url="https://etherscan.io/address/0xdeployer",
                explorer_url=f"https://etherscan.io/address/{addr}",
            ),
        ]
        contracts = _build_contracts(entries, limit=10)
        assert len(contracts) == 1
        assert contracts[0]["chain"] == "ethereum"
        assert "deployer_expansion" in contracts[0]["source"]

    def test_deployer_corroboration_boosts_confidence(self):
        """Address found by both sources should have higher confidence than either alone."""
        addr = "0x" + "a" * 40
        tavily_only = [_entry(address=addr, chain="ethereum")]
        combined = [
            _entry(address=addr, chain="ethereum"),
            _entry(
                address=addr,
                chain="unknown",
                kind="deployer_expansion",
                url="https://etherscan.io/address/0xdeployer",
                explorer_url=f"https://etherscan.io/address/{addr}",
            ),
        ]
        tavily_contracts = _build_contracts(tavily_only, limit=10)
        combined_contracts = _build_contracts(combined, limit=10)

        assert combined_contracts[0]["confidence"] > tavily_contracts[0]["confidence"]

    def test_deployer_only_entry_included(self):
        """Address found only by deployer expansion is still included."""
        addr = "0x" + "d" * 40
        entries = [
            _entry(
                address=addr,
                name="DeployerFound",
                chain="unknown",
                kind="deployer_expansion",
                url="https://etherscan.io/address/0xdeployer",
                explorer_url=f"https://etherscan.io/address/{addr}",
            ),
        ]
        contracts = _build_contracts(entries, limit=10)
        assert len(contracts) == 1
        assert contracts[0]["name"] == "DeployerFound"
        assert contracts[0]["source"] == ["deployer_expansion"]


# ---------------------------------------------------------------------------
# expand_from_deployers — mocked Etherscan calls
# ---------------------------------------------------------------------------


class TestExpandFromDeployers:
    def test_empty_seeds_returns_empty(self):
        assert expand_from_deployers([]) == []

    def test_expand_with_mocked_etherscan(self, monkeypatch):
        """Mock Etherscan API to verify the deployer expansion flow."""
        deployer = "0x" + "de" * 20
        # Supply enough seeds from one deployer to pass the min_seed_count threshold
        seeds = [f"0x{i:040x}" for i in range(1, 4)]
        new_contract = "0x" + "b" * 40

        def fake_etherscan_get(module, action, **params):
            if action == "getcontractcreation":
                return {
                    "status": "1",
                    "result": [
                        {"contractAddress": s, "contractCreator": deployer, "txHash": "0x" + "1" * 64} for s in seeds
                    ],
                }
            if action == "txlist":
                return {
                    "status": "1",
                    "result": [
                        *[{"to": "", "contractAddress": s, "hash": "0x" + "1" * 64} for s in seeds],
                        {"to": "", "contractAddress": new_contract, "hash": "0x" + "2" * 64},
                        {"to": "0x" + "c" * 40, "contractAddress": "", "hash": "0x" + "3" * 64},
                    ],
                }
            if action == "getsourcecode":
                addr = params.get("address", "").lower()
                if addr == new_contract:
                    return {"status": "1", "result": [{"ContractName": "DiscoveredToken"}]}
                return {"status": "1", "result": [{"ContractName": ""}]}
            return {"status": "0", "result": []}

        monkeypatch.setattr("services.discovery.deployer.etherscan.get", fake_etherscan_get)
        monkeypatch.setattr("services.discovery.deployer.time.sleep", lambda _: None)

        entries = expand_from_deployers(seeds)

        # 3 seeds + 1 new contract = 4 entries
        assert len(entries) == 4
        addresses = {e["address"] for e in entries}
        assert any(new_contract.lower() in a for a in addresses)

        for entry in entries:
            assert entry["kind"] == "deployer_expansion"
            assert entry["chain"] == "unknown"
            assert entry["explorer_url"] is not None

        # The new contract should have its resolved name
        new_entry = next(e for e in entries if new_contract.lower() in e["address"])
        assert new_entry["name"] == "DiscoveredToken"

    def test_deployer_below_threshold_filtered_out(self, monkeypatch):
        """A deployer that created only 1 seed should be rejected."""
        seed = "0x" + "a" * 40
        deployer = "0x" + "de" * 20

        def fake_get(module, action, **params):
            if action == "getcontractcreation":
                return {
                    "status": "1",
                    "result": [
                        {"contractAddress": seed, "contractCreator": deployer, "txHash": "0x" + "1" * 64},
                    ],
                }
            return {"status": "0", "result": []}

        monkeypatch.setattr("services.discovery.deployer.etherscan.get", fake_get)
        monkeypatch.setattr("services.discovery.deployer.time.sleep", lambda _: None)

        # With default thresholds (min_seed_count=3), 1 seed is not enough
        entries = expand_from_deployers([seed])
        assert entries == []

        # With lowered thresholds, same deployer qualifies
        entries = expand_from_deployers([seed], min_seed_count=1, min_seed_share=0.0)
        assert len(entries) == 0  # txlist not mocked, so no deployments found

    def test_no_creators_found(self, monkeypatch):
        """If getcontractcreation fails for all seeds, return empty."""

        def fake_get(*_a, **_kw):
            raise RuntimeError("No data found")

        monkeypatch.setattr("services.discovery.deployer.etherscan.get", fake_get)
        monkeypatch.setattr("services.discovery.deployer.time.sleep", lambda _: None)

        entries = expand_from_deployers(["0x" + "a" * 40])
        assert entries == []

    def test_deployer_with_no_creations(self, monkeypatch):
        """If deployer txlist has no contract creations, still returns seed entry."""
        seed = "0x" + "a" * 40
        deployer = "0x" + "de" * 20

        def fake_get(module, action, **params):
            if action == "getcontractcreation":
                return {
                    "status": "1",
                    "result": [
                        {"contractAddress": seed, "contractCreator": deployer, "txHash": "0x" + "1" * 64},
                    ],
                }
            if action == "txlist":
                # Deployer has transactions but none are contract creations
                return {
                    "status": "1",
                    "result": [
                        {"to": "0x" + "f" * 40, "contractAddress": "", "hash": "0x" + "2" * 64},
                    ],
                }
            return {"status": "1", "result": [{"ContractName": ""}]}

        monkeypatch.setattr("services.discovery.deployer.etherscan.get", fake_get)
        monkeypatch.setattr("services.discovery.deployer.time.sleep", lambda _: None)

        entries = expand_from_deployers([seed])
        assert entries == []
