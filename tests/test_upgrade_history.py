import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.discovery import upgrade_history as uh


def ADDR(n: int) -> str:
    return "0x" + hex(n)[2:].zfill(40)


def _topic_for(addr: str) -> str:
    return "0x" + "0" * 24 + addr[2:]


def _admin_data(old: str, new: str) -> str:
    return "0x" + "0" * 24 + old[2:] + "0" * 24 + new[2:]


def _make_log(
    address, topic0, topic1=None, data="0x", block="0x1", tx="0xaaa", log_index="0x0", timestamp="0x65a00000"
):
    log = {
        "address": address,
        "topics": [topic0] + ([topic1] if topic1 else []),
        "data": data,
        "blockNumber": block,
        "transactionHash": tx,
        "logIndex": log_index,
        "timeStamp": timestamp,
    }
    return log


def _write_deps(tmp_path, target, deps_dict):
    data = {"address": target, "dependencies": deps_dict}
    p = tmp_path / "dependencies.json"
    p.write_text(json.dumps(data))
    return p


def _mock_no_enrichment(monkeypatch):
    """Stub out get_contract_info so no real Etherscan calls are made."""
    from utils import etherscan

    monkeypatch.setattr(etherscan, "get_contract_info", lambda addr: (None, {}))


# ---------------------------------------------------------------------------
# parse_upgrade_log — boundary between raw Etherscan data and domain model
# ---------------------------------------------------------------------------


class TestParseUpgradeLog:
    """Consolidated tests for the Etherscan-log-to-domain-model boundary."""

    def test_all_event_types(self):
        """Each of the three EIP-1967 event types parses correctly."""
        upgraded_log = _make_log(
            ADDR(1),
            uh.UPGRADED_TOPIC0,
            _topic_for(ADDR(42)),
            block="0xa",
            tx="0xabc",
            log_index="0x1",
        )
        admin_log = _make_log(
            ADDR(1),
            uh.ADMIN_CHANGED_TOPIC0,
            data=_admin_data(ADDR(1), ADDR(2)),
            block="0x14",
        )
        beacon_log = _make_log(
            ADDR(1),
            uh.BEACON_UPGRADED_TOPIC0,
            _topic_for(ADDR(99)),
            block="0x1e",
        )

        upgraded = uh.parse_upgrade_log(upgraded_log)
        assert upgraded is not None
        assert upgraded["event_type"] == "upgraded"
        assert upgraded["implementation"] == ADDR(42)
        assert upgraded["block_number"] == 10
        assert upgraded["tx_hash"] == "0xabc"
        assert upgraded["log_index"] == 1
        assert upgraded["timestamp"] > 0
        assert upgraded["_emitter"] == ADDR(1)

        admin = uh.parse_upgrade_log(admin_log)
        assert admin is not None
        assert admin["event_type"] == "admin_changed"
        assert admin["previous_admin"] == ADDR(1)
        assert admin["new_admin"] == ADDR(2)

        beacon = uh.parse_upgrade_log(beacon_log)
        assert beacon is not None
        assert beacon["event_type"] == "beacon_upgraded"
        assert beacon["beacon"] == ADDR(99)

    def test_malformed_logs_return_none(self):
        """Unknown topic0, empty topics, and missing data are handled gracefully."""
        assert uh.parse_upgrade_log({"topics": [], "data": "0x", "blockNumber": "0x1"}) is None
        assert uh.parse_upgrade_log(_make_log(ADDR(1), "0xdeadbeef" * 8)) is None

    def test_partial_data(self):
        """Upgraded without topic1 and admin_changed with short data still parse."""
        upgraded_no_impl = uh.parse_upgrade_log(_make_log(ADDR(1), uh.UPGRADED_TOPIC0))
        assert upgraded_no_impl is not None
        assert upgraded_no_impl["event_type"] == "upgraded"
        assert "implementation" not in upgraded_no_impl

        admin_short_data = uh.parse_upgrade_log(_make_log(ADDR(1), uh.ADMIN_CHANGED_TOPIC0, data="0x00"))
        assert admin_short_data is not None
        assert admin_short_data["event_type"] == "admin_changed"
        assert "previous_admin" not in admin_short_data

    def test_hex_to_int_edge_cases(self):
        """Bare '0x', empty string, and '0x0' all parse to 0."""
        assert uh._hex_to_int("0x") == 0
        assert uh._hex_to_int("0x0") == 0
        assert uh._hex_to_int("") == 0
        assert uh._hex_to_int(0) == 0
        assert uh._hex_to_int("0xa") == 10
        assert uh._hex_to_int(42) == 42

    def test_bare_hex_log_index(self):
        """Etherscan sometimes returns '0x' for logIndex — must not crash."""
        log = _make_log(
            ADDR(1),
            uh.UPGRADED_TOPIC0,
            _topic_for(ADDR(42)),
            log_index="0x",
        )
        event = uh.parse_upgrade_log(log)
        assert event is not None
        assert event["log_index"] == 0

    def test_non_indexed_upgraded_event(self):
        """OZ legacy proxies emit Upgraded(address) with impl in data, not topics."""
        impl = ADDR(42)
        data = "0x" + "0" * 24 + impl[2:]
        log = _make_log(ADDR(1), uh.UPGRADED_TOPIC0, data=data)
        event = uh.parse_upgrade_log(log)
        assert event is not None
        assert event["event_type"] == "upgraded"
        assert event["implementation"] == impl

    def test_non_indexed_beacon_upgraded_event(self):
        """BeaconUpgraded with beacon address in data instead of topics."""
        beacon = ADDR(99)
        data = "0x" + "0" * 24 + beacon[2:]
        log = _make_log(ADDR(1), uh.BEACON_UPGRADED_TOPIC0, data=data)
        event = uh.parse_upgrade_log(log)
        assert event is not None
        assert event["event_type"] == "beacon_upgraded"
        assert event["beacon"] == beacon

    def test_indexed_admin_changed_event(self):
        """AdminChanged with addresses in topics instead of data."""
        old_admin, new_admin = ADDR(50), ADDR(51)
        log = {
            "address": ADDR(1),
            "topics": [
                uh.ADMIN_CHANGED_TOPIC0,
                _topic_for(old_admin),
                _topic_for(new_admin),
            ],
            "data": "0x",
            "blockNumber": "0x1",
            "transactionHash": "0xaaa",
            "logIndex": "0x0",
            "timeStamp": "0x65a00000",
        }
        event = uh.parse_upgrade_log(log)
        assert event is not None
        assert event["event_type"] == "admin_changed"
        assert event["previous_admin"] == old_admin
        assert event["new_admin"] == new_admin

    def test_none_in_topics_array(self):
        """Topics list with None entries must not crash."""
        log = {
            "address": ADDR(1),
            "topics": [uh.UPGRADED_TOPIC0, None],
            "data": "0x",
            "blockNumber": "0x1",
            "transactionHash": "0xaaa",
            "logIndex": "0x0",
            "timeStamp": "0x65a00000",
        }
        event = uh.parse_upgrade_log(log)
        assert event is not None
        assert event["event_type"] == "upgraded"
        assert "implementation" not in event


# ---------------------------------------------------------------------------
# build_upgrade_history — full pipeline integration tests
# ---------------------------------------------------------------------------


class TestBuildUpgradeHistory:
    """Integration tests for the primary entry point. Mocks only at the
    boundary: _fetch_logs_etherscan (Etherscan network) and
    get_contract_info (Etherscan name resolution)."""

    def test_no_proxies_returns_empty_schema(self, tmp_path):
        """When dependencies.json has no proxy entries, output is a valid
        empty schema with zero upgrades."""
        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                ADDR(1): {"type": "regular"},
                ADDR(2): {"type": "library"},
            },
        )
        result = uh.build_upgrade_history(deps_path)
        assert result["schema_version"] == "0.1"
        assert result["target_address"] == ADDR(0)
        assert result["proxies"] == {}
        assert result["total_upgrades"] == 0

    def test_proxy_with_no_upgrade_events(self, monkeypatch, tmp_path):
        """A proxy that has never emitted Upgraded events still appears in
        the output with its current implementation as the sole timeline entry."""
        proxy = ADDR(1)
        impl = ADDR(10)
        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                proxy: {"type": "proxy", "proxy_type": "eip1967", "implementation": impl},
            },
        )
        monkeypatch.setattr(uh, "_fetch_logs_etherscan", lambda addr, t: [])
        _mock_no_enrichment(monkeypatch)

        result = uh.build_upgrade_history(deps_path)
        assert proxy in result["proxies"]
        h = result["proxies"][proxy]
        assert h["proxy_type"] == "eip1967"
        assert h["current_implementation"] == impl
        assert h["upgrade_count"] == 0
        assert h["first_upgrade_block"] is None
        assert h["last_upgrade_block"] is None
        assert len(h["implementations"]) == 1
        assert h["implementations"][0]["address"] == impl
        assert h["events"] == []
        assert result["total_upgrades"] == 0

    def test_single_proxy_full_output(self, monkeypatch, tmp_path):
        """A single proxy with two Upgraded events produces correct timeline,
        timestamps, block ranges, and enriched contract names."""
        proxy = ADDR(1)
        impl_v1, impl_v2 = ADDR(10), ADDR(11)
        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                proxy: {
                    "type": "proxy",
                    "proxy_type": "eip1967",
                    "implementation": impl_v2,
                    "contract_name": "MyProxy",
                },
            },
        )

        def mock_fetch(address, topic0):
            if topic0 != uh.UPGRADED_TOPIC0:
                return []
            return [
                _make_log(
                    proxy, uh.UPGRADED_TOPIC0, _topic_for(impl_v1), block="0x64", tx="0xa", timestamp="0x65a00000"
                ),
                _make_log(
                    proxy, uh.UPGRADED_TOPIC0, _topic_for(impl_v2), block="0xc8", tx="0xb", timestamp="0x65b00000"
                ),
            ]

        monkeypatch.setattr(uh, "_fetch_logs_etherscan", mock_fetch)
        from utils import etherscan

        monkeypatch.setattr(etherscan, "get_contract_info", lambda addr: ("ImplContract", {}))

        result = uh.build_upgrade_history(deps_path)

        assert result["schema_version"] == "0.1"
        assert result["target_address"] == ADDR(0)
        assert result["total_upgrades"] == 2

        h = result["proxies"][proxy]
        assert h["proxy_address"] == proxy
        assert h["proxy_type"] == "eip1967"
        assert h["current_implementation"] == impl_v2
        assert h["upgrade_count"] == 2
        assert h["first_upgrade_block"] == 0x64
        assert h["last_upgrade_block"] == 0xC8

        # Implementation timeline
        impls = h["implementations"]
        assert len(impls) == 2
        assert impls[0]["address"] == impl_v1
        assert impls[0]["block_introduced"] == 0x64
        assert impls[0]["timestamp_introduced"] == 0x65A00000
        assert impls[0]["block_replaced"] == 0xC8
        assert impls[0]["timestamp_replaced"] == 0x65B00000
        assert "block_replaced" not in impls[1]

        # Contract name enrichment happened
        assert impls[0].get("contract_name") == "ImplContract"

        # Events are present and stripped of internal keys
        assert len(h["events"]) == 2
        for event in h["events"]:
            assert "_emitter" not in event
            assert "event_type" in event
            assert "block_number" in event

    def test_multiple_proxies_events_grouped_correctly(self, monkeypatch, tmp_path):
        """Events from multiple proxies are grouped to the correct proxy entry."""
        proxy_a, proxy_b = ADDR(1), ADDR(2)
        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                proxy_a: {"type": "proxy", "proxy_type": "eip1967", "implementation": ADDR(10)},
                proxy_b: {"type": "proxy", "proxy_type": "eip1967", "implementation": ADDR(21)},
            },
        )

        def mock_fetch(address, topic0):
            if topic0 != uh.UPGRADED_TOPIC0:
                return []
            if address == proxy_a:
                return [_make_log(proxy_a, uh.UPGRADED_TOPIC0, _topic_for(ADDR(10)), block="0x64")]
            if address == proxy_b:
                return [
                    _make_log(proxy_b, uh.UPGRADED_TOPIC0, _topic_for(ADDR(20)), block="0xc8"),
                    _make_log(proxy_b, uh.UPGRADED_TOPIC0, _topic_for(ADDR(21)), block="0x12c"),
                ]
            return []

        monkeypatch.setattr(uh, "_fetch_logs_etherscan", mock_fetch)
        _mock_no_enrichment(monkeypatch)

        result = uh.build_upgrade_history(deps_path)

        assert result["total_upgrades"] == 3
        assert result["proxies"][proxy_a]["upgrade_count"] == 1
        assert len(result["proxies"][proxy_a]["implementations"]) == 1
        assert result["proxies"][proxy_b]["upgrade_count"] == 2
        assert len(result["proxies"][proxy_b]["implementations"]) == 2

    def test_admin_changed_events_in_output(self, monkeypatch, tmp_path):
        """AdminChanged events appear in the events list alongside upgrades,
        but don't count as upgrades and don't affect the implementation timeline."""
        proxy = ADDR(1)
        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                proxy: {"type": "proxy", "proxy_type": "eip1967", "implementation": ADDR(10)},
            },
        )

        def mock_fetch(address, topic0):
            if topic0 == uh.UPGRADED_TOPIC0:
                return [_make_log(proxy, uh.UPGRADED_TOPIC0, _topic_for(ADDR(10)), block="0x64", tx="0xa")]
            if topic0 == uh.ADMIN_CHANGED_TOPIC0:
                return [
                    _make_log(
                        proxy, uh.ADMIN_CHANGED_TOPIC0, data=_admin_data(ADDR(50), ADDR(51)), block="0x65", tx="0xb"
                    )
                ]
            return []

        monkeypatch.setattr(uh, "_fetch_logs_etherscan", mock_fetch)
        _mock_no_enrichment(monkeypatch)

        result = uh.build_upgrade_history(deps_path)
        h = result["proxies"][proxy]
        assert h["upgrade_count"] == 1
        assert len(h["implementations"]) == 1
        # Both events in the events list
        event_types = [e["event_type"] for e in h["events"]]
        assert "upgraded" in event_types
        assert "admin_changed" in event_types
        admin_event = next(e for e in h["events"] if e["event_type"] == "admin_changed")
        assert admin_event["previous_admin"] == ADDR(50)
        assert admin_event["new_admin"] == ADDR(51)

    def test_implementation_as_dict_in_dependencies(self, monkeypatch, tmp_path):
        """When dependencies.json has implementation as a dict (with address
        and contract_name), the pipeline extracts the address correctly and
        reuses the known name without calling Etherscan."""
        proxy = ADDR(1)
        impl = ADDR(10)
        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                proxy: {
                    "type": "proxy",
                    "proxy_type": "eip1967",
                    "implementation": {"address": impl, "contract_name": "KnownImpl"},
                },
            },
        )

        def mock_fetch(address, topic0):
            if topic0 == uh.UPGRADED_TOPIC0:
                return [_make_log(proxy, uh.UPGRADED_TOPIC0, _topic_for(impl), block="0x64", tx="0xa")]
            return []

        monkeypatch.setattr(uh, "_fetch_logs_etherscan", mock_fetch)
        from utils import etherscan

        # Should NOT be called for the known impl
        monkeypatch.setattr(
            etherscan,
            "get_contract_info",
            lambda addr: pytest.fail(f"get_contract_info should not be called for known address {addr}"),
        )

        result = uh.build_upgrade_history(deps_path)
        assert result["proxies"][proxy]["implementations"][0]["contract_name"] == "KnownImpl"

    def test_enrichment_calls_etherscan_for_unknown_implementations(self, monkeypatch, tmp_path):
        """Historical implementations not named in dependencies.json get their
        names resolved via get_contract_info."""
        proxy = ADDR(1)
        old_impl, new_impl = ADDR(10), ADDR(11)
        # Only new_impl is named in deps
        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                proxy: {
                    "type": "proxy",
                    "proxy_type": "eip1967",
                    "implementation": {"address": new_impl, "contract_name": "ImplV2"},
                },
            },
        )

        def mock_fetch(address, topic0):
            if topic0 == uh.UPGRADED_TOPIC0:
                return [
                    _make_log(proxy, uh.UPGRADED_TOPIC0, _topic_for(old_impl), block="0x64", tx="0xa"),
                    _make_log(proxy, uh.UPGRADED_TOPIC0, _topic_for(new_impl), block="0xc8", tx="0xb"),
                ]
            return []

        monkeypatch.setattr(uh, "_fetch_logs_etherscan", mock_fetch)
        from utils import etherscan

        monkeypatch.setattr(etherscan, "get_contract_info", lambda addr: ("ImplV1", {}))

        result = uh.build_upgrade_history(deps_path)
        impls = result["proxies"][proxy]["implementations"]
        assert impls[0]["contract_name"] == "ImplV1"  # fetched via etherscan
        assert impls[1]["contract_name"] == "ImplV2"  # reused from deps

    def test_enrichment_deduplicates_calls(self, monkeypatch, tmp_path):
        """get_contract_info is called at most once per unique unknown address,
        even when the same implementation appears in multiple proxies."""
        proxy_a, proxy_b = ADDR(1), ADDR(2)
        shared_impl = ADDR(10)
        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                proxy_a: {"type": "proxy", "proxy_type": "eip1967", "implementation": shared_impl},
                proxy_b: {"type": "proxy", "proxy_type": "eip1967", "implementation": shared_impl},
            },
        )

        def mock_fetch(address, topic0):
            if topic0 == uh.UPGRADED_TOPIC0:
                return [_make_log(address, uh.UPGRADED_TOPIC0, _topic_for(shared_impl), block="0x64")]
            return []

        monkeypatch.setattr(uh, "_fetch_logs_etherscan", mock_fetch)
        from utils import etherscan

        call_count = [0]

        def counting_get_info(addr):
            call_count[0] += 1
            return ("SharedImpl", {})

        monkeypatch.setattr(etherscan, "get_contract_info", counting_get_info)

        result = uh.build_upgrade_history(deps_path)
        # shared_impl appears in both proxies' timelines, but should only be fetched once
        assert call_count[0] == 1
        for proxy_addr in (proxy_a, proxy_b):
            impls = result["proxies"][proxy_addr]["implementations"]
            assert impls[0]["contract_name"] == "SharedImpl"

    def test_enrich_false_skips_etherscan_but_applies_known_names(self, monkeypatch, tmp_path):
        """enrich=False never calls get_contract_info but still applies names
        already present in dependencies.json."""
        proxy = ADDR(1)
        old_impl, new_impl = ADDR(10), ADDR(11)
        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                proxy: {
                    "type": "proxy",
                    "proxy_type": "eip1967",
                    "implementation": {"address": new_impl, "contract_name": "ImplV2"},
                },
            },
        )

        def mock_fetch(address, topic0):
            if topic0 == uh.UPGRADED_TOPIC0:
                return [
                    _make_log(proxy, uh.UPGRADED_TOPIC0, _topic_for(old_impl), block="0x64", tx="0xa"),
                    _make_log(proxy, uh.UPGRADED_TOPIC0, _topic_for(new_impl), block="0xc8", tx="0xb"),
                ]
            return []

        monkeypatch.setattr(uh, "_fetch_logs_etherscan", mock_fetch)
        from utils import etherscan

        monkeypatch.setattr(
            etherscan,
            "get_contract_info",
            lambda addr: pytest.fail("get_contract_info should not be called when enrich=False"),
        )

        result = uh.build_upgrade_history(deps_path, enrich=False)
        impls = result["proxies"][proxy]["implementations"]
        assert impls[1].get("contract_name") == "ImplV2"  # known name applied
        assert "contract_name" not in impls[0]  # unknown, not fetched

    def test_target_contract_is_itself_a_proxy(self, monkeypatch, tmp_path):
        """When the target address itself is classified as a proxy (via
        target_classification.type = "proxy" in dependencies.json), it should
        appear in the output proxies dict with its upgrade history.

        This happens when the user runs PSAT against a proxy contract directly
        rather than a non-proxy that depends on proxies.
        """
        target = ADDR(0)
        target_impl = ADDR(10)
        deps = {
            "address": target,
            "target_classification": {
                "type": "proxy",
                "proxy_type": "eip1967",
                "implementation": target_impl,
            },
            "dependencies": {},
        }
        p = tmp_path / "dependencies.json"
        p.write_text(json.dumps(deps))

        def mock_fetch(address, topic0):
            if address == target and topic0 == uh.UPGRADED_TOPIC0:
                return [_make_log(target, uh.UPGRADED_TOPIC0, _topic_for(target_impl), block="0x64")]
            return []

        monkeypatch.setattr(uh, "_fetch_logs_etherscan", mock_fetch)
        _mock_no_enrichment(monkeypatch)

        result = uh.build_upgrade_history(p)
        assert target in result["proxies"], "Target contract is a proxy and should appear in the proxies output"
        h = result["proxies"][target]
        assert h["proxy_type"] == "eip1967"
        assert h["current_implementation"] == target_impl
        assert h["upgrade_count"] == 1

    def test_empty_events_for_all_proxies_with_current_impl(self, monkeypatch, tmp_path):
        """Multiple proxies all have zero events from Etherscan, but each has
        a current_implementation set. Each proxy should get a single-entry
        timeline containing just its current implementation address."""
        proxy_a = ADDR(1)
        proxy_b = ADDR(2)
        proxy_c = ADDR(3)
        impl_a = ADDR(10)
        impl_b = ADDR(20)
        impl_c = ADDR(30)

        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                proxy_a: {"type": "proxy", "proxy_type": "eip1967", "implementation": impl_a},
                proxy_b: {"type": "proxy", "proxy_type": "transparent", "implementation": impl_b},
                proxy_c: {"type": "proxy", "proxy_type": "uups", "implementation": impl_c},
            },
        )
        monkeypatch.setattr(uh, "_fetch_logs_etherscan", lambda addr, t: [])
        _mock_no_enrichment(monkeypatch)

        result = uh.build_upgrade_history(deps_path)

        assert result["total_upgrades"] == 0

        for proxy_addr, expected_impl, expected_type in [
            (proxy_a, impl_a, "eip1967"),
            (proxy_b, impl_b, "transparent"),
            (proxy_c, impl_c, "uups"),
        ]:
            assert proxy_addr in result["proxies"]
            h = result["proxies"][proxy_addr]
            assert h["proxy_type"] == expected_type
            assert h["current_implementation"] == expected_impl
            assert h["upgrade_count"] == 0
            assert h["first_upgrade_block"] is None
            assert h["last_upgrade_block"] is None
            assert h["events"] == []
            # Single-entry timeline with just current implementation
            assert len(h["implementations"]) == 1
            assert h["implementations"][0]["address"] == expected_impl

    def test_non_indexed_upgraded_in_full_pipeline(self, monkeypatch, tmp_path):
        """OZ legacy proxies with implementation in data (not topics) produce
        correct timelines through the full build_upgrade_history pipeline."""
        proxy = ADDR(1)
        impl_v1, impl_v2 = ADDR(10), ADDR(11)
        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                proxy: {"type": "proxy", "proxy_type": "oz_legacy", "implementation": impl_v2},
            },
        )

        def data_for(addr):
            return "0x" + "0" * 24 + addr[2:]

        def mock_fetch(address, topic0):
            if topic0 != uh.UPGRADED_TOPIC0:
                return []
            # Return logs with NO topic1 — implementation in data only
            return [
                {
                    "address": proxy,
                    "topics": [uh.UPGRADED_TOPIC0],
                    "data": data_for(impl_v1),
                    "blockNumber": "0x64",
                    "transactionHash": "0xa",
                    "logIndex": "0x0",
                    "timeStamp": "0x65a00000",
                },
                {
                    "address": proxy,
                    "topics": [uh.UPGRADED_TOPIC0],
                    "data": data_for(impl_v2),
                    "blockNumber": "0xc8",
                    "transactionHash": "0xb",
                    "logIndex": "0x0",
                    "timeStamp": "0x65b00000",
                },
            ]

        monkeypatch.setattr(uh, "_fetch_logs_etherscan", mock_fetch)
        _mock_no_enrichment(monkeypatch)

        result = uh.build_upgrade_history(deps_path)
        h = result["proxies"][proxy]
        assert h["upgrade_count"] == 2
        assert len(h["implementations"]) == 2
        assert h["implementations"][0]["address"] == impl_v1
        assert h["implementations"][1]["address"] == impl_v2


# ---------------------------------------------------------------------------
# write_upgrade_history — file I/O integration tests
# ---------------------------------------------------------------------------


class TestWriteUpgradeHistory:
    """Integration tests for the file-writing entry point."""

    def test_writes_valid_json(self, monkeypatch, tmp_path):
        """write_upgrade_history writes a valid JSON file with the full
        output structure when proxies exist."""
        proxy = ADDR(1)
        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                proxy: {"type": "proxy", "proxy_type": "eip1967", "implementation": ADDR(10)},
            },
        )

        def mock_fetch(address, topic0):
            if topic0 == uh.UPGRADED_TOPIC0:
                return [_make_log(proxy, uh.UPGRADED_TOPIC0, _topic_for(ADDR(10)), block="0x64")]
            return []

        monkeypatch.setattr(uh, "_fetch_logs_etherscan", mock_fetch)
        _mock_no_enrichment(monkeypatch)

        out = uh.write_upgrade_history(deps_path)
        assert out is not None
        assert out == tmp_path / "upgrade_history.json"
        assert out.exists()
        data = json.loads(out.read_text())
        assert data["schema_version"] == "0.1"
        assert proxy in data["proxies"]
        assert data["total_upgrades"] == 1

    def test_custom_output_path(self, monkeypatch, tmp_path):
        """write_upgrade_history respects a custom output_path argument."""
        proxy = ADDR(1)
        deps_path = _write_deps(
            tmp_path,
            ADDR(0),
            {
                proxy: {"type": "proxy", "proxy_type": "eip1967", "implementation": ADDR(10)},
            },
        )
        monkeypatch.setattr(uh, "_fetch_logs_etherscan", lambda addr, t: [])
        _mock_no_enrichment(monkeypatch)

        custom = tmp_path / "subdir" / "history.json"
        custom.parent.mkdir()
        out = uh.write_upgrade_history(deps_path, output_path=custom)
        assert out == custom
        assert custom.exists()

    def test_returns_none_when_no_proxies(self, tmp_path):
        """write_upgrade_history returns None and writes no file when there
        are no proxy dependencies."""
        deps_path = _write_deps(tmp_path, ADDR(0), {ADDR(1): {"type": "regular"}})
        result = uh.write_upgrade_history(deps_path)
        assert result is None
        assert not (tmp_path / "upgrade_history.json").exists()
