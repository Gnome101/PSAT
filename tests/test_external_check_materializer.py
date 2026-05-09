from __future__ import annotations

from services.resolution.external_check_materializer import materialize_external_check_from_events


def test_materialize_external_check_probes_event_candidates(monkeypatch):
    import services.resolution.external_check_materializer as mod

    mod._CANDIDATE_CACHE.clear()
    member = "0x" + "aa" * 20
    non_member = "0x" + "bb" * 20

    monkeypatch.setattr(
        mod,
        "_candidate_addresses_from_events",
        lambda **_kwargs: [member, non_member],
    )
    monkeypatch.setattr(mod, "_candidate_addresses_from_hypersync", lambda **_kwargs: [])

    def fake_batch(_rpc_url, calls):
        assert len(calls) == 2
        return [
            ("0x" + "0" * 63 + "1", False),
            ("0x" + "0" * 64, False),
        ]

    monkeypatch.setattr(mod, "rpc_batch_request_with_status", fake_batch)

    cap = materialize_external_check_from_events(
        session=object(),  # type: ignore[arg-type]
        rpc_url="http://rpc",
        chain_id=1,
        checker_address="0x" + "11" * 20,
        checker_selector="0xb7009613",
        call_args=[
            {"source": "root_caller"},
            {"source": "constant", "constant_value": "0x" + "22" * 20},
            {"source": "constant", "constant_value": "0x12345678"},
        ],
    )

    assert cap is not None
    assert cap.kind == "finite_set"
    assert cap.members == [member]
    assert cap.membership_quality == "lower_bound"
    assert cap.trace and cap.trace[0]["step"] == "external_check_materialized"
