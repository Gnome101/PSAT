from __future__ import annotations

from services.aggregations.company_overview import _bridge_summary


def test_bridge_summary_keeps_route_security_compact() -> None:
    peer = "0x3333333333333333333333333333333333333333"
    summary = _bridge_summary(
        {"is_bridge": True, "protocols": ["LayerZero"]},
        {
            "status": "resolved",
            "protocol": "LayerZero",
            "protocols": ["LayerZero"],
            "routes": [
                {
                    "chain_display_name": "Base",
                    "peer_address": peer,
                    "peer_analysis": {"status": "queued"},
                    "receive_uln": {
                        "required_dvn_count": 2,
                        "optional_dvn_count": 1,
                        "optional_dvn_threshold": 1,
                        "required_dvns": ["0x4444444444444444444444444444444444444444"],
                    },
                }
            ],
            "policies": [{"label": "owner controls local app admin functions"}],
        },
    )

    assert summary == {
        "protocol": "LayerZero",
        "status": "1 route",
        "route_count": 1,
        "route_overflow": 0,
        "routes": [
            {
                "chain": "Base",
                "peer": "0x3333..3333",
                "peer_status": "queued",
                "security": "2 required DVNs, 1 optional, threshold 1",
            }
        ],
        "peers": "1 queued",
        "config_control": "Owner",
    }
