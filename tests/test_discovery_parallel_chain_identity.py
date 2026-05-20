from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast

from workers.discovery import DiscoveryWorker


def test_parallel_discovery_children_preserve_chain_identity(monkeypatch):
    created: list[dict[str, Any]] = []

    def fake_create_job(_session: Any, request: dict[str, Any], initial_stage: Any = None) -> Any:
        created.append({"request": request, "stage": initial_stage})
        return SimpleNamespace(id=uuid.uuid4())

    monkeypatch.setattr("workers.discovery.create_job", fake_create_job)

    job = SimpleNamespace(id=uuid.uuid4(), protocol_id=7)
    DiscoveryWorker()._spawn_parallel_discovery(
        cast(Any, SimpleNamespace()),
        cast(Any, job),
        "LayerZero Example",
        {"chain": "base", "rpc_url": "https://rpc.example", "analyze_limit": 2},
        str(job.id),
        resolved={"slug": "lz-example", "all_slugs": ["lz-core"], "url": "https://app.example"},
    )

    requests = [call["request"] for call in created]
    assert len(requests) == 2
    assert requests[0]["defillama_protocol"] == "lz-core"
    assert requests[0]["chain"] == "base"
    assert requests[0]["chain_id"] == 8453
    assert requests[1]["dapp_urls"] == ["https://app.example"]
    assert requests[1]["chain"] == "base"
    assert requests[1]["chain_id"] == 8453
