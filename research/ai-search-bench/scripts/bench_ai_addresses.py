#!/usr/bin/env python3
"""Parallel address-discovery benchmark.

Runs `search_protocol_inventory()` per backend (via _tavily_search
monkeypatch) plus Exa Deep Research asking for deployed addresses.

Writes bench_results_addresses/<protocol>/<backend>__<mode>.json.
Ground truth = union across all configs.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from services.discovery import inventory as inventory_mod  # noqa: E402
from services.discovery import inventory_domain as inventory_domain_mod  # noqa: E402
from utils import brave, exa, tavily  # noqa: E402

RESULTS_DIR = ROOT / "bench_results_addresses"

PROTOCOLS = [
    "ether.fi",
    "uniswap",
    "sky",
    "lido",
    "morpho",
    "aave",
    "avantis",
    "gains network",
    "gmx",
    "compound v3",
    "aerodrome",
    "ethena",
]

CONFIGS: list[tuple[str, str]] = [
    ("exa", "neural"),
    ("exa", "regular"),
    ("exa", "instant"),
    ("tavily", "default"),
    ("brave", "default"),
    ("exa", "research"),
    ("exa", "deep-lite"),
    ("exa", "deep"),
    ("exa", "deep-reasoning"),
]


def _slug(name: str) -> str:
    return name.replace(" ", "_").replace(".", "").lower()


def _make_search_fn(backend: str, mode: str):
    def fn(
        query: str,
        max_results: int,
        queries_used: list[int],
        max_queries: int,
        errors: list[dict],
        debug: bool = False,
    ) -> list[dict]:
        if queries_used[0] >= max_queries:
            return []
        queries_used[0] += 1
        try:
            if backend == "exa":
                return exa.search(query, max_results=max_results, mode=mode)
            if backend == "tavily":
                return tavily.search(
                    query,
                    max_results=max_results,
                    topic="general",
                    include_raw_content=False,
                )
            if backend == "brave":
                return brave.search(query, max_results=max_results, mode=mode)
        except Exception as exc:
            errors.append({"provider": backend, "error": str(exc), "query": query[:120]})
            return []
        return []

    return fn


def _patch_search(backend: str, mode: str):
    fn = _make_search_fn(backend, mode)
    inventory_mod._tavily_search = fn  # type: ignore[attr-defined]
    inventory_domain_mod._tavily_search = fn  # type: ignore[attr-defined]


def _restore_search(original):
    inventory_mod._tavily_search = original  # type: ignore[attr-defined]
    inventory_domain_mod._tavily_search = original  # type: ignore[attr-defined]


def run_pipeline(protocol: str, backend: str, mode: str) -> dict[str, Any]:
    t0 = time.monotonic()
    original = inventory_domain_mod._tavily_search  # type: ignore[attr-defined]
    _patch_search(backend, mode)
    try:
        result = inventory_mod.search_protocol_inventory(
            protocol,
            chain=None,
            limit=500,
            max_queries=4,
            run_deployer=False,
            debug=False,
        )
    finally:
        _restore_search(original)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    return {
        "protocol": protocol,
        "backend": backend,
        "mode": mode,
        "elapsed_ms": elapsed_ms,
        "contracts": result.get("contracts", []),
        "official_domain": result.get("official_domain"),
        "errors": result.get("errors", []),
        "notes": result.get("notes", []),
    }


_ADDRESS_RESEARCH_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["contracts"],
    "additionalProperties": False,
    "properties": {
        "contracts": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["name", "address"],
                "additionalProperties": False,
                "properties": {
                    "name": {"type": "string"},
                    "address": {"type": "string", "description": "0x-prefixed EVM address"},
                    "chain": {"type": "string"},
                    "role": {"type": "string"},
                },
            },
        }
    },
}


def run_deep_research(protocol: str) -> dict[str, Any]:
    t0 = time.monotonic()
    instructions = (
        f"Find the main deployed smart contract addresses for the {protocol} protocol. "
        f"List the core production contracts (not peripheral helpers) with their names and "
        f"0x-prefixed on-chain addresses. Include the chain where each is deployed. Prioritize "
        f"contracts the protocol team themselves publish on their official docs or governance pages."
    )
    record: dict[str, Any] = {
        "protocol": protocol,
        "backend": "exa",
        "mode": "research",
        "instructions": instructions,
        "contracts": [],
        "errors": [],
        "notes": [],
    }
    try:
        r = exa.deep_research(instructions, schema=_ADDRESS_RESEARCH_SCHEMA, timeout_seconds=900)
        record["task_id"] = r.get("task_id")
        for item in r.get("data", {}).get("contracts", []):
            addr = str(item.get("address") or "").strip()
            if not addr.lower().startswith("0x") or len(addr) != 42:
                continue
            record["contracts"].append(
                {
                    "name": item.get("name"),
                    "address": addr.lower(),
                    "chains": [item.get("chain")] if item.get("chain") else [],
                    "role": item.get("role"),
                    "source": ["exa_deep_research"],
                    "confidence": 1.0,
                }
            )
    except Exception as exc:
        record["errors"].append({"stage": "deep_research", "error": repr(exc), "trace": traceback.format_exc()})
    record["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
    return record


def run_one(protocol: str, backend: str, mode: str, *, out_dir: Path, force: bool = False) -> dict[str, Any]:
    proto_dir = out_dir / _slug(protocol)
    proto_dir.mkdir(parents=True, exist_ok=True)
    out_path = proto_dir / f"{backend}__{mode}.json"
    if out_path.exists() and not force:
        return {"status": "skipped", "reason": "exists", "path": str(out_path)}

    if backend == "exa" and mode == "research":
        record = run_deep_research(protocol)
    else:
        try:
            record = run_pipeline(protocol, backend, mode)
        except Exception as exc:
            record = {
                "protocol": protocol,
                "backend": backend,
                "mode": mode,
                "errors": [{"stage": "pipeline", "error": repr(exc), "trace": traceback.format_exc()}],
                "contracts": [],
            }

    out_path.write_text(json.dumps(record, indent=2))
    return {
        "status": "ok",
        "path": str(out_path),
        "contracts": len(record.get("contracts", [])),
        "errors": len(record.get("errors", [])),
        "elapsed_ms": record.get("elapsed_ms"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol", help="Single protocol")
    parser.add_argument("--backend", help="Single backend")
    parser.add_argument("--mode", help="Single mode")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--out", default=str(RESULTS_DIR))
    args = parser.parse_args()

    protocols = [args.protocol] if args.protocol else PROTOCOLS
    configs = [(args.backend, args.mode or "default")] if args.backend else CONFIGS

    out_dir = Path(args.out)
    targets = [(p, b, m) for p in protocols for b, m in configs]
    print(f"running {len(targets)} address-discovery configs → {out_dir}")

    summary: list[dict[str, Any]] = []
    for i, (protocol, backend, mode) in enumerate(targets, start=1):
        print(f"  [{i}/{len(targets)}] {protocol} × {backend}/{mode}", end=" ", flush=True)
        result = run_one(protocol, backend, mode, out_dir=out_dir, force=args.force)
        result.update({"protocol": protocol, "backend": backend, "mode": mode})
        summary.append(result)
        if result["status"] == "skipped":
            print(f"skip ({result['reason']})")
        else:
            contracts = result.get("contracts", 0)
            errs = result.get("errors", 0)
            ms = result.get("elapsed_ms") or 0
            print(f"contracts={contracts} errs={errs} ms={ms}")

    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"summary → {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
