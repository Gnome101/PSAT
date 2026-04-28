#!/usr/bin/env python3
"""Benchmark AI search backends for audit-report discovery.

Runs each (protocol, backend, mode) combination through the same
two-stage flow the real pipeline uses:

    1. broad search: `"{protocol}" smart contract security audit report`
    2. LLM follow-up query against the broad results
    3. LLM classifier over the union

Writes bench_results/<protocol>/<backend>__<mode>.json with raw
+ classified output so later passes can compute recall/precision.
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

from services.discovery.audit_reports_llm import classify_search_results, generate_followup_query  # noqa: E402
from utils import brave, exa, tavily  # noqa: E402

RESULTS_DIR = ROOT / "bench_results"

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
    ("exa", "deep"),
    ("exa", "regular"),
    ("exa", "instant"),
    ("tavily", "default"),
    ("brave", "default"),
]


def _slug(name: str) -> str:
    return name.replace(" ", "_").replace(".", "").lower()


def _run_search(backend: str, mode: str, query: str, max_results: int = 10) -> list[dict[str, Any]]:
    if backend == "exa":
        return exa.search(query, max_results=max_results, mode=mode)
    if backend == "tavily":
        return tavily.search(query, max_results=max_results)
    if backend == "brave":
        return brave.search(query, max_results=max_results, mode=mode)
    raise ValueError(f"unknown backend: {backend!r}")


def _dedupe_by_url(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in results:
        url = (r.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(r)
    return out


def run_one(
    protocol: str,
    backend: str,
    mode: str,
    *,
    out_dir: Path,
    force: bool = False,
) -> dict[str, Any]:
    """Execute one (protocol, backend, mode) run and persist the JSON."""
    proto_dir = out_dir / _slug(protocol)
    proto_dir.mkdir(parents=True, exist_ok=True)
    out_path = proto_dir / f"{backend}__{mode}.json"
    if out_path.exists() and not force:
        return {"status": "skipped", "path": str(out_path), "reason": "exists"}

    broad_query = f'"{protocol}" smart contract security audit report'
    record: dict[str, Any] = {
        "protocol": protocol,
        "backend": backend,
        "mode": mode,
        "broad_query": broad_query,
        "followup_query": None,
        "broad_results": [],
        "followup_results": [],
        "all_results_deduped": [],
        "classified": [],
        "timings_ms": {},
        "errors": [],
    }

    t0 = time.monotonic()
    try:
        broad = _run_search(backend, mode, broad_query, max_results=10)
        record["broad_results"] = broad
    except Exception as exc:
        record["errors"].append({"stage": "broad_search", "error": repr(exc), "trace": traceback.format_exc()})
        broad = []
    record["timings_ms"]["broad_search"] = int((time.monotonic() - t0) * 1000)

    if broad:
        t1 = time.monotonic()
        try:
            followup = generate_followup_query(broad, protocol)
            record["followup_query"] = followup
        except Exception as exc:
            record["errors"].append({"stage": "followup_query", "error": repr(exc)})
            followup = None
        record["timings_ms"]["followup_query"] = int((time.monotonic() - t1) * 1000)

        if followup:
            t2 = time.monotonic()
            try:
                record["followup_results"] = _run_search(backend, mode, followup, max_results=10)
            except Exception as exc:
                record["errors"].append({"stage": "followup_search", "error": repr(exc)})
            record["timings_ms"]["followup_search"] = int((time.monotonic() - t2) * 1000)

    all_results = _dedupe_by_url(broad + record["followup_results"])
    record["all_results_deduped"] = all_results

    t3 = time.monotonic()
    try:
        record["classified"] = classify_search_results(all_results, protocol)
    except Exception as exc:
        record["errors"].append({"stage": "classify", "error": repr(exc)})
    record["timings_ms"]["classify"] = int((time.monotonic() - t3) * 1000)

    out_path.write_text(json.dumps(record, indent=2))
    return {
        "status": "ok",
        "path": str(out_path),
        "raw_count": len(all_results),
        "classified_count": len(record["classified"]),
        "errors": len(record["errors"]),
    }


def _iter_targets(
    protocols: list[str],
    configs: list[tuple[str, str]],
) -> list[tuple[str, str, str]]:
    return [(p, b, m) for p in protocols for b, m in configs]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol", help="Single protocol to run (default: all 12)")
    parser.add_argument("--backend", help="Single backend: exa|tavily|brave (default: all)")
    parser.add_argument("--mode", help="Single mode (only meaningful with --backend exa)")
    parser.add_argument("--force", action="store_true", help="Rerun even if output exists")
    parser.add_argument("--out", default=str(RESULTS_DIR), help="Output directory")
    args = parser.parse_args()

    protocols = [args.protocol] if args.protocol else PROTOCOLS
    if args.backend:
        configs = [(args.backend, args.mode or "default")]
    else:
        configs = CONFIGS

    out_dir = Path(args.out)
    targets = _iter_targets(protocols, configs)
    print(f"running {len(targets)} configs → {out_dir}")

    summary: list[dict[str, Any]] = []
    for i, (protocol, backend, mode) in enumerate(targets, start=1):
        print(f"  [{i}/{len(targets)}] {protocol} × {backend}/{mode}", end=" ", flush=True)
        result = run_one(protocol, backend, mode, out_dir=out_dir, force=args.force)
        result["protocol"] = protocol
        result["backend"] = backend
        result["mode"] = mode
        summary.append(result)
        if result["status"] == "skipped":
            print(f"skip ({result['reason']})")
        else:
            raw = result.get("raw_count", 0)
            classified = result.get("classified_count", 0)
            errs = result.get("errors", 0)
            print(f"raw={raw} classified={classified} errs={errs}")

    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"summary → {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
