#!/usr/bin/env python3
"""Full-pipeline AI-search benchmark.

Runs the production `search_audit_reports()` orchestrator (stages 0-3.5
including Solodit seed, broad+followup search, page fetch + LLM extract,
link-following, and curated auditor portfolio crawl) with each backend
substituted for Tavily via monkeypatch.

Also runs Exa Deep Research as its own 6th config (single /research call
per protocol; no pipeline substitution — the research endpoint does its
own multi-step search + synthesis).

Writes bench_results_full/<protocol>/<backend>__<mode>.json.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from services.discovery import audit_reports as audit_reports_mod  # noqa: E402
from services.discovery import inventory_domain as inventory_domain_mod  # noqa: E402
from utils import brave, exa, tavily  # noqa: E402

RESULTS_DIR = ROOT / "bench_results_full"

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
    ("exa", "research"),
    ("exa", "research_plus"),
]


def _slug(name: str) -> str:
    return name.replace(" ", "_").replace(".", "").lower()


def _make_search_fn(backend: str, mode: str, *, research_citations: list[dict] | None = None):
    """Return a drop-in replacement for `_tavily_search(query, max_results,
    queries_used, max_queries, errors, debug) -> list[dict]`.

    For mode=='research_plus', the first call returns the pre-fetched Deep
    Research citations, and subsequent calls (followup query, stage-3 hops)
    fall through to exa.search(auto).
    """
    call_count = [0]

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
        call_count[0] += 1
        try:
            if backend == "exa" and mode == "research_plus":
                if call_count[0] == 1 and research_citations is not None:
                    return research_citations[:max_results]
                return exa.search(query, max_results=max_results, mode="auto")
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
            errors.append(
                {
                    "provider": backend,
                    "error": str(exc),
                    "query": query[:120],
                }
            )
            return []
        return []

    return fn


def _patch_search(backend: str, mode: str, *, research_citations: list[dict] | None = None):
    fn = _make_search_fn(backend, mode, research_citations=research_citations)
    audit_reports_mod._tavily_search = fn  # type: ignore[attr-defined]
    inventory_domain_mod._tavily_search = fn  # type: ignore[attr-defined]


def _restore_search(original):
    audit_reports_mod._tavily_search = original  # type: ignore[attr-defined]
    inventory_domain_mod._tavily_search = original  # type: ignore[attr-defined]


def _fetch_research_citations(protocol: str) -> tuple[list[dict], str | None]:
    """Call Deep Research once and convert its output into tavily-shape
    citations suitable for injecting into stage 1a."""
    instructions = (
        f"Find all third-party smart contract security audit reports published for the "
        f"{protocol} protocol. Include pre-launch audits, formal verification reports, "
        f"contest reports (Code4rena/Sherlock/Cantina), audit-firm blog posts, and PDF "
        f"reports on GitHub or auditor websites. For each, provide the auditor name, "
        f"a URL to the audit document, and the date if known."
    )
    r = exa.deep_research(instructions, timeout_seconds=600)
    citations: list[dict] = []
    for item in r.get("data", {}).get("auditReports", []):
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        title = f"{item.get('auditor') or 'Audit'} — {protocol}"
        citations.append(
            {
                "url": url,
                "title": title,
                "content": f"{item.get('auditor') or ''} audit report for {protocol}. {item.get('date') or ''}".strip(),
                "score": 1.0,
            }
        )
    return citations, r.get("task_id")


def run_pipeline(protocol: str, backend: str, mode: str) -> dict[str, Any]:
    """Run full search_audit_reports() with backend patched in."""
    t0 = time.monotonic()
    original = inventory_domain_mod._tavily_search  # type: ignore[attr-defined]
    research_citations: list[dict] | None = None
    research_task_id: str | None = None
    if backend == "exa" and mode == "research_plus":
        research_citations, research_task_id = _fetch_research_citations(protocol)
    _patch_search(backend, mode, research_citations=research_citations)
    try:
        result = audit_reports_mod.search_audit_reports(
            protocol,
            official_domain=None,
            max_queries=4,
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
        "reports": result.get("reports", []),
        "queries_used": result.get("queries_used"),
        "errors": result.get("errors", []),
        "notes": result.get("notes", []),
        "research_task_id": research_task_id,
        "research_seed_count": len(research_citations or []),
    }


def run_deep_research(protocol: str) -> dict[str, Any]:
    """Run Exa Deep Research for a single protocol."""
    t0 = time.monotonic()
    instructions = (
        f"Find all third-party smart contract security audit reports published for the "
        f"{protocol} protocol. Include pre-launch audits, formal verification reports, "
        f"contest reports (Code4rena/Sherlock/Cantina), audit-firm blog posts, and PDF "
        f"reports on GitHub or auditor websites. For each, provide the auditor name, "
        f"a URL to the audit document, and the date if known."
    )
    record: dict[str, Any] = {
        "protocol": protocol,
        "backend": "exa",
        "mode": "research",
        "instructions": instructions,
        "reports": [],
        "errors": [],
        "notes": [],
    }
    try:
        r = exa.deep_research(instructions, timeout_seconds=600)
        record["task_id"] = r.get("task_id")
        for item in r.get("data", {}).get("auditReports", []):
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            record["reports"].append(
                {
                    "url": url,
                    "auditor": item.get("auditor"),
                    "title": item.get("title"),
                    "date": item.get("date"),
                    "confidence": 1.0,
                    "source": "exa_deep_research",
                }
            )
    except Exception as exc:
        record["errors"].append({"stage": "deep_research", "error": repr(exc), "trace": traceback.format_exc()})
    record["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
    return record


def run_one(
    protocol: str,
    backend: str,
    mode: str,
    *,
    out_dir: Path,
    force: bool = False,
) -> dict[str, Any]:
    proto_dir = out_dir / _slug(protocol)
    proto_dir.mkdir(parents=True, exist_ok=True)
    out_path = proto_dir / f"{backend}__{mode}.json"
    if out_path.exists() and not force:
        return {"status": "skipped", "path": str(out_path), "reason": "exists"}

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
                "reports": [],
            }

    out_path.write_text(json.dumps(record, indent=2))
    return {
        "status": "ok",
        "path": str(out_path),
        "reports": len(record.get("reports", [])),
        "errors": len(record.get("errors", [])),
        "elapsed_ms": record.get("elapsed_ms"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol", help="Single protocol (default: all 12)")
    parser.add_argument("--backend", help="Single backend: exa|tavily|brave (default: all)")
    parser.add_argument("--mode", help="Single mode")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--out", default=str(RESULTS_DIR))
    args = parser.parse_args()

    protocols = [args.protocol] if args.protocol else PROTOCOLS
    if args.backend:
        configs = [(args.backend, args.mode or "default")]
    else:
        configs = CONFIGS

    out_dir = Path(args.out)
    targets = [(p, b, m) for p in protocols for b, m in configs]
    print(f"running {len(targets)} full-pipeline configs → {out_dir}")

    summary: list[dict[str, Any]] = []
    for i, (protocol, backend, mode) in enumerate(targets, start=1):
        label = f"{protocol} × {backend}/{mode}"
        print(f"  [{i}/{len(targets)}] {label}", end=" ", flush=True)
        result = run_one(protocol, backend, mode, out_dir=out_dir, force=args.force)
        result.update({"protocol": protocol, "backend": backend, "mode": mode})
        summary.append(result)
        if result["status"] == "skipped":
            print(f"skip ({result['reason']})")
        else:
            elapsed = result.get("elapsed_ms") or 0
            print(f"reports={result.get('reports', 0)} errs={result.get('errors', 0)} ms={elapsed}")

    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"summary → {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
