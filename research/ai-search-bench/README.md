# AI-Search Backend Benchmark

Benchmarks of AI-search backends (Exa, Tavily, Brave) against the PSAT discovery pipeline, for both audit-report discovery and deployed-contract-address discovery. The work spans the full 12 protocols named in the benchmark brief: ether.fi, uniswap, sky, lido, morpho, aave, avantis, gains network, gmx, compound v3, aerodrome, ethena.

## Layout

- **[`FINAL_REPORT.md`](FINAL_REPORT.md)** — start here. Comprehensive writeup with methodology, results tables, cost tiers, production recommendation.
- **[`MATRIX.md`](MATRIX.md)** — score matrices at a glance (per-protocol × per-backend recall for both tasks).
- **[`AUDIT_DEEP_DIVE.md`](AUDIT_DEEP_DIVE.md)** — earlier audit-focused deep dive (still accurate; superseded only by FINAL_REPORT for the cross-task view).
- **[`subagent_findings/`](subagent_findings/)** — per-protocol ground-truth passes from WebSearch+reasoning subagents; each file lists audits + contract addresses no backend surfaced.
- **[`scripts/`](scripts/)** — bench harness (search, address, comparison, combos).

## Headline findings

- **Best single audit backend:** `exa/deep-lite` at 41.9% mean recall.
- **Best single address backend:** `exa/regular` at 48.9% mean recall.
- **Best audit pair:** `exa/deep-lite + exa/research` at 59.4%.
- **Best address pair:** `exa/regular + exa/research` at 82.4%.
- **Production recommendation:** Premium+Deps tier — $0.90–1.40 per protocol cold, $0.05 cached re-run, 75% audit / 82% address recall.

## Reproducing the results

Code lives in:
- `services/discovery/run_discovery.py` — production orchestrator (Premium+Deps tier)
- `research/ai-search-bench/scripts/bench_ai_search_full.py` — full-pipeline audit benchmark
- `research/ai-search-bench/scripts/bench_ai_addresses.py` — address benchmark
- `research/ai-search-bench/scripts/bench_ai_compare_full.py`, `bench_ai_compare_addresses.py` — comparison + stats
- `research/ai-search-bench/scripts/bench_ai_combos.py` — K-combo analysis

Raw per-run JSON artifacts (60+ protocol × backend runs) are written to `bench_results_full/` and `bench_results_addresses/` at repo root, gitignored for size reasons. Regenerate with:

```
uv run python research/ai-search-bench/scripts/bench_ai_search_full.py
uv run python research/ai-search-bench/scripts/bench_ai_addresses.py
uv run python research/ai-search-bench/scripts/bench_ai_compare_full.py
uv run python research/ai-search-bench/scripts/bench_ai_compare_addresses.py
```

Each full run takes ~1–2 hours and costs ~$10–15 in Exa + LLM fees.
