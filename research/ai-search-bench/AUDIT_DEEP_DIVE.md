# AI-search benchmark — full-pipeline report

Supersedes `bench_results/REPORT.md` (raw search + classifier only). This run uses the full production `search_audit_reports()` orchestrator (Solodit seed → broad+followup search → page fetch + LLM extract → link follow → auditor-portfolio crawl → dedup+cluster) with each backend monkeypatched for the search layer. Exa Deep Research runs as its own config via the `/research` endpoint (no substitution).

## Setup

- 12 protocols × 6 configs = 72 runs
- `scripts/bench_ai_search_full.py`, `scripts/bench_ai_compare_full.py`
- Raw JSON: `bench_results_full/<protocol>/<backend>__<mode>.json`
- Aggregate: `bench_results_full/comparison.json`

Configs: `exa/{deep,regular,instant,research}`, `tavily/default`, `brave/default`.

## Overall ranking

Union size across all 12 protocols: **388** unique audit URLs.

| Config | Unique found | Recall vs union | Outright wins (of 12) |
|---|---|---|---|
| **exa/regular** | 163 | **42.0%** | 1 |
| exa/deep | 146 | 37.6% | 3 |
| exa/instant | 139 | 35.8% | 0 |
| tavily/default | 138 | 35.6% | 1 |
| **exa/research** | 120 | 30.9% | **6** |
| brave/default | 65 | 16.8% | 1 (tied) |

**The aggregate is misleading.** Lido alone contributes 162/388 URLs (42% of the total union) because the full pipeline's `_fetch_github_tree_as_reports` expands `lidofinance/audits/*` into 100+ individual PDFs. Any backend that surfaces the GitHub tree URL gets all of them; Deep Research doesn't, so its aggregate recall is penalized.

## Per-protocol recall (the signal)

| Protocol | best config | best recall | exa/research | Δ research vs best of others |
|---|---|---|---|---|
| ether.fi | **exa/research** | **78%** | 78% | **+69pp** vs next (9%) |
| gains network | **exa/research** | **62%** | 62% | +24pp vs next (38%) |
| aave | **exa/research** | **45%** | 45% | +20pp vs next (25%) |
| compound v3 | **exa/research** | **50%** | 50% | +17pp vs next (33%) |
| gmx | **exa/research** | **41%** | 41% | +12pp vs next (29%) |
| morpho | **exa/research** | **39%** | 39% | +9pp vs next (30%) |
| lido | exa/regular | 62% | 13% | −49pp |
| ethena | exa/deep | 55% | 38% | −17pp |
| avantis | exa/deep | 55% | 36% | −19pp |
| aerodrome | exa/deep | 54% | 31% | −23pp |
| sky | tavily | 33% | 28% | −5pp |
| uniswap | brave (tie with exa/{deep,regular,instant}) | 35% | 27% | −8pp |

## What this means

Two distinct classes of protocol:

### Class A — scattered-audit protocols (research wins)
Protocols where audits live across many domains (auditor blogs, Cantina portfolios, Sherlock contests, gitbook uploads, governance forums). Single-shot search + extraction struggles because each auditor has to be discovered independently.

**Exa Deep Research dominates** because its multi-step reasoning loop specifically iterates "what auditors have I not checked yet?" and goes looking. For ether.fi it returned **25 audits** including every PDF in `etherfi-protocol/smart-contracts/audits/*`. The raw pipeline with a generic backend found 1-3 because stages 2-3 only expand URLs the raw search already returned.

Research wins: ether.fi, gains network, aave, compound v3, gmx, morpho (6/12).

### Class B — centralized-audit-repo protocols (pipeline wins)
Protocols where most audits live in a single GitHub tree that `_fetch_github_tree_as_reports` walks. Here the backend just has to surface the tree URL once; stage 2 handles the rest.

**exa/regular and exa/deep dominate** because they're best at surfacing the primary GitHub tree URL on the first hit.

Pipeline wins: lido (62% exa/regular), ethena, avantis, aerodrome (all 54-55% exa/deep), sky (tavily), uniswap (tied).

## Backend health check

- **exa/regular** — top of aggregate (42%), wins lido outright, consistent runner-up elsewhere. Best general-purpose single-shot backend.
- **exa/deep (neural)** — wins avantis/aerodrome/ethena. Best for fork-of-fork protocols where fuzzy name-matching matters.
- **exa/instant (keyword)** — never wins. Drop.
- **tavily/default** — mid-pack (35.6%), wins sky only. Solid third vote.
- **brave/default** — collapses in full pipeline (16.8% vs 33.2% in raw-search bench). Returns fewer seed URLs → stage 2 has nothing to expand. Retain only as raw-search fallback.
- **exa/research** — wins 6/12 on recall and by landslides on the hardest protocols. Use as primary for high-value discovery or when the raw pipeline returns <5 reports.

## Recommended architecture

Instead of picking one backend, **combine both modes**:

1. **Raw pipeline first** — `exa/regular` substituted for tavily in `_tavily_search`. Stages 0-3.5 run as today. Cost: ~10× API calls per protocol, ~30-60s.
2. **Trigger Deep Research** when stage 1b classifies <5 audits OR extraction count in stage 2 is 0. Exa /research call takes ~30-90s and ~$0.10-0.50. Adds the high-confidence audit URLs the raw pipeline missed.
3. **Union the outputs** through the existing `_llm_validate_and_cluster` dedup.

Expected recall lift: ~+12pp on scattered-audit protocols (class A) with negligible cost on class B protocols where the raw pipeline is already sufficient.

## Cost notes

- Raw search + classifier (from previous bench): $0.01-0.05 per protocol/config.
- Full pipeline per protocol/config: $0.20-1.00 (dominated by LLM extraction calls on 5-20 pages).
- Exa Deep Research per call: observed ~$0.10 per task (schema-constrained). Cheaper than running the whole pipeline for class A protocols.

## Files

- `bench_results_full/<protocol>/<backend>__<mode>.json` — 72 pipeline traces with full report list
- `bench_results_full/summary.json` — per-run status
- `bench_results_full/comparison.json` — per-backend recall by protocol
- `bench_results_full/REPORT.md` — this file
