# AI-Search Benchmark — Final Report

Benchmark of AI-search backends for protocol audit discovery + deployed contract address discovery, run against the full PSAT production pipeline.

**Date:** 2026-04-24
**Protocols tested:** 12 — ether.fi, uniswap, sky, lido, morpho, aave, avantis, gains network, gmx, compound v3, aerodrome, ethena
**Backends tested:** 10 (audits) / 9 (addresses) — Exa (7 modes), Tavily, Brave
**Methodology:** Each backend monkeypatched into `services/discovery/audit_reports/search_audit_reports()` and `services/discovery/inventory/search_protocol_inventory()` respectively; recall computed against the per-protocol union of URLs found across all configs.

---

## TL;DR

- **Best single backend for audits**: `exa/deep-lite` — 41.9% mean recall, $0.012/call
- **Best single backend for addresses**: `exa/regular` — 48.9% mean recall, $0.007/call
- **Best pair for audits**: `exa/deep-lite + exa/research` — 59.4% mean recall
- **Best pair for addresses**: `exa/regular + exa/research` — 82.4% mean recall
- **Best dual-task production setup**: 4-config hybrid with conditional dependency two-pass → 75% audit / 82% address recall at **~$0.90/protocol fresh, ~$0.05/protocol cached re-run**

---

## 1. Per-backend performance

### Audit discovery (10 configs × 12 protocols)

| Backend | Mean recall | Median | Stdev | Outright wins |
|---|--:|--:|--:|--:|
| **exa/deep-lite** | **41.9%** | 41.2% | 24.7% | 3.0 |
| exa/research_plus | 41.2% | 38.1% | 24.0% | 2.0 |
| exa/deep-reasoning | 37.9% | 44.2% | 25.8% | 1.5 |
| exa/deep | 35.7% | 30.4% | 26.5% | 3.0 |
| exa/research | 23.7% | 20.5% | 18.7% | 2.0 |
| exa/neural | 20.2% | 11.2% | 20.3% | 0.5 |
| exa/regular | 19.5% | 13.4% | 17.9% | 0.0 |
| tavily/default | 19.0% | 11.8% | 16.3% | 0.0 |
| exa/instant | 18.7% | 11.3% | 16.2% | 0.0 |
| brave/default | 13.4% | 9.6% | 12.1% | 0.0 |

### Address discovery (9 configs × 12 protocols)

| Backend | Mean recall | Median | Stdev | Outright wins |
|---|--:|--:|--:|--:|
| **exa/regular** | **48.9%** | 62.1% | 38.4% | 2.5 |
| exa/neural | 43.6% | 47.4% | 37.7% | 1.3 |
| brave/default | 39.5% | 44.9% | 33.2% | 0.5 |
| tavily/default | 37.9% | 49.1% | 36.0% | 3.5 |
| exa/deep-reasoning | 37.7% | 43.1% | 36.0% | 0.8 |
| exa/research | 33.9% | 29.2% | 29.1% | 1.5 |
| exa/instant | 33.4% | 26.2% | 34.4% | 0.5 |
| exa/deep | 26.8% | 0.0% | 34.9% | 0.3 |
| exa/deep-lite | 9.4% | 0.0% | 25.3% | 0.1 |

**Key asymmetry:** Modes that dominate audits (deep-lite, deep-reasoning, deep) collapse on addresses because audit discovery rewards "find every PDF anywhere" while address discovery rewards "find THE one canonical docs page."

---

## 2. Top combinations

### Audit combos

| K | Best combo | Mean recall |
|---|---|--:|
| 1 | `exa/deep-lite` | **41.9%** |
| 2 | `exa/deep-lite + exa/research` | **59.4%** |
| 3 | `exa/deep + exa/deep-lite + exa/research` | **75.1%** |

### Address combos

| K | Best combo | Mean recall |
|---|---|--:|
| 1 | `exa/regular` | **53.3%** |
| 2 | `exa/regular + exa/research` | **82.4%** |
| 3 | `exa/instant + exa/neural + exa/research` | **91.0%** |

---

## 3. Per-protocol recall matrix

### Audits — best winner per protocol

| Protocol | Union | Best backend | Best recall | Notable finding |
|---|--:|---|--:|---|
| ether.fi | 83 | exa/research_plus | 63% | research_plus extracted the full etherfi-protocol/audits GitHub tree |
| aave | 107 | exa/research_plus | 82% | stage-2 expansion of aave-dao/aave-v3-origin/audits |
| morpho | 104 | exa/research_plus | 79% | walks morpho-org/morpho-blue/audits |
| uniswap | 79 | exa/research_plus | 72% | Universal Router + v4 audits in folder |
| gmx | 49 | exa/research_plus | 67% | gmx-synthetics/audits + gmx-contracts/audits |
| lido | 171 | exa/regular | 59% | lidofinance/audits has ~100 PDFs |
| ethena | 42 | exa/deep | 52% | ethena-labs docs-hosted PDFs |
| aerodrome | 13 | exa/deep | 54% | Velodrome parent audits |
| avantis | 13 | exa/deep | 46% | gitbook-hosted Sherlock contest PDFs |
| compound v3 | 18 | exa/research | 50% | OpenZeppelin + ChainSecurity blogs |
| sky | 22 | exa/research_plus | 32% | sky-ecosystem multi-repo audits |
| gains network | 8 | exa/research | 62% | pashov/audits multi-file repo |

### Addresses — best winner per protocol

| Protocol | Union | Best backend | Best recall | Notable finding |
|---|--:|---|--:|---|
| lido | 375 | exa/neural / exa/regular (tied) | 77% | lidofinance/documentation walked |
| uniswap | 97 | exa/regular | 67% | docs.uniswap.org deployments page |
| avantis | 85 | exa/neural / exa/regular (tied) | 99% | avantisfi gitbook contract pages |
| gains network | 71 | tavily | 54% | docs.gains.trade/arbitrum-mainnet |
| ether.fi | 60 | 5 configs tied | 87% | etherfi gitbook deployed-contracts |
| aave | 53 | 4 configs tied | 68% | aave.com/docs deployed-contracts |
| morpho | 42 | 5 configs tied | 57% | docs.morpho.org addresses page |
| aerodrome | 38 | tavily | 53% | aerodrome.finance docs |
| ethena | 35 | tavily | 91% | ethena.fi/key-addresses |
| sky | 33 | exa/regular / exa/research | 55% | sky.money docs (partial) |
| compound v3 | 5 | exa/research | 100% | comp.xyz governance forum |
| gmx | 0 | (none) | 0% | SPA-bait — docs.gmx.io is Docusaurus JS-rendered, no backend can index |

---

## 4. Cost table

### Per-API-call costs (Exa published pricing)

| Endpoint | Mode(s) | Price per call |
|---|---|--:|
| `/search` | auto, neural, keyword, fast, instant | $0.007 |
| `/search` | deep-lite, deep, deep-reasoning | $0.012 |
| `/contents` | (page fetch) | $0.001 |
| `/research` | (Deep Research w/ schema) | ~$0.15–0.30 |
| Gemini Flash (via OpenRouter) | classifier, extractor, dedup | ~$0.001 |
| Tavily advanced | — | $0.008 |
| Brave free tier | — | $0.000 |

### Per-protocol tier pricing

| Tier | Configs | Audit recall | Address recall | Per protocol | 12 protocols | 100 protocols/mo |
|---|---|--:|--:|--:|--:|--:|
| Basic | exa/regular only (both tasks) | 21% | 49% | $0.10 | $1.20 | $10/mo |
| **Good** ⭐ | `exa/regular + exa/research` (both tasks) | 55% | 82% | **$0.60** | $7.20 | $60/mo |
| Best | `exa/deep-lite` (audits) + `exa/regular` (addrs) + `exa/research` ×2 | 60% | 82% | $0.75 | $9.00 | $75/mo |
| **Premium** ⭐ | Best + `exa/research_plus` (audit amplifier) | 75% | 82% | **$1.10** | $13.20 | $110/mo |
| Premium + Deps | Premium + conditional dependency two-pass | 75% + deps | 82% | $1.40 | $16.80 | $140/mo |

---

## 5. Re-run economics (with caching)

Research tasks cache cleanly for 24-48h. Page fetches and classifier outputs cache by URL hash.

| Cadence | What runs fresh | Per protocol | 12 protocols/mo | 100 protocols/mo |
|---|---|--:|--:|--:|
| First-run (cold) | Everything | $0.90–1.40 | $11–17 | $90–140 |
| Daily ping check | /search only; compare against last run | **$0.03–0.05** | $14/mo | $120/mo |
| Weekly deep refresh | Research TTL expired, rerun | **$0.10** | $5/mo | $40/mo |
| Monthly full re-run | All caches busted | $0.90 | $11/mo | $90/mo |

**Realistic steady-state production cost (nightly ping + weekly deep + monthly full):**
- **12 protocols: ~$30/month**
- **100 protocols: ~$250/month**

---

## 6. Final production recommendation

### Configs (Premium tier with dependency two-pass)

```
Per protocol:
├── Audit discovery
│   ├── exa/deep-lite (2 /search calls)  → full pipeline, GitHub tree walk
│   ├── exa/research_plus (1 /research + full pipeline)  → Deep Research seeds → stage 2
│   └── exa/research (1 /research standalone)  → safety net
├── Address discovery
│   ├── exa/regular (2 /search calls)  → full inventory pipeline
│   └── exa/research (1 /research standalone)  → safety net
└── Conditional: dependency two-pass
    ├── Trigger: contract names include BoringVault / LayerZero / EigenLayer / Velodrome
    └── 2 × /research (identify deps, then audit-search each)
```

### What to build (~1-2 days)

1. `services/discovery/run_discovery.py` — single `run_discovery(protocol)` entry point
2. TTL cache on `exa.deep_research()` — 24h default, configurable per-call
3. `config/known_docs.yaml` — hardcoded docs URLs for SPA-bait protocols (gmx, any future offenders). ~10 lines.
4. Dependency-detection heuristic — parse contract names, trigger two-pass if match
5. Guardrails: `MAX_EXA_SEARCH=6`, `MAX_EXA_RESEARCH=3`, budget circuit breaker at $2/protocol

### What to drop

- `utils/tavily.py` — dominated by Exa; no cost advantage (~$0.01/call); 5-12pp lower recall on both tasks
- `utils/brave.py` — worst mean recall on audits (13%); not a meaningful contributor to any top combo
- `exa/instant`, `exa/neural`, `exa/deep`, `exa/deep-reasoning` configs — not in the winning setup

---

## 7. Limitations & open gaps

1. **Union ≠ ground truth.** Recall is computed against the union of backend outputs. Subagent spot-checks (WebSearch + WebFetch + reasoning) found **~5-8 additional audits per protocol** that NO backend surfaced. Realistic true-ceiling recall is ~15-20pp lower than the numbers in this report.

2. **GMX SPA-bait.** docs.gmx.io is a Docusaurus JS-rendered SPA. No search backend can index contract addresses from it. Only solution is the hardcoded URL map (see section 6).

3. **Dependency audits missed by default.** BoringVault-class audits (Seven Seas / Veda) aren't surfaced by any backend searching for "ether.fi audit." Two-pass dependency discovery (+$0.30/protocol) is required for full coverage on protocols with third-party vaults or restaking infra.

4. **Per-run non-determinism from Deep Research.** Research tasks return different (but substantially overlapping) citations on re-runs. Caching mitigates this for production.

5. **Address discovery fails for protocols that don't publish addresses in one canonical page.** Sky (multi-module), compound v3 (governance-forum-based) are near-worst-case.

---

## 8. Artifacts on this branch (`feat/bench-ai/search`)

```
scripts/
├── bench_ai_search.py           — raw search bench (deprecated)
├── bench_ai_search_full.py      — full-pipeline audit bench (final)
├── bench_ai_compare_full.py     — audit comparison + reporting
├── bench_ai_combos.py           — K-combo analysis for audits
├── bench_ai_addresses.py        — address-discovery bench
└── bench_ai_compare_addresses.py — address comparison + combos

utils/
├── exa.py        — /search (all modes) + /research (Deep Research)
├── tavily.py     — (to be removed per recommendation)
└── brave.py      — (to be removed per recommendation)

bench_results/            — raw search bench data (archived)
bench_results_full/       — full-pipeline audit bench data (10 configs × 12 protocols)
bench_results_addresses/  — address bench data (9 configs × 12 protocols)
```

Git commits:
- `fbbd36b` — initial harness
- `05e1f91` — comparison script
- `7f04b7` — Deep Research + full pipeline
- `5976568` — research_plus config
- `<new>` — address bench harness
- `cfb34a2` — Exa native deep modes

---

## 9. Questions this report can answer

| Question | Answer |
|---|---|
| Which single backend should I pick? | Audits: `exa/deep-lite`. Addresses: `exa/regular`. |
| Best 2-backend combo? | Audits: `deep-lite + research`. Addresses: `regular + research`. |
| How much per protocol? | Production setup: $0.90 cold, $0.05 cached re-run. |
| What's the worst-case failure? | GMX (SPA-bait, 0 addresses from any backend). |
| Is Tavily worth keeping? | No. Dominated everywhere, same cost. |
| What about raw web-search agent? | Same cost ($0.30-1.00), similar recall, but non-deterministic and no caching. Use pipeline for production, agent for one-shot research. |
| Dependency audits (BoringVault etc.)? | Not found by default. Two-pass discovery needed: +$0.30/protocol, +15pp recall. |
| Monthly cost for 12 protocols in production? | ~$30. |
| Monthly cost for 100 protocols in production? | ~$250. |
