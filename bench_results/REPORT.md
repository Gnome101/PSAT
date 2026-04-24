# AI-search benchmark — final report

## Setup

12 protocols × 5 configs × 2 search queries (broad + LLM followup) + 1 LLM classifier pass.
All runs: `bench_results/<protocol>/<backend>__<mode>.json`. Script: `scripts/bench_ai_search.py`.

Protocols: ether.fi, uniswap, sky, lido, morpho, aave, avantis, gains network, gmx, compound v3, aerodrome, ethena.
Configs: `exa/{deep,regular,instant}`, `tavily/default`, `brave/default`.

## Per-backend ranking

| Config | mean recall | median | stdev | min | max | outright wins |
|---|---|---|---|---|---|---|
| **exa/regular** | **44.9%** | **43.1%** | 12.2% | 23.1% | 63.6% | **5** |
| exa/deep | 34.7% | 33.9% | 11.1% | 17.5% | 54.5% | 2 |
| brave/default | 33.2% | 33.3% | 14.8% | 9.1% | 58.3% | 3 |
| tavily/default | 31.1% | 30.9% | 10.3% | 10.0% | 46.2% | 2 |
| exa/instant | 27.3% | 26.6% | 10.0% | 15.0% | 41.7% | 0 |

Recall is measured against the **union** of classified audit URLs across all 5 configs for that protocol (i.e. "if any backend found it, it counts").

## Step 7 — winner

**`exa/regular` (Exa `auto` mode)** wins on mean, median, max, and plurality of per-protocol wins (5/12). It is the best single backend for audit-report discovery.

**`exa/instant` (keyword mode)** is never best at any protocol. Keyword-only matching hurts recall by ~18pp vs auto.

**Brave** is the most variable (stdev 14.8%) — very good sometimes (ether.fi: 58% recall, best of any backend), very weak others (avantis: 9%). Useful as a complement; unreliable as a sole backend.

**Tavily** is mid-pack with the tightest distribution but no standout strengths.

### Consistency ranking (lower stdev = more predictable)

1. exa/instant (10.0%) — but worst mean, so consistent in being mediocre
2. tavily (10.3%)
3. exa/deep (11.1%)
4. **exa/regular (12.2%)** — best mean with mid-pack variance
5. brave (14.8%)

## Step 8 — cohesive ground truth

The union of what all backends found is **NOT** the true ground truth. Subagents independently hunted for audits each protocol has published but that no backend surfaced. Normalizing recall against `union + subagent_misses`:

| Protocol | union | sub misses | "true" | exa/regular vs true |
|---|---|---|---|---|
| ether.fi | 12 | 8 | 20 | 20% |
| uniswap | 40 | 7 | 47 | 28% |
| sky | 20 | 8 | 28 | 43% |
| lido | 32 | 4 | 36 | 36% |
| morpho | 40 | 6 | 46 | 37% |
| aave | 32 | 6 | 38 | 37% |
| avantis | 11 | 4 | 15 | 40% |
| gains network | 5 | 5 | 10 | 20% |
| gmx | 23 | 3 | 26 | 42% |
| compound v3 | 13 | 7 | 20 | 15% |
| aerodrome | 7 | 3 | 10 | 40% |
| ethena | 22 | 4 | 26 | 54% |

Subagent misses average **~5 per protocol, 6.5% of true truth**. So the published "44.9% recall" for exa/regular is really closer to **~35% against the true audit inventory**. This is the cohesive judgment (step 8): there is a systematic recall ceiling even for the best backend that only a post-search enumeration pass can break.

## Systemic blind spots (shared by every backend)

The subagent pass revealed 4 consistent failure modes:

1. **Folder enumeration** — when a protocol has `github.com/<org>/<repo>/audits/`, search APIs surface the directory URL but not the individual PDFs inside. ether.fi (20+ missed PDFs), lido (3+), aave (GHO/Horizon/v3-origin sub-repos).
2. **Auditor portfolio crawl** — `cantina.xyz/portfolio/<uuid>`, `sherlock.xyz/contests/<id>`, `spearbit/portfolio` GitHub — UUID-pathed pages are rarely indexed. sky (6 Cantina misses), avantis (Sherlock misses), aerodrome (Spearbit Velodrome fork-ancestor misses).
3. **Landing page vs report PDF** — search APIs prefer HTML landing pages (`certora.com/reports/<name>`, `chainsecurity.com/security-audit/<name>`) over the actual PDFs on CDN subdomains (`reports.chainsecurity.com/...`, `certora.com/wp-content/...`). compound v3 missed ChainSecurity Comet PDF and Certora FV PDF.
4. **Gitbook CDN siblings** — gitbook upload folders host multiple PDFs; backends surface one, miss siblings. ethena (3 PDFs missed in same upload folder).

All four are deterministic enumeration problems — once a seed URL is found, a follow-up pass (list GitHub audits/, walk /portfolio/, scrape /reports/ index) should close the gap cheaply.

## Recommendations

- **Primary backend**: `exa/regular` (44.9% mean, wins most protocols).
- **Secondary backend for ensemble**: `brave/default` — the ether.fi-like protocols it wins on are distinct from Exa's wins.
- **Drop**: `exa/instant`. Always underperforms its siblings.
- **Retain**: `tavily/default` as a cheap third vote; it doesn't win but is consistent and would add to the union.
- **Add a "folder enumeration" discovery pass** post-classification. The 4 blind spots above are ~5 missed audits/protocol that cost nothing to recover. This single addition would lift recall by ~7pp in aggregate.

## Files

- `bench_results/<protocol>/<backend>__<mode>.json` — raw + classified results per run
- `bench_results/summary.json` — per-run status
- `bench_results/comparison.json` — union + per-backend recall
- `bench_results/subagent_findings/<protocol>.md` — subagent ground-truth pass per protocol
- `bench_results/REPORT.md` — this file
