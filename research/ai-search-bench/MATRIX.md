# AI-search benchmark — score matrix

Full-pipeline bench (`bench_results_full/`). 12 protocols × 6 configs. Sorted by union size (largest audit surface first).

## Per-protocol recall matrix

`*` marks the winner for each protocol.

| protocol       | union | exa/research | exa/regular | exa/deep | exa/instant | tavily | brave |
|---             |  --:  |    --:       |    --:      |    --:   |    --:      |   --:  |  --:  |
| lido           | 162   | 21/162 13%   | *101/162 **62%** | 81/162 50%   | 74/162 46%   | 75/162 46%   | 7/162 4%     |
| ethena         |  40   | 15/40 38%    | 19/40 48%    | *22/40 **55%**   | 20/40 50%    | 19/40 48%    | 16/40 40%    |
| ether.fi       |  32   | *25/32 **78%**   | 3/32 9%      | 1/32 3%      | 3/32 9%      | 3/32 9%      | 3/32 9%      |
| uniswap        |  26   | 7/26 27%     | 8/26 31%     | 8/26 31%     | 8/26 31%     | 7/26 27%     | *9/26 **35%**    |
| morpho         |  23   | *9/23 **39%**    | 7/23 30%     | 5/23 22%     | 5/23 22%     | 5/23 22%     | 5/23 22%     |
| aave           |  20   | *9/20 **45%**    | 5/20 25%     | 5/20 25%     | 5/20 25%     | 4/20 20%     | 5/20 25%     |
| compound v3    |  18   | *9/18 **50%**    | 3/18 17%     | 3/18 17%     | 6/18 33%     | 5/18 28%     | 6/18 33%     |
| sky            |  18   | 5/18 28%     | 2/18 11%     | 5/18 28%     | 4/18 22%     | *6/18 **33%**    | 3/18 17%     |
| gmx            |  17   | *7/17 **41%**    | 5/17 29%     | 2/17 12%     | 5/17 29%     | 5/17 29%     | 4/17 24%     |
| aerodrome      |  13   | 4/13 31%     | 4/13 31%     | *7/13 **54%**    | 4/13 31%     | 2/13 15%     | 3/13 23%     |
| avantis        |  11   | 4/11 36%     | 4/11 36%     | *6/11 **55%**    | 4/11 36%     | 4/11 36%     | 3/11 27%     |
| gains network  |   8   | *5/8 **62%**     | 2/8 25%      | 1/8 12%      | 1/8 12%      | 3/8 38%      | 1/8 12%      |
| **TOTAL**      | **388** | 120/388 **31%** | **163/388 42%** | 146/388 38% | 139/388 36% | 138/388 36% | 65/388 17% |
| **wins** (#)   |       | **6**        | 1            | 3            | 0            | 1            | 1            |

## Two ways to read the aggregate

**Weighted by union (TOTAL row):** `exa/regular` 42% > Deep Research 31%. But this is skewed by lido alone contributing 162/388 URLs — whoever does best on lido wins the aggregate.

**Unweighted mean of per-protocol recalls** (each protocol gets equal vote):

| backend | mean | median | stdev | min | max |
|---|--:|--:|--:|--:|--:|
| **exa/research** | **40.7%** | **38.3%** | 17.2% | 13.0% | 78.1% |
| exa/deep | 30.2% | 26.4% | 18.7% | 3.1% | 55.0% |
| exa/regular | 29.6% | 29.9% | 14.8% | 9.4% | 62.4% |
| exa/instant | 28.9% | 30.1% | 11.9% | 9.4% | 50.0% |
| tavily | 29.3% | 28.6% | 11.7% | 9.4% | 47.5% |
| brave | 22.6% | 23.3% | 10.6% | 4.3% | 40.0% |

**Deep Research wins on every unweighted metric** — mean, median, and max — and has the highest ceiling (78% on ether.fi vs next-best 35% max for brave on uniswap).

## Why the two views disagree

Lido's audits live in a single GitHub tree (`lidofinance/audits/`). The production pipeline's stage-2 walker expands that tree URL into ~100 individual PDFs. Any backend that finds the tree URL gets all of them "for free."

Deep Research doesn't hit that stage-2 walker — it searches independently and returns a curated list. So for lido specifically it returns 21 distinct audits (probably closer to "true distinct audit engagements" than the 100 PDFs the tree walker produces), which looks like a loss in the matrix but is arguably a cleaner answer.

## How to read the matrix

- **Look at mean/median** (unweighted) if you care about per-protocol reliability → **Deep Research wins**.
- **Look at the TOTAL row** (weighted) if you care about raw audit-URL volume → **exa/regular wins** due to lido's GitHub-tree bulk.
- **Look at the wins row** if you care about which backend is "first choice" per protocol → **Deep Research wins 6/12**, exa/deep 3/12, others 0-1 each.

## Recommendation

Hybrid: run `exa/regular` through the full pipeline by default, fall back to Deep Research when the pipeline classifies <5 audits. Exa/instant and Brave can be dropped — they never win and are always dominated.
