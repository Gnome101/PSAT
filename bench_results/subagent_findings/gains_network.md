# gains network — subagent ground-truth pass

Confidence: **High** on pashov audits + Arbitrum addresses. Could not verify standalone Halborn/ChainSecurity PDFs.

## Missed audits (not in benchmark union)

- https://github.com/pashov/audits/blob/master/team/pdf/GainsNetwork-security-review_2025-05-26.pdf — Pashov Audit Group (May 2025)
- https://github.com/pashov/audits/blob/master/team/pdf/GainsNetwork-security-review-May.pdf — Pashov Audit Group (May 2024)
- https://github.com/pashov/audits/blob/master/team/pdf/GainsNetwork-security-review-February.pdf — Pashov Audit Group (Feb 2024)
- https://github.com/pashov/audits/blob/master/team/pdf/GainsNetwork-security-review.pdf — Pashov Audit Group (Dec 2023)
- https://docs.gains.trade/what-is-gains-network/contract-addresses/arbitrum-mainnet — Official docs contract index page

## Contract addresses (Arbitrum One, verified via docs.gains.trade + Arbiscan)

- GNSMultiCollatDiamond: 0xFF162c694eAA571f685030649814282eA457f169
- GNSStaking: 0x7edDE7e5900633F698EaB0Dbc97DE640fC5dC015
- gDAI vault: 0xd85E038593d7A098614721EaE955EC2022B9B91B
- gUSDC vault: 0xd3443ee1e91aF28e5FB858Fbd0D72A63bA8046E0
- gETH vault: 0x5977A9682D7AF81D347CFc338c61692163a2784C
- GNS token: 0x18c11FD286C5EC11c3b683Caa813B77f5163A122

## Signal

Benchmark had **2** Pashov audits in the union, missed **4** more from the same pashov/audits repo. Suggests systemic blind spot: search backends find the repo-root `README` or one PDF but don't enumerate the folder. Recovery path: when a search hits a pashov/audits PDF, the discovery pipeline should follow up by listing sibling files in the same folder.
