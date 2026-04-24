# compound v3 — subagent ground-truth pass

Confidence: **High** — audit URLs pulled from primary vendor sites; addresses from compound-finance/comet `roots.json`.

## Missed audits (not in benchmark union)

- https://reports.chainsecurity.com/Compound/ChainSecurity_Compound_Comet_Audit.pdf — ChainSecurity (main Comet audit, May 2022)
- https://www.certora.com/reports/compound-report — Certora (Comet formal verification)
- https://certora.com/wp-content/uploads/2022/06/CometReport.pdf — Certora (Comet FV report PDF)
- https://blog.openzeppelin.com/scroll-alpha-comet-deployment-audit — OpenZeppelin (Scroll Alpha Comet)
- https://blog.openzeppelin.com/compound-comprehensive-protocol-audit — OpenZeppelin
- https://blog.openzeppelin.com/compound-polygon-bridge-receiver-audit/ — OpenZeppelin (Polygon bridge receiver)
- https://www.comp.xyz/t/security-service-providers-progress-report/7715 — Compound governance forum SSP progress report

## Contract addresses (Ethereum, from compound-finance/comet roots.json)

- cUSDCv3 (Comet USDC proxy): 0xc3d688B66703497DAA19211EEdff47f25384cdc3
- cWETHv3 (Comet WETH proxy): 0xA17581A9E3356d9A858b789D68B4d866e593aE94
- Configurator: 0x316f9708bB98af7dA9c68C1C3b5e79039cD336E3
- CometRewards: 0x1B0e765F6224C21223AeA2af16c1C46E38885a40
- MainnetBulker: 0xa397a8C2086C554B531c02E29f3291c9704B00c7

## Signal

Benchmark found the ChainSecurity and Certora **landing pages** but not the actual **report PDFs** that sit on `reports.chainsecurity.com/...` or `certora.com/wp-content/...`. Same blind spot pattern: search APIs index HTML landing pages over CDN-hosted PDFs. Recommendation for the discovery pipeline: when a classified result points to an auditor's root domain, follow links to their /reports subdomain and /wp-content uploads.
