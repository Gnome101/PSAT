# lido — subagent ground-truth pass

Confidence: **High** — audits verified in lidofinance/audits GitHub + docs.lido.fi; addresses Etherscan-verified.

## Missed audits (not in benchmark union)

- https://github.com/lidofinance/audits/blob/main/Sigma%20Prime%20-%20Lido%20Finance%20Security%20Assessment%20Report%20v2.1.pdf — Sigma Prime (v2.1, 2020)
- https://github.com/lidofinance/audits/blob/main/Sigma%20Prime%20-%20Lido%20-%20dc4bc%20Security%20Assessment%20Report%20-%20v2.1%2003-2023.pdf — Sigma Prime (dc4bc, 03-2023)
- https://github.com/lidofinance/audits/blob/main/Statemind%20Lido%20V2%20Audit%20Report%2004-23.pdf — Statemind (Lido V2, 04-2023)
- https://www.openzeppelin.com/news/linea-yield-manager-audit — OpenZeppelin (Linea Yield Manager / Lido V3 stVault, 02-2026)

## Contract addresses (Ethereum)

- stETH: 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84
- wstETH: 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0
- NodeOperatorsRegistry (Curated Staking Module, proxy): 0x55032650b14dF07B85bF18A3a3eC8E0Af2e028d5
- StakingRouter: 0xFdDf38947aFB03C621C71b06C9C70bce73f12999
- WithdrawalQueueERC721: 0x889edC2eDab5f40e902b864aD4d7AdE8E412F9B1

## Signal

Specific PDFs inside `lidofinance/audits/` are missed even when the repo is surfaced — same "folder-enumeration" blind spot as ether.fi.
