# ether.fi — subagent ground-truth pass

Confidence: **High** — all audits pulled directly from etherfi-protocol/smart-contracts GitHub audits/ directory; addresses cross-checked on gitbook + Etherscan.

## Missed audits (not in benchmark union)

- https://github.com/etherfi-protocol/smart-contracts/blob/master/audits/2023.05.16%20-%20Omniscia.pdf — Omniscia
- https://github.com/etherfi-protocol/smart-contracts/blob/master/audits/2024.04.08%20-%20Decurity.pdf — Decurity
- https://github.com/etherfi-protocol/smart-contracts/blob/master/audits/2024.06.25%20-%20Halborn%20-%20EtherFi_L2_Governance_Token_Smart_Contract_Security_Assessment_Report.pdf — Halborn (L2 Governance Token)
- https://github.com/etherfi-protocol/smart-contracts/blob/master/audits/2024.08.02%20-%20Halborn%20-%20EtherFi_EFIP_5_%26_EFIP_8_Implementation_Smart_Contract_Security_Assessment.pdf — Halborn (EFIP-5/8)
- https://github.com/etherfi-protocol/smart-contracts/blob/master/audits/2024.09.30%20-%20Paladin_EtherFi_OFT_Adapter_Migration.pdf — Paladin
- https://github.com/etherfi-protocol/smart-contracts/blob/master/audits/2024.10.08%20-%20Certora%20-%20EtherFi%20draft.pdf — Certora draft (plus 12+ additional Certora reports 2025–2026 in same folder covering Pectra, Slashing, Withdrawals, Priority Queue)
- https://github.com/etherfi-protocol/smart-contracts/blob/master/audits/NM-0217%20-%20EtherFi%20Deposit%20Adapter%20Contract.md — Nethermind (NM-0217 Deposit Adapter)
- https://github.com/etherfi-protocol/smart-contracts/blob/master/audits/NM-0217%20-%20EtherFi%20Restaking%20Of%20stETH%20Holdings.md — Nethermind (NM-0217 stETH Restaking)

## Contract addresses (Ethereum)

- LiquidityPool: 0x308861A430be4cce5502d0A12724771Fc6DaF216
- EtherFiNodesManager: 0x8B71140AD2e5d1E7018d2a7f8a288BD3CD38916F
- eETH: 0x35fa164735182de50811e8e2e824cfb9b6118ac2
- weETH: 0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee

## Signal

Benchmark found ~12 URLs from ether.fi's own gitbook but **missed most of the protocol's own `audits/` GitHub directory** (~20+ PDFs). Confirms the systemic blind spot: search APIs index rendered HTML + gitbook but don't crawl GitHub tree contents. A `walk_github_audits_folder` follow-up would capture a huge recall bump.
