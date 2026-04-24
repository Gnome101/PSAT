# uniswap — subagent ground-truth pass

Confidence: **Bench is effectively saturating.** 40 URLs cover v1/v2/v3/v4/UniswapX/Permit2/Universal Router/UniStaker. Most "misses" are direct PDFs whose folder-level URL the union already has.

## Missed v4 audits (not in benchmark union)

- https://cdn.jsdelivr.net/npm/@uniswap/v4-core@1.0.2/docs/security/audits/TrailOfBits_audit_core.pdf — Trail of Bits (direct PDF)
- https://github.com/abdk-consulting/audits/blob/main/uniswap/ABDK_Uniswap_v4_core_v_0_9.pdf — ABDK (direct v4-core PDF)
- https://www.certora.com/reports/uniswap-v4 — Certora (v4 core FV report)
- https://github.com/Certora/uniswap-v4-periphery-cantina-fv/blob/main/Report.md — Certora (v4-periphery FV)
- https://www.certora.com/blog/uniswap-v4-audits-what-we-learned-about-defi-security — Certora write-up
- https://blog.openzeppelin.com/uniswap-hooks-library-milestone-1-audit — OpenZeppelin (Hooks Library M1)
- https://dedaub.com/blog/the-11m-cork-protocol-hack-a-critical-lesson-in-uniswap-v4-hook-security/ — Dedaub (v4 hook post-mortem)

## Contract addresses (Ethereum, Uniswap v4)

- PoolManager: 0x000000000004444c5dc75cB358380D2e3dE08A90
- PositionManager: 0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e
- Universal Router (v4): 0x66a9893cc07d91d95644aedd05d03f95e1dba8af
- StateView: 0x7ffe42c4a5deea5b0fec41c94c136cf115597227
- Quoter (v4): 0x52f0e24d1c21c8a0cb1e5a5dd6198556bd9e1203

## Signal

At 40 URLs for a protocol this size the benchmark is near-saturating. The "misses" are direct PDFs where the folder is already in the union — consistent blind spot pattern.
