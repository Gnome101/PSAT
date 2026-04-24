# aave — subagent ground-truth pass

Confidence: **High** on v3/GHO/Horizon auditor lineup and addresses; medium on URL-level absence (repo paths overlap but file paths differ).

## Missed audits (not in benchmark union)

- https://github.com/aave-dao/aave-v3-origin/tree/main/audits — Certora (v3.3.0, 2024-11-07) + Certora Collector Rev6 (2025-01-20) in aave-v3-origin
- https://www.certora.com/reports/aave-vault — Certora (Aave Vault audit & FV)
- https://github.com/aave/gho-core/tree/main/audits — OZ (2022-11-10), ABDK (2023-03-01), SigmaPrime (2023-07-06), Certora FV (2023-02-28) GHO bundle
- https://aave.com/blog/aave-v4-security-by-design — Aave V4 disclosure (Trail of Bits, ChainSecurity, Blackthorn, Enigma Dark, Certora)
- https://github.com/Certora/aave-v3-horizon — Certora FV fork for Aave Horizon / v3.3
- https://governance.aave.com/t/security-by-design-aave-v4/24224 — ACI / governance V4 security thread

## Contract addresses (Ethereum)

- Pool v3 (proxy): 0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2
- PoolConfigurator v3 (proxy): 0x64b761D848206f447Fe2dd461b0c635Ec39EbB27
- PoolAddressesProvider v3: 0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e
- GHO Token: 0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f
- WrappedTokenGatewayV3: 0xD322A49006FC828F9B5B37Ab215F99B4E5caB19C
- AAVE (v3 reserve): 0xA700b4eB416Be35b2911fd5Dee80678ff64fF6C9

## Signal

Cross-product audits (GHO, Umbrella, Horizon, V4) each live on their own subrepo with own /audits folder — benchmark catches some, misses others. Same folder-enumeration pattern.
