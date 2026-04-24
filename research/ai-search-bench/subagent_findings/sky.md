# sky — subagent ground-truth pass

Confidence: **Moderate-high** on audits (verified on sky-ecosystem GitHub + Cantina portfolios); addresses Etherscan-verified.

## Missed audits (not in benchmark union)

- https://cantina.xyz/portfolio/5afcb016-c1e8-4e42-8245-87857f8e1e1a — Cantina (Sky Token Conversion)
- https://cantina.xyz/portfolio/4a1d31e0-c429-4349-8aa0-2b38c4430acc — Cantina (Sky Vote Delegate)
- https://github.com/sky-ecosystem/usds/blob/dev/audit/20240703-cantina-report-maker-nst.pdf — Cantina (USDS/NST)
- https://github.com/sky-ecosystem/usds/blob/dev/audit/20231124-cantina-report-review-makerdao-nst.pdf — Cantina (MakerDAO NST review)
- https://github.com/sky-ecosystem/lockstake/blob/dev/audit/20240626-cantina-report-maker-LSE.pdf — Cantina (Lockstake Engine)
- https://github.com/sky-ecosystem/sky/blob/dev/audit/cantina-report-review-makerdao-ngt.pdf — Cantina (NGT/SKY token)
- https://forum.sky.money/t/publication-of-the-runtime-verification-audit/976 — Runtime Verification (MCD)
- https://immunefi.com/bug-bounty/sky/ — Immunefi bug bounty program

## Contract addresses (Ethereum)

- SKY token: 0x56072C95FAA701256059aa122697B133aDEd9279
- USDS token: 0xdc035d45d973e3ec169d2276ddab16f1e407384f
- sUSDS (Savings USDS): 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD
- MKR→SKY Converter: 0xa1Ea1bA18E88C381C724a75F23a130420C403f9a
- UsdsPsmWrapper (Spark): 0xA188EEC8F81263234dA3622A406892F3D630f98c
- PSM (USDS): 0xf6e72Db5454dd049d0788e411b06CfAF16853042
- UsdsJoin: 0x3C0f895007CA717Aa01c8693e59DF1e8C3777FEB
- Sky Staking Reward (USDS→SKY farm): 0x0650Caf159C5A49f711e8169D4336ECB9b950275

## Signal

Benchmark missed Cantina portfolio items that live on `cantina.xyz/portfolio/<uuid>` — search backends don't surface these UUID-pathed pages well. Recovery: when an audit firm's domain is already in the union, crawl their portfolio/reports index.
