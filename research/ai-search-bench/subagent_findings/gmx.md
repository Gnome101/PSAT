# gmx — subagent ground-truth pass

Confidence: **High** on v2 addresses; **moderate** on "missed audits" — Halborn link is a hack post-mortem rather than a pre-launch audit; no Watchpug GMX audit appears to exist.

## Missed audits (not in benchmark union)

- https://www.halborn.com/blog/post/explained-the-gmx-hack-july-2025 — Halborn (v1 reentrancy hack post-mortem, July 2025 — not a pre-launch audit but security-relevant)
- https://gov.gmx.io/t/gmx-solana-mainnet-test-and-second-round-of-audit/4268 — GMX governance (Solana audit coordination)
- https://gmxio.gitbook.io/gmx/audit — GMX gitbook audit page (distinct from docs.gmx.io, which is in union)

## Contract addresses (Arbitrum, GMX v2 Synthetics)

- ExchangeRouter: 0x602b805EedddBbD9ddff44A7dcBD46cb07849685 (newer: 0x7c68c7866a64fa2160f78eeae12217ffbf871fa8)
- DataStore: 0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8
- Reader: 0x22199a49A999c351eF7927602CFB187ec3cae489
- EventEmitter: 0xC8ee91A54287DB53897056e12D9819156D3822Fb

## Signal

Benchmark found the GitHub umbrella folders (`gmx-synthetics/tree/main/audits`, `gmx-contracts/tree/master/audits`), which do cover ABDK/Certora/Dedaub/Guardian/Sherlock/Quantstamp. Coverage at the *folder level* is good for GMX — the "missed audits" category is narrow. Suggests that **folder/tree URLs are a high-signal classification target** that the pipeline is already catching correctly here. The specific hack-forensics posts are a different class than pre-deployment audits.
