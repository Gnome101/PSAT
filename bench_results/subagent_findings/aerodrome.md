# aerodrome — subagent ground-truth pass

Confidence: **High** — Spearbit PDFs verified in portfolio repo, all addresses verified on BaseScan.

## Missed audits (not in benchmark union)

- https://raw.githubusercontent.com/spearbit/portfolio/master/pdfs/Velodrome-Spearbit-Security-Review.pdf — Spearbit (Velodrome V2, parent codebase Aerodrome forked)
- https://raw.githubusercontent.com/spearbit/portfolio/master/pdfs/Velodrome-Spearbit-Security-Review-Nov23.pdf — Spearbit (Velodrome Slipstream / CL, inherited by Aerodrome Slipstream)
- https://code4rena.com/reports/2022-05-velodrome — Code4rena (Velodrome Finance contest, 6H/17M)

Note: these cover Velodrome, not Aerodrome directly, but Aerodrome is an acknowledged fork — so they apply to most of the code.

## Contract addresses (Base, verified on BaseScan)

- Voter: 0x16613524e02ad97eDfeF371bC883F2F5d6C480A5
- PoolFactory: 0x420DD381b31aEf6683db6B902084cB0FFECe40Da
- Router: 0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43
- VotingEscrow (veAERO): 0xeBf418Fe2512e7E6bd9b87a8F0f294aCDC67e6B4
- FactoryRegistry: 0x5C3F18F06CC09CA1910767A34a20F771039E37C0
- AERO token: 0x940181a94A35A4569E4529A3CDfB74e38FD98631 *(already in union)*

## Signal

Benchmark missed fork-ancestor audits (Velodrome). Same-repo auditor portfolios (Spearbit GitHub) are another blind spot — classifier only sees the rendered PDF URL if search surfaces it; directory listing isn't being walked.
