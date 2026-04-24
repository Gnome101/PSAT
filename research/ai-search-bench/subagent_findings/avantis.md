# avantis — subagent ground-truth pass

Confidence: **High** — audits verified on docs.avantisfi.com + zokyo-sec GitHub; addresses from Avantis-Labs/avantisfi-integration SDK + BaseScan.

## Missed audits (not in benchmark union)

- https://audits.sherlock.xyz/contests/485 — Sherlock (Avantis v1.5: Cross-Asset Leverage contest)
- https://1312337203-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2F76vAZHPcNKY10NzuKsC4%2Fuploads%2F7wBgRID4Op6TuWhWgxxn%2FAvantis%20v1.5%20Cross%20Asset%20Leverage%20Audit%20Report.pdf — Sherlock (v1.5 final PDF)
- https://1312337203-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2F76vAZHPcNKY10NzuKsC4%2Fuploads%2FNJ5XV09sg4mskC12Z0uq%2FAvantis%20-%20Zellic%20Audit%20Report.pdf — Zellic (v1.5, distinct from original)
- https://github.com/zokyo-sec/audit-reports/blob/main/Avantis/Avantis_Zokyo_audit_report_Dec23_2023.pdf — Zokyo (canonical GitHub, Dec 23 2023)

## Contract addresses (Base, from Avantis-Labs/avantisfi-integration + BaseScan)

- Trading (proxy "Avantis V1.5: Trading"): 0x44914408af82bc9983bbb330e3578e1105e11d4e
- TradingStorage: 0x8a311D7048c35985aa31C131B9A13e03a5f7422d
- AVNT token: 0x696F9436B67233384889472Cd7cD58A6fB5DF4f1
- USDC (collateral, canonical Circle): 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913

## Signal

Sherlock contest pages (`audits.sherlock.xyz/contests/<id>`) are another blind spot. Recovery: enumerate sherlock.xyz contests when the protocol's name matches one. Also: the gitbook CDN has multiple PDFs per protocol; backends surface one and miss siblings.
