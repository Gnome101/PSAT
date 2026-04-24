# ethena — subagent ground-truth pass

Confidence: **High** on addresses; moderate on some Pashov sub-reports that are referenced on ethena.fi docs but aren't exact URL matches.

## Missed audits (not in benchmark union)

- https://596495599-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2FsBsPyff5ft3inFy9jyjt%2Fuploads%2FsX7xO54StGnS6RlZM0Qa%2FSpearbit%20_Ethena_v2_Final_report-ethena__1_.pdf — Spearbit (v1, Oct 2023)
- https://596495599-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2FsBsPyff5ft3inFy9jyjt%2Fuploads%2F17Ucep7IYMBZ6mAHGLyw%2FEthena%20Final%20Report%20(1).pdf — Quantstamp (v1, Oct 2023)
- https://596495599-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2FsBsPyff5ft3inFy9jyjt%2Fuploads%2FJHbdthPqKCPoZFzrpryW%2Fv2-audit.pdf — Pashov (v2, May 2024)
- https://code4rena.com/reports/2024-11-ethena-labs — Cyfrin USDtb (Oct 31, 2024)
- Chaos Labs — Economic & Financial Risk review (referenced on ethena.fi docs)
- Pashov — ENA & LP Staking (Dec 2023) + sENA (Sept 2024) reviews (referenced on docs)

## Contract addresses (Ethereum)

- USDe: 0x4c9edd5852cd905f086c759e8383e09bff1e68b3
- sUSDe (Staking): 0x9d39a5de30e57443bff2a8307a4256c8797a3497
- ENA: 0x57e114B691Db790C35207b2e685D4A43181e6061
- sENA: 0x8bE3460A480c80728a8C4D7a5D5303c85ba7B3b9
- EthenaMinting V1: 0x2cc440b721d2cafd6d64908d6d8c4acc57f8afc3
- EthenaMinting V2: 0xe3490297a08d6fC8Da46Edb7B6142E4F461b62D3
- USDe→sUSDe Rewards Distributor: 0xf2fa332bd83149c66b09b45670bce64746c6b439

## Signal

Benchmark found the ethena.fi audits index page and *some* gitbook-hosted PDFs but not *all* of them, despite them living at the same gitbook CDN. Symptom: search backends sometimes list one PDF from a gitbook upload folder and miss siblings.
