// Audit-coverage matching helpers for the Upgrades tab.
//
// Extracted from App.jsx so they can be unit-tested and kept pure.
// These functions decide which audit coverage row (from the
// audit_timeline API) attaches to which implementation era in the
// upgrade_history artifact.
//
// Backend note: the `contracts` table only has a row for the CURRENT
// implementation of each proxy, so every coverage row's
// `impl_address` points at the current impl — even for audits that
// cover past impls. The frontend must fall through to temporal
// (block/date) matching for those rows, except when the match is a
// source-equivalence proof (`match_type === "reviewed_commit"`),
// which is binding to a specific impl's source hash and should not
// spread across other eras.

export function parseAuditTs(date) {
  if (!date) return null;
  const t = Date.parse(date);
  return Number.isNaN(t) ? null : t;
}

export function matchesEra(cov, impl) {
  const addrMatch = !!(
    cov?.impl_address &&
    impl?.address &&
    cov.impl_address.toLowerCase() === impl.address.toLowerCase()
  );

  if (cov?.match_type === "reviewed_commit") {
    // Source-equivalence is binding to one impl — don't fall through.
    return addrMatch;
  }

  if (addrMatch) return true;

  // Fall through to block/date temporal logic. `direct` and `impl_era`
  // matches arrive with cov.impl_address set to the CURRENT impl
  // (backend has no Contract row for past impls), so address equality
  // alone would pin every such audit to the current era.
  const covFrom = cov?.covered_from_block;
  const covTo = cov?.covered_to_block;
  const hasBlocks = covFrom != null || covTo != null;
  if (hasBlocks) {
    const eraFrom = impl?.block_introduced ?? -Infinity;
    const eraTo = impl?.block_replaced ?? Infinity;
    const cFrom = covFrom ?? -Infinity;
    const cTo = covTo ?? Infinity;
    return cFrom < eraTo && cTo > eraFrom;
  }

  // Null block range → compare audit date against impl-era timestamps,
  // else the same audit would surface under every era.
  const auditTs = parseAuditTs(cov?.date);
  if (auditTs == null) return false;
  // impl timestamps are unix seconds; Date.parse returns ms.
  const eraFromTs =
    impl?.timestamp_introduced != null ? impl.timestamp_introduced * 1000 : -Infinity;
  const eraToTs =
    impl?.timestamp_replaced != null ? impl.timestamp_replaced * 1000 : Infinity;
  return auditTs >= eraFromTs && auditTs < eraToTs;
}
