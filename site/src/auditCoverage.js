export function isBytecodeVerifiedAudit(audit) {
  const status = String(audit?.equivalence_status || "").toLowerCase();
  const matchType = String(audit?.match_type || "").toLowerCase();
  const proofKind = String(audit?.proof_kind || "").toLowerCase();
  const source = String(audit?.coverage_source || "").toLowerCase();

  const strictReviewedCommit = (
    status === "proven" &&
    matchType === "reviewed_commit" &&
    proofKind !== "cited_only" &&
    audit?.bytecode_drift !== true
  );
  const canonicalStandard = (
    status === "proven" &&
    source === "canonical_standard" &&
    matchType === "canonical_standard" &&
    proofKind === "canonical_standard"
  );

  return strictReviewedCommit || canonicalStandard;
}

export function isCanonicalStandardCoverage(audit) {
  return String(audit?.coverage_source || "").toLowerCase() === "canonical_standard";
}

export function bytecodeVerifiedAudits(audits) {
  return Array.isArray(audits) ? audits.filter(isBytecodeVerifiedAudit) : [];
}
