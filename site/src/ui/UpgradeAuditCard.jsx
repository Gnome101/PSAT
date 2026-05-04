import {
  DRIFT_FALSE_META,
  DRIFT_TRUE_META,
  EQUIVALENCE_META,
  formatAuditDate,
  formatAuditTimestamp,
  MATCH_TYPE_META,
  MetaBadge,
  proofKindTitle,
  PROOF_KIND_META,
} from "../auditUi.jsx";

const BLOCK_NUMBER_FORMAT = new Intl.NumberFormat("en-US");

function formatCoverageWindow(coverage) {
  const fromBlock = coverage?.covered_from_block;
  const toBlock = coverage?.covered_to_block;
  if (fromBlock == null && toBlock == null) return null;
  if (fromBlock != null && toBlock != null) {
    return `blocks ${BLOCK_NUMBER_FORMAT.format(fromBlock)}-${BLOCK_NUMBER_FORMAT.format(toBlock)}`;
  }
  if (fromBlock != null) {
    return `from block ${BLOCK_NUMBER_FORMAT.format(fromBlock)}`;
  }
  return `through block ${BLOCK_NUMBER_FORMAT.format(toBlock)}`;
}

function coverageVerificationLabel(coverage) {
  const stamp = formatAuditTimestamp(coverage?.verified_at || coverage?.equivalence_checked_at);
  if (!stamp) return null;
  return `${coverage?.equivalence_status === "proven" ? "verified" : "checked"} ${stamp}`;
}

function coverageSummary(coverage) {
  if (coverage?.proof_kind === "pre_fix_unpatched") {
    return proofKindTitle(coverage.proof_kind);
  }
  if (coverage?.equivalence_status === "proven" && coverage?.equivalence_reason) {
    return `Source proof: ${coverage.equivalence_reason}`;
  }
  if (coverage?.proof_kind === "post_fix" || coverage?.proof_kind === "cited_only") {
    return proofKindTitle(coverage.proof_kind);
  }
  if (coverage?.equivalence_reason) {
    return coverage.equivalence_reason;
  }
  if (coverage?.match_type === "reviewed_address") {
    return "The audit scope explicitly pinned this deployed address.";
  }
  return null;
}

export function UpgradeAuditCard({ coverage, companyName }) {
  const matchMeta = MATCH_TYPE_META[coverage.match_type] || MATCH_TYPE_META.direct;
  const driftKnown = coverage.bytecode_drift === true || coverage.bytecode_drift === false;
  const verificationLabel = coverageVerificationLabel(coverage);
  const blockWindow = formatCoverageWindow(coverage);
  const note = coverageSummary(coverage);
  const findings = coverage.live_findings || [];
  const findingTitle = findings
    .slice(0, 3)
    .map((finding) => finding?.title)
    .filter(Boolean)
    .join(" • ");
  const href = companyName
    ? `/company/${encodeURIComponent(companyName)}/audits?audit=${encodeURIComponent(coverage.audit_id)}`
    : null;

  const content = (
    <>
      <div className="upgrade-audit-card-top">
        <div className="upgrade-audit-card-auditor">{coverage.auditor || "Unknown"}</div>
        <div className="upgrade-audit-card-date">{formatAuditDate(coverage.date)}</div>
      </div>
      <div className="upgrade-audit-card-title">{coverage.title || ""}</div>
      <div className="ps-inspector-badges upgrade-audit-card-badges">
        <MetaBadge meta={matchMeta} />
        {coverage.match_confidence ? (
          <span className="ps-badge" style={{ "--badge-accent": "#6b7590", fontSize: 10 }}>
            {coverage.match_confidence}
          </span>
        ) : null}
        {coverage.equivalence_status && coverage.equivalence_status !== "proven" && EQUIVALENCE_META[coverage.equivalence_status] ? (
          <MetaBadge
            meta={EQUIVALENCE_META[coverage.equivalence_status]}
            title={coverage.equivalence_reason || ""}
          />
        ) : null}
        {coverage.equivalence_status === "proven" && coverage.proof_kind && PROOF_KIND_META[coverage.proof_kind] ? (
          <MetaBadge
            meta={PROOF_KIND_META[coverage.proof_kind]}
            title={proofKindTitle(coverage.proof_kind)}
          />
        ) : null}
        {coverage.bytecode_drift === true ? (
          <MetaBadge
            meta={DRIFT_TRUE_META}
            label="⚠ code changed"
            title="Runtime bytecode at this impl changed since the audit was matched"
          />
        ) : null}
        {coverage.bytecode_drift === false ? (
          <MetaBadge
            meta={DRIFT_FALSE_META}
            label="✓ bytecode stable"
            title="Runtime bytecode hash matches the hash captured at audit match time"
          />
        ) : null}
        {!driftKnown && coverage.bytecode_keccak_now && !coverage.bytecode_keccak_at_match ? (
          <span
            className="ps-badge"
            title="Anchor not set — refresh coverage to stamp runtime bytecode hash"
            style={{ "--badge-accent": "#6b7590", fontSize: 10 }}
          >
            drift unverified
          </span>
        ) : null}
        {findings.length ? (
          <span
            className="ps-badge"
            title={findingTitle || "This audit still has live findings on the current code"}
            style={{ "--badge-accent": "#92400e", fontSize: 10 }}
          >
            {findings.length} live finding{findings.length === 1 ? "" : "s"}
          </span>
        ) : null}
      </div>
      {verificationLabel || blockWindow ? (
        <div className="upgrade-audit-card-facts">
          {verificationLabel ? <span>{verificationLabel}</span> : null}
          {blockWindow ? <span>{blockWindow}</span> : null}
        </div>
      ) : null}
      {note ? <div className="upgrade-audit-card-note">{note}</div> : null}
    </>
  );

  return href ? (
    <a className="upgrade-audit-card" href={href}>
      {content}
    </a>
  ) : (
    <div className="upgrade-audit-card">{content}</div>
  );
}

export default UpgradeAuditCard;
