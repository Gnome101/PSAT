// Per-function guard summary (label, sublabel, accent, principals).
// Pure — no React.

import { TYPE_META } from "../meta.js";
import { formatDelay, shortAddr } from "../format.js";
import { collectPrincipals } from "./controlGraph.js";

function isExactEmptyCapability(cap) {
  if (!cap || typeof cap !== "object") return false;
  if (cap.kind === "finite_set") {
    return cap.membership_quality === "exact" && Array.isArray(cap.members) && cap.members.length === 0;
  }
  const children = Array.isArray(cap.children) ? cap.children : [];
  if (cap.kind === "AND") return children.some(isExactEmptyCapability);
  if (cap.kind === "OR") return children.length > 0 && children.every(isExactEmptyCapability);
  return false;
}

function isResolvedEmptyFunction(fn) {
  return fn?.status === "resolved_empty" || isExactEmptyCapability(fn?.capability_expr);
}

export function guardSummary(fn, companyData) {
  const { direct, indirect } = collectPrincipals(fn, companyData);
  // `principals` stays as the direct list for backward compatibility — every
  // consumer that reads `fnView.guard.principals` only cares about who can
  // actually call the function *now*, not the governance chain above that.
  const principals = direct;

  if (!direct.length) {
    const kind = fn.authority_public ? "open" : isResolvedEmptyFunction(fn) ? "resolved_empty" : "unknown";
    const meta = TYPE_META[kind];
    return {
      kind,
      label: meta.label,
      sublabel: fn.authority_public ? "public" : kind === "resolved_empty" ? "no active principal" : "unresolved",
      accent: meta.accent,
      principals,
      indirect,
    };
  }

  if (direct.length > 1) {
    return {
      kind: "many",
      label: `${direct.length}P`,
      sublabel: "mixed",
      accent: TYPE_META.many.accent,
      principals,
      indirect,
    };
  }

  const principal = direct[0];
  const principalKind = principal.resolvedType === "unknown" ? "address" : principal.resolvedType;
  const type = TYPE_META[principalKind] || TYPE_META.unknown;
  const safeOwners = Array.isArray(principal.details?.owners) ? principal.details.owners.length : 0;
  const threshold = Number(principal.details?.threshold);
  const delay = formatDelay(principal.details?.delay);

  let sublabel = shortAddr(principal.address);
  if (principal.resolvedType === "safe" && safeOwners) {
    sublabel = Number.isFinite(threshold) && threshold > 0 ? `${threshold}/${safeOwners}` : `${safeOwners} sig`;
  } else if (principal.resolvedType === "timelock" && delay) {
    sublabel = delay;
  } else if (principal.resolvedType === "contract") {
    // Prefer the contract's own name from the protocol inventory over the
    // generic word "contract" — fetchNextKeyIndex resolving to "AuctionManager"
    // tells the user something; "contract" doesn't.
    const targetAddr = principal.address?.toLowerCase();
    const named = (companyData?.contracts || []).find(
      (c) => c.address?.toLowerCase() === targetAddr,
    );
    sublabel = principal.label || named?.name || "contract";
  }

  return {
    kind: principalKind,
    label: type.label,
    sublabel,
    accent: type.accent,
    principals,
    indirect,
  };
}
