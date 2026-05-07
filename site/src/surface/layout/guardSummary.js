// Per-function guard summary (label, sublabel, accent, principals).
// Pure — no React.

import { TYPE_META } from "../meta.js";
import { formatDelay, shortAddr } from "../format.js";
import { collectPrincipals } from "./controlGraph.js";

export function guardSummary(fn, companyData) {
  const { direct, indirect } = collectPrincipals(fn, companyData);
  // `principals` stays as the direct list for backward compatibility — every
  // consumer that reads `fnView.guard.principals` only cares about who can
  // actually call the function *now*, not the governance chain above that.
  const principals = direct;

  if (!direct.length) {
    const meta = TYPE_META[fn.authority_public ? "open" : "unknown"];
    return {
      kind: fn.authority_public ? "open" : "unknown",
      label: meta.label,
      sublabel: fn.authority_public ? "public" : "unresolved",
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
  const type = TYPE_META[principal.resolvedType] || TYPE_META.unknown;
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
    kind: principal.resolvedType,
    label: type.label,
    sublabel,
    accent: type.accent,
    principals,
    indirect,
  };
}
