// Resolve /address/<x> URLs to a specific analysis run.
//
// Extracted from App.jsx so it's unit-testable in isolation. The
// distinction matters: a proxy address can match many runs (every
// impl run stamps the proxy into its ``proxy_address`` field), and
// picking the wrong one loads the wrong ``contract_id`` +
// ``upgrade_history`` and breaks the Upgrades tab's audit chips.

export function findRunByAddress(analyses, address) {
  const target = String(address || "").toLowerCase();
  if (!target) return null;

  // Preferred: the URL's address IS this run's primary subject.
  const byAddress = (analyses || []).find(
    (a) => String(a?.address || "").toLowerCase() === target,
  );
  if (byAddress) return byAddress.job_id || null;

  // Fallback: the URL names a proxy that some impl run was behind.
  // Useful when the user pastes a proxy address that doesn't have
  // its own run — we'll load the impl run instead so they see some
  // data rather than a 404.
  const byProxy = (analyses || []).find((a) => {
    const proxyAddr = String(a?.proxy_address || a?.proxy_address_display || "").toLowerCase();
    return proxyAddr === target;
  });
  return byProxy?.job_id || null;
}
