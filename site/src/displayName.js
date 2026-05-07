// `display_name` is the user-facing label for a contract entry. Falls
// back through display_name → contract_name → run_name. The proxy
// shortlist exists because raw `contract_name` values like
// "TransparentUpgradeableProxy" are useless in lists — when we hit
// one, prefer the run_name (which is usually the protocol's own name).

const GENERIC_PROXY_NAMES = new Set([
  "uupsproxy",
  "erc1967proxy",
  "transparentupgradeableproxy",
  "proxy",
  "beaconproxy",
  "ossifiableproxy",
  "withdrawalsmanagerproxy",
  "upgradeablebeacon",
]);

export function displayName(entry) {
  const explicit = entry?.display_name || "";
  if (explicit) {
    return explicit;
  }
  const contractName = entry?.contract_name || "";
  if (GENERIC_PROXY_NAMES.has(contractName.toLowerCase())) {
    return entry.run_name || contractName;
  }
  return contractName || entry?.run_name || "";
}
