// Filter + sort the machines/principals list for SearchNavigator. Pure.

export function buildSearchResults(machines, principals, mode, sortKey, query) {
  let items = [];

  if (mode === "safe" || mode === "eoa" || mode === "timelock") {
    // Show principals of this type
    const targetType = mode;
    for (const p of principals) {
      if (p.type !== targetType) continue;
      const controlled = (p.controls || []);
      const controlledMachines = machines.filter((m) =>
        controlled.some((a) => a.toLowerCase() === m.address?.toLowerCase())
      );
      const totalValue = controlledMachines.reduce((sum, m) => sum + (m.total_usd || 0), 0);
      const signers = p.details?.threshold || (p.details?.owners?.length) || 0;
      const delay = p.details?.delay || 0;
      items.push({
        kind: "principal",
        address: p.address,
        name: p.label || "",
        type: p.type,
        value: totalValue,
        signers,
        delay,
        functions: controlled.length,
        controlledMachines,
        // Select the first controlled contract when navigating to this principal
        machine: controlledMachines[0] || null,
        principal: p,
      });
    }
  } else {
    // Show contracts
    for (const m of machines) {
      const ownerPrincipal = principals.find((p) =>
        (p.controls || []).some((a) => a.toLowerCase() === m.address?.toLowerCase())
      );
      items.push({
        kind: "contract",
        address: m.address,
        name: m.name || "",
        type: ownerPrincipal?.type || "unknown",
        value: m.total_usd || 0,
        signers: ownerPrincipal?.details?.threshold || 0,
        delay: 0,
        functions: m.totalFunctions || 0,
        machine: m,
        principal: ownerPrincipal,
      });
    }
    if (mode === "funds") items = items.filter((i) => i.value > 0);
  }

  // Text query
  if (query) {
    const q = query.toLowerCase().trim();
    const minMatch = q.match(/(?:min(?:imum)?\s*)?value\s*(?:of\s*|>\s*|>=\s*)?\$?(\d+(?:\.\d+)?)\s*(m|k)?/i);
    if (minMatch) {
      let threshold = parseFloat(minMatch[1]);
      const unit = (minMatch[2] || "").toLowerCase();
      if (unit === "m") threshold *= 1e6;
      else if (unit === "k") threshold *= 1e3;
      items = items.filter((i) => i.value >= threshold);
    } else {
      items = items.filter((i) => {
        const haystack = [i.name, i.address, i.type].join(" ").toLowerCase();
        return haystack.includes(q);
      });
    }
  }

  // Sort
  if (sortKey === "value") items.sort((a, b) => b.value - a.value);
  else if (sortKey === "signers") items.sort((a, b) => b.signers - a.signers);
  else if (sortKey === "functions") items.sort((a, b) => b.functions - a.functions);
  else if (sortKey === "name") items.sort((a, b) => a.name.localeCompare(b.name));

  return items;
}
