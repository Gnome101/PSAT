// Direct tests for the pure helpers extracted from ProtocolSurface.jsx.
// Previously exercised only through the rendered surface — these run the
// builders against the rich fixture so a regression in any of them shows
// up before we hit a render assertion.

import { describe, it, expect } from "vitest";

import {
  ETHERFI_COMPANY_RICH,
  RICH_ADDRESSES,
} from "../../test/fixtures.js";
import { buildMachines } from "./buildMachines.js";
import { collectPrincipals } from "./controlGraph.js";
import { guardSummary } from "./guardSummary.js";
import { buildSearchResults } from "./search.js";
import { buildGraphLayout } from "./elkLayout.js";

const functionData = Object.fromEntries(
  ETHERFI_COMPANY_RICH.contracts.map((c) => [c.address, c.functions || []]),
);

describe("buildMachines", () => {
  it("groups each fixture function into a lane and skips role constants", () => {
    const machines = buildMachines(ETHERFI_COMPANY_RICH, functionData);
    expect(machines).toHaveLength(2);
    const vault = machines.find((m) => m.address === RICH_ADDRESSES.VAULT);
    const pool = machines.find((m) => m.address === RICH_ADDRESSES.POOL);
    expect(vault.totalFunctions).toBe(6);
    expect(pool.totalFunctions).toBe(3);
    // Vault: upgrade/pause/unpause/setFee → control, deposit → inflow,
    // withdraw → outflow.
    expect(vault.lanes.top.map((f) => f.name)).toEqual(
      expect.arrayContaining(["upgrade", "pause", "unpause"]),
    );
    expect(vault.lanes.left.map((f) => f.name)).toContain("deposit");
    expect(vault.lanes.right.map((f) => f.name)).toContain("withdraw");
  });

  it("sorts machines by totalFunctions desc", () => {
    const machines = buildMachines(ETHERFI_COMPANY_RICH, functionData);
    expect(machines[0].totalFunctions).toBeGreaterThanOrEqual(machines[1].totalFunctions);
  });
});

describe("collectPrincipals", () => {
  it("returns a direct caller for a guarded fixture function", () => {
    const fn = ETHERFI_COMPANY_RICH.contracts[0].functions.find((f) => f.function === "pause");
    const { direct } = collectPrincipals(fn, ETHERFI_COMPANY_RICH);
    expect(direct).toHaveLength(1);
    expect(direct[0].resolvedType).toBe("safe");
  });

  it("returns no direct caller for a public-authority function", () => {
    const fn = ETHERFI_COMPANY_RICH.contracts[0].functions.find((f) => f.function === "deposit");
    const { direct } = collectPrincipals(fn, ETHERFI_COMPANY_RICH);
    expect(direct).toHaveLength(0);
  });
});

describe("guardSummary", () => {
  it("classifies a Safe-guarded function with a threshold sublabel", () => {
    const fn = ETHERFI_COMPANY_RICH.contracts[0].functions.find((f) => f.function === "pause");
    const guard = guardSummary(fn, ETHERFI_COMPANY_RICH);
    expect(guard.kind).toBe("safe");
    expect(guard.sublabel).toBe("2/3");
  });

  it("classifies a Timelock-guarded function with a delay sublabel", () => {
    const fn = ETHERFI_COMPANY_RICH.contracts[0].functions.find((f) => f.function === "upgrade");
    const guard = guardSummary(fn, ETHERFI_COMPANY_RICH);
    expect(guard.kind).toBe("timelock");
    expect(guard.sublabel).toBe("1d");
  });

  it("classifies a public function as kind=open", () => {
    const fn = ETHERFI_COMPANY_RICH.contracts[0].functions.find((f) => f.function === "deposit");
    const guard = guardSummary(fn, ETHERFI_COMPANY_RICH);
    expect(guard.kind).toBe("open");
  });

  it("classifies a no-principal non-public function as kind=unknown", () => {
    const fn = ETHERFI_COMPANY_RICH.contracts[1].functions.find((f) => f.function === "setOracle");
    const guard = guardSummary(fn, ETHERFI_COMPANY_RICH);
    expect(guard.kind).toBe("unknown");
  });
});

describe("buildSearchResults", () => {
  const machines = buildMachines(ETHERFI_COMPANY_RICH, functionData);
  const principals = ETHERFI_COMPANY_RICH.resolved_principals.map((p) => ({
    address: p.address,
    type: p.resolved_type,
    label: p.display_name,
    details: p.details,
    controls: machines.map((m) => m.address),
  }));

  it("filters to safes when mode=safe", () => {
    const results = buildSearchResults(machines, principals, "safe", "name", "");
    expect(results.every((r) => r.kind === "principal" && r.type === "safe")).toBe(true);
  });

  it("returns contracts when mode=all", () => {
    const results = buildSearchResults(machines, principals, "all", "name", "");
    expect(results.every((r) => r.kind === "contract")).toBe(true);
    expect(results.length).toBe(machines.length);
  });

  it("filters by query against name/address/type", () => {
    const results = buildSearchResults(machines, principals, "all", "name", "Vault");
    expect(results.length).toBe(1);
    expect(results[0].name).toBe("Vault");
  });

  it("supports `value > 1m` style numeric filters", () => {
    const machinesWithValue = machines.map((m) => ({ ...m, total_usd: m.address === RICH_ADDRESSES.VAULT ? 5_000_000 : 100 }));
    const results = buildSearchResults(machinesWithValue, principals, "all", "value", "value > 1m");
    expect(results.every((r) => r.value >= 1_000_000)).toBe(true);
  });
});

describe("buildGraphLayout", () => {
  const machines = buildMachines(ETHERFI_COMPANY_RICH, functionData);

  it("returns one node per contract + one per principal, with edges from fund_flows", () => {
    const principals = ETHERFI_COMPANY_RICH.resolved_principals.map((p) => ({
      address: p.address,
      type: p.resolved_type,
      label: p.display_name,
      details: p.details,
      controls: [machines[0].address],
    }));
    const { nodes, edges } = buildGraphLayout(machines, ETHERFI_COMPANY_RICH.fund_flows, principals);
    const contractNodes = nodes.filter((n) => n.type === "contract");
    const principalNodes = nodes.filter((n) => n.type === "principal");
    expect(contractNodes.length).toBe(machines.length);
    expect(principalNodes.length).toBe(principals.length);
    // Vault → Pool fund flow generates one edge between two contracts the
    // builder also placed nodes for.
    expect(edges.some((e) => e.id.startsWith("flow-"))).toBe(true);
  });
});
