import { describe, it, expect } from "vitest";

import { matchesEra, parseAuditTs } from "./auditMatching.js";

// Unix seconds
const TS_NOV_2023 = 1698796800; // 2023-11-01
const TS_FEB_2024 = 1706745600; // 2024-02-01

const IMPL_V1 = {
  address: "0x1111111111111111111111111111111111111111",
  block_introduced: 18000000,
  block_replaced: 19000000,
  timestamp_introduced: TS_NOV_2023,
  timestamp_replaced: TS_FEB_2024,
};

const IMPL_V2_CURRENT = {
  address: "0x2222222222222222222222222222222222222222",
  block_introduced: 19000000,
  timestamp_introduced: TS_FEB_2024,
};

describe("parseAuditTs", () => {
  it("returns null for falsy input", () => {
    expect(parseAuditTs(null)).toBeNull();
    expect(parseAuditTs(undefined)).toBeNull();
    expect(parseAuditTs("")).toBeNull();
  });

  it("returns null for an unparseable string", () => {
    expect(parseAuditTs("not-a-date")).toBeNull();
  });

  it("parses ISO dates into milliseconds", () => {
    expect(parseAuditTs("2023-11-15")).toBe(Date.parse("2023-11-15"));
  });
});

describe("matchesEra: reviewed_commit is strictly address-bound", () => {
  const cov = {
    audit_id: 1,
    match_type: "reviewed_commit",
    impl_address: IMPL_V2_CURRENT.address,
    date: "2024-03-15",
  };

  it("attaches to the impl whose address matches", () => {
    expect(matchesEra(cov, IMPL_V2_CURRENT)).toBe(true);
  });

  it("does NOT fall through to temporal matching on other impls", () => {
    // Even if the audit date fits V1's era, reviewed_commit should
    // stay pinned to the address it was proven against.
    const earlyCov = { ...cov, date: "2023-11-15" };
    expect(matchesEra(earlyCov, IMPL_V1)).toBe(false);
  });

  it("matches case-insensitively", () => {
    const mixedCase = { ...cov, impl_address: cov.impl_address.toUpperCase() };
    expect(matchesEra(mixedCase, IMPL_V2_CURRENT)).toBe(true);
  });
});

describe("matchesEra: impl_era / direct falls through to temporal", () => {
  // Backend stores cov.impl_address = current impl (V2) because it only
  // has a Contract row for the current impl. The frontend must fall
  // through to block/date logic to correctly place past-era audits.
  //
  // Note: the address-match path also succeeds for V2 — the audit
  // surfaces on both eras. Dedup at the chip-count level is handled
  // by the caller (placed Set keyed by audit_id).
  it("places a Nov 2023 impl_era audit on V1 via date (previously blocked by address-equality early-return)", () => {
    const cov = {
      audit_id: 1,
      match_type: "impl_era",
      impl_address: IMPL_V2_CURRENT.address, // backend set to current
      date: "2023-11-15",
      covered_from_block: null,
      covered_to_block: null,
    };
    // The fix: V1 now matches via the date fall-through.
    expect(matchesEra(cov, IMPL_V1)).toBe(true);
    // V2 still matches via address (not the behavior we want for
    // impl_era, but the dedup Set upstream ensures we don't double-count).
    expect(matchesEra(cov, IMPL_V2_CURRENT)).toBe(true);
  });

  it("still allows an audit on the current impl when addresses match", () => {
    // An audit where address matches AND no blocks AND no date still
    // binds via address.
    const cov = {
      audit_id: 2,
      match_type: "impl_era",
      impl_address: IMPL_V2_CURRENT.address,
      date: null,
    };
    expect(matchesEra(cov, IMPL_V2_CURRENT)).toBe(true);
  });

  it("uses block range when covered_from_block / covered_to_block are set", () => {
    const cov = {
      audit_id: 3,
      match_type: "direct",
      impl_address: null,
      covered_from_block: 18500000,
      covered_to_block: 18600000,
    };
    expect(matchesEra(cov, IMPL_V1)).toBe(true);
    expect(matchesEra(cov, IMPL_V2_CURRENT)).toBe(false);
  });

  it("returns false if neither block range nor date resolves", () => {
    const cov = {
      audit_id: 4,
      match_type: "impl_era",
      impl_address: null,
      date: null,
      covered_from_block: null,
      covered_to_block: null,
    };
    expect(matchesEra(cov, IMPL_V1)).toBe(false);
    expect(matchesEra(cov, IMPL_V2_CURRENT)).toBe(false);
  });

  it("handles an open-ended current era (no block_replaced / timestamp_replaced)", () => {
    const cov = {
      audit_id: 5,
      match_type: "impl_era",
      impl_address: null,
      date: "2025-09-01",
      covered_from_block: null,
      covered_to_block: null,
    };
    expect(matchesEra(cov, IMPL_V2_CURRENT)).toBe(true);
    expect(matchesEra(cov, IMPL_V1)).toBe(false);
  });

  it("date boundary check: timestamp_replaced is exclusive", () => {
    // An audit dated exactly at V1's replaced time should belong to V2
    // (half-open interval [eraFromTs, eraToTs)).
    const cov = {
      audit_id: 6,
      match_type: "impl_era",
      impl_address: null,
      date: new Date(TS_FEB_2024 * 1000).toISOString(),
      covered_from_block: null,
      covered_to_block: null,
    };
    expect(matchesEra(cov, IMPL_V1)).toBe(false);
    expect(matchesEra(cov, IMPL_V2_CURRENT)).toBe(true);
  });
});

describe("matchesEra: defensive handling", () => {
  it("returns false for an empty coverage row with no date or block range", () => {
    expect(matchesEra({}, IMPL_V1)).toBe(false);
  });

  it("returns false when cov is an empty coverage row and impl is null", () => {
    // No address match, no blocks, no date → cannot resolve to any era.
    expect(matchesEra({}, null)).toBe(false);
  });
});
