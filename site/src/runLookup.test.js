import { describe, it, expect } from "vitest";

import { findRunByAddress } from "./runLookup.js";

// Real case from the Rewards Router refresh regression:
//   /address/<PROXY> on refresh picked the IMPL run (because its
//   ``proxy_address`` matched the URL). Detail loaded the impl's
//   ``upgrade_history`` with target=impl, ``isTarget(proxyAddr)``
//   failed for every impl-entry, and all audit chips disappeared
//   even though the backend had matched coverage to the impl.
const PROXY = "0x73f7b1184b5cd361cc0f7654998953e2a251dd58";
const IMPL = "0x408de8d339f40086c5643ee4778e0f872ab5e423";

const PROXY_RUN = {
  job_id: "proxy-run-id",
  address: PROXY,
  proxy_address: null,
};
const IMPL_RUN = {
  job_id: "impl-run-id",
  address: IMPL,
  proxy_address: PROXY,
};

describe("findRunByAddress", () => {
  it("returns the run whose primary address matches the URL (proxy URL → proxy run)", () => {
    // This is the regression fix: previously the impl run was picked
    // because its proxy_address matched the URL first.
    expect(findRunByAddress([IMPL_RUN, PROXY_RUN], PROXY)).toBe("proxy-run-id");
    // Order-independent: same result even when IMPL_RUN comes second.
    expect(findRunByAddress([PROXY_RUN, IMPL_RUN], PROXY)).toBe("proxy-run-id");
  });

  it("returns the impl run for the impl URL", () => {
    expect(findRunByAddress([PROXY_RUN, IMPL_RUN], IMPL)).toBe("impl-run-id");
  });

  it("falls back to proxy_address when no primary-address match exists", () => {
    // User pastes a proxy address whose direct run was never submitted.
    // We still want them to see the impl's analysis rather than 404.
    expect(findRunByAddress([IMPL_RUN], PROXY)).toBe("impl-run-id");
  });

  it("returns null for an unknown address", () => {
    expect(findRunByAddress([PROXY_RUN, IMPL_RUN], "0x0000000000000000000000000000000000000000")).toBeNull();
  });

  it("returns null for an empty / missing address", () => {
    expect(findRunByAddress([PROXY_RUN], "")).toBeNull();
    expect(findRunByAddress([PROXY_RUN], null)).toBeNull();
    expect(findRunByAddress([PROXY_RUN], undefined)).toBeNull();
  });

  it("matches case-insensitively", () => {
    expect(findRunByAddress([PROXY_RUN, IMPL_RUN], PROXY.toUpperCase())).toBe("proxy-run-id");
  });

  it("is defensive against missing analyses list", () => {
    expect(findRunByAddress(null, PROXY)).toBeNull();
    expect(findRunByAddress(undefined, PROXY)).toBeNull();
    expect(findRunByAddress([], PROXY)).toBeNull();
  });

  it("falls back to proxy_address_display if proxy_address is absent", () => {
    const run = { job_id: "display-run", address: IMPL, proxy_address_display: PROXY };
    expect(findRunByAddress([run], PROXY)).toBe("display-run");
  });

  it("skips analyses with missing job_id", () => {
    const runNoId = { address: PROXY };
    // Returns null (not undefined), matching the original contract.
    expect(findRunByAddress([runNoId], PROXY)).toBeNull();
  });
});
