// Sanity check: vitest setup file installs all the polyfills we depend
// on. If this file ever fails, every other render test will too — fail
// here first so the diagnostic is local.

import { describe, expect, it } from "vitest";

import { resetFetchMock, setFetchHandler } from "./fetchMock.js";

describe("test environment", () => {
  it("installs ResizeObserver polyfill", () => {
    expect(typeof window.ResizeObserver).toBe("function");
    const ro = new window.ResizeObserver(() => {});
    expect(typeof ro.observe).toBe("function");
  });

  it("installs IntersectionObserver polyfill", () => {
    expect(typeof window.IntersectionObserver).toBe("function");
  });

  it("installs matchMedia polyfill", () => {
    expect(typeof window.matchMedia).toBe("function");
    const m = window.matchMedia("(min-width: 600px)");
    expect(m.matches).toBe(false);
  });

  it("routes fetch through the mock dispatcher by default", async () => {
    resetFetchMock();
    const res = await fetch("/api/analyses");
    expect(res.ok).toBe(true);
    expect(await res.json()).toEqual({});
  });

  it("respects setFetchHandler overrides", async () => {
    resetFetchMock();
    setFetchHandler("/api/analyses", () => [{ job_id: "x" }]);
    const res = await fetch("/api/analyses");
    expect(await res.json()).toEqual([{ job_id: "x" }]);
  });
});
