// Top-level App router smoke tests. Each case sets a target URL,
// renders <App />, and asserts a stable landmark for that route renders
// without throwing. The goal is regression coverage for the upcoming
// App.jsx file split — if any internal page (PipelineDashboard,
// ProtocolMonitoringPage, ProxyWatcherPage, CompanyOverview, the per-tab
// renderers) breaks during the split, one of these will fail.
//
// Each test also asserts the ErrorBoundary fallback ("Something went
// wrong") is NOT showing, so a thrown render error is caught even when
// the surrounding shell still mounts.
//
// Assertions intentionally hit class-name selectors / role-based queries
// rather than full snapshots, to avoid breaking on incidental DOM shape
// changes during legitimate refactors.

import React from "react";
import { describe, it, expect, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";

import App from "./App.jsx";
import { setFetchHandler } from "./test/fetchMock.js";
import {
  ANALYSIS_LIST,
  ETHERFI_COMPANY,
  COVERAGE_FIXTURE,
  ANALYSIS_DETAIL,
} from "./test/fixtures.js";

const ETHERFI_ADDR = "0x1111111111111111111111111111111111111111";

function navigateTo(path) {
  window.history.replaceState({}, "", path);
}

function expectNoCrash() {
  // The App-level ErrorBoundary renders this header when a child throws.
  // Asserting absence catches silent crashes that the surrounding nav
  // doesn't expose.
  expect(screen.queryByText(/Something went wrong/i)).not.toBeInTheDocument();
}

function installDefaultApiMocks() {
  setFetchHandler(/^\/api\/analyses$/, () => ANALYSIS_LIST);
  setFetchHandler(
    (url) => /^\/api\/analyses\//.test(url.pathname),
    () => ANALYSIS_DETAIL,
  );
  setFetchHandler(/^\/api\/jobs/, () => []);
  setFetchHandler(/^\/api\/stats/, () => ({}));
  setFetchHandler(/^\/api\/audits\/pipeline$/, () => ({ groups: [], recent_completed: [] }));
  setFetchHandler(
    (url) => url.pathname === "/api/company/etherfi/audit_coverage",
    () => COVERAGE_FIXTURE,
  );
  setFetchHandler(
    (url) => url.pathname === "/api/company/etherfi",
    () => ETHERFI_COMPANY,
  );
  setFetchHandler(/^\/api\/monitored-contracts/, () => ({ items: [] }));
  // One real proxy so ProxyWatcherPage's row .map() actually runs in tests
  // (an empty list silently skips the React.Fragment branch — the very thing
  // that crashed in prod when the import was missing).
  setFetchHandler(/^\/api\/watched-proxies$/, () => [
    {
      id: "proxy-1",
      label: "test-proxy",
      proxy_address: "0x2222222222222222222222222222222222222222",
      proxy_type: "EIP1967",
      last_known_implementation: "0x3333333333333333333333333333333333333333",
      needs_polling: false,
      last_scanned_block: 12345,
    },
  ]);
  setFetchHandler(
    (url) => /^\/api\/watched-proxies\/[^/]+\/subscriptions$/.test(url.pathname),
    () => [],
  );
  setFetchHandler(/^\/api\/proxy-events/, () => []);
  setFetchHandler(/^\/api\/address_labels$/, () => ({ labels: {} }));
  setFetchHandler(
    (url) => /^\/api\/company\/[^/]+\/audits$/.test(url.pathname),
    () => ({ audit_count: 0, audits: [] }),
  );
  setFetchHandler(
    (url) => /^\/api\/protocols\//.test(url.pathname),
    (url) => {
      // /monitoring, /subscriptions, /events return arrays in real API.
      if (/\/(monitoring|subscriptions|events)/.test(url.pathname)) return [];
      return {};
    },
  );
}

describe("App router smoke tests", () => {
  beforeEach(() => {
    installDefaultApiMocks();
  });

  it("renders the home / runs page at /", async () => {
    navigateTo("/");
    render(<App />);
    expect(await screen.findByText(/Detect every/i)).toBeInTheDocument();
    expect(document.querySelector(".ph-title")).toBeInTheDocument();
    expectNoCrash();
  });

  it("renders the pipeline dashboard at /monitor", async () => {
    navigateTo("/monitor");
    render(<App />);
    // PipelineDashboard either shows the loading state or the full
    // dashboard — both prove the route resolved + the component mounted.
    await waitFor(() => {
      const loading = screen.queryByText(/Loading pipeline status/i);
      const eyebrow = screen.queryByText(/Pipeline Status/i);
      expect(loading || eyebrow).toBeInTheDocument();
    });
    expectNoCrash();
  });

  it("renders the proxy watcher at /proxies", async () => {
    navigateTo("/proxies");
    render(<App />);
    expect(await screen.findByText(/Watched Proxies/i)).toBeInTheDocument();
    // Wait for the row to mount so the .map() runs the React.Fragment path —
    // empty-list fixtures silently skip this and let import bugs slip through.
    expect(await screen.findByText("test-proxy")).toBeInTheDocument();
    expectNoCrash();
  });

  it("renders the company overview at /company/:name", async () => {
    navigateTo("/company/etherfi");
    render(<App />);
    expect(await screen.findByRole("heading", { name: "etherfi" })).toBeInTheDocument();
    expectNoCrash();
  });

  it("renders the company surface tab at /company/:name/surface", async () => {
    navigateTo("/company/etherfi/surface");
    render(<App />);
    await waitFor(() => {
      expect(document.querySelector(".fullscreen-surface")).toBeInTheDocument();
    });
    expectNoCrash();
  });

  it("renders the company graph tab at /company/:name/graph", async () => {
    navigateTo("/company/etherfi/graph");
    render(<App />);
    await waitFor(() => {
      expect(document.querySelector(".protocol-graph-wrapper")).toBeInTheDocument();
    });
    expectNoCrash();
  });

  it("renders the company risk tab at /company/:name/risk", async () => {
    navigateTo("/company/etherfi/risk");
    render(<App />);
    // RiskSurface is lazy-loaded; assert on its loading or loaded state.
    await waitFor(() => {
      const loading = screen.queryByText(/Loading risk (surface|matrix)/i);
      const container = document.querySelector(".rs-container");
      expect(loading || container).toBeTruthy();
    });
    expectNoCrash();
  });

  it("renders the company monitoring tab at /company/:name/monitoring", async () => {
    navigateTo("/company/etherfi/monitoring");
    const { container } = render(<App />);
    await waitFor(() => {
      // ProtocolMonitoringPage shows either a "Loading protocol
      // monitoring..." paragraph, the "Protocol Monitoring" eyebrow, or
      // its full UI once the company fetch resolves. All three prove
      // the route reached the right component.
      const text = container.textContent || "";
      const matched =
        /Loading protocol monitoring/i.test(text) ||
        /Protocol Monitoring/i.test(text) ||
        /Webhook Subscriptions/i.test(text);
      expect(matched).toBe(true);
    });
    expectNoCrash();
  });

  it("renders the company audits tab at /company/:name/audits", async () => {
    navigateTo("/company/etherfi/audits");
    render(<App />);
    // AuditsTab lazy-loads then shows an "Audits" eyebrow when loaded.
    await waitFor(() => {
      const eyebrow = screen.queryByText(/^Audits$/);
      const loading = screen.queryByText(/Loading audits/i);
      expect(eyebrow || loading).toBeTruthy();
    });
    expectNoCrash();
  });

  it.each([
    ["summary", "Summary"],
    ["permissions", "Permissions"],
    ["principals", "Principals"],
    ["graph", "Graph"],
    ["dependencies", "Dependencies"],
    ["upgrades", "Upgrades"],
    ["raw", "Raw JSON"],
  ])("renders the address page tab %s", async (tab, label) => {
    navigateTo(`/address/${ETHERFI_ADDR}/${tab}`);
    render(<App />);
    // The tab buttons render only after loadAnalysis() resolves and
    // selectedDetail is populated — so finding the active tab proves
    // the API mock was wired and the address branch picked up the URL.
    const activeTab = await waitFor(() => {
      const el = document.querySelector(".tab.active");
      expect(el).toBeInTheDocument();
      return el;
    });
    expect(activeTab).toHaveTextContent(label);
    expectNoCrash();
  });
});
