// @ts-check
import { test, expect } from "@playwright/test";

/**
 * Regression tests for Bug S2 (audit-to-impl mapping) and Bug S3
 * (page refresh on /address/<addr>/upgrades loses audit data).
 *
 * The test simulates a direct URL hit — equivalent to a browser refresh —
 * at /address/<proxy>/upgrades. The UI must fetch the audit timeline
 * and render both the "N audits in history" chip and per-era chips.
 *
 * NOTE: Playwright matches routes in reverse registration order. The
 * broad catch-all must be registered BEFORE the specific routes.
 */

const TARGET_ADDR = "0x83bc649fcdb2c8da146b2154a559ddedf937ef12";
const IMPL_V1 = "0x1111111111111111111111111111111111111111";
const IMPL_V2 = "0x2222222222222222222222222222222222222222";
const CONTRACT_ID = 42;
const RUN_ID = "run-refresh-fixture";

const ANALYSIS_ROW = {
  job_id: RUN_ID,
  address: TARGET_ADDR,
  run_name: "TestProxy",
  contract_name: "TestProxy",
};

const DETAIL_PAYLOAD = {
  address: TARGET_ADDR,
  run_name: "TestProxy",
  contract_name: "TestProxy",
  contract_id: CONTRACT_ID,
  company: "TestCo",
  upgrade_history: {
    target_address: TARGET_ADDR,
    total_upgrades: 2,
    proxies: {
      [TARGET_ADDR]: {
        proxy_type: "UUPS",
        current_implementation: IMPL_V2,
        upgrade_count: 2,
        first_upgrade_block: 18000000,
        last_upgrade_block: 20000000,
        implementations: [
          {
            address: IMPL_V1,
            block_introduced: 18000000,
            block_replaced: 19000000,
            timestamp_introduced: 1698796800, // 2023-11-01
            timestamp_replaced: 1706745600, // 2024-02-01
            contract_name: "Impl V1",
          },
          {
            address: IMPL_V2,
            block_introduced: 19000000,
            timestamp_introduced: 1706745600, // 2024-02-01
            contract_name: "Impl V2",
          },
        ],
        events: [],
      },
    },
  },
};

const AUDIT_TIMELINE = {
  current_status: "audited",
  coverage: [
    // impl_era match — backend stores cov.impl_address = current impl
    // (V2) because the Contract row for V1 doesn't exist. The frontend
    // must fall through to temporal logic and place this on V1.
    {
      audit_id: 101,
      auditor: "Solidified",
      date: "2023-11-15", // inside V1's era (Nov 2023 – Feb 2024)
      title: "V1 Audit",
      match_type: "impl_era",
      match_confidence: "high",
      covered_from_block: null,
      covered_to_block: null,
      impl_address: IMPL_V2,
    },
    // reviewed_commit match — source-equivalence proof; binds strictly
    // to the impl address it was proven against (V2).
    {
      audit_id: 102,
      auditor: "Trail of Bits",
      date: "2024-03-15", // inside V2's era
      title: "V2 Audit",
      match_type: "reviewed_commit",
      match_confidence: "high",
      covered_from_block: null,
      covered_to_block: null,
      impl_address: IMPL_V2,
    },
  ],
};

/**
 * Register API mocks. The broad /api catch-all goes first so that the
 * later, more-specific mocks take precedence (Playwright runs routes
 * in reverse registration order).
 */
async function mockApis(page, { analyses = [ANALYSIS_ROW], detail = DETAIL_PAYLOAD, timeline = AUDIT_TIMELINE, detailAddress = TARGET_ADDR } = {}) {
  await page.route(/127\.0\.0\.1:5173\/api\//, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" }),
  );
  await page.route("**/api/analyses", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(analyses) }),
  );
  // The App now loads detail via /api/analyses/<URL-address> directly
  // (bypassing the merged /api/analyses list that hides proxy runs behind
  // impl runs). Serve the detail payload at both the URL address and the
  // explicit run id so tests exercising either path work.
  await page.route(`**/api/analyses/${encodeURIComponent(RUN_ID)}`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(detail) }),
  );
  await page.route(`**/api/analyses/${encodeURIComponent(detailAddress)}`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(detail) }),
  );
  await page.route(`**/api/contracts/${CONTRACT_ID}/audit_timeline`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(timeline) }),
  );
}

test.describe("Bug S3: /address/.../upgrades refresh", () => {
  test("audit chips render on direct URL hit (refresh equivalent)", async ({ page }) => {
    await mockApis(page);

    await page.goto(`/address/${TARGET_ADDR}/upgrades`);

    // Wait for the upgrades tab panel to render
    await page.waitForSelector(".timeline", { timeout: 10000 });

    // The upgrades tab must be the active one — the refresh shouldn't
    // leave us on the summary tab.
    const upgradesTab = page.locator(".tab.active", { hasText: "Upgrades" });
    await expect(upgradesTab).toBeVisible();

    // "audits in history" chip should appear at top of upgrades tab
    const historyChip = page.locator(".chip", { hasText: /audit[s]? in history/ });
    await expect(historyChip).toBeVisible({ timeout: 5000 });

    // Per-era chips:
    //   V1 (past)    — Solidified only (impl_era, date in V1 era)
    //   V2 (current) — Trail of Bits only (reviewed_commit binds by address)
    // Solidified's 2023-11-15 date is >14 days outside V2's era even with
    // grace, so matchesEra correctly keeps it off the current impl.
    const auditorChips = page.locator(".timeline .chip", { hasText: /Solidified|Trail of Bits/ });
    await expect(auditorChips).toHaveCount(2, { timeout: 5000 });
  });

  test("audit chips persist across a real page reload()", async ({ page }) => {
    await mockApis(page);

    // First visit — chips render.
    await page.goto(`/address/${TARGET_ADDR}/upgrades`);
    await page.waitForSelector(".timeline .chip", { timeout: 10000 });
    const before = page.locator(".timeline .chip", { hasText: /Solidified|Trail of Bits/ });
    await expect(before).toHaveCount(2, { timeout: 5000 });

    // Browser refresh — chips must come back on the same URL.
    await page.reload();
    await page.waitForSelector(".timeline .chip", { timeout: 10000 });
    const after = page.locator(".timeline .chip", { hasText: /Solidified|Trail of Bits/ });
    await expect(after).toHaveCount(2, { timeout: 5000 });

    // Upgrades tab is still the active tab — the tab shouldn't silently
    // default to Summary after refresh.
    await expect(page.locator(".tab.active", { hasText: "Upgrades" })).toBeVisible();
  });

  test("proxy URL loads proxy run's detail (not the merged impl row)", async ({ page }) => {
    // Real regression: /api/analyses merges proxy+impl into a single row
    // whose `address` is the IMPL's — so a list-based lookup (the old
    // findRunByAddress flow) would load the impl's detail on refresh of
    // /address/<proxy>/upgrades. The impl detail's upgrade_history is
    // keyed to the impl and does NOT include the impl's own proxy chain,
    // so the Upgrades tab shows random dependency proxies first and the
    // target contract's own history disappears.
    //
    // Fix: for /address/<addr> URLs, the App passes <addr> directly to
    // /api/analyses/<name>, which falls back to a by-address lookup and
    // returns the proxy run (with full history). This test proves the
    // proxy run's detail is fetched by asserting the chip count that
    // only the proxy's upgrade_history (2 impls) can produce.
    const MERGED_ROW = {
      job_id: "impl-run-id",
      // Merged row: address = IMPL, proxy_address = URL target
      address: IMPL_V2,
      proxy_address: TARGET_ADDR,
      run_name: "TestProxy",
      contract_name: "Impl V2",
    };
    // Impl detail: its upgrade_history doesn't include its own proxy.
    const IMPL_DETAIL = {
      address: IMPL_V2,
      run_name: "TestProxy: (impl)",
      contract_name: "Impl V2",
      contract_id: CONTRACT_ID,
      company: "TestCo",
      upgrade_history: {
        target_address: IMPL_V2,
        total_upgrades: 0,
        proxies: {}, // no proxies — impl's view
      },
    };
    // Proxy detail: full history with the proxy's 2 impls.
    const PROXY_DETAIL = DETAIL_PAYLOAD;

    // /api/analyses (list) → merged row only, as prod does today.
    await page.route(/127\.0\.0\.1:5173\/api\//, (route) =>
      route.fulfill({ status: 200, contentType: "application/json", body: "[]" }),
    );
    await page.route("**/api/analyses", (route) =>
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify([MERGED_ROW]) }),
    );
    // By-address lookup: proxy address → proxy detail (what the
    // backend's Job.address fallback returns).
    await page.route(`**/api/analyses/${encodeURIComponent(TARGET_ADDR)}`, (route) =>
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(PROXY_DETAIL) }),
    );
    // Guard: if anything accidentally fetches by the merged row's job_id,
    // it would get the impl detail and the test would fail the chip count.
    await page.route(`**/api/analyses/impl-run-id`, (route) =>
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(IMPL_DETAIL) }),
    );
    await page.route(`**/api/contracts/${CONTRACT_ID}/audit_timeline`, (route) =>
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(AUDIT_TIMELINE) }),
    );

    await page.goto(`/address/${TARGET_ADDR}/upgrades`);
    await page.waitForSelector(".timeline", { timeout: 10000 });
    await page.waitForSelector(".timeline .chip", { timeout: 10000 });

    // If we loaded the impl detail (impl_history has no proxies), the
    // timeline would be empty and this count would be 0. Proxy detail
    // yields V1 Solidified + V2 Trail of Bits → 2 chips.
    const auditorChips = page.locator(".timeline .chip", { hasText: /Solidified|Trail of Bits/ });
    await expect(auditorChips).toHaveCount(2, { timeout: 5000 });
  });
});

test.describe("Bug S2: audit-to-impl mapping (reviewed_commit vs impl_era)", () => {
  test("reviewed_commit binds only to impl address; impl_era falls through to temporal match", async ({ page }) => {
    // Solidified (impl_era, 2023-11-15) must attach to V1 via its date —
    // before the fix, matchesEra returned early on cov.impl_address === V2
    // and V1 showed "no audit coverage".
    //
    // Trail of Bits (reviewed_commit) must bind only to V2; the source-
    // equivalence proof is keyed to the impl's source hash and must not
    // show on V1 even though V1's date range contains the audit date.
    await mockApis(page);

    await page.goto(`/address/${TARGET_ADDR}/upgrades`);

    await page.waitForSelector(".timeline", { timeout: 10000 });
    await page.waitForSelector(".timeline .chip:not(.warn)", { timeout: 10000 });

    // V1 (past) era must now show the Solidified chip (this is the
    // regression the fix addresses) and must NOT show Trail of Bits
    // (reviewed_commit is strictly address-bound).
    const v1Entry = page.locator(".timeline-entry.past").first();
    await expect(v1Entry.locator(".chip", { hasText: "Solidified" })).toHaveCount(1, { timeout: 5000 });
    await expect(v1Entry.locator(".chip", { hasText: "Trail of Bits" })).toHaveCount(0);
    await expect(v1Entry.locator(".chip", { hasText: "no audit coverage" })).toHaveCount(0);

    // V2 (current) era must show Trail of Bits (reviewed_commit against
    // this impl).
    const v2Entry = page.locator(".timeline-entry.current");
    await expect(v2Entry.locator(".chip", { hasText: "Trail of Bits" })).toHaveCount(1, { timeout: 5000 });
  });
});
