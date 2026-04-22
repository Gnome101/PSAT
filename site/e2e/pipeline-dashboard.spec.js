import { test, expect } from "@playwright/test";

// Smoke test for the unified /monitor page: one card per protocol (with audit
// sidecar folded in), no standalone "Audit Extraction" section, and a
// "Recently Completed" tape at the bottom. Uses route-level mocks for the
// three endpoints the dashboard polls so it runs offline.
test.describe("pipeline dashboard", () => {
  test.beforeEach(async ({ page }) => {
    await page.route(
      (url) => url.pathname.startsWith("/api/"),
      (route) => route.fulfill({ status: 200, contentType: "application/json", body: "{}" }),
    );

    const now = Date.now();
    const iso = (msAgo) => new Date(now - msAgo).toISOString();

    await page.route(/\/api\/jobs(\?|$)/, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          // etherfi: discovery in progress
          {
            job_id: "j1",
            company: "etherfi",
            address: null,
            name: "etherfi",
            status: "processing",
            stage: "discovery",
            detail: "Discovering audit reports for etherfi",
            request: { company: "etherfi" },
            error: null,
            created_at: iso(40_000),
            updated_at: iso(5_000),
          },
          // etherfi: contract static analysis
          {
            job_id: "j2",
            company: "etherfi",
            address: "0x1111111111111111111111111111111111111111",
            name: "Weeth",
            status: "processing",
            stage: "static",
            detail: "Running Slither",
            request: {},
            created_at: iso(30_000),
            updated_at: iso(3_000),
          },
          // lido: queued
          {
            job_id: "j3",
            company: "lido",
            address: null,
            name: "lido",
            status: "queued",
            stage: "discovery",
            detail: "",
            request: { company: "lido" },
            created_at: iso(1_500),
            updated_at: iso(1_500),
          },
          // a completed etherfi child (shouldn't clutter the running card, only
          // contributes to "done" count in the chips)
          {
            job_id: "j4",
            company: "etherfi",
            address: "0x2222222222222222222222222222222222222222",
            name: "LiquidityPool",
            status: "completed",
            stage: "done",
            detail: "Analysis complete",
            request: {},
            created_at: iso(5 * 60_000),
            updated_at: iso(3 * 60_000),
          },
          // A recently completed thing, no company — shows in the tape only
          {
            job_id: "j5",
            company: null,
            address: "0x3333333333333333333333333333333333333333",
            name: "stETH",
            status: "completed",
            stage: "done",
            detail: "Analysis complete",
            request: {},
            created_at: iso(10 * 60_000),
            updated_at: iso(8 * 60_000),
          },
        ]),
      }),
    );

    await page.route(/\/api\/stats/, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ unique_addresses: 3, total_jobs: 5, completed_jobs: 2, failed_jobs: 0 }),
      }),
    );

    await page.route(/\/api\/audits\/pipeline/, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          text_extraction: {
            processing: [
              {
                audit_id: "a1",
                auditor: "Certora",
                title: "eEtherFi vault review",
                company: "etherfi",
                elapsed_seconds: 12,
                worker_id: "w-text-1",
                error: null,
              },
            ],
            pending: [
              {
                audit_id: "a2",
                auditor: "Spearbit",
                title: "LiquidityPool audit",
                company: "etherfi",
                elapsed_seconds: null,
                worker_id: null,
                error: null,
              },
            ],
            failed: [],
          },
          scope_extraction: {
            processing: [],
            pending: [],
            failed: [],
          },
        }),
      }),
    );
  });

  test("pipeline page groups by protocol and folds audits into one card", async ({ page }) => {
    const errors = [];
    page.on("pageerror", (e) => errors.push(`pageerror: ${e.message}`));
    page.on("console", (m) => {
      if (m.type() !== "error") return;
      errors.push(`console.error: ${m.text()}`);
    });

    await page.goto("/monitor");

    // Pipeline status + running protocols header
    await expect(page.getByText("Pipeline Status")).toBeVisible();
    await expect(page.getByText("Running Protocols")).toBeVisible();

    // One card for etherfi (has both job + audit activity), one for lido (queued job)
    const cards = page.locator(".protocol-card");
    await expect(cards).toHaveCount(2);

    // etherfi card shows its audit sidecar alongside the pipeline lane
    const etherfiCard = page.locator(".protocol-card", { hasText: "etherfi" });
    await expect(etherfiCard.locator(".protocol-lane")).toHaveCount(2); // pipeline lane + audit lane
    await expect(etherfiCard.locator(".protocol-lane-audit")).toBeVisible();

    // The standalone "Audit Extraction" section is gone — audit data lives
    // inside the protocol card now.
    await expect(page.getByText("Audit Extraction")).toHaveCount(0);

    // Recently-completed tape present; j4 (etherfi completed 3min ago) should
    // appear since it's within the last hour. j5 (stETH, 8min ago) too.
    const tape = page.locator(".completion-tape .completion-row");
    await expect(tape.filter({ hasText: "LiquidityPool" })).toHaveCount(1);
    await expect(tape.filter({ hasText: "stETH" })).toHaveCount(1);

    expect(errors, errors.join("\n")).toEqual([]);
  });
});
