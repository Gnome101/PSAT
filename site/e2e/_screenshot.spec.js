import { test } from "@playwright/test";

test("capture company/etherfi", async ({ page }) => {
  const errors = [];
  page.on("pageerror", (e) => errors.push(`pageerror: ${e.message}\n${e.stack || ""}`));
  page.on("console", (m) => {
    if (m.type() === "error") errors.push(`console.error: ${m.text()}`);
  });

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("http://127.0.0.1:5173/company/etherfi", { waitUntil: "networkidle" });
  await page.waitForTimeout(1500);

  // Above the fold
  await page.screenshot({ path: "e2e-screens/etherfi-viewport.png", fullPage: false });

  // Full page (will be tall but clipped)
  await page.screenshot({ path: "e2e-screens/etherfi-full.png", fullPage: true, clip: undefined });

  // Element-level grabs for the key sections
  const hero = page.locator(".company-hero").first();
  if (await hero.count()) {
    await hero.screenshot({ path: "e2e-screens/etherfi-hero.png" });
  }
  const surfacePreview = page.locator(".surface-preview").first();
  if (await surfacePreview.count()) {
    await surfacePreview.screenshot({ path: "e2e-screens/etherfi-surface-preview.png" });
  }
  const surfaceEmbed = page.locator(".company-surface-embed").first();
  if (await surfaceEmbed.count()) {
    // ReactFlow sometimes needs a few hundred ms to finish its first fit-to-view
    await page.waitForTimeout(400);
    await surfaceEmbed.scrollIntoViewIfNeeded();
    await surfaceEmbed.screenshot({ path: "e2e-screens/etherfi-surface-embed.png" });

    // Flip to the Audits tab and capture
    const auditsTab = surfaceEmbed.locator(".ps-sidebar-tab", { hasText: "Audits" });
    if (await auditsTab.count()) {
      await auditsTab.click();
      await page.waitForTimeout(400);
      await surfaceEmbed.screenshot({ path: "e2e-screens/etherfi-surface-audits.png" });

      // Click the first audit to expose the covered-contracts panel
      const firstAudit = surfaceEmbed.locator(".ps-audits-row-main").first();
      if (await firstAudit.count()) {
        await firstAudit.click();
        await page.waitForTimeout(300);
        await surfaceEmbed.screenshot({ path: "e2e-screens/etherfi-surface-audits-active.png" });

        // Open the Read modal
        const readBtn = surfaceEmbed.locator(".ps-audits-row-read").first();
        if (await readBtn.count()) {
          await readBtn.click();
          // Wait for the aside list to populate AND the text index to compute.
          // Once the "Indexing references…" hint disappears (or times out after
          // ~5s) the page badges will be visible.
          await page
            .locator(".ps-audit-modal-aside-list")
            .first()
            .waitFor({ state: "visible", timeout: 4000 })
            .catch(() => {});
          await page.waitForTimeout(2600);
          await page.screenshot({ path: "e2e-screens/etherfi-audit-modal.png", fullPage: false });

          // Try clicking the first "clickable" covered contract (one with a
          // page match) and capture the page-jump state.
          const jumpRow = page.locator(".ps-audit-modal-aside-row.clickable").first();
          if (await jumpRow.count()) {
            await jumpRow.click();
            await page.waitForTimeout(900);
            await page.screenshot({ path: "e2e-screens/etherfi-audit-modal-jump.png", fullPage: false });
          }
          // close modal via escape so subsequent tests aren't blocked
          await page.keyboard.press("Escape");
        }
      }
    }
  }

  // --- Addresses modal (Stage C) ------------------------------------------
  await page.keyboard.press("Escape").catch(() => {});
  await page.waitForTimeout(200);
  await page.goto("http://127.0.0.1:5173/company/etherfi", { waitUntil: "networkidle" });
  await page.waitForTimeout(600);
  const addressesBtn = page
    .locator(".company-surface-action", { hasText: "Addresses" })
    .first();
  if (await addressesBtn.count()) {
    await addressesBtn.scrollIntoViewIfNeeded();
    await addressesBtn.click();
    await page.waitForTimeout(700);
    await page.screenshot({ path: "e2e-screens/etherfi-addresses-modal.png", fullPage: false });

    // Exercise compare mode — paste one address that exists and one that
    // doesn't, confirm the matched/missing badges render.
    const compareToggle = page
      .locator(".ps-audit-modal-btn", { hasText: "Compare" })
      .first();
    if (await compareToggle.count()) {
      await compareToggle.click();
      await page.waitForTimeout(250);
      await page
        .locator(".ps-addresses-modal-compare-input")
        .fill(
          "0x7d5706f6ef3f89b3951e23e557cdfbc3239d4e2c\n0x0000000000000000000000000000000000000001",
        );
      await page.waitForTimeout(400);
      await page.screenshot({
        path: "e2e-screens/etherfi-addresses-compare.png",
        fullPage: false,
      });
    }
    await page.keyboard.press("Escape");
    await page.waitForTimeout(250);
  }

  // --- Audits admin modal ---------------------------------------------------
  const auditsBtn = page
    .locator(".company-surface-action", { hasText: "Audits" })
    .first();
  if (await auditsBtn.count()) {
    await auditsBtn.scrollIntoViewIfNeeded();
    await auditsBtn.click();
    await page.waitForTimeout(800);
    await page.screenshot({ path: "e2e-screens/etherfi-audits-admin-modal.png", fullPage: false });
    await page.keyboard.press("Escape");
  }

  if (errors.length) console.log("ERRORS:\n" + errors.join("\n---\n"));
});
