// @ts-check
import { test, expect } from "@playwright/test";

/**
 * Minimal API fixture for /api/company/testco.
 *
 * Contains one contract ("Vault") with four functions, each guarded by
 * a different principal configuration:
 *   - setSafe:        single safe principal
 *   - setTimelock:    single timelock principal
 *   - setOwner:       single contract (proxy admin) principal
 *   - setMixed:       two principals (safe + timelock) → "2P mixed"
 */
const SAFE_ADDR = "0xaaaa000000000000000000000000000000000001";
const TL_ADDR = "0xbbbb000000000000000000000000000000000002";
const CON_ADDR = "0xcccc000000000000000000000000000000000003";
const VAULT_ADDR = "0xdddd000000000000000000000000000000000004";

const COMPANY_FIXTURE = {
  contracts: [
    {
      address: VAULT_ADDR,
      name: "Vault",
      display_name: "Vault",
      role: "governance",
      functions: [
        {
          function: "setSafe()",
          selector: "0x11111111",
          effect_labels: ["ownership_transfer"],
          direct_owner: {
            address: SAFE_ADDR,
            resolved_type: "safe",
            details: { threshold: 2, owners: ["0x0001", "0x0002", "0x0003"] },
          },
          authority_roles: [],
          controllers: [],
        },
        {
          function: "setTimelock()",
          selector: "0x22222222",
          effect_labels: ["pause_toggle"],
          direct_owner: {
            address: TL_ADDR,
            resolved_type: "timelock",
            details: { delay: 86400 },
          },
          authority_roles: [],
          controllers: [],
        },
        {
          function: "setOwner()",
          selector: "0x33333333",
          effect_labels: ["ownership_transfer"],
          direct_owner: {
            address: CON_ADDR,
            resolved_type: "contract",
            details: {},
          },
          authority_roles: [],
          controllers: [],
        },
        {
          function: "setMixed()",
          selector: "0x44444444",
          effect_labels: ["authority_update"],
          direct_owner: {
            address: SAFE_ADDR,
            resolved_type: "safe",
            details: { threshold: 2, owners: ["0x0001", "0x0002", "0x0003"] },
          },
          authority_roles: [],
          controllers: [
            {
              controller_id: "extra",
              label: "secondary",
              principals: [
                {
                  address: TL_ADDR,
                  resolved_type: "timelock",
                  details: { delay: 86400 },
                },
              ],
            },
          ],
        },
      ],
    },
  ],
  principals: [
    {
      address: SAFE_ADDR,
      type: "safe",
      label: "TestSafe",
      details: { threshold: 2, owners: ["0x0001", "0x0002", "0x0003"] },
      controls: [VAULT_ADDR],
    },
    {
      address: TL_ADDR,
      type: "timelock",
      label: "TestTimelock",
      details: { delay: 86400 },
      controls: [VAULT_ADDR],
    },
  ],
  fund_flows: [],
};

/** Intercept the company API and return our fixture. */
async function mockApi(page) {
  await page.route("**/api/company/testco", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(COMPANY_FIXTURE),
    })
  );
}

/** Navigate to the company surface page. */
async function goToSurface(page) {
  await mockApi(page);
  await page.goto("/company/testco");
  // Wait for surface header to render
  await page.waitForSelector(".ps-surface", { timeout: 10000 });
  // Wait for ReactFlow canvas + ELK layout to produce nodes
  await page.waitForSelector(".react-flow__node", { timeout: 15000 });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test.describe("Guard badge navigation", () => {
  test("clicking SAFE badge navigates to safe principal detail", async ({ page }) => {
    await goToSurface(page);

    // Click the Vault contract node to select it
    await page.locator(".ps-node").first().click();
    await page.waitForSelector(".ps-machine", { timeout: 5000 });

    // Find the guard button for setSafe — it should show "SAFE"
    const safeGuard = page.locator(".ps-guard-button", { hasText: "SAFE" }).first();
    await expect(safeGuard).toBeVisible();

    // Click it
    await safeGuard.click();

    // Should navigate to principal detail — sidebar shows safe info
    const principalHeader = page.locator(".ps-machine-name");
    await expect(principalHeader).toBeVisible({ timeout: 5000 });

    // URL should have focus param
    await expect(page).toHaveURL(/focus=/);

    // Should show the principal's address in the sidebar
    const addrText = page.locator(".ps-machine-address");
    await expect(addrText).toContainText(SAFE_ADDR.slice(0, 6));
  });

  test("clicking TL badge navigates to timelock principal detail", async ({ page }) => {
    await goToSurface(page);

    await page.locator(".ps-node").first().click();
    await page.waitForSelector(".ps-machine", { timeout: 5000 });

    const tlGuard = page.locator(".ps-guard-button", { hasText: "TL" }).first();
    await expect(tlGuard).toBeVisible();
    await tlGuard.click();

    // Should show timelock detail
    const principalHeader = page.locator(".ps-machine-name");
    await expect(principalHeader).toBeVisible({ timeout: 5000 });

    const addrText = page.locator(".ps-machine-address");
    await expect(addrText).toContainText(TL_ADDR.slice(0, 6));
  });

  test("clicking CON badge navigates to contract", async ({ page }) => {
    await goToSurface(page);

    await page.locator(".ps-node").first().click();
    await page.waitForSelector(".ps-machine", { timeout: 5000 });

    const conGuard = page.locator(".ps-guard-button", { hasText: "CON" }).first();
    await expect(conGuard).toBeVisible();
    await conGuard.click();

    // URL should update with focus param pointing to the contract principal
    await expect(page).toHaveURL(/focus=/);
  });

  test("clicking 2P badge navigates to first principal and shows tour nav", async ({ page }) => {
    await goToSurface(page);

    await page.locator(".ps-node").first().click();
    await page.waitForSelector(".ps-machine", { timeout: 5000 });

    const mixedGuard = page.locator(".ps-guard-button", { hasText: "2P" }).first();
    await expect(mixedGuard).toBeVisible();
    await mixedGuard.click();

    // Should show tour navigator on the canvas
    const tourNav = page.locator(".ps-tour-nav");
    await expect(tourNav).toBeVisible({ timeout: 5000 });

    // Should show counter "1 / 2"
    const counter = page.locator(".ps-tour-counter");
    await expect(counter).toContainText("1 / 2");

    // Click next arrow to go to second principal
    await page.locator(".ps-tour-controls button", { hasText: "▶" }).click();
    await expect(counter).toContainText("2 / 2");

    // Click back button to return to source contract
    await page.locator(".ps-tour-back").click();
    await expect(tourNav).not.toBeVisible();
  });

  test("clicking function name opens guard inspector (not navigation)", async ({ page }) => {
    await goToSurface(page);

    await page.locator(".ps-node").first().click();
    await page.waitForSelector(".ps-machine", { timeout: 5000 });

    // Click the function name text, not the guard badge
    const fnName = page.locator(".ps-port-name", { hasText: "setSafe" }).first();
    await expect(fnName).toBeVisible();
    await fnName.click();

    // Should show guard inspector
    const inspector = page.locator(".ps-inspector");
    await expect(inspector).toBeVisible({ timeout: 5000 });
    await expect(inspector.locator("h3")).toContainText("setSafe");
  });
});

test.describe("URL focus parameter", () => {
  test("focus param in URL highlights node with gold border on load", async ({ page }) => {
    await mockApi(page);
    await page.goto(`/company/testco?focus=${VAULT_ADDR}`);
    await page.waitForSelector(".ps-node", { timeout: 10000 });

    // Wait for focus to apply
    await page.waitForTimeout(500);

    // The focused node should have the gold dotted border class
    const focusedNode = page.locator(".ps-node-focused");
    await expect(focusedNode).toBeVisible({ timeout: 5000 });
  });

  test("navigating updates focus param to new target", async ({ page }) => {
    await goToSurface(page);

    // Click a node to select it
    await page.locator(".ps-node").first().click();
    await page.waitForSelector(".ps-machine", { timeout: 5000 });

    // URL should have focus on the vault contract
    await expect(page).toHaveURL(new RegExp(`focus=${VAULT_ADDR}`));

    // Click safe guard badge to navigate to the safe principal
    const safeGuard = page.locator(".ps-guard-button", { hasText: "SAFE" }).first();
    await safeGuard.click();

    // URL focus should now point to the safe address, not the vault
    await expect(page).toHaveURL(new RegExp(`focus=${SAFE_ADDR}`));
  });
});

test.describe("Breadcrumbs", () => {
  test("navigating from contract to principal adds breadcrumb", async ({ page }) => {
    await goToSurface(page);

    // Select the vault
    await page.locator(".ps-node").first().click();
    await page.waitForSelector(".ps-machine", { timeout: 5000 });

    // Click safe guard to navigate
    const safeGuard = page.locator(".ps-guard-button", { hasText: "SAFE" }).first();
    await safeGuard.click();

    // Breadcrumb should appear with the previous contract
    const breadcrumb = page.locator(".ps-breadcrumb");
    await expect(breadcrumb.first()).toBeVisible({ timeout: 5000 });
  });
});
