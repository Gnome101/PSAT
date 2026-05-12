import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { GuardButton } from "./GuardButton.jsx";

describe("GuardButton", () => {
  it("shows concrete untyped direct principals instead of icon-only unresolved state", () => {
    const html = renderToStaticMarkup(
      <GuardButton
        fnView={{
          name: "transferOwnership",
          guard: {
            kind: "address",
            label: "ADDR",
            sublabel: "0x9f26..20761",
            accent: "#94a3b8",
            principals: [
              {
                address: "0x9f26d4c958fd811a1f59b01b86be7dffc9d20761",
                resolvedType: "unknown",
                details: {},
              },
            ],
          },
        }}
        onSelect={vi.fn()}
        onNavigate={vi.fn()}
      />,
    );

    expect(html).toContain("ADDR");
    expect(html).toContain("0x9f26..20761");
    expect(html).not.toContain("ps-guard-icon-only");
  });

  it("shows exact empty caller sets as no active principal", () => {
    const html = renderToStaticMarkup(
      <GuardButton
        fnView={{
          name: "recover",
          guard: {
            kind: "resolved_empty",
            label: "NONE",
            sublabel: "no active principal",
            accent: "#64748b",
            principals: [],
          },
        }}
        onSelect={vi.fn()}
        onNavigate={vi.fn()}
      />,
    );

    expect(html).toContain("NONE");
    expect(html).toContain("no active principal");
    expect(html).not.toContain("ps-guard-icon-only");
  });
});
