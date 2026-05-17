// Render tests for the inspector panels in
// site/src/surface/inspector/. These ship as named exports already, so
// pinning them here ensures the upcoming ProtocolSurface split doesn't
// drift their public API.

import React from "react";
import { describe, it, expect, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";

import { UpgradesPanel } from "./UpgradesPanel.jsx";
import { AgentPanel } from "./AgentPanel.jsx";
import MarkdownBubble from "./MarkdownBubble.jsx";
import { setFetchHandler } from "../../test/fetchMock.js";

describe("UpgradesPanel", () => {
  beforeEach(() => {
    setFetchHandler(
      (url) => /^\/api\/contracts\/.+\/audit_timeline$/.test(url.pathname),
      () => ({ current_status: "unknown", coverage: [] }),
    );
  });

  it("renders the loading copy when loading=true", () => {
    render(<UpgradesPanel loading upgradeHistory={null} />);
    expect(screen.getByText(/Loading upgrade history/i)).toBeInTheDocument();
  });

  it("renders the empty state when no proxies are present", () => {
    render(<UpgradesPanel upgradeHistory={{ proxies: {} }} />);
    expect(screen.getByText(/No upgrades on record/i)).toBeInTheDocument();
  });

  it("renders proxy history when present", async () => {
    const history = {
      target_address: "0xproxy",
      total_upgrades: 1,
      proxies: {
        "0xproxy": {
          proxy_type: "UUPS",
          current_implementation: "0x2",
          upgrade_count: 1,
          first_upgrade_block: 10,
          last_upgrade_block: 20,
          implementations: [
            {
              address: "0x1",
              block_introduced: 5,
              block_replaced: 10,
              timestamp_introduced: 1700000000,
              timestamp_replaced: 1710000000,
              contract_name: "Impl V1",
            },
            {
              address: "0x2",
              block_introduced: 10,
              timestamp_introduced: 1710000000,
              contract_name: "Impl V2",
            },
          ],
          events: [],
        },
      },
    };
    render(
      <UpgradesPanel
        upgradeHistory={history}
        contractId={42}
        contractAddress="0xproxy"
        contractName="Proxy"
      />,
    );
    await waitFor(() => {
      expect(document.body.textContent).toContain("UUPS");
    });
  });
});

describe("AgentPanel", () => {
  it("renders without selecting a machine", () => {
    render(
      <AgentPanel
        companyName="etherfi"
        selectedMachine={null}
        onHighlight={() => {}}
        onFocusAddress={() => {}}
      />,
    );
    // Suggestion list is rendered on first mount before any messages.
    expect(screen.getByText(/Who controls upgrades\?/i)).toBeInTheDocument();
  });

  it("surfaces selected bridge context in the agent header", () => {
    render(
      <AgentPanel
        companyName="bridge-protocol"
        selectedMachine={{
          name: "LayerZeroBridge",
          address: "0x1111111111111111111111111111111111111111",
          bridge_context: {
            status: "resolved",
            protocols: ["LayerZero"],
            routes: [{ eid: 30184, chain: "base", peer: "0x2222222222222222222222222222222222222222" }],
          },
        }}
        onHighlight={() => {}}
        onFocusAddress={() => {}}
      />,
    );
    expect(screen.getByText("LayerZero · 1 active routes")).toBeInTheDocument();
  });
});

describe("MarkdownBubble", () => {
  it("renders markdown text", async () => {
    render(<MarkdownBubble text="**bold** and _italic_" components={{}} />);
    await waitFor(() => {
      expect(screen.getByText("bold")).toBeInTheDocument();
      expect(screen.getByText("italic")).toBeInTheDocument();
    });
  });
});
