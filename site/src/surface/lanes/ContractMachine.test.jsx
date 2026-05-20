import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import { ContractMachine } from "./ContractMachine.jsx";

function bridgeMachine() {
  return {
    address: "0x1111111111111111111111111111111111111111",
    name: "LayerZeroTellerWithRateLimiting",
    chain: "ethereum",
    role: "bridge",
    totalFunctions: 0,
    lanes: { top: [], ops: [], left: [], right: [] },
    balances: [],
    bridge_summary: {
      protocol: "LayerZero",
      status: "2 routes",
      route_count: 2,
      routes: [
        {
          chain: "base",
          peer_address: "0x2222222222222222222222222222222222222222",
          peer_status: "missing_rpc",
          security: "2 required DVNs, threshold 1",
        },
        {
          chain: "arbitrum",
          peer_address: "0x3333333333333333333333333333333333333333",
          peer_status: "queued",
        },
      ],
      peers: "1 queued, 1 missing RPC",
      config_control: "Owner",
    },
  };
}

describe("ContractMachine bridge tab", () => {
  it("surfaces compact bridge route context on bridge contracts", async () => {
    const user = userEvent.setup();
    render(<ContractMachine machine={bridgeMachine()} />);

    await user.click(screen.getByRole("button", { name: /Bridges\s*2/i }));

    expect(screen.getByText("LayerZero bridge")).toBeInTheDocument();
    expect(screen.getByText("Ethereum -> Base, Arbitrum")).toBeInTheDocument();
    expect(screen.getByText("1 queued, 1 missing RPC")).toBeInTheDocument();
    expect(screen.getByText("Owner")).toBeInTheDocument();
    expect(screen.getByText("Peer: missing RPC")).toBeInTheDocument();
    expect(screen.getByText("2 required DVNs, threshold 1")).toBeInTheDocument();

    const text = document.body.textContent || "";
    expect(text).not.toContain("0x2222222222222222222222222222222222222222");
    expect(text).not.toContain("peer_address");
  });
});
