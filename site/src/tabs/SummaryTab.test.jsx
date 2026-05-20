import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import SummaryTab from "./SummaryTab.jsx";

describe("SummaryTab bridge card", () => {
  it("renders compact routes without raw bridge internals", () => {
    const peer = "0x3333333333333333333333333333333333333333";
    const dvn = "0x4444444444444444444444444444444444444444";

    render(
      <SummaryTab
        detail={{
          address: "0x1111111111111111111111111111111111111111",
          chain: "ethereum",
          summary: {},
          bridge_summary: {
            protocol: "LayerZero",
            status: "3 routes",
            route_count: 4,
            route_overflow: 1,
            routes: [
              {
                chain: "base",
                peer_address: peer,
                peer_status: "queued",
                security: "2 required DVNs, 1 optional, threshold 1",
                required_dvns: [dvn],
              },
            ],
            peers: "1 queued",
            config_control: "Owner",
          },
        }}
      />,
    );

    expect(screen.getByText("LayerZero bridge")).toBeInTheDocument();
    expect(screen.getByText("3 routes")).toBeInTheDocument();
    expect(screen.getByText(/Ethereum -> Base, \+1 more/)).toBeInTheDocument();
    expect(screen.getByText(/Base -> 0x3333..3333/)).toBeInTheDocument();
    expect(screen.getByText("Peer: queued")).toBeInTheDocument();
    expect(screen.getByText("Security: 2 required DVNs, 1 optional, threshold 1")).toBeInTheDocument();

    const text = document.body.textContent || "";
    expect(text).not.toContain(peer);
    expect(text).not.toContain(dvn);
    expect(text).not.toContain("required_dvns");
  });
});
