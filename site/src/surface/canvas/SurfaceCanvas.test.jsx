import { describe, expect, it } from "vitest";

import { buildNetworkZones } from "./SurfaceCanvas.jsx";
import { separateNetworkLanes } from "../layout/elkLayout.js";

describe("buildNetworkZones", () => {
  it("groups contract nodes by chain with compact labels", () => {
    const zones = buildNetworkZones([
      {
        id: "0x1111111111111111111111111111111111111111",
        type: "contract",
        position: { x: 100, y: 80 },
        data: { machine: { chain: "ethereum" } },
      },
      {
        id: "0x2222222222222222222222222222222222222222",
        type: "contract",
        position: { x: 400, y: 120 },
        data: { machine: { chain: "base" } },
      },
      {
        id: "0x3333333333333333333333333333333333333333",
        type: "contract",
        parentId: "safe",
        position: { x: 40, y: 50 },
        data: { machine: { chain: "base" } },
      },
      {
        id: "safe",
        type: "group",
        position: { x: 500, y: 200 },
        data: { principal: { address: "safe" } },
      },
    ]);

    expect(zones).toHaveLength(2);
    expect(zones.map((zone) => zone.data.label).sort()).toEqual(["Base · 2", "Ethereum · 1"]);
    expect(zones.every((zone) => zone.type === "networkZone")).toBe(true);
  });
});

describe("separateNetworkLanes", () => {
  it("moves chain zones into separate horizontal lanes", () => {
    const separated = separateNetworkLanes([
      {
        id: "0x1111111111111111111111111111111111111111",
        type: "contract",
        position: { x: 100, y: 80 },
        data: { machine: { chain: "ethereum" } },
      },
      {
        id: "0x2222222222222222222222222222222222222222",
        type: "contract",
        position: { x: 120, y: 90 },
        data: { machine: { chain: "base" } },
      },
    ]);

    const zones = buildNetworkZones(separated);
    const ethereum = zones.find((zone) => zone.data.label.startsWith("Ethereum"));
    const base = zones.find((zone) => zone.data.label.startsWith("Base"));

    expect(base.position.x).toBeGreaterThan(ethereum.position.x + ethereum.style.width);
  });

  it("widens mixed-chain owner groups when child lanes spread out", () => {
    const separated = separateNetworkLanes([
      {
        id: "safe",
        type: "group",
        position: { x: 0, y: 0 },
        style: { width: 400, height: 240 },
        data: { principal: { address: "safe" } },
      },
      {
        id: "0x1111111111111111111111111111111111111111",
        type: "contract",
        parentId: "safe",
        position: { x: 24, y: 74 },
        data: { machine: { chain: "ethereum" } },
      },
      {
        id: "0x2222222222222222222222222222222222222222",
        type: "contract",
        parentId: "safe",
        position: { x: 24, y: 74 },
        data: { machine: { chain: "base" } },
      },
    ]);

    const group = separated.find((node) => node.id === "safe");
    const eth = separated.find((node) => node.id.startsWith("0x1111"));
    const base = separated.find((node) => node.id.startsWith("0x2222"));

    expect(base.position.x).toBeGreaterThan(eth.position.x + 260);
    expect(group.style.width).toBeGreaterThan(400);
  });

  it("keeps duplicate addresses chain-scoped during lane separation", () => {
    const duplicate = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
    const separated = separateNetworkLanes([
      {
        id: duplicate,
        type: "contract",
        position: { x: 100, y: 80 },
        data: { machine: { chain: "ethereum", name: "WETH9" } },
      },
      {
        id: duplicate,
        type: "contract",
        position: { x: 110, y: 220 },
        data: { machine: { chain: "base", name: "WETH9" } },
      },
    ]);

    const ethereum = separated[0];
    const base = separated[1];

    expect(base.position.x).toBeGreaterThan(ethereum.position.x + 260);
  });
});
