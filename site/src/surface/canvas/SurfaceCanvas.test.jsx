import { describe, expect, it } from "vitest";

import { buildNetworkZones } from "./SurfaceCanvas.jsx";

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
