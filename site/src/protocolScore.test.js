import { describe, expect, it } from "vitest";

import { computeProtocolScore } from "./protocolScore.js";

const SAFE_4_OF_7 = {
  address: "0xsafe",
  resolved_type: "safe",
  details: {
    threshold: 4,
    owners: ["0x1", "0x2", "0x3", "0x4", "0x5", "0x6", "0x7"],
  },
};

const EOA = {
  address: "0xeoa",
  resolved_type: "eoa",
  details: {},
};

function contractWithUnpause(principal) {
  return {
    address: "0xcontract",
    name: "Vault",
    source_verified: true,
    is_proxy: false,
    role: "value_handler",
    total_usd: 100_000_000,
    risk_level: "high",
    functions: [
      {
        function: "pause()",
        effect_labels: ["pause_toggle"],
        controllers: [{ principals: [EOA] }],
      },
      {
        function: "unpause()",
        effect_labels: ["pause_toggle"],
        controllers: [{ principals: [principal] }],
      },
    ],
  };
}

function axis(score, key) {
  return score.axes.find((entry) => entry.key === key)?.value;
}

describe("computeProtocolScore", () => {
  it("scores pause authority differently from unpause authority", () => {
    const safeUnpause = computeProtocolScore({ contracts: [contractWithUnpause(SAFE_4_OF_7)] }, null);
    const eoaUnpause = computeProtocolScore({ contracts: [contractWithUnpause(EOA)] }, null);

    expect(axis(safeUnpause, "pause")).toBeGreaterThan(axis(eoaUnpause, "pause"));
  });

  it("does not use Slither risk_level as a scoring axis", () => {
    const highRisk = computeProtocolScore({ contracts: [contractWithUnpause(SAFE_4_OF_7)] }, null);
    const lowRiskContract = { ...contractWithUnpause(SAFE_4_OF_7), risk_level: "low" };
    const lowRisk = computeProtocolScore({ contracts: [lowRiskContract] }, null);

    expect(highRisk.composite).toBe(lowRisk.composite);
    expect(highRisk.axes.some((entry) => entry.key === "risk")).toBe(false);
  });

  it("only credits bytecode-verified audit coverage", () => {
    const contract = { ...contractWithUnpause(SAFE_4_OF_7), address: "0xabc" };
    const heuristicCoverage = {
      coverage: [
        {
          address: "0xabc",
          audit_count: 1,
          audits: [
            {
              audit_id: 1,
              match_type: "impl_era",
              match_confidence: "high",
              equivalence_status: "no_source_repo",
            },
          ],
        },
      ],
    };
    const verifiedCoverage = {
      coverage: [
        {
          address: "0xabc",
          audit_count: 1,
          audits: [
            {
              audit_id: 1,
              match_type: "reviewed_commit",
              match_confidence: "high",
              equivalence_status: "proven",
              proof_kind: "clean",
            },
          ],
        },
      ],
    };

    expect(axis(computeProtocolScore([contract], [], heuristicCoverage), "audits")).toBe(0);
    expect(axis(computeProtocolScore([contract], [], verifiedCoverage), "audits")).toBe(1);
  });

  it("adds detail copy with positive notes and concrete negative examples", () => {
    const score = computeProtocolScore({ contracts: [contractWithUnpause(EOA)] }, null);
    const pauseAxis = score.axes.find((entry) => entry.key === "pause");

    expect(pauseAxis.tooltip.description).toContain("emergency stops");
    expect(pauseAxis.tooltip.positive).toContain("pause");
    expect(pauseAxis.tooltip.negative).toContain("unpause");
    expect(pauseAxis.tooltip.negativeExamples).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          title: expect.stringContaining("unpause"),
          detail: expect.stringContaining("EOA-controlled"),
          contractAddress: "0xcontract",
          functionSignature: "unpause()",
        }),
      ]),
    );
  });
});
