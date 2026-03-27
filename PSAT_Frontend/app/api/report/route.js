import { NextResponse } from "next/server";

const statuses = ["Passed", "Needs Review", "Failed"];
const severities = ["Low", "Medium", "High", "Critical"];
const findings = [
  "Weak TLS cipher suites detected",
  "Outdated SSH configuration",
  "Missing SPF email record",
  "Open management port discovered",
  "Certificate expires soon",
  "Default credentials still enabled",
  "Unrestricted CORS policy found"
];
const owners = ["Security Team", "Platform Ops", "Compliance", "Infrastructure"];

function pickRandom(items) {
  return items[Math.floor(Math.random() * items.length)];
}

function createFinding(index) {
  return {
    id: `F-${String(index + 1).padStart(3, "0")}`,
    title: pickRandom(findings),
    severity: pickRandom(severities),
    status: pickRandom(statuses),
    owner: pickRandom(owners)
  };
}

export async function GET() {
  const score = Math.floor(Math.random() * 31) + 70;
  const totalFindings = Math.floor(Math.random() * 4) + 3;
  const data = {
    reportId: `RPT-${Date.now()}`,
    generatedAt: new Date().toISOString(),
    client: "Acme Industries",
    assessmentType: "Protocol Security Review",
    overallScore: score,
    riskLevel: score >= 90 ? "Low" : score >= 80 ? "Moderate" : "High",
    findings: Array.from({ length: totalFindings }, (_, index) =>
      createFinding(index)
    )
  };

  return NextResponse.json(data);
}
