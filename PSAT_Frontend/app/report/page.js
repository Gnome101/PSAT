"use client";

import { useEffect, useState } from "react";

function DetailItem({ label, value }) {
  return (
    <div className="reportDetailItem">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

export default function ReportPage() {
  const [report, setReport] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function loadReport() {
    try {
      setLoading(true);
      setError("");

      const response = await fetch("/api/report", { cache: "no-store" });

      if (!response.ok) {
        throw new Error("Unable to load report data.");
      }

      const data = await response.json();
      setReport(data);
    } catch (err) {
      setError(err.message || "Something went wrong while loading the report.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadReport();
  }, []);

  return (
    <main className="reportPage">
      <section className="reportCard">
        <div className="reportHeader">
          <div>
            <p className="eyebrow">Generated Report</p>
            <h1>Assessment Report</h1>
            <p className="reportIntro">
              Review a randomized sample report generated from the test API.
            </p>
          </div>
          <button type="button" className="btn" onClick={loadReport}>
            Refresh Data
          </button>
        </div>

        {loading ? <p className="reportMessage">Loading report data...</p> : null}
        {error ? <p className="reportError">{error}</p> : null}

        {!loading && !error && report ? (
          <>
            <div className="reportDetails">
              <DetailItem label="Report ID" value={report.reportId} />
              <DetailItem label="Client" value={report.client} />
              <DetailItem
                label="Generated"
                value={new Date(report.generatedAt).toLocaleString()}
              />
              <DetailItem label="Assessment" value={report.assessmentType} />
            </div>

            <div className="reportSummary">
              <article className="summaryCard">
                <span>Overall Score</span>
                <strong>{report.overallScore}/100</strong>
              </article>
              <article className="summaryCard">
                <span>Risk Level</span>
                <strong>{report.riskLevel}</strong>
              </article>
              <article className="summaryCard">
                <span>Total Findings</span>
                <strong>{report.findings.length}</strong>
              </article>
            </div>

            <div className="reportTableWrap">
              <table className="reportTable">
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Finding</th>
                    <th>Severity</th>
                    <th>Status</th>
                    <th>Owner</th>
                  </tr>
                </thead>
                <tbody>
                  {report.findings.map((finding) => (
                    <tr key={finding.id}>
                      <td>{finding.id}</td>
                      <td>{finding.title}</td>
                      <td>{finding.severity}</td>
                      <td>{finding.status}</td>
                      <td>{finding.owner}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        ) : null}

        <div className="reportActions">
          <a href="/" className="btn btnSecondary">
            Back Home
          </a>
          <a href="/assessment" className="btn btnSecondary">
            Open Assessment
          </a>
        </div>
      </section>
    </main>
  );
}
