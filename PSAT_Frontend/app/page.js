export default function HomePage() {
  return (
    <main className="page">
      <header className="header">
        <div className="logo">PSAT</div>
        <nav className="nav" aria-label="Primary">
          <span>Home</span>
          <span>Features</span>
          <span>Contact</span>
        </nav>
      </header>

      <section className="hero">
        <h1>Protocal Security Assessment Tool</h1>
        <p>
          A simple platform to evaluate, report, and improve your
          organization&apos;s protocol security posture.
        </p>
        <div className="heroActions">
          <a href="/assessment" className="btn">
            Start Assessment
          </a>
          <a href="/report" className="btn btnSecondary">
            View Report
          </a>
          <a
            href="/download_test.csv"
            download="download_test.csv"
            className="btn btnSecondary"
          >
            Download Test
          </a>
        </div>
      </section>

      <section className="features">
        <h2>Core Features</h2>
        <div className="cards">
          <article className="card">
            <h3>Automated Checks</h3>
            <p>
              Run baseline protocol scans and quickly detect common weaknesses.
            </p>
          </article>
          <article className="card">
            <h3>Risk Scoring</h3>
            <p>
              Prioritize findings with clear severity ratings and remediation
              guidance.
            </p>
          </article>
          <article className="card">
            <h3>Exportable Reports</h3>
            <p>
              Generate clean reports for stakeholders and compliance
              documentation.
            </p>
          </article>
        </div>
      </section>

      <footer className="footer">
        &copy; 2026 Protocal Security Assessment Tool
      </footer>
    </main>
  );
}
