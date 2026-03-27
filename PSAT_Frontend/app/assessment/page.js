export default function AssessmentPage() {
  return (
    <main className="assessmentPage">
      <section className="assessmentCard">
        <h1>Start Assessment</h1>
        <p>Enter your assessment notes below.</p>
        <textarea
          className="assessmentInput"
          placeholder="Type your assessment details..."
          rows={10}
        />
        <div className="assessmentActions">
          <a href="/report" className="btn">
            Generate Sample Report
          </a>
          <a href="/" className="btn btnSecondary">
            Back Home
          </a>
        </div>
      </section>
    </main>
  );
}
