export default function LoadingFallback({ label = "Loading..." }) {
  return (
    <div className="page">
      <section className="panel">
        <p className="empty" style={{ textAlign: "center", padding: "2rem 0", color: "#64748b" }}>
          {label}
        </p>
      </section>
    </div>
  );
}
