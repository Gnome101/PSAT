export function StatCard({ label, value }) {
  return (
    <div className="stat-card">
      <div className="eyebrow">{label}</div>
      <div className="stat">{value}</div>
    </div>
  );
}

export default StatCard;
