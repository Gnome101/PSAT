export function Badge({ accent, children, className = "", style, title }) {
  const merged = accent ? { ...style, "--badge-accent": accent } : style;
  return (
    <span
      className={className ? `ps-badge ${className}` : "ps-badge"}
      style={merged}
      title={title}
    >
      {children}
    </span>
  );
}

export default Badge;
