import hourglassIcon from "../assets/hourglass-empty.svg";
import questionMarkIcon from "../assets/question-mark.svg";
import vaultIcon from "../assets/vault.svg";

export function GuardGlyph({ kind, accent, title }) {
  const common = {
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    fill: "none",
    xmlns: "http://www.w3.org/2000/svg",
    "aria-hidden": "true",
  };

  if (kind === "unknown") {
    return (
      <span
        className="ps-guard-svg-mask"
        style={{ "--guard-icon-accent": accent, maskImage: `url(${questionMarkIcon})` }}
        title={title}
      />
    );
  }

  if (kind === "safe") {
    return (
      <span
        className="ps-guard-svg-mask"
        style={{ "--guard-icon-accent": accent, maskImage: `url(${vaultIcon})` }}
        title={title}
      />
    );
  }

  if (kind === "timelock") {
    return (
      <span
        className="ps-guard-svg-mask"
        style={{ "--guard-icon-accent": accent, maskImage: `url(${hourglassIcon})` }}
        title={title}
      />
    );
  }

  if (kind === "eoa") {
    return (
      <svg {...common}>
        <circle cx="8" cy="5.3" r="2.2" stroke={accent} strokeWidth="1.4" fill={`${accent}18`} />
        <path d="M4.2 12.4C4.8 10.5 6.2 9.5 8 9.5C9.8 9.5 11.2 10.5 11.8 12.4" stroke={accent} strokeWidth="1.4" strokeLinecap="round" />
      </svg>
    );
  }

  if (kind === "contract" || kind === "proxy_admin" || kind === "access_control_hint") {
    return (
      <svg {...common}>
        <rect x="2.6" y="3" width="10.8" height="10" rx="1.8" stroke={accent} strokeWidth="1.4" fill={`${accent}16`} />
        <path d="M5.3 5.4H10.7M5.3 8H10.7M5.3 10.6H8.8" stroke={accent} strokeWidth="1.4" strokeLinecap="round" />
      </svg>
    );
  }

  if (kind === "open") {
    return (
      <svg {...common}>
        <rect x="3.2" y="7.2" width="9.6" height="5.8" rx="1.6" stroke={accent} strokeWidth="1.4" fill={`${accent}16`} />
        <path d="M5.4 7.2V5.8C5.4 4.2 6.7 3 8.2 3C9.2 3 10 3.5 10.5 4.2" stroke={accent} strokeWidth="1.4" strokeLinecap="round" />
      </svg>
    );
  }

  if (kind === "many") {
    return (
      <svg {...common}>
        <circle cx="5.7" cy="6.2" r="2" stroke={accent} strokeWidth="1.3" fill={`${accent}18`} />
        <circle cx="10.5" cy="5.6" r="1.8" stroke={accent} strokeWidth="1.3" fill={`${accent}10`} />
        <path d="M3.7 12.2C4.2 10.8 5.2 10.1 6.5 10.1C7.8 10.1 8.8 10.8 9.3 12.2" stroke={accent} strokeWidth="1.3" strokeLinecap="round" />
        <path d="M9.4 11.4C9.8 10.5 10.5 10 11.4 10" stroke={accent} strokeWidth="1.3" strokeLinecap="round" />
      </svg>
    );
  }

  return (
    <span
      className="ps-guard-svg-mask"
      style={{ "--guard-icon-accent": accent, maskImage: `url(${questionMarkIcon})` }}
      title={title}
    />
  );
}

export default GuardGlyph;
