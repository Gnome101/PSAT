import { GuardButton } from "../../ui/GuardButton.jsx";

export function FunctionPort({ fnView, onSelect, onNavigate, orientation, highlighted }) {
  return (
    <div className={`ps-port ps-port-${orientation}${highlighted ? " ps-port-score-highlight" : ""}`} style={{ "--port-accent": fnView.tone }}>
      <div className="ps-port-copy" onClick={() => onSelect(fnView)} style={{ cursor: "pointer" }}>
        <div className="ps-port-name">{fnView.name}</div>
        {fnView.action && <div className="ps-port-action">{fnView.action}</div>}
      </div>
      <GuardButton fnView={fnView} onSelect={onSelect} onNavigate={onNavigate} />
    </div>
  );
}
