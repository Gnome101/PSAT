import { useState } from "react";

import { formatJson } from "../router.js";

export default function RawTab({ detail }) {
  const [selection, setSelection] = useState("contract_analysis");
  const available = {
    contract_analysis: detail?.contract_analysis,
    control_snapshot: detail?.control_snapshot,
    dependencies: detail?.dependencies,
    dependency_graph_viz: detail?.dependency_graph_viz,
    effective_permissions: detail?.effective_permissions,
    principal_labels: detail?.principal_labels,
    resolved_control_graph: detail?.resolved_control_graph,
    upgrade_history: detail?.upgrade_history,
  };

  return (
    <div className="stack">
      <select className="select" value={selection} onChange={(event) => setSelection(event.target.value)}>
        {Object.keys(available).map((key) => (
          <option key={key} value={key}>
            {key}
          </option>
        ))}
      </select>
      <pre className="pre-wrap code-block">{formatJson(available[selection] || {})}</pre>
    </div>
  );
}
