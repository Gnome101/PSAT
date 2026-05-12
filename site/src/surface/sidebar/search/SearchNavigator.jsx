import { useEffect, useMemo, useState } from "react";

import { formatDelay, formatUsd, shortAddr } from "../../format.js";
import { buildSearchResults } from "../../layout/search.js";
import { SORT_OPTIONS } from "../../meta.js";

export function SearchNavigator({ machines, principals, onFocus, mode, setMode }) {
  const [sortKey, setSortKey] = useState("value");
  const [query, setQuery] = useState("");
  const [index, setIndex] = useState(0);
  const [hasInteracted, setHasInteracted] = useState(false);

  const results = useMemo(
    () => buildSearchResults(machines, principals, mode, sortKey, query),
    [machines, principals, mode, sortKey, query]
  );

  // Reset index when results change
  useEffect(() => { setIndex(0); }, [results.length, mode, sortKey, query]);

  // Notify parent when the user drives the navigator. The initial preview
  // should not become a selected/focused contract by itself.
  useEffect(() => {
    if (!hasInteracted) return;
    if (results.length > 0 && results[index]) {
      onFocus(results[index]);
    } else {
      onFocus(null);
    }
  }, [hasInteracted, index, results]);

  const prev = () => {
    setHasInteracted(true);
    setIndex((i) => (i > 0 ? i - 1 : results.length - 1));
  };
  const next = () => {
    setHasInteracted(true);
    setIndex((i) => (i < results.length - 1 ? i + 1 : 0));
  };

  const current = results[index];

  return (
    <div className="ps-search-nav">
      {/* Mode pills (All / Safes / EOAs / Timelocks / Has Funds) now render
          at top-left via <SearchModesBar />. The rest of the search nav
          (input, sort, arrows, preview) stays in the centre overlay. */}
      <div className="ps-search-controls">
        <input
          type="text"
          value={query}
          onChange={(e) => {
            setHasInteracted(true);
            setQuery(e.target.value);
          }}
          placeholder="Search... (e.g. 'min value 3M')"
          className="ps-search-input"
        />
        <select
          value={sortKey}
          onChange={(e) => {
            setHasInteracted(true);
            setSortKey(e.target.value);
          }}
          className="ps-search-sort"
        >
          {SORT_OPTIONS.map((o) => <option key={o.key} value={o.key}>{o.label}</option>)}
        </select>
        <div className="ps-search-arrows">
          <button onClick={prev} disabled={results.length === 0} title="Previous">▲</button>
          <span className="ps-search-counter">
            {results.length > 0 ? `${index + 1} / ${results.length}` : "0"}
          </span>
          <button onClick={next} disabled={results.length === 0} title="Next">▼</button>
        </div>
      </div>
      {current && (
        <div className="ps-search-preview">
          <span className="ps-search-preview-name">{current.name || shortAddr(current.address)}</span>
          <span className="ps-search-preview-type">{current.type}</span>
          <span className="ps-search-preview-addr">{shortAddr(current.address)}</span>
          {current.value > 0 && <span className="ps-search-preview-value">{formatUsd(current.value)}</span>}
          {current.kind === "principal" && current.type === "safe" && current.signers > 0 && (
            <span className="ps-search-preview-meta">{current.signers}/{current.principal?.details?.owners?.length || "?"} signers</span>
          )}
          {current.kind === "principal" && current.type === "timelock" && current.delay > 0 && (
            <span className="ps-search-preview-meta">{formatDelay(current.delay)} delay</span>
          )}
          {current.kind === "principal" && (
            <span className="ps-search-preview-meta">controls {current.functions} contracts</span>
          )}
          {current.kind === "contract" && (
            <span className="ps-search-preview-meta">{current.functions} fns</span>
          )}
        </div>
      )}
    </div>
  );
}
