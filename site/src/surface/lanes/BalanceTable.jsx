import { useState } from "react";

import { formatUsd } from "../format.js";

export function BalanceTable({ machine }) {
  const [hideDust, setHideDust] = useState(true);

  if (!machine.balances || machine.balances.length === 0) {
    return <div className="ps-lane-empty">No token balances</div>;
  }

  const filtered = hideDust
    ? machine.balances.filter((b) => b.usd_value == null || b.usd_value >= 10)
    : machine.balances;
  const hiddenCount = machine.balances.length - filtered.length;

  return (
    <section className="ps-balance-section">
      <div className="ps-balance-header">
        <span>Balances</span>
        {machine.total_usd ? <span className="ps-balance-total">{formatUsd(machine.total_usd)}</span> : null}
      </div>
      <button
        className={`ps-balance-filter${hideDust ? " active" : ""}`}
        onClick={() => setHideDust(!hideDust)}
      >
        {hideDust ? `Hide <$10 (${hiddenCount})` : "Show all"}
      </button>
      <div className="ps-balance-list">
        {filtered.map((b, i) => {
          const human = Number(b.raw_balance) / (10 ** b.decimals);
          const amount = human >= 1e6 ? `${(human / 1e6).toFixed(1)}M`
            : human >= 1e3 ? `${(human / 1e3).toFixed(1)}K`
            : human >= 1 ? human.toFixed(2)
            : human.toFixed(6);
          return (
            <div key={i} className="ps-balance-row">
              <div className="ps-balance-token">
                <span className="ps-balance-symbol">{b.token_symbol}</span>
                <span className="ps-balance-name">{b.token_name}</span>
              </div>
              <div className="ps-balance-values">
                <span className="ps-balance-amount">{amount}</span>
                <span className="ps-balance-usd">{b.usd_value ? formatUsd(b.usd_value) : "—"}</span>
              </div>
            </div>
          );
        })}
        {filtered.length === 0 && <div className="ps-lane-empty">No balances above $10</div>}
      </div>
    </section>
  );
}
