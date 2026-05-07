import { useMemo } from "react";

import { ALL_ROLES, ROLE_META } from "../meta.js";

export function RoleFilterBar({ machines, enabledRoles, onToggle }) {
  const counts = useMemo(() => {
    const c = {};
    for (const r of ALL_ROLES) c[r] = 0;
    for (const m of machines) c[m.role] = (c[m.role] || 0) + 1;
    return c;
  }, [machines]);

  return (
    <div className="ps-role-bar">
      {ALL_ROLES.map((role) => {
        const meta = ROLE_META[role];
        const on = enabledRoles.has(role);
        return (
          <button
            key={role}
            type="button"
            className={`ps-role-chip${on ? " ps-role-chip-on" : ""}`}
            style={{ "--role-color": meta.color }}
            onClick={() => onToggle(role)}
          >
            <span className="ps-role-dot" />
            <span>{meta.label}</span>
            <span className="ps-role-count">{counts[role]}</span>
          </button>
        );
      })}
    </div>
  );
}
