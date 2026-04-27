"""Lightweight memory introspection helpers (Linux /proc + cgroup v2, no extra deps).

Used by workers/base.py to log per-process and per-job RSS so we can attribute
OOM kills back to specific worker × job pairs without pulling in psutil.

All functions return ``None`` (or an empty/zero default) on non-Linux hosts
or when the relevant proc/cgroup file is unreadable, so callers can use them
unconditionally.
"""

from __future__ import annotations

import os
from pathlib import Path

_PROC_STATUS = Path("/proc/self/status")
_CGROUP_V2_MAX = Path("/sys/fs/cgroup/memory.max")
_CGROUP_V2_CURRENT = Path("/sys/fs/cgroup/memory.current")
_CGROUP_V2_PEAK = Path("/sys/fs/cgroup/memory.peak")


def current_rss_bytes() -> int:
    """RSS of this process in bytes; 0 if /proc/self/status is unreadable."""
    try:
        for line in _PROC_STATUS.read_text().splitlines():
            if line.startswith("VmRSS:"):
                # "VmRSS:    13648 kB"
                return int(line.split()[1]) * 1024
    except Exception:
        return 0
    return 0


def _read_cgroup_int(path: Path) -> int | None:
    try:
        text = path.read_text().strip()
    except Exception:
        return None
    if text == "max":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def cgroup_memory_max_bytes() -> int | None:
    """Container memory limit (cgroup v2). None on dev hosts / cgroup v1."""
    return _read_cgroup_int(_CGROUP_V2_MAX)


def cgroup_memory_current_bytes() -> int | None:
    """Aggregate RSS across all processes in the cgroup. None outside a container."""
    return _read_cgroup_int(_CGROUP_V2_CURRENT)


def cgroup_memory_peak_bytes() -> int | None:
    """High-water mark of cgroup memory since boot. None outside a container."""
    return _read_cgroup_int(_CGROUP_V2_PEAK)


def count_sibling_python_procs() -> int:
    """Number of python processes visible in /proc — approximation of the
    fleet shape inside this VM. Returns 0 on non-Linux / restricted /proc."""
    n = 0
    try:
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                comm = Path(f"/proc/{entry}/comm").read_text().strip()
            except Exception:
                continue
            if comm.startswith("python"):
                n += 1
    except Exception:
        return 0
    return n


def mb(bytes_value: int | None) -> str:
    """Format bytes as MB (no decimals); '?' for None."""
    if bytes_value is None:
        return "?"
    return f"{bytes_value / (1024 * 1024):.0f}"


# ---------------------------------------------------------------------------
# Cache-pressure threshold tracking
# ---------------------------------------------------------------------------

# {cache_name: highest_threshold_pct_logged}; reset via reset_cache_pressure_state.
_CACHE_PRESSURE_STATE: dict[str, int] = {}


def cache_pressure_message(name: str, current: int, max_size: int) -> str | None:
    """Return a one-line pressure message when *current* crosses 50/75/95% of
    *max_size* for the first time; None otherwise. Per-name state, in-memory.

    Caller should ``logger.info("[CACHE_PRESSURE] %s", msg)`` when non-None.
    """
    if max_size <= 0:
        return None
    pct = (current / max_size) * 100
    last = _CACHE_PRESSURE_STATE.get(name, 0)
    for threshold in (95, 75, 50):
        if pct >= threshold > last:
            _CACHE_PRESSURE_STATE[name] = threshold
            return f"cache={name} size={current}/{max_size} ({pct:.0f}%)"
    return None


def reset_cache_pressure_state(name: str | None = None) -> None:
    """Forget the last threshold for *name* (or clear all if None). Call from
    cache ``clear_*`` helpers so post-test/manual resets don't suppress the
    next genuine pressure event."""
    if name is None:
        _CACHE_PRESSURE_STATE.clear()
    else:
        _CACHE_PRESSURE_STATE.pop(name, None)
