"""Tiny in-process TTL cache for expensive GET endpoints.

Design goals:
- Zero dependencies — standard library only.
- Thread-safe (FastAPI dispatches via a thread pool for sync views).
- Per-process — no Redis required. Each web machine has its own cache
  so a cache miss on machine A doesn't block machine B.
- Short default TTL (30s) so a completed pipeline job surfaces in the
  UI within that window without manual invalidation.

Typical use on an expensive read endpoint:

    from utils.cache import ttl_cache

    @app.get("/api/company/{name}")
    @ttl_cache(seconds=30, key=lambda name: f"company:{name.lower()}")
    def company_overview(name: str) -> dict: ...

The wrapped function must be deterministic in its args — side-effect
free apart from the return value. Don't wrap writes or endpoints that
depend on ``request.headers`` (they're not in the cache key).
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from functools import wraps
from typing import Any


def _cache_disabled() -> bool:
    """Tests set ``PSAT_CACHE_DISABLED=1`` so state doesn't bleed
    between tests — otherwise a 200-OK response from test A would leak
    into test B's expected-500 error path via the shared in-memory
    dict. In production the env is unset and caching runs normally.

    Read on each call (not once at import) so a test fixture that sets
    the env mid-process still takes effect.
    """
    return (os.environ.get("PSAT_CACHE_DISABLED") or "").lower() in ("1", "true", "yes")


class _TTLStore:
    """A bounded dict keyed on a string; entries expire on read after
    the configured TTL. Not LRU — overflowing entries are evicted on
    next-access scan, which is cheap at our single-process scale."""

    def __init__(self, max_entries: int = 256) -> None:
        self._data: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._max = max_entries

    def get(self, key: str, ttl_seconds: float) -> Any | None:
        now = time.monotonic()
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if expires_at <= now:
                self._data.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any, ttl_seconds: float) -> None:
        expires_at = time.monotonic() + ttl_seconds
        with self._lock:
            if len(self._data) >= self._max:
                # Drop the single oldest-expiring entry. Coarse but
                # simple; replaces the whole Redis/Caffeine rabbit hole.
                oldest_key = min(self._data, key=lambda k: self._data[k][0])
                self._data.pop(oldest_key, None)
            self._data[key] = (expires_at, value)

    def invalidate(self, prefix: str | None = None) -> int:
        """Drop entries whose key starts with ``prefix`` (or all if None)."""
        with self._lock:
            if prefix is None:
                n = len(self._data)
                self._data.clear()
                return n
            to_drop = [k for k in self._data if k.startswith(prefix)]
            for k in to_drop:
                self._data.pop(k, None)
            return len(to_drop)


_store = _TTLStore()


def ttl_cache(
    seconds: float = 30.0,
    key: Callable[..., str] | None = None,
) -> Callable:
    """Decorator: cache the return value of a function for ``seconds``.

    ``key`` maps the function's positional+keyword args to a cache key
    string. If omitted, a key is built by repr()'ing the args tuple.

    Thread-safe: simultaneous cache-miss callers both run the wrapped
    function (no stampede guard — acceptable at our scale where the
    expensive reads cost ~3s, not ~30s). If you need coalescing, add
    a per-key lock; it's ~10 LOC extra and currently not worth it.
    """

    def _decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def _wrapper(*args, **kwargs):
            if _cache_disabled():
                return fn(*args, **kwargs)
            if key is not None:
                k = key(*args, **kwargs)
            else:
                k = f"{fn.__module__}.{fn.__qualname__}({args!r}, {sorted(kwargs.items())!r})"
            cached = _store.get(k, seconds)
            if cached is not None:
                return cached
            result = fn(*args, **kwargs)
            _store.set(k, result, seconds)
            return result

        _wrapper.cache_invalidate = lambda prefix=None: _store.invalidate(prefix)  # type: ignore[attr-defined]
        return _wrapper

    return _decorator


def invalidate_cache(prefix: str | None = None) -> int:
    """Clear cache entries starting with ``prefix``, or all if None."""
    return _store.invalidate(prefix)
