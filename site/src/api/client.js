// Shared HTTP helper: injects X-PSAT-Admin-Key, prompts for a key on 401,
// and parses JSON / text based on the response Content-Type. Kept in one
// place so every new page gets the same admin-key flow as App.jsx.

const ADMIN_KEY_STORAGE = "psat_admin_key";

export function getAdminKey() {
  try {
    return window.localStorage.getItem(ADMIN_KEY_STORAGE) || "";
  } catch {
    return "";
  }
}

export function setAdminKey(key) {
  try {
    if (key) window.localStorage.setItem(ADMIN_KEY_STORAGE, key);
    else window.localStorage.removeItem(ADMIN_KEY_STORAGE);
  } catch {
    // localStorage unavailable (private mode, etc.) — admin actions will
    // require re-entering the key on every request.
  }
}

function buildHeadersWithKey(options, key) {
  const headers = new Headers(options.headers || {});
  if (key && !headers.has("X-PSAT-Admin-Key")) {
    headers.set("X-PSAT-Admin-Key", key);
  }
  return headers;
}

export async function api(path, options = {}) {
  let response = await fetch(path, { ...options, headers: buildHeadersWithKey(options, getAdminKey()) });
  if (response.status === 401) {
    const entered = window.prompt(
      "Admin key required for this action.\nPaste your PSAT admin key:",
      getAdminKey(),
    );
    if (entered) {
      setAdminKey(entered);
      response = await fetch(path, { ...options, headers: buildHeadersWithKey(options, entered) });
    }
  }
  if (!response.ok) {
    throw new Error(await response.text());
  }
  const type = response.headers.get("content-type") || "";
  if (type.includes("application/json")) {
    return response.json();
  }
  return response.text();
}

// ─── Stale-while-revalidate in-memory cache ──────────────────────────────
// Navigations back to a page we already loaded feel instant because we
// serve the last cached response immediately, while fetching a fresh one
// in the background. When the fresh response arrives we call the
// subscriber's callback so it can re-render with new data.
//
// Keyed on (method + path + body). Only GET requests are auto-cached;
// call apiSwr explicitly from the page to opt in.

const _swrCache = new Map(); // key → { value, ts, inflight }
const _swrSubs = new Map(); // key → Set<callback>
const SWR_FRESH_MS = 15_000; // "don't refetch yet" window

function _swrKey(path, options) {
  // Body keys matter for non-GET, but apiSwr is GET-only by contract.
  return `${options?.method || "GET"} ${path}`;
}

/**
 * Stale-while-revalidate fetch. Returns a cached value immediately if
 * one exists, then calls ``onUpdate`` when a fresh value arrives (only
 * if it differs from the cached one).
 *
 * Usage in a React component:
 *
 *   useEffect(() => {
 *     let live = true;
 *     apiSwr("/api/company/foo", null, (fresh) => { if (live) setData(fresh); })
 *       .then((initial) => { if (live) setData(initial); });
 *     return () => { live = false; };
 *   }, []);
 */
export async function apiSwr(path, options, onUpdate) {
  const key = _swrKey(path, options);
  const entry = _swrCache.get(key);
  const now = Date.now();
  const stale = !entry || now - entry.ts > SWR_FRESH_MS;

  // Fire a background refresh if needed.
  if (stale && !entry?.inflight) {
    const p = api(path, options || {})
      .then((value) => {
        _swrCache.set(key, { value, ts: Date.now(), inflight: null });
        // Notify anyone who subscribed while this was in flight.
        const subs = _swrSubs.get(key);
        if (subs && onUpdate && entry?.value !== value) onUpdate(value);
        return value;
      })
      .catch((e) => {
        _swrCache.set(key, { ...(entry || {}), ts: Date.now(), inflight: null });
        throw e;
      });
    _swrCache.set(key, { ...(entry || {}), inflight: p });
    if (!entry) return p; // first-ever fetch — return the promise
  }
  return entry?.value;
}

/** Drop cached entries starting with ``prefix`` (or all if null). */
export function invalidateSwr(prefix) {
  if (prefix == null) {
    _swrCache.clear();
    return;
  }
  for (const k of [..._swrCache.keys()]) {
    if (k.includes(prefix)) _swrCache.delete(k);
  }
}
