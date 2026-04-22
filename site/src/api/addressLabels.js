// Thin API wrappers for /api/address_labels/*. See api.py for the server
// shapes. Admin mutations (put/delete) use the shared api() helper so the
// X-PSAT-Admin-Key header is auto-injected; a missing/invalid key triggers
// the built-in 401 prompt-retry flow.

import { api } from "./client.js";

export function listAddressLabels() {
  return api("/api/address_labels");
}

export function upsertAddressLabel(address, name, note = null) {
  const body = note == null ? { name } : { name, note };
  return api(`/api/address_labels/${encodeURIComponent(address)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function deleteAddressLabel(address) {
  return api(`/api/address_labels/${encodeURIComponent(address)}`, {
    method: "DELETE",
  });
}
