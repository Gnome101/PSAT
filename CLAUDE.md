# CLAUDE.md

Agent notes. Non-obvious things only.

## Tests

Two suites, marker-filtered: `pytest -m "not live"` (offline, CI runs this) and `pytest -m live`. Tests under `tests/live/` are auto-marked by `tests/live/conftest.py`.

**Never use `-k "not live"`** — it silently caught false positives like `test_live_findings_*`. Use the marker.

### Offline

```bash
docker compose up postgres minio minio-init -d
set -a; source .env; set +a
PSAT_LLM_STUB_DIR=tests/fixtures/scope_extraction/llm_responses \
  uv run pytest -m "not live" -q
```

- **`PSAT_LLM_STUB_DIR` is load-bearing.** Without it, scope-extraction integration tests hit real OpenRouter. Kept out of `.env` so dev pipelines don't accidentally use stubs.
- Host Postgres port is **5433** (`5433:5432` in compose); CI uses 5432. Only matters if writing URLs by hand.
- Schema drift (`UndefinedColumn` / `UndefinedTable`) → drop+recreate `psat_test`; the conftest session fixture reapplies migrations on next run. Manual: `DATABASE_URL=$TEST_DATABASE_URL uv run alembic upgrade head`.

### Live

`PSAT_LIVE_URL` defaults to `http://127.0.0.1:8000`. No `PSAT_ADMIN_KEY` → suite skips cleanly (not a bug).

- `PSAT_LIVE_AUDIT_URL`: CI pins to `tests/fixtures/audits/sample_audit.pdf` via `raw.githubusercontent.com/<repo>/<head_sha>/...` — repo-owned, immune to upstream rot.
- `PSAT_LIVE_OTHER_PR=<n>` enables the cross-PR tenancy check; without it `test_artifact_tenancy.py` skips.
- CI runs this as the `live-tests` job in `pr.yml` after `ci` and `deploy`. Sticky comment header: `psat-live-tests`.

### Live test gotchas

- Never `requests.post()` an admin endpoint directly — you'll forget the header. Go through `live_client`. Auth/CORS tests are the documented exception; comment why when bypassing.
- `requests.Session` isn't thread-safe across calls. `test_concurrency.py` spawns one `LiveClient` per thread; reuse that pattern when parallelizing.
- `MonitoredContract` rows leak across runs: `WatchedProxy.delete` is `ON DELETE SET NULL` for the link (`db/models.py`), and there's no admin DELETE. Use a stable `(address, chain)` rather than randomizing — the unique key keeps it a singleton.


## Style

Comments only when the *why* is non-obvious. Don't narrate what the code says.
