# CLAUDE.md

Agent notes. Non-obvious things only.

## Tests — two suites, filtered by marker

- `pytest -m "not live"` — offline suite. CI runs this. Needs local Postgres + MinIO from docker-compose.
- `pytest -m live` — live suite. Hits a deployed PSAT API. Needs `PSAT_LIVE_URL` + `PSAT_ADMIN_KEY`.

Tests under `tests/live/` are auto-marked by `tests/live/conftest.py` — no per-file `pytestmark` needed. **Never use `-k "not live"`** — it was dropped because it silently caught false positives like `test_live_findings_*`.

## Offline suite

`.env` already has `TEST_DATABASE_URL` + the `TEST_ARTIFACT_STORAGE_*` block. Source it; supply only the one missing var:

```bash
docker compose up postgres minio minio-init -d
set -a; source .env; set +a
PSAT_LLM_STUB_DIR=tests/fixtures/scope_extraction/llm_responses \
  uv run pytest -m "not live" -q
```

- **`PSAT_LLM_STUB_DIR` is load-bearing.** Without it, scope-extraction integration tests hit real OpenRouter. It's kept out of `.env` so dev pipelines don't accidentally use stubs.
- **Schema drift** → `UndefinedColumn` / `UndefinedTable` errors. Drop and recreate the test DB:
  ```bash
  docker exec psat-postgres-1 psql -U psat -d psat -c "DROP DATABASE IF EXISTS psat_test WITH (FORCE);"
  docker exec psat-postgres-1 psql -U psat -d psat -c "CREATE DATABASE psat_test OWNER psat;"
  ```
- Host Postgres port is **5433** (docker-compose maps `5433:5432`); CI uses 5432. Only matters if you're writing URLs by hand.

## Live suite

```bash
PSAT_LIVE_URL=https://psat-pr-42.fly.dev \
PSAT_ADMIN_KEY=... \
  uv run pytest -m live -q
```

- No `PSAT_ADMIN_KEY` → whole suite skips cleanly (session fixture). Not a bug.
- `PSAT_LIVE_URL` defaults to `http://127.0.0.1:8000` — run `./start_local.sh` first if you want that.
- `PSAT_LIVE_AUDIT_URL` overrides the default audit PDF used by `test_audits.py`. The default points at a Spearbit repo URL that will eventually rot — override rather than chase links.
- `.github/workflows/live.yml` runs this suite on every PR after `pr-preview.yml` finishes, against the per-PR Fly preview. Sticky comment header is `psat-live-tests`.

## Writing new live tests

Use fixtures from `tests/live/conftest.py`. Do not reinvent polling, auth, or URL building.

```python
def test_static_artifact_emitted(analyzed_weth, live_client):
    a = live_client.artifact(analyzed_weth["name"], "contract_analysis")
    assert isinstance(a, dict) and "subject" in a
```

Available fixtures:

- `analyzed_weth` — session-scoped; one completed WETH analysis, shared across all tests that request it. Use this whenever you just need "some finished job to inspect."
- `analyze_and_wait(addr)` — factory when a test needs its own fresh analysis of a specific address.
- `analyzed_company` — session-scoped; ensures `DEFAULT_TEST_COMPANY` (etherfi) has a Protocol row. Use this when a test POSTs to an endpoint that 404s without an existing company (audits, coverage).
- `live_client` — the HTTP client. Methods:
  - `.analyze(addr)` / `.analyze_company(name, limit=)` — POST `/api/analyze` with admin-key header attached.
  - `.job(job_id)` / `.jobs()` / `.children_of(parent_id)` — job reads.
  - `.artifact(run_name, artifact_name)` — artifact fetch; handles `.json` vs `.txt` routing.
  - `.poll_job_until_done(job_id)` / `.poll_children_until_done(parent_id)` — polling with timeout.
  - `.submit_and_wait(addr)` / `.submit_company_and_wait(name)` — analyze + poll in one call.

Never `requests.post()` an admin-protected endpoint directly — you'll forget the header. Go through `live_client`.
