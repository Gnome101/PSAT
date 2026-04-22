# Deployment

PSAT ships to [Fly.io](https://fly.io) with Postgres on [Neon](https://neon.tech)
and S3-compatible object storage on [Fly Tigris](https://fly.io/docs/reference/tigris/).

Two deploy paths:

- **Production** — one long-lived Fly app named `psat`, redeployed on every
  push to `main` by `.github/workflows/prod-deploy.yml`.
- **PR previews** — an ephemeral Fly app `psat-pr-<N>` per open pull request,
  managed by `.github/workflows/pr-preview.yml` (create/update) and
  `.github/workflows/pr-cleanup.yml` (destroy).

Each preview gets its own Neon database (`psat_pr_<N>`) and writes artifacts
under a per-PR prefix (`pr-<N>/`) inside a shared Tigris bucket.

---

## One-time provisioning

These steps happen once, by hand, by an operator with Fly + Neon access.
The workflows fail until they're complete.

### 1. Neon

Create a Neon account and two root databases — use either two Neon projects
or two databases inside one project. Typical names:

- `psat_staging` — hosts every preview database (`psat_pr_42`, `psat_pr_51`, …)
- `psat_prod` — hosts production

For each, note:

- The **direct endpoint** hostname (e.g. `ep-cold-moon-12345.us-east-2.aws.neon.tech`).
- The **pooler endpoint** hostname — same base, with `-pooler` suffixed on the
  compute id (e.g. `ep-cold-moon-12345-pooler.us-east-2.aws.neon.tech`).
- The database role and password.

> **Why both endpoints?** The pooler runs pgbouncer in transaction mode and
> blocks admin commands like `CREATE DATABASE` / `DROP DATABASE`. The app uses
> the pooler (pooled, high-connection-count safe); CI uses the direct endpoint
> for admin ops.

The role used for `CREATE DATABASE` must have `CREATEDB` privilege. Neon's
default role does.

> **`dbname=postgres` assumption.** The workflows connect to a maintenance
> database called `postgres` to issue `CREATE/DROP DATABASE`. If your Neon
> project's default database is named `neondb` instead, edit the two psql
> commands in `pr-preview.yml` / `pr-cleanup.yml` accordingly.

### 2. Fly

```bash
# Install flyctl locally if you haven't: https://fly.io/docs/flyctl/install/

# Prod app shell (Dockerfile builds once the first deploy runs).
fly apps create psat --org psat

# Tigris buckets — one for prod, one shared by every preview.
fly storage create --name psat-artifacts --org psat
fly storage create --name psat-artifacts-staging --org psat
```

`fly storage create` prints an endpoint URL + access/secret keys. Capture
them; they go into the Fly (prod) secrets and GitHub (staging) environment
below.

### 3. Prod secrets (one-time, set directly on Fly)

```bash
fly secrets set -a psat \
  DATABASE_URL="postgresql://psat:PASSWORD@NEON_PROD_POOLER_HOST/psat_prod?sslmode=require" \
  PSAT_ADMIN_KEY="..." \
  PSAT_SITE_ORIGIN="https://psat.fly.dev" \
  ETHERSCAN_API_KEY="..." \
  ETH_RPC="..." \
  ENVIO_API_TOKEN="..." \
  TAVILY_API_KEY="..." \
  OPEN_ROUTER_KEY="..." \
  GITHUB_TOKEN="..." \
  ARTIFACT_STORAGE_ENDPOINT="..." \
  ARTIFACT_STORAGE_BUCKET="psat-artifacts" \
  ARTIFACT_STORAGE_ACCESS_KEY="..." \
  ARTIFACT_STORAGE_SECRET_KEY="..."
```

Note the prod `DATABASE_URL` uses the Neon prod **pooler** hostname. No
`ARTIFACT_STORAGE_PREFIX` — prod writes to the bucket root.

### 4. GitHub Environments

In the repo: **Settings → Environments → New environment**. Create two:

- **`staging`** — used by `pr-preview.yml` and `pr-cleanup.yml`.
- **`production`** — used by `prod-deploy.yml`. Optionally enable
  "Required reviewers" here for a manual approval gate before every prod deploy.

Populate each with the secrets listed below.

---

## Secrets layout

### `staging` environment

| Secret                          | Purpose                                                 |
| ------------------------------- | ------------------------------------------------------- |
| `FLY_API_TOKEN`                 | Org-scoped token; can create/destroy preview apps.      |
| `NEON_STAGING_HOST`             | Direct compute endpoint. Used for CREATE/DROP DATABASE. |
| `NEON_STAGING_POOLER_HOST`      | `-pooler` endpoint. Used in the app's DATABASE_URL.     |
| `NEON_STAGING_USER`             | Neon role with CREATEDB (e.g. `psat`).                  |
| `NEON_STAGING_PASSWORD`         | Password for the above role. Must be URL-safe (see below). |
| `PSAT_ADMIN_KEY`                | Any high-entropy string; gates non-GET API calls.       |
| `ETHERSCAN_API_KEY`             | Etherscan V2 API key.                                   |
| `ETH_RPC`                       | Ethereum RPC URL (ideally with trace support).          |
| `ENVIO_API_TOKEN`               | Envio HyperSync token.                                  |
| `TAVILY_API_KEY`                | Tavily search API key.                                  |
| `OPEN_ROUTER_KEY`               | OpenRouter LLM token.                                   |
| `PROTOCOL_GITHUB_TOKEN`         | PAT for GitHub org enumeration. **Renamed** from `GITHUB_TOKEN` so the workflow's built-in `GITHUB_TOKEN` isn't shadowed; the preview workflow copies this into the Fly `GITHUB_TOKEN` secret at deploy time. |
| `ARTIFACT_STORAGE_ENDPOINT`     | Staging Tigris endpoint URL.                            |
| `ARTIFACT_STORAGE_BUCKET`       | `psat-artifacts-staging`.                               |
| `ARTIFACT_STORAGE_ACCESS_KEY`   | Staging bucket S3 access key.                           |
| `ARTIFACT_STORAGE_SECRET_KEY`   | Staging bucket S3 secret key.                           |

### `production` environment

| Secret                | Purpose                                                      |
| --------------------- | ------------------------------------------------------------ |
| `FLY_PROD_API_TOKEN`  | Deploy token **scoped only** to the `psat` app — smaller blast radius on leak than an org token. Generate with `fly tokens create deploy -a psat`. |

Prod `DATABASE_URL` and all other runtime secrets live as **Fly** secrets on
the `psat` app (see the one-time provisioning step above). They are not
GitHub secrets, so they can't be exfiltrated via a compromised workflow.

### Password URL-safety

`DATABASE_URL` for previews is built inline in the workflow from
`NEON_STAGING_PASSWORD`. If the password contains `@`, `:`, `/`, `?`, `#`, or
other URL-reserved characters, the connection string will parse wrong.
Regenerate the password on Neon until it's URL-safe, or edit the workflow
to URL-encode before interpolation.

---

## Preview lifecycle

1. **PR opened/updated**, base is `main` or any branch:
   - Workflow `pr-preview.yml` fires.
   - `flyctl apps create psat-pr-<N>` (idempotent).
   - `CREATE DATABASE psat_pr_<N>` on the Neon staging direct endpoint.
   - `flyctl secrets set --stage` populates per-app secrets — including
     `ARTIFACT_STORAGE_PREFIX=pr-<N>/` so artifact/source-file keys are
     scoped to that prefix inside the shared staging bucket.
   - `flyctl deploy -a psat-pr-<N> --remote-only` builds + ships the image.
   - Sticky PR comment is posted with the URL.

2. **PR closed or merged**:
   - Workflow `pr-cleanup.yml` fires.
   - `flyctl apps destroy psat-pr-<N> --yes`.
   - `DROP DATABASE psat_pr_<N> WITH (FORCE)` on Neon (terminates lingering
     worker connections so the drop succeeds).
   - `aws s3 rm s3://<bucket>/pr-<N>/ --recursive` against the Tigris endpoint.
   - Sticky PR comment is updated.
   - All destructive commands are `|| true` so a "closed with no pushes →
     nothing to clean up" PR is a no-op.

3. **Push to `main`**:
   - Workflow `prod-deploy.yml` fires (gated by the `production` environment).
   - `flyctl deploy -a psat --remote-only`.

### Fork PRs

Preview deploys are **skipped** for PRs opened from forks. GitHub hides
secrets from `pull_request` events on forks, which would make the deploy
silently fail partway through and leave a half-provisioned Fly app + Neon
database dangling. To preview a fork contribution, cherry-pick its commits
into a same-repo branch and push that.

### Same image, both environments

Prod and previews share `Dockerfile` and `fly.toml`. Only the secrets
differ. Previews inherit `min_machines_running = 1`, so they stay warm for
as long as the PR is open. A handful of active previews cost a few dollars
a month; teardown on PR close bounds the total.

---

## Runtime shape

Every Fly machine (prod or preview) runs **one container** that starts, in
order:

1. `python -c "from db.models import create_tables; create_tables()"` — idempotent schema init.
2. `./start_workers.sh &` — background, starts every worker (discovery,
   static, resolution, policy, coverage, dapp_crawl, defillama, selection,
   audit_text_extraction, audit_scope_extraction).
3. `exec uvicorn api:app --host 0.0.0.0 --port 8000` — foreground, so
   `SIGTERM` from Fly reaches uvicorn directly. Workers get cleaned up via
   container teardown.

Entrypoint is `/app/start_container.sh` inside the image.

---

## Neon gotchas

- **Always use the pooler endpoint from the app.** The app + ~10 workers each
  open their own SQLAlchemy pools. Two or three concurrent previews against
  the direct compute endpoint will exhaust Neon's connection cap.
- **Always use the direct endpoint for admin SQL** (`CREATE/DROP DATABASE`,
  anything transactional-by-default like `CREATE INDEX CONCURRENTLY`).
  pgbouncer in transaction pooling mode blocks these.
- **Compute auto-suspends on idle.** While reviewers are actively poking a
  preview, workers poll every 2–5 seconds and keep it warm. When everyone
  walks away, the DB compute suspends. The next query cold-starts in
  ~500ms–1s; harmless unless you're benchmarking.
- **SSL is required.** Connection strings must include `?sslmode=require`.
  The workflows already do this; if you point the app at a different host
  and the connection hangs, check the SSL param first.
- **`WITH (FORCE)` on drop.** Requires Postgres ≥ 16 (Neon supports it).
  Without it, the drop fails if any worker hasn't fully disconnected yet.

## Tigris gotchas

- **Single shared staging bucket, prefix per PR.** Tigris doesn't make
  per-PR buckets cheap or pleasant. The app reads `ARTIFACT_STORAGE_PREFIX`
  (normalized to end in `/`) and prepends it to every storage key; cleanup
  deletes the prefix recursively.
- **Prod writes to the bucket root** — no `ARTIFACT_STORAGE_PREFIX` set.

---

## Token rotation

### FLY_API_TOKEN / FLY_PROD_API_TOKEN

```bash
# Revoke the leaked one first.
fly tokens revoke <token-id>
# List current tokens to confirm.
fly tokens list

# Create replacements.
fly tokens create org psat --name ci-staging       # staging env
fly tokens create deploy -a psat --name ci-prod    # production env
```

Paste the new token into the relevant GitHub environment secret. No app
restart needed — Fly re-reads the token on next CLI call.

### Neon password

1. In the Neon dashboard, reset the password for the `psat` role.
2. Update `NEON_STAGING_PASSWORD` (GitHub `staging` env) and the prod
   `DATABASE_URL` Fly secret (`fly secrets set -a psat DATABASE_URL=...`).
3. Previews created before the rotation still have the old password baked
   into their `DATABASE_URL` Fly secret. They keep working until the next
   push forces a redeploy, which restages the secret with the new password.
   If you need them off the old password immediately, trigger a redeploy
   manually: `flyctl deploy -a psat-pr-<N> --remote-only`.

### Tigris credentials

Regenerate on Fly (`fly storage`) and update both GitHub staging env and
prod Fly secrets. Same lag caveat as Neon password for in-flight previews.

### App-level keys (Etherscan, Envio, Tavily, OpenRouter, GitHub PAT)

Rotate on the upstream provider, update the GitHub `staging` environment
secret and the prod Fly secret, trigger a redeploy.

---

## Troubleshooting

### Force-rebuild a stuck preview

If a deploy fails mid-way (build error, network flake, secrets
misconfigured) and the preview is in a wedged state:

```bash
# Nuclear option — destroy everything and let the next workflow run
# recreate it from scratch. Safer than trying to hand-repair.
flyctl apps destroy psat-pr-<N> --yes
PGPASSWORD=... psql "sslmode=require host=<neon-direct> user=<role> dbname=postgres" \
  -c "DROP DATABASE IF EXISTS \"psat_pr_<N>\" WITH (FORCE);"
aws s3 rm "s3://psat-artifacts-staging/pr-<N>/" --recursive \
  --endpoint-url <tigris-endpoint>
```

Then push an empty commit to the PR (`git commit --allow-empty -m "rebuild preview"`
+ push) to trigger `pr-preview.yml` fresh.

### Preview comment shows the wrong URL

Sticky comments are keyed by `header: psat-preview`. If you've changed the
preview app naming scheme, delete the comment manually; the next run will
post a new one.

### Prod deploy stuck in "waiting for approval"

The `production` environment has "Required reviewers" enabled. Go to the
run's page in Actions and approve it. Logged in the audit trail.

### Workflow action SHAs

Three actions are pinned to commit SHAs for supply-chain safety:

- `superfly/flyctl-actions/setup-flyctl@ed8efb3…` — tracks `master`; upgrade
  by resolving the latest master SHA on the repo.
- `marocchino/sticky-pull-request-comment@773744…` — v2.9.4.

`actions/checkout@v4` is left as a tag since it's a first-party GitHub action.
