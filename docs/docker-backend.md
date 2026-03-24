# Docker Services

This repo now includes a multi-container Docker setup for the FastAPI API and the Vite site.

For local frontend + backend development without Docker, see `docs/site-demo.md`.

## What it includes

- `api`
  - Python backend with project dependencies installed via `uv`
  - Foundry installed so Slither/Foundry-backed analysis can run
  - `contracts/` mounted from the host so analysis artifacts persist outside the container
- `site`
  - Vite development server
  - proxies `/api` to the `api` container
  - serves the React frontend from `site/`

## Start the containers

```bash
docker compose up --build api site
```

The services will be available at:

```text
API:  http://127.0.0.1:8000
Site: http://127.0.0.1:5173
```

## Stop it

```bash
docker compose down
```

## Notes

- The `api` service reads environment variables from `.env`.
- The host `contracts/` directory is mounted into `/app/contracts`.
- The `site` service uses `VITE_API_PROXY_TARGET=http://api:8000`.
- If you change Docker build inputs, rebuild the relevant service:

```bash
docker compose build api
docker compose build site
```
