# Docker Backend

This repo now includes a backend-focused Docker setup for the FastAPI demo server and analysis pipeline.

For local frontend + backend development without Docker, see [site-demo.md](/home/gnome2/asu/capstone/PSAT/docs/site-demo.md).

## What it includes

- Python backend with project dependencies installed via `uv`
- Foundry installed in the container so Slither/Foundry-backed analysis can run
- Built React site copied into the image and served by `web_demo.py`
- `contracts/` mounted from the host so analysis artifacts persist outside the container

## Start the backend

```bash
docker compose up --build backend
```

The API and demo site will be available at:

```text
http://127.0.0.1:8000
```

## Stop it

```bash
docker compose down
```

## Notes

- The service reads environment variables from `.env`.
- The host `contracts/` directory is mounted into `/app/contracts`.
- The image bakes in the current `site/` frontend build during `docker build`.
- If you only change frontend code, rebuild the image:

```bash
docker compose build backend
```
