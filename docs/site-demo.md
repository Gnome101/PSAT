# Demo Site

This repo ships a small demo site built with:

- FastAPI backend in `web_demo.py`
- React + Vite frontend in `site/`

Backend logic is grouped into the domain packages:

- [`services/discovery/`](/home/gnome2/asu/capstone/PSAT/services/discovery)
- [`services/static/`](/home/gnome2/asu/capstone/PSAT/services/static)
- [`services/resolution/`](/home/gnome2/asu/capstone/PSAT/services/resolution)
- [`services/policy/`](/home/gnome2/asu/capstone/PSAT/services/policy)

## Run It Locally

Start the backend:

```bash
uv run uvicorn web_demo:app --host 127.0.0.1 --port 8000
```

Start the frontend:

```bash
cd site
npm run dev -- --host 127.0.0.1 --port 5173
```

Open:

```text
http://127.0.0.1:5173
```

The frontend proxies `/api` to the backend.

## Deep Links

You can navigate directly to a contract view by address or run name.

Examples:

- `/address/0x08c6F91e2B681FaF5e17227F2a44C307b3C1364C/summary`
- `/address/0x08c6F91e2B681FaF5e17227F2a44C307b3C1364C/graph`
- `/runs/BoringVault_08c6F91e/graph`

Supported tabs:

- `summary`
- `permissions`
- `principals`
- `graph`
- `raw`

## What The Site Shows

For a completed run, the site reads:

- `contract_analysis.json`
- `control_snapshot.json`
- `resolved_control_graph.json`
- `effective_permissions.json`
- `principal_labels.json`

The Graph tab is a graph-first view of:

- upstream controllers and owners
- authority role gates
- permissioned functions
- the protected contract

## Notes

- The backend stores run artifacts under `contracts/`.
- Recursive artifacts are written as `recursive_*` workspaces under `contracts/`.
- HyperSync authority-policy backfill only runs when `ENVIO_API_TOKEN` is set.
