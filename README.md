# PSAT

Protocol Security Assessment Tool.

The repo fetches verified contract source, runs static analysis, resolves current control state, reconstructs authority policy when available, and exposes the results through a FastAPI + Vite demo site.

## Main Outputs

Each run is written under [`contracts/`](/home/gnome2/asu/capstone/PSAT/contracts) and typically includes:

- `contract_analysis.json`
- `control_tracking_plan.json`
- `control_snapshot.json`
- `resolved_control_graph.json`
- `effective_permissions.json`
- `principal_labels.json`
- `analysis_report.txt`

Some contracts also produce:

- `policy_state.json`
- `policy_event_history.jsonl`

## Local Development

### Python setup

1. Install dependencies:
   ```bash
   uv sync
   ```
2. Create env file:
   ```bash
   cp .env.example .env
   ```
3. Set the keys you need in `.env`.

Common env vars:

- `ETHERSCAN_API_KEY`
- `ETH_RPC`
- `PSAT_DEMO_RPC_URL`
- `ENVIO_API_TOKEN`
- `PSAT_HYPERSYNC_URL`
- `NVIDIA_API_KEY`
- `TAVILY_API_KEY`
- `OPEN_ROUTER_KEY`

### Backend

Run the FastAPI demo server:

```bash
uv run uvicorn web_demo:app --host 127.0.0.1 --port 8000
```

Backend/API URL:

```text
http://127.0.0.1:8000
```

### Frontend

The site lives in [`site/`](/home/gnome2/asu/capstone/PSAT/site).

Install frontend deps once:

```bash
cd site
npm install
```

Run the Vite dev server:

```bash
cd site
npm run dev -- --host 127.0.0.1 --port 5173
```

Frontend URL:

```text
http://127.0.0.1:5173
```

The Vite app proxies `/api` to the FastAPI backend.

## URL Routing

The site supports deep links by address and tab.

Examples:

- `http://127.0.0.1:5173/address/0x08c6F91e2B681FaF5e17227F2a44C307b3C1364C/summary`
- `http://127.0.0.1:5173/address/0x08c6F91e2B681FaF5e17227F2a44C307b3C1364C/graph`
- `http://127.0.0.1:5173/runs/BoringVault_08c6F91e/graph`

Tabs:

- `summary`
- `permissions`
- `principals`
- `graph`
- `raw`

## Running The Pipeline Manually

The CLI entrypoint is:

```bash
uv run python main.py --help
```

Typical run:

```bash
uv run python main.py 0x08c6F91e2B681FaF5e17227F2a44C307b3C1364C --name BoringVault_08c6F91e --no-llm --no-deps
```

Then build the live control artifacts:

```bash
uv run python services/control_tracker.py contracts/BoringVault_08c6F91e/control_tracking_plan.json --rpc https://ethereum-rpc.publicnode.com --snapshot-out contracts/BoringVault_08c6F91e/control_snapshot.json --changes-out contracts/BoringVault_08c6F91e/control_change_events.jsonl --once
```

## Demo Site Flow

The FastAPI demo runner does this for a submitted address:

1. fetches verified source
2. scaffolds a local project under `contracts/<run_name>`
3. runs Slither/static analysis
4. builds `contract_analysis.json`
5. builds `control_tracking_plan.json`
6. resolves `control_snapshot.json`
7. builds `resolved_control_graph.json`
8. optionally backfills authority policy via HyperSync
9. writes `effective_permissions.json`
10. writes `principal_labels.json`

The implementation is in [`services/demo_runner.py`](/home/gnome2/asu/capstone/PSAT/services/demo_runner.py).

## Docker Backend

The backend can also be run in Docker.

```bash
docker compose up --build backend
```

See [docs/docker-backend.md](/home/gnome2/asu/capstone/PSAT/docs/docker-backend.md).

## Examples

Real generated frontend artifacts are copied into [`examples/`](/home/gnome2/asu/capstone/PSAT/examples).

Each example contains the same JSON files the site reads:

- `contract_analysis.json`
- `control_snapshot.json`
- `effective_permissions.json`
- `principal_labels.json`
- `resolved_control_graph.json`

Current examples:

- [`examples/boringvault_08c6f91e`](/home/gnome2/asu/capstone/PSAT/examples/boringvault_08c6f91e)
- [`examples/morpho_bbbbbbbb`](/home/gnome2/asu/capstone/PSAT/examples/morpho_bbbbbbbb)

## Tests

Run the non-live suite:

```bash
uv run pytest -k "not live"
```
