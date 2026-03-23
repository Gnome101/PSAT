# PSAT

This is the main repo for the protocol security assesment tool.

The goal is for this repo is to collect information about protocols and analyze the using SOTA LLMs. 

The services directory contains the exisiting services on the pipeline.

The utils directory contains any important API or utility that the services might call.

Currently, addresses are fed into the pipeline via `addresses.json` and results are dumped under `contracts/`.

Each run now attempts:
- static dependent-contract discovery (`dependencies.json`)
- dynamic trace-based dependency discovery (`dynamic_dependencies.json`)
- structured contract analysis (`contract_analysis.json`)
- runtime watch-plan compilation (`control_tracking_plan.json`)

`contract_analysis.json` now includes a `permission_graph` section with:
- state-write sinks
- structural guard kinds inferred from Slither CFG/IR
- controller references linked to those guards

This is still an early source-level pass. It does not yet resolve live controller
addresses from chain state or provide bytecode fallback when source-level
structure is missing.

CLI flags:
- `--no-deps` skips all dependency discovery
- `--deps-rpc <url>` uses a specific RPC for static dependency discovery
- `--no-dynamic-deps` skips dynamic dependency discovery
- `--dynamic-rpc <url>` uses a tracing-enabled RPC for dynamic dependency discovery
- `--dynamic-tx-limit <n>` traces up to `n` representative transactions
- `--dynamic-tx-hash <hash>` traces an explicit transaction hash (repeatable)
- `--discover <name>` looks up one contract by name via Blockscout
- `--discover-ai <company> <contract_name>` finds one contract via Tavily + LLM domain/page selection
- `--discover-inventory <company_or_domain>` finds an official published protocol contract inventory via Tavily + LLM page selection
- `--discover-limit <n>` limits discovery output; default is `10`, or `100` for `--discover-inventory`

Output directories:
- contract analysis pipeline results: `contracts/`
- `--discover` / `--discover-ai`: `contracts/<name>/discovery.json`
- `--discover-inventory`: `protocols/<name>/contract_inventory.json`

## Development (uv)

1. Install dependencies and create `.venv`:
   `uv sync`
2. Configure environment:
   `cp .env.example .env` and set `ETHERSCAN_API_KEY`, `ETH_RPC`, `NVIDIA_API_KEY`, `TAVILY_API_KEY`, `OPEN_ROUTER_KEY`
3. Run the pipeline:
   `uv run python main.py --help`
4. Run tests (excluding live RPC tests):
   `uv run pytest -k "not live"`

## HyperSync Policy Backfill

For authority contracts with `tracked_policies` in `control_tracking_plan.json`, you can
backfill policy events and reconstruct a current `policy_state.json` from HyperSync:

`uv run python services/hypersync_backfill.py contracts/<name>/control_tracking_plan.json --url https://eth.hypersync.xyz`

This requires `ENVIO_API_TOKEN` or `--token <token>`.
