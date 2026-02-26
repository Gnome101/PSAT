# PSAT

This is the main repo for the protocol security assesment tool.

The goal is for this repo is to collect information about protocols and analyze the using SOTA LLMs. 

The services directory contains the exisiting services on the pipeline.

The utils directory contains any important API or utility that the services might call.

Currently, addresses are fed into the pipeline via `addresses.json` and results are dumped under `contracts/`.

Each run now attempts:
- static dependent-contract discovery (`dependencies.json`)
- dynamic trace-based dependency discovery (`dynamic_dependencies.json`)

CLI flags:
- `--no-deps` skips all dependency discovery
- `--deps-rpc <url>` uses a specific RPC for static dependency discovery
- `--no-dynamic-deps` skips dynamic dependency discovery
- `--dynamic-rpc <url>` uses a tracing-enabled RPC for dynamic dependency discovery
- `--dynamic-tx-limit <n>` traces up to `n` representative transactions
- `--dynamic-tx-hash <hash>` traces an explicit transaction hash (repeatable)

## Development (uv)

1. Install dependencies and create `.venv`:
   `uv sync`
2. Configure environment:
   `cp .env.example .env` and set `ETHERSCAN_API_KEY`, `ETH_RPC`, `NVIDIA_API_KEY`
3. Run the pipeline:
   `uv run python main.py --help`
4. Run tests (excluding live RPC tests):
   `uv run pytest -k "not live"`
