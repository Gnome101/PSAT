# PSAT

This is the main repo for the protocol security assesment tool.

The goal is for this repo is to collect information about protocols and analyze the using SOTA LLMs. 

The services directory contains the exisiting services on the pipeline.

The utils directory contains any important API or utility that the services might call.

Currently, addresses are fed into the pipeline via `addresses.json` and results are dumped under `contracts/`.

Each run performs:
- Slither static analysis
- Privilege & access control analysis (`privilege_analysis.json`)
- Pausability analysis (`pausability_analysis.json`)
- Timelock analysis (`timelock_analysis.json`)
- LLM flow analysis (`llm_analysis.md`)

CLI flags:
- `--no-llm` skips LLM analysis
- `--no-privilege` skips privilege analysis
- `--no-pausability` skips pausability analysis
- `--no-timelock` skips timelock analysis

## Development (uv)

1. Install dependencies and create `.venv`:
   `uv sync`
2. Configure environment:
   `cp .env.example .env` and set `ETHERSCAN_API_KEY`, `ETH_RPC`, `NVIDIA_API_KEY`
3. Run the pipeline:
   `uv run python main.py --help`
4. Run tests (excluding live RPC tests):
   `uv run pytest -k "not live"`
