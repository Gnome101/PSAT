# PSAT

This is the main repo for the protocol security assesment tool.

The goal is for this repo is to collect information about protocols and analyze the using SOTA LLMs. 

The services directory contains the exisiting services on the pipeline.

The utils directory contains any important API or utility that the services might call.

Currently, addresses are fed into the pipeline via `addresses.json` and results are dumped under `contracts/`.

Each run now also attempts static dependent-contract discovery and writes `dependencies.json` under each contract directory.

CLI flags:
- `--no-deps` skips dependency discovery
- `--deps-rpc <url>` uses a specific RPC for dependency discovery
