# Control Tracking Runtime Plan

This document captures the intended runtime architecture for controller recursion and live tracking.
It is intentionally a design note only. For now, recursive analysis is run manually.

## Goal

Starting from a contract's static analysis output:

- resolve live controllers
- track controller changes with `wss logs + polling fallback`
- recursively analyze controller contracts when a controller resolves to another contract
- eventually emit semantic alerts such as:
  - `owner_changed`
  - `authority_changed`
  - `safe_owners_changed`
  - `safe_threshold_changed`
  - `timelock_delay_changed`
  - `implementation_changed`
  - `permission_model_changed`

## Current State

Already implemented in this repo:

- static contract analysis and permission graph
- controller tracking metadata in `contract_analysis.json`
- runtime watch-plan compiler in `control_tracking_plan.json`
- one-shot / long-running tracker skeleton with:
  - `wss` event subscriptions
  - polling reconciliation
  - controller classification

Not implemented yet:

- recursive controller analysis orchestration
- persistent queue
- persistent DB-backed state graph
- notification service

## Proposed Runtime Architecture

Long-term services:

1. `api/orchestrator`
- accepts seed contracts to track
- exposes status and manual enqueue actions

2. `analyzer-worker`
- fetches verified source
- runs Slither/static analysis
- writes `contract_analysis.json`
- writes `control_tracking_plan.json`

3. `tracker-worker`
- subscribes to control-change events over `wss`
- runs polling fallback and reconciliation
- writes snapshots and change events

4. `resolver-worker`
- resolves and classifies controller addresses
- decides whether a controller should be recursively analyzed
- enqueues controller contracts

5. `queue`
- Redis-backed job queue is the simplest starting point

6. `database`
- stores contracts, controller graph, snapshots, and change events

## Queue / Job Model

Recommended job types:

- `analyze_contract`
  - input: `chain`, `address`
- `build_watch_plan`
  - input: `analysis_path` or `contract_id`
- `track_contract`
  - input: `contract_id`
- `resolve_controller`
  - input: `parent_contract`, `controller_address`, `depth`
- `reanalyze_contract`
  - input: `contract_id`, `reason`

Required controls:

- dedupe by `(chain, address)`
- recursion depth limit
- visited-address set
- retries with backoff
- dead-letter handling for persistent failures

## Recursion Strategy

When a controller resolves to:

- `zero`
  - stop recursion
- `eoa`
  - stop recursion
- `safe`
  - resolve owners + threshold, then stop unless one of those controllers is another contract model we want to recurse into later
- `timelock`
  - resolve delay and any controlling address/role contract, then recurse if that controlling address is another contract
- `proxy_admin`
  - resolve owner/admin if available, recurse if that is another contract
- `contract`
  - enqueue a new manual or queued analysis for that controller contract

## Event-First Tracking Model

For each tracked controller:

- if deterministic associated events exist:
  - subscribe to those events via `wss`
  - on event, confirm current state by RPC read
- always keep polling reconciliation
- if no deterministic event exists:
  - use polling only

This means the runtime should treat:

- events as low-latency triggers
- polling as current-truth confirmation and missed-event recovery

## Recursive Resolution

Recursion is now handled automatically inside the worker pipeline by
`services/resolution/recursive.py`, which is driven by
`workers/resolution_worker.py`. Starting from a seed contract, the
resolver walks each controller with `resolved_type: contract`, scaffolds
and analyzes it, and continues until the chain of control reaches one of:

- `eoa`
- `zero`
- `safe`
- `timelock`
- `proxy_admin`
- or a custom contract boundary requiring deeper analysis

## Immediate Next Step

Keep testing with the current manual workflow and use the runtime artifacts as the source of truth:

- `contract_analysis.json`
- `control_tracking_plan.json`
- `control_snapshot.json`
- `control_change_events.jsonl`

When the manual workflow is stable, containerize the workers and put recursion behind a queue.
