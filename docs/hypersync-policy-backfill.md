# HyperSync Policy Backfill

This note documents the manual HyperSync workflow for reconstructing
table-backed authority policy from emitted events.

## Goal

Some authority contracts, such as `RolesAuthority`, store permission state in
non-enumerable mappings:

- `getUserRoles[user]`
- `isCapabilityPublic[target][functionSig]`
- `getRolesWithCapability[target][functionSig]`

For those contracts, polling alone cannot recover the full current policy set.
Instead, replay the emitted policy-update events from deployment:

- `UserRoleUpdated`
- `PublicCapabilityUpdated`
- `RoleCapabilityUpdated`

## Command

Run the HyperSync backfill against a generated `control_tracking_plan.json`:

```bash
ENVIO_API_TOKEN=<token> \
uv run python services/hypersync_backfill.py \
  contracts/<name>/control_tracking_plan.json \
  --url https://eth.hypersync.xyz
```

## Outputs

The command writes two files next to the control tracking plan:

- `policy_event_history.jsonl`
  - raw historical policy events, already decoded into structured fields
- `policy_state.json`
  - reconstructed latest-known state for:
    - public capabilities
    - role capabilities
    - user roles

## Current Example

For the manually analyzed authority contract:

- contract: `0x485bde66bb668a51f2372e34e45b1c6226798122`
- plan: `contracts/authorityManual/control_tracking_plan.json`

the backfill produced:

- `contracts/authorityManual/policy_event_history.jsonl`
- `contracts/authorityManual/policy_state.json`

These artifacts can then be joined with the static permission graph of the
protected contract to answer questions like:

- which selectors on the protected contract are role-gated
- which roles currently have those capabilities
- which users currently hold those roles

## Boundary

This is historical reconstruction from event logs.

It works well when:

- static analysis shows the policy state is mutated through explicit writer
  functions
- those writers emit deterministic events
- the contract is not mutating the same policy state silently through some other
  path

If a custom authority contract mutates policy without emitting events, this
backfill will be incomplete and polling/manual review is still required.
