# Predicate Pipeline v2 Cleanup

Scope: `feat/predicate-pipeline-v2`.

The PR is intended to include the full v1-to-v2 cutover. Static should emit
`contract_analysis`, `predicate_trees`, and `effects`; policy/resolution should
consume semantic v2 artifacts instead of v1 shims or name-based authority
heuristics.

## P0 - Semantic Correctness

1. Done: replace role discovery based on `_looks_like_role_identifier_name`.
   - Current hot spots:
     - `services/static/contract_analysis_pipeline/shared.py`
     - `services/static/contract_analysis_pipeline/summaries.py`
     - `services/static/contract_analysis_pipeline/tracking.py`
   - Implemented:
     - Discover role identifiers from structure: bytes32 constants used as role
       keys, key sources in `hasRole(bytes32,address)` selector leaves, event
       topics, or resolved constant values.
     - Keep names only as labels for already-semantic discoveries.

2. Done: remove role-key hashing from names in `AccessControlAdapter`.
   - Current hot spot:
     - `services/resolution/adapters/access_control.py`
   - Implemented:
     - Prefer literal bytes32 constants or observed `RoleGranted` /
       `RoleRevoked` rows.
     - Do not derive role bytes by hashing a callee or state-variable name.

3. Done for the active static path: replace owner/admin/modifier/inheritance pattern labels.
   - Current hot spot:
     - `services/static/contract_analysis_pipeline/summaries.py`
   - Implemented:
     - Derive `pattern` from predicate leaf kinds, authority roles, set
       descriptors, controller tracking kinds, and effects.
     - Treat owner/admin names as compatibility display fields only until the
       schema is simplified.

## P1 - Compatibility Bridges

1. Done for the live v2 path: stop treating `access_control.privileged_functions`
   as an input to effective permissions.
   - Function records are built directly from `capability_resolver_output`
     joined with `effects.functions` whenever v2 inputs are present.
   - `access_control.privileged_functions` remains only as a compatibility
     source for old fixtures/read paths that do not pass v2 artifacts.

2. Done: remove `_write_compatibility_principal_rows()`.
   - `FunctionPrincipal` rows are now emitted only from resolver-native
     capability expressions.
   - Old `direct_owner`, `controllers`, and `authority_roles` read-model
     fields may still ride through `EffectiveFunction`, but they no longer
     backfill principal rows.

3. Done: remove the policy-worker sink bridge / external-call-guard bridge.
   - The worker no longer re-fetches `EffectiveFunction` rows to append
     principals from `sinks` or `external_call_guards`.
   - Anything not expressed by the capability resolver remains in
     `capability_expr` / status fields instead of being projected by worker
     compatibility code.
   - `effective_permissions` no longer copies `sinks` or
     `external_call_guards`; those stay in the v2 `effects` /
     `contract_analysis` artifacts.

## P2 - PR Polish

1. Rename misleading `v1`, `legacy`, `cutover`, and `shim` comments in live v2
   modules. Keep legitimate historical references in unrelated proxy, migration,
   and test fixtures.

2. Mark compatibility read-model fields explicitly or remove their consumers:
   `guards`, `guard_kinds`, `controller_refs`, `sink_ids`, and
   `access_control.privileged_functions`.

3. Split the final diff into reviewable commits after cleanup:
   - artifact/writer cutover
   - type/lint/test fixes
   - selector/signature semantic fixes
   - remaining semantic-only cleanup

## Verification Gate

Run after each semantic cleanup:

```bash
uv run ruff check
uv run pyright
git diff --check
TEST_DATABASE_URL=postgresql://127.0.0.1:9/psat_unavailable uv run pytest tests/test_effects.py tests/test_effective_permissions_v2.py tests/test_b1_field_propagation.py tests/test_controller_tracking_v2.py tests/test_detect_access_control_v2.py tests/test_reentrancy_pause.py -q
TEST_DATABASE_URL=postgresql://127.0.0.1:9/psat_unavailable uv run pytest tests/test_effective_permissions.py -q
```

Run `tests/test_capability_resolver.py` against a fresh temporary Postgres
database before calling the branch PR-ready.
