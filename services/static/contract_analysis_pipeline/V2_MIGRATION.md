# V2 migration tracker

Reference for **what to delete in #17** once the schema-v2 cutover (#18) is
signed off and v1 readers are gone. Every entry here is a v1 path that has
a v2 equivalent in production today (artifact + repo + adapter), kept alive
only because at least one v1 consumer hasn't moved yet.

The format per row is:

| v1 surface | What it produces | v2 replacement | Blocked by |
|---|---|---|---|

## Static stage outputs

| v1 surface | What it produces | v2 replacement | Blocked by |
|---|---|---|---|
| `services/static/contract_analysis_pipeline/semantic_guards.py:build_semantic_guards` | `semantic_guards.json` artifact (per-function `guards: list[str]` heuristic labels) | `predicate_trees.json` artifact (typed `PredicateTree` per externally-callable function) + `resolve_contract_capabilities` for the resolved view | `workers/policy_worker.py:348` reads `semantic_guards` to drive `services/policy/effective_permissions.py`; needs to switch to `predicate_trees` first |
| `services/static/contract_analysis_pipeline/summaries.py:_internal_auth_calls` (~L124) | Name-heuristic detection of internal calls that look like auth (require/onlyOwner/...) | `RevertDetector` cross-fn recursion in `predicates.py` walks helper bodies structurally | direct call from `_infer_function_guards` (~L137); switch via predicate_trees consumption in policy stage |
| `services/static/contract_analysis_pipeline/summaries.py:_build_method_to_role_map` name path (~L847) | Maps each method to a role name based on guard labels | v2's `PredicateTree.role_domain` + `RoleGrantsRepo.list_observed_roles` produce structured role membership | called from `_detect_access_control` (~L905); v2 equivalent is the `v2_capabilities` field exposed by `/api/analyses/{run}` |
| `services/static/contract_analysis_pipeline/summaries.py:_detect_access_control` inheritance check (~L868) | Inheritance-based AC detection (looks for `AccessControl` in the contract's parent list) | `services/resolution/adapters/access_control.py:AccessControlAdapter.matches` scores by `RoleGranted` topic / `hasRole` selector / role_domain — structural, no inheritance check | same call path as above |
| `services/static/contract_analysis_pipeline/summaries.py:_looks_like_access_guard` / `_looks_like_pause_guard` (~L53/L58) | Modifier-name heuristics ("onlyOwner", "whenNotPaused", ...) | `apply_writer_gate_pass` + `apply_reentrancy_pause_pass` pin authority_role from the structural shape, name-free | inline calls at L172, L1068, L1075 |
| `services/static/contract_analysis_pipeline/caller_sinks.py:_classify_caller_*` (~L262, L386) | Name-heuristic classification of caller-sinks (msg.sender, owner, role-holders) | `predicates.py` Operand source taxonomy + `_classify_authority_equality` / `_classify_authority_membership` produce typed leaf | tracking_plan / policy_worker via `caller_sinks` |
| `services/static/contract_analysis_pipeline/caller_sinks.py` helper-name seeds (~L37) | List of recognized OZ/DSAuth/Aragon helper names | `services/resolution/adapters/*.py` adapters detect via selector + topic, no name match | same |
| `services/static/contract_analysis_pipeline/graph.py:_looks_like_external_authority_call` (~L93) | Name-heuristic for "is this external call an authority check" | `predicates._build_external_bool_leaf` + AC/Aragon adapters produce typed `external_bool` / `delegated_authority` | called from `build_permission_graph` (~L527) which feeds `summaries._detect_access_control` |
| `services/static/contract_analysis_pipeline/graph.py:checkCallerIsX` (~L708) | Name-pattern match for caller-check helpers | `predicates.py`'s structural caller-source detection | same |

## Resolution stage

| v1 surface | What it produces | v2 replacement | Blocked by |
|---|---|---|---|
| `services/resolution/controller_adapters.py` probe ladder (~L82, L218, L374) | Per-adapter detection ladder mixed into one module | Per-adapter `matches()` methods on `AccessControlAdapter` / `SafeAdapter` / `AragonACLAdapter` / `DSAuthAdapter` / `EIP1271Adapter` | direct call from `services/resolution/recursive.py` |
| `services/resolution/recursive.py` Safe/Timelock/ProxyAdmin nested-principal switch (~L713) | Hardcoded type-based dispatch on resolved-principal kinds | `AdapterRegistry.pick(descriptor, ctx)` scores adapters generically | the `recursive.py` resolver consumed by `policy_worker` |
| `services/resolution/tracking.py:role_identifier` (~L447) | Role-name heuristic for tracking | `RoleGrantsRepo.list_observed_roles` returns structured role bytes | tracking_plan stage |
| `services/resolution/tracking.py` Safe/Timelock probes (~L178, L343) | RPC-call probe ladder per type | `RpcSafeRepo` / role_grants RPC layer | same |

## Workers / stages

| v1 surface | What it produces | v2 replacement | Blocked by |
|---|---|---|---|
| `workers/policy_worker.py:348` reads `semantic_guards` | Drives `effective_permissions` calculation | `predicate_trees` + `v2_capabilities` already on the artifact set | needs `effective_permissions.py` to accept v2 inputs (deferred — biggest single blocker) |
| `services/policy/effective_permissions.py:_semantic_guards_by_function` (~L292) | Folds semantic_guards into per-function permission state | `resolve_contract_capabilities` already produces the per-function shape; `effective_permissions` would consume that instead | upstream switch in policy_worker |

## API / UI

| v1 surface | What it produces | v2 replacement | Blocked by |
|---|---|---|---|
| `/api/company/{name}` `controllers` / `principals` rendering | v1 inferred controllers per function | `/api/company/{name}/v2_capabilities` returns typed `CapabilityExpr` per fn | `site/src/ProtocolSurface.jsx` consumer migration (frontend task) |
| `/api/analyses/{run_name}` `access_control.privileged_functions` | v1 privileged_functions list with guard labels | Same endpoint now also includes `v2_capabilities` field — UI can read either | UI migration |
| `site/src/ProtocolSurface.jsx:374 ACCESS_CONTROL_HINTS` | Hardcoded UI label table | `v2_capabilities` is schema-driven — per-role expansion comes from the typed kind/membership_quality fields | UI rewrite (#15) |

## Deletion checkpoints

The wholesale deletion in #17 unlocks once:

1. `policy_worker` consumes `predicate_trees` instead of `semantic_guards` — the single biggest blocker. After this `semantic_guards.py` deletes cleanly.
2. UI code paths in `site/src/ProtocolSurface.jsx` / `auditUi.jsx` migrate from v1 controllers/principals to `v2_capabilities`.
3. `cutover_dry_run.py` reports `regression == 0` across the entire production fleet.

Until those land, every entry above stays in service. The v2 paths run in parallel today so consumers can migrate at their own pace; the cutover is choosing when to pull the v1 backstop, not whether to.
