# Generic predicate-based access analysis — v3 (post-round-2 review)

This revision folds every finding from codex's two prior reviews. Diffs vs
v2 are at the bottom.

## Goals

1. Static stage emits structural predicates only — no name-based admission.
2. Predicates are admitted iff they have **caller-dependent or delegated-authority** semantics. Reentrancy, pause, time, and business invariants annotate but never admit.
3. Resolver evaluates **capability expressions** (finite sets, threshold groups, cofinite blacklists, signature witnesses, external check-only, conditional-universal, AND/OR) — not flat principal sets.
4. Standard ABI knowledge lives in adapters behind one interface that gets full evaluation context.
5. No backwards compatibility. Single schema-v2 cutover.
6. Every unsupported pattern surfaces explicitly — never silently degraded to "Unresolved" or to a wrong principal.

## Predicate IR

### Boolean tree

```python
class PredicateTree(TypedDict):
    op: Literal["AND", "OR", "LEAF"]
    children: list[PredicateTree] | None
    leaf: LeafPredicate | None
```

`NOT` does not exist as a tree node. Negation is captured via the
`operator` field on each leaf (next section).

### Polarity normalization (codex round-2 #3 fix)

`operator` is its own field, decoupled from `kind`:

```python
class LeafPredicate(TypedDict):
    kind: Literal[
        "membership", "equality", "comparison",
        "external_bool", "signature_auth", "unsupported",
    ]
    operator: Literal[
        "eq", "ne",                   # equality / inequality
        "lt", "lte", "gt", "gte",     # comparison
        "truthy", "falsy",            # membership / external_bool / unary boolean
    ]
    authority_role: Literal[
        "caller_authority", "delegated_authority",
        "time", "reentrancy", "pause", "business",
    ]
    operands: list[Operand]
    set_descriptor: SetDescriptor | None      # only for membership
    unsupported_reason: str | None            # only for unsupported
    references_msg_sender: bool
    parameter_indices: list[int]
    expression: str
    basis: list[str]
```

Normalization rules (applied during predicate building, before evaluator
ever sees a tree):

| Source pattern | Normalized leaf |
|---|---|
| `require(a == b)` | `equality, op=eq` |
| `require(a != b)` | `equality, op=ne` |
| `if (a == b) revert` | `equality, op=ne` (the *allowed* condition, not the revert condition) |
| `if (a != b) revert` | `equality, op=eq` |
| `require(!(a == b))` | `equality, op=ne` |
| `if (!(a == b)) revert` | `equality, op=eq` |
| `require(map[k][m])` | `membership, op=truthy` |
| `require(!map[k][m])` | `membership, op=falsy` |
| `if (map[k][m]) revert` | `membership, op=falsy` |
| `if (!map[k][m]) revert` | `membership, op=truthy` |
| `require(authority.canCall(...))` | `external_bool, op=truthy` |
| `require(a > b)` | `comparison, op=gt` |
| `if (a > b) revert` | `comparison, op=lte` |
| `require(_status != _ENTERED)` | `equality, op=ne, authority_role=reentrancy` |

**The rule is: extract the allowed condition.** `require(C)` says "allowed if
C." `if (R) revert` says "allowed if NOT R." All NOTs are pushed into the
operator before emission. After normalization, no `NOT` survives anywhere
in the tree or in the leaves.

### Operand provenance engine (codex round-2 #1 fix)

v2 said "extend MsgSenderTaint." That underestimates the lift. The
existing `MsgSenderTaint` at `caller_sinks.py:86` is single-pass
assignment/type-conversion/Phi taint plus a few helper-name seeds. The
generic predicate pipeline needs a real **operand provenance engine**:

```python
class ProvenanceEngine:
    """Builds a def-use forest over Slither IR for one function context.
    Tracks every value's source: parameter, msg.sender, tx.origin,
    state-var, constant, computed, view-call return, external-call
    return, signature-recovery output, block-context.

    IR opcodes covered:
      - Assignment, TypeConversion, Phi (existing MsgSenderTaint)
      - Binary, Unary (NEW: operand decomposition)
      - Index, Member (NEW: structured access)
      - HighLevelCall, InternalCall, LibraryCall, LowLevelCall (NEW: returns + arg taint)
      - Send/Transfer (NEW)
      - SolidityCall (NEW: ecrecover, keccak256, abi.* — these go to "computed" or "signature_recovery")
      - Return (NEW: callees recurse on return value)
      - NewContract / NewArray / NewElementaryType (NEW: "constructed" source)
    """
```

Built from scratch in `services/static/contract_analysis_pipeline/provenance.py`,
~400 lines. Reuses Slither IR types, doesn't reuse `MsgSenderTaint`'s
heuristic seeds (signature-helper name list at `caller_sinks.py:37` is
deleted). Tested with fixtures for each IR opcode plus composite
patterns (`address signer = ECDSA.recover(hash, sig); require(signer == owner);`
must classify `signer`'s source as `signature_recovery`, not `view_call`).

### authority_role classification (codex round-2 #1, again)

Reentrancy and pause are NOT `mapping_events.py`-style patterns. They
need their own analyzers:

```python
class AuthorityClassifier:
    """Per-leaf classification using ProvenanceEngine output.

    caller_authority: any operand traces (transitively, via the
        provenance engine) to msg_sender, tx_origin, or
        signature_recovery.
    delegated_authority: any operand traces to an external_call
        whose target is read from a state_variable AND whose call
        args include msg_sender or signature_recovery.
    time: every varying operand is block_context (timestamp/number).
    reentrancy: leaf reads a state-var that ReentrancyAnalyzer flagged
        as a guard variable. Independent analyzer; see below.
    pause: leaf reads a state-var that PauseAnalyzer flagged. Independent.
    business: residual.

    UNCLASSIFIED → kind="unsupported", reason="authority_role_uninferred",
        authority_role="business" (so doesn't admit).
    """

class ReentrancyAnalyzer:
    """Identifies reentrancy-status state-vars by control-flow:
       - State variable, uint8/bool/uint256
       - Has exactly two writes per gated function: one before any external
         call (transition to ENTERED), one after (transition to NOT_ENTERED).
       - The gated function reads that state-var and reverts on ENTERED.
       Output: set of state-var names → mark leaves reading those as reentrancy.
    """

class PauseAnalyzer:
    """Identifies pause-status state-vars by control-flow:
       - Bool state-var.
       - Has at least one writer function gated by an existing privileged
         predicate (the pauser).
       - Other functions read the bool and revert when true.
       Output: set of state-var names → mark leaves reading those as pause.
    """
```

Both analyzers are pure-structural. Neither uses the words "ReentrancyGuard"
or "Pausable" or any name match. Each has explicit fixtures including
renamed-equivalent contracts in the test corpus.

### Modifier parameter substitution (codex round-2 #2 fix)

v2 said "use the existing template." Wrong — the existing modifier walker
at `caller_sinks.py:600` doesn't bind invocation args to modifier
parameters at all. v3 introduces:

```python
class ParameterBindingEnv:
    """Maps each modifier (or callee) parameter name to the IR variable
    bound at the invocation site. Used during predicate building so that
    `gate(BREAK_GLASS)` inside the body of `modifier gate(bytes32 r)`
    binds `r` to the constant `BREAK_GLASS` for that invocation.

    For dynamic invocation (`gate(roleArg)` where roleArg is a parameter),
    the binding maps `r` to the function's parameter — preserving param
    indexing through the substitution.
    """
```

Built in week 1. Test fixtures:
1. **Constant binding**: `function f() onlyRole(BREAK_GLASS) { ... }` —
   predicate must reference `BREAK_GLASS`, not symbol `r`.
2. **Dynamic binding**: `function f(bytes32 roleArg) onlyRole(roleArg)
   { ... }` — predicate must report `parameter_index=0`, not the local
   modifier name.
3. **Library call binding**: same pattern but via internal library
   `LibAccess.requireAccess(roleArg)`.
4. **Two modifiers chained**: `function f() onlyRole(A) onlyRole(B)
   { ... }` — predicate is AND of both.

### unsupported is first-class

Same as v2: `kind="unsupported"` is a real leaf, with `unsupported_reason`
field and propagation rules. Fail-closed under AND, preserved under OR.

### SetDescriptor — fat (codex round-2 #6 fix on covering index)

```python
class SetDescriptor(TypedDict):
    kind: Literal[
        "mapping_membership",
        "array_contains",
        "external_set",
        "bitwise_role_flag",
        "diamond_facet_acl",
    ]
    storage_var: str | None
    storage_slot: str | None
    key_sources: list[Operand]
    truthy_value: str | None
    enumeration_hint: list[EventHint]
    authority_contract: AuthorityContract | None
    role_domain: RoleDomain | None
    selector_context: SelectorContext | None
```

The `bitwise_role_flag` and `diamond_facet_acl` kinds **only ever appear
inside `unsupported` leaves' descriptors** in v3.0 — they describe what
was found structurally so the adapter can later support them, but they
never drive admission decisions. (Per codex's correct flag at the end of
round-2: keep pattern labels behind adapters / unsupported, never as
admission criteria.)

### EventHint

Same as v2.

### RoleDomain seeding & recursion (codex round-2 #5 fix)

```python
class RoleDomain(TypedDict):
    parameter_index: int
    auto_seed_default_admin: bool   # NEW: AC-shaped descriptors must be True
    sources: list[Literal[
        "compile_time_constants",
        "role_granted_history",
        "abi_declared",
        "manual_pinned",
    ]]
    recursive_role_admin_expansion: bool   # NEW: walk getRoleAdmin to fixed point
```

Resolver-side expansion algorithm:

```python
def expand_role_domain(descriptor: SetDescriptor, ctx) -> set[bytes32]:
    domain: set[bytes32] = set()
    if descriptor.role_domain.auto_seed_default_admin:
        domain.add(b"\x00" * 32)   # DEFAULT_ADMIN_ROLE
    for source in descriptor.role_domain.sources:
        domain |= source_query(source, descriptor, ctx)
    if descriptor.role_domain.recursive_role_admin_expansion:
        # Walk getRoleAdmin(role) for every role in domain to a fixed point.
        # Captures roles that admin other roles but were never directly granted.
        frontier = set(domain)
        while frontier:
            new = set()
            for role in frontier:
                admin = adapter_call("getRoleAdmin(bytes32)", [role], ctx)
                if admin is not None and admin not in domain:
                    new.add(admin)
            domain |= new
            frontier = new
    return domain
```

`auto_seed_default_admin` is True for any descriptor whose
`enumeration_hint` includes the OZ `RoleGranted` topic0
(`0x2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d`)
or whose ABI declares `getRoleAdmin(bytes32)`. Discovered structurally
from the artifacts. No name match.

## Capability expression — confidence-aware (codex round-2 #4 fix)

```python
class CapabilityExpr:
    kind: Literal[
        "finite_set", "threshold_group", "cofinite_blacklist",
        "signature_witness", "external_check_only",
        "conditional_universal", "unsupported", "AND", "OR",
    ]
    members: list[str] | None             # always sorted, lowercased, deduped
    threshold: tuple[int, list[str]] | None
    blacklist: list[str] | None           # sorted/lowercased/deduped
    signer: 'CapabilityExpr | None'
    check: ExternalCheck | None
    conditions: list[Condition]           # NEW: side-conditions list
    unsupported_reason: str | None
    children: list['CapabilityExpr'] | None
    membership_quality: Literal["exact", "lower_bound", "upper_bound"]  # NEW
    confidence: Literal["enumerable", "partial", "check_only"]
    last_indexed_block: int | None
```

Two new fields (codex round-2 #4):
- `conditions: list[Condition]` — every capability carries a side-condition
  list (time gates, reentrancy, pause). `intersect(finite_set, conditional_universal)`
  produces `finite_set` with the time condition appended to `conditions`.
- `membership_quality: "exact" | "lower_bound" | "upper_bound"` — under
  partial confidence, set algebra yields lower-bound facts (∩) or
  upper-bound facts (∪). The evaluator never claims `exact` if any input
  was partial.

### Combinator definitions (closed)

```
intersect(finite_set_A, finite_set_B):
    if quality(A) == quality(B) == "exact":
        return finite_set(sort_lower_dedup(A ∩ B), quality="exact")
    if quality(A) == "exact" and quality(B) == "lower_bound":
        return finite_set(A ∩ B, quality="lower_bound")
    if both "lower_bound":
        return finite_set(A ∩ B, quality="lower_bound")
        # We have known-members of each; their intersection is known-in-both,
        # but there may be additional shared members on either side.
    if either is "upper_bound":
        return structural_AND(A, B)  # can't soundly combine

intersect(finite_set, conditional_universal):
    out = copy(finite_set)
    out.conditions.extend(conditional_universal.conditions)
    return out

intersect(threshold_group(M, signers), finite_set):
    return structural_AND(threshold_group, finite_set)
    # Could in principle restrict signers to signers ∩ finite_set, but that
    # changes M's semantics ambiguously. Defer to UI rendering.

intersect with cofinite_blacklist:
    if other side is finite_set: return finite_set(members - blacklist)
    else: return structural_AND

union(finite_set_A, finite_set_B):
    if quality(A) == quality(B) == "exact":
        return finite_set(A ∪ B, quality="exact")
    else:
        return finite_set(A ∪ B, quality="lower_bound")
        # Union of two lower-bound sets is itself a lower bound.

union(signature_witness, finite_set):
    return structural_OR(signature_witness, finite_set)

negate(finite_set, quality="exact"):
    return cofinite_blacklist(members)
negate(finite_set, quality="lower_bound" | "upper_bound"):
    return unsupported("negate_partial_set")
negate(threshold_group | signature_witness | external_check_only):
    return unsupported("negate_unsupported_capability")
negate(conditional_universal):
    return conditional_universal(condition=negate_condition(...))
negate(unsupported):
    return unsupported("negate_unsupported")
```

All capability operations are total functions — they never throw. When a
sound result can't be computed, they return `structural_AND` / `structural_OR`
/ `unsupported` and the UI renders the structure faithfully.

Member canonicalization: addresses are lowercased and sorted before any
set operation. Bytes32 role hashes likewise.

## Static stage implementation

### Files touched

| File | Change |
|---|---|
| `schemas/contract_analysis.py` | Replace `PrivilegedFunction.controller_refs/guards/sinks` with `predicate_tree`. Bump schema_version → "2.0". |
| `services/static/contract_analysis_pipeline/provenance.py` | **New.** ProvenanceEngine + def-use forest over Slither IR. |
| `services/static/contract_analysis_pipeline/predicates.py` | **New.** Predicate builder, polarity normalization, ParameterBindingEnv, AuthorityClassifier. |
| `services/static/contract_analysis_pipeline/reentrancy_pause.py` | **New.** ReentrancyAnalyzer + PauseAnalyzer (control-flow / state-transition). |
| `services/static/contract_analysis_pipeline/caller_sinks.py` | Delete `_classify_caller_*` (L262-305, L386-459), helper-name seeds (L37-54). MsgSenderTaint subsumed by ProvenanceEngine. |
| `services/static/contract_analysis_pipeline/summaries.py` | Delete `_internal_auth_calls` (L53-180), `_build_method_to_role_map` (L825-865), `_detect_access_control` (L868-904). Admission becomes: predicate tree contains `caller_authority` or `delegated_authority` leaf. |
| `services/static/contract_analysis_pipeline/semantic_guards.py` | **Delete.** |
| `services/static/contract_analysis_pipeline/graph.py` | Drop `_looks_like_external_authority_call` (L93-114), `checkCallerIsX` regex (L708-733). Keep bytes32 role aliasing (L210-320). |
| `services/static/contract_analysis_pipeline/mapping_events.py` | Extend output → `EventHint` records. |

### Edge-case revert detection (codex round-2 #8 fix)

```python
class RevertDetector:
    """Identifies all paths in a function that lead to a revert,
    plus the predicate guarding each path. Handles:

    1. require / assert / SolidityCall(revert) — already at caller_sinks.py:136
    2. if (cond) revert / if (cond) revert CustomError() — extends path tracking
    3. assembly { if iszero(x) { revert(0,0) } } — inline assembly conditional
    4. try external.call() catch { revert(); } — try/catch fallback gate
    5. State-stored function pointers: function p; require(p == expectedSig) —
       captured as `equality(operand=parameter, operand=state_variable)`
       leaf of unknown semantics → `unsupported("function_pointer_dispatch")`

    For (3), reuses Slither's `InlineAssemblyOperation` IR; for (4),
    walks try/catch successor blocks; for (5), provenance engine
    surfaces the function-pointer source.

    When control dependence cannot be recovered (rare — Yul with
    arbitrary jumps), emits `unsupported(reason="opaque_control_flow")`
    and admits the function with that leaf so it surfaces in UI rather
    than being silently treated as public.
    """
```

Test fixtures: one per case (1)-(5) above, plus a "fully opaque" Yul
fallback. Each fixture asserts the exact predicate emitted.

## Resolver stage implementation

### Files touched

Same as v2 minus rewordings:

| File | Change |
|---|---|
| `services/resolution/predicates.py` | **New.** Capability evaluator. |
| `services/resolution/capabilities.py` | **New.** CapabilityExpr + total-function combinators. |
| `services/resolution/adapters/__init__.py` | **New.** SetAdapter Protocol + AdapterRegistry + EvaluationContext. |
| `services/resolution/adapters/access_control.py` | OZ AccessControl + Enumerable. |
| `services/resolution/adapters/safe.py` | Returns threshold_group. |
| `services/resolution/adapters/dsauth_aragon.py` | Authority-contract resolution + ACL log replay. |
| `services/resolution/adapters/event_indexed.py` | Generic event-replay over `enumeration_hint`. |
| `services/resolution/adapters/eip1271.py` | Returns external_check_only with API route to issue the probe. |
| `services/resolution/recursive.py` | Strip Safe/Timelock/ProxyAdmin switch (L713-805). |
| `services/resolution/tracking.py` | Strip role_identifier (L447-495), Safe/Timelock probes (L178-186, L343-375). |
| `services/resolution/controller_adapters.py` | Probe ladder moves to per-adapter `detect()`. |
| `routers/contracts.py` | **New endpoint** `POST /api/contract/<addr>/check_membership` (codex round-2 #9). Body: `{predicate_index, descriptor, member}`. Returns `{trit: yes|no|unknown, evidence}`. Used by UI's external_check_only "query by address." |

### SetAdapter contract (codex round-2 #9 fallback)

```python
class SetAdapter(Protocol):
    @classmethod
    def matches(cls, descriptor: SetDescriptor, ctx: EvaluationContext) -> int:
        """0–100. 0 means definitely not this adapter. 100 means definitely.
        Adapters returning >0 must be able to call enumerate() or
        membership() correctly; partial coverage adapters return 50–80."""

    def enumerate(self, descriptor, ctx) -> EnumerationResult: ...
    def membership(self, descriptor, ctx, member: Address) -> Trit: ...
```

Resolver picks max-scoring adapter; ties broken by registration order;
score-0 means **`unsupported("no_adapter")`**, never `external_check_only`.

`external_check_only` is a result kind, not a fallback — it is returned
**only** when an adapter actually implements `membership()` against a live
backend (`eip1271.py` does this by issuing `isValidSignature(hash,sig)`
calls; `dsauth_aragon.py` does it by issuing `canCall(...)`).

When the result is `external_check_only`, the API route
`/api/contract/<addr>/check_membership` accepts a probe address, calls
the adapter's `membership()`, and returns a `Trit`. UI wires this to a
text input + button + result badge in the auth panel.

### role_grants_events (codex round-2 #6 fix on covering index)

```sql
CREATE TABLE role_grants_events (
    chain_id    INTEGER NOT NULL,
    contract_id INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    role        BYTEA   NOT NULL,
    member      VARCHAR(42) NOT NULL,
    direction   VARCHAR(8)  NOT NULL CHECK (direction IN ('grant', 'revoke')),
    block_number  BIGINT  NOT NULL,
    block_hash    BYTEA   NOT NULL,
    tx_hash       BYTEA   NOT NULL,
    log_index     INTEGER NOT NULL,
    transaction_index INTEGER NOT NULL,   -- NEW (codex round-2 #6)
    PRIMARY KEY (chain_id, contract_id, tx_hash, log_index)
);
-- Covering index for the current-state query (codex round-2 #6):
CREATE INDEX role_grants_events_state ON role_grants_events
    (chain_id, contract_id, role, member, block_number DESC, log_index DESC, direction);

CREATE TABLE role_grants_cursor (...);
CREATE TABLE chain_finality_config (...);
```

The role-grants indexer is **independent** of `unified_watcher` /
`monitored_events`. v2 implied possible reuse; v3 makes them separate.
Reasons: `monitored_events` lacks `log_index` and dedupes by `(mc, tx,
block, type)` (`unified_watcher.py:197`), neither of which is sufficient
for our needs.

The `role_grants_events` writer takes a Postgres advisory lock per
`(chain_id, contract_id)`, holds `FOR UPDATE` on `role_grants_cursor`,
and rewinds-deletes-replays the confirmation window atomically. Reorg
detection by `block_hash` mismatch.

The current-state view:

```sql
CREATE VIEW role_grants_current AS
SELECT DISTINCT ON (chain_id, contract_id, role, member)
       chain_id, contract_id, role, member, direction, block_number, log_index
FROM role_grants_events
ORDER BY chain_id, contract_id, role, member, block_number DESC, log_index DESC;
```

Filtered to `direction='grant'` for the active set. The covering index
above makes this index-only.

## UI changes

| Capability | UI |
|---|---|
| `finite_set, exact` | List of addresses |
| `finite_set, lower_bound` | "Known: …; may have additional members" |
| `finite_set, upper_bound` | "At most: …; some may not currently hold" |
| `threshold_group(M, signers)` | "M-of-N" badge; expandable signer list |
| `cofinite_blacklist` | "Anyone except …" |
| `signature_witness(signer_expr)` | "Anyone with a signature from \<expr\>" |
| `external_check_only` | "Membership-checkable" + address-input form (calls new API route) |
| `conditional_universal(time/reentrancy/pause)` | "Anyone, while \<condition\>" |
| `unsupported(reason)` | "Pattern not yet supported: \<reason\>" + raw expression block |
| `AND/OR` structural | Expression tree |
| Empty predicate tree | "Public" |
| `conditions` non-empty on any leaf | Side-condition badge per condition |

Conditions render as small chips next to the principal: "after T",
"unless reentered", "while not paused".

## Migration — single schema-v2 cutover

Same as v2: one branch lands schema + emitter + evaluator + adapters +
UI together. role_grants indexer ships as a worker addition.

### Week-0 measurement (codex round-2 #7 fix)

Before any code is written, run:

```bash
scripts/measure_reprocess_cost.py \
    --table-stats \
    --slither-times-from stage_timing_static \
    --object-storage-bytes \
    --concurrency-projection N=1,4,8,16
```

Outputs:
- Total contract count
- p50 / p95 / max Slither runtime per contract from
  `stage_timing_static`
- Object-storage payload size (artifacts)
- Projected reprocess time at N=1, 4, 8, 16 parallel workers
- Estimated downtime window

The "~hour" in v2 is replaced with measured numbers from this command.
If reprocess > 4 hours at full concurrency, plan changes to either:
- Parallel reprocess on a separate read replica with cutover, or
- Lazy reprocess: queue a re-run job on first read of stale schema-v1 artifacts

Decision deferred until the measurement runs.

## Corpus tests — semantic, not superset

Same as v2: per-fixture manifest with `keep`, `remove`, `add`, expected
capability shapes, expected unsupported counts.

5 protocols: EtherFi, Aave V3, MakerDAO Vat, Gnosis Safe, Compound III.

Add 6 more: a contract with each unsupported pattern (Diamond, EIP-1271,
bitwise flag, opaque assembly, function-pointer dispatch, M-of-N native).
These pin the unsupported_reason strings.

## Test strategy

| Layer | Test |
|---|---|
| ProvenanceEngine | Per-IR-opcode fixtures (Binary, Index, Member, calls, returns, etc.) |
| Polarity normalization | Truth table for all 8 operators × 2 polarities × 6 contexts |
| ParameterBindingEnv | Constant binding, dynamic binding, library binding, chained modifiers |
| ReentrancyAnalyzer / PauseAnalyzer | Renamed-equivalent contracts; structural-only |
| Predicate emitter | 16 patterns + 6 unsupported pinned |
| Capability combinators | Property tests over confidence/quality cross-product |
| Adapter `matches()` | Each adapter on hybrid contracts; tie warnings |
| Adapter `enumerate()` | Against fixture chain state |
| `role_grants` indexer | Reorg simulation, advisory-lock contention, double-grant, transaction_index correctness |
| Edge-case reverts | Inline asm, try/catch, function-pointer, opaque-Yul fixtures |
| Corpus | 5 protocols + 6 unsupported, semantic diff |
| Live E2E | EtherFi pre-prod canary |
| Existing 41 fuzz xfails | All flip to passing |

## Phasing — 6–8 weeks

| Week | Work | Acceptance |
|---|---|---|
| 0 | Run `measure_reprocess_cost.py`. Set N-weeks based on measured runtime. Build adapter API protocol contract + EvaluationContext stub. | Measurement report attached. Adapter API frozen. |
| 1 | ProvenanceEngine + AuthorityClassifier + ReentrancyAnalyzer + PauseAnalyzer + per-opcode fixtures. | All operand sources classified correctly on the 16-fixture corpus. |
| 2 | Predicate builder + polarity normalizer + ParameterBindingEnv + RevertDetector + edge-case fixtures. | Predicate tree emits cleanly for 16 supported + 6 unsupported patterns. |
| 3 | CapabilityExpr + combinators + property tests. AccessControl adapter + Safe adapter. role_grants schema + advisory-locked indexer + reorg simulation. | EtherFi grantRole resolves end-to-end against live RPC. |
| 4 | Aragon/DSAuth + EIP-1271 + event-indexed adapters. Parametric role-domain expansion (auto-seed + recursive). | Maker wards + Compound Comet + an EIP-1271 contract resolve. |
| 5 | UI: ProtocolSurface, auditUi, protocolScore. New API route `/api/contract/<addr>/check_membership`. Per-role expansion. | Full v2 capability rendering shipped to pre-prod. |
| 6 | Corpus tests for 5 supported + 6 unsupported protocols. CI integration. Regression diff in PR. | Corpus diff approved. |
| 7 | Cleanup: delete dead heuristics, remove probe ladder from controller_adapters.py, perf benchmarking. | < 5ms p99 per-function predicate build; resolver evaluator < 50ms p99 per function (excluding RPC). |
| 8 | Cutover: drain queue, run reprocess, deploy v2 binary, monitor canary. | Production cutover complete. |

If week-0 measurement says reprocess takes > 1 day, weeks 8 → 9-10 to
account for staged cutover.

## Risks and mitigations

(Same as v2, with two additions:)

| Risk | Mitigation |
|---|---|
| ProvenanceEngine misses an IR opcode used by a real contract | Per-opcode fixture coverage; fall through to `unsupported(reason="provenance_unknown_opcode")` rather than crashing. |
| Reentrancy/pause analyzers misclassify a custom guard as one or the other | Each analyzer reports its decision basis in the leaf's `basis` field; corpus tests pin expected classifications; misclassification surfaces in CI diff. |
| Predicate tree becomes huge for very-many-modifier functions | Tree depth cap (8) + width cap (32 leaves at any AND/OR node); excess truncates to `unsupported("predicate_complexity")`. |
| `role_grants_events` table grows unbounded | Add `prune_role_grants_events` cron job: rows with `revoke` superseded by later state can be archived after N blocks. Out of scope for initial cutover; add at week 6+. |

## Out of scope (same as v2)

zk proofs / attestations: `unsupported("zk_proof")`. Cross-chain auth:
`external_check_only`. Optimistic governance: `external_check_only`.
Diamond ACL: `unsupported("diamond_acl_v1")` — concrete adapter in v2.1.
EIP-1271: `external_check_only`. Bitwise role flags:
`unsupported("bitwise_role_flag")`. Permit: not admitted (asset auth).
Reentrancy / pause / business: annotated, not admitted.

## v2 → v3 diff

| Codex round-2 finding | v3 fix |
|---|---|
| #1 authority_role overclaimed | Operand provenance engine as week-1 deliverable; ReentrancyAnalyzer + PauseAnalyzer as separate control-flow analyzers, not mapping_events-style |
| #2 modifier param substitution unfolded | ParameterBindingEnv with explicit constant/dynamic/library binding + chained-modifier fixtures |
| #3 polarity inconsistent | `operator` field separate from `kind`; explicit normalization table; no `inequality` kind invented |
| #4 capability combinators not closed | `membership_quality: exact|lower_bound|upper_bound`, `conditions: list[Condition]`, total-function combinators with structural fallback |
| #5 RoleDomain misses zero-role and recursion | `auto_seed_default_admin`, `recursive_role_admin_expansion`; both default-true for AC-shaped descriptors detected structurally |
| #6 role_grants indexer reuse risk + missing covering index | Independent indexer separate from `monitored_events`; `transaction_index` column added; covering index `(chain_id, contract_id, role, member, block_number DESC, log_index DESC, direction)` |
| #7 cutover timing not defensible | Week-0 `measure_reprocess_cost.py` script before any code is written; downtime window from measured numbers |
| #8 edge-case revert fixtures missing | RevertDetector with explicit cases for inline-asm, try/catch, function-pointer, opaque-Yul; `unsupported("opaque_control_flow")` when control dependence not recoverable |
| #9 adapter fallback + EIP-1271 API undefined | Score-0 → `unsupported("no_adapter")`; `external_check_only` only when adapter implements `membership()`; new `POST /api/contract/<addr>/check_membership` route |

## Open questions for codex (round 3)

1. **ProvenanceEngine vs MsgSenderTaint coexistence**: should week 1
   *replace* MsgSenderTaint outright (delete it), or keep it as a
   compatibility shim used by the new engine? The old engine has a
   helper-name seed list at `caller_sinks.py:37` that's name-based —
   killing it is the goal, but anything currently relying on its taint
   output (`caller_in_mapping`, etc.) breaks.

2. **ParameterBindingEnv depth limits**: modifier-of-library-of-modifier
   chains aren't unheard of in production. Cap binding-substitution
   depth at 4? 8? When exceeded, emit
   `unsupported("substitution_depth_exceeded")`?

3. **Capability `structural_AND` rendering**: the UI plan says "render
   as expression tree" but doesn't specify *how*. Do we have a UI
   primitive for tree expressions, or does this need new component
   work I'm not budgeting?

4. **role_grants indexer backfill**: brand-new contracts have zero
   events and adapters return empty `finite_set`. New contracts with
   `DEFAULT_ADMIN_ROLE` granted in the constructor (most OZ contracts):
   does the indexer scan from contract genesis block, or only from
   first observation? If genesis, large mainnet contracts could have
   many MB of historical events to backfill — do we accept a one-time
   long backfill at first analysis?

5. **Week-0 measurement precondition**: do we run it on the production
   DB, or on a recent snapshot? Production has the real numbers but
   touching it might violate the "no shared-state changes" guidance.
