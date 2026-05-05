# Generic predicate-based access analysis — v4 (final)

Folds all findings from codex rounds 1, 2, 3. Diff vs v3 at bottom.

## Goals

1. Static stage emits structural predicates only — no name-based admission.
2. A function is admitted iff its predicate tree contains at least one
   leaf with `authority_role ∈ {caller_authority, delegated_authority}`.
   Business / time / reentrancy / pause leaves are preserved and
   evaluated, but never admit on their own.
3. Resolver evaluates **capability expressions** (finite_set with
   exact/lower_bound/upper_bound quality, threshold_group,
   cofinite_blacklist, signature_witness, external_check_only,
   conditional_universal, AND/OR, unsupported) — never flat principal
   sets, never silently miscombined.
4. Standard-ABI knowledge lives in adapters behind one interface that
   gets full evaluation context.
5. No backwards compatibility. Single schema-v2 cutover.
6. Every unsupported pattern surfaces explicitly with a typed reason.

## Predicate IR

### Boolean tree

```python
class PredicateTree(TypedDict):
    op: Literal["AND", "OR", "LEAF"]
    children: list[PredicateTree] | None
    leaf: LeafPredicate | None
```

No `NOT` node. All negations folded into `operator` on each leaf during
normalization.

### LeafPredicate

```python
class LeafPredicate(TypedDict):
    kind: Literal[
        "membership", "equality", "comparison",
        "external_bool", "signature_auth", "unsupported",
    ]
    operator: Literal[
        "eq", "ne",
        "lt", "lte", "gt", "gte",
        "truthy", "falsy",
    ]
    authority_role: Literal[
        "caller_authority", "delegated_authority",
        "time", "reentrancy", "pause", "business",
    ]
    operands: list[Operand]
    set_descriptor: SetDescriptor | None
    unsupported_reason: str | None
    references_msg_sender: bool
    parameter_indices: list[int]
    expression: str
    basis: list[str]
```

### Polarity normalization (round-2 #3 fix, validated round-3)

| Source pattern | Normalized leaf |
|---|---|
| `require(a == b)` | `equality, op=eq` |
| `require(a != b)` | `equality, op=ne` |
| `if (a == b) revert` | `equality, op=ne` (allowed: `a != b`) |
| `if (a != b) revert` | `equality, op=eq` |
| `require(!(a == b))` | `equality, op=ne` |
| `if (!(a == b)) revert` | `equality, op=eq` |
| `require(map[k][m])` | `membership, op=truthy` |
| `require(!map[k][m])` | `membership, op=falsy` |
| `if (map[k][m]) revert` | `membership, op=falsy` |
| `if (!map[k][m]) revert` | `membership, op=truthy` |
| `require(authority.canCall(...))` | `external_bool, op=truthy` |
| `require(!authority.allowed(...))` | `external_bool, op=falsy` |
| `require(a > b)` | `comparison, op=gt` |
| `if (a > b) revert` | `comparison, op=lte` |
| `require(_status != _ENTERED)` | `equality, op=ne, authority_role=reentrancy` |

Rule: extract the *allowed* condition. After normalization, no NOT
exists in the IR.

## ProvenanceEngine — worklist dataflow (round-3 blocker #1 fix)

`services/static/contract_analysis_pipeline/provenance.py` is a
worklist/fixed-point dataflow analyzer over Slither's SSA IR + CFG. Not
a forest.

```python
class ProvenanceEngine:
    """Forward dataflow over Slither SSA values + CFG.

    Lattice element per SSA value: a set of Sources, plus a 'top' (any)
    state for unresolved cycles. Sources are the same union as Operand
    above (msg_sender, parameter, state_variable, constant, view_call,
    external_call, computed, block_context, signature_recovery, top).

    Transfer functions per IR opcode:
      - Assignment: lhs := rhs's source set
      - TypeConversion: passthrough rvalue
      - Phi: union of incoming sources, plus 'top' if any incoming is 'top'
      - Binary / Unary: source set is union of operands tagged "computed"
      - Index / Member: source includes the base's source plus key/field info
      - HighLevelCall: lhs gets {external_call(callee, args_provenance)}
      - InternalCall / LibraryCall: lhs gets the callee's return source
        (computed lazily via per-callee cache; bounded recursion depth 4)
      - LowLevelCall (call/staticcall/delegatecall): {external_call(target_unknown)}
      - SolidityCall(ecrecover): {signature_recovery(args_provenance)}
      - SolidityCall(keccak/sha256/etc): {computed(callee_args_provenance)}
      - Send/Transfer: bool source = {computed}
      - Return: callee return-source = union of all returned values' sources
      - NewContract / NewArray: {computed(args)}

    Cycle handling: SSA values reached during their own resolution chain
    yield 'top'. The leaf using a 'top' operand emits
    unsupported(reason='provenance_cycle').

    Loop handling: standard worklist iteration to fixed point. Loop-carried
    Phi nodes converge or saturate to 'top'.

    Output: ProvenanceMap[SSAValue → SourceSet]. Used by the predicate
    builder to populate Operand records.
    """
```

Built fresh in week 1. Reuses `slither.slithir.operations` and
`slither.core.cfg.node` only; **no carry-over from MsgSenderTaint's
helper-name seed list**. Helper-seed code at `caller_sinks.py:37` is
deleted.

Test fixtures cover every IR opcode listed above plus cycle/loop cases:
- Plain assignments, type conversions, Phi
- Binary/Unary chains
- Index/Member nesting (mapping[k1][k2].field)
- View-call return propagation through depth-3 internal calls
- External call return + arg taint
- ecrecover output classified as signature_recovery
- abi.encodePacked output classified as computed with arg list
- Loop with role-derivation in body — must converge or saturate to top

## Predicate builder

`services/static/contract_analysis_pipeline/predicates.py`. Per function:

1. Run RevertDetector to find all gated revert paths.
2. For each revert path, extract the gating condition and its operands.
3. Classify each operand using ProvenanceEngine.
4. Build leaves with `kind` + `operator` per the polarity normalization
   table.
5. Apply AuthorityClassifier to each leaf to set `authority_role`.
6. Combine leaves into a tree using AND for sequential reverts and the
   short-circuit boolean structure (AND/OR) of compound conditions.
7. Normalize: push NOT into operators; canonicalize operand order; cap
   tree depth/width.

### ParameterBindingEnv (round-2 #2 fix)

```python
class ParameterBindingEnv:
    """Stack-of-frames mapping callee/modifier parameter names to the
    provenance source of the value at the invocation site.

    For modifier invocation `m(arg1, arg2)`:
      - frame[r] = ProvenanceSource of arg1  (where r is m's first param)
      - resolved during predicate building when analyzing m's body
      - popped after the body is processed

    Substitution depth cap: 6. Beyond that, leaf becomes
    unsupported("substitution_depth_exceeded"). (Round-2 #9 open question
    answered.)
    """
```

Test fixtures (round-2 #2 explicit list):
1. Constant binding: `function f() onlyRole(BREAK_GLASS)` — leaf
   references constant BREAK_GLASS.
2. Dynamic binding: `function f(bytes32 r) onlyRole(r)` — leaf reports
   `parameter_index=0`.
3. Library binding: `function f(bytes32 r) { LibAccess.requireAccess(r); }`.
4. Chained modifiers: `function f() onlyRole(A) onlyRole(B)` — AND of
   both.
5. Nested modifier-of-library-of-modifier — within depth-6 cap.

### AuthorityClassifier (round-2 #1 fix)

```python
class AuthorityClassifier:
    """Per-leaf classification using ProvenanceEngine output.

    1. caller_authority — any operand's source set includes msg_sender,
       tx_origin, or signature_recovery (transitively through view_call/
       external_call args).
    2. delegated_authority — leaf is external_bool whose callee target
       traces to a state_variable AND whose callee_args include
       msg_sender or signature_recovery transitively.
    3. time — every varying operand sources from block_context.
    4. reentrancy — leaf reads a state_variable that ReentrancyAnalyzer
       flagged as a guard variable.
    5. pause — leaf reads a state_variable that PauseAnalyzer flagged.
    6. business — residual.
    """
```

### ReentrancyAnalyzer / PauseAnalyzer (round-3 #9 fix: dominance + confidence)

```python
class ReentrancyAnalyzer:
    """Identifies reentrancy guard state-vars by control-flow + dominance:

    Required structural conditions for HIGH confidence:
      1. State variable, type uint8/uint256/bool.
      2. There exists a function F where F's body contains:
         - A write of "entered" value to V at a CFG node N1 that
           DOMINATES every external call.
         - A write of "not entered" value to V at a CFG node N2 that
           is POST-DOMINATED by all external call returns.
         - A read of V (and revert if "entered") at a node that
           dominates N1.
      3. The same V is read with revert in other public functions
         that contain external calls.

    For MEDIUM confidence: only conditions 1+3 hold, no full dominance
    pattern (e.g., transient-storage TSTORE/TLOAD pattern with no
    explicit reset).

    For LOW (rejected): the variable does not satisfy the dominance
    pattern. Classified as 'business_toggle' (an `equality, op=ne,
    authority_role=business` leaf, because state may be auction phase
    / market closed / etc).

    Output: dict[StateVar.name → confidence]. Predicate builder uses
    HIGH/MEDIUM to set authority_role=reentrancy; LOW falls into business.
    """

class PauseAnalyzer:
    """Same structural framing for pause guards:
      1. State variable, type bool/uint8.
      2. Has at least one writer function F where F itself has a
         caller_authority/delegated_authority predicate (the pauser).
      3. Other functions read V and revert when the "paused" value
         is set.
      4. CONFIDENCE: HIGH if writer is admitted-privileged; MEDIUM
         if ambiguous; LOW (→ business) otherwise.
    """
```

`basis` field on each leaf records the analyzer's reasoning so the UI
can show "classified reentrancy because dominance pattern matched at
nodes N1/N2."

### RevertDetector (round-2 #8 + round-3 #8)

```python
class RevertDetector:
    """Identifies all revert paths and their gating predicates. Cases:

    1. require(C) / require(C, msg) — gating predicate is C.
    2. assert(C) — same.
    3. if (R) revert / revert ErrorName(args) — gating predicate is NOT R.
    4. SolidityCall(revert) — same as (3).
    5. Inline assembly: assembly { if iszero(X) { revert(0,0) } } —
       walks Slither's InlineAssemblyOperation IR; treats as
       `if (iszero(X)) revert` ⇒ gating predicate `truthy(X)`.
    6. try ... catch { revert(); } — control-dependent on the try
       statement's success; predicate is "external_call(target).succeeded";
       authority_role=business (success of external call isn't caller auth).
    7. State-stored function pointers: `function p; require(p == sig);`
       — provenance classifies p as state_variable, sig as constant or
       parameter; emits `equality(state_var, constant/parameter)` with
       authority_role=business unless one operand traces to msg_sender
       (then caller_authority).
    8. Fully opaque control flow (Yul with arbitrary jumps not modeled
       by Slither): unsupported("opaque_control_flow"); function still
       admits with that leaf (UI surfaces it explicitly).
    """
```

Test fixtures: one per case 1-8. Case 7 specifically asserts that a
function-pointer dispatch resolves cleanly to `business` rather than
`unsupported`.

## Capability expression — closed combinators (round-3 #3 fix)

```python
class CapabilityExpr:
    kind: Literal[
        "finite_set", "threshold_group", "cofinite_blacklist",
        "signature_witness", "external_check_only",
        "conditional_universal", "unsupported", "AND", "OR",
    ]
    members: list[str] | None        # sorted, lowercased, deduped
    threshold: tuple[int, list[str]] | None
    blacklist: list[str] | None
    signer: 'CapabilityExpr | None'
    check: ExternalCheck | None
    conditions: list[Condition]      # side-conditions: time / reentrancy / pause / business
    unsupported_reason: str | None
    children: list['CapabilityExpr'] | None
    membership_quality: Literal["exact", "lower_bound", "upper_bound"]
    confidence: Literal["enumerable", "partial", "check_only"]
    last_indexed_block: int | None
```

### Combinator semantics — fully enumerated

```
intersect(finite_set_A, finite_set_B):
    members = sort_dedup_lower(A.members ∩ B.members)
    if A.quality == B.quality == "exact":
        return finite_set(members, quality="exact")
    if {A.quality, B.quality} ⊆ {"exact", "lower_bound"}:
        return finite_set(members, quality="lower_bound")
    if "upper_bound" in {A.quality, B.quality}:
        return structural_AND(A, B)
        # upper-bound says "members are a superset of true set";
        # intersecting with anything else doesn't yield a sound bound
        # without splitting known/possible.

intersect(finite_set, conditional_universal(c)):
    out = copy(finite_set)
    out.conditions.append(c)
    return out

intersect(threshold_group, finite_set):
    return structural_AND(threshold_group, finite_set)

intersect(cofinite_blacklist, finite_set):
    members = sort_dedup_lower(finite_set.members - blacklist.blacklist)
    return finite_set(members, quality=finite_set.quality)

intersect(any, unsupported(r)):
    return unsupported("intersect_with_unsupported_" + r)

union(finite_set_A, finite_set_B):
    members = sort_dedup_lower(A.members ∪ B.members)
    if A.quality == B.quality == "exact":
        return finite_set(members, quality="exact")
    if {A.quality, B.quality} ⊆ {"exact", "lower_bound"}:
        return finite_set(members, quality="lower_bound")
        # known members of either side are known members of union.
    if A.quality == B.quality == "upper_bound":
        return finite_set(members, quality="upper_bound")
        # possible-on-A ∪ possible-on-B is possible-in-union.
    if {"lower_bound", "upper_bound"} == {A.quality, B.quality}:
        return structural_OR(A, B)
        # mixing known-floor with possible-ceiling has no sound combine
        # without splitting representations.

union(finite_set, conditional_universal(c)):
    return structural_OR(finite_set, conditional_universal(c))
    # owner OR (anyone iff time-condition) — not collapsible.

union(threshold_group | signature_witness | external_check_only, anything):
    return structural_OR(...)

union(any, unsupported):
    return structural_OR(any, unsupported)

negate(finite_set with quality="exact"):
    return cofinite_blacklist(blacklist=members)
negate(finite_set with quality≠"exact"):
    return unsupported("negate_partial_set")
negate(threshold_group | signature_witness | external_check_only):
    return unsupported("negate_unsupported_capability")
negate(conditional_universal(c)):
    return conditional_universal(condition=negate_condition(c))
negate(unsupported):
    return unsupported("negate_of_" + reason)
negate(cofinite_blacklist):
    return finite_set(members=blacklist, quality="exact")
```

All combinators are total functions returning either a typed capability
or `unsupported(reason)`. Property tests cover the cross-product.

### Operator-driven evaluator (round-3 #4 fix)

```python
def _evaluate_leaf(leaf, ctx) -> CapabilityExpr:
    if leaf.authority_role in ("reentrancy", "pause", "business", "time"):
        return CapabilityExpr.conditional_universal(condition=Condition.from_leaf(leaf))

    # caller_authority / delegated_authority below

    if leaf.kind == "membership":
        adapter = ctx.adapter_registry.pick(leaf.set_descriptor, ctx)
        result = adapter.enumerate(leaf.set_descriptor, ctx)
        cap = CapabilityExpr.from_enumeration(result)
        if leaf.operator == "falsy":
            cap = cap.negate()
        return cap

    if leaf.kind == "equality":
        if leaf.operator in ("eq", "ne"):
            base = _resolve_equality_principal(leaf, ctx)
            return base if leaf.operator == "eq" else base.negate()
        return CapabilityExpr.unsupported(f"equality_op_{leaf.operator}_unsupported")

    if leaf.kind == "external_bool":
        cap = _resolve_external_bool(leaf, ctx)
        if leaf.operator == "falsy":
            cap = cap.negate()
        return cap

    if leaf.kind == "signature_auth":
        return _resolve_signature_auth(leaf, ctx)
        # operator should always be 'eq' (recovered_signer == expected) or
        # 'truthy' (isValidSignature returned true); negation is rare.

    if leaf.kind == "comparison":
        # Caller-authority comparisons are exotic; mostly time-gates,
        # which are routed via authority_role above.
        return CapabilityExpr.conditional_universal(condition=Condition.from_leaf(leaf))

    if leaf.kind == "unsupported":
        return CapabilityExpr.unsupported(leaf.unsupported_reason)
```

### Business preservation under OR (round-3 blocker #2 fix)

`require(msg.sender == owner || amount < cap)` produces
`OR(equality(caller_authority, op=eq), comparison(business, op=lt))`.

Admission rule: tree contains a `caller_authority`/`delegated_authority`
leaf — admit. The function's capability is then:

```
union(
    CapabilityExpr.finite_set([owner], quality=exact),
    CapabilityExpr.conditional_universal(condition=business(amount < cap))
)
= structural_OR(finite_set(owner), conditional_universal(business))
```

The UI renders this as "owner OR anyone if amount < cap" — not as
"owner-only" (which would be wrong) and not dropped (which would also
be wrong). Business leaves are first-class citizens of the capability
graph; they just don't admit alone.

## SetDescriptor & RoleDomain (round-3 #7 fix on AC-shaped detection)

```python
class SetDescriptor(TypedDict):
    kind: Literal[
        "mapping_membership", "array_contains", "external_set",
        "bitwise_role_flag", "diamond_facet_acl",
    ]
    storage_var: str | None
    storage_slot: str | None
    key_sources: list[Operand]
    truthy_value: str | None
    enumeration_hint: list[EventHint]
    authority_contract: AuthorityContract | None
    role_domain: RoleDomain | None
    selector_context: SelectorContext | None

class RoleDomain(TypedDict):
    parameter_index: int
    auto_seed_default_admin: bool        # set by adapter, not statically
    sources: list[Literal[
        "compile_time_constants",
        "role_granted_history",
        "abi_declared",
        "manual_pinned",
    ]]
    recursive_role_admin_expansion: bool
```

**`auto_seed_default_admin` is set by the AccessControl adapter's
`matches()` decision, not by the static stage.** The static stage emits
the descriptor with `auto_seed_default_admin=False` by default; the
adapter inspects the contract's bytecode and bytecode-derived ABI:
- Has `hasRole(bytes32,address)` selector? AND
- (Has `RoleGranted(bytes32,address,address)` event topic in the
  contract's emitted-event set OR a successful `getRoleAdmin(bytes32)`
  call returns without revert)

If both, the adapter returns score 90+ for the descriptor and sets
`auto_seed_default_admin=True`. False-matches on custom contracts
that happen to define `hasRole(bytes32,address)` but not the rest are
filtered.

`bitwise_role_flag` and `diamond_facet_acl` only ever appear inside
`unsupported` leaves' descriptors in v4.0. They describe what was
found structurally so a future adapter can take them; they never
drive admission.

## Static stage files

| File | Change |
|---|---|
| `schemas/contract_analysis.py` | Replace `controller_refs/guards/sinks` with `predicate_tree`. Bump schema_version → "2.0". |
| `services/static/contract_analysis_pipeline/provenance.py` | **New.** Worklist dataflow. ~500 lines. |
| `services/static/contract_analysis_pipeline/predicates.py` | **New.** Predicate builder + polarity normalizer + ParameterBindingEnv + AuthorityClassifier. ~400 lines. |
| `services/static/contract_analysis_pipeline/reentrancy_pause.py` | **New.** ReentrancyAnalyzer + PauseAnalyzer with dominance/post-dominance. ~250 lines. |
| `services/static/contract_analysis_pipeline/revert_detect.py` | **New.** RevertDetector with all 8 cases. ~200 lines. |
| `services/static/contract_analysis_pipeline/caller_sinks.py` | Delete `_classify_caller_*`, helper-name seeds, MsgSenderTaint. |
| `services/static/contract_analysis_pipeline/summaries.py` | Delete `_internal_auth_calls`, `_build_method_to_role_map` name path, `_detect_access_control` inheritance. |
| `services/static/contract_analysis_pipeline/semantic_guards.py` | **Delete.** |
| `services/static/contract_analysis_pipeline/graph.py` | Drop `_looks_like_external_authority_call`, `checkCallerIsX`. Keep bytes32 role aliasing. |
| `services/static/contract_analysis_pipeline/mapping_events.py` | Extend output → `EventHint` records. |

## Resolver stage files

| File | Change |
|---|---|
| `services/resolution/predicates.py` | **New.** Capability evaluator + `evaluate(tree, ctx)`. ~400 lines. |
| `services/resolution/capabilities.py` | **New.** CapabilityExpr + total-function combinators. ~300 lines. |
| `services/resolution/adapters/__init__.py` | **New.** SetAdapter Protocol + AdapterRegistry + EvaluationContext. ~150 lines. |
| `services/resolution/adapters/access_control.py` | OZ AccessControl + Enumerable. AC-shape detection in `matches()`. |
| `services/resolution/adapters/safe.py` | Returns threshold_group. |
| `services/resolution/adapters/dsauth_aragon.py` | Authority-contract resolution + ACL log replay. |
| `services/resolution/adapters/event_indexed.py` | Generic event-replay over `enumeration_hint`. |
| `services/resolution/adapters/eip1271.py` | Returns `external_check_only` with signature-validity probe API. |
| `services/resolution/recursive.py` | Strip Safe/Timelock/ProxyAdmin switch (L713-805). |
| `services/resolution/tracking.py` | Strip role_identifier (L447-495), Safe/Timelock probes (L178-186, L343-375). |
| `services/resolution/controller_adapters.py` | Probe ladder moves into per-adapter `detect()`. |

## SetAdapter (round-3 #5 + final spec)

```python
class SetAdapter(Protocol):
    @classmethod
    def matches(cls, descriptor: SetDescriptor, ctx: EvaluationContext) -> int:
        """0-100. 0 means definitely not. 100 means definitely yes."""

    def enumerate(self, descriptor, ctx) -> EnumerationResult: ...
    def membership(self, descriptor, ctx, member: Address) -> Trit: ...

    @classmethod
    def supports_external_check_only(cls) -> bool:
        """Whether membership() against a live backend is supported."""
```

Resolver picks max-scoring adapter. Score 0 from all adapters →
`unsupported("no_adapter")`. `external_check_only` returned only when
the chosen adapter has `supports_external_check_only=True` and
`enumerate()` returns `confidence="check_only"`.

## API routes (round-3 #5 fix)

Two new routes (in a new `routers/contracts.py`, registered before
the SPA catch-all in `api.py`):

### `POST /api/contract/<addr>/probe/membership`

```json
Request:
{
  "predicate_index": int,         // index into the artifact's predicate trees
  "member": "0xabc..."            // 42-char address
}
Response:
{
  "trit": "yes" | "no" | "unknown",
  "evidence": {...},
  "checked_at_block": int
}
```

Server-side: load the predicate by `(contract_id, function_signature,
predicate_index)` from artifacts. Validate `member` is a checksummed
address. Pick adapter, call `membership()`. **Descriptor never accepted
from the client.**

Auth: `Depends(deps.require_admin_key)`.
Rate limiting: 10 req/min per admin key per contract.
Caching: response cached for 60 blocks against `(contract, predicate_index, member)`.

### `POST /api/contract/<addr>/probe/signature`

```json
Request:
{
  "predicate_index": int,
  "hash": "0x...",                // 32 bytes
  "signature": "0x..."            // 65 bytes ECDSA / EIP-1271 sig
}
Response:
{
  "trit": "yes" | "no" | "unknown",
  "is_valid_signature": bool,
  "checked_at_block": int
}
```

For EIP-1271 / signature-auth predicates only. Calls
`isValidSignature(hash, signature)` on the resolved authority contract.

Same auth + rate limit. Caching off (signature inputs vary).

## role_grants_events — reorg-safe + index-only (round-2 #6 fix folded)

```sql
CREATE TABLE role_grants_events (
    chain_id          INTEGER NOT NULL,
    contract_id       INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    role              BYTEA   NOT NULL,
    member            VARCHAR(42) NOT NULL,
    direction         VARCHAR(8)  NOT NULL CHECK (direction IN ('grant', 'revoke')),
    block_number      BIGINT  NOT NULL,
    block_hash        BYTEA   NOT NULL,
    tx_hash           BYTEA   NOT NULL,
    log_index         INTEGER NOT NULL,
    transaction_index INTEGER NOT NULL,
    PRIMARY KEY (chain_id, contract_id, tx_hash, log_index)
);
CREATE INDEX role_grants_events_state ON role_grants_events
    (chain_id, contract_id, role, member, block_number DESC, log_index DESC, direction);

CREATE TABLE role_grants_cursor (
    chain_id INTEGER NOT NULL,
    contract_id INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    last_indexed_block BIGINT NOT NULL,
    PRIMARY KEY (chain_id, contract_id)
);

CREATE TABLE chain_finality_config (
    chain_id INTEGER PRIMARY KEY,
    confirmation_depth INTEGER NOT NULL,
    rpc_safe_block_alias VARCHAR(16),
    name VARCHAR(64) NOT NULL
);
-- Seed: mainnet=12, polygon=128, arbitrum=20, base=24, optimism=24,
-- linea=24, scroll=24.
```

Indexer is **independent of `monitored_events` / `unified_watcher`**.
Per-`(chain_id, contract_id)` advisory lock + `FOR UPDATE` on cursor.
Rewinds-deletes-replays the confirmation window atomically. Reorg
detection by `block_hash` mismatch.

### Backfill from contract genesis (round-3 #9 decision)

On first analysis of an AccessControl-shaped contract, the indexer
backfills from the contract's deployment block (sourced from
existing controller-adapters' code-start-block pattern) to current
head. This catches constructor / initializer `RoleGranted` events.
During backfill, capability is exposed with `confidence="partial"`
and `membership_quality="lower_bound"` so the UI shows "Known: …;
indexing in progress."

For long histories (mainnet AC contracts may have years of events),
the backfill is parallelizable with batched `eth_getLogs` calls
across block ranges.

## UI

| Capability | UI |
|---|---|
| `finite_set, exact` | List of addresses |
| `finite_set, lower_bound` | "Known: …; may have additional members" |
| `finite_set, upper_bound` | "At most: …; some may not currently hold" |
| `threshold_group(M, signers)` | "M-of-N" badge; expandable list |
| `cofinite_blacklist` | "Anyone except …" |
| `signature_witness(signer_expr)` | "Anyone with a signature from \<expr\>" |
| `external_check_only` | Address/signature input; calls probe routes |
| `conditional_universal(c)` | "Anyone, while \<c\>" |
| `unsupported(reason)` | "Pattern not yet supported: \<reason\>" + raw IR |
| `AND/OR` structural | Expression tree (new component `<CapTree />`) |
| Empty predicate tree | "Public" |

Conditions render as small chips next to principals: "after T", "while
not paused", etc.

`<CapTree />` is a new React component, ~150 lines. Renders nested
AND/OR boxes recursively. Round-3 question on UI primitive answered:
new component, budgeted in week 7.

## Migration — single schema-v2 cutover

Same as v3.

### Week-0 measurement (round-3 #6 fix on actually-runnable script)

`scripts/measure_reprocess_cost.py`:

```python
"""Reads stage_timing_static artifacts from the artifacts table; computes
percentiles; reports projected reprocess time at varying parallelism.

Source data: query
    SELECT a.data, a.storage_key
    FROM artifacts a
    WHERE a.name = 'stage_timing_static';

Decode payload (object-storage indirection if storage_key non-null;
inline JSON otherwise via db/queue.py:get_artifact). Each payload has
{stage_seconds: float}.

Outputs: contract count, source availability rate (Contract.source_files
non-empty), p50/p95/max stage_static_seconds, projected reprocess at
N=1,4,8,16 worker concurrency, plus object-storage payload bytes
estimate from artifact.size_bytes column.
"""
```

Direct DB credentials suffice — no admin key needed (not going through
`/api/jobs/<id>/stage_timings`).

If projected reprocess > 4 hours, decision: lazy reprocess on first read
of stale schema-v1 artifacts, gated behind a "stale" flag in API
responses until the worker picks them up.

## Corpus tests — semantic, with unsupported pinning

Same as v3, with the same 5 supported + 6 unsupported pinned protocols.
Per-fixture manifest: `keep`, `remove`, `add`, expected capability
shapes per function, expected unsupported_reason strings.

## Phasing — 9–10 weeks (round-3 phasing fix)

| Week | Work | Acceptance |
|---|---|---|
| 0 | Run `measure_reprocess_cost.py`. Build adapter API + EvaluationContext stub. | Measurement attached. Adapter API frozen. |
| 1 | ProvenanceEngine: worklist dataflow over SSA + CFG + Phi joins + cycle guards. Per-IR-opcode fixtures. | Provenance correct on the 16-fixture corpus + cycle/loop fixtures. |
| 2 | Predicate builder + polarity normalizer + ParameterBindingEnv + RevertDetector (all 8 cases). | Predicates emit cleanly for 16 supported + 8 revert-edge-case fixtures. |
| 3 | AuthorityClassifier + ReentrancyAnalyzer + PauseAnalyzer with dominance/post-dominance + confidence. | Renamed-equivalent contracts classify correctly. |
| 4 | CapabilityExpr + total-function combinators + property tests. | Property test suite green on cross-product of qualities/confidences. |
| 5 | role_grants schema + advisory-locked indexer + reorg simulation + backfill from genesis + per-chain finality config. AccessControl adapter + Safe adapter. | EtherFi grantRole resolves end-to-end against live RPC. |
| 6 | DSAuth/Aragon + EIP-1271 + event-indexed adapters. Parametric role-domain expansion (auto-seed + recursive). | Maker wards + Compound Comet + EIP-1271 contract resolve. |
| 7 | UI: ProtocolSurface, auditUi, protocolScore. New `<CapTree />` component. New API routes `/probe/membership` and `/probe/signature`. | Full v2 capability rendering on pre-prod. |
| 8 | Corpus tests (5 supported + 6 unsupported). CI integration. | Corpus diff approved. |
| 9 | Cleanup: delete dead heuristics, controller_adapters probe ladder. Perf benchmarking. | < 5ms p99 predicate build; < 50ms p99 evaluator (excluding RPC). |
| 10 | Cutover: drain queue, run reprocess (or trigger lazy reprocess flag), deploy v2 binary, monitor canary. | Production cutover complete. |

If week-0 measurement says reprocess > 1 day, week 10 → 11-12 with
staged cutover and lazy reprocess.

## Risks & mitigations

(Same as v3, plus:)

| Risk | Mitigation |
|---|---|
| ProvenanceEngine doesn't converge on pathological loops | Worklist iteration cap (200 iterations per function); on cap, saturate to 'top' and emit `unsupported("provenance_iteration_cap")` |
| Reentrancy/pause LOW-confidence cases pollute UI as business_toggle | Acceptable — they're true business toggles or ambiguous; UI labels them "guarded by `<state_var>` (business)" without claiming they're auth |
| AC adapter misclassifies hybrid contract (has hasRole but not really AC) | `matches()` returns score, not boolean; corpus tests pin per-adapter score on hybrids; adapter returns lower score for partial-shape; multiple adapters can claim and fight via score |
| Genesis backfill takes hours for old AC contracts | Parallelizable; expose `confidence=partial` with a UI badge "indexing"; user sees progress |
| New API routes leak descriptor content | Server-side fetch by index; client never supplies descriptor; auth guards both routes |

## Out of scope

zk proofs, cross-chain auth, optimistic governance, Diamond ACL,
EIP-1271 enumeration (only check-only), bitwise role flags, permit-style
asset auth. Each pinned in corpus as `unsupported(reason=...)` with
explicit fixture.

## v3 → v4 diff

| Codex round-3 finding | v4 fix |
|---|---|
| Blocker #1: ProvenanceEngine cannot be a forest | Worklist/fixed-point dataflow over SSA + CFG with Phi joins, cycle guards, iteration cap; saturates to top on cycles |
| Blocker #2: business under OR underspecified | Business leaves preserved as conditional_universal; under OR with caller_authority leaves, structural OR rendered faithfully (owner OR conditional-universal-on-business); admission still "tree contains caller/delegated authority" but capability isn't owner-only |
| High #3: union table wrong for upper_bound | Combinator table fully enumerated by quality cross-product; mixed lower/upper stays structural |
| High #4: operator-driven evaluator not specified | `_evaluate_leaf` dispatches on `operator` for membership/external_bool (truthy → cap, falsy → cap.negate) and equality (eq → base, ne → base.negate) |
| High #5: probe routes need auth + descriptor not from client | Two routes: `/probe/membership` (address) and `/probe/signature` (EIP-1271 hash+sig); auth via require_admin_key; rate limited; cached; descriptor fetched server-side by predicate_index |
| High #6: week-0 script not runnable | Script defined to query `artifacts` table for `stage_timing_static` rows, decode inline-or-storage payloads via `get_artifact()` |
| Medium #7: AC-shaped detection too loose | Detection moved into AC adapter's `matches()`: requires hasRole selector AND (RoleGranted topic OR successful getRoleAdmin call). Static stage doesn't decide; adapter does |
| Medium #8: function-pointer dispatch shouldn't be blindly unsupported | RevertDetector case 7 lets provenance classify both operands; emits business equality unless source is opaque |
| Medium #9: reentrancy/pause classifiers need uniqueness | Dominance + post-dominance checks; HIGH/MEDIUM/LOW confidence; LOW falls into business_toggle |
| Phasing | 10 weeks, week 1+2 split for provenance vs predicate-build, week 3 isolated for classifiers, week 5 narrowed to capabilities |
| Backfill | Decision: scan from contract deployment block; expose lower_bound during backfill |

## Open questions for codex (round 4)

1. Property test coverage for combinators — are there equational
   identities I should pin (e.g., `intersect(A, A) = A`,
   `intersect(A, universe) = A`, associativity, commutativity) or is
   per-cross-product asserting enough?
2. ProvenanceEngine's bounded recursion depth (4) for InternalCall —
   is that the right number, or should it be configurable per-pipeline-run?
3. The new `<CapTree />` UI component: should AND boxes default
   collapsed or expanded? Auth panels can get tall fast on real protocols.
4. RoleDomain `manual_pinned` source — what's the admin curation flow?
   A YAML manifest under `services/resolution/role_pins/` checked into
   the repo, or DB rows?
5. Would you sign off as implementable-as-written if v4.1 covers any
   round-4 nits, or is there any remaining structural concern?
