# Generic predicate-based access analysis — implementation plan

## Goals (what "generic" means)

1. **Static stage emits structural predicates only.** No `kind == "<shape-name>"` switch
   ladders. No identifier-name substring heuristics in any layer that controls
   admission, sink detection, or role mapping. Output is a tree of typed
   predicate nodes capturing the IR-level gate exactly.
2. **Resolver evaluates predicates without per-shape branches.** The resolver
   loop reads a predicate node, classifies operand sources structurally,
   binds parameters, and queries a uniform set-adapter interface. Standard
   ABI knowledge (`hasRole`, `getRoleMember`, `RoleGranted`, `getOwners`,
   `canCall`) lives only inside set-adapter implementations, behind one
   interface signature.
3. **UI renders parametric states as first-class.** The frontend grows new
   states beyond `direct.length === 0 → "Unresolved"`: per-key principal
   sets, partial enumerations, signature-auth, time-gated, etc.
4. **No backwards compatibility.** Schema breaks cleanly. Existing artifacts
   are reprocessed. No dual-emission, no heuristic-shadow paths.
5. **Robustness over surface area.** Land a small predicate vocabulary
   that handles 90% of real cases correctly, with explicit
   "non-enumerable" handling for the rest. Better to say "we know this is
   gated but cannot enumerate" than to silently regress.

## Predicate IR — the central data structure

### Boolean tree (mandatory)

Predicates form an expression tree:

```python
class PredicateTree(TypedDict):
    op: Literal["AND", "OR", "NOT", "LEAF"]
    children: list[PredicateTree]  # for AND/OR/NOT
    leaf: LeafPredicate | None     # for LEAF
```

Multiple `require()` statements in one function fold into an AND root with
each leaf as a child. `require(A || B)` is OR. `require(!_blacklist[…])` is
a NOT around a LEAF. Without the tree, disjunctions and negations cannot be
faithfully represented and the resolver collapses to a wrong principal set.

### LeafPredicate — five shapes, all structural

```python
class LeafPredicate(TypedDict):
    kind: Literal[
        "membership",       # mapping[k1][k2]...[kN] / set membership
        "equality",         # operand_a == operand_b
        "comparison",       # operand_a < operand_b, etc.
        "external_bool",    # external_call(...) returning bool
        "signature_auth",   # ECDSA.recover(...) == operand
    ]
    operands: list[Operand]
    set_descriptor: SetDescriptor | None  # only for "membership"
    references_msg_sender: bool
    parameter_indices: list[int]
    expression: str            # human-readable IR string for debug + UI fallback
    basis: list[str]           # IR fragment trail for diagnostics
```

**These five `kind` values are NOT shape labels in the bad sense.** They
correspond to genuinely distinct IR-level operations (BinaryOp eq vs.
mapping Index vs. external HighLevelCall returning bool vs. ECDSA precompile
call). The resolver dispatches on them but does not enumerate auth
*patterns* — those are encoded by the operand sources, not by `kind`.

### Operand — where each value comes from

```python
class Operand(TypedDict):
    source: Literal[
        "msg_sender",
        "parameter",
        "state_variable",
        "constant",
        "view_call",        # call to a view fn on this contract
        "external_call",    # call to a fn on a different contract
        "computed",          # arithmetic / hash / abi.encode
        "block_context",    # block.timestamp, block.number, etc.
    ]
    parameter_index: int | None
    parameter_name: str | None
    state_variable_name: str | None
    callee: str | None                    # for view_call / external_call
    callee_args: list[Operand] | None     # recursive — args carry their own taint
    constant_value: str | None
    computed_kind: str | None             # "keccak256", "abi.encode", "add", etc.
```

Operands are recursive: `_getRoleAdmin(role)` is `view_call` with
`callee_args=[parameter#0]`, so the resolver sees the chain without
re-walking IR. The `computed` source covers things like
`keccak256(abi.encodePacked(role, account))` — common in custom ACL
contracts.

### SetDescriptor — instructions for membership predicates

```python
class SetDescriptor(TypedDict):
    kind: Literal[
        "mapping_membership",  # n-key mapping → bool / uint flag
        "array_contains",      # storage array contains value
        "external_set",        # external view enumerates a set
    ]
    storage_var: str | None              # for mapping/array
    key_sources: list[Operand]           # for mapping_membership; len = n keys
    truthy_value: str | None             # "1" for Maker wards, omitted for bool
    enumeration_hint: list[EventHint]    # events that mutate this set
    external_set_call: ExternalSetCall | None  # for external_set
```

The `enumeration_hint` field is populated by the existing
`mapping_events.py:198-237` detector. Each entry says: "writes to this
storage var emit event E with key positions [...]". The resolver replays
those events to enumerate the current set.

## Static stage implementation

### Files touched

| File | Change |
|---|---|
| `schemas/contract_analysis.py` | Add `parametric_guards: PredicateTree` field on PrivilegedFunction. **Drop** `guards`, `controller_refs`, `sinks` — they're replaced. |
| `services/static/contract_analysis_pipeline/predicates.py` | **New.** PredicateTree builder. Walks IR, produces tree from a function's gate. ~200 lines. |
| `services/static/contract_analysis_pipeline/caller_sinks.py` | Strip name-matching at L37-54, L262-305, L386-459. `caller_reach_analysis` returns `PredicateTree | None` instead of bool + sinks list. |
| `services/static/contract_analysis_pipeline/summaries.py` | **Drop** `_internal_auth_calls` (L53-180), `_build_method_to_role_map` name path (L825-865), `_detect_access_control` inheritance match (L868-904), and the inferred-guards admission gate (L920-921). Admission becomes: "predicate tree non-empty." |
| `services/static/contract_analysis_pipeline/semantic_guards.py` | **Delete entirely.** Shape-label collapsing layer; no longer needed. |
| `services/static/contract_analysis_pipeline/graph.py` | Strip `_looks_like_external_authority_call` keyword gate (L93-114) and `checkCallerIsX` regex (L708-733). Keep bytes32 role aliasing (L210-320) — that's structural. External-call guards build from the predicate tree's `external_bool` leaves, not keyword match. |
| `services/static/contract_analysis_pipeline/mapping_events.py` | **Keep, extend.** Already structural. New: emit `EventHint` records into `SetDescriptor.enumeration_hint`. |

### Admission gate — name-free

```python
# summaries.py — replaces L920-921
predicate_tree = build_predicate_tree(function, slither_ir)
if predicate_tree is None or _trivially_satisfiable(predicate_tree):
    continue  # not access-controlled
privileged_functions.append({
    "function": function.full_name,
    "parametric_guards": predicate_tree,
    "writes": writes_summary(function),
    ...
})
```

`_trivially_satisfiable(tree)`: returns True for predicate trees that allow
any caller (e.g., `require(true)`, `require(block.timestamp > 0)`). Keeps
overinclusive admission honest. A handful of unit tests pin its behavior.

### Boolean tree construction

`build_predicate_tree` walks all revert paths in the function. For each
revert path:
- Collect all `require`/`assert` conditions and `if (X) revert(...)` checks
  as leaf predicates.
- Within one `if (A && B)` branch, leaves combine as AND.
- Within `if (A || B)`, leaves combine as OR.
- `if (!X) revert` is the leaf X under NOT.

Multiple sequential `require()` statements in the same function fold as
AND at the tree root. Modifier expansion already happens at the IR level
(Slither inlines modifier bodies before emitting IR), so modifier-based
gates appear as predicates in the function body without special casing.
Validate this on real-contract fixtures before merging.

### Operand classification (the structural core)

For each operand of a leaf predicate, walk back through the data-flow until
hitting a source. The existing `caller_sinks.py` taint infrastructure
already does this for `msg.sender`; we generalize to all sources. Implementation
sketch:

```python
def classify_operand(value, function_context) -> Operand:
    if value is msg.sender: return Operand(source="msg_sender")
    if value is a parameter: return Operand(source="parameter", parameter_index=..., parameter_name=...)
    if value is a state-var read: return Operand(source="state_variable", state_variable_name=...)
    if value is a constant: return Operand(source="constant", constant_value=...)
    if value is the result of an internal call: ...recurse on the callee body...
        return Operand(source="view_call", callee=..., callee_args=[classify_operand(a) for a in args])
    if value is the result of an external call:
        return Operand(source="external_call", callee=..., callee_args=[...])
    if value is computed (keccak/abi.encode/arithmetic):
        return Operand(source="computed", computed_kind=..., callee_args=[recurse on each input])
    if value is from block context:
        return Operand(source="block_context", computed_kind="block.timestamp", ...)
    raise StructuralUnknown(value)  # rare; logged for triage
```

`StructuralUnknown` exits the predicate emission for that gate; the
function still admits (predicate tree may be partial), but the leaf is
marked `kind="unknown"` so the resolver knows to flag the whole gate as
non-evaluable. Log every occurrence — these are the cases that need
new structural support.

## Resolver stage implementation

### Files touched

| File | Change |
|---|---|
| `services/resolution/predicates.py` | **New.** Predicate evaluator. Reads `PredicateTree`, returns a principal expression. ~250 lines. |
| `services/resolution/adapters/__init__.py` | **New.** SetAdapter Protocol + AdapterRegistry. ~80 lines. |
| `services/resolution/adapters/access_control.py` | **New.** OZ AccessControl + AccessControlEnumerable adapter. ~150 lines. |
| `services/resolution/adapters/safe.py` | **New.** Gnosis Safe `getOwners()` adapter. ~80 lines. |
| `services/resolution/adapters/dsauth.py` | **New.** DSAuth/Aragon ACL adapter. ~120 lines. |
| `services/resolution/adapters/event_indexed.py` | **New.** Generic event-replay adapter that reads from `role_grants`-like tables for ANY contract emitting trackable membership events. ~180 lines. |
| `services/resolution/recursive.py` | Strip Safe/Timelock/ProxyAdmin nested-principal switch (L713-805). Replace with: evaluate each privileged function's predicate via `predicates.py`, attach result. |
| `services/resolution/tracking.py` | Strip `role_identifier` special-case (L447-495) and Safe/Timelock probe ladder (L178-186, L343-375). Tracking spec built from predicate tree. |
| `services/resolution/controller_adapters.py` | Move probe ladder (L82-132, L218-349, L374-387) into per-adapter `detect()` methods; remove the ladder from the resolver loop. |

### SetAdapter interface

```python
class SetDescriptor(TypedDict):
    kind: str                  # from static stage
    contract_address: str
    chain_id: int
    storage_var: str | None
    key_sources: list[ResolvedOperand]    # parameters bound to actual values where possible
    enumeration_hint: list[EventHint]
    external_set_call: ExternalSetCall | None

class EnumerationResult(TypedDict):
    members: list[str] | None     # populated when fully enumerable
    confidence: Literal["enumerable", "partial", "check_only"]
    partial_reason: str | None
    last_indexed_block: int | None

class SetAdapter(Protocol):
    @classmethod
    def matches(cls, descriptor: SetDescriptor, contract_meta: ContractMeta) -> int:
        """Return a match score 0-100. Resolver picks highest scorer."""
    def enumerate(self, descriptor: SetDescriptor, block: int) -> EnumerationResult: ...
    def membership(self, descriptor: SetDescriptor, member: Address, block: int) -> bool | None: ...
```

Adapters self-register at import. Resolver picks the best-scoring adapter
for each descriptor. Score is structural: AccessControlEnumerable scores
100 if `getRoleMember` selector is present in the bytecode, else 0;
`event_indexed` scores 50 if the descriptor has at least one
`enumeration_hint`; etc. **No naming.**

### Predicate evaluator

```python
def evaluate(tree: PredicateTree, ctx: EvaluationContext) -> PrincipalExpression:
    if tree["op"] == "LEAF":
        return _evaluate_leaf(tree["leaf"], ctx)
    children = [evaluate(c, ctx) for c in tree["children"]]
    if tree["op"] == "AND":
        return PrincipalExpression.intersect(children)
    if tree["op"] == "OR":
        return PrincipalExpression.union(children)
    if tree["op"] == "NOT":
        return PrincipalExpression.complement(children[0])

def _evaluate_leaf(leaf: LeafPredicate, ctx) -> PrincipalExpression:
    if leaf["kind"] == "equality":
        return _resolve_equality(leaf["operands"], ctx)  # principal set is one operand
    if leaf["kind"] == "membership":
        adapter = ctx.adapter_registry.pick(leaf["set_descriptor"], ctx.contract_meta)
        result = adapter.enumerate(leaf["set_descriptor"], ctx.block)
        return PrincipalExpression.from_set(result)
    if leaf["kind"] == "external_bool":
        # follow the external call into another contract's analysis if available
        return _resolve_external_bool(leaf, ctx)
    if leaf["kind"] == "signature_auth":
        # principal is the operand the signature is required to match
        return _resolve_signature_auth(leaf, ctx)
    if leaf["kind"] == "comparison":
        # block.timestamp > X → time gate, not a caller gate
        return PrincipalExpression.universal_with_condition(...)
    raise UnknownLeafKind
```

`PrincipalExpression` carries: a set (when enumerable), a confidence level,
a list of conditions (e.g., "after block N"), and metadata for the UI
(per-parameter expansion when the descriptor was parametric).

### role_grants table

Needed because monitoring already decodes `RoleGranted`/`RoleRevoked` events
(`event_topics.py:144-156`), but the data isn't persisted in a queryable
shape. Schema:

```sql
CREATE TABLE role_grants (
    contract_id INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    role        BYTEA NOT NULL,           -- bytes32
    member      VARCHAR(42) NOT NULL,     -- address
    granted_block BIGINT NOT NULL,
    revoked_block BIGINT,                 -- NULL = currently held
    chain_id    INTEGER NOT NULL,
    PRIMARY KEY (contract_id, role, member, granted_block)
);
CREATE INDEX role_grants_active ON role_grants (contract_id, role) WHERE revoked_block IS NULL;

CREATE TABLE role_grants_cursor (
    contract_id INTEGER PRIMARY KEY REFERENCES contracts(id) ON DELETE CASCADE,
    last_indexed_block BIGINT NOT NULL,
    confirmation_depth INTEGER NOT NULL DEFAULT 12
);
```

Indexer runs as a worker pass after resolution, replays
`RoleGranted`/`RoleRevoked` from `last_indexed_block - confirmation_depth`
to head, folds into the table. Reorg-safe via the cursor + confirmation
depth. Same model the existing controller-value worker uses; reuse its
RPC layer.

### Time / block-context predicates

Compare predicates whose only varying operand is `block.timestamp` or
`block.number` are time gates, not caller gates. The evaluator returns
`PrincipalExpression.universal_with_condition(time_predicate)`. UI renders
"anyone, after T = …". Static stage marks the leaf
`kind="comparison"` with `operands=[block_context, constant]` —
no special casing.

## UI changes

`site/src/protocolScore.js` and `site/src/auditUi.jsx`: the existing
`direct.length === 0 → "Unresolved"` branch must read `parametric_guards`
before falling back. New rendered states:

| Backend signal | UI label |
|---|---|
| `direct.length > 0` | unchanged: list of addresses |
| `parametric_guards` non-empty AND adapter returned `enumerable` per-key | "Per-{role}: \[addresses\]" expandable list |
| `parametric_guards` non-empty AND adapter returned `partial` | "Partial: known holders \[…\]; may have additional members" |
| `parametric_guards` non-empty AND adapter returned `check_only` | "Membership-checkable; query by address" |
| Predicate has `signature_auth` leaf | "Gated by signature from \[address\]" |
| Predicate has time-only `comparison` leaf | "Anyone, after \[time\]" |
| Predicate emission failed (StructuralUnknown) | "Gated (structure not yet supported); raw IR available" |
| Predicate tree empty | unchanged: not access-controlled |

## Test strategy

### Synthetic fuzz (existing 41 tests)

`test_parametric_guard_fuzz.py` — already in place, label-free assertion.
Should flip from 41 xfail to 41 pass when predicate emission lands. Strict
xfail forces removing the decorator.

### Real-contract corpus pinning (NEW)

Pin `effective_permissions` snapshots for 5 real protocols at fixed blocks:
- EtherFi (the original symptom)
- AaveV3
- MakerDAO Vat (wards-style)
- Gnosis Safe (signature-set membership)
- Compound III (Comet — custom ACL)

Test: load snapshot, run new pipeline against the project fixture, diff.
Pre-merge: every diff is reviewed manually. The pinned snapshots become
the regression bar.

### Unit tests

| Module | Coverage |
|---|---|
| `predicates.py` (static) | Boolean tree construction, modifier expansion, operand classification, all 5 leaf kinds, NOT/AND/OR composition, StructuralUnknown handling |
| `predicates.py` (resolver) | Evaluator on each leaf kind, intersect/union/complement, parametric-key expansion, partial-enumeration confidence propagation |
| Each adapter | `matches()` scoring, `enumerate()` against fixture chain state, `membership()` correctness |
| `role_grants` indexer | Reorg simulation: confirmation depth respects rollback, no double-count |

### End-to-end

Add a live-marker integration test: spin up etherfi project end-to-end,
hit `/api/contract/<addr>/permissions`, assert grantRole resolves to
non-empty per-role principal sets including the actual default-admin
holders.

## Phasing

We don't need backcompat, but we do want each phase to leave the system
demonstrably better than the last so we can ship incrementally and bisect.

**Phase 1 — Predicate IR + emitter (1-2 weeks).** Schema, predicates.py
(static), strip name-matching from caller_sinks/summaries/graph,
delete semantic_guards.py, fuzz xfails flip. Resolver still uses
existing logic on existing fields, so live UI may regress — gate behind
`PSAT_PREDICATE_EMISSION=1` env flag for pre-prod rollout.

**Phase 2 — Adapter interface + 3 core adapters (1 week).** SetAdapter
protocol, AccessControl, Safe, DSAuth/Aragon. Predicate evaluator with
membership/equality leaf kinds. role_grants table + indexer. Live
integration test: etherfi grantRole resolves.

**Phase 3 — Remaining leaf kinds + UI (3-5 days).** external_bool,
signature_auth, comparison/time-gates. UI states for all 7 outcomes.
Real-contract corpus pinned for the 5 protocols above.

**Phase 4 — Cleanup (2-3 days).** Remove `PSAT_PREDICATE_EMISSION` flag.
Drop `controller_adapters.py` probe ladder. Verify per-adapter `matches()`
fully replaces it. Final corpus diff review.

Each phase is one PR, with the corpus diff attached for manual review.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Phase 1 lands but predicate emission misses cases the heuristic caught → silent regression in admission set | Phase 1's PR includes a CI check: for each fixture in tests/live/projects/, the privileged_functions set must be a superset of the pre-merge set. Fail the PR otherwise. |
| Modifier expansion doesn't compose (predicate emitter only sees inline `require`) | Add fixture explicitly testing modifier-only access: a contract using `onlyRole(BLAH)` modifier with no inline checks. Verify predicate emission. |
| Boolean tree explodes on functions with many short-circuited checks | Cap tree depth at 8 with a `truncated: true` field. Fall back to "non-evaluable" for deeper trees (extremely rare in practice). |
| Adapter scoring picks wrong adapter for a hybrid contract | `matches()` returns score; ties broken by registration order. Per-adapter unit test on hybrid fixtures (e.g., AccessControlEnumerable + custom Safe wrapping). |
| role_grants reorg handling races during chain reorgs | Cursor + confirmation depth (12 blocks ≈ 3 minutes on mainnet). Indexer holds an advisory lock per contract during fold. |
| External-bool predicates depend on contracts not yet analyzed | Recursive resolver already handles cross-contract resolution; predicate evaluator delegates to it for `external_bool` leaves. Returns `partial` if target unanalyzed. |
| Live UI regression while phase 1 is shipping | env flag `PSAT_PREDICATE_EMISSION=0` keeps prod on the heuristic path until phase 2 lands and corpus diffs review clean. |

## Out of scope (explicitly)

- Optimistic / governance-vote gating (`require(governorVoted(proposalId))`):
  treated as `external_bool` leaf; concrete principal resolution is "the
  governor's voter set," handled by recursive resolver delegation.
- Encrypted / off-chain auth (zk proofs, attestations): predicate IR
  records the call shape; resolver returns `check_only`.
- Cross-chain authorization (LayerZero, Wormhole): same; recorded as
  `external_bool` with partial confidence.
- The `_internal_auth_calls` substring layer in summaries.py is **deleted,
  not preserved**, per the no-backcompat directive. Any contract that was
  admitting solely on that heuristic and not on a structural predicate
  will fall out of `privileged_functions`. The corpus-superset CI check
  catches this; if a real protocol regresses, the fix is to extend
  predicate emission to capture that structure, not to restore the
  heuristic.

## Open questions for codex

1. The 5 leaf kinds — is anything missing? Specifically: are there
   common access patterns (e.g., bitwise role flags, EIP-2535 diamond
   facet selectors) that don't fit cleanly into membership / equality /
   external_bool?
2. The `computed` operand source — is recursing into arithmetic /
   keccak / abi.encode the right level of generality, or should we draw
   the line earlier and just record the IR fragment as opaque?
3. Tree depth cap of 8 — too aggressive, too lax, or wrong axis (depth
   is a poor proxy for complexity)?
4. role_grants confirmation depth of 12 blocks — should this be
   per-chain configurable from the start, or is mainnet-only fine for v1?
5. Adapter scoring as integers 0-100 with registration-order tiebreak —
   or is there a cleaner discriminator (e.g., "must score uniquely; no
   ties allowed")?
6. The corpus-superset CI check — is "must be a superset" the right bar
   or should we go strictly equal? Superset accepts new admissions
   (good), but might also accept overinclusive admissions silently.
