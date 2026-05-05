# Generic predicate-based access analysis — v2 (post-codex review)

This revision folds every codex finding from v1. Diff vs v1 is at the bottom.

## Goals

1. **Static stage emits structural predicates only.** No name-based admission, no shape labels for routing decisions. Output is a typed predicate tree.
2. **Predicates carry authority semantics.** Each predicate is annotated with how its caller-dependence works — only caller- or authority-dependent predicates admit a function as privileged. Non-caller predicates (reentrancy, pause, time, business invariants) are recorded but don't admit.
3. **Resolver evaluates capability expressions, not flat principal sets.** Capability expressions express threshold groups, cofinite blacklists, signature witnesses, and check-only sets natively — no fake set-algebra.
4. **Standard ABI knowledge lives in adapters behind one interface,** with full evaluation context (chain, RPC, block, finality). Every standard not yet supported has an explicit `unsupported(reason)` capability with a fixture pinning that status.
5. **No backwards compatibility.** One schema-v2 cutover. All consumers (policy, resolver, UI) ship together. No dual-emission, no flag-shadowed paths.
6. **Robustness over surface area.** Land a small, sound vocabulary that handles ~80% of real protocols correctly. Anything unsupported flips to a typed unsupported leaf — never silently to "Unresolved" or to a wrong principal.

## Predicate IR — corrected

### Boolean tree

```python
class PredicateTree(TypedDict):
    op: Literal["AND", "OR", "LEAF"]
    children: list[PredicateTree] | None  # for AND/OR
    leaf: LeafPredicate | None            # for LEAF
```

`NOT` is gone as a tree node. Negation is captured at the leaf level via
`equality.negated: bool` and `membership.negated: bool`. This avoids the
v1 trap where `NOT(membership)` looked like an enumerable principal set
but actually requires a cofinite-blacklist representation. With negation
on the leaf, the capability layer can decide its representation locally.

**Polarity normalization (codex blocker #2 fix).** `require(E)` ⇒ allowed
condition is `E`. `if (B) revert` ⇒ allowed condition is `NOT(B)`, which
is then pushed into leaves: `NOT(membership)` ⇒ `membership(negated=true)`,
`NOT(equality)` ⇒ `inequality leaf with operands swapped`. After
normalization no `NOT` node survives. This is the rule the predicate
builder applies before emitting.

### LeafPredicate — six structural kinds

```python
class LeafPredicate(TypedDict):
    kind: Literal[
        "membership",
        "equality",
        "comparison",
        "external_bool",
        "signature_auth",
        "unsupported",   # first-class — codex blocker #3 fix
    ]
    authority_role: Literal[
        "caller_authority",     # gate depends on msg.sender / tx.origin / signature
        "delegated_authority",  # gate depends on an external authority that depends on caller
        "time",                 # block.timestamp / block.number conditions
        "reentrancy",           # nonReentrant pattern
        "pause",                # whenNotPaused / whenPaused
        "business",             # deadline, amount > 0, balance >= x
    ]
    negated: bool                          # leaf-level negation, codex finding #2 fix
    operands: list[Operand]
    set_descriptor: SetDescriptor | None   # only for "membership"
    unsupported_reason: str | None         # only for "unsupported"; pinned + tested
    references_msg_sender: bool
    parameter_indices: list[int]
    expression: str
    basis: list[str]
```

`authority_role` is the codex blocker #1 fix. Admission is now: a function
is privileged iff its predicate tree contains at least one leaf whose
`authority_role ∈ {"caller_authority", "delegated_authority"}`. Reentrancy
guards, pause checks, deadline checks, and business invariants annotate
the function but never admit it on their own.

`authority_role` is **not** a name-based heuristic — it's classified
structurally:
- A leaf is `caller_authority` if any operand traces (transitively
  through view_call / external_call args / computed inputs) back to
  `msg.sender`, `tx.origin`, or a signature recovery output.
- A leaf is `delegated_authority` if any operand traces to an external
  call returning bool whose call args include `msg.sender`/recovered
  signer and the call target is read from a state variable.
- `time` if the only varying operand source is `block_context`
  (timestamp/number).
- `reentrancy` if it's a check on a known reentrancy-status state-var
  pattern: a uint8/bool with two writes — set before the gated call,
  reset after — and the leaf reads it. Identified structurally via
  `mapping_events.py`-style writer detection, no name match. (`Pausable`
  pattern is the same shape with different transition trigger; it
  classifies as `pause`.)
- `business` is the residual.

If the classifier can't decide, the leaf becomes `kind="unsupported"`
with `authority_role="business"` so it doesn't accidentally admit. Every
unsupported leaf is logged for triage and pinned in tests.

### unsupported leaves — first-class

The v1 mistake was raising `StructuralUnknown` from the emitter and
`UnknownLeafKind` from the evaluator with no schema slot. v2 makes
`unsupported` a real leaf kind with a `unsupported_reason` field. The
evaluator returns `Capability.unsupported(reason)` for them, which
propagates fail-closed through AND (intersection) but is preserved as a
disjunct through OR (because the OR'd alternatives may be enumerable).
Fixtures pin the expected `unsupported_reason` for every pattern not yet
fully covered (Diamond ACL, EIP-1271, bitwise flags, etc.).

### Operand — recursive, with explicit polarity-safe shapes

```python
class Operand(TypedDict):
    source: Literal[
        "msg_sender", "tx_origin", "parameter", "state_variable",
        "constant", "view_call", "external_call", "computed",
        "block_context", "signature_recovery",
    ]
    parameter_index: int | None
    parameter_name: str | None
    state_variable_name: str | None
    callee: str | None
    callee_args: list[Operand] | None
    constant_value: str | None
    computed_kind: str | None
    block_context_kind: str | None  # "timestamp", "number", "chainid", "coinbase"
```

`signature_recovery` is its own source kind so signature-auth predicates
can be classified without name-matching ECDSA/SignatureChecker.
Detection is structural: the operand is the return of a precompile call
to address `0x01` (ecrecover) or a HighLevelCall to a contract whose
call selector matches the EIP-1271 `isValidSignature` shape.

### SetDescriptor — full enough for adapters (codex finding #7 fix)

```python
class SetDescriptor(TypedDict):
    kind: Literal[
        "mapping_membership",
        "array_contains",
        "external_set",
        "bitwise_role_flag",       # for bitwise_flag leaves
        "diamond_facet_acl",       # for Diamond storage
    ]
    storage_var: str | None
    storage_slot: str | None       # for diamond, eip-1967, etc.
    key_sources: list[Operand]
    truthy_value: str | None
    enumeration_hint: list[EventHint]
    authority_contract: AuthorityContract | None  # for delegated_authority
    role_domain: RoleDomain | None
    selector_context: SelectorContext | None      # for diamond/canCall

class AuthorityContract(TypedDict):
    address_source: Operand        # how to read the authority addr (e.g., kernel())
    abi_hint: str | None           # "aragon_acl", "dsauth", "oz_access_control"

class RoleDomain(TypedDict):
    """Codex finding #10 fix: how to enumerate the parametric role argument."""
    sources: list[Literal[
        "compile_time_constants",  # bytes32 constants in source
        "role_granted_history",    # any role ever seen in RoleGranted logs
        "abi_declared",            # roles enumerated by getRoleMember loops
        "manual_pinned",           # admin-curated set
    ]]
    parameter_index: int           # which fn arg this domain is for

class SelectorContext(TypedDict):
    selectors: list[str]           # bytes4 selectors when guard is per-selector
```

This is fat enough that adapters don't need to re-derive context.
Authority-contract source, role-domain provenance, selector context all
travel with the descriptor.

### EventHint — reorg-grade

```python
class EventHint(TypedDict):
    event_address: str             # contract emitting the event (often != subject)
    topic0: str                    # event signature hash
    topics_to_keys: dict[int, int] # topic_position → key index
    data_to_keys: dict[int, int]   # data_position → key index
    direction: Literal["add", "remove"]
    key_value_taint: str | None    # for value-bearing events (e.g. ward[ilk][user] = 1)
```

This matches the `mapping_events.py` shape. Adapters use it to do
event-replay enumeration without per-protocol code.

## Capability expression — replaces "principal set" (codex finding #8 fix)

The v1 plan said the resolver returns a `PrincipalExpression` and used
ordinary set algebra (intersect/union/complement). Codex correctly
flagged that complement over the address space is intractable, OR with
signature_auth is not finite, and Safe ownership is a threshold group not
a flat set.

```python
class CapabilityExpr:
    kind: Literal[
        "finite_set",            # exact set of addresses
        "threshold_group",       # M-of-N (Safe, multi-sig)
        "cofinite_blacklist",    # everyone except this set (rare; from negated membership)
        "signature_witness",     # anyone with a valid signature from <signer expr>
        "external_check_only",   # query-only (EIP-1271, oracle policy)
        "conditional_universal", # anyone, given <condition> (time gates, deadlines)
        "unsupported",           # explicit
        "AND", "OR",
    ]
    members: list[str] | None             # finite_set
    threshold: tuple[int, list[str]] | None  # threshold_group: (M, signers)
    blacklist: list[str] | None           # cofinite_blacklist
    signer: 'CapabilityExpr | None'       # signature_witness
    check: ExternalCheck | None           # external_check_only
    condition: TimeCondition | None       # conditional_universal
    unsupported_reason: str | None
    children: list['CapabilityExpr'] | None  # AND/OR
    confidence: Literal["enumerable", "partial", "check_only"]
    last_indexed_block: int | None
```

Combinators only do what's actually defined:
- `AND(finite_set_A, finite_set_B) = finite_set(A ∩ B)`
- `AND(finite_set, conditional_universal) = finite_set` (intersect with universal is identity, but condition is preserved as side data)
- `AND(threshold_group, …)` raises `UnsupportedCapability` because intersecting a threshold with an arbitrary capability has no clean meaning. UI shows AND of both.
- `OR(finite_set_A, finite_set_B) = finite_set(A ∪ B)` if confidences match
- `OR(finite_set, signature_witness) = OR(finite_set, signature_witness)` — kept as a structural OR; UI renders both.
- Negated membership produces `cofinite_blacklist`.
- Anything else: stay as the structural combinator and let UI render it.

Safe ownership lands as `threshold_group(M, owners)`, never as a flat
finite_set. Tracking-side classification (`tracking.py:343`) already
records the threshold separately — we now route that into the capability
type instead of squashing it.

## Static stage implementation (revised)

### Files touched

| File | Change |
|---|---|
| `schemas/contract_analysis.py` | Replace `PrivilegedFunction.controller_refs/guards/sinks` with `predicate_tree: PredicateTree`. Bump `schema_version` to `"2.0"`. |
| `services/static/contract_analysis_pipeline/predicates.py` | **New.** PredicateTree builder + polarity normalization + authority_role classifier. ~350 lines. |
| `services/static/contract_analysis_pipeline/caller_sinks.py` | Strip name-matching at L37-54, L262-305, L386-459. `caller_reach_analysis` is reused by `predicates.py` (msg.sender taint is its core), but no longer emits sinks/shape-labels. Delete `_classify_caller_*` helpers. |
| `services/static/contract_analysis_pipeline/summaries.py` | Delete `_internal_auth_calls` (L53-180), `_build_method_to_role_map` (L825-865), `_detect_access_control` (L868-904). Admission becomes: predicate tree contains a `caller_authority` or `delegated_authority` leaf. |
| `services/static/contract_analysis_pipeline/semantic_guards.py` | **Delete entirely.** |
| `services/static/contract_analysis_pipeline/graph.py` | Drop `_looks_like_external_authority_call` (L93-114) and `checkCallerIsX` (L708-733). Keep bytes32 role aliasing (L210-320). |
| `services/static/contract_analysis_pipeline/mapping_events.py` | Extend output → `EventHint` records. |

### Modifier handling (codex finding #6 fix)

`predicates.py` walks: function body, each modifier in `function.modifiers`,
each internal/library callee invoked from those bodies (with parameter
substitution). The current `caller_sinks.py:600` modifier-walk pattern is
the template. **No assumption that Slither inlines modifiers.** Tests
include a contract with no inline `require` and access entirely via
`onlyRole` modifiers — explicit fixture, asserted to admit.

Recursion depth caps:
- Function body + modifier expansion: unlimited (modifiers are usually shallow).
- View-call recursion in operand classification: depth 4 (deeper is real but rare; capped to bound work).
- Predicate tree depth: capped at 8 short-circuit branches; deeper truncates the tree and inserts an `unsupported(reason="predicate_too_deep")` leaf.

### Fixture corpus for emitter

Pinned tests for every supported and explicitly-unsupported pattern:
- OZ AccessControl `grantRole` (membership, parametric)
- OZ AccessControlEnumerable
- OZ Ownable (`onlyOwner`)
- Maker `wards` (membership with `==1`)
- Aragon ACL (`canPerform` external_bool with `delegated_authority`)
- DSAuth (`auth` modifier → `external_bool`)
- Gnosis Safe (`execTransaction` → threshold_group via signature_auth)
- Diamond ACL via `LibDiamond` (currently → `unsupported(reason="diamond_acl_v1")`)
- EIP-1271 contract sigs (currently → `unsupported(reason="eip1271")`)
- Bitwise role flags (currently → `unsupported(reason="bitwise_role_flag")`)
- Permit-style `permit()` flow (annotated, but **not admitted** since it's asset-authority not function-caller authority)
- Reentrancy guard `nonReentrant` (annotated `authority_role="reentrancy"`, does not admit)
- Pausable `whenNotPaused` (annotated `authority_role="pause"`, does not admit on its own)

Each fixture asserts the exact predicate tree emitted, including
`authority_role` annotations. `unsupported` reasons are pinned strings.

## Resolver stage implementation (revised)

### Files touched

| File | Change |
|---|---|
| `services/resolution/predicates.py` | **New.** Capability evaluator. ~400 lines. |
| `services/resolution/capabilities.py` | **New.** `CapabilityExpr` types + safe combinators. ~150 lines. |
| `services/resolution/adapters/__init__.py` | **New.** `SetAdapter` Protocol + registry + `EvaluationContext`. ~120 lines. |
| `services/resolution/adapters/access_control.py` | OZ + Enumerable. |
| `services/resolution/adapters/safe.py` | Returns threshold_group. |
| `services/resolution/adapters/dsauth_aragon.py` | Authority-contract resolution + ACL log replay. |
| `services/resolution/adapters/event_indexed.py` | Generic event-replay adapter for any descriptor with `enumeration_hint`. |
| `services/resolution/adapters/eip1271.py` | Returns `external_check_only`. |
| `services/resolution/recursive.py` | Strip Safe/Timelock/ProxyAdmin nested-principal switch (L713-805). Use predicate evaluator. |
| `services/resolution/tracking.py` | Strip `role_identifier` special case (L447-495) + Safe/Timelock probe ladder (L178-186, L343-375). Tracking spec built from predicate tree. |
| `services/resolution/controller_adapters.py` | Per-adapter `detect()` implementations replace the in-line probe ladder. |

### SetAdapter — fat enough (codex finding #7 fix)

```python
class EvaluationContext:
    chain_id: int
    rpc_url: str
    block: int
    finality_depth: int                  # per-chain, configured
    contract_meta: ContractMeta          # subject contract metadata
    role_grants: RoleGrantsRepo          # injected DB access
    recursive_resolver: RecursiveResolver  # for cross-contract follow-up

class SetAdapter(Protocol):
    @classmethod
    def matches(cls, descriptor: SetDescriptor, ctx: EvaluationContext) -> int:
        """0–100 score. Tied scores resolve to highest-priority registration; ties >0 logged as warnings."""

    def enumerate(self, descriptor: SetDescriptor, ctx: EvaluationContext) -> EnumerationResult:
        """Either populates the set or returns partial/check-only with a reason."""

    def membership(self, descriptor: SetDescriptor, ctx: EvaluationContext, member: Address) -> Trit:
        """Returns YES, NO, or UNKNOWN. Used when full enumeration isn't possible."""
```

`Trit` is `{YES, NO, UNKNOWN}` (an explicit three-state, not bool).

### role_grants — reorg-safe (codex blocker #5 fix)

```sql
CREATE TABLE role_grants_events (
    contract_id INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    role        BYTEA   NOT NULL,
    member      VARCHAR(42) NOT NULL,
    direction   VARCHAR(8)  NOT NULL CHECK (direction IN ('grant', 'revoke')),
    block_number BIGINT NOT NULL,
    block_hash  BYTEA   NOT NULL,
    tx_hash     BYTEA   NOT NULL,
    log_index   INTEGER NOT NULL,
    chain_id    INTEGER NOT NULL,
    PRIMARY KEY (chain_id, contract_id, tx_hash, log_index)
);
CREATE INDEX role_grants_events_member ON role_grants_events (chain_id, contract_id, role, block_number DESC);

CREATE TABLE role_grants_cursor (
    chain_id INTEGER NOT NULL,
    contract_id INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    last_indexed_block BIGINT NOT NULL,
    PRIMARY KEY (chain_id, contract_id)
);

CREATE TABLE chain_finality_config (
    chain_id INTEGER PRIMARY KEY,
    confirmation_depth INTEGER NOT NULL,  -- mainnet=12, polygon=64, arbitrum=20, base=24
    rpc_safe_block_alias VARCHAR(16)      -- "safe" / "finalized" / NULL
);
```

This is event-sourced — current state is derived from
`role_grants_events` by folding (`grant` adds, `revoke` removes; latest
event by `(block_number, log_index)` wins per `(role, member)`). Reorgs
are handled by:
1. Indexer takes advisory lock per `(chain_id, contract_id)`.
2. `SELECT ... FROM role_grants_cursor WHERE ... FOR UPDATE`.
3. Reads new `RoleGranted`/`RoleRevoked` from `last_indexed_block - confirmation_depth` to current head.
4. For each block in the reorg window, verifies `block_hash` against RPC; on mismatch deletes all rows whose `block_hash` no longer matches and re-fetches.
5. Inserts new rows; advances cursor.
6. Releases lock.

Per-chain `confirmation_depth` from `chain_finality_config` (codex finding
on confirmation depth — addressed). When RPC supports the `safe` /
`finalized` block alias, the adapter prefers it over confirmation depth.

`PRIMARY KEY (chain_id, contract_id, tx_hash, log_index)` makes
double-inserts impossible. Read queries against `role_grants_events` use
a current-state view:

```sql
CREATE VIEW role_grants_current AS
SELECT DISTINCT ON (chain_id, contract_id, role, member)
       chain_id, contract_id, role, member, direction, block_number
FROM role_grants_events
ORDER BY chain_id, contract_id, role, member, block_number DESC, log_index DESC;
```

The evaluator queries `role_grants_current` filtered to `direction='grant'`.
The query plan is index-only via the secondary index.

### Parametric role-domain discovery (codex finding #10 fix)

The descriptor's `role_domain.sources` enumerates how to discover the
universe of role values for a parametric role argument. The resolver
takes their union:

1. `compile_time_constants` — bytes32 constants from the source
   (already discovered by `_build_method_to_role_map`'s structural
   path; the name-based path goes away but the constant-extraction
   stays).
2. `role_granted_history` — `SELECT DISTINCT role FROM
   role_grants_events WHERE chain_id=? AND contract_id=?`. Captures
   roles never named in source but granted at runtime.
3. `abi_declared` — for AccessControlEnumerable, calls
   `getRoleMemberCount(role)` once per known role to populate.
4. `manual_pinned` — admin curation when needed.

Output: `{role_value: capability_expr_for_that_role}`. The UI renders a
per-role expansion. EtherFiTimelock `grantRole` works end-to-end via
sources 1+2.

### Predicate evaluator — sound (codex finding #8 + #11 fix)

```python
def evaluate(tree: PredicateTree, ctx: EvaluationContext) -> CapabilityExpr:
    if tree["op"] == "LEAF":
        return _evaluate_leaf(tree["leaf"], ctx)
    children = [evaluate(c, ctx) for c in tree["children"]]
    if tree["op"] == "AND":
        return CapabilityExpr.intersect(children)
    if tree["op"] == "OR":
        return CapabilityExpr.union(children)

def _evaluate_leaf(leaf: LeafPredicate, ctx) -> CapabilityExpr:
    if leaf["authority_role"] in ("reentrancy", "pause", "business"):
        return CapabilityExpr.conditional_universal(condition=leaf)
    if leaf["authority_role"] == "time":
        return CapabilityExpr.conditional_universal(condition=_time_condition(leaf))
    # caller_authority / delegated_authority
    if leaf["kind"] == "membership":
        adapter = ctx.adapter_registry.pick(leaf["set_descriptor"], ctx)
        result = adapter.enumerate(leaf["set_descriptor"], ctx)
        cap = CapabilityExpr.from_enumeration(result)
        return cap.negate() if leaf["negated"] else cap
    if leaf["kind"] == "equality":
        return _resolve_equality(leaf, ctx)  # principal is the runtime-bound operand
    if leaf["kind"] == "external_bool":
        return _resolve_external_bool(leaf, ctx)
    if leaf["kind"] == "signature_auth":
        return _resolve_signature_auth(leaf, ctx)
    if leaf["kind"] == "comparison":
        return CapabilityExpr.conditional_universal(condition=_comparison_condition(leaf))
    if leaf["kind"] == "unsupported":
        return CapabilityExpr.unsupported(leaf["unsupported_reason"])
```

`negate()` on an enumerable finite_set produces a `cofinite_blacklist`.
`negate()` on a threshold_group, signature_witness, or external_check
returns `unsupported("negation_unsupported")`. Honest.

`intersect` and `union` use the rules in the capability section above.
Not closed over arbitrary inputs — when an unsound combination is
attempted, the result is a structural `AND/OR` node that the UI
renders as both terms. Soundness preserved by never claiming a
representation we can't compute.

## UI changes (codex finding on UI mismatch)

UI ships in the same PR as the schema cutover. Schema-v2 artifacts
arrive with `predicate_tree` and `capability_expr` populated;
`direct_owner`/`controllers`/`controller_refs` are removed. UI reads only
v2 fields.

Components touched:
- `site/src/ProtocolSurface.jsx` — replace direct/controllers reading with
  capability-expression rendering. ~150 lines of new render code, ~80 lines
  removed.
- `site/src/auditUi.jsx` — same.
- `site/src/protocolScore.js` — scoring uses capability metadata
  (threshold-group score depends on M/N, blacklist scored as universal-minus,
  unsupported scored conservatively).

New rendered states (one cell per row):

| Capability kind | UI |
|---|---|
| `finite_set` (enumerable) | List of addresses with labels |
| `finite_set` (partial) | "Known: …; may have additional members" |
| `threshold_group(M, signers)` | "M-of-N: …" badge; expandable signer list |
| `cofinite_blacklist` | "Anyone except: …" |
| `signature_witness` | "Anyone with a signature from \<signer\>" |
| `external_check_only` | "Membership-checkable; query by address" with input |
| `conditional_universal(time)` | "Anyone, after \<T\>" |
| `unsupported(reason)` | "Pattern not yet supported: \<reason\>" + raw expression |
| `AND/OR` structural | Rendered as a small expression tree |
| Empty predicate tree | "Public" (no auth) |

`grantRole` is a per-role expansion: render as a table with
`{role: capability}`. UI special-case for `grantRole` at
`ProtocolSurface.jsx:374` is **deleted** — the per-role expansion is
the schema's capability shape, not a hard-coded UI branch.

## Migration (single schema-v2 cutover)

Codex correctly killed the v1 phase-1-only-emit-no-evaluator approach.
The cutover is one branch with these atomic changes:

1. **`schema_version="2.0"` everywhere**: schemas, artifacts, all
   consumers gated on `schema_version`.
2. **Old artifacts reprocess**: a one-shot job re-runs the static
   pipeline against every existing `Contract.id` and rewrites artifacts
   on schema-v2. Production cutover is: drain queue → run reprocess →
   bring up new code. ~hour or so on existing corpus.
3. **All consumers updated**: policy, resolver, UI, API responses, tests.
4. **role_grants indexer is a separate worker** added to start_workers.sh.
   Doesn't block schema cutover; on first run with empty `role_grants_events`,
   adapters report `partial` for AC contracts until the backfill catches up.

Branch is large but reviewable — codex review (this same flow) before merge.
We prepare a corpus-diff report against the 5 protocols below for the PR.

## Corpus tests — semantic, not superset (codex finding #11 fix)

Pinned fixtures for 5 protocols at fixed blocks:

```yaml
# tests/corpus/etherfi.yaml
contract: 0x...
block: 18_400_000
expected_admissions:
  keep: ["grantRole(bytes32,address)", "renounceRole(bytes32,address)", ...]
  remove: ["someFn(...)"]   # was admitted by heuristic, false positive
  add: ["otherFn(...)"]     # missed by heuristic, structural finds it
expected_capabilities:
  "grantRole(bytes32,address)":
    kind: per_role
    roles:
      "0x00...00":           # DEFAULT_ADMIN_ROLE
        kind: finite_set
        confidence: enumerable
        members: ["0xabc...", "0xdef..."]
      "0x123...456":          # CUSTOM_ROLE
        kind: finite_set
        members: ["0xfee..."]
  "renounceRole(bytes32,address)":
    kind: per_role
    pattern: caller_equals_argument  # diagnostic only; assertion is on capability shape
expected_unsupported_count: 0
```

Per-fixture: `keep`/`remove`/`add` lists, expected capability shapes,
expected unsupported counts. CI compares semantically. Diffs are surfaced
in PR comment for manual review. Not a raw superset.

5 protocols: EtherFi (AC + Timelock), Aave V3, MakerDAO Vat (wards),
Gnosis Safe (threshold), Compound III Comet (custom ACL).

## Test strategy (consolidated)

| Layer | Test |
|---|---|
| Predicate emitter | Per-pattern fixtures (16 patterns), modifier expansion, polarity normalization |
| `authority_role` classifier | Per-leaf classification fixtures including residual `business` |
| Capability combinators | Property tests: `intersect(finite, finite) ⊆ finite`, `union(blacklist, finite)`, etc. |
| Adapter `matches()` | Score on known-fixture contracts; tie warnings logged |
| Adapter `enumerate()` | Against fixture chain state for each adapter |
| `role_grants` indexer | Reorg simulation (rewind window, hash mismatch, double-grant) under advisory lock contention |
| Corpus | 5 protocols, semantic diff against pinned expectations |
| Live | End-to-end against a deployed pre-prod with EtherFi as the canary |
| Existing 41 fuzz xfails | Flip to passing |

## Phasing — revised, 5–7 weeks (codex finding #12 fix)

| Week | Work | Ships |
|---|---|---|
| 1 | Predicate IR + builder + `authority_role` classifier, polarity normalization, modifier walking, fixture corpus (16 patterns) | Static side compiles + emits v2 schema; resolver/policy/UI still on v1 — branch isolated |
| 2 | Capability expression + evaluator + AccessControl adapter + Safe adapter | Single-contract e2e: EtherFi grantRole → per-role finite_set (in-memory event scan; no role_grants table yet) |
| 3 | role_grants schema + indexer + reorg replay + advisory lock + per-chain finality config | `role_grants_events` populated for AC contracts; evaluator switches to durable lookup |
| 4 | Aragon/DSAuth + EIP-1271 adapters + parametric role-domain discovery + signature_auth/comparison/external_bool leaves | All 6 leaf kinds wired; threshold_group for Safe |
| 5 | UI: ProtocolSurface, auditUi, protocolScore — full v2 capability rendering + per-role expansions | Visible end-to-end on EtherFi pre-prod |
| 6 | Corpus tests (5 protocols), fixture manifest, CI integration, parametric role enumeration on real contracts | CI gating; corpus diff in PR |
| 7 | Cleanup: delete dead heuristics in summaries.py / graph.py / tracking.py / recursive.py / controller_adapters.py probe ladder; performance benchmarking; cutover plan | Merge candidate |

Reorg simulation, corpus pinning, modifier-expansion fixtures, and
adapter scoring tie-breaks are all in-week-7 polish *or* week-they-touch
acceptance criteria. None deferred to "later."

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Real-protocol pattern emerges that doesn't fit any leaf kind | Falls into `unsupported`; corpus diff surfaces it; ratchets the fixture; never silently mis-classifies |
| Modifier composition edge case missed | Explicit modifier-only fixture + library-callee fixture in week 1 |
| Capability-combinator unsoundness | Property tests in week 2; `unsupported_reason` returned over silent miscalculation |
| `role_grants` indexer falls behind | `confidence: partial` flag in capability + `last_indexed_block` propagated to UI; user sees the staleness |
| Reorg during indexing | Advisory lock + cursor `FOR UPDATE` + block-hash verification window + atomic delete-replay |
| Adapter scoring picks the wrong adapter | Tie warnings logged; per-adapter unit tests on hybrid contracts; manual override via `manual_pinned` field on descriptor |
| Schema cutover regresses production | Corpus tests gate the PR; pre-prod cutover with paused workers; rollback is "redeploy old binary, drop schema_version='2.0' artifacts" |
| Performance regression in static stage | Benchmark in week 7; per-function predicate-build time budget 5ms p99; > budget falls into `unsupported(reason="too_complex")` rather than blocking pipeline |

## Out of scope

Same as v1 except now explicit:
- zk proof / attestation auth: `unsupported("zk_proof")`
- Cross-chain (LayerZero, Wormhole): `external_check_only` via recursive resolver
- Optimistic governance vote gating: `external_check_only` with the governor
- Diamond ACL (LibDiamond fully): `unsupported("diamond_acl_v1")` with an open follow-up to add a Diamond adapter in v2.1
- EIP-1271 contract-signature auth: `external_check_only` (the authority is the signing contract)
- Bitwise role flags: `unsupported("bitwise_role_flag")`
- Permit-style approvals: not admitted (asset authority, separate from function-caller auth)
- Reentrancy / pause / business invariants: annotated, not admitted

Each has a fixture pinning the expected unsupported reason. UI renders
them with the reason string visible.

## v1 → v2 diff (what changed)

| Codex finding | v2 fix |
|---|---|
| Blocker #1: admission too broad | `authority_role` annotation; only caller/delegated admit |
| Blocker #2: polarity backwards | `if (!X) revert ⇒ X`; negation pushed to leaf |
| Blocker #3: `unknown` contradicts schema | `unsupported` is first-class leaf kind |
| Blocker #4: phase 1 cannot ship | Single schema-v2 cutover branch, all consumers together |
| Blocker #5: role_grants not reorg-safe | Event-sourced + tx_hash + log_index PK + advisory lock + per-chain finality + block-hash verification |
| High #6: modifier inlining false | Predicate builder walks function + modifiers + internal/library callees explicitly |
| High #7: SetAdapter too thin | EvaluationContext into matches/enumerate/membership; descriptor fattened with authority_contract/role_domain/selector_context |
| High #8: principal algebra unsound | Replaced flat sets with capability expressions: finite_set / threshold_group / cofinite_blacklist / signature_witness / external_check_only / conditional_universal / unsupported / AND / OR |
| High #9: missing real-world patterns | Each pinned as `unsupported(reason)` with fixture; Diamond, EIP-1271, bitwise, M-of-N as threshold_group, timelocked, permit, reentrancy explicitly handled |
| High #10: parametric role domains missing | `RoleDomain.sources`: compile_time_constants ∪ role_granted_history ∪ abi_declared ∪ manual_pinned |
| Medium #11: corpus superset wrong | Per-fixture keep/remove/add manifest with expected capability shapes |
| Medium #12: timeline not credible | 5–7 weeks, with reorg sim / corpus / modifier fixtures budgeted in-week |

## Open questions for codex (round 2)

1. Is the `authority_role` enum complete? Specifically: where does
   "checking that the caller is a trusted facet" (Diamond) sit —
   `caller_authority` because the caller equals the facet's selector
   region, or its own role since the call goes through delegatecall?
2. The negation-on-leaf rule pushes `NOT(equality)` to operand swap
   when both operands are addresses. What's the right normalization
   when one operand is a `view_call` and the other a parameter? Is
   `inequality(view_call, parameter)` cleanly representable, or does
   it become `unsupported("negated_view_call_compare")`?
3. `intersect(threshold_group, finite_set)` — does the plan render as
   "structural AND" really capture user intent, or should we define
   threshold-group ∩ finite_set = `threshold_group(M, signers ∩ finite_set)`?
   The latter is cleaner if the finite set is the active signer set,
   but wrong if the finite set is the role-admin list.
4. RoleGranted log scan for the parametric role-domain — should
   `role_granted_history` filter by `revoked_block IS NULL` only, or
   include historically-granted-then-revoked roles too? Affects which
   roles appear in the UI per-role expansion.
5. `chain_finality_config` table — am I missing chains we need to
   support? Optimism, Base, Polygon, Arbitrum, Mainnet are obvious;
   Linea/Scroll/zkEVM finality semantics differ.
