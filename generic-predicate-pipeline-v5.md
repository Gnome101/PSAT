# v5 — final delta

This is a delta document. The base plan is `generic-predicate-pipeline-v4.md`.
Apply only the two changes below; everything else in v4 stands.

## Change 1 — AuthorityClassifier is kind/operator/descriptor-specific (round-4 #1 fix)

v4 said `caller_authority` fires when "any operand's source set includes
msg_sender." That admits `balances[msg.sender] >= amount`, `claimed[msg.sender]`,
nonce/cooldown checks. All of those have msg.sender in operand provenance
but are business-invariant predicates, not access control.

Replace v4's AuthorityClassifier with this stricter version:

```python
class AuthorityClassifier:
    """Per-leaf classification, kind/operator/descriptor-aware.
    Just having msg_sender in operand provenance is necessary but not
    sufficient. The leaf's shape must also match an authority pattern.

    A leaf is `caller_authority` iff one of:

      A. CALLER EQUALITY:
         - kind == "equality"
         - operator == "eq"
         - exactly one operand sources from {msg_sender, tx_origin}
         - the OTHER operand sources from {state_variable, view_call,
           parameter, signature_recovery, computed} — i.e., it
           represents *who is allowed*.
         (Examples: msg.sender == owner; recoveredSigner == owner;
         msg.sender == _delegates[arg].)

      B. AUTH-SHAPED MEMBERSHIP:
         - kind == "membership"
         - operator ∈ {"truthy", "falsy"}
         - set_descriptor.key_sources contains an Operand with
           source ∈ {msg_sender, signature_recovery}
         - set_descriptor.kind ∈ {"mapping_membership", "array_contains"}
         - set_descriptor.truthy_value is None or "1" (i.e., bool/uint
           flag, not an arbitrary value comparison)
         (Examples: _members[role][msg.sender]; wards[ilk][msg.sender] == 1.
         Excluded: balances[msg.sender] >= amount, which is comparison
         not membership.)

      C. SIGNATURE AUTH:
         - kind == "signature_auth"
         (Already shape-tight by construction.)

    A leaf is `delegated_authority` iff:

      D. EXTERNAL BOOL TO AUTHORITY:
         - kind == "external_bool"
         - operator ∈ {"truthy", "falsy"}
         - the call target traces (via provenance) to a state_variable
         - the call args include an operand sourcing from
           {msg_sender, signature_recovery}
         (Example: authority.canCall(msg.sender, target, sel).)

    Otherwise:

      E. TIME — every varying operand sources from block_context only.

      F. REENTRANCY / PAUSE — leaf reads a state_var that
         ReentrancyAnalyzer / PauseAnalyzer flagged as a guard with
         HIGH or MEDIUM confidence.

      G. BUSINESS — residual. INCLUDES:
         - Caller-keyed business state with comparison operators:
           `balances[msg.sender] >= amount` →
           kind=membership but op is comparison-derived, NOT truthy/falsy.
           Actually this is `comparison(membership(...), amount)` — the
           outer leaf is a comparison; the membership is just an
           operand source.
         - `nonces[msg.sender] == expected` → kind=equality but the
           OTHER operand is `parameter`/`computed` (a *value*), not
           a *who*. Distinguish by: the non-msg.sender operand's
           role in the predicate. If it's a *value* (uint/bytes
           consumed in arithmetic or hash), business. If it's a
           *who* (address-typed, used as an identity), caller_authority.
         - claim/cooldown/deadline/amount checks.
    """
```

The rule for distinguishing "who" vs "value" operand in case (A):
- The other operand's static type. If it's `address`, it's a who.
- If it's `bytes32` and resolves through a chain that includes a
  bytes32 role-constant or `getRoleAdmin` call, it's a who (in role-key
  position, not member position — but role-key membership goes through
  case B, not A).
- Anything else (uint/bool) is a value → business.

Test fixtures (added to week 3 acceptance):

| Source | Expected classification | Why |
|---|---|---|
| `require(msg.sender == owner)` | caller_authority (A) | address operand vs msg.sender |
| `require(_members[role][msg.sender])` | caller_authority (B) | bool mapping with caller as key |
| `require(authority.canCall(msg.sender, t, sel))` | delegated_authority (D) | external bool with state-var target |
| `require(balances[msg.sender] >= amount)` | business (G) | comparison with uint value |
| `require(nonces[msg.sender] == expected)` | business (G) | uint operand on the other side |
| `require(claimed[msg.sender] == false)` | business (G) | bool operand but the *check* is equality with literal value, not membership in caller-keyed set |
| `require(_blacklist[msg.sender] == false)` | business unless `_blacklist` is structurally an auth-domain | comparison-style bool check; subtle case — see fix below |
| `require(!_blacklist[msg.sender])` | caller_authority (B) with op=falsy | bool mapping with caller as key, used as membership not equality |
| `require(deadline > block.timestamp)` | time (E) | block_context |
| `require(_status != _ENTERED)` | reentrancy (F) | reentrancy analyzer flagged |
| `require(msg.sender == ECDSA.recover(...))` | caller_authority (A) | other operand is signature_recovery |

The `_blacklist[msg.sender] == false` vs `!_blacklist[msg.sender]` case
deserves explicit attention. Both are semantically "not blacklisted,"
but they normalize differently:
- `require(!_blacklist[msg.sender])` → polarity-normalized to
  `membership, op=falsy`. Auth-shaped (B). caller_authority.
  Capability: cofinite_blacklist of the mapping members.
- `require(_blacklist[msg.sender] == false)` → polarity-normalized to
  `equality, op=eq` between membership-result and `false` literal. Not
  auth-shaped (A doesn't fit because operand "false" is a uint/bool
  literal, not "who"; B doesn't fit because it's equality not
  membership). Falls to business.

The distinction is unfortunate (semantically identical Solidity), but
forcing developers to write `!_blacklist[msg.sender]` (the canonical
form) is reasonable. We document this. If false-business
classifications surface in corpus tests, we can add a specific
normalization step to canonicalize `!= false` and `== false` over bool
membership expressions into the negated-membership form.

## Change 2 — Unsupported leaves do NOT admit (round-4 #2 fix)

v4 RevertDetector case 8 said opaque Yul emits `unsupported("opaque_control_flow")`
and "function still admits with that leaf." That contradicts the
admission rule (admit iff caller/delegated authority leaf present).

Replace with:

> RevertDetector case 8 (opaque control flow): emits
> `unsupported(reason="opaque_control_flow")` leaf with
> `authority_role="business"`. The function does **not** admit on this
> leaf alone. If the function has no other authority leaves, it goes
> into a new top-level slot `needs_review_functions` (parallel to
> `privileged_functions`) on the AccessControl analysis output, with
> the unsupported reason attached so the UI can flag "this function
> has guard structure we couldn't parse — manual review recommended."

Schema update:

```python
class AccessControlAnalysis(TypedDict):
    privileged_functions: list[PrivilegedFunction]
    needs_review_functions: list[NeedsReviewFunction]   # NEW
    role_definitions: list[RoleDefinition]
    current_holders: CurrentHolders
    ...

class NeedsReviewFunction(TypedDict):
    contract: str
    function: str
    visibility: str
    predicate_tree: PredicateTree     # full tree even though no admit
    review_reasons: list[str]         # unsupported_reason values from leaves
```

Resolver-side: `needs_review_functions` are not evaluated for capabilities
(no admitted authority to evaluate). The UI lists them in a separate
"Needs review" panel with the predicate IR rendered as text + the
review reasons.

This applies to ALL `unsupported` leaves, not just opaque control flow:
if a function has only unsupported leaves (no caller/delegated authority),
it goes to `needs_review_functions`. If it has at least one
caller/delegated authority leaf plus some unsupported leaves, it stays
in `privileged_functions` and the unsupported leaves are rendered as
structural fragments in the capability tree.

## Updated open questions (round-5)

The two round-4 questions were the blockers above. v5 fixes both. No
new structural concerns introduced. Remaining open questions from v4
(property-test coverage, recursion depth, UI defaults, manual_pinned
flow) are tracked as in-flight risks during implementation, not plan
revisions.

## Sign-off bar

After applying the two changes above, all known structural blockers are
resolved. The plan is implementable as written. Anything else found
during implementation flips to a TODO comment + unit test, not another
plan revision.
