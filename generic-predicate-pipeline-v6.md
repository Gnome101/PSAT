# v6 — final final delta

Base: v4 + v5. Applying three more surgical fixes from codex round 5.
This is the last revision; remaining issues become TODOs in code.

## Change 3 — Two-pass writer-gate analysis for auth-shape detection (round-5 #1 fix)

The core problem: `!claimed[msg.sender]`, `!hasVoted[msg.sender]`,
`isRegistered[msg.sender]` are structurally identical to
`!_blacklist[msg.sender]`. The first three are business; the last is
authority. They differ semantically by **who writes them** — admins
write blacklists, users write their own claim flags.

Replace v5's Rule B (auth-shaped membership) with a two-pass algorithm:

```
PASS 1 — Initial classification:
  For each function, emit predicates. For membership leaves with
  msg_sender in key_sources where the auth-shape would otherwise
  match (Rule B from v5):
    - If set_descriptor.key_sources has more than one key beyond
      msg_sender (i.e., 2+ keys, like _members[role][msg.sender])
      → classify as caller_authority. Multi-key mappings are
      structurally permission tables.
    - Otherwise (1-key caller-only mapping, e.g. flag[msg.sender])
      → classify as `needs_writer_gate_check`. NOT yet caller_authority.

PASS 2 — Writer-gate analysis (after all functions classified in pass 1):
  For each needs_writer_gate_check leaf:
    1. Find all writer functions of the same set_descriptor.storage_var
       in the same contract (Slither has cross-function writes_to_state).
    2. For each writer function, look up its pass-1 predicate tree.
    3. If ANY writer function has at least one caller_authority or
       delegated_authority leaf in its predicate tree
       → promote this leaf to caller_authority.
    4. Otherwise (writer is open/self-service)
       → demote to authority_role="business". The function admits
         only if other leaves are caller/delegated authority.
```

Cycle handling: if function A's classification depends on function B's
which depends on function A's, the second pass iterates to fixed point
with bounded iterations (cap 5). Cycles in writer-gate dependence are
rare but real (e.g., a contract where role-management functions are
themselves role-gated). Convergence: a leaf either reaches
caller_authority or stays business; once business, never promoted; once
caller_authority, stays.

This makes the classification depend on who writes the storage var,
not on Solidity syntax. `_blacklist[msg.sender]` admits because a
gated `setBlacklisted(addr, bool)` writes it. `claimed[msg.sender]`
doesn't admit because users write it themselves.

Test fixtures (added to week 3):
- 1-key mapping written only by users (claim/vote/nonce) → business
- 1-key mapping written by Ownable function → caller_authority
- 1-key mapping with both gated and ungated writer functions → caller_authority (any gated writer admits)
- 2-key mapping `_members[role][msg.sender]` → caller_authority directly (no pass 2 needed)
- Cyclic dependence (a→b→a) → fixed-point iteration converges within cap

## Change 4 — Self-service caller equality resolves to `conditional_universal` (round-5 #2 fix)

Rule A (caller equality) admitted `require(msg.sender == account)` where
`account` is a function parameter. That's correct — it IS a permission
boundary (the function only operates on the caller's own data). But
the *capability* should not be `finite_set([account])` (which is
nonsense — `account` is a runtime parameter, not a principal). It
should be `conditional_universal(self_service)`.

Update `_resolve_equality_principal` in the evaluator:

```python
def _resolve_equality_principal(leaf, ctx) -> CapabilityExpr:
    # Find the operand that is NOT msg.sender / signature_recovery.
    # That operand identifies who is allowed.
    other = [op for op in leaf.operands
             if op.source not in ("msg_sender", "tx_origin", "signature_recovery")]
    if len(other) != 1:
        return CapabilityExpr.unsupported("equality_operand_ambiguous")
    op = other[0]

    if op.source == "constant":
        return CapabilityExpr.finite_set([op.constant_value], quality="exact")

    if op.source == "state_variable":
        # Resolve the state var via controller_value (existing path).
        addr = ctx.read_state_var(op.state_variable_name)
        if addr is None:
            return CapabilityExpr.finite_set([], quality="lower_bound")  # not yet resolved
        return CapabilityExpr.finite_set([addr], quality="exact")

    if op.source == "view_call":
        # Authority-contract dispatch (e.g., owner()): recursively
        # resolve via adapter or controller_value.
        return _resolve_view_call_principal(op, ctx)

    if op.source == "parameter":
        # Self-service: caller's allowed identity is whatever they
        # passed in. Anyone can call this, but only operating on
        # their own data.
        return CapabilityExpr.conditional_universal(
            condition=Condition(
                kind="self_service",
                parameter_index=op.parameter_index,
                parameter_name=op.parameter_name,
                description=f"caller acting on their own {op.parameter_name}",
            )
        )

    if op.source == "signature_recovery":
        # The principal is the signature's claimed signer.
        return CapabilityExpr.signature_witness(
            signer=_resolve_signer_expression(op, ctx)
        )

    # Computed (e.g., keccak of caller + nonce) — give up cleanly.
    return CapabilityExpr.unsupported(f"equality_operand_source_{op.source}")
```

UI for `conditional_universal(self_service)`:
- Label: "Self-service" / "Anyone, on their own data"
- Sublabel uses the parameter name: "Anyone can call for their own `account`"

renounceRole(role, account) with `require(account == msg.sender)`
resolves to `conditional_universal(self_service, parameter='account')`.
That's the right answer — any caller, but only for themselves. UI shows
"Self-service" instead of "Unresolved" or a wrong principal list.

## Change 5 — Evaluator order: unsupported before authority_role (round-5 #3 fix)

Move the `kind == "unsupported"` check to the top of `_evaluate_leaf`,
before the authority_role dispatch:

```python
def _evaluate_leaf(leaf, ctx) -> CapabilityExpr:
    # 0. unsupported is structural; check first.
    if leaf.kind == "unsupported":
        return CapabilityExpr.unsupported(leaf.unsupported_reason)

    # 1. Non-authority leaves go to side-conditions.
    if leaf.authority_role in ("reentrancy", "pause", "business", "time"):
        return CapabilityExpr.conditional_universal(
            condition=Condition.from_leaf(leaf),
        )

    # 2. caller_authority / delegated_authority below — same as v4.
    ...
```

Now an unsupported leaf inside an otherwise-privileged tree renders as
an unsupported structural fragment, not as a business condition. The
predicate tree `AND(equality(caller_authority), unsupported(opaque))`
evaluates to `intersect(finite_set([owner]), unsupported(opaque))`
which combinator rules turn into
`unsupported("intersect_with_unsupported_opaque")`. UI surfaces it
faithfully.

## Net status

After v6, the plan covers:
- All 12 round-1 findings
- All 9 round-2 findings
- All 9 round-3 findings (incl. 2 blockers)
- All 3 round-4 blockers
- All 3 round-5 blockers (this delta)

Remaining open questions are implementation-time TODOs, not plan-level
blockers:
- `<CapTree />` UI defaults (collapsed vs expanded)
- `manual_pinned` curation flow (YAML vs DB)
- Property-test equational identities for combinators
- ProvenanceEngine InternalCall recursion depth tunable
- Pass-2 writer-gate iteration cap exact value (5 is a guess)
- Self-service condition rendering (label, sublabel format)

These are addressable in the implementation phase as code-level TODOs
+ unit tests, not plan revisions.

## Sign-off bar

This is the final plan. Apply v4 + v5 + v6 deltas; start coding.
Implementation TODOs replace further plan revisions.
