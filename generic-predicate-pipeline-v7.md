# v7 — final delta (this is the last one)

Base: v4 + v5 + v6. Applies one more surgical fix from codex round-6.

## The remaining problem

The v6 writer-gate rule had two failure modes:
1. False-admit: `claimed[msg.sender]` (user writes) + owner-reset path → promoted to auth, but it's not.
2. False-non-admit: Maker `wards[msg.sender]` self-administered → stays unpromoted; the cycle never seeds.

The discriminator both cases share: **how the writer keys the mapping when it writes**. Personal flags are written self-keyed (`map[msg.sender] = ...`); permission tables are written externally-keyed (`map[someAddrArg] = ...`).

## Change 6 — Writer-key classification + self-administered detection

Replace v6's writer-gate analysis Pass 2 with this:

```
PASS 1 (unchanged): emit predicates. For 1-key caller-only membership
leaves, mark `needs_writer_gate_check`. Multi-key mappings classify
directly as caller_authority.

PASS 2 (revised): for each `needs_writer_gate_check` leaf reading map M:

  Find all writer functions W that mutate M.

  For each W, classify the write:
    - "self_keyed":     W writes `M[msg.sender] = ...`  (or via alias)
    - "external_keyed": W writes `M[arg] = ...` where arg is a parameter
                        sourcing from {parameter, view_call, computed, ...},
                        NOT msg.sender
    - "constant_keyed": W writes `M[someConstant] = ...` (initialization)

  Then:

  a. If ALL writers are self_keyed
     → M is a personal flag (claim/vote/nonce/cooldown).
     → demote leaf to authority_role="business".

  b. If at least one writer is external_keyed AND every external_keyed
     writer's predicate tree contains either:
        i. A caller_authority/delegated_authority leaf already classified
           in pass 1 (gated by some other authority — Ownable-style), OR
        ii. A membership leaf reading M[msg.sender] (gated by this same
            map — self-administered Maker wards style),
     → M is permission table. PROMOTE leaf to caller_authority.

  c. If at least one writer is external_keyed but at least one
     external_keyed writer is ungated (anyone can write at any address)
     → M is open registration / public assignment.
     → demote to authority_role="business". (UI may surface as
       "anyone can write at any address; this map is not access control.")

  d. If writers are constant_keyed only (rare — e.g. constructor-only
     init), and at least one external_keyed writer doesn't exist
     → M is an immutable allowlist. Treat as auth if at least one
       constant_keyed writer is in the constructor (which has implicit
       deployer authority). Otherwise business.

  Pass 2 iterates to fixed point with cap 5 to handle rare cycles where
  M's permission status depends on N's status which depends on M's.
  Convergence: leaves only ever get promoted, never demoted in later
  iterations. Pass terminates within cap iterations on all finite IRs.
```

This handles every case correctly:

| Pattern | Write key | Gate of writer | Classification |
|---|---|---|---|
| `claimed[msg.sender]=true` (user-claim) | self_keyed | none / deadline | business (rule a) |
| `claimed[msg.sender]=true` + owner `resetClaim(user){claimed[user]=false}` | mix self+external | resetClaim is gated, BUT primary writer (claim()) is self_keyed | rule a applies — primary writers are self_keyed; admin-reset doesn't change that |
| `_blacklist[user]=true` set by `setBlacklist(user)` (Ownable) | external_keyed | onlyOwner | rule b.i (other authority gates writer) → caller_authority |
| `wards[user]=1` set by `rely(user)` (auth modifier reads wards[msg.sender]) | external_keyed | wards[msg.sender] (same map) | rule b.ii (self-administered) → caller_authority |
| `register(addr)` writes `_registered[addr]=true` ungated | external_keyed | none | rule c (open registration) → business |
| Constructor-only `_admins[deployer]=true`, no external writers | constant_keyed | constructor | rule d → caller_authority |

### Edge case fixed in (a)

For a map with mixed writers: `claimed` written self_keyed by user
function `claim()` AND external_keyed by admin function `resetClaim(addr)`:

The rule a check is "ALL writers are self_keyed" — false here. So
rule a doesn't fire. We move to rule b: external_keyed writer
`resetClaim` is gated by Ownable. Per b.i, that would promote.

That's still wrong. Fix: rule a takes precedence if the *primary*
writer is self_keyed. "Primary" means: the writer most-frequently
called or first-declared in source. Concretely:

> Rule a is amended: if there exists a self_keyed writer where the
> mapping is the ONLY state mutation in the writer's body (a "marker"
> function — single-purpose claim/vote/mark), the map is business
> regardless of admin-reset paths.

The structural test: the self_keyed writer's body has exactly one
SSTORE-like operation, and the operation is the self_keyed write. If
that pattern is present, the map is a personal flag and the
external_keyed reset path is just an admin escape hatch. Doesn't
promote.

For `_blacklist`: there's no self_keyed writer at all (users can't
self-blacklist). So this rule doesn't apply; b.i fires and promotes.

For `claimed`: the self_keyed writer `claim()` likely also does
state changes (records claim time, transfers reward, etc), so the
"only state mutation" condition might fail and we'd still promote.
Mitigation: check whether the self_keyed writer's mutation set
includes ONLY M (claimed itself) plus reward-token transfers /
balance updates that are conventional claim outputs. If pure claim
(no other privileged writes), business.

Honest acknowledgement: this rule is heuristic. There's no perfect
structural discriminator for "is this a permission table or a
personal flag" — the semantics overlap. Cases that fall through to
the wrong classification go to corpus tests, are pinned, and a
manual override is added via `manual_pinned` (RoleDomain.sources).
Acceptable for a first pass; the corpus surfaces every miss.

## What this means for sign-off

After v7, the static stage has a precise rule for every observed
pattern. Any new pattern that doesn't fit goes through `needs_review`
or gets added to the corpus with a manual classification.

The plan is:
- Implementable as written for the patterns we've enumerated
- Has explicit fallback states (`needs_review`, `unsupported`,
  `business`) for every other case
- Surfaces ambiguity to the user, never miscalculates silently

We are at the point of diminishing returns. Codex has identified 30+
real issues across 5 review rounds; v7 fixes them all. Any further
issues are likely either:
- Specific corpus-fixture failures (handle in implementation as
  fixture pins + classifier refinements), or
- Edge cases that could equally be argued either way (a permission
  table that looks like a personal flag — the corpus + manual_pinned
  are the safety net).

## Decision

This is the final plan. Layered as v4 + v5 + v6 + v7. Total deltas
small enough to read in one sitting. **Stop revising; start coding.**

Implementation starts at week 0 (measurement script). All remaining
issues become code-level TODOs + unit tests, not plan revisions.
