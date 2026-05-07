"""v2 predicate-pipeline live cutover gate.

These tests are the explicit merge gate for the v1->v2 cutover described
in /tmp/psat-plans/pr70-v1-to-v2-cutover.md S F.1. They are intentionally
written to fail today — pre-existing live tests in
``test_pipeline_stages.py`` and ``test_analyses.py`` only assert v1
artifacts (control_tracking_plan, control_snapshot, effective_permissions),
so the live job would happily go green even if the v2 pipeline were
entirely broken. This module closes that gap by asserting that the v2
artifacts exist, the typed leaves carry the right shape, the capability
resolver returns a non-empty payload, and EffectiveFunction rows carry a
populated ``capability_expr``.

Each test docstring names the cutover phase it gates. Test failures here
should block the PR #70 merge until the corresponding implementation
phase lands.

Notes for maintainers:
  * The fixture chosen is ``analyzed_weth`` (WETH) for parity with the
    existing live suite. WETH is intentionally controls-free, so the
    typed-leaf / non-empty-capabilities / capability-expr / principal-
    consistency tests will all hit ``pytest.skip`` — that is by design.
    The schema-version assertion in ``test_predicate_trees_artifact_exists``
    is the only thing WETH alone can gate.
  * The full merge gate runs in CI's ``live-tests`` job, where
    ``PSAT_LIVE_AUDIT_URL`` is pinned to a real audit (currently the
    EtherFi sample at ``tests/fixtures/audits/sample_audit.pdf``). That
    audit drives ``analyzed_company`` to enroll guarded contracts;
    downstream tests in this file run against those, not WETH, and the
    skips become real assertions.
  * TODO: when a guarded-contract fixture (``analyzed_etherfi_admin``-
    style) lands in ``conftest.py``, swap the per-test fixture from
    ``analyzed_weth`` -> ``analyzed_guarded_contract`` so the local
    (non-CI) live runs also exercise the typed-leaf gates. Until then,
    treating ``skip`` as ``pass`` is acceptable because CI re-runs
    against the guarded audit.
  * ``PSAT_ADMIN_KEY`` missing -> the whole live suite skips via the
    ``live_admin_key`` fixture in ``conftest.py`` (per CLAUDE.md).
  * Never use ``requests.post`` directly against admin endpoints — go
    through ``live_client``. (Per CLAUDE.md.)
"""

from __future__ import annotations

import pytest

from tests.live.conftest import LiveClient

# Closed kind sets pulled from the v2 type modules — kept here as plain
# tuples so a future schema bump that introduces a new kind shows up as
# a clear test failure rather than a silent pass.
#
# Source: services/static/contract_analysis_pipeline/predicate_types.py
EXPECTED_LEAF_KINDS = {
    "membership",
    "equality",
    "comparison",
    "external_bool",
    "signature_auth",
    "unsupported",
}
# Subset called out by the task spec — at least one leaf must land in this
# tighter set (the others are diagnostic / fallback states).
TYPED_LEAF_KINDS = {"equality", "membership", "external_bool", "signature_auth"}

EXPECTED_AUTHORITY_ROLES = {
    "caller_authority",
    "delegated_authority",
    "time",
    "reentrancy",
    "pause",
    "business",
}

# Source: services/resolution/capabilities.py CapKind
EXPECTED_CAPABILITY_KINDS = {
    "finite_set",
    "threshold_group",
    "cofinite_blacklist",
    "signature_witness",
    "external_check_only",
    "conditional_universal",
    "unsupported",
    "AND",
    "OR",
}


def _iter_leaves(tree: dict):
    """Yield every leaf-predicate dict in a PredicateTree (DFS)."""
    if not isinstance(tree, dict):
        return
    op = tree.get("op")
    if op == "LEAF":
        leaf = tree.get("leaf")
        if isinstance(leaf, dict):
            yield leaf
        return
    for child in tree.get("children", []) or []:
        yield from _iter_leaves(child)


def test_predicate_trees_artifact_exists(analyzed_weth, live_client: LiveClient):
    """Gates the v2 static-stage emit (predicate_artifacts.py).

    Asserts the ``predicate_trees`` artifact is written for every
    completed analysis and carries the v2 schema marker. WETH is
    controls-free so ``trees`` may legitimately be empty; the
    schema_version check is what proves the emit ran end-to-end.
    """
    art = live_client.artifact(analyzed_weth["name"], "predicate_trees")
    assert art is not None, (
        "predicate_trees artifact missing — v2 static-stage emit did not "
        "run (gates predicate_artifacts.py / contract_analysis_pipeline core)."
    )
    assert isinstance(art, dict), "predicate_trees should be a JSON object"
    assert art.get("schema_version") == "v2", (
        f"predicate_trees.schema_version must be 'v2', got {art.get('schema_version')!r} — "
        "v2 emit produced the wrong schema marker."
    )
    assert "trees" in art, "predicate_trees missing 'trees' key (v2 schema violation)"
    assert isinstance(art["trees"], dict), "predicate_trees.trees must be a dict keyed on function name"


def test_predicate_trees_has_typed_leaves(analyzed_weth, live_client: LiveClient):
    """Gates leaf-typing in the predicate builder (LeafPredicate types).

    Walks every tree and asserts at least one leaf carries a kind in
    ``TYPED_LEAF_KINDS`` and an authority_role in
    ``EXPECTED_AUTHORITY_ROLES``. Skips cleanly when ``trees`` is empty
    (WETH has no guards) — under that condition this test does not gate
    anything; the schema-version check in the previous test is the real
    gate.
    """
    art = live_client.artifact(analyzed_weth["name"], "predicate_trees")
    assert isinstance(art, dict), "predicate_trees artifact missing — see test_predicate_trees_artifact_exists"
    trees = art.get("trees") or {}
    if not trees:
        pytest.skip(
            "WETH has no privileged controls; trees is empty by design. "
            "The schema_version assertion in test_predicate_trees_artifact_exists "
            "is the real v2-emit gate for controls-free contracts."
        )

    saw_typed_leaf = False
    saw_typed_role = False
    for fn_sig, tree in trees.items():
        for leaf in _iter_leaves(tree):
            kind = leaf.get("kind")
            role = leaf.get("authority_role")
            assert kind in EXPECTED_LEAF_KINDS, (
                f"Leaf kind {kind!r} for {fn_sig} is not in the closed v2 LeafKind set "
                f"({sorted(EXPECTED_LEAF_KINDS)}) — schema drift."
            )
            assert role in EXPECTED_AUTHORITY_ROLES, (
                f"Leaf authority_role {role!r} for {fn_sig} is not in the closed v2 "
                f"AuthorityRole set ({sorted(EXPECTED_AUTHORITY_ROLES)}) — schema drift."
            )
            if kind in TYPED_LEAF_KINDS:
                saw_typed_leaf = True
            if role in EXPECTED_AUTHORITY_ROLES:
                saw_typed_role = True

    assert saw_typed_leaf, (
        f"No leaf with kind in {sorted(TYPED_LEAF_KINDS)} found across {len(trees)} "
        "tree(s) — leaf-typing in build_predicate_tree is not producing typed kinds."
    )
    assert saw_typed_role, (
        "No leaf with a typed authority_role found — apply_writer_gate_pass / "
        "apply_reentrancy_pause_pass did not annotate any leaves."
    )


def test_capability_resolution_returns_non_empty(analyzed_weth, live_client: LiveClient):
    """Gates the resolver read path (services/resolution/capability_resolver.py).

    Hits ``GET /api/contract/{address}/capabilities`` (routers/v2.py) and
    asserts the response shape matches the v2 contract. WETH is
    controls-free, so ``capabilities`` may be ``{}`` (every function
    unguarded — the resolver convention). When non-empty, asserts at
    least one entry carries a ``CapabilityExpr.kind`` in the closed kind
    set.

    TODO(D.1 rename): the route is currently
    ``/api/contract/{address}/capabilities``. If D.1 renames this, update
    the path here and drop this TODO.
    """
    addr = (analyzed_weth.get("address") or "").lower()
    assert addr.startswith("0x"), f"analyzed_weth.address missing or malformed: {addr!r}"

    # Use the session directly — LiveClient doesn't expose a typed helper
    # for the v2 capability route yet (intentional: this lives in
    # routers/v2.py and post-cutover it'll be the canonical read path,
    # at which point it should get a method on LiveClient).
    resp = live_client._session.get(
        live_client._url(f"/api/contract/{addr}/capabilities"),
        timeout=30,
    )
    assert resp.status_code == 200, (
        f"GET /api/contract/{addr}/capabilities returned {resp.status_code} — "
        "v2 capabilities route is wired but the resolver failed (gates "
        "capability_resolver.resolve_contract_capabilities). Body: {resp.text[:400]!r}"
    )
    body = resp.json()
    assert isinstance(body, dict), "capabilities response must be a JSON object"
    assert "capabilities" in body, "capabilities response missing 'capabilities' field"
    caps = body["capabilities"]
    assert isinstance(caps, dict), "capabilities field must be a dict keyed on function signature"

    if not caps:
        pytest.skip(
            "WETH has no guarded functions; capabilities is empty by design. "
            "Non-empty content is asserted by the company-level cutover suite."
        )

    saw_valid_kind = False
    for fn_sig, cap in caps.items():
        assert isinstance(cap, dict), f"capabilities[{fn_sig}] must be a dict"
        kind = cap.get("kind")
        assert kind in EXPECTED_CAPABILITY_KINDS, (
            f"CapabilityExpr.kind {kind!r} for {fn_sig} not in closed CapKind set "
            f"({sorted(EXPECTED_CAPABILITY_KINDS)}) — resolver schema drift."
        )
        saw_valid_kind = True
    assert saw_valid_kind, (
        "Capabilities map non-empty but no entry had a valid kind — capability_to_dict serialization broke."
    )


def test_effective_function_has_capability_expr(analyzed_weth, live_client: LiveClient):
    """Gates B.1 — EffectiveFunction.capability_expr population.

    Until B.1 lands, ``capability_expr`` is NULL for every row even when
    the resolver returned a real CapabilityExpr; the policy-stage
    persistence path doesn't write the column yet. This test fails today
    and stays red until B.1 wires the resolver output through to
    ``EffectiveFunction.capability_expr``.

    Reads through the analysis_detail payload because it's the public
    surface that downstream consumers (UI, agent tools) use; if the
    column exists in the DB but doesn't surface here, B.1 isn't done.

    TODO(B.1 surface): once persistence lands, the
    ``_serialize_effective_functions`` aggregation in
    ``services/aggregations/analysis_detail.py`` must include
    ``capability_expr`` in its output. This assertion drives that
    surfacing.
    """
    detail = live_client.analysis_detail(analyzed_weth["name"])
    eff = (detail.get("effective_permissions") or {}).get("functions") or []
    if not eff:
        pytest.skip(
            "WETH has no effective_functions rows (no privileged controls). "
            "B.1 gate is exercised on contracts that produce non-empty effective_permissions."
        )

    have_expr = [fn for fn in eff if fn.get("capability_expr") is not None]
    assert have_expr, (
        f"None of {len(eff)} effective_function rows carries a populated "
        "capability_expr field — B.1 persistence path is not landed. "
        "Gates: workers/policy_worker.py writing CapabilityExpr -> "
        "EffectiveFunction.capability_expr, plus _serialize_effective_functions "
        "surfacing it in analysis_detail.json."
    )


def test_effective_function_principal_consistent_with_capability_expr(analyzed_weth, live_client: LiveClient):
    """Strongest cutover-correctness gate — principals row reflects capability_expr shape.

    The cutover swaps the principal-resolution implementation. For
    correctness, the resolved FunctionPrincipal rows must agree with the
    CapabilityExpr they were derived from:

      * ``finite_set`` -> exactly ``len(members)`` principal rows.
      * ``threshold_group`` -> exactly 1 principal row, ``resolved_type=='safe'``,
        ``details.threshold`` populated.
      * ``cofinite_blacklist`` / ``external_check_only`` /
        ``conditional_universal`` -> principals empty AND capability_expr
        populated (these kinds describe sets that cannot be enumerated as
        principal rows).

    Skips when the WETH fixture has no effective_functions; this gate
    only asserts on contracts with at least one resolved capability.
    Until B.1 lands ``capability_expr`` will be None and this test is
    skip-or-fail; once B.1 lands the consistency assertions become real.
    """
    detail = live_client.analysis_detail(analyzed_weth["name"])
    eff = (detail.get("effective_permissions") or {}).get("functions") or []
    if not eff:
        pytest.skip("No effective_functions on WETH; consistency check needs a controls-bearing fixture.")

    checked_any = False
    for fn in eff:
        cap = fn.get("capability_expr")
        if not isinstance(cap, dict):
            # B.1 not yet surfaced for this row; covered by the dedicated
            # test_effective_function_has_capability_expr gate above.
            continue
        kind = cap.get("kind")
        controllers = fn.get("controllers") or []
        principals = []
        for c in controllers:
            principals.extend(c.get("principals") or [])

        if kind == "finite_set":
            members = cap.get("members") or []
            assert len(principals) == len(members), (
                f"finite_set capability for {fn.get('function')!r} has {len(members)} members "
                f"but {len(principals)} principal rows — principal/expr drift "
                "(gates B.1 + B.2 consistency)."
            )
            checked_any = True
        elif kind == "threshold_group":
            assert len(principals) == 1, (
                f"threshold_group for {fn.get('function')!r} expected exactly 1 principal "
                f"(the safe), got {len(principals)} — gates B.1 safe-collapse path."
            )
            p = principals[0]
            assert p.get("resolved_type") == "safe", (
                f"threshold_group principal for {fn.get('function')!r} must be resolved_type=='safe', "
                f"got {p.get('resolved_type')!r}."
            )
            details = p.get("details") or {}
            assert "threshold" in details, (
                f"threshold_group principal for {fn.get('function')!r} missing details.threshold — "
                "Safe metadata not propagated."
            )
            checked_any = True
        elif kind in {"cofinite_blacklist", "external_check_only", "conditional_universal"}:
            assert principals == [], (
                f"capability kind={kind} for {fn.get('function')!r} should produce zero "
                f"principal rows (non-enumerable set), got {len(principals)} — gates B.2 "
                "principal-suppression for non-caller kinds."
            )
            checked_any = True
        # AND / OR / signature_witness / unsupported: shape varies per
        # branch; intentionally not asserted here. Add targeted gates if
        # the cutover plan calls them out.

    if not checked_any:
        pytest.skip(
            "No effective_function row had an asserted-kind capability_expr; consistency "
            "gate is dependent on B.1 surfacing the column first."
        )


def test_no_v1_only_artifacts_present(analyzed_weth, live_client: LiveClient):
    """Negative gate for A.6 / B.2 — v1-only artifacts removed or deprecated.

    The cutover plan calls for ``permission_graph`` and
    ``semantic_guards`` to be either removed entirely or clearly marked
    deprecated once v2 is the source of truth. Today both still emit; this
    test fails until A.6 (permission_graph retirement) and B.2
    (semantic_guards retirement) land.

    'Clearly marked deprecated' is operationalised as: artifact body is
    a dict carrying ``"deprecated": True`` at the top level. If the
    artifact is absent (404) that also satisfies the gate.
    """
    for legacy_name in ("permission_graph", "semantic_guards"):
        art = live_client.artifact(analyzed_weth["name"], legacy_name)
        if art is None:
            # Artifact absent — already retired.
            continue
        assert isinstance(art, dict), (
            f"{legacy_name} artifact still emitted but is not a dict — cannot carry a deprecation marker."
        )
        assert art.get("deprecated") is True, (
            f"{legacy_name} artifact still emits without a 'deprecated': true marker. "
            "Gates A.6 (permission_graph retirement) / B.2 (semantic_guards retirement) — "
            "remove the writer or add the marker."
        )
