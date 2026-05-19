"""Live gate for the semantic predicate pipeline on a guarded company child."""

from __future__ import annotations

from typing import Any

import pytest

from tests.live.conftest import DEFAULT_SINGLE_TIMEOUT, LiveClient

EXPECTED_LEAF_KINDS = {
    "membership",
    "equality",
    "comparison",
    "external_bool",
    "signature_auth",
    "unsupported",
}
TYPED_LEAF_KINDS = {"equality", "membership", "external_bool", "signature_auth"}
AUTHORITY_LEAF_ROLES = {"caller_authority", "delegated_authority"}

EXPECTED_AUTHORITY_ROLES = {
    "caller_authority",
    "delegated_authority",
    "time",
    "reentrancy",
    "pause",
    "business",
}

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
GUARDED_PREDICATE_ADDRESS = "0x2c4a81e257381f87f5a5c4bd525116466d972e50"


def _iter_leaves(tree: dict[str, Any]):
    if not isinstance(tree, dict):
        return
    if tree.get("op") == "LEAF":
        leaf = tree.get("leaf")
        if isinstance(leaf, dict):
            yield leaf
        return
    for child in tree.get("children", []) or []:
        yield from _iter_leaves(child)


def _leaves_from_artifact(artifact: dict[str, Any]) -> list[dict[str, Any]]:
    trees = artifact.get("trees") or {}
    leaves: list[dict[str, Any]] = []
    for tree in trees.values():
        leaves.extend(_iter_leaves(tree))
    return leaves


@pytest.fixture(scope="module")
def guarded_contract_analysis(live_client: LiveClient) -> dict[str, Any]:
    job = live_client.submit_and_wait(GUARDED_PREDICATE_ADDRESS, timeout=DEFAULT_SINGLE_TIMEOUT * 2)
    if job.get("status") != "completed":
        pytest.fail(f"Guarded contract analysis did not complete: {job.get('status')} {job.get('error')}")

    artifact = live_client.artifact(job["name"], "predicate_trees")
    if not isinstance(artifact, dict):
        pytest.fail(f"{job.get('name')} {job.get('address')}: missing predicate_trees")
    trees = artifact.get("trees")
    if not isinstance(trees, dict) or not trees:
        pytest.fail(f"{job.get('name')} {job.get('address')}: no guarded trees")
    leaves = _leaves_from_artifact(artifact)
    if any(leaf.get("authority_role") in AUTHORITY_LEAF_ROLES for leaf in leaves):
        return {"job": job, "predicate_trees": artifact, "leaves": leaves}

    pytest.fail(
        "guarded contract produced predicate_trees but no authority leaves; "
        f"name={job.get('name')} address={job.get('address')} leaves={leaves[:10]}"
    )


def test_predicate_trees_artifact_exists(guarded_contract_analysis):
    artifact = guarded_contract_analysis["predicate_trees"]
    trees = artifact.get("trees")

    assert artifact.get("schema_version") == "semantic", (
        f"predicate_trees.schema_version must be 'semantic', got {artifact.get('schema_version')!r}"
    )
    assert isinstance(trees, dict) and trees, "guarded child predicate_trees.trees must be non-empty"


def test_predicate_trees_has_typed_leaves(guarded_contract_analysis):
    leaves = guarded_contract_analysis["leaves"]
    assert leaves, "guarded child predicate_trees must contain at least one leaf"

    saw_typed_leaf = False
    saw_authority_leaf = False
    for leaf in leaves:
        kind = leaf.get("kind")
        role = leaf.get("authority_role")
        assert kind in EXPECTED_LEAF_KINDS, (
            f"Leaf kind {kind!r} is not in the closed semantic LeafKind set ({sorted(EXPECTED_LEAF_KINDS)})"
        )
        assert role in EXPECTED_AUTHORITY_ROLES, (
            f"Leaf authority_role {role!r} is not in the closed semantic AuthorityRole set "
            f"({sorted(EXPECTED_AUTHORITY_ROLES)})"
        )
        saw_typed_leaf = saw_typed_leaf or kind in TYPED_LEAF_KINDS
        saw_authority_leaf = saw_authority_leaf or role in AUTHORITY_LEAF_ROLES

    assert saw_typed_leaf, f"No leaf with kind in {sorted(TYPED_LEAF_KINDS)} found"
    assert saw_authority_leaf, f"No authority leaf with role in {sorted(AUTHORITY_LEAF_ROLES)} found"


def test_capability_resolution_returns_non_empty(guarded_contract_analysis, live_client: LiveClient):
    job = guarded_contract_analysis["job"]
    addr = (job.get("address") or "").lower()
    assert addr.startswith("0x"), f"guarded child address missing or malformed: {addr!r}"

    resp = live_client._session.get(
        live_client._url(f"/api/contract/{addr}/capabilities"),
        timeout=30,
    )
    assert resp.status_code == 200, (
        f"GET /api/contract/{addr}/capabilities returned {resp.status_code}: {resp.text[:400]!r}"
    )
    body = resp.json()
    caps = body.get("capabilities")
    assert isinstance(caps, dict), "capabilities response must include a dict keyed on function signature"
    assert caps, "guarded child capability map must be non-empty"

    for fn_sig, cap in caps.items():
        assert isinstance(cap, dict), f"capabilities[{fn_sig}] must be a dict"
        kind = cap.get("kind")
        assert kind in EXPECTED_CAPABILITY_KINDS, (
            f"CapabilityExpr.kind {kind!r} for {fn_sig} not in closed CapKind set ({sorted(EXPECTED_CAPABILITY_KINDS)})"
        )


def test_effective_function_has_capability_expr(guarded_contract_analysis, live_client: LiveClient):
    job = guarded_contract_analysis["job"]
    detail = live_client.analysis_detail(job["name"])
    functions = (detail.get("effective_permissions") or {}).get("functions") or []

    assert functions, "guarded child must produce effective_permissions.functions rows"
    assert any(fn.get("capability_expr") is not None for fn in functions), (
        f"None of {len(functions)} effective function rows carries capability_expr"
    )


def test_effective_function_principal_consistent_with_capability_expr(
    guarded_contract_analysis,
    live_client: LiveClient,
):
    job = guarded_contract_analysis["job"]
    detail = live_client.analysis_detail(job["name"])
    functions = (detail.get("effective_permissions") or {}).get("functions") or []

    checked_any = False
    for fn in functions:
        cap = fn.get("capability_expr")
        if not isinstance(cap, dict):
            continue
        kind = cap.get("kind")
        principals = []
        for controller in fn.get("controllers") or []:
            principals.extend(controller.get("principals") or [])

        if kind == "finite_set":
            members = cap.get("members") or []
            assert len(principals) == len(members), (
                f"finite_set capability for {fn.get('function')!r} has {len(members)} members "
                f"but {len(principals)} principal rows"
            )
            checked_any = True
        elif kind == "threshold_group":
            assert len(principals) == 1, (
                f"threshold_group for {fn.get('function')!r} expected exactly 1 principal, got {len(principals)}"
            )
            principal = principals[0]
            assert principal.get("resolved_type") == "safe", (
                f"threshold_group principal for {fn.get('function')!r} must be resolved_type='safe', "
                f"got {principal.get('resolved_type')!r}"
            )
            assert "threshold" in (principal.get("details") or {}), (
                f"threshold_group principal for {fn.get('function')!r} missing details.threshold"
            )
            checked_any = True
        elif kind in {"cofinite_blacklist", "external_check_only", "conditional_universal"}:
            assert principals == [], (
                f"capability kind={kind} for {fn.get('function')!r} should produce zero principal rows, "
                f"got {len(principals)}"
            )
            checked_any = True

    assert checked_any, "No effective function row had an asserted-kind capability_expr"


def test_no_retired_artifacts_present(guarded_contract_analysis, live_client: LiveClient):
    job = guarded_contract_analysis["job"]
    for retired_name in ("permission_graph", "semantic_guards"):
        artifact = live_client.artifact(job["name"], retired_name)
        if artifact is None:
            continue
        assert isinstance(artifact, dict), f"{retired_name} still emits but is not a dict"
        assert artifact.get("deprecated") is True, f"{retired_name} still emits without deprecated=true"
