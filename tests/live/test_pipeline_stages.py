"""Live integration tests for the single-contract pipeline output.

Asserts that a completed WETH analysis reaches the ``done`` stage and
emits the artifacts every downstream stage is supposed to produce:
discovery → static → resolution → policy → coverage.

The asserted artifacts are intentionally narrow: if policy/coverage add a
new output, adding it here is a good signal, but churny additions that
WETH doesn't always produce should live in a protocol-specific test, not
this smoke.
"""

from __future__ import annotations

import pytest

from tests.live.conftest import LiveClient


def test_pipeline_reaches_done_stage(analyzed_weth):
    assert analyzed_weth["status"] == "completed"
    assert analyzed_weth["stage"] == "done"


def test_contract_analysis_artifact(analyzed_weth, live_client: LiveClient):
    art = live_client.artifact(analyzed_weth["name"], "contract_analysis")
    assert isinstance(art, dict)
    subject = art.get("subject")
    assert isinstance(subject, dict) and subject.get("name"), "contract_analysis.subject.name missing"
    assert "summary" in art, "contract_analysis.summary missing"


def test_analysis_report_is_text(analyzed_weth, live_client: LiveClient):
    # Slither's plain-text report — served as text/plain by the artifact route.
    art = live_client.artifact(analyzed_weth["name"], "analysis_report")
    assert isinstance(art, str), f"expected str body, got {type(art).__name__}"
    assert art.strip(), "analysis_report should not be empty"


def test_contract_flags_artifact(analyzed_weth, live_client: LiveClient):
    art = live_client.artifact(analyzed_weth["name"], "contract_flags")
    assert isinstance(art, dict)
    assert "is_proxy" in art, "contract_flags.is_proxy missing"


def test_dependencies_artifact(analyzed_weth, live_client: LiveClient):
    art = live_client.artifact(analyzed_weth["name"], "dependencies")
    assert isinstance(art, dict)
    assert "dependencies" in art


def test_control_tracking_plan_artifact(analyzed_weth, live_client: LiveClient):
    # WETH has no privileged controls, so the plan can legitimately be empty,
    # but the artifact itself must still be emitted by the static stage.
    art = live_client.artifact(analyzed_weth["name"], "control_tracking_plan")
    assert isinstance(art, dict), "control_tracking_plan artifact should exist"


@pytest.mark.parametrize(
    "artifact_name",
    ["control_snapshot", "effective_permissions", "principal_labels"],
)
def test_unconditionally_emitted_artifacts(analyzed_weth, live_client: LiveClient, artifact_name: str):
    """Resolution + policy each unconditionally emit these — see
    resolution_worker.py:97 (control_snapshot) and policy_worker.py:213/283
    (effective_permissions / principal_labels). A missing one here means
    the pipeline stage silently skipped."""
    art = live_client.artifact(analyzed_weth["name"], artifact_name)
    assert art is not None, f"{artifact_name} artifact was not emitted"
    assert isinstance(art, (dict, list)), f"{artifact_name} should be structured JSON"


def test_resolved_control_graph_when_non_empty(analyzed_weth, live_client: LiveClient):
    # resolution_worker.py:142 only stores this artifact when the graph has
    # nodes. WETH has no privileged controls, so this artifact is expected
    # to be absent — the test asserts that when present it's well-formed,
    # rather than treating absence as a failure.
    art = live_client.artifact(analyzed_weth["name"], "resolved_control_graph")
    if art is None:
        pytest.skip("resolved_control_graph not emitted (expected for controls-free contracts)")
    assert isinstance(art, dict)
    assert "nodes" in art or "edges" in art
