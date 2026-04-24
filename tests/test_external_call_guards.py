"""Phase 2b projection tests for the legacy `external_call_guards` shape.

The underlying detection logic is now `caller_reach_analysis` (tested
in `test_caller_sinks.py`). This file pins the *projection* from the
canonical `CallerSink` list to the legacy `ExternalCallGuard` dicts —
the consumers that haven't migrated yet (policy_worker's v5 bridge,
downstream effective_permissions reads) expect that exact shape.

The old unit tests on `_infer_external_call_guards` /
`_extract_external_call_guards_from_nodes` were retired along with the
detectors. Their semantic guarantees are preserved:
- Pattern A: `X.onlyFoo(msg.sender)` — captured with method name.
- Pattern B: `X.hasRole(ROLE, msg.sender)` — role in `role_args`.
- Rejected: non-state-var destinations, missing msg.sender flow,
  empty method names.
- All of the above continue to hold via `caller_reach_analysis` →
  `sinks_to_external_call_guards` composition.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from typing import Any, cast  # noqa: E402

from services.static.contract_analysis_pipeline.caller_sinks import (  # noqa: E402
    sinks_to_external_call_guards as _project,
)


def sinks_to_external_call_guards(sinks: list[dict]) -> list[dict]:
    """Test shim — `list[dict]` is structurally a `list[CallerSink]`,
    but pyright's TypedDict invariance blocks the assignment. We cast
    once here rather than sprinkle `cast` across every call site."""
    return _project(cast(Any, sinks))


# ---------------------------------------------------------------------------
# Tests — projection sinks -> legacy external_call_guards
# ---------------------------------------------------------------------------


def _sink(
    *,
    kind: str = "caller_external_call",
    target: str = "roleManager",
    target_type: str = "IRoleManager",
    method: str = "onlyFoo",
    role_args: list[str] | None = None,
    evidence: dict | None = None,
) -> dict:
    rec: dict = {
        "kind": kind,
        "evidence": evidence or {"detail": "test"},
        "revert_on_mismatch": True,
        "external_target_state_var": target,
        "target_type": target_type,
        "external_method": method,
    }
    if role_args:
        rec["external_role_args"] = role_args
    return rec


def test_projection_emits_legacy_shape_with_required_fields():
    """Each caller_external_call sink becomes one ExternalCallGuard
    dict carrying target_state_var/target_type/method/sender_in_args."""
    out = sinks_to_external_call_guards([_sink()])
    assert len(out) == 1
    r = out[0]
    assert r["target_state_var"] == "roleManager"
    assert r["target_type"] == "IRoleManager"
    assert r["method"] == "onlyFoo"
    assert r["sender_in_args"] is True
    assert r["kind"] == "inline"


def test_projection_carries_role_args_through():
    """Pattern B: `hasRole(PROTOCOL_PAUSER, msg.sender)` — the role
    argument survives the projection."""
    out = sinks_to_external_call_guards([_sink(method="hasRole", role_args=["PROTOCOL_PAUSER"])])
    assert out[0]["role_args"] == ["PROTOCOL_PAUSER"]


def test_projection_drops_non_external_call_sinks():
    """caller_equals, caller_in_mapping, caller_signature, caller_merkle,
    caller_internal_call, caller_unknown — none of these are in the
    legacy external-call-guard vocabulary, so the projection skips them."""
    other_kinds = [
        _sink(kind="caller_equals"),
        _sink(kind="caller_in_mapping"),
        _sink(kind="caller_internal_call"),
        _sink(kind="caller_signature"),
        _sink(kind="caller_merkle"),
        _sink(kind="caller_unknown"),
    ]
    assert sinks_to_external_call_guards(other_kinds) == []


def test_projection_drops_sinks_missing_target_or_method():
    """Legacy consumers require non-empty target + method. A sink
    where either is missing is dropped."""
    missing_target = _sink(target="", target_type="")
    missing_method = _sink(method="")
    out = sinks_to_external_call_guards([missing_target, missing_method])
    assert out == []


def test_projection_preserves_order():
    """Multiple sinks project in order — two different external calls
    on the same function must both appear, not get deduped."""
    a = _sink(target="roleA", method="onlyA")
    b = _sink(target="roleB", method="onlyB")
    out = sinks_to_external_call_guards([a, b])
    assert [r["method"] for r in out] == ["onlyA", "onlyB"]


def test_projection_handles_empty_list():
    assert sinks_to_external_call_guards([]) == []


def test_projection_does_not_crash_on_missing_optional_fields():
    """Sinks that don't carry `external_role_args` must still project —
    the field is just absent in the output."""
    minimal: dict = {
        "kind": "caller_external_call",
        "external_target_state_var": "role",
        "target_type": "IRole",
        "external_method": "check",
    }
    out = sinks_to_external_call_guards([minimal])
    assert len(out) == 1
    assert "role_args" not in out[0]
