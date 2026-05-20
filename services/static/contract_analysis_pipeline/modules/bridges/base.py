"""Shared helpers for bridge-context static modules."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Protocol

from schemas.contract_analysis import BridgeStaticFact

_CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_TOKEN_SPLIT_RE = re.compile(r"[^A-Za-z0-9]+")


class BridgeModule(Protocol):
    name: str

    def detect_function(self, fn: Any, effect_info: Mapping[str, Any] | None = None) -> list[BridgeStaticFact]: ...


def full_name(fn: Any) -> str:
    return str(getattr(fn, "full_name", None) or getattr(fn, "name", None) or "<anonymous>")


def function_name(fn: Any) -> str:
    return str(getattr(fn, "name", None) or full_name(fn).split("(", 1)[0])


def compact(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def tokens(value: object) -> set[str]:
    out: set[str] = set()
    for part in _TOKEN_SPLIT_RE.split(str(value or "")):
        if not part:
            continue
        for token in _CAMEL_BOUNDARY_RE.sub(" ", part).split():
            normalized = token.lower()
            if normalized:
                out.add(normalized)
    return out


def parameter_names(fn: Any) -> set[str]:
    return {compact(getattr(param, "name", "") or "") for param in getattr(fn, "parameters", []) or []}


def parameter_tokens(fn: Any) -> set[str]:
    out: set[str] = set()
    for param in getattr(fn, "parameters", []) or []:
        out.update(tokens(getattr(param, "name", "") or ""))
    return out


def parameter_type_names(fn: Any) -> set[str]:
    return {compact(getattr(param, "type", "") or "") for param in getattr(fn, "parameters", []) or []}


def function_name_tokens(fn: Any) -> set[str]:
    return tokens(function_name(fn))


def function_name_compact(fn: Any) -> str:
    return compact(function_name(fn))


def contract_type_names(fn: Any) -> set[str]:
    contract = getattr(fn, "contract", None)
    names = {compact(getattr(contract, "name", "") or "")}
    for attr in ("inheritance", "inheritance_reverse"):
        for inherited in getattr(contract, attr, []) or []:
            names.add(compact(getattr(inherited, "name", "") or ""))
    return {name for name in names if name}


def state_variable_names(fn: Any) -> set[str]:
    out: set[str] = set()
    for attr in ("all_state_variables_read", "all_state_variables_written"):
        getter = getattr(fn, attr, None)
        if not callable(getter):
            continue
        try:
            variables = getter()
        except Exception:
            continue
        if not isinstance(variables, (list, tuple, set)):
            continue
        for variable in variables or []:
            name = compact(getattr(variable, "name", "") or "")
            if name:
                out.add(name)
    return out


def state_variable_tokens(fn: Any) -> set[str]:
    out: set[str] = set()
    for attr in ("all_state_variables_read", "all_state_variables_written"):
        getter = getattr(fn, attr, None)
        if not callable(getter):
            continue
        try:
            variables = getter()
        except Exception:
            continue
        if not isinstance(variables, (list, tuple, set)):
            continue
        for variable in variables or []:
            out.update(tokens(getattr(variable, "name", "") or ""))
    return out


def state_variable_type_names(fn: Any) -> set[str]:
    out: set[str] = set()
    for attr in ("all_state_variables_read", "all_state_variables_written"):
        getter = getattr(fn, attr, None)
        if not callable(getter):
            continue
        try:
            variables = getter()
        except Exception:
            continue
        if not isinstance(variables, (list, tuple, set)):
            continue
        for variable in variables or []:
            type_name = compact(getattr(variable, "type", "") or "")
            if type_name:
                out.add(type_name)
    return out


def effect_labels(effect_info: Mapping[str, Any] | None) -> set[str]:
    if not isinstance(effect_info, Mapping):
        return set()
    return {str(label) for label in effect_info.get("effect_labels") or []}


def effect_targets(effect_info: Mapping[str, Any] | None) -> set[str]:
    if not isinstance(effect_info, Mapping):
        return set()
    out = {compact(target) for target in effect_info.get("effect_targets") or []}
    for sink in effect_info.get("sinks") or []:
        if isinstance(sink, Mapping):
            out.add(compact(sink.get("target") or ""))
    return {target for target in out if target}


def fact(
    kind: str,
    protocol: str,
    fn: Any,
    evidence: str,
    confidence: str = "medium",
    *,
    module: str | None = None,
    display: str | None = None,
) -> BridgeStaticFact:
    out: BridgeStaticFact = {
        "kind": kind,
        "protocol": protocol,
        "function": full_name(fn),
        "evidence": evidence,
        "confidence": confidence,
    }
    if module:
        out["module"] = module
    if display:
        out["display"] = display
    return out
