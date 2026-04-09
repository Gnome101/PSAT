"""Summary and compatibility views for contract analysis."""

from __future__ import annotations

import re
from pathlib import Path

from schemas.contract_analysis import (
    AccessControlAnalysis,
    ContractClassification,
    ControlModel,
    PausabilityAnalysis,
    PermissionGraph,
    RiskLevel,
    RoleDefinition,
    SlitherFinding,
    SlitherSummary,
    TimelockAnalysis,
    TrackingHint,
    UpgradeabilityAnalysis,
)

from .constants import (
    ACCESS_CONTROL_INHERITANCE,
    ACCESS_GUARD_KEYWORDS,
    ADMIN_VAR_KEYWORDS,
    FACTORY_NAME_KEYWORDS,
    PAUSE_MODIFIER_KEYWORDS,
    ROLE_CONSTANT_PATTERN,
    ROLE_NAME_PATTERNS,
    SEVERITY_ORDER,
    STANDARD_EVENTS,
    STANDARD_SIGNATURES,
)
from .graph import privileged_functions_from_graph
from .shared import (
    _all_modifiers,
    _all_state_variables,
    _call_or_value,
    _contract_events,
    _contract_functions,
    _contract_signatures,
    _declaring_contract_name,
    _dedupe_strings,
    _entry_points,
    _function_effects,
    _node_contains_require_or_assert,
    _source_evidence,
    _source_fragment,
)


def _looks_like_access_guard(name: str) -> bool:
    lowered = name.lower()
    return any(keyword in lowered for keyword in ACCESS_GUARD_KEYWORDS)


def _looks_like_pause_guard(name: str) -> bool:
    lowered = name.lower()
    return any(keyword in lowered for keyword in PAUSE_MODIFIER_KEYWORDS)


def _normalize_guard_label(name: str) -> str:
    lowered = name.lower()
    if "owner" in lowered:
        return "owner"
    if "role" in lowered:
        return "role"
    if "timelock" in lowered:
        return "timelock"
    if "admin" in lowered:
        return "admin"
    if "guardian" in lowered:
        return "guardian"
    if "pauser" in lowered:
        return "pauser"
    return name


def _role_constants_from_function(function, project_dir: Path) -> list[str]:
    roles = set(ROLE_CONSTANT_PATTERN.findall(_source_fragment(function, project_dir)))

    for variable in _call_or_value(function, "all_state_variables_read"):
        name = getattr(variable, "name", "")
        if ROLE_CONSTANT_PATTERN.fullmatch(name):
            roles.add(name)

    for call in _call_or_value(function, "all_internal_calls"):
        for argument in getattr(call, "arguments", []) or []:
            name = getattr(argument, "name", "")
            if ROLE_CONSTANT_PATTERN.fullmatch(name):
                roles.add(name)

    return sorted(roles)


def _state_variable_looks_like_auth(name: str) -> bool:
    lowered = name.lower()
    return (
        any(keyword in lowered for keyword in ADMIN_VAR_KEYWORDS)
        or lowered in {"wards", "admins", "roles", "authority", "authorities"}
        or "auth" in lowered
        or "role" in lowered
        or "ward" in lowered
    )


def _high_level_call_guards(function) -> list[str]:
    guards = []
    for node in getattr(function, "nodes", []):
        for _, call in getattr(node, "high_level_calls", []) or []:
            function_name = getattr(call, "function_name", None) or getattr(call, "function", None)
            named_function = getattr(function_name, "name", None) if function_name is not None else None
            if named_function is not None:
                function_name = named_function
            if str(function_name) != "canCall":
                continue
            destination = getattr(call, "destination", None)
            destination_name = getattr(destination, "name", None) or str(destination)
            guards.append(f"{destination_name}.canCall")
    return guards


def _internal_auth_calls(function) -> list[str]:
    guards = []
    for call in _call_or_value(function, "all_internal_calls"):
        if type(call).__name__ != "InternalCall":
            continue
        callee = getattr(call, "function", None)
        callee_name = getattr(callee, "name", None) or str(callee)
        lowered = callee_name.lower()
        if any(token in lowered for token in ("checkauth", "checkrole", "authorize", "authoriz", "auth")):
            guards.append(callee_name)
    return guards


def _infer_function_guards(function, project_dir: Path) -> list[str]:
    guards = []
    modifier_names = [modifier.name for modifier in getattr(function, "modifiers", [])]
    guards.extend(modifier_names)
    guards.extend(_normalize_guard_label(name) for name in modifier_names)

    role_constants = _role_constants_from_function(function, project_dir)
    guards.extend(role_constants)
    if role_constants:
        guards.append("role")

    guards.extend(_internal_auth_calls(function))
    guards.extend(_high_level_call_guards(function))

    all_solidity_reads = [str(variable) for variable in _call_or_value(function, "all_solidity_variables_read")]
    all_state_reads = [
        getattr(variable, "name", "") for variable in _call_or_value(function, "all_state_variables_read")
    ]
    if "msg.sender" in all_solidity_reads:
        guards.extend(name for name in all_state_reads if _state_variable_looks_like_auth(name))

    for node in getattr(function, "nodes", []):
        if not _node_contains_require_or_assert(node):
            continue
        node_solidity_reads = [str(variable) for variable in getattr(node, "solidity_variables_read", [])]
        node_state_reads = [getattr(variable, "name", "") for variable in getattr(node, "state_variables_read", [])]
        if "msg.sender" in node_solidity_reads:
            guards.extend(name for name in node_state_reads if _state_variable_looks_like_auth(name))
        guards.extend(ROLE_CONSTANT_PATTERN.findall(str(getattr(node, "expression", "") or "")))

    return _dedupe_strings(guards)


def _is_access_control_guard_label(label: str) -> bool:
    return (
        _looks_like_access_guard(label)
        or _state_variable_looks_like_auth(label)
        or ROLE_CONSTANT_PATTERN.fullmatch(label) is not None
        or label == "role"
    )


def _access_control_inferred_guards(function, project_dir: Path) -> list[str]:
    return [label for label in _infer_function_guards(function, project_dir) if _is_access_control_guard_label(label)]


def _controller_refs_from_inferred_guards(guards: list[str]) -> list[str]:
    refs = []
    for guard in guards:
        normalized = _normalize_guard_label(guard)
        lowered = guard.lower()
        if _is_access_control_guard_label(normalized):
            preserve_explicit = (
                normalized != guard
                and not lowered.startswith(("only", "_"))
                and (
                    (
                        normalized in {"owner", "authority", "admin", "timelock", "governance", "guardian", "pauser"}
                        and ("_" in guard or any(char.isupper() for char in guard))
                    )
                    or (
                        normalized == "role"
                        and any(token in lowered for token in ("registry", "controller"))
                        and _state_variable_looks_like_auth(guard)
                    )
                )
            )
            if preserve_explicit:
                refs.append(guard)
            refs.append(normalized)
    return _dedupe_strings(refs)


def _controller_refs_from_effect_targets(effect_targets: list[str]) -> list[str]:
    refs: list[str] = []
    for target in effect_targets:
        lowered = str(target).lower()
        if ".onlyprotocolupgrader" in lowered and "roleregistry" in lowered:
            refs.append("roleRegistry")
    return _dedupe_strings(refs)


def _has_graph_permission_evidence(graph_entry: dict[str, object] | None) -> bool:
    if not graph_entry:
        return False
    guards = graph_entry.get("guards", [])
    guard_kinds = graph_entry.get("guard_kinds", [])
    controller_refs = graph_entry.get("controller_refs", [])
    return bool(guards or guard_kinds or controller_refs)


def _internal_call_names(function) -> list[str]:
    names = []
    for call in _call_or_value(function, "all_internal_calls"):
        callee = getattr(call, "function", None)
        name = getattr(callee, "name", None) or getattr(call, "name", None)
        if name:
            names.append(name)
    return _dedupe_strings(names)


def _effect_targets(function, graph_entry: dict | None, effects: list[str]) -> list[str]:
    if graph_entry:
        return list(graph_entry.get("effect_targets", []))
    return [effect.split(":", 1)[1] for effect in effects if effect.startswith("writes:")]


# ---------------------------------------------------------------------------
# Structural detection helpers (name-independent, AST/IR-based)
# ---------------------------------------------------------------------------

# Known ERC20 function selectors (decimal form as Slither represents them)
_KNOWN_SELECTORS: dict[int, str] = {
    0xA9059CBB: "asset_send",  # transfer(address,uint256)
    0x23B872DD: "asset_pull",  # transferFrom(address,address,uint256)
    0x40C10F19: "mint",  # mint(address,uint256)
    0x42966C68: "burn",  # burn(uint256)
    0x9DC29FAC: "burn",  # burn(address,uint256)
    0x79CC6790: "burn",  # burnFrom(address,uint256)
}


def _function_has_low_level_value_call(function) -> bool:
    """Check if the function (or any internal function it calls) sends ETH via .call{value:}."""
    visited: set[int] = set()

    def _check(fn) -> bool:
        fn_id = id(fn)
        if fn_id in visited:
            return False
        visited.add(fn_id)
        for node in fn.nodes:
            for ir in node.irs:
                ir_str = str(ir)
                if "LOW_LEVEL_CALL" in ir_str and "value:" in ir_str:
                    return True
        for call in _call_or_value(fn, "all_internal_calls"):
            callee = getattr(call, "function", call) if not callable(call) else call
            if hasattr(callee, "nodes") and _check(callee):
                return True
        return False

    return _check(function)


def _writes_delegatecall_target(function) -> bool:
    """Structural impl detection: does this function write a state var that
    a fallback/receive reads before delegatecalling?  Works regardless of
    variable name."""
    contract = function.contract
    written_vars = set(function.all_state_variables_written())
    if not written_vars:
        return False

    for fn in contract.functions:
        if not (fn.is_fallback or fn.is_receive):
            continue
        # Check if fallback has delegatecall in IR
        has_dc = any("delegatecall" in str(ir).lower() for node in fn.nodes for ir in node.irs)
        if not has_dc:
            continue
        # Check if fallback reads any var this function writes
        fallback_reads = set(fn.all_state_variables_read())
        if written_vars & fallback_reads:
            return True
    return False


def _writes_assembly_delegatecall_slot(function) -> bool:
    """Detect assembly sstore to a slot that the fallback sloads before delegatecalling."""
    contract = function.contract

    # Find sstore slots in this function
    sstore_slots: set[str] = set()
    for node in function.nodes:
        for ir in node.irs:
            ir_str = str(ir)
            if "sstore" in ir_str.lower():
                # Extract slot argument: sstore(slot, value)
                # IR looks like: SOLIDITY_CALL sstore(uint256,uint256)(slot_var, val_var)
                parts = ir_str.split("(")
                if len(parts) >= 3:
                    args = parts[-1].rstrip(")")
                    slot_arg = args.split(",")[0].strip()
                    sstore_slots.add(slot_arg)
    if not sstore_slots:
        return False

    # Check if fallback sloads the same slot and delegatecalls the result
    for fn in contract.functions:
        if not (fn.is_fallback or fn.is_receive):
            continue
        sload_slots: set[str] = set()
        has_dc = False
        for node in fn.nodes:
            for ir in node.irs:
                ir_str = str(ir)
                if "sload" in ir_str.lower():
                    parts = ir_str.split("(")
                    if len(parts) >= 3:
                        args = parts[-1].rstrip(")")
                        sload_slots.add(args.split(",")[0].strip())
                if "delegatecall" in ir_str.lower():
                    has_dc = True
        if has_dc and sstore_slots & sload_slots:
            return True
    return False


def _writes_pause_like_bool(function) -> bool:
    """Structural pause detection: does this function write a bool state var
    that a modifier reads, and that modifier gates other functions?"""
    contract = function.contract
    written_bools = {v for v in function.all_state_variables_written() if str(getattr(v, "type", "")) == "bool"}
    if not written_bools:
        return False
    for modifier in contract.modifiers:
        mod_bools = {v for v in modifier.all_state_variables_read() if str(getattr(v, "type", "")) == "bool"}
        if not (written_bools & mod_bools):
            continue
        # This modifier reads a bool we write — check if it gates other functions
        for fn in contract.functions:
            if fn != function and modifier in fn.modifiers:
                return True
    return False


def _writes_owner_like_address(function) -> bool:
    """Structural ownership detection: does this function write an address state var
    that a modifier compares against msg.sender?"""
    contract = function.contract
    written_addrs = {v for v in function.all_state_variables_written() if str(getattr(v, "type", "")) == "address"}
    if not written_addrs:
        return False
    written_names = {getattr(v, "name", "").lower() for v in written_addrs}
    for modifier in contract.modifiers:
        for node in modifier.nodes:
            for ir in node.irs:
                ir_str = str(ir).lower()
                # Look for: TMP = msg.sender == <var_name>
                if "msg.sender" in ir_str:
                    for name in written_names:
                        if name and name in ir_str:
                            return True
    return False


def _writes_authority_reference(function) -> bool:
    """Structural authority detection: does this function write an address state var
    that a modifier makes a high-level call to (i.e., calls it for auth checks)?"""
    contract = function.contract
    written_vars = set(function.all_state_variables_written())
    if not written_vars:
        return False
    for modifier in contract.modifiers:
        # Get all state vars that the modifier calls (not just reads/compares)
        for callee_contract, call_ir in modifier.all_high_level_calls():
            ir_str = str(call_ir)
            # The IR contains "dest:VARNAME(Type)" — check if the dest is a var we write
            for var in written_vars:
                var_name = getattr(var, "name", "")
                if var_name and f"dest:{var_name}" in ir_str:
                    return True
    return False


def _writes_hook_reference(function) -> bool:
    """Structural hook detection: does this function write an address state var
    that another function calls AND that other function also writes to a mapping
    (i.e., it's a transfer/state-changing function with a hook callback)?"""
    contract = function.contract
    written_vars = set(function.all_state_variables_written())
    if not written_vars:
        return False
    for fn in contract.functions:
        if fn == function or fn.is_constructor:
            continue
        # Does this function write to a mapping? (balance-changing function)
        writes_mapping = any("mapping" in str(getattr(v, "type", "")) for v in fn.all_state_variables_written())
        if not writes_mapping:
            continue
        # Does it call a state var that our function writes?
        for callee_contract, call_ir in fn.all_high_level_calls():
            ir_str = str(call_ir)
            for var in written_vars:
                var_name = getattr(var, "name", "")
                if var_name and f"dest:{var_name}" in ir_str:
                    return True
    return False


def _detect_encoded_selectors(function) -> set[str]:
    """Scan IR for abi.encodeWithSelector calls with known ERC20 selectors."""
    labels: set[str] = set()
    visited: set[int] = set()

    def _check(fn) -> None:
        fn_id = id(fn)
        if fn_id in visited:
            return
        visited.add(fn_id)
        for node in fn.nodes:
            for ir in node.irs:
                ir_str = str(ir)
                if "abi.encodeWithSelector" not in ir_str:
                    continue
                # Extract the selector value from IR
                # IR: TMP = SOLIDITY_CALL abi.encodeWithSelector()(2835717307,to,amount)
                paren_start = ir_str.rfind("(")
                if paren_start < 0:
                    continue
                args = ir_str[paren_start + 1 :].rstrip(")")
                first_arg = args.split(",")[0].strip()
                try:
                    selector_val = int(first_arg)
                    label = _KNOWN_SELECTORS.get(selector_val)
                    if label:
                        labels.add(label)
                except (ValueError, TypeError):
                    pass
        for call in _call_or_value(fn, "all_internal_calls"):
            callee = getattr(call, "function", call) if not callable(call) else call
            if hasattr(callee, "nodes"):
                _check(callee)

    _check(function)
    return labels


# ---------------------------------------------------------------------------
# Main effect label function
# ---------------------------------------------------------------------------


def _effect_labels(function, effects: list[str], effect_targets: list[str], graph_entry: dict | None) -> list[str]:
    labels: set[str] = set()
    targets_lower = {target.lower() for target in effect_targets}
    sink_kinds = set(graph_entry.get("sink_kinds", [])) if graph_entry else set()

    # --- Layer 1: Slither's own effect classification ---
    _EFFECT_MAP = {
        "pause_state_change": "pause_toggle",
        "upgrade_control": "implementation_update",
        "ownership_change": "ownership_transfer",
        "role_management": "role_management",
        "mint_capability": "mint",
        "burn_capability": "burn",
        "timelock_control": "timelock_operation",
        "factory_deployment": "contract_deployment",
        "delegatecall_control": "delegatecall_execution",
        "selfdestruct_capability": "selfdestruct_capability",
        "privileged_external_call": "external_contract_call",
    }
    for effect in effects:
        mapped = _EFFECT_MAP.get(effect)
        if mapped:
            labels.add(mapped)

    # --- Layer 2: Structural detection (name-independent) ---

    # Pause: writes a bool that a modifier reads and gates other functions
    if _writes_pause_like_bool(function):
        labels.add("pause_toggle")

    # Ownership: writes an address var that a modifier compares to msg.sender
    if _writes_owner_like_address(function):
        labels.add("ownership_transfer")

    # Implementation update: writes a var the fallback reads before delegatecall
    if _writes_delegatecall_target(function):
        labels.add("implementation_update")

    # Implementation update: assembly sstore to a slot the fallback sloads + delegatecalls
    if _writes_assembly_delegatecall_slot(function):
        labels.add("implementation_update")

    # Asset send: low-level .call{value:} (ETH transfer)
    if _function_has_low_level_value_call(function):
        labels.add("asset_send")

    # Encoded selectors: abi.encodeWithSelector with known ERC20 selectors
    labels.update(_detect_encoded_selectors(function))

    # --- Layer 3: Structural authority/hook + arbitrary call detection ---
    if any(target.endswith(".functioncallwithvalue") for target in targets_lower):
        labels.discard("external_contract_call")
        labels.add("arbitrary_external_call")

    # Authority: writes an address var that a modifier calls for auth checks
    if _writes_authority_reference(function):
        labels.add("authority_update")

    # Hook: writes an address var that a mapping-writing function calls
    if _writes_hook_reference(function):
        labels.add("hook_update")

    # --- Layer 4: Sink kinds from permission graph ---
    if sink_kinds.intersection({"contract_creation"}):
        labels.add("contract_deployment")
    if sink_kinds.intersection({"delegatecall"}):
        labels.add("delegatecall_execution")
    if sink_kinds.intersection({"selfdestruct"}):
        labels.add("selfdestruct_capability")

    # --- Layer 5: Internal + cross-contract call targets ---
    # Internal calls to _mint/_burn (the contract's own functions, not external guessing)
    internal_call_names = {name.lower() for name in _internal_call_names(function)}
    if any(name in internal_call_names for name in {"_mint", "mint"}):
        labels.add("mint")
    if any(name in internal_call_names for name in {"_burn", "burn"}):
        labels.add("burn")
    # Cross-contract: external calls to .mint()/.burn()/.transfer() etc.
    if any(target.endswith(".mint") for target in targets_lower):
        labels.add("mint")
    if any(target.endswith(".burn") or target.endswith(".burnfrom") for target in targets_lower):
        labels.add("burn")
    if any(target.endswith(".transfer") or target.endswith(".safetransfer") for target in targets_lower):
        labels.add("asset_send")
    if any(target.endswith(".transferfrom") or target.endswith(".safetransferfrom") for target in targets_lower):
        labels.add("asset_pull")

    # Downgrade generic external_contract_call when a more specific label applies
    if labels.intersection({"asset_pull", "asset_send", "arbitrary_external_call", "mint", "burn"}):
        labels.discard("external_contract_call")

    return _dedupe_strings(list(labels))


_TRANSFER_METHODS = {
    "transfer": "out",
    "safetransfer": "out",
    "transferfrom": "in",
    "safetransferfrom": "in",
    "mint": "mint",
    "burn": "burn",
}


def _extract_value_flows(function) -> list[dict]:
    """Extract detailed value flow info: which tokens move in/out via which calls.

    Returns a list of dicts:
        {"direction": "in"|"out"|"mint"|"burn"|"eth_out",
         "token_var": "rewardsToken"|None,
         "token_type": "IERC20"|"address"|None,
         "method": "transfer"|"call{value}"|etc,
         "is_parameter": True if the token is a function param (arbitrary token)}
    """
    flows: list[dict] = []
    param_names = {p.name.lower() for p in function.parameters}

    # High-level calls: token.transfer(), token.mint(), etc.
    for _ct, call_ir in function.all_high_level_calls():
        ir_str = str(call_ir)
        if "dest:" not in ir_str:
            continue

        # Extract dest var name and function name
        dest_part = ir_str.split("dest:")[1]
        var_name = dest_part.split("(")[0].strip()
        var_type = ""
        if "(" in dest_part:
            var_type = dest_part.split("(")[1].split(")")[0]

        fn_part = ir_str.split("function:") if "function:" in ir_str else None
        if not fn_part or len(fn_part) < 2:
            continue
        called_fn = fn_part[1].split(",")[0].strip()

        direction = _TRANSFER_METHODS.get(called_fn.lower())
        if direction:
            flows.append(
                {
                    "direction": direction,
                    "token_var": var_name,
                    "token_type": var_type or None,
                    "method": called_fn,
                    "is_parameter": var_name.lower() in param_names,
                }
            )

    # Low-level calls with value: ETH transfer
    visited: set[int] = set()

    def _check_low_level(fn) -> None:
        fn_id = id(fn)
        if fn_id in visited:
            return
        visited.add(fn_id)
        for node in fn.nodes:
            for ir in node.irs:
                ir_str = str(ir)
                if "LOW_LEVEL_CALL" in ir_str and "value:" in ir_str:
                    flows.append(
                        {
                            "direction": "eth_out",
                            "token_var": None,
                            "token_type": "ETH",
                            "method": "call{value}",
                            "is_parameter": False,
                        }
                    )
                    return
        for call in _call_or_value(fn, "all_internal_calls"):
            callee = getattr(call, "function", call) if not callable(call) else call
            if hasattr(callee, "nodes"):
                _check_low_level(callee)

    _check_low_level(function)

    return flows


def _action_summary(effect_labels: list[str], effect_targets: list[str]) -> str:
    labels = set(effect_labels)

    if {"asset_pull", "mint"}.issubset(labels):
        return "Pulls assets into the contract and mints contract balances or shares."
    if {"burn", "asset_send"}.issubset(labels):
        return "Burns contract balances or shares and sends assets out of the contract."
    if "arbitrary_external_call" in labels:
        return "Executes arbitrary external calldata from the contract."
    if "external_contract_call" in labels:
        return "Calls an external contract from the contract context."
    if "authority_update" in labels:
        return "Updates the authority contract used for permission checks."
    if "ownership_transfer" in labels:
        return "Transfers contract ownership."
    if "hook_update" in labels:
        return "Updates hook configuration that can affect later contract behavior."
    if "pause_toggle" in labels:
        return "Changes the contract pause state."
    if "implementation_update" in labels:
        return "Changes implementation or upgrade control state."
    if "role_management" in labels:
        return "Changes role-based permissions."
    if "timelock_operation" in labels:
        return "Schedules, executes, or cancels timelocked operations."
    if "contract_deployment" in labels:
        return "Deploys a new contract instance."
    if "delegatecall_execution" in labels:
        return "Executes delegatecall-controlled logic."
    if "selfdestruct_capability" in labels:
        return "Can destroy the contract."
    if "asset_pull" in labels:
        return "Pulls assets into the contract."
    if "asset_send" in labels:
        return "Sends assets out of the contract."
    if "mint" in labels:
        return "Mints contract balances or shares."
    if "burn" in labels:
        return "Burns contract balances or shares."
    if effect_targets:
        return f"Writes or calls into: {', '.join(effect_targets)}."
    return "Performs a permissioned contract action."


def _detect_contract_classification(contract, project_dir: Path) -> ContractClassification:
    standards = set()
    erc_detector = getattr(contract, "ercs", None)
    if callable(erc_detector):
        erc_values = erc_detector()
        if isinstance(erc_values, (list, set, tuple)):
            standards.update(str(value) for value in erc_values)

    signatures = _contract_signatures(contract)
    events = _contract_events(contract)
    for standard, expected_signatures in STANDARD_SIGNATURES.items():
        if expected_signatures.issubset(signatures) and STANDARD_EVENTS[standard].issubset(events):
            standards.add(standard)

    factory_functions = []
    evidence = []
    for function in _entry_points(contract):
        fragment = _source_fragment(function, project_dir).lower()
        name = getattr(function, "name", "").lower()
        if not fragment and not name:
            continue
        creates_contract = bool(
            re.search(r"\bnew\s+[a-z_][a-z0-9_]*\s*\(", fragment)
            or "create2" in fragment
            or ".clone(" in fragment
            or "clonedeterministic" in fragment
        )
        if creates_contract or any(keyword in name for keyword in FACTORY_NAME_KEYWORDS):
            factory_functions.append(getattr(function, "full_name", function.name))
            evidence.append(_source_evidence(function, project_dir))

    standards_list = sorted(standards)
    return {
        "standards": standards_list,
        "is_erc20": "ERC20" in standards,
        "is_erc721": "ERC721" in standards,
        "is_erc1155": "ERC1155" in standards,
        "is_nft": "ERC721" in standards or "ERC1155" in standards,
        "is_factory": bool(factory_functions),
        "factory_functions": sorted(factory_functions),
        "evidence": evidence,
    }


def _detect_access_control(contract, project_dir: Path, permission_graph: PermissionGraph) -> AccessControlAnalysis:
    state_variables = _all_state_variables(contract)
    modifiers = _all_modifiers(contract)
    functions = _entry_points(contract)
    inheritance_names = [base.name for base in getattr(contract, "inheritance", [])]
    lower_inheritance_names = [name.lower() for name in inheritance_names]

    owner_variables = []
    admin_variables = []
    role_definitions = []
    for variable in state_variables:
        name = getattr(variable, "name", "")
        lowered = name.lower()
        if "owner" in lowered:
            owner_variables.append(name)
        if any(keyword in lowered for keyword in ADMIN_VAR_KEYWORDS):
            admin_variables.append(name)
        if any(pattern.match(name) for pattern in ROLE_NAME_PATTERNS):
            role_definitions.append(
                {
                    "role": name,
                    "declared_in": _declaring_contract_name(variable, contract.name),
                    "evidence": [_source_evidence(variable, project_dir)],
                }
            )

    if any("accesscontrol" in name for name in lower_inheritance_names):
        for inferred_role in ("DEFAULT_ADMIN_ROLE",):
            if inferred_role not in {role["role"] for role in role_definitions}:
                role_definitions.append(
                    {
                        "role": inferred_role,
                        "declared_in": contract.name,
                        "evidence": [],
                    }
                )

    privileged_functions_by_signature = privileged_functions_from_graph(contract, permission_graph)
    privileged_functions = []
    for function in functions:
        function_signature = getattr(function, "full_name", getattr(function, "name", ""))
        graph_entry = privileged_functions_by_signature.get(function_signature)
        inferred_guards = _access_control_inferred_guards(function, project_dir)
        inferred_controller_refs = _controller_refs_from_inferred_guards(inferred_guards)
        graph_guard_controller_refs = _controller_refs_from_inferred_guards(
            graph_entry["guards"] if graph_entry else []
        )
        effects = graph_entry["effects"] if graph_entry else _function_effects(function)
        effect_targets = _effect_targets(function, graph_entry, effects)
        target_controller_refs = _controller_refs_from_effect_targets(effect_targets)
        effect_labels = _effect_labels(function, effects, effect_targets, graph_entry)
        action_summary = _action_summary(effect_labels, effect_targets)
        if not _has_graph_permission_evidence(graph_entry) and not inferred_guards:
            continue

        guards = _dedupe_strings((graph_entry["guards"] if graph_entry else []) + inferred_guards)
        privileged_functions.append(
            {
                "contract": _declaring_contract_name(function, contract.name),
                "function": function_signature,
                "visibility": getattr(function, "visibility", "unknown"),
                "guards": guards,
                "guard_kinds": graph_entry["guard_kinds"] if graph_entry else [],
                "controller_refs": _dedupe_strings(
                    (graph_entry["controller_refs"] if graph_entry else [])
                    + graph_guard_controller_refs
                    + inferred_controller_refs
                    + target_controller_refs
                ),
                "sink_ids": graph_entry["sink_ids"] if graph_entry else [],
                "effects": effects,
                "effect_targets": effect_targets,
                "effect_labels": effect_labels,
                "value_flows": _extract_value_flows(function),
                "action_summary": action_summary,
            }
        )

    modifier_names = [modifier.name.lower() for modifier in modifiers]
    pattern = "unknown"
    if (
        any("accesscontrol" in name for name in lower_inheritance_names)
        or any("onlyrole" in name for name in modifier_names)
        or any(item["role"].endswith("_ROLE") for item in role_definitions)
    ):
        pattern = "access_control"
    elif any("ownable" in name for name in lower_inheritance_names) or "onlyowner" in modifier_names or owner_variables:
        pattern = "ownable"
    elif (
        any("auth" in name for name in lower_inheritance_names)
        or "auth" in modifier_names
        or any(role["role"].lower() == "wards" for role in role_definitions)
    ):
        pattern = "auth"
    elif any(any(keyword in name for keyword in ACCESS_CONTROL_INHERITANCE) for name in lower_inheritance_names):
        pattern = "governance"
    elif privileged_functions or admin_variables:
        pattern = "custom"

    return {
        "pattern": pattern,
        "owner_variables": _dedupe_strings(owner_variables),
        "admin_variables": _dedupe_strings(admin_variables),
        "role_definitions": sorted(role_definitions, key=lambda role: role["role"]),
        "privileged_functions": sorted(privileged_functions, key=lambda item: item["function"]),
        "current_holders": {
            "status": "unknown_static_only",
        },
    }


def _detect_upgradeability(contract, project_dir: Path) -> UpgradeabilityAnalysis:
    inheritance_names = [contract.name, *[base.name for base in getattr(contract, "inheritance", [])]]
    lower_names = [name.lower() for name in inheritance_names]
    function_names = {getattr(function, "name", "").lower(): function for function in _contract_functions(contract)}
    state_variables = _all_state_variables(contract)

    pattern = "none"
    if (
        any("uups" in name for name in lower_names)
        or "_authorizeupgrade" in function_names
        or "proxiableuuid" in function_names
    ):
        pattern = "uups"
    elif any("transparentupgradeableproxy" in name or "proxyadmin" in name for name in lower_names):
        pattern = "transparent"
    elif any("beacon" in name for name in lower_names):
        pattern = "beacon"
    elif getattr(contract, "is_upgradeable_proxy", False) or any(name.startswith("upgrade") for name in function_names):
        pattern = "custom"
    elif getattr(contract, "is_upgradeable", False):
        pattern = "unknown"

    implementation_slots = [
        variable.name
        for variable in state_variables
        if any(token in variable.name.lower() for token in ("implementation", "eip1967", "admin_slot", "beacon"))
    ]
    if pattern in {"uups", "transparent", "beacon"} and not implementation_slots:
        implementation_slots.append("eip1967.proxy.implementation")
    if pattern == "transparent":
        implementation_slots.append("eip1967.proxy.admin")

    admin_paths = []
    evidence = []
    for function in _contract_functions(contract):
        name = getattr(function, "name", "")
        lowered = name.lower()
        if lowered.startswith("upgrade") or lowered in {"_authorizeupgrade", "proxiableuuid"}:
            admin_paths.append(getattr(function, "full_name", name))
            evidence.append(_source_evidence(function, project_dir))

    return {
        "is_upgradeable": getattr(contract, "is_upgradeable", False) or pattern != "none",
        "is_upgradeable_proxy": getattr(contract, "is_upgradeable_proxy", False),
        "pattern": pattern,
        "upgradeable_version": getattr(contract, "upgradeable_version", None),
        "implementation_slots": _dedupe_strings(implementation_slots),
        "admin_paths": _dedupe_strings(admin_paths),
        "evidence": evidence,
    }


def _detect_pausability(contract, project_dir: Path) -> PausabilityAnalysis:
    state_variables = _all_state_variables(contract)
    modifiers = _all_modifiers(contract)
    pause_functions = []
    unpause_functions = []
    authorized_roles = []

    for function in _entry_points(contract):
        name = getattr(function, "name", "")
        lowered = name.lower()
        inferred_guards = _infer_function_guards(function, project_dir)
        if lowered == "pause" or lowered.endswith("_pause"):
            pause_functions.append(getattr(function, "full_name", name))
            authorized_roles.extend(inferred_guards)
        if lowered == "unpause" or lowered.endswith("_unpause"):
            unpause_functions.append(getattr(function, "full_name", name))
            authorized_roles.extend(inferred_guards)

    gating_modifiers = [modifier.name for modifier in modifiers if _looks_like_pause_guard(modifier.name)]
    pause_variables = [
        variable.name for variable in state_variables if variable.name.lower() in {"paused", "_paused", "live", "_live"}
    ]

    evidence = []
    for modifier in modifiers:
        if _looks_like_pause_guard(modifier.name):
            evidence.append(_source_evidence(modifier, project_dir))

    return {
        "is_pausable": bool(pause_functions or unpause_functions or gating_modifiers or pause_variables),
        "pause_functions": sorted(pause_functions),
        "unpause_functions": sorted(unpause_functions),
        "gating_modifiers": sorted(gating_modifiers),
        "pause_variables": sorted(pause_variables),
        "authorized_roles": _dedupe_strings(authorized_roles),
        "evidence": evidence,
    }


def _detect_timelock(contract, project_dir: Path, role_definitions: list[RoleDefinition]) -> TimelockAnalysis:
    inheritance_names = [contract.name, *[base.name for base in getattr(contract, "inheritance", [])]]
    lower_names = [name.lower() for name in inheritance_names]
    delay_variables = []
    queue_execute_functions = []
    authorized_roles = []
    evidence = []

    for variable in _all_state_variables(contract):
        lowered = variable.name.lower()
        if "delay" in lowered or "timelock" in lowered or lowered == "eta":
            delay_variables.append(variable.name)
            evidence.append(_source_evidence(variable, project_dir))

    for function in _entry_points(contract):
        lowered = getattr(function, "name", "").lower()
        if any(keyword in lowered for keyword in ("schedule", "queue", "execute", "cancel")):
            queue_execute_functions.append(getattr(function, "full_name", function.name))
            authorized_roles.extend(_infer_function_guards(function, project_dir))
            evidence.append(_source_evidence(function, project_dir))

    role_names = {role["role"] for role in role_definitions}
    if any("timelockcontroller" in name for name in lower_names):
        pattern = "oz_timelock"
    elif any("timelock" in name and "governor" in name for name in lower_names):
        pattern = "governor_timelock"
    elif delay_variables or queue_execute_functions or any("timelock" in role_name.lower() for role_name in role_names):
        pattern = "custom"
    else:
        pattern = "none"

    return {
        "has_timelock": pattern != "none",
        "pattern": pattern,
        "delay_variables": _dedupe_strings(delay_variables),
        "queue_execute_functions": sorted(queue_execute_functions),
        "authorized_roles": _dedupe_strings(authorized_roles),
        "evidence": evidence,
    }


def _summarize_slither(slither_output: dict) -> SlitherSummary:
    detectors = slither_output.get("results", {}).get("detectors", [])
    counts = {impact: 0 for impact in SEVERITY_ORDER}
    for detector in detectors:
        impact = detector.get("impact", "Informational")
        counts.setdefault(impact, 0)
        counts[impact] += 1

    key_findings: list[SlitherFinding] = []
    for detector in sorted(detectors, key=lambda item: SEVERITY_ORDER.get(item.get("impact", ""), 99))[:10]:
        description = str(detector.get("description", "")).strip().split("\n")[0]
        key_findings.append(
            {
                "check": detector.get("check", "unknown"),
                "impact": detector.get("impact", "Unknown"),
                "confidence": detector.get("confidence", "Unknown"),
                "description": description,
            }
        )

    return {
        "detector_counts": counts,
        "key_findings": key_findings,
    }


def _derive_static_risk_level(detector_counts: dict[str, int]) -> RiskLevel:
    if detector_counts.get("High", 0) > 0:
        return "high"
    if detector_counts.get("Medium", 0) > 0:
        return "medium"
    if sum(detector_counts.values()) > 0:
        return "low"
    return "unknown"


def _determine_control_model(
    contract, access_control: AccessControlAnalysis, timelock: TimelockAnalysis
) -> ControlModel:
    lower_names = [base.name.lower() for base in getattr(contract, "inheritance", [])]
    if timelock["has_timelock"] or any("governor" in name for name in lower_names):
        return "governance"
    return access_control["pattern"]


def _build_tracking_hints(
    access_control: AccessControlAnalysis,
    upgradeability: UpgradeabilityAnalysis,
    pausability: PausabilityAnalysis,
    timelock: TimelockAnalysis,
) -> list[TrackingHint]:
    hints: list[TrackingHint] = []
    for owner_variable in access_control["owner_variables"]:
        hints.append({"kind": "owner_variable", "label": owner_variable, "source": owner_variable})
    for admin_variable in access_control["admin_variables"]:
        hints.append({"kind": "admin_variable", "label": admin_variable, "source": admin_variable})
    for role in access_control["role_definitions"]:
        hints.append({"kind": "role", "label": role["role"], "source": role["role"]})
    for pause_variable in pausability["pause_variables"]:
        hints.append({"kind": "pause_flag", "label": pause_variable, "source": pause_variable})
    for slot in upgradeability["implementation_slots"]:
        hints.append({"kind": "proxy_slot", "label": slot, "source": slot})
    for delay_variable in timelock["delay_variables"]:
        hints.append({"kind": "timelock_delay", "label": delay_variable, "source": delay_variable})

    seen = set()
    deduped = []
    for hint in hints:
        key = (hint["kind"], hint["label"], hint["source"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(hint)
    return deduped
