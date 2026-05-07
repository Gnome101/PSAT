"""Summary and compatibility views for contract analysis."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from schemas.contract_analysis import (
    AccessControlAnalysis,
    ContractClassification,
    ControlModel,
    PausabilityAnalysis,
    RiskLevel,
    RoleDefinition,
    SlitherFinding,
    SlitherSummary,
    TimelockAnalysis,
    TrackingHint,
    UpgradeabilityAnalysis,
)

from .constants import (
    FACTORY_NAME_KEYWORDS,
    SEVERITY_ORDER,
    STANDARD_EVENTS,
    STANDARD_SIGNATURES,
)
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
    _source_evidence,
    _source_fragment,
)


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


_SENSITIVE_SINK_KINDS = frozenset({"state_write", "external_call", "delegatecall", "contract_creation", "selfdestruct"})


def _tree_has_caller_or_delegated_authority(tree: dict | None) -> bool:
    """True iff some leaf in ``tree`` carries
    ``authority_role IN {caller_authority, delegated_authority}``.
    The structural inclusion gate for v2 ``privileged_functions``
    (replaces the v1 effects+guards heuristic which over-included
    side-condition trees of only time/reentrancy/pause/business roles)."""
    if not isinstance(tree, dict):
        return False
    if tree.get("op") == "LEAF":
        leaf = tree.get("leaf") or {}
        return leaf.get("authority_role") in ("caller_authority", "delegated_authority")
    for child in tree.get("children") or []:
        if _tree_has_caller_or_delegated_authority(child):
            return True
    return False


def _function_has_sensitive_sink(effect_info: dict | None) -> bool:
    if not isinstance(effect_info, dict):
        return False
    for sink in effect_info.get("sinks") or []:
        if isinstance(sink, dict) and sink.get("kind") in _SENSITIVE_SINK_KINDS:
            return True
    return False


_OZ_ROLE_GRANTED_TOPIC0 = "0x2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d"


def _is_oz_role_descriptor(descriptor: Mapping[str, Any]) -> bool:
    if descriptor.get("kind") != "mapping_membership":
        return False
    return any(
        isinstance(hint, dict) and hint.get("topic0") == _OZ_ROLE_GRANTED_TOPIC0
        for hint in descriptor.get("enumeration_hint") or []
    )


def _role_key_names_from_descriptor(descriptor: Mapping[str, Any]) -> set[str]:
    if not _is_oz_role_descriptor(descriptor):
        return set()
    names: set[str] = set()
    for key_source in descriptor.get("key_sources") or []:
        if not isinstance(key_source, dict):
            continue
        if key_source.get("source") == "state_variable":
            name = key_source.get("state_variable_name")
        elif key_source.get("source") == "external_call":
            name = key_source.get("callee")
        else:
            name = None
        if isinstance(name, str) and name:
            names.add(name)
    return names


def _role_names_from_tree(tree: dict | None, state_vars_by_name: Mapping[str, Any] | None = None) -> set[str]:
    if not isinstance(tree, dict):
        return set()
    roles: set[str] = set()

    def visit(node: Any) -> None:
        if not isinstance(node, dict):
            return
        if node.get("op") == "LEAF":
            leaf = node.get("leaf") or {}
            if not isinstance(leaf, dict):
                return
            descriptor = leaf.get("set_descriptor") or {}
            if isinstance(descriptor, dict):
                roles.update(_role_key_names_from_descriptor(descriptor))
            if leaf.get("authority_role") in {"caller_authority", "delegated_authority"}:
                for operand in leaf.get("operands") or []:
                    if not isinstance(operand, dict) or operand.get("source") != "state_variable":
                        continue
                    name = operand.get("state_variable_name")
                    if (
                        isinstance(name, str)
                        and state_vars_by_name is not None
                        and _is_bytes32_constant(state_vars_by_name.get(name))
                    ):
                        roles.add(name)
            return
        for child in node.get("children") or []:
            visit(child)

    visit(tree)
    return roles


def _role_names_from_predicate_trees(
    predicate_trees: Mapping[str, Any] | None,
    state_vars_by_name: Mapping[str, Any] | None = None,
) -> set[str]:
    if not isinstance(predicate_trees, dict):
        return set()
    trees = predicate_trees.get("trees")
    if not isinstance(trees, dict):
        return set()
    roles: set[str] = set()
    for tree in trees.values():
        roles.update(_role_names_from_tree(tree, state_vars_by_name))
    return roles


def _is_bytes32_constant(variable: Any) -> bool:
    return (
        variable is not None
        and str(getattr(variable, "type", "")) == "bytes32"
        and bool(getattr(variable, "is_constant", False))
    )


def _caller_equality_state_vars_from_tree(tree: dict | None) -> set[str]:
    if not isinstance(tree, dict):
        return set()
    out: set[str] = set()

    def visit(node: Any) -> None:
        if not isinstance(node, dict):
            return
        if node.get("op") == "LEAF":
            leaf = node.get("leaf") or {}
            if not isinstance(leaf, dict):
                return
            if leaf.get("kind") != "equality" or leaf.get("authority_role") != "caller_authority":
                return
            operands = [op for op in leaf.get("operands") or [] if isinstance(op, dict)]
            has_caller = any(op.get("source") in {"msg_sender", "tx_origin", "signature_recovery"} for op in operands)
            if not has_caller:
                return
            for operand in operands:
                if operand.get("source") == "state_variable":
                    name = operand.get("state_variable_name")
                    if isinstance(name, str) and name:
                        out.add(name)
            return
        for child in node.get("children") or []:
            visit(child)

    visit(tree)
    return out


def _authority_roles_from_tree(tree: dict | None) -> set[str]:
    if not isinstance(tree, dict):
        return set()
    roles: set[str] = set()

    def visit(node: Any) -> None:
        if not isinstance(node, dict):
            return
        if node.get("op") == "LEAF":
            leaf = node.get("leaf") or {}
            if isinstance(leaf, dict):
                role = leaf.get("authority_role")
                if isinstance(role, str) and role:
                    roles.add(role)
            return
        for child in node.get("children") or []:
            visit(child)

    visit(tree)
    return roles


def _controller_refs_from_tree(tree: dict | None) -> list[str]:
    """Walk a predicate_tree and return the unique state-variable / role
    operand names referenced by any leaf. Used to populate the legacy
    ``privileged_functions[*].controller_refs`` field from v2 data.

    Also surfaces external-call callees only when they appear as a key source
    on an OZ-style ``mapping_membership`` descriptor.
    """
    if not isinstance(tree, dict):
        return []
    refs: list[str] = []
    seen: set[str] = set()

    def add(name: str | None) -> None:
        if isinstance(name, str) and name and name not in seen:
            seen.add(name)
            refs.append(name)

    def visit(node):
        if not isinstance(node, dict):
            return
        if node.get("op") == "LEAF":
            leaf = node.get("leaf") or {}
            for operand in leaf.get("operands") or []:
                if not isinstance(operand, dict):
                    continue
                if operand.get("source") == "state_variable":
                    add(operand.get("state_variable_name"))
            descriptor = leaf.get("set_descriptor") or {}
            if isinstance(descriptor, dict):
                authority = descriptor.get("authority_contract") or {}
                if isinstance(authority, dict):
                    address_source = authority.get("address_source") or {}
                    if isinstance(address_source, dict) and address_source.get("source") == "state_variable":
                        add(address_source.get("state_variable_name"))
                for key_source in descriptor.get("key_sources") or []:
                    if not isinstance(key_source, dict):
                        continue
                    if key_source.get("source") == "state_variable":
                        add(key_source.get("state_variable_name"))
                    elif key_source.get("source") == "external_call":
                        callee = key_source.get("callee")
                        if not isinstance(callee, str):
                            continue
                        if callee in _role_key_names_from_descriptor(descriptor):
                            add(callee)
            return
        for child in node.get("children") or []:
            visit(child)

    visit(tree)
    return refs


def _sink_ids_from_effect_info(effect_info: dict | None) -> list[str]:
    """Carry sink IDs through from the v2 effects record so the legacy
    ``privileged_functions[*].sink_ids`` field stays populated."""
    if not isinstance(effect_info, dict):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for sink in effect_info.get("sinks") or []:
        if not isinstance(sink, dict):
            continue
        sid = sink.get("id")
        if isinstance(sid, str) and sid and sid not in seen:
            seen.add(sid)
            out.append(sid)
    return out


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


def _is_address_like_state_var(var) -> bool:
    """Address or contract/interface-typed state var."""
    type_str = str(getattr(var, "type", ""))
    if "address" in type_str.lower():
        return True
    # User-defined contract/interface types start uppercase (e.g. "IHook", "AuthorityLike").
    return bool(type_str) and type_str[0].isupper()


def _writes_unclassified_address_pointer(function) -> bool:
    """Bare setter that writes a single address/contract state var with no other effects
    and no owner/authority/pause/impl role — e.g. ``function setHook(address h) { hook = h; }``."""
    if _writes_owner_like_address(function):
        return False
    if _writes_authority_reference(function):
        return False
    if _writes_pause_like_bool(function):
        return False
    if _writes_delegatecall_target(function):
        return False
    if _writes_assembly_delegatecall_slot(function):
        return False
    written = list(function.all_state_variables_written())
    if len(written) != 1 or not _is_address_like_state_var(written[0]):
        return False
    # Modifier auth calls don't count as effects; only inspect the body.
    for node in function.nodes:
        for ir in node.irs:
            op = type(ir).__name__
            if op in ("HighLevelCall", "LowLevelCall", "LibraryCall"):
                return False
    return True


def _detect_supply_change_pattern(function) -> str | None:
    """Mint/burn via pre/post totalSupply: X.totalSupply() twice around a non-totalSupply call
    on X, compared with > (mint) or < (burn). Catches randomized method names."""

    def _extract_dest(ir_str: str) -> str | None:
        idx = ir_str.find("dest:")
        if idx < 0:
            return None
        rest = ir_str[idx + 5 :]
        paren = rest.find("(")
        return rest[:paren] if paren > 0 else None

    total_supply_dests: list[str] = []
    other_dests: set[str] = set()
    has_greater = False
    has_less = False
    for node in function.nodes:
        for ir in node.irs:
            op = type(ir).__name__
            ir_str = str(ir)
            if op == "HighLevelCall":
                dest = _extract_dest(ir_str)
                if not dest:
                    continue
                if "function:totalSupply" in ir_str:
                    total_supply_dests.append(dest)
                else:
                    other_dests.add(dest)
            elif op == "Binary":
                if " > " in ir_str:
                    has_greater = True
                elif " < " in ir_str:
                    has_less = True

    from collections import Counter

    for receiver, count in Counter(total_supply_dests).items():
        if count >= 2 and receiver in other_dests:
            if has_greater:
                return "mint"
            if has_less:
                return "burn"
    return None


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

    supply_change = _detect_supply_change_pattern(function)
    if supply_change:
        labels.add(supply_change)

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

    # Fallback: fires only if no more specific label matched.
    if not labels and _writes_unclassified_address_pointer(function):
        labels.add("hook_update")

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


def _build_method_to_role_map(
    predicate_trees: Mapping[str, Any] | None,
    state_vars_by_name: Mapping[str, Any] | None = None,
) -> dict[str, list[str]]:
    """Map authority methods to role identifiers from predicate trees.

    This replaces the previous call/name scan. A method maps to a role only
    when its predicate tree carries an OZ role-key descriptor or a bytes32
    constant used in an authority leaf.
    """
    result: dict[str, list[str]] = {}
    if not isinstance(predicate_trees, dict):
        return result
    trees = predicate_trees.get("trees")
    if not isinstance(trees, dict):
        return result
    for signature, tree in trees.items():
        if not isinstance(signature, str):
            continue
        roles = _role_names_from_tree(tree, state_vars_by_name)
        if roles:
            name = signature.split("(", 1)[0]
            if name:
                result[name] = sorted(set(roles))
    return result


def _detect_access_control(
    contract,
    project_dir: Path,
    predicate_trees: Mapping[str, Any] | None,
    effects: Mapping[str, Any] | None,
) -> AccessControlAnalysis:
    """Build the access-control analysis from v2 sources only.

    Privileged-function inclusion is structural: a function is privileged
    iff EITHER its predicate tree contains a leaf with
    ``authority_role IN {caller_authority, delegated_authority}`` OR
    its effects record carries a sensitive sink (state_write,
    external_call, delegatecall, contract_creation, selfdestruct).

    Role definitions come from role keys observed in predicate-tree leaves.
    """
    state_variables = _all_state_variables(contract)
    state_vars_by_name = {getattr(variable, "name", ""): variable for variable in state_variables}
    functions = _entry_points(contract)
    v2_trees = (predicate_trees or {}).get("trees") or {}
    effects_functions = (effects or {}).get("functions") or {}

    owner_variables = sorted(
        {
            name
            for tree in v2_trees.values()
            for name in _caller_equality_state_vars_from_tree(tree if isinstance(tree, dict) else None)
        }
    )
    admin_variables: list[str] = []
    role_definitions = []
    for name in sorted(_role_names_from_predicate_trees(predicate_trees, state_vars_by_name)):
        variable = state_vars_by_name.get(name)
        if variable is not None:
            role_definitions.append(
                {
                    "role": name,
                    "declared_in": _declaring_contract_name(variable, contract.name),
                    "evidence": [_source_evidence(variable, project_dir)],
                }
            )
        else:
            role_definitions.append({"role": name, "declared_in": contract.name, "evidence": []})

    privileged_functions = []
    for function in functions:
        function_signature = getattr(function, "full_name", getattr(function, "name", ""))
        tree = v2_trees.get(function_signature)
        effect_info = effects_functions.get(function_signature)

        has_caller_authority_leaf = _tree_has_caller_or_delegated_authority(tree)
        has_sensitive_sink = _function_has_sensitive_sink(effect_info)
        # Structural inclusion gate (codex's correction): caller/delegated
        # authority leaf OR sensitive effect. Tree-keys-as-privileged is
        # gone — pause/reentrancy/business/time-only trees no longer
        # admit a function into privileged_functions.
        if not (has_caller_authority_leaf or has_sensitive_sink):
            continue

        # Source effect/effect_target/effect_label/action_summary from the
        # per-function effects record. If the effects artifact is missing,
        # leave these compatibility fields empty rather than reintroducing
        # a separate heuristic path.
        if isinstance(effect_info, dict):
            effects_list = list(effect_info.get("effects") or [])
            effect_targets = list(effect_info.get("effect_targets") or [])
            effect_labels = list(effect_info.get("effect_labels") or [])
            action_summary = effect_info.get("action_summary") or _action_summary(effect_labels, effect_targets)
        else:
            effects_list = []
            effect_targets = []
            effect_labels = []
            action_summary = _action_summary(effect_labels, effect_targets)

        target_controller_refs = _controller_refs_from_effect_targets(effect_targets)
        # Schema-v2 cutover: aux fields (guards / guard_kinds / sink_ids /
        # controller_refs) used to come from the v1 permission graph. The
        # v2 path no longer reads it — these fields are populated from the
        # predicate-tree leaves where possible. Phase A.6 deletes them.
        leaf_controller_refs = _controller_refs_from_tree(tree) if isinstance(tree, dict) else []
        sink_ids = _sink_ids_from_effect_info(effect_info)

        entry: dict = {
            "contract": _declaring_contract_name(function, contract.name),
            "function": function_signature,
            "visibility": getattr(function, "visibility", "unknown"),
            "guards": [],
            "guard_kinds": [],
            "controller_refs": _dedupe_strings(leaf_controller_refs + target_controller_refs),
            "sink_ids": sink_ids,
            "effects": effects_list,
            "effect_targets": effect_targets,
            "effect_labels": effect_labels,
            "value_flows": _extract_value_flows(function),
            "action_summary": action_summary,
        }
        privileged_functions.append(entry)

    authority_roles = {
        role
        for tree in v2_trees.values()
        for role in _authority_roles_from_tree(tree if isinstance(tree, dict) else None)
    }
    has_oz_role_descriptor = bool(_role_names_from_predicate_trees(predicate_trees, state_vars_by_name))
    pattern = "unknown"
    if has_oz_role_descriptor or "delegated_authority" in authority_roles:
        pattern = "access_control"
    elif owner_variables:
        pattern = "ownable"
    elif privileged_functions:
        pattern = "custom"

    result: AccessControlAnalysis = {
        "pattern": pattern,
        "owner_variables": _dedupe_strings(owner_variables),
        "admin_variables": _dedupe_strings(admin_variables),
        "role_definitions": sorted(role_definitions, key=lambda role: role["role"]),
        "privileged_functions": sorted(privileged_functions, key=lambda item: item["function"]),
        "current_holders": {
            "status": "unknown_static_only",
        },
    }
    if pattern in ("access_control", "governance") or role_definitions:
        method_to_role = _build_method_to_role_map(predicate_trees, state_vars_by_name)
        if method_to_role:
            result["method_to_role"] = method_to_role
    from .mapping_events import discover_mapping_writer_events

    mapping_writer_events = discover_mapping_writer_events(contract)
    if mapping_writer_events:
        result["mapping_writer_events"] = [dict(spec) for spec in mapping_writer_events]
    return result


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


def _detect_pausability(
    contract,
    project_dir: Path,
    pause_info: Mapping[str, Any] | None = None,
) -> PausabilityAnalysis:
    """Detect pausability structurally from the v2 ``PauseInfo`` export.

    ``pause_info`` (returned by ``apply_reentrancy_pause_pass``) carries
    the structural pause-state-var set and toggle-function list — the
    modifier-name heuristic ``_looks_like_pause_guard`` is gone.

    Modifiers that read a structural pause var are surfaced as
    ``gating_modifiers``. ``pause_functions`` / ``unpause_functions``
    are derived from the toggle list by inspecting which value the
    function writes (true = pause, false = unpause); when the structural
    classification can't disambiguate, every toggle function is listed in
    both pause and unpause.
    """
    info = pause_info or {}
    pause_state_vars: list[str] = list(info.get("pause_state_vars") or [])
    toggle_functions: list[str] = list(info.get("pause_toggle_functions") or [])

    pause_var_set = set(pause_state_vars)
    pause_functions: set[str] = set()
    unpause_functions: set[str] = set()

    if pause_var_set:
        functions_by_full = {}
        for fn in getattr(contract, "functions", []) or []:
            full = getattr(fn, "full_name", None) or getattr(fn, "name", None)
            if isinstance(full, str):
                functions_by_full[full] = fn

        for full_name in toggle_functions:
            fn = functions_by_full.get(full_name)
            if fn is None:
                pause_functions.add(full_name)
                continue
            polarity = _classify_pause_toggle_polarity(fn, pause_var_set)
            if polarity == "pause":
                pause_functions.add(full_name)
            elif polarity == "unpause":
                unpause_functions.add(full_name)
            else:
                # Ambiguous polarity (parameter-driven setPaused(bool)
                # or branched writes): surface as both.
                pause_functions.add(full_name)
                unpause_functions.add(full_name)

    modifiers = _all_modifiers(contract)
    gating_modifiers: list[str] = []
    evidence = []
    if pause_var_set:
        for modifier in modifiers:
            read_names = {getattr(v, "name", "") for v in getattr(modifier, "state_variables_read", []) or []}
            if read_names & pause_var_set:
                gating_modifiers.append(modifier.name)
                evidence.append(_source_evidence(modifier, project_dir))

    return {
        "is_pausable": bool(pause_functions or unpause_functions or gating_modifiers or pause_state_vars),
        "pause_functions": sorted(pause_functions),
        "unpause_functions": sorted(unpause_functions),
        "gating_modifiers": sorted(gating_modifiers),
        "pause_variables": sorted(pause_state_vars),
        "authorized_roles": [],
        "evidence": evidence,
    }


def _classify_pause_toggle_polarity(function, pause_vars: set[str]) -> str:
    """Return ``"pause"`` if ``function`` writes one of ``pause_vars``
    with a true-ish constant, ``"unpause"`` if false-ish, or ``""`` if
    the polarity can't be determined statically.

    Walks IR Assignment ops for ``var = <const>`` shapes; anything else
    (param write, cross-branch toggle, derived value) returns ambiguous."""
    polarities: set[str] = set()
    for node in getattr(function, "nodes", []) or []:
        for ir in getattr(node, "irs", []) or []:
            op = type(ir).__name__
            if op != "Assignment":
                continue
            lvalue = getattr(ir, "lvalue", None)
            target = getattr(lvalue, "name", None)
            if isinstance(target, str):
                # Strip Slither SSA suffix.
                parts = target.rsplit("_", 1)
                if len(parts) == 2 and parts[1].isdigit():
                    target = parts[0]
            if target not in pause_vars:
                continue
            rvalue = getattr(ir, "rvalue", None)
            rtext = getattr(rvalue, "name", None) or getattr(rvalue, "value", None) or str(rvalue or "")
            rtext_lower = str(rtext).strip().lower()
            if rtext_lower in ("true", "1"):
                polarities.add("pause")
            elif rtext_lower in ("false", "0"):
                polarities.add("unpause")
    if polarities == {"pause"}:
        return "pause"
    if polarities == {"unpause"}:
        return "unpause"
    return ""


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
