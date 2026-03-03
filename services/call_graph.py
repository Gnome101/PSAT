"""Export dynamic dependency data into Mermaid, Graphviz, and HTML explorers."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

SPECIAL_FUNCTION_NAMES = {"constructor", "fallback", "receive"}


def _norm_address(address: str) -> str:
    return "0x" + address.lower().replace("0x", "", 1)


def _normalize_selector(selector: Any) -> str:
    if not isinstance(selector, str):
        return "0x"
    raw = selector.lower().strip()
    if raw.startswith("0x"):
        raw = raw[2:]
    if len(raw) < 8 or not re.fullmatch(r"[0-9a-f]+", raw):
        return "0x"
    return "0x" + raw[:8]


def _short_label(address: str) -> str:
    normalized = _norm_address(address)
    return f"{normalized[:8]}...{normalized[-6:]}"


def _node_id(address: str) -> str:
    normalized = _norm_address(address)
    return "n_" + normalized[2:]


def _label_escape(label: str) -> str:
    # Mermaid and DOT both tolerate plain ASCII labels; escape quotes for safety.
    return label.replace('"', '\\"')


def _walk_nodes(node: Any):
    stack = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            yield current
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            for value in current:
                if isinstance(value, (dict, list)):
                    stack.append(value)


def _split_top_level_csv(csv_text: str) -> list[str]:
    if not csv_text:
        return []

    parts: list[str] = []
    depth = 0
    start = 0

    for idx, ch in enumerate(csv_text):
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == "," and depth == 0:
            piece = csv_text[start:idx].strip()
            if piece:
                parts.append(piece)
            start = idx + 1

    tail = csv_text[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _signature_param_count(signature: str) -> int:
    if "(" not in signature or ")" not in signature:
        return 0
    params_text = signature.split("(", 1)[1].rsplit(")", 1)[0]
    return len(_split_top_level_csv(params_text))


def _canonical_param_type(type_string: str) -> str:
    cleaned = " ".join(type_string.split())
    for token in (" memory", " storage", " calldata", " storage pointer", " storage ref"):
        cleaned = cleaned.replace(token, "")
    cleaned = cleaned.replace(" payable", "")
    cleaned = re.sub(r"^contract\\s+", "", cleaned)
    cleaned = re.sub(r"^struct\\s+", "", cleaned)
    cleaned = re.sub(r"^enum\\s+", "", cleaned)
    return cleaned


def _collect_graph_model(payload: dict) -> tuple[str, list[str], list[dict]]:
    root = _norm_address(payload["address"])
    edge_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    seen_nodes: set[str] = {root}

    graph_entries = payload.get("dependency_graph")
    if isinstance(graph_entries, list) and graph_entries:
        for entry in graph_entries:
            src = entry.get("from")
            dst = entry.get("to")
            op = str(entry.get("op", "CALL")).upper()
            if not src or not dst:
                continue

            src = _norm_address(src)
            dst = _norm_address(dst)
            seen_nodes.add(src)
            seen_nodes.add(dst)
            provenance = entry.get("provenance")
            count = len(provenance) if isinstance(provenance, list) and provenance else 1
            key = (src, dst, op)
            if key not in edge_map:
                edge_map[key] = {
                    "from": src,
                    "to": dst,
                    "op": op,
                    "count": 0,
                    "tx_hashes": set(),
                }
            edge_map[key]["count"] += count
            if isinstance(provenance, list):
                for record in provenance:
                    tx_hash = record.get("tx_hash")
                    if isinstance(tx_hash, str) and tx_hash:
                        edge_map[key]["tx_hashes"].add(tx_hash)

    else:
        # Fallback path if dependency_graph is absent.
        provenance = payload.get("provenance", {})
        if isinstance(provenance, dict):
            for dep, records in provenance.items():
                if not isinstance(records, list):
                    continue
                for record in records:
                    src = record.get("from")
                    if not src:
                        continue
                    op = str(record.get("op", "CALL")).upper()
                    src = _norm_address(src)
                    dst = _norm_address(dep)
                    seen_nodes.add(src)
                    seen_nodes.add(dst)
                    key = (src, dst, op)
                    if key not in edge_map:
                        edge_map[key] = {
                            "from": src,
                            "to": dst,
                            "op": op,
                            "count": 0,
                            "tx_hashes": set(),
                        }
                    edge_map[key]["count"] += 1
                    tx_hash = record.get("tx_hash")
                    if isinstance(tx_hash, str) and tx_hash:
                        edge_map[key]["tx_hashes"].add(tx_hash)

    dependencies = payload.get("dependencies")
    if isinstance(dependencies, list):
        for dep in dependencies:
            if isinstance(dep, str):
                seen_nodes.add(_norm_address(dep))

    nodes = sorted(seen_nodes)
    edges = []
    for key in sorted(edge_map):
        entry = edge_map[key]
        edges.append(
            {
                "from": entry["from"],
                "to": entry["to"],
                "op": entry["op"],
                "count": entry["count"],
                "tx_hashes": sorted(entry["tx_hashes"]),
            }
        )

    return root, nodes, edges


def _render_mermaid(root: str, nodes: list[str], edges: list[dict], title: str) -> str:
    lines = []
    lines.append(f"%% {title}")
    lines.append("flowchart LR")
    lines.append('  classDef target fill:#fef3c7,stroke:#92400e,stroke-width:2px;')

    for address in nodes:
        node_name = _node_id(address)
        lines.append(f'  {node_name}["{_label_escape(_short_label(address))}"]')

    for edge in edges:
        edge_label = f"{edge['op']} x{edge['count']}"
        lines.append(
            f"  {_node_id(edge['from'])} -->|{_label_escape(edge_label)}| {_node_id(edge['to'])}"
        )

    lines.append(f"  class {_node_id(root)} target;")
    return "\n".join(lines) + "\n"


def _render_dot(root: str, nodes: list[str], edges: list[dict], title: str) -> str:
    lines = []
    lines.append("digraph G {")
    lines.append('  rankdir=LR;')
    lines.append(f'  label="{_label_escape(title)}";')
    lines.append('  labelloc="t";')
    lines.append('  node [shape=box, style="rounded"];')

    for address in nodes:
        attrs = [f'label="{_label_escape(_short_label(address))}"']
        if address == root:
            attrs.append('style="rounded,filled"')
            attrs.append('fillcolor="#fef3c7"')
            attrs.append('color="#92400e"')
            attrs.append("penwidth=2")
        lines.append(f'  {_node_id(address)} [{", ".join(attrs)}];')

    for edge in edges:
        edge_label = f"{edge['op']} x{edge['count']}"
        lines.append(
            f'  {_node_id(edge["from"])} -> {_node_id(edge["to"])} [label="{_label_escape(edge_label)}"];'
        )

    lines.append("}")
    return "\n".join(lines) + "\n"


def _load_json(path: Path) -> dict | None:
    try:
        raw = path.read_text()
    except OSError:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _find_contract_artifact(project_dir: Path, contract_name: str | None) -> Path | None:
    out_dir = project_dir / "out"
    if not out_dir.exists():
        return None

    if contract_name:
        exact = [p for p in out_dir.rglob(f"{contract_name}.json") if "build-info" not in p.parts]
        if exact:
            exact.sort(key=lambda p: (len(p.parts), str(p)))
            return exact[0]

    # Fallback: pick an artifact with AST + methodIdentifiers and avoid build-info blobs.
    candidates = []
    for path in out_dir.rglob("*.json"):
        if "build-info" in path.parts:
            continue
        data = _load_json(path)
        if data and "ast" in data and "methodIdentifiers" in data:
            candidates.append(path)

    if not candidates:
        return None

    candidates.sort(key=lambda p: (len(p.parts), str(p)))
    return candidates[0]


def _function_display_name(function_node: dict, declaring_contract: str) -> str:
    kind = function_node.get("kind")
    raw_name = str(function_node.get("name") or "").strip()

    if kind == "constructor":
        return "constructor"
    if kind == "fallback":
        return "fallback"
    if kind == "receive":
        return "receive"

    # Old compiler versions encode constructors as function named after the contract.
    if raw_name and raw_name == declaring_contract:
        return "constructor"

    return raw_name or "(anonymous)"


def _function_param_types(function_node: dict) -> list[str]:
    params_obj = function_node.get("parameters")
    params = params_obj.get("parameters", []) if isinstance(params_obj, dict) else []

    out = []
    for param in params:
        if not isinstance(param, dict):
            continue
        type_desc = param.get("typeDescriptions")
        type_string = None
        if isinstance(type_desc, dict):
            type_string = type_desc.get("typeString")
        if not isinstance(type_string, str) or not type_string:
            type_name = param.get("typeName")
            if isinstance(type_name, dict):
                type_string = type_name.get("name") or type_name.get("nodeType")
        if not isinstance(type_string, str):
            type_string = "unknown"
        out.append(_canonical_param_type(type_string))
    return out


def _collect_state_refs(node: Any, state_var_ids: set[int]) -> set[int]:
    refs: set[int] = set()
    if node is None:
        return refs

    for child in _walk_nodes(node):
        if child.get("nodeType") in {"Identifier", "MemberAccess"}:
            ref = child.get("referencedDeclaration")
            if isinstance(ref, int) and ref in state_var_ids:
                refs.add(ref)
    return refs


def _collect_state_access(function_body: Any, state_var_ids: set[int]) -> tuple[set[int], set[int]]:
    if not isinstance(function_body, dict):
        return set(), set()

    writes: set[int] = set()
    reads: set[int] = set()

    for node in _walk_nodes(function_body):
        node_type = node.get("nodeType")

        if node_type == "Assignment":
            lhs = node.get("leftHandSide")
            rhs = node.get("rightHandSide")
            lhs_refs = _collect_state_refs(lhs, state_var_ids)
            writes |= lhs_refs
            if node.get("operator") != "=":
                reads |= lhs_refs
            reads |= _collect_state_refs(rhs, state_var_ids)

        elif node_type == "UnaryOperation":
            operator = node.get("operator")
            if operator in {"++", "--", "delete"}:
                refs = _collect_state_refs(node.get("subExpression"), state_var_ids)
                writes |= refs
                reads |= refs

    reads |= _collect_state_refs(function_body, state_var_ids)
    return writes, reads


def _build_contract_intelligence(project_dir: Path, payload: dict, root: str) -> dict:
    meta = _load_json(project_dir / "contract_meta.json") or {}
    contract_name = meta.get("contract_name") if isinstance(meta.get("contract_name"), str) else None

    artifact_path = _find_contract_artifact(project_dir, contract_name)
    if artifact_path is None:
        return {
            "available": False,
            "warning": "No compiled artifact with AST was found under out/. Run Slither/compile first.",
            "contract_name": contract_name or "Unknown",
            "state_variables": [],
            "functions": [],
        }

    artifact = _load_json(artifact_path)
    if not artifact:
        return {
            "available": False,
            "warning": f"Failed to parse artifact: {artifact_path}",
            "contract_name": contract_name or "Unknown",
            "state_variables": [],
            "functions": [],
        }

    ast = artifact.get("ast")
    if not isinstance(ast, dict):
        return {
            "available": False,
            "warning": f"Artifact has no AST: {artifact_path}",
            "contract_name": contract_name or "Unknown",
            "state_variables": [],
            "functions": [],
        }

    contract_defs: dict[int, dict] = {}
    for node in _walk_nodes(ast):
        if node.get("nodeType") == "ContractDefinition" and isinstance(node.get("id"), int):
            contract_defs[node["id"]] = node

    if not contract_defs:
        return {
            "available": False,
            "warning": "AST did not contain any contract definitions.",
            "contract_name": contract_name or "Unknown",
            "state_variables": [],
            "functions": [],
        }

    warning = ""
    target_contract = None
    if contract_name:
        target_contract = next(
            (contract for contract in contract_defs.values() if contract.get("name") == contract_name),
            None,
        )

    if target_contract is None:
        fallback_name = artifact_path.stem
        target_contract = next(
            (contract for contract in contract_defs.values() if contract.get("name") == fallback_name),
            None,
        )

    if target_contract is None:
        target_contract = next(iter(contract_defs.values()))
        warning = (
            f"Configured contract '{contract_name}' was not found in AST; "
            f"using '{target_contract.get('name', 'Unknown')}'."
        )

    target_name = str(target_contract.get("name") or (contract_name or "Unknown"))

    lineage_ids = target_contract.get("linearizedBaseContracts")
    if not isinstance(lineage_ids, list) or not lineage_ids:
        lineage_ids = [target_contract.get("id")]

    lineage_contracts = [
        contract_defs[cid]
        for cid in lineage_ids
        if isinstance(cid, int) and cid in contract_defs
    ]
    if not lineage_contracts:
        lineage_contracts = [target_contract]

    state_vars: dict[int, dict] = {}
    for contract in lineage_contracts:
        contract_nodes = contract.get("nodes", [])
        for node in contract_nodes:
            if not isinstance(node, dict):
                continue
            if node.get("nodeType") != "VariableDeclaration" or not node.get("stateVariable"):
                continue
            var_id = node.get("id")
            if not isinstance(var_id, int) or var_id in state_vars:
                continue

            type_desc = node.get("typeDescriptions") if isinstance(node.get("typeDescriptions"), dict) else {}
            state_vars[var_id] = {
                "id": f"v{var_id}",
                "name": str(node.get("name") or "(anonymous)"),
                "type": str(type_desc.get("typeString") or "unknown"),
                "declaring_contract": str(contract.get("name") or ""),
                "visibility": str(node.get("visibility") or ""),
                "mutability": str(node.get("mutability") or "mutable"),
            }

    state_var_ids = set(state_vars.keys())

    method_identifiers = artifact.get("methodIdentifiers")
    if not isinstance(method_identifiers, dict):
        method_identifiers = {}

    signature_to_selector: dict[str, str] = {}
    selector_to_signature: dict[str, str] = {}
    for signature, selector in method_identifiers.items():
        if not isinstance(signature, str) or not isinstance(selector, str):
            continue
        norm_selector = _normalize_selector(selector)
        if norm_selector == "0x":
            continue
        signature_to_selector[signature] = norm_selector
        selector_to_signature[norm_selector] = signature

    function_index: dict[str, dict] = {}
    function_by_selector: dict[str, dict] = {}

    for contract in lineage_contracts:
        contract_nodes = contract.get("nodes", [])
        declaring_contract = str(contract.get("name") or "")

        for node in contract_nodes:
            if not isinstance(node, dict) or node.get("nodeType") != "FunctionDefinition":
                continue

            name = _function_display_name(node, declaring_contract)
            params = _function_param_types(node)

            signature = None
            selector = None

            if name not in SPECIAL_FUNCTION_NAMES and name != "(anonymous)":
                selector_candidate = node.get("functionSelector")
                if isinstance(selector_candidate, str) and selector_candidate:
                    selector = _normalize_selector(selector_candidate)
                    signature = selector_to_signature.get(selector)

                if signature is None:
                    candidate_sig = f"{name}({','.join(params)})"
                    if candidate_sig in signature_to_selector:
                        signature = candidate_sig
                        selector = signature_to_selector[candidate_sig]

                if signature is None:
                    same_name = [
                        sig
                        for sig in signature_to_selector
                        if sig.startswith(f"{name}(") and _signature_param_count(sig) == len(params)
                    ]
                    if len(same_name) == 1:
                        signature = same_name[0]
                        selector = signature_to_selector[signature]

            writes, reads = _collect_state_access(node.get("body"), state_var_ids)
            write_names = sorted({state_vars[var_id]["name"] for var_id in writes if var_id in state_vars})
            read_names = sorted({state_vars[var_id]["name"] for var_id in reads if var_id in state_vars})

            fn_id = node.get("id")
            unique_key = selector if selector else f"{declaring_contract}:{name}:{fn_id}"
            if unique_key in function_index:
                continue

            entry = {
                "selector": selector,
                "signature": signature,
                "display_name": signature or name,
                "declaring_contract": declaring_contract,
                "visibility": str(node.get("visibility") or ""),
                "state_mutability": str(node.get("stateMutability") or ""),
                "state_writes": write_names,
                "state_reads": read_names,
                "tx_count": 0,
                "tx_hashes": [],
                "dependencies": [],
            }
            function_index[unique_key] = entry
            if selector and selector not in function_by_selector:
                function_by_selector[selector] = entry

    for signature, selector in sorted(signature_to_selector.items(), key=lambda item: item[0]):
        if selector in function_by_selector:
            continue
        entry = {
            "selector": selector,
            "signature": signature,
            "display_name": signature,
            "declaring_contract": "unknown",
            "visibility": "unknown",
            "state_mutability": "unknown",
            "state_writes": [],
            "state_reads": [],
            "tx_count": 0,
            "tx_hashes": [],
            "dependencies": [],
        }
        function_index[f"selector:{selector}"] = entry
        function_by_selector[selector] = entry

    selector_to_txs: dict[str, set[str]] = defaultdict(set)
    tx_selector_by_hash: dict[str, str] = {}
    transactions = []

    tx_list = payload.get("transactions_analyzed")
    if isinstance(tx_list, list):
        for tx in tx_list:
            if not isinstance(tx, dict):
                continue
            tx_hash = tx.get("tx_hash")
            if not isinstance(tx_hash, str) or not tx_hash:
                continue

            selector = _normalize_selector(tx.get("method_selector"))
            tx_selector_by_hash[tx_hash] = selector
            selector_to_txs[selector].add(tx_hash)
            transactions.append(
                {
                    "tx_hash": tx_hash,
                    "block_number": tx.get("block_number"),
                    "method_selector": selector,
                    "signature": (
                        function_by_selector.get(selector, {}).get("signature")
                        or selector_to_signature.get(selector)
                    ),
                }
            )

    dependency_records: dict[tuple[str, str], dict[str, Any]] = {}
    provenance = payload.get("provenance")
    if isinstance(provenance, dict):
        for dependency, records in provenance.items():
            if not isinstance(dependency, str) or not isinstance(records, list):
                continue
            dependency_address = _norm_address(dependency)

            for record in records:
                if not isinstance(record, dict):
                    continue
                tx_hash = record.get("tx_hash")
                selector = tx_selector_by_hash.get(tx_hash, "0x")
                from_addr = record.get("from")
                if not isinstance(from_addr, str) or not from_addr:
                    continue
                from_address = _norm_address(from_addr)
                op = str(record.get("op") or "CALL").upper()

                key = (selector, dependency_address)
                if key not in dependency_records:
                    dependency_records[key] = {
                        "selector": selector,
                        "dependency": dependency_address,
                        "direct_calls": 0,
                        "transitive_calls": 0,
                        "ops": set(),
                        "tx_hashes": set(),
                        "from_addresses": set(),
                    }

                entry = dependency_records[key]
                if from_address == root:
                    entry["direct_calls"] += 1
                else:
                    entry["transitive_calls"] += 1
                entry["ops"].add(op)
                if isinstance(tx_hash, str) and tx_hash:
                    entry["tx_hashes"].add(tx_hash)
                entry["from_addresses"].add(from_address)

    dependencies_by_selector: dict[str, list[dict]] = defaultdict(list)
    for entry in dependency_records.values():
        selector = entry["selector"]
        function_entry = function_by_selector.get(selector)
        affected_vars = function_entry.get("state_writes", []) if function_entry else []
        dependencies_by_selector[selector].append(
            {
                "dependency": entry["dependency"],
                "direct_calls": entry["direct_calls"],
                "transitive_calls": entry["transitive_calls"],
                "ops": sorted(entry["ops"]),
                "tx_hashes": sorted(entry["tx_hashes"]),
                "from_addresses": sorted(entry["from_addresses"]),
                "affected_state_vars": affected_vars,
            }
        )

    all_selectors = set(function_by_selector.keys()) | set(selector_to_txs.keys()) | set(dependencies_by_selector.keys())

    function_rows = []
    for selector in sorted(all_selectors):
        base_entry = function_by_selector.get(selector)
        if base_entry is None:
            base_entry = {
                "selector": selector,
                "signature": selector_to_signature.get(selector),
                "display_name": selector_to_signature.get(selector) or selector,
                "declaring_contract": "unknown",
                "visibility": "unknown",
                "state_mutability": "unknown",
                "state_writes": [],
                "state_reads": [],
                "tx_count": 0,
                "tx_hashes": [],
                "dependencies": [],
            }

        tx_hashes = sorted(selector_to_txs.get(selector, set()))
        deps = dependencies_by_selector.get(selector, [])
        deps.sort(
            key=lambda dep: (
                -(dep["direct_calls"] + dep["transitive_calls"]),
                dep["dependency"],
            )
        )

        row = {
            "selector": selector,
            "signature": base_entry.get("signature") or selector_to_signature.get(selector),
            "display_name": base_entry.get("display_name") or selector,
            "declaring_contract": base_entry.get("declaring_contract") or "unknown",
            "visibility": base_entry.get("visibility") or "unknown",
            "state_mutability": base_entry.get("state_mutability") or "unknown",
            "state_writes": list(base_entry.get("state_writes") or []),
            "state_reads": list(base_entry.get("state_reads") or []),
            "tx_count": len(tx_hashes),
            "tx_hashes": tx_hashes,
            "dependencies": deps,
        }
        function_rows.append(row)

    function_rows.sort(
        key=lambda fn: (
            -fn["tx_count"],
            fn["signature"] or fn["display_name"] or fn["selector"],
        )
    )

    state_variables = sorted(
        state_vars.values(),
        key=lambda var: (var["declaring_contract"], var["name"]),
    )

    unmatched_transactions = [
        tx for tx in transactions if tx["method_selector"] not in function_by_selector
    ]

    artifact_relative = str(artifact_path.relative_to(project_dir))
    lineage_names = [str(contract.get("name") or "") for contract in lineage_contracts]

    return {
        "available": True,
        "warning": warning,
        "contract_name": target_name,
        "artifact": artifact_relative,
        "lineage": lineage_names,
        "state_variables": state_variables,
        "functions": function_rows,
        "transactions": transactions,
        "unmatched_transactions": unmatched_transactions,
    }


def _build_known_address_names(
    project_dir: Path,
    node_addresses: list[str],
    root: str,
    root_contract_name: str | None,
) -> dict[str, str]:
    names: dict[str, str] = {}
    targets = {address for address in node_addresses}

    if root_contract_name:
        names[root] = root_contract_name

    contracts_root = project_dir.parent
    if contracts_root.exists():
        for meta_path in contracts_root.glob("*/contract_meta.json"):
            meta = _load_json(meta_path)
            if not meta:
                continue
            address = meta.get("address")
            if not isinstance(address, str):
                continue
            normalized = _norm_address(address)
            if normalized not in targets:
                continue
            contract_name = meta.get("contract_name")
            if isinstance(contract_name, str) and contract_name.strip():
                names[normalized] = contract_name.strip()
            else:
                names[normalized] = meta_path.parent.name

    label_sources = [
        project_dir / "address_labels.json",
        project_dir.parent / "address_labels.json",
    ]
    for labels_path in label_sources:
        labels = _load_json(labels_path)
        if isinstance(labels, dict):
            for address, label in labels.items():
                if not isinstance(address, str) or not isinstance(label, str):
                    continue
                normalized = _norm_address(address)
                if normalized in targets and label.strip():
                    names[normalized] = label.strip()

    return names


def _build_edge_function_enrichment(edges: list[dict], contract_info: dict) -> list[dict]:
    tx_by_hash: dict[str, dict] = {}
    txs = contract_info.get("transactions")
    if isinstance(txs, list):
        for tx in txs:
            if not isinstance(tx, dict):
                continue
            tx_hash = tx.get("tx_hash")
            if isinstance(tx_hash, str) and tx_hash:
                tx_by_hash[tx_hash] = tx

    fn_by_selector: dict[str, dict] = {}
    functions = contract_info.get("functions")
    if isinstance(functions, list):
        for fn in functions:
            if not isinstance(fn, dict):
                continue
            selector = fn.get("selector")
            if isinstance(selector, str) and selector:
                fn_by_selector[selector] = fn

    enriched = []
    for edge in edges:
        signatures = set()
        selectors = set()
        state_writes = set()
        state_reads = set()

        tx_hashes = edge.get("tx_hashes", [])
        if isinstance(tx_hashes, list):
            for tx_hash in tx_hashes:
                tx = tx_by_hash.get(tx_hash)
                if not tx:
                    continue
                selector = tx.get("method_selector")
                if isinstance(selector, str) and selector:
                    selectors.add(selector)
                    fn = fn_by_selector.get(selector)
                    if fn:
                        for var in fn.get("state_writes") or []:
                            if isinstance(var, str):
                                state_writes.add(var)
                        for var in fn.get("state_reads") or []:
                            if isinstance(var, str):
                                state_reads.add(var)
                signature = tx.get("signature")
                if isinstance(signature, str) and signature:
                    signatures.add(signature)
                elif isinstance(selector, str) and selector:
                    signatures.add(selector)

        enriched.append(
            {
                "function_signatures": sorted(signatures),
                "selectors": sorted(selectors),
                "state_writes": sorted(state_writes),
                "state_reads": sorted(state_reads),
            }
        )

    return enriched


def _render_html(
    root: str,
    nodes: list[str],
    edges: list[dict],
    title: str,
    contract_info: dict,
    node_names: dict[str, str],
) -> str:
    edge_enrichment = _build_edge_function_enrichment(edges, contract_info)
    graph_payload = {
        "title": title,
        "root": root,
        "nodes": [
            {
                "id": _node_id(address),
                "address": address,
                "label": _short_label(address),
                "name": node_names.get(address, ""),
                "is_root": address == root,
            }
            for address in nodes
        ],
        "edges": [
            {
                "id": f"e{index}",
                "from": _node_id(edge["from"]),
                "to": _node_id(edge["to"]),
                "from_address": edge["from"],
                "to_address": edge["to"],
                "op": edge["op"],
                "count": edge["count"],
                "tx_hashes": edge["tx_hashes"],
                "function_signatures": edge_enrichment[index - 1]["function_signatures"],
                "selectors": edge_enrichment[index - 1]["selectors"],
                "state_writes": edge_enrichment[index - 1]["state_writes"],
                "state_reads": edge_enrichment[index - 1]["state_reads"],
            }
            for index, edge in enumerate(edges, start=1)
        ],
    }

    app_payload = {
        "graph": graph_payload,
        "contract": contract_info,
    }
    app_json = json.dumps(app_payload, separators=(",", ":")).replace("</", "<\\/")

    template = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>@@TITLE@@</title>
  <style>
    :root {
      --bg: #f7f7f4;
      --panel: #ffffff;
      --text: #1f2937;
      --muted: #6b7280;
      --accent: #92400e;
      --border: #e5e7eb;
      --ok: #166534;
      --ok-bg: #dcfce7;
      --warn: #92400e;
      --warn-bg: #fef3c7;
      --danger: #991b1b;
      --danger-bg: #fee2e2;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "Helvetica Neue", sans-serif;
      color: var(--text);
      background: var(--bg);
    }
    .layout {
      display: grid;
      grid-template-columns: 440px 1fr;
      min-height: 100vh;
      gap: 16px;
      padding: 16px;
    }
    main {
      min-width: 0;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px;
      overflow: auto;
      max-height: calc(100vh - 32px);
    }
    h1 {
      margin: 0 0 10px 0;
      font-size: 18px;
    }
    h2 {
      margin: 14px 0 8px 0;
      font-size: 13px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .stat {
      margin: 6px 0;
      font-size: 13px;
      color: var(--muted);
    }
    .stat strong {
      color: var(--text);
    }
    .controls label {
      display: block;
      font-size: 13px;
      margin-bottom: 6px;
    }
    .controls input[type="text"],
    .controls input[type="number"],
    .controls select {
      width: 100%;
      padding: 8px 10px;
      border: 1px solid var(--border);
      border-radius: 8px;
      margin-bottom: 10px;
      background: #fff;
    }
    .ops {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 4px 10px;
      margin-bottom: 10px;
    }
    button {
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel);
      color: var(--text);
      padding: 8px 10px;
      cursor: pointer;
      margin-right: 8px;
    }
    button:hover {
      border-color: var(--accent);
      color: var(--accent);
    }
    .button-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    #network {
      width: 100%;
      height: calc(100vh - 34px);
      border: 1px solid var(--border);
      border-radius: 12px;
      background: #ffffff;
    }
    #details {
      margin-top: 10px;
      font-size: 12px;
      line-height: 1.45;
      color: var(--muted);
      white-space: pre-wrap;
      word-break: break-word;
    }
    .badge {
      display: inline-block;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 11px;
      margin-right: 6px;
    }
    .badge-write { color: var(--ok); background: var(--ok-bg); }
    .badge-read { color: #1d4ed8; background: #dbeafe; }
    .badge-none { color: var(--muted); background: #f3f4f6; }
    .table-wrap {
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
      margin-bottom: 10px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }
    th, td {
      border-bottom: 1px solid var(--border);
      padding: 6px 8px;
      text-align: left;
      vertical-align: top;
      word-break: break-word;
    }
    th {
      background: #fafafa;
      color: var(--muted);
      font-weight: 600;
    }
    tr:last-child td {
      border-bottom: none;
    }
    .hint {
      color: var(--muted);
      font-size: 12px;
      margin: 4px 0 10px 0;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .error {
      color: var(--danger);
      background: var(--danger-bg);
      border: 1px solid #fecaca;
      border-radius: 8px;
      padding: 10px;
      margin-top: 10px;
      font-size: 12px;
    }
    .warn {
      color: var(--warn);
      background: var(--warn-bg);
      border: 1px solid #fde68a;
      border-radius: 8px;
      padding: 10px;
      margin-top: 10px;
      font-size: 12px;
    }
    @media (max-width: 1200px) {
      .layout {
        grid-template-columns: 380px 1fr;
      }
    }
    @media (max-width: 960px) {
      .layout {
        grid-template-columns: 1fr;
        min-height: auto;
      }
      main {
        order: 1;
      }
      .panel {
        order: 2;
        max-height: none;
      }
      #network {
        height: 58vh;
      }
      .ops {
        grid-template-columns: 1fr;
      }
      .table-wrap {
        max-height: 240px;
        overflow: auto;
      }
      table {
        font-size: 11px;
      }
    }
  </style>
  <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
</head>
<body>
  <div class="layout">
    <aside class="panel">
      <h1>Call Graph Explorer</h1>
      <div class="stat"><strong>Target:</strong> @@ROOT@@</div>
      <div class="stat"><strong>Contract:</strong> <span id="contractName">-</span></div>
      <div class="stat" id="stats"></div>

      <h2>Graph Filters</h2>
      <div class="controls">
        <label>Address Search</label>
        <input type="text" id="search" placeholder="0x..." />

        <label>Min Edge Count</label>
        <input type="number" id="minCount" min="1" value="2" />

        <label style="margin-top: 8px;">
          <input type="checkbox" id="showIsolated" />
          Show isolated nodes
        </label>
        <label style="margin-top: 8px;">
          <input type="checkbox" id="filterByFunction" checked />
          Filter graph by selected function transactions
        </label>
        <label style="margin-top: 8px;">
          <input type="checkbox" id="hideSelfLoops" checked />
          Hide self-loop edges
        </label>
        <label style="margin-top: 8px;">
          <input type="checkbox" id="showEdgeLabels" checked />
          Show function names on edges
        </label>

        <h2>Operations</h2>
        <div id="ops" class="ops"></div>

        <div class="button-row">
          <button id="apply">Apply Filters</button>
          <button id="reset">Reset View</button>
        </div>
      </div>

      <h2>Function Impact</h2>
      <div class="controls">
        <label>Function / Selector</label>
        <select id="fnSelect"></select>
        <div id="fnMeta" class="hint"></div>
      </div>

      <div class="table-wrap">
        <table id="stateVarsTable">
          <thead>
            <tr>
              <th>State Variable</th>
              <th>Type</th>
              <th>Access In Function</th>
            </tr>
          </thead>
          <tbody id="stateVarsBody"></tbody>
        </table>
      </div>

      <div class="table-wrap">
        <table id="depsTable">
          <thead>
            <tr>
              <th>Dependency</th>
              <th>Direct</th>
              <th>Transitive</th>
              <th>Ops</th>
              <th>Affected Vars</th>
            </tr>
          </thead>
          <tbody id="depsBody"></tbody>
        </table>
      </div>

      <h2>Selection</h2>
      <div id="details">Select a node or edge to inspect details.</div>
      <div id="warningBox" class="warn" style="display:none;"></div>
      <div id="errorBox" class="error" style="display:none;"></div>
    </aside>
    <main>
      <div id="network"></div>
    </main>
  </div>

  <script>
    const appData = @@DATA@@;
    const graphData = appData.graph;
    const contractData = appData.contract;

    const statsEl = document.getElementById("stats");
    const detailsEl = document.getElementById("details");
    const errorEl = document.getElementById("errorBox");
    const warningEl = document.getElementById("warningBox");
    const opsEl = document.getElementById("ops");
    const minCountEl = document.getElementById("minCount");
    const searchEl = document.getElementById("search");
    const isolatedEl = document.getElementById("showIsolated");
    const filterByFunctionEl = document.getElementById("filterByFunction");
    const hideSelfLoopsEl = document.getElementById("hideSelfLoops");
    const showEdgeLabelsEl = document.getElementById("showEdgeLabels");
    const contractNameEl = document.getElementById("contractName");
    const fnSelectEl = document.getElementById("fnSelect");
    const fnMetaEl = document.getElementById("fnMeta");
    const stateVarsBodyEl = document.getElementById("stateVarsBody");
    const depsBodyEl = document.getElementById("depsBody");
    function showError(message) {
      errorEl.textContent = message;
      errorEl.style.display = "block";
    }

    function showWarning(message) {
      warningEl.textContent = message;
      warningEl.style.display = "block";
    }

    if (typeof vis === "undefined") {
      showError("vis-network failed to load. Check internet access, then reopen this file.");
      detailsEl.textContent = "Raw data remains available in dynamic_dependencies.json.";
      throw new Error("vis-network unavailable");
    }

    const allOps = [...new Set(graphData.edges.map((edge) => edge.op))].sort();
    for (const op of allOps) {
      const label = document.createElement("label");
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.value = op;
      checkbox.checked = true;
      label.appendChild(checkbox);
      label.appendChild(document.createTextNode(" " + op));
      opsEl.appendChild(label);
    }

    const allNodesById = new Map(graphData.nodes.map((node) => [node.id, node]));
    const allEdgesById = new Map(graphData.edges.map((edge) => [edge.id, edge]));

    const nodeDataSet = new vis.DataSet([]);
    const edgeDataSet = new vis.DataSet([]);

    const network = new vis.Network(
      document.getElementById("network"),
      { nodes: nodeDataSet, edges: edgeDataSet },
      {
        autoResize: true,
        layout: { improvedLayout: true, randomSeed: 7 },
        physics: {
          solver: "forceAtlas2Based",
          forceAtlas2Based: {
            gravitationalConstant: -55,
            centralGravity: 0.01,
            springLength: 180,
            springConstant: 0.05,
            damping: 0.4,
            avoidOverlap: 1
          },
          minVelocity: 0.75,
          stabilization: { enabled: true, iterations: 250, updateInterval: 25 }
        },
        interaction: { hover: true, tooltipDelay: 120 },
        edges: {
          arrows: "to",
          font: { size: 10, align: "top", color: "#374151" },
          smooth: { type: "dynamic", roundness: 0.16 }
        },
        nodes: {
          shape: "box",
          borderWidth: 1,
          margin: 8,
          font: { size: 12, face: "Consolas" }
        }
      }
    );

    let currentFunction = null;

    function selectedOps() {
      return new Set(
        [...opsEl.querySelectorAll("input[type='checkbox']")]
          .filter((box) => box.checked)
          .map((box) => box.value)
      );
    }

    function currentFunctionTxSet() {
      if (!currentFunction || !Array.isArray(currentFunction.tx_hashes) || currentFunction.tx_hashes.length === 0) {
        return null;
      }
      return new Set(currentFunction.tx_hashes);
    }

    function applyFilters() {
      const ops = selectedOps();
      const minCount = Math.max(1, Number(minCountEl.value || 1));
      const showIsolated = isolatedEl.checked;
      const hideSelfLoops = hideSelfLoopsEl.checked;
      const showEdgeLabels = showEdgeLabelsEl.checked;

      let edges = graphData.edges.filter((edge) => ops.has(edge.op) && edge.count >= minCount);

      if (hideSelfLoops) {
        edges = edges.filter((edge) => edge.from !== edge.to);
      }

      if (filterByFunctionEl.checked) {
        const txSet = currentFunctionTxSet();
        if (txSet) {
          edges = edges.filter((edge) => edge.tx_hashes.some((hash) => txSet.has(hash)));
        }
      }

      const connected = new Set();
      for (const edge of edges) {
        connected.add(edge.from);
        connected.add(edge.to);
      }
      connected.add("n_" + graphData.root.slice(2));

      const nodes = graphData.nodes.filter((node) => showIsolated || connected.has(node.id));

      nodeDataSet.clear();
      edgeDataSet.clear();

      nodeDataSet.add(
        nodes.map((node) => {
          const color = node.is_root
            ? { background: "#fef3c7", border: "#92400e" }
            : { background: "#ffffff", border: "#cbd5e1" };
          const labelText = node.name
            ? node.name + "\\n(" + node.label + ")"
            : node.address;
          const titleText = node.name
            ? node.name + "\\n" + node.address
            : node.address;
          return {
            id: node.id,
            label: labelText,
            title: titleText,
            color: color
          };
        })
      );

      edgeDataSet.add(
        edges.map((edge) => {
          const baseColor = edge.op === "DELEGATECALL" ? "#b45309" : "#94a3b8";
          const fnSignatures = Array.isArray(edge.function_signatures) ? edge.function_signatures : [];
          const fnText = fnSignatures.length ? fnSignatures.join(", ") : "unknown";
          const txText = edge.tx_hashes.length ? edge.tx_hashes.join("\\n") : "No tx hashes captured";
          const writesText = edge.state_writes && edge.state_writes.length
            ? edge.state_writes.join(", ")
            : "none";
          const readsText = edge.state_reads && edge.state_reads.length
            ? edge.state_reads.join(", ")
            : "none";
          return {
            id: edge.id,
            from: edge.from,
            to: edge.to,
            label: formatEdgeLabel(edge, showEdgeLabels),
            title:
              "functions: " + fnText + "\\n" +
              "state writes: " + writesText + "\\n" +
              "state reads: " + readsText + "\\n\\n" +
              txText,
            value: edge.count,
            width: 1 + Math.min(8, edge.count * 0.7),
            color: { color: baseColor, opacity: 0.75 }
          };
        })
      );

      statsEl.innerHTML = "<strong>Nodes:</strong> " + nodes.length + " | <strong>Edges:</strong> " + edges.length;
      network.fit({ animation: false });
      detailsEl.textContent = "Select a node or edge to inspect details.";
    }

    function inspectSelection(params) {
      if (params.nodes.length > 0) {
        const node = allNodesById.get(params.nodes[0]);
        if (!node) return;
        const incoming = graphData.edges.filter((edge) => edge.to === node.id);
        const outgoing = graphData.edges.filter((edge) => edge.from === node.id);
        const incomingFlow = incoming
          .slice(0, 4)
          .map((edge) => {
            const source = allNodesById.get(edge.from);
            const sourceLabel = source && source.name ? source.name : edge.from;
            const names = edgeFunctionNames(edge);
            const fnLabel = names.length ? names.join(", ") : "unknown";
            return "  - " + sourceLabel + " -> this: " + fnLabel + " (" + edge.op + ", x" + edge.count + ")";
          })
          .join("\\n");

        const outgoingFlow = outgoing
          .slice(0, 4)
          .map((edge) => {
            const target = allNodesById.get(edge.to);
            const targetLabel = target && target.name ? target.name : edge.to;
            const names = edgeFunctionNames(edge);
            const fnLabel = names.length ? names.join(", ") : "unknown";
            return "  - this -> " + targetLabel + ": " + fnLabel + " (" + edge.op + ", x" + edge.count + ")";
          })
          .join("\\n");

        const incomingLabel = incomingFlow || "  - none";
        const outgoingLabel = outgoingFlow || "  - none";
        const labelLine = node.name ? ("name: " + node.name + "\\n") : "";
        detailsEl.textContent =
          "Node\\n" +
          labelLine +
          "address: " + node.address + "\\n" +
          "incoming edges: " + incoming.length + "\\n" +
          "outgoing edges: " + outgoing.length + "\\n\\n" +
          "Sample incoming flow:\\n" +
          incomingLabel + "\\n\\n" +
          "Sample outgoing flow:\\n" +
          outgoingLabel;
        return;
      }

      if (params.edges.length > 0) {
        const edge = allEdgesById.get(params.edges[0]);
        if (!edge) return;
        const fromNode = allNodesById.get(edge.from);
        const toNode = allNodesById.get(edge.to);
        const fromLabel = fromNode && fromNode.name ? fromNode.name : edge.from;
        const toLabel = toNode && toNode.name ? toNode.name : edge.to;
        const txLines = edge.tx_hashes.length
          ? edge.tx_hashes.map((hash) => "  - " + hash).join("\\n")
          : "  - none recorded";
        const fnLines = edge.function_signatures && edge.function_signatures.length
          ? edge.function_signatures.map((name) => "  - " + name).join("\\n")
          : "  - unknown";
        const writeLines = edge.state_writes && edge.state_writes.length
          ? edge.state_writes.map((name) => "  - " + name).join("\\n")
          : "  - none detected";
        const readLines = edge.state_reads && edge.state_reads.length
          ? edge.state_reads.map((name) => "  - " + name).join("\\n")
          : "  - none detected";
        detailsEl.textContent =
          "Edge\\n" +
          "from: " + fromLabel + " [" + edge.from_address + "]" + "\\n" +
          "to:   " + toLabel + " [" + edge.to_address + "]" + "\\n" +
          "op: " + edge.op + "\\n" +
          "count: " + edge.count + "\\n" +
          "functions:\\n" + fnLines + "\\n" +
          "possible root state writes:\\n" + writeLines + "\\n" +
          "possible root state reads:\\n" + readLines + "\\n" +
          "tx_hashes:\\n" + txLines;
      }
    }

    function shortAddress(address) {
      if (!address || typeof address !== "string" || address.length < 14) return String(address || "");
      return address.slice(0, 8) + "..." + address.slice(-6);
    }

    function shortNodeName(name) {
      if (typeof name !== "string" || !name) return "";
      if (name.length <= 18) return name;
      return name.slice(0, 17) + "…";
    }

    function shortFunctionName(signature) {
      if (typeof signature !== "string" || !signature) return "unknown";
      const openParen = signature.indexOf("(");
      const name = openParen > 0 ? signature.slice(0, openParen) : signature;
      if (name.length <= 20) return name;
      return name.slice(0, 19) + "…";
    }

    function edgeFunctionNames(edge) {
      const uniqueNames = new Set();
      const signatures = Array.isArray(edge.function_signatures) ? edge.function_signatures : [];
      for (const signature of signatures) {
        if (signature) {
          uniqueNames.add(shortFunctionName(signature));
        }
      }
      return [...uniqueNames];
    }

    function formatEdgeLabel(edge, showEdgeLabels) {
      if (!showEdgeLabels) return "";

      const names = edgeFunctionNames(edge);
      if (names.length === 0) {
        return edge.op + " x" + edge.count;
      }
      if (names.length === 1) {
        return names[0] + " · " + edge.op + " x" + edge.count;
      }

      const prefix = names.slice(0, 2).join(", ");
      const remainder = names.length > 2 ? " +" + (names.length - 2) : "";
      return prefix + remainder + " · " + edge.op + " x" + edge.count;
    }

    function truncateText(text, maxLength) {
      if (typeof text !== "string") return String(text || "");
      if (text.length <= maxLength) return text;
      return text.slice(0, maxLength - 1) + "…";
    }

    function renderStateVariables(fn) {
      stateVarsBodyEl.innerHTML = "";

      const vars = Array.isArray(contractData.state_variables) ? contractData.state_variables : [];
      const writes = new Set((fn && Array.isArray(fn.state_writes)) ? fn.state_writes : []);
      const reads = new Set((fn && Array.isArray(fn.state_reads)) ? fn.state_reads : []);

      if (vars.length === 0) {
        stateVarsBodyEl.innerHTML = '<tr><td colspan="3">No state variables were detected in the compiled AST lineage.</td></tr>';
        return;
      }

      for (const variable of vars) {
        const tr = document.createElement("tr");
        const access = writes.has(variable.name)
          ? '<span class="badge badge-write">write</span>'
          : (reads.has(variable.name)
              ? '<span class="badge badge-read">read</span>'
              : '<span class="badge badge-none">none</span>');

        tr.innerHTML =
          "<td><strong>" + variable.name + "</strong><br><span class='hint'>" + variable.declaring_contract + "</span></td>" +
          "<td>" + variable.type + "</td>" +
          "<td>" + access + "</td>";
        stateVarsBodyEl.appendChild(tr);
      }
    }

    function renderDependencies(fn) {
      depsBodyEl.innerHTML = "";
      const deps = (fn && Array.isArray(fn.dependencies)) ? fn.dependencies : [];

      if (!fn) {
        depsBodyEl.innerHTML = '<tr><td colspan="5">No function selected.</td></tr>';
        return;
      }

      if (deps.length === 0) {
        depsBodyEl.innerHTML = '<tr><td colspan="5">No traced dependency calls recorded for this function selection.</td></tr>';
        return;
      }

      for (const dep of deps) {
        const affected = dep.affected_state_vars && dep.affected_state_vars.length
          ? dep.affected_state_vars.join(", ")
          : "none detected";

        const tr = document.createElement("tr");
        tr.innerHTML =
          "<td><span title='" + dep.dependency + "'>" + shortAddress(dep.dependency) + "</span></td>" +
          "<td>" + dep.direct_calls + "</td>" +
          "<td>" + dep.transitive_calls + "</td>" +
          "<td>" + dep.ops.join(", ") + "</td>" +
          "<td>" + affected + "</td>";
        depsBodyEl.appendChild(tr);
      }
    }

    function renderFunctionPanel(fn) {
      currentFunction = fn;
      if (!fn) {
        fnMetaEl.textContent = "No function/selector data available.";
        renderStateVariables(null);
        renderDependencies(null);
        applyFilters();
        return;
      }

      const signature = fn.signature || fn.display_name || fn.selector || "unknown";
      fnMetaEl.textContent =
        truncateText(signature, 120) + "\\n" +
        "selector " + (fn.selector || "n/a") + " | txs " + fn.tx_count + "\\n" +
        "declared in " + (fn.declaring_contract || "unknown") + " | " +
        (fn.visibility || "unknown") + " / " + (fn.state_mutability || "unknown");

      renderStateVariables(fn);
      renderDependencies(fn);
      applyFilters();
    }

    function initializeContractPanel() {
      contractNameEl.textContent = contractData.contract_name || "Unknown";

      if (contractData.warning) {
        showWarning(contractData.warning);
      }

      if (!contractData.available) {
        fnSelectEl.innerHTML = '<option value="">No contract intelligence available</option>';
        renderFunctionPanel(null);
        return;
      }

      const functions = Array.isArray(contractData.functions) ? contractData.functions : [];
      if (functions.length === 0) {
        fnSelectEl.innerHTML = '<option value="">No function intelligence found</option>';
        renderFunctionPanel(null);
        return;
      }

      functions.forEach((fn, index) => {
        const option = document.createElement("option");
        option.value = String(index);
        const name = fn.signature || fn.display_name || fn.selector || "unknown";
        option.textContent = name + " (" + (fn.selector || "n/a") + ", txs " + fn.tx_count + ")";
        fnSelectEl.appendChild(option);
      });

      fnSelectEl.addEventListener("change", () => {
        const index = Number(fnSelectEl.value);
        const fn = Number.isFinite(index) ? functions[index] : null;
        renderFunctionPanel(fn || null);
      });

      renderFunctionPanel(functions[0]);
    }

    document.getElementById("apply").addEventListener("click", applyFilters);
    document.getElementById("reset").addEventListener("click", () => {
      network.fit({ animation: true });
    });

    filterByFunctionEl.addEventListener("change", applyFilters);
    hideSelfLoopsEl.addEventListener("change", applyFilters);
    showEdgeLabelsEl.addEventListener("change", applyFilters);

    searchEl.addEventListener("change", () => {
      const query = searchEl.value.trim().toLowerCase();
      if (!query) return;
      const node = graphData.nodes.find((entry) => entry.address.toLowerCase().includes(query));
      if (node) {
        network.selectNodes([node.id], false);
        network.focus(node.id, { scale: 1.2, animation: true });
      } else {
        showError("Address not found in current graph: " + query);
      }
    });

    network.on("select", inspectSelection);
    initializeContractPanel();
    applyFilters();
  </script>
</body>
</html>
"""

    return (
        template.replace("@@TITLE@@", _label_escape(title))
        .replace("@@ROOT@@", root)
        .replace("@@DATA@@", app_json)
    )


def export_call_graph(payload: dict, output_dir: Path, base_name: str = "dynamic_call_graph") -> tuple[Path, Path, Path]:
    """Write Mermaid, DOT, and HTML graph files for a dynamic dependency payload."""
    root, nodes, edges = _collect_graph_model(payload)
    title = f"Dynamic Call Graph for {root}"
    contract_info = _build_contract_intelligence(output_dir, payload, root)
    node_names = _build_known_address_names(
        output_dir,
        nodes,
        root,
        contract_info.get("contract_name") if isinstance(contract_info, dict) else None,
    )

    mermaid_text = _render_mermaid(root, nodes, edges, title)
    dot_text = _render_dot(root, nodes, edges, title)
    html_text = _render_html(root, nodes, edges, title, contract_info, node_names)

    mermaid_path = output_dir / f"{base_name}.mmd"
    dot_path = output_dir / f"{base_name}.dot"
    html_path = output_dir / f"{base_name}.html"

    mermaid_path.write_text(mermaid_text)
    dot_path.write_text(dot_text)
    html_path.write_text(html_text)

    return mermaid_path, dot_path, html_path


def export_call_graph_from_file(dynamic_json_path: Path, base_name: str = "dynamic_call_graph") -> tuple[Path, Path, Path]:
    """Load a dynamic dependency file and export graph files in the same directory."""
    payload = json.loads(dynamic_json_path.read_text())
    return export_call_graph(payload, dynamic_json_path.parent, base_name=base_name)


def discover_dynamic_dependency_files(path: Path) -> list[Path]:
    """Return dynamic dependency json files under a file or directory input."""
    if path.is_file():
        if path.name != "dynamic_dependencies.json":
            raise RuntimeError(f"Expected a dynamic_dependencies.json file, got: {path}")
        return [path]
    if not path.is_dir():
        raise RuntimeError(f"Path not found: {path}")
    return sorted(path.rglob("dynamic_dependencies.json"))
