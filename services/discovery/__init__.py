"""Discovery package."""

from .activity import enrich_with_activity
from .audit_reports import merge_audit_reports, search_audit_reports
from .chain_resolver import resolve_unknown_chains
from .classifier import classify_contracts
from .dependency_graph_builder import write_dependency_visualization
from .deployer import expand_from_deployers
from .dynamic_dependencies import find_dynamic_dependencies
from .fetch import CONTRACTS_DIR, fetch, parse_remappings, parse_sources, parse_verification_bundle, scaffold
from .inventory import search_protocol_inventory
from .static_dependencies import find_dependencies
from .unified_dependencies import build_unified_dependencies, enrich_dependency_metadata

__all__ = [
    "build_unified_dependencies",
    "CONTRACTS_DIR",
    "classify_contracts",
    "merge_audit_reports",
    "enrich_dependency_metadata",
    "enrich_with_activity",
    "expand_from_deployers",
    "resolve_unknown_chains",
    "fetch",
    "find_dependencies",
    "find_dynamic_dependencies",
    "parse_remappings",
    "parse_sources",
    "parse_verification_bundle",
    "scaffold",
    "search_audit_reports",
    "search_protocol_inventory",
    "write_dependency_visualization",
]
