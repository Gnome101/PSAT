"""Discovery package."""

from .activity import enrich_with_activity
from .classifier import classify_contracts
from .dependency_graph_builder import write_dependency_visualization
from .deployer import expand_from_deployers
from .dynamic_dependencies import find_dynamic_dependencies
from .fetch import CONTRACTS_DIR, fetch, parse_remappings, parse_sources, parse_verification_bundle, scaffold
from .inventory import search_protocol_inventory
from .static_dependencies import find_dependencies

__all__ = [
    "CONTRACTS_DIR",
    "classify_contracts",
    "enrich_with_activity",
    "expand_from_deployers",
    "fetch",
    "find_dependencies",
    "find_dynamic_dependencies",
    "parse_remappings",
    "parse_sources",
    "parse_verification_bundle",
    "scaffold",
    "search_protocol_inventory",
    "write_dependency_visualization",
]
