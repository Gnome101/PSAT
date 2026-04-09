"""DefiLlama adapter crawler — extracts contract addresses from DefiLlama-Adapters repo."""

from services.crawlers.defillama.extract import extract_protocol, extract_addresses_from_file
from services.crawlers.defillama.core_assets import load_core_assets, build_address_to_chain_map
from services.crawlers.defillama.scan import scan_protocol, scan_all_protocols

__all__ = [
    "extract_protocol",
    "extract_addresses_from_file",
    "load_core_assets",
    "build_address_to_chain_map",
    "scan_protocol",
    "scan_all_protocols",
]
