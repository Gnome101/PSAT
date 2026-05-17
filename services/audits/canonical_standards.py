"""Canonical standard coverage helpers.

These entries are not audit reports. They mark contracts whose deployed source
name is an exact match for a small allowlist of well-known standard shells.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CanonicalStandard:
    key: str
    display_name: str
    family: str
    contract_names: tuple[str, ...]


CANONICAL_STANDARDS: tuple[CanonicalStandard, ...] = (
    CanonicalStandard(
        key="gnosis-safe",
        display_name="Gnosis Safe",
        family="safe",
        contract_names=("GnosisSafe",),
    ),
    CanonicalStandard(
        key="gnosis-safe-proxy",
        display_name="Gnosis Safe Proxy",
        family="safe",
        contract_names=("GnosisSafeProxy",),
    ),
    CanonicalStandard(
        key="openzeppelin-transparent-upgradeable-proxy",
        display_name="OpenZeppelin TransparentUpgradeableProxy",
        family="openzeppelin",
        contract_names=("TransparentUpgradeableProxy",),
    ),
    CanonicalStandard(
        key="openzeppelin-erc1967-proxy",
        display_name="OpenZeppelin ERC1967Proxy",
        family="openzeppelin",
        contract_names=("ERC1967Proxy",),
    ),
    CanonicalStandard(
        key="openzeppelin-beacon-proxy",
        display_name="OpenZeppelin BeaconProxy",
        family="openzeppelin",
        contract_names=("BeaconProxy",),
    ),
    CanonicalStandard(
        key="openzeppelin-upgradeable-beacon",
        display_name="OpenZeppelin UpgradeableBeacon",
        family="openzeppelin",
        contract_names=("UpgradeableBeacon",),
    ),
    CanonicalStandard(
        key="openzeppelin-proxy-admin",
        display_name="OpenZeppelin ProxyAdmin",
        family="openzeppelin",
        contract_names=("ProxyAdmin",),
    ),
    CanonicalStandard(
        key="solady-uups-proxy",
        display_name="Solady UUPSProxy",
        family="solady",
        contract_names=("UUPSProxy",),
    ),
    CanonicalStandard(
        key="solady-optimized-transparent-upgradeable-proxy",
        display_name="Solady OptimizedTransparentUpgradeableProxy",
        family="solady",
        contract_names=("OptimizedTransparentUpgradeableProxy",),
    ),
    CanonicalStandard(
        key="solady-erc1967-factory",
        display_name="Solady ERC1967Factory",
        family="solady",
        contract_names=("ERC1967Factory",),
    ),
)

_BY_EXACT_NAME = {name.lower(): standard for standard in CANONICAL_STANDARDS for name in standard.contract_names}


def canonical_standard_for_contract(contract: Any) -> CanonicalStandard | None:
    """Return the canonical standard for an exact contract-name match."""
    name = str(getattr(contract, "contract_name", "") or "").strip()
    if not name:
        return None
    return _BY_EXACT_NAME.get(name.lower())


def canonical_coverage_brief(contract: Any) -> dict[str, Any] | None:
    standard = canonical_standard_for_contract(contract)
    if standard is None:
        return None

    contract_name = str(getattr(contract, "contract_name", "") or "").strip()
    return {
        "audit_id": f"canonical:{standard.key}",
        "auditor": "Canonical standard",
        "title": standard.display_name,
        "date": None,
        "match_type": "canonical_standard",
        "match_confidence": "high",
        "covered_from_block": None,
        "covered_to_block": None,
        "equivalence_status": "proven",
        "equivalence_reason": (
            f"Exact canonical contract-name match for {contract_name}; "
            "covers this standard shell only, not any proxy implementation."
        ),
        "equivalence_checked_at": None,
        "proof_kind": "canonical_standard",
        "matched_commit_sha": None,
        "coverage_source": "canonical_standard",
        "canonical_standard": standard.key,
        "canonical_standard_name": standard.display_name,
        "canonical_family": standard.family,
    }
