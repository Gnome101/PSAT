"""Compatibility facade for the contract analysis package."""

from __future__ import annotations

from .contract_analysis_pipeline import analyze_contract, collect_contract_analysis

__all__ = ["analyze_contract", "collect_contract_analysis"]
