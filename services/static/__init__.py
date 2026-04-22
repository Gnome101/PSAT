"""Static analysis package."""

from .contract_analysis import collect_contract_analysis
from .slither import analyze, format_report, run_slither

__all__ = [
    "analyze",
    "collect_contract_analysis",
    "format_report",
    "run_slither",
]
