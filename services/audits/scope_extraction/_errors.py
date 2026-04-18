"""Exception types raised during scope extraction."""

from __future__ import annotations


class ScopeExtractionError(RuntimeError):
    """Base class for recoverable failures during scope extraction."""


class LLMUnavailableError(ScopeExtractionError):
    """LLM call failed or returned unparseable output."""
