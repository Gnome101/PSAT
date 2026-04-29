"""
Pydantic models for the docs discovery pipeline.

These flow through the pipeline in this order:
  SearchResult  (raw Tavily output, normalised)
      ↓
  RelevanceCheck  (cheap LLM pre-filter)
      ↓
  ExtractedSignals  (full LLM extraction)
      ↓
  DiscoveredSeed  (typed entry for the SeedManifest)
  DiscoveredDocument  (full record written to the DB)
"""
from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Tavily layer
# ---------------------------------------------------------------------------

class SearchResult(BaseModel):
    """
    Normalised from a single raw Tavily result dict.

    Raw Tavily fields we care about:
      result["url"]            → url
      result["raw_content"]    → content  (full page text when include_raw_content=True)
      result["score"]          → score
    The source_type and query_used are stamped by the searcher, not Tavily.
    """
    url: str
    content: str
    score: float
    source_type: str        # "docs" | "github" | "governance"
    query_used: str


# ---------------------------------------------------------------------------
# OpenRouter / LLM layer
# ---------------------------------------------------------------------------

class RelevanceCheck(BaseModel):
    """
    Output of the cheap relevance pre-filter LLM call.
    Model is prompted to return exactly this JSON shape.
    """
    is_relevant: bool
    confidence: float       # 0.0 – 1.0
    reason: str             # one-line explanation


class ExtractedSignals(BaseModel):
    """
    Structured output from the full extraction LLM call.
    All fields are optional — the model may not find every signal.
    """
    doc_type: str                           # "docs_page" | "github_readme" | "governance_post"
    summary: str                            # 2-3 sentence plain-English summary
    is_security_relevant: bool

    # Access control
    admin_roles: list[str] = Field(default_factory=list)
    privileged_functions: list[str] = Field(default_factory=list)

    # Upgrade
    upgrade_pattern: str | None = None      # "UUPS" | "Transparent Proxy" | "Beacon" | None
    timelock_delay: str | None = None       # e.g. "48 hours"

    # Pause
    is_pausable: bool | None = None
    pause_controllers: list[str] = Field(default_factory=list)

    # Anything that doesn't fit the above
    other_signals: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Seed manifest layer
# ---------------------------------------------------------------------------

class DiscoveredSeed(BaseModel):
    """
    A single verified URL that a downstream crawler should target.
    seed_type must match a key in services/crawlers/registry.py.
    """
    url: str
    seed_type: str          # "docs_site" | "github_org" | "governance"
    confidence: float       # inherited from Tavily score
    protocol_id: str
    discovered_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict = Field(default_factory=dict)


class SeedManifest(BaseModel):
    """
    Full discovery output for one protocol.
    Persisted to DB and consumed by docs_discovery_worker.
    """
    protocol_id: str
    protocol_name: str
    contract_address: str
    seeds: list[DiscoveredSeed]
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Final DB record
# ---------------------------------------------------------------------------

class DiscoveredDocument(BaseModel):
    """
    Written to the documents table after extraction.
    Combines the raw Tavily content with OpenRouter signals.
    """
    protocol_id: str
    source_url: str
    doc_type: str
    raw_text: str
    content_hash: str
    title: str | None = None
    tavily_score: float | None = None
    signals: ExtractedSignals | None = None
    discovered_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict = Field(default_factory=dict)
