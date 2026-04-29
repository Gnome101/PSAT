"""
DocsExtractor
=============
PSAT-specific LLM prompts and JSON parsing built on top of utils.llm.chat().

utils/llm.py already handles:
  - Auth (OPEN_ROUTER_KEY from .env)
  - Streaming SSE collection
  - Default model (gemini-2.0-flash-001)
  - Arbitrary model override via model= kwarg

This module only adds:
  - System prompts per task type
  - JSON response parsing and validation
  - Model routing: cheap model for classification, capable model for extraction
  - Token budget enforcement before sending content
"""
from __future__ import annotations

import json
import logging
import re

from utils.llm import chat
from services.discovery.docs.models import ExtractedSignals, RelevanceCheck

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model routing
# ---------------------------------------------------------------------------

# Cheap fast model for binary classification (relevance check, doc type)
CLASSIFIER_MODEL = "google/gemini-2.0-flash-001"

# Capable model for full structured extraction
EXTRACTOR_MODEL = "anthropic/claude-sonnet-4-5"

# ---------------------------------------------------------------------------
# Token budget
# ---------------------------------------------------------------------------

# Rough limit before sending content to the extractor model.
# ~4 chars/token is a conservative estimate.
MAX_CONTENT_CHARS = 16_000      # ~4,000 tokens

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

RELEVANCE_SYSTEM = """
You are a security researcher evaluating web pages for a DeFi protocol analysis tool.
Determine whether a page is an official or directly relevant resource for the given protocol.
Respond ONLY with a JSON object — no markdown fences, no extra text:
{"is_relevant": true/false, "confidence": 0.0-1.0, "reason": "one sentence"}
""".strip()

EXTRACTION_SYSTEM = """
You are a smart contract security analyst.
Extract structured security signals from the provided documentation.
Focus on: admin roles, upgrade patterns, timelocks, pause mechanisms.
Respond ONLY with a valid JSON object matching the schema given in the user message.
No markdown fences. No text outside the JSON object.
""".strip()

EXTRACTION_SCHEMA = """
{
  "doc_type": "docs_page | github_readme | governance_post",
  "summary": "2-3 sentence plain English summary",
  "is_security_relevant": true/false,
  "admin_roles": ["list of role names found in text"],
  "privileged_functions": ["list of function names"],
  "upgrade_pattern": "UUPS | Transparent Proxy | Beacon | null",
  "timelock_delay": "e.g. 48 hours | null",
  "is_pausable": true/false/null,
  "pause_controllers": ["list of role names or addresses"],
  "other_signals": ["any other security-relevant observations"]
}
""".strip()


class DocsExtractor:
    """
    Two-stage LLM processing for each SearchResult:
      Stage 1: is_relevant()    — cheap model, yes/no filter
      Stage 2: extract_signals() — capable model, full structured extraction
    """

    def is_relevant(self, content: str, protocol_name: str) -> RelevanceCheck:
        """
        Cheap pre-filter. Calls utils.llm.chat() with CLASSIFIER_MODEL.
        Returns a RelevanceCheck — callers should check .is_relevant and
        .confidence before proceeding to full extraction.

        Uses only the first 500 chars of content — enough for classification,
        cheap on tokens.
        """
        preview = content[:500]
        messages = [
            {"role": "system", "content": RELEVANCE_SYSTEM},
            {
                "role": "user",
                "content": (
                    f"Protocol: {protocol_name}\n\n"
                    f"Page content preview:\n{preview}\n\n"
                    f"Is this an official or directly relevant resource for {protocol_name}?"
                ),
            },
        ]
        try:
            raw = chat(messages, model=CLASSIFIER_MODEL)
            parsed = self._parse_json(raw)
            return RelevanceCheck(**parsed)
        except Exception as exc:
            logger.warning(
                "[extractor] is_relevant parse failed for protocol %r: %s",
                protocol_name,
                exc,
            )
            return RelevanceCheck(
                is_relevant=False,
                confidence=0.0,
                reason="parse error",
            )

    def extract_signals(self, content: str, doc_type_hint: str) -> ExtractedSignals:
        """
        Full structured extraction. Calls utils.llm.chat() with EXTRACTOR_MODEL.
        Only called after is_relevant() returns True.

        Content is truncated to MAX_CONTENT_CHARS before sending.
        """
        truncated = content[:MAX_CONTENT_CHARS]
        messages = [
            {"role": "system", "content": EXTRACTION_SYSTEM},
            {
                "role": "user",
                "content": (
                    f"Document type hint: {doc_type_hint}\n\n"
                    f"Document content:\n{truncated}\n\n"
                    f"Extract security signals using this exact JSON schema:\n{EXTRACTION_SCHEMA}"
                ),
            },
        ]
        try:
            raw = chat(messages, model=EXTRACTOR_MODEL)
            parsed = self._parse_json(raw)
            return ExtractedSignals(**parsed)
        except Exception as exc:
            logger.warning(
                "[extractor] extract_signals failed (hint=%r): %s",
                doc_type_hint,
                exc,
            )
            # Return a minimal valid object so the document is still stored
            # with whatever Tavily fetched — the signals fields will be empty
            # but the raw_text remains useful for manual review and search.
            return ExtractedSignals(
                doc_type=doc_type_hint,
                summary="",
                is_security_relevant=False,
            )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_json(self, raw_text: str) -> dict:
        """
        Strips markdown code fences that models sometimes wrap JSON in,
        then parses the result.

        Models often return:
          ```json
          { ... }
          ```
        even when instructed not to. Strip the fences before json.loads().

        Raises ValueError if the cleaned string cannot be parsed as JSON.
        Logs raw_text[:500] before raising so failures are debuggable
        without flooding logs with full page content.
        """
        # Strip ```json ... ``` or ``` ... ``` wrappers.
        # The pattern matches an optional "json" language tag after the opening fence.
        cleaned = re.sub(r"```(?:json)?\s*", "", raw_text)
        cleaned = re.sub(r"```", "", cleaned).strip()

        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as exc:
            logger.warning(
                "[extractor] JSON parse failed. Raw response (first 500 chars):\n%s",
                raw_text[:500],
            )
            raise ValueError(f"Could not parse LLM response as JSON: {exc}") from exc
