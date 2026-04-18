"""Prompt building and LLM call for scope extraction.

``_call_llm`` is the single swap point — tests patch
``services.audits.scope_extraction._llm._call_llm`` to intercept calls.
"""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path
from typing import Final

from ._errors import LLMUnavailableError
from ._locate import ScopeSection

PROMPT_VERSION: Final[str] = "scope-v1"

# Cap on prompt payload. Scope sections are ~10 KB normally; 40 KB
# protects against a degenerate slice.
_MAX_SCOPE_TEXT_CHARS: Final[int] = 40_000


_SCOPE_PROMPT_TEMPLATE = """\
You are extracting the list of contracts that were in scope for a smart-contract security audit.

Audit title: {title}
Auditor: {auditor}

Below is the scope section(s) from the audit report PDF. Different auditors \
use different formats (markdown tables, bulleted URL lists, line-count \
tables, flat src/ trees, prose enumeration) — extract every contract \
regardless of format.

Rules:
- Return a JSON array of contract names ONLY. No prose, no file paths, no \
explanations.
- Contract names are the basenames of .sol / .vy files WITHOUT the extension, \
e.g. "MorphoBlue", "Pool", "BundlerV3".
- Deduplicate names. If the same contract appears in multiple repos or \
across audit phases, include it once.
- EXCLUDE test files (Test*, *Test, *.t.sol), mocks (Mock*, *Mock), \
deployment scripts (Deploy*, *.s.sol), and anything explicitly marked \
"out of scope" or "reference only".

Avoid these specific false positives that trip up extraction:
- Do NOT treat project names, product names, or section headers as \
contract names. "EtherFi RewardsManager" as a section title is not the \
same as a RewardsManager.sol file being audited.
- Do NOT include EXTERNAL dependencies described in a "System Overview" \
or architecture-background section — these are typically contracts the \
protocol INTEGRATES with (e.g. "Hyperliquid's CoreWriter at 0x3333..."), \
not audit targets.
- If a contract is mentioned ONLY in a single finding (e.g. \
"Issue L-01: BeaconFactory is vulnerable") without also appearing in a \
scope list or tree, it's probably not in scope — but if it IS in a scope \
list AND discussed in findings, include it normally.

Include these as in-scope when they appear:
- Contracts listed in an explicit scope section, audited-files table, \
"Program:" declaration, "Target:" field, flat src/ tree printout, or \
a bulleted contract list.
- Contracts the audit clearly reviewed even if the structural header is \
missing — a flat listing of .sol filenames IS a scope signal.

If no contracts can be identified from the text, return an empty array [].

Scope section text:
---
{scope_text}
---

Respond with the JSON array only."""


def _build_prompt(sections: list[ScopeSection], title: str, auditor: str) -> str:
    joined = "\n\n===\n\n".join(s.text_slice for s in sections)
    if len(joined) > _MAX_SCOPE_TEXT_CHARS:
        joined = joined[:_MAX_SCOPE_TEXT_CHARS]
    return _SCOPE_PROMPT_TEMPLATE.format(
        title=title or "(unknown)",
        auditor=auditor or "(unknown)",
        scope_text=joined,
    )


def _call_llm(prompt: str) -> tuple[str, str]:
    """Call the LLM, returning ``(response_text, model_identifier)``.

    When ``PSAT_LLM_STUB_DIR`` is set, routes to fixture files keyed by the
    SHA-256 of the prompt, falling back to ``_default.json``. Lets
    integration tests run deterministically without OpenRouter.
    """
    stub_dir = os.environ.get("PSAT_LLM_STUB_DIR")
    if stub_dir:
        digest = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        base = Path(stub_dir)
        specific = base / f"{digest}.json"
        if specific.exists():
            return specific.read_text(), f"stub:{digest[:12]}"
        default = base / "_default.json"
        if default.exists():
            return default.read_text(), "stub:_default"
        raise LLMUnavailableError(f"no LLM stub for prompt digest {digest} in {stub_dir}")

    try:
        from utils.llm import openrouter
    except Exception as exc:  # pragma: no cover - dep configured in pyproject
        raise LLMUnavailableError(f"openrouter client unavailable: {exc}") from exc

    model = os.environ.get("PSAT_SCOPE_LLM_MODEL", "google/gemini-2.0-flash-001")
    try:
        response = openrouter.chat(
            [{"role": "user", "content": prompt}],
            model=model,
            max_tokens=2048,
            temperature=0.0,
        )
    except Exception as exc:
        raise LLMUnavailableError(f"LLM call failed: {exc}") from exc
    return response, model


def extract_scope_with_llm(sections: list[ScopeSection], title: str, auditor: str) -> tuple[list[str], str, str]:
    """Call the LLM for the scope list. Returns ``(names, raw_response, model)``.

    Raises ``LLMUnavailableError`` on call failure or unparseable output.
    Hallucination filtering happens later in ``validate_contracts``.
    """
    from services.discovery.audit_reports_llm import _parse_json_array

    prompt = _build_prompt(sections, title, auditor)
    response, model = _call_llm(prompt)
    parsed = _parse_json_array(response)
    if parsed is None:
        raise LLMUnavailableError(f"LLM returned unparseable output: {response[:200]!r}")

    names: list[str] = []
    seen: set[str] = set()
    for item in parsed:
        if isinstance(item, str):
            candidate = item.strip()
        elif isinstance(item, dict):
            # Tolerate drift where the model returns [{name: ...}, ...].
            raw = item.get("contract_name") or item.get("name") or item.get("file")
            candidate = str(raw).strip() if raw else ""
        else:
            continue
        if not candidate:
            continue
        # Drop the .sol/.vy extension if the model leaked it in.
        stem = re.sub(r"\.(?:sol|vy)$", "", candidate, flags=re.IGNORECASE)
        key = stem.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(stem)
    return names, response, model
