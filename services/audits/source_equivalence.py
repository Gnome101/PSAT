"""Prove an audit reviewed the code currently deployed at an impl address.

Forensic complement to temporal matching: if the audit PDF mentions commit
X and commit X's source is byte-identical to the impl's Etherscan-verified
source, the audit's coverage of that impl is proven. Source-text equality
is sufficient — no compilation step needed.

Impl source: DB ``SourceFile`` rows first (populated by the static worker),
Etherscan ``getsourcecode`` fallback. Audit source: GitHub raw, keyed on
``source_repo`` + ``reviewed_commits``.

Per-pair flow inside ``check_audit_covers_impl``: for each (commit, scope
name) × each candidate source path, compare SHA-256. One match proves
coverage. In-process ``lru_cache`` covers the hot path; persistent caching
is future work.
"""

from __future__ import annotations

import functools
import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Reviewed-commit extraction
# ---------------------------------------------------------------------------


# 7-40 char hex tokens: 7 is git's abbrev default, 40 is a full SHA.
_HEX_TOKEN_RE = re.compile(r"\b([0-9a-f]{7,40})\b", re.IGNORECASE)


def extract_reviewed_commits(text: str) -> list[str]:
    """Pull commit-SHA-like hex tokens from audit PDF text.

    Deduped, lowercased, first-seen order. Pure-digit tokens (block
    numbers) and all-same-char tokens (``0000000``, ``ffffffff``) are
    rejected as noise. No GitHub validation — the caller decides.
    """
    if not text:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for m in _HEX_TOKEN_RE.finditer(text):
        token = m.group(1).lower()
        # Require at least one hex letter so we don't catch block numbers,
        # issue IDs, etc. that happen to be 7+ chars of digits.
        if not any(c in "abcdef" for c in token):
            continue
        # Reject all-same-char tokens (0000000, aaaaaaa, ffffffff) — common
        # padding / placeholder strings.
        if len(set(token)) < 3:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


# ---------------------------------------------------------------------------
# Etherscan source fetch
# ---------------------------------------------------------------------------


def _hash_source_text(text: str) -> str:
    """Stable hash of a source file's content for equality comparison."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class VerifiedSource:
    """Parsed Etherscan verified-source response for one address."""

    contract_name: str | None
    compiler_version: str | None
    files: dict[str, str]  # path -> sha256(content)


def fetch_etherscan_source_files(address: str) -> VerifiedSource | None:
    """Return parsed verified-source for ``address`` as file-path→sha256.

    Delegates parsing to ``services.discovery.fetch.parse_sources`` — the
    same function the discovery worker uses when persisting source to the
    DB. This keeps path normalization consistent across the two paths, so
    a GitHub lookup against ``src/LiquidityPool.sol`` matches whether the
    verified source came from the DB or an Etherscan fallback here.

    Returns None if Etherscan doesn't have verified source. Uses the shared
    ``utils.etherscan.get`` cache, so repeated calls within a run cost one
    API request only the first time.
    """
    from services.discovery.fetch import parse_sources
    from utils.etherscan import get

    try:
        data = get("contract", "getsourcecode", address=address)
        result = data["result"][0]
    except Exception:
        return None

    contract_name = (result.get("ContractName") or "").strip() or None
    compiler_version = (result.get("CompilerVersion") or "").strip() or None
    files = parse_sources(result)
    if not files:
        return None
    hashed = {path: _hash_source_text(content) for path, content in files.items()}
    return VerifiedSource(
        contract_name=contract_name,
        compiler_version=compiler_version,
        files=hashed,
    )


def fetch_db_source_files(session: Any, contract_id: int) -> VerifiedSource | None:
    """Return a Contract's verified source from ``SourceFile`` rows.

    Reuses what the discovery worker already persisted (keyed on
    ``Contract.job_id``) to avoid an Etherscan round-trip. Returns ``None``
    when the contract hasn't been analyzed yet — caller falls back to
    ``fetch_etherscan_source_files``.
    """
    from db.models import Contract
    from db.queue import get_source_files

    contract = session.get(Contract, contract_id)
    if contract is None or contract.job_id is None:
        return None
    try:
        files = get_source_files(session, contract.job_id)
    except Exception:
        logger.exception("DB source file fetch failed for contract %s", contract_id)
        return None
    if not files:
        return None
    hashed = {path: _hash_source_text(content) for path, content in files.items()}
    return VerifiedSource(
        contract_name=contract.contract_name,
        compiler_version=contract.compiler_version,
        files=hashed,
    )


def fetch_contract_source_files(session: Any, contract_id: int) -> VerifiedSource | None:
    """DB-first resolver: try ``SourceFile`` rows, fall back to Etherscan.

    The preferred entry point for matchers — saves Etherscan traffic when
    the impl has already been through the static pipeline. For orphan
    impls (historical, never analyzed), transparently calls Etherscan.
    """
    from db.models import Contract

    db_source = fetch_db_source_files(session, contract_id)
    if db_source is not None:
        return db_source
    # Fall back: fetch via Etherscan using the Contract's address.
    contract = session.get(Contract, contract_id)
    if contract is None or not contract.address:
        return None
    return fetch_etherscan_source_files(contract.address)


# ---------------------------------------------------------------------------
# GitHub source fetch
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=4096)
def _fetch_github_raw(url: str, token: str | None) -> str | None:
    headers = {"User-Agent": "PSAT-source-equivalence/0.1"}
    if token:
        headers["Authorization"] = f"token {token}"
    try:
        r = requests.get(url, headers=headers, timeout=15)
    except requests.RequestException as exc:
        logger.warning("github raw fetch failed for %s: %s", url, exc)
        return None
    if r.status_code != 200:
        return None
    # Reject likely binary or huge responses — source files are plain text
    # and shouldn't exceed a few hundred KB. This guards against a repo
    # path collision with a PDF or similar.
    if "content-type" in r.headers and "text" not in r.headers["content-type"].lower():
        if "application/octet-stream" not in r.headers["content-type"].lower():
            return None
    if len(r.content) > 5 * 1024 * 1024:
        return None
    return r.text


def fetch_github_source_hash(repo: str, commit: str, path: str, *, token: str | None = None) -> str | None:
    """Hash the file at ``github.com/<repo>/<commit>/<path>``.

    Returns None when the file doesn't exist at that commit (404) or the
    fetch fails. Same hash function as ``_hash_source_text`` so an
    Etherscan-side hash can be compared directly.
    """
    if not (repo and commit and path):
        return None
    url = f"https://raw.githubusercontent.com/{repo}/{commit}/{path}"
    content = _fetch_github_raw(url, token)
    if content is None:
        return None
    return _hash_source_text(content)


# ---------------------------------------------------------------------------
# Candidate path generation
# ---------------------------------------------------------------------------


def _candidate_paths_for_name(name: str, etherscan_paths: list[str]) -> list[str]:
    """Paths in Etherscan's source that plausibly correspond to ``name``.

    Prefers Etherscan paths verbatim (they carry the project's actual
    layout); falls back to conventional ``src/`` / ``contracts/`` when the
    bundle doesn't include a matching name (flattened verification).
    """
    name_lc = name.lower()
    matches = [p for p in etherscan_paths if p.rsplit("/", 1)[-1].lower() in (f"{name_lc}.sol", f"{name_lc}.vy")]
    if matches:
        return matches
    return [f"src/{name}.sol", f"contracts/{name}.sol"]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EquivalenceMatch:
    """Proof that audit at commit X reviewed the source file at path Y."""

    commit: str
    scope_name: str
    etherscan_path: str
    source_sha256: str


def check_audit_covers_impl(
    *,
    reviewed_commits: list[str],
    scope_contracts: list[str],
    impl_source: VerifiedSource,
    source_repo: str | None,
    github_token: str | None = None,
) -> list[EquivalenceMatch]:
    """Find every (commit, scope_name, path) triple with byte-identical source.

    Empty list = no overlap proven. Multiple entries = same file across
    several commits, or several files at one commit — preserved so the
    UI can show "audit @ commit abc covers LiquidityPool.sol + …".
    Bails when ``reviewed_commits`` / ``source_repo`` / ``impl_source.files``
    is missing.
    """
    if not reviewed_commits or not source_repo or not impl_source.files:
        return []
    if not scope_contracts:
        return []

    etherscan_paths = list(impl_source.files.keys())
    matches: list[EquivalenceMatch] = []

    for commit in reviewed_commits:
        for name in scope_contracts:
            for path in _candidate_paths_for_name(name, etherscan_paths):
                etherscan_hash = impl_source.files.get(path)
                if not etherscan_hash:
                    # Path doesn't exist in Etherscan's bundle — skip.
                    continue
                github_hash = fetch_github_source_hash(source_repo, commit, path, token=github_token)
                if github_hash is None:
                    continue
                if github_hash == etherscan_hash:
                    matches.append(
                        EquivalenceMatch(
                            commit=commit,
                            scope_name=name,
                            etherscan_path=path,
                            source_sha256=etherscan_hash,
                        )
                    )
    return matches


def check_audit_row_covers_contract(
    session: Any,
    audit_id: int,
    contract_id: int,
    *,
    github_token: str | None = None,
) -> list[EquivalenceMatch]:
    """DB-bound wrapper: fetch both rows, pull reviewed_commits + scope +
    source_repo from the audit, resolve impl source (DB-preferred,
    Etherscan fallback), run ``check_audit_covers_impl``.

    Returns ``[]`` when inputs are insufficient. Never raises on network
    failure — treats unreachable remote as "no proof" (= empty list).
    """
    from db.models import AuditReport, Contract

    audit = session.get(AuditReport, audit_id)
    contract = session.get(Contract, contract_id)
    if audit is None or contract is None:
        return []

    commits = list(audit.reviewed_commits or [])
    scope = list(audit.scope_contracts or [])
    repo = audit.source_repo
    if not (commits and scope and repo and contract.address):
        return []

    impl_source = fetch_contract_source_files(session, contract_id)
    if impl_source is None:
        return []

    return check_audit_covers_impl(
        reviewed_commits=commits,
        scope_contracts=scope,
        impl_source=impl_source,
        source_repo=repo,
        github_token=github_token,
    )
