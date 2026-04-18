"""Unit tests for ``services.audits.source_equivalence`` internals.

The DB-integrated behaviours (coverage matcher upgrading via
``check_audit_row_covers_contract``) are covered in
``test_audit_coverage.py``. What lives here is the network-side contract
surface: Etherscan verified-source parsing, GitHub raw fetch guards,
candidate-path generation, and the zero-input short-circuits on the
``check_audit_row_covers_contract`` entry point. No DB, no network — we
stub ``requests.get`` and ``utils.etherscan.get`` at module scope.
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.audits import source_equivalence  # noqa: E402
from services.audits.source_equivalence import (  # noqa: E402
    EquivalenceMatch,
    VerifiedSource,
    _candidate_paths_for_name,
    _fetch_github_raw,
    _hash_source_text,
    check_audit_covers_impl,
    check_audit_row_covers_contract,
    extract_reviewed_commits,
    fetch_db_source_files,
    fetch_etherscan_source_files,
    fetch_github_source_hash,
)


@pytest.fixture(autouse=True)
def _clear_lru_cache():
    """``_fetch_github_raw`` caches at module scope — stale cache hits
    would poison tests that stub ``requests.get`` for the same URL."""
    _fetch_github_raw.cache_clear()
    yield
    _fetch_github_raw.cache_clear()


# ---------------------------------------------------------------------------
# extract_reviewed_commits — filter rules beyond what test_audit_coverage covers
# ---------------------------------------------------------------------------


class TestExtractReviewedCommitsFilters:
    def test_rejects_token_with_fewer_than_three_unique_chars(self):
        """Tokens like ``ababab`` (2 unique chars) or ``cdcdcdcd`` (2 unique)
        are noise — usually incidental 7+ char alternations that happen to
        pass the hex-letter check. Covers the ``len(set(token)) < 3`` guard
        that the all-digit test doesn't hit."""
        # "abababab" — 2 unique chars, passes the hex-letter check ('a','b').
        assert extract_reviewed_commits("noise abababab more") == []
        # Mix real + noisy so we prove only the noisy one gets rejected.
        assert extract_reviewed_commits("noise abababab real 1a2b3c4d") == ["1a2b3c4d"]

    def test_dedupes_repeat_occurrences(self):
        """A SHA mentioned twice in the text must appear once in the output,
        in first-seen position."""
        text = "commit 1a2b3c4d\nseen again 1a2b3c4d\nalso deadbeefcafe01"
        assert extract_reviewed_commits(text) == ["1a2b3c4d", "deadbeefcafe01"]


# ---------------------------------------------------------------------------
# fetch_etherscan_source_files — happy path, empty, and Etherscan failure
# ---------------------------------------------------------------------------


class TestFetchEtherscanSourceFiles:
    """The ``services.discovery`` package re-exports ``fetch`` (function)
    into its namespace, shadowing the submodule — so we patch via the
    submodule object loaded through ``importlib``. ``utils.etherscan.get``
    is similarly patched at the submodule level for consistency."""

    def test_returns_verified_source_for_successful_getsourcecode(self, monkeypatch):
        import importlib

        content = "contract LiquidityPool {}"
        captured = {
            "result": [
                {
                    "ContractName": "LiquidityPool",
                    "CompilerVersion": "v0.8.27+commit.40a35a09",
                    "SourceCode": content,
                }
            ]
        }
        fetch_module = importlib.import_module("services.discovery.fetch")
        etherscan_module = importlib.import_module("utils.etherscan")
        monkeypatch.setattr(etherscan_module, "get", lambda *_a, **_k: captured)
        monkeypatch.setattr(fetch_module, "parse_sources", lambda _res: {"LiquidityPool.sol": content})

        got = fetch_etherscan_source_files("0x" + "a" * 40)
        assert got is not None
        assert got.contract_name == "LiquidityPool"
        assert got.compiler_version == "v0.8.27+commit.40a35a09"
        assert got.files == {"LiquidityPool.sol": _hash_source_text(content)}

    def test_returns_none_when_parse_sources_empty(self, monkeypatch):
        """Unverified contracts come back with no parseable source — caller
        (coverage matcher) treats as no-equivalence rather than crashing."""
        import importlib

        fetch_module = importlib.import_module("services.discovery.fetch")
        etherscan_module = importlib.import_module("utils.etherscan")
        monkeypatch.setattr(
            etherscan_module,
            "get",
            lambda *_a, **_k: {"result": [{"ContractName": "", "CompilerVersion": "", "SourceCode": ""}]},
        )
        monkeypatch.setattr(fetch_module, "parse_sources", lambda _res: {})
        assert fetch_etherscan_source_files("0x" + "a" * 40) is None

    def test_returns_none_when_etherscan_raises(self, monkeypatch):
        """Rate limits, network errors, malformed responses — any exception
        from Etherscan gets swallowed so the matcher's per-pair loop can
        continue without poisoning the whole audit."""
        import importlib

        def boom(*_a, **_k):
            raise RuntimeError("etherscan down")

        etherscan_module = importlib.import_module("utils.etherscan")
        monkeypatch.setattr(etherscan_module, "get", boom)
        assert fetch_etherscan_source_files("0x" + "a" * 40) is None

    def test_strips_blank_metadata_to_none(self, monkeypatch):
        """Whitespace-only ContractName/CompilerVersion fields are normalized
        to None so downstream checks (``if source.contract_name``) work."""
        import importlib

        fetch_module = importlib.import_module("services.discovery.fetch")
        etherscan_module = importlib.import_module("utils.etherscan")
        monkeypatch.setattr(
            etherscan_module,
            "get",
            lambda *_a, **_k: {
                "result": [
                    {
                        "ContractName": "   ",
                        "CompilerVersion": "",
                        "SourceCode": "contract X {}",
                    }
                ]
            },
        )
        monkeypatch.setattr(
            fetch_module,
            "parse_sources",
            lambda _res: {"X.sol": "contract X {}"},
        )
        got = fetch_etherscan_source_files("0x" + "a" * 40)
        assert got is not None
        assert got.contract_name is None
        assert got.compiler_version is None


# ---------------------------------------------------------------------------
# fetch_db_source_files — DB-lookup helper (no Etherscan call)
# ---------------------------------------------------------------------------


class TestFetchDbSourceFilesShortCircuits:
    def test_returns_none_when_contract_missing(self):
        """Session.get returning None means the contract doesn't exist —
        the resolver must return None without attempting a source lookup."""
        session = MagicMock()
        session.get.return_value = None
        assert fetch_db_source_files(session, 999) is None

    def test_returns_none_when_contract_has_no_job_id(self):
        """Contract exists but was never analyzed (job_id is NULL) — no
        SourceFile rows to read, so the caller falls back to Etherscan."""
        session = MagicMock()
        contract = MagicMock()
        contract.job_id = None
        session.get.return_value = contract
        assert fetch_db_source_files(session, 1) is None

    def test_returns_none_when_get_source_files_raises(self, monkeypatch):
        """DB errors during source-file fetch must not bubble — the matcher
        should degrade gracefully to the Etherscan fallback."""
        import importlib

        session = MagicMock()
        contract = MagicMock()
        contract.job_id = "job-id"
        session.get.return_value = contract

        def boom(*_a, **_k):
            raise RuntimeError("DB gone")

        queue_module = importlib.import_module("db.queue")
        monkeypatch.setattr(queue_module, "get_source_files", boom)
        assert fetch_db_source_files(session, 1) is None

    def test_returns_none_when_no_source_files_rows(self, monkeypatch):
        """Job completed but somehow no SourceFile rows exist — same result
        as ``job_id=None``: punt to Etherscan."""
        import importlib

        session = MagicMock()
        contract = MagicMock()
        contract.job_id = "job-id"
        session.get.return_value = contract
        queue_module = importlib.import_module("db.queue")
        monkeypatch.setattr(queue_module, "get_source_files", lambda *_a, **_k: {})
        assert fetch_db_source_files(session, 1) is None


# ---------------------------------------------------------------------------
# _fetch_github_raw — HTTP contract boundaries (LRU cache cleared per test)
# ---------------------------------------------------------------------------


def _resp(
    *,
    status_code: int = 200,
    text: str = "",
    content_type: str = "text/plain",
    content_bytes: bytes | None = None,
) -> MagicMock:
    r = MagicMock()
    r.status_code = status_code
    r.text = text
    r.content = content_bytes if content_bytes is not None else text.encode("utf-8")
    r.headers = {"content-type": content_type}
    return r


class TestFetchGithubRaw:
    def test_non_200_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            "services.audits.source_equivalence.requests.get",
            lambda *_a, **_k: _resp(status_code=404, text="Not Found"),
        )
        assert _fetch_github_raw("https://raw.githubusercontent.com/x/y/abc/file.sol", None) is None

    def test_network_exception_returns_none(self, monkeypatch):
        def raising(*_a, **_kw):
            raise requests.ConnectionError("timeout")

        monkeypatch.setattr("services.audits.source_equivalence.requests.get", raising)
        assert _fetch_github_raw("https://raw.githubusercontent.com/x/y/abc/file.sol", None) is None

    def test_binary_content_type_rejected(self, monkeypatch):
        """A repo that returned an image/pdf at the conventional path would
        poison hashes if we accepted it — the content-type check catches
        that without parsing the body."""
        monkeypatch.setattr(
            "services.audits.source_equivalence.requests.get",
            lambda *_a, **_k: _resp(content_type="image/png", text="binary"),
        )
        assert _fetch_github_raw("https://raw.githubusercontent.com/x/y/abc/file.sol", None) is None

    def test_octet_stream_content_type_accepted(self, monkeypatch):
        """Some raw-content CDNs serve source as ``application/octet-stream``;
        the guard explicitly allows it so we don't false-negative on them."""
        monkeypatch.setattr(
            "services.audits.source_equivalence.requests.get",
            lambda *_a, **_k: _resp(content_type="application/octet-stream", text="contract X {}"),
        )
        assert _fetch_github_raw("https://raw.githubusercontent.com/x/y/abc/file.sol", None) == "contract X {}"

    def test_oversized_body_rejected(self, monkeypatch):
        """A 6MB response almost certainly isn't a single Solidity file —
        reject to keep the hot path from spending memory on junk."""
        big = b"x" * (6 * 1024 * 1024)
        monkeypatch.setattr(
            "services.audits.source_equivalence.requests.get",
            lambda *_a, **_k: _resp(content_type="text/plain", content_bytes=big, text="x" * 10),
        )
        assert _fetch_github_raw("https://raw.githubusercontent.com/x/y/abc/file.sol", None) is None

    def test_authorization_header_set_when_token_provided(self, monkeypatch):
        """Private repos require ``token ghp_...``. Verify the header
        actually reaches requests.get so rate-limit-evaders work."""
        captured = {}

        def capture(url, headers=None, timeout=None):  # noqa: ARG001
            captured.update({"url": url, "headers": headers or {}})
            return _resp(text="contract X {}")

        monkeypatch.setattr("services.audits.source_equivalence.requests.get", capture)
        _fetch_github_raw("https://example/file.sol", "secret-token")
        assert captured["headers"].get("Authorization") == "token secret-token"


class TestFetchGithubSourceHash:
    def test_returns_none_on_missing_inputs(self):
        """The wrapper short-circuits without any HTTP call when any of repo,
        commit, or path is empty — lets callers pass through optional
        fields without an explicit guard."""
        assert fetch_github_source_hash("", "abc", "file.sol") is None
        assert fetch_github_source_hash("r/n", "", "file.sol") is None
        assert fetch_github_source_hash("r/n", "abc", "") is None

    def test_returns_sha256_when_content_fetched(self, monkeypatch):
        content = "contract Pool { function f() {} }"
        monkeypatch.setattr(
            "services.audits.source_equivalence.requests.get",
            lambda *_a, **_k: _resp(text=content, content_type="text/plain"),
        )
        assert fetch_github_source_hash("r/n", "abc1234", "src/Pool.sol") == _hash_source_text(content)

    def test_returns_none_when_raw_returns_none(self, monkeypatch):
        """404 on the raw fetch propagates as None through the wrapper —
        so matchers can distinguish "file missing" from "hash mismatch"."""
        monkeypatch.setattr(
            "services.audits.source_equivalence.requests.get",
            lambda *_a, **_k: _resp(status_code=404),
        )
        assert fetch_github_source_hash("r/n", "abc1234", "src/Pool.sol") is None


# ---------------------------------------------------------------------------
# _candidate_paths_for_name — Etherscan-first, conventional fallback
# ---------------------------------------------------------------------------


class TestCandidatePathsForName:
    def test_prefers_matching_etherscan_paths(self):
        """When the bundle contains the basename verbatim, return THOSE
        paths — they reflect the project's actual layout and the GitHub
        fetch will succeed against them."""
        paths = ["contracts/pool/MyPool.sol", "contracts/utils/Other.sol"]
        assert _candidate_paths_for_name("MyPool", paths) == ["contracts/pool/MyPool.sol"]

    def test_falls_back_to_conventional_paths_when_no_etherscan_match(self):
        """Flattened verifications (Etherscan collapses everything into one
        ``Contract.sol`` file) don't carry the real tree — try ``src/``
        and ``contracts/`` which cover the vast majority of projects."""
        assert _candidate_paths_for_name("Vault", []) == ["src/Vault.sol", "contracts/Vault.sol"]

    def test_matches_vyper_files(self):
        """``.vy`` is the other accepted extension — covered so Curve-style
        repos don't false-negative."""
        paths = ["src/pool.vy"]
        assert _candidate_paths_for_name("pool", paths) == ["src/pool.vy"]


# ---------------------------------------------------------------------------
# check_audit_covers_impl — empty-input short-circuits
# ---------------------------------------------------------------------------


class TestCheckAuditCoversImplShortCircuits:
    def _src(self, files):
        return VerifiedSource(contract_name="X", compiler_version="v0.8", files=files)

    def test_empty_reviewed_commits_returns_empty(self):
        assert (
            check_audit_covers_impl(
                reviewed_commits=[],
                scope_contracts=["Pool"],
                impl_source=self._src({"src/Pool.sol": "abc"}),
                source_repo="r/n",
            )
            == []
        )

    def test_missing_source_repo_returns_empty(self):
        """No repo = no URL to fetch from = can't prove equivalence."""
        assert (
            check_audit_covers_impl(
                reviewed_commits=["abc1234"],
                scope_contracts=["Pool"],
                impl_source=self._src({"src/Pool.sol": "abc"}),
                source_repo=None,
            )
            == []
        )

    def test_impl_source_with_no_files_returns_empty(self):
        assert (
            check_audit_covers_impl(
                reviewed_commits=["abc1234"],
                scope_contracts=["Pool"],
                impl_source=self._src({}),
                source_repo="r/n",
            )
            == []
        )

    def test_empty_scope_contracts_returns_empty(self):
        assert (
            check_audit_covers_impl(
                reviewed_commits=["abc1234"],
                scope_contracts=[],
                impl_source=self._src({"src/Pool.sol": "abc"}),
                source_repo="r/n",
            )
            == []
        )

    def test_candidate_path_not_in_etherscan_bundle_skipped(self, monkeypatch):
        """The conventional-fallback path (``src/Pool.sol``) won't match
        bundles that use a deeper tree (``contracts/pool/Pool.sol``). Prove
        we skip rather than false-positive when the path is absent."""

        def should_not_be_called(*_a, **_k):
            raise AssertionError("GitHub fetch must not run when path is absent from Etherscan")

        monkeypatch.setattr(
            "services.audits.source_equivalence.fetch_github_source_hash",
            should_not_be_called,
        )
        assert (
            check_audit_covers_impl(
                reviewed_commits=["abc1234"],
                scope_contracts=["Pool"],  # falls back to src/Pool.sol, contracts/Pool.sol
                impl_source=self._src({"lib/other/File.sol": "hash"}),
                source_repo="r/n",
            )
            == []
        )

    def test_github_fetch_returning_none_skipped(self, monkeypatch):
        """GitHub 404 on a candidate path = file didn't exist at that commit.
        Don't record a match; try the next (commit, path) pair."""
        monkeypatch.setattr(
            "services.audits.source_equivalence.fetch_github_source_hash",
            lambda *_a, **_k: None,
        )
        assert (
            check_audit_covers_impl(
                reviewed_commits=["abc1234"],
                scope_contracts=["Pool"],
                impl_source=self._src({"src/Pool.sol": "hash"}),
                source_repo="r/n",
            )
            == []
        )

    def test_records_match_on_hash_equality(self, monkeypatch):
        """The one true-positive path. Preserved both as sanity and so we
        know the match's dataclass shape is actually constructable."""
        monkeypatch.setattr(
            "services.audits.source_equivalence.fetch_github_source_hash",
            lambda *_a, **_k: "matching-hash",
        )
        matches = check_audit_covers_impl(
            reviewed_commits=["abc1234"],
            scope_contracts=["Pool"],
            impl_source=self._src({"src/Pool.sol": "matching-hash"}),
            source_repo="r/n",
        )
        assert matches == [
            EquivalenceMatch(
                commit="abc1234",
                scope_name="Pool",
                etherscan_path="src/Pool.sol",
                source_sha256="matching-hash",
            )
        ]


# ---------------------------------------------------------------------------
# check_audit_row_covers_contract — DB-wrapper short-circuits (mock sessions)
# ---------------------------------------------------------------------------


class TestCheckAuditRowCoversContractShortCircuits:
    """Cover every ``return []`` short-circuit so the matcher can be called
    from places where inputs aren't guaranteed to be complete (e.g. before
    scope extraction fills in ``reviewed_commits``)."""

    def test_returns_empty_when_audit_missing(self):
        session = MagicMock()
        session.get.return_value = None
        assert check_audit_row_covers_contract(session, 1, 2) == []

    def test_returns_empty_when_contract_missing(self):
        session = MagicMock()
        audit = MagicMock()
        # First session.get for AuditReport returns audit, second for Contract returns None
        session.get.side_effect = [audit, None]
        assert check_audit_row_covers_contract(session, 1, 2) == []

    def test_returns_empty_when_reviewed_commits_empty(self):
        session = MagicMock()
        audit = MagicMock()
        audit.reviewed_commits = []
        audit.scope_contracts = ["Pool"]
        audit.source_repo = "r/n"
        contract = MagicMock()
        contract.address = "0x" + "a" * 40
        session.get.side_effect = [audit, contract]
        assert check_audit_row_covers_contract(session, 1, 2) == []

    def test_returns_empty_when_source_repo_missing(self):
        session = MagicMock()
        audit = MagicMock()
        audit.reviewed_commits = ["abc1234"]
        audit.scope_contracts = ["Pool"]
        audit.source_repo = None
        contract = MagicMock()
        contract.address = "0x" + "a" * 40
        session.get.side_effect = [audit, contract]
        assert check_audit_row_covers_contract(session, 1, 2) == []

    def test_returns_empty_when_contract_has_no_address(self):
        """Can't reach Etherscan for a contract with no address — punt rather
        than pass an empty string down and get a useless API error."""
        session = MagicMock()
        audit = MagicMock()
        audit.reviewed_commits = ["abc1234"]
        audit.scope_contracts = ["Pool"]
        audit.source_repo = "r/n"
        contract = MagicMock()
        contract.address = ""
        session.get.side_effect = [audit, contract]
        assert check_audit_row_covers_contract(session, 1, 2) == []

    def test_returns_empty_when_impl_source_unavailable(self, monkeypatch):
        """DB + Etherscan both empty — no source to compare against."""
        session = MagicMock()
        audit = MagicMock()
        audit.reviewed_commits = ["abc1234"]
        audit.scope_contracts = ["Pool"]
        audit.source_repo = "r/n"
        contract = MagicMock()
        contract.address = "0x" + "a" * 40
        contract.job_id = None
        session.get.side_effect = [audit, contract, contract]  # .get may be called again inside
        monkeypatch.setattr(
            source_equivalence,
            "fetch_contract_source_files",
            lambda *_a, **_k: None,
        )
        assert check_audit_row_covers_contract(session, 1, 2) == []

    def test_delegates_to_check_audit_covers_impl_on_full_inputs(self, monkeypatch):
        """All inputs present: delegate with the expected arguments and
        return the result. This is the single happy-path shape the
        coverage matcher relies on."""
        session = MagicMock()
        audit = MagicMock()
        audit.reviewed_commits = ["abc1234", "def5678"]
        audit.scope_contracts = ["Pool", "Vault"]
        audit.source_repo = "r/n"
        contract = MagicMock()
        contract.address = "0x" + "a" * 40
        session.get.side_effect = [audit, contract]

        src = VerifiedSource(contract_name="X", compiler_version="v0.8", files={"src/Pool.sol": "h"})
        monkeypatch.setattr(
            source_equivalence,
            "fetch_contract_source_files",
            lambda *_a, **_k: src,
        )

        expected = [
            EquivalenceMatch(commit="abc1234", scope_name="Pool", etherscan_path="src/Pool.sol", source_sha256="h"),
        ]
        monkeypatch.setattr(
            source_equivalence,
            "check_audit_covers_impl",
            lambda *, reviewed_commits, scope_contracts, impl_source, source_repo, github_token=None: expected,
        )
        got = check_audit_row_covers_contract(session, 1, 2, github_token="tok")
        assert got is expected


# ---------------------------------------------------------------------------
# _hash_source_text — sanity: same text → same hash
# ---------------------------------------------------------------------------


def test_hash_source_text_is_sha256():
    content = "contract X {}"
    assert _hash_source_text(content) == hashlib.sha256(content.encode("utf-8")).hexdigest()
