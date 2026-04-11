"""Tests for DAppCrawlWorker — process() code paths."""

from __future__ import annotations

import importlib
import sys
import uuid
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from workers.base import JobHandledDirectly

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ADDR_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDR_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
ADDR_C = "0xcccccccccccccccccccccccccccccccccccccccc"


@pytest.fixture
def dapp_worker_module(monkeypatch: pytest.MonkeyPatch):
    """Import the worker with a scoped Playwright stub.

    The crawler stack imports Playwright at module import time.  Load the worker
    under a temporary stub so the test stays isolated and does not leave fake
    modules behind for the rest of the suite.
    """
    pw = ModuleType("playwright")
    pw_async = ModuleType("playwright.async_api")
    pw_async.async_playwright = MagicMock()  # type: ignore[attr-defined]
    pw_async.BrowserContext = MagicMock()  # type: ignore[attr-defined]
    pw_async.Page = MagicMock()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "playwright", pw)
    monkeypatch.setitem(sys.modules, "playwright.async_api", pw_async)

    for module_name in (
        "workers.dapp_crawl_worker",
        "services.crawlers.dapp.crawl",
        "services.crawlers.dapp.browser",
    ):
        sys.modules.pop(module_name, None)

    module = importlib.import_module("workers.dapp_crawl_worker")
    yield module

    for module_name in (
        "workers.dapp_crawl_worker",
        "services.crawlers.dapp.crawl",
        "services.crawlers.dapp.browser",
    ):
        sys.modules.pop(module_name, None)


def _job(**overrides: Any) -> SimpleNamespace:
    """Create a minimal fake job with sensible defaults."""
    payload: dict[str, Any] = {
        "id": uuid.uuid4(),
        "name": None,
        "protocol_id": None,
        "request": {
            "dapp_urls": ["https://example.com"],
        },
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _patch_worker_deps(
    monkeypatch: pytest.MonkeyPatch,
    worker_module: Any,
    *,
    crawl_result=None,
    already_used=0,
):
    """Patch all external deps of DAppCrawlWorker.process().

    Returns a dict of spy lists so tests can inspect calls.
    """
    if crawl_result is None:
        crawl_result = {"addresses": [], "interaction_count": 0}

    store_calls: list[tuple[str, Any]] = []
    create_calls: list[dict] = []
    complete_calls: list[tuple] = []

    monkeypatch.setattr(worker_module, "crawl_dapp", lambda urls, chain_id=1, wait=10, progress=None: crawl_result)
    # BaseWorker.update_detail calls update_job_detail; patch it on the class
    monkeypatch.setattr(
        worker_module.DAppCrawlWorker,
        "update_detail",
        lambda self, session, job, detail: None,
    )

    def fake_store_artifact(session, job_id, name, data=None, text_data=None):
        store_calls.append((name, data))

    monkeypatch.setattr(worker_module, "store_artifact", fake_store_artifact)

    monkeypatch.setattr(worker_module, "count_analysis_children", lambda session, root_job_id: already_used)

    def fake_create_job(session, request_dict):
        child = SimpleNamespace(id=uuid.uuid4())
        create_calls.append(request_dict)
        return child

    monkeypatch.setattr(worker_module, "create_job", fake_create_job)

    def fake_complete_job(session, job_id, detail=""):
        complete_calls.append((job_id, detail))

    monkeypatch.setattr(worker_module, "complete_job", fake_complete_job)

    return {
        "store_calls": store_calls,
        "create_calls": create_calls,
        "complete_calls": complete_calls,
    }


def _session_with_dedup(existing_addresses: set[str]) -> MagicMock:
    """Return a MagicMock session whose execute() recognises known addresses.

    The worker calls ``session.execute(select(Job).where(Job.address == addr).limit(1))``
    for each address.  We extract the bound address value from the compiled
    SQLAlchemy statement and return a fake existing job if it is in the set.
    """
    session = MagicMock()

    def fake_execute(stmt):
        result = MagicMock()
        # Determine which table is being queried
        stmt_str = str(stmt)
        addr = _extract_address_from_stmt(stmt)
        if "jobs" in stmt_str and addr and addr in existing_addresses:
            result.scalar_one_or_none.return_value = SimpleNamespace(id=uuid.uuid4())
        else:
            result.scalar_one_or_none.return_value = None
        return result

    session.execute.side_effect = fake_execute
    return session


def _extract_address_from_stmt(stmt):
    """Best-effort extraction of the address literal from a SQLAlchemy select.

    Handles the typical pattern: ``select(Job).where(Job.address == addr).limit(1)``
    The WHERE clause lives in ``stmt.whereclause`` and may be a single
    ``BinaryExpression`` or a ``BooleanClauseList``.
    """
    try:
        wc = stmt.whereclause
        if wc is None:
            return None
        # If it's a single BinaryExpression (no AND/OR wrapper)
        clauses = getattr(wc, "clauses", [wc])
        for criterion in clauses:
            if hasattr(criterion, "right"):
                right = criterion.right
                # BindParameter stores the value in .value
                val = getattr(right, "value", None)
                if isinstance(val, str) and val.startswith("0x"):
                    return val
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMissingDappUrls:
    """process() raises ValueError when dapp_urls is absent or empty."""

    def test_missing_key(self, dapp_worker_module):
        worker = dapp_worker_module.DAppCrawlWorker()
        session = MagicMock()
        job = _job(request={})

        with pytest.raises(ValueError, match="missing dapp_urls"):
            worker.process(session, cast(Any, job))

    def test_empty_list(self, dapp_worker_module):
        worker = dapp_worker_module.DAppCrawlWorker()
        session = MagicMock()
        job = _job(request={"dapp_urls": []})

        with pytest.raises(ValueError, match="missing dapp_urls"):
            worker.process(session, cast(Any, job))

    def test_request_is_none(self, dapp_worker_module):
        worker = dapp_worker_module.DAppCrawlWorker()
        session = MagicMock()
        job = _job(request=None)

        with pytest.raises(ValueError, match="missing dapp_urls"):
            worker.process(session, cast(Any, job))


class TestHappyPath:
    """Crawl finds addresses, creates child jobs, stores artifacts, raises JobHandledDirectly."""

    def test_creates_children_and_stores_artifacts(self, monkeypatch, dapp_worker_module):
        crawl_result = {
            "addresses": [ADDR_A, ADDR_B],
            "interaction_count": 5,
        }
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job()

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # Two child jobs created
        assert len(spies["create_calls"]) == 2
        created_addresses = [c["address"] for c in spies["create_calls"]]
        assert ADDR_A in created_addresses
        assert ADDR_B in created_addresses

        # Artifacts stored: dapp_crawl_results + discovery_summary
        stored_names = [name for name, _ in spies["store_calls"]]
        assert "dapp_crawl_results" in stored_names
        assert "discovery_summary" in stored_names

        # dapp_crawl_results has correct data
        crawl_artifact = next(d for n, d in spies["store_calls"] if n == "dapp_crawl_results")
        assert crawl_artifact["addresses_found"] == 2
        assert crawl_artifact["interaction_count"] == 5

        # discovery_summary reflects 2 analyzed
        summary = next(d for n, d in spies["store_calls"] if n == "discovery_summary")
        assert summary["discovered_count"] == 2
        assert summary["analyzed_count"] == 2

        # complete_job was called
        assert len(spies["complete_calls"]) == 1


class TestDeduplication:
    """Existing addresses are skipped and not turned into child jobs."""

    def test_existing_address_skipped(self, monkeypatch, dapp_worker_module):
        crawl_result = {
            "addresses": [ADDR_A, ADDR_B],
            "interaction_count": 3,
        }
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        # ADDR_A already has a job
        session = _session_with_dedup({ADDR_A})
        job = _job()

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # Only ADDR_B gets a child job
        assert len(spies["create_calls"]) == 1
        assert spies["create_calls"][0]["address"] == ADDR_B

    def test_all_existing_no_children(self, monkeypatch, dapp_worker_module):
        crawl_result = {
            "addresses": [ADDR_A, ADDR_B],
            "interaction_count": 2,
        }
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup({ADDR_A, ADDR_B})
        job = _job()

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert len(spies["create_calls"]) == 0

        summary = next(d for n, d in spies["store_calls"] if n == "discovery_summary")
        assert summary["analyzed_count"] == 0


class TestAnalyzeLimit:
    """The analyze_limit cap restricts how many child jobs are created."""

    def test_cap_with_no_prior_children(self, monkeypatch, dapp_worker_module):
        crawl_result = {
            "addresses": [ADDR_A, ADDR_B, ADDR_C],
            "interaction_count": 3,
        }
        # analyze_limit=2 means only 2 addresses are selected
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result, already_used=0)
        session = _session_with_dedup(set())
        job = _job(request={"dapp_urls": ["https://example.com"], "analyze_limit": 2})

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # Only 2 of 3 addresses processed
        assert len(spies["create_calls"]) == 2

    def test_cap_with_prior_children(self, monkeypatch, dapp_worker_module):
        crawl_result = {
            "addresses": [ADDR_A, ADDR_B, ADDR_C],
            "interaction_count": 3,
        }
        # analyze_limit defaults to 25, but already_used=24 leaves room for 1
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result, already_used=24)
        session = _session_with_dedup(set())
        job = _job()

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert len(spies["create_calls"]) == 1

    def test_cap_exhausted(self, monkeypatch, dapp_worker_module):
        crawl_result = {
            "addresses": [ADDR_A, ADDR_B],
            "interaction_count": 2,
        }
        # already_used >= analyze_limit => remaining = 0
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result, already_used=25)
        session = _session_with_dedup(set())
        job = _job()

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert len(spies["create_calls"]) == 0


class TestJobName:
    """Job name is set when missing, but not overwritten when already present."""

    def test_name_set_when_missing(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [], "interaction_count": 0}
        _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job(name=None, request={"dapp_urls": ["https://a.com", "https://b.com"]})

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert job.name == "DApp crawl (2 URLs)"
        session.commit.assert_called()

    def test_name_not_overwritten(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [], "interaction_count": 0}
        _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job(name="My Custom Name")

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert job.name == "My Custom Name"


class TestChainPropagation:
    """When request has a 'chain' key, it is passed through to child requests."""

    def test_chain_key_forwarded(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [ADDR_A], "interaction_count": 1}
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job(request={"dapp_urls": ["https://example.com"], "chain": "ethereum"})

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert spies["create_calls"][0]["chain"] == "ethereum"

    def test_no_chain_key_omitted(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [ADDR_A], "interaction_count": 1}
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job()

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert "chain" not in spies["create_calls"][0]


class TestCrawlParameters:
    """chain_id, wait, and analyze_limit are read from request and passed correctly."""

    def test_custom_chain_id_and_wait(self, monkeypatch, dapp_worker_module):
        captured = {}

        def fake_crawl(urls, chain_id=1, wait=10, progress=None):
            captured["chain_id"] = chain_id
            captured["wait"] = wait
            return {"addresses": [], "interaction_count": 0}

        monkeypatch.setattr(dapp_worker_module, "crawl_dapp", fake_crawl)
        _patch_worker_deps(monkeypatch, dapp_worker_module)
        # Re-patch crawl_dapp since _patch_worker_deps overwrites it
        monkeypatch.setattr(dapp_worker_module, "crawl_dapp", fake_crawl)

        session = _session_with_dedup(set())
        job = _job(request={"dapp_urls": ["https://x.com"], "chain_id": 137, "wait": 20})

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert captured["chain_id"] == 137
        assert captured["wait"] == 20

    def test_missing_wait_uses_default(self, monkeypatch, dapp_worker_module):
        captured = {}

        def fake_crawl(urls, chain_id=1, wait=10, progress=None):
            captured["wait"] = wait
            return {"addresses": [], "interaction_count": 0}

        monkeypatch.setattr(dapp_worker_module, "crawl_dapp", fake_crawl)
        _patch_worker_deps(monkeypatch, dapp_worker_module)
        monkeypatch.setattr(dapp_worker_module, "crawl_dapp", fake_crawl)

        session = _session_with_dedup(set())
        job = _job(request={"dapp_urls": ["https://x.com"]})

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert captured["wait"] == 10


class TestAnalyzeLimitWithDedup:
    """analyze_limit should be filled with new addresses after deduplication."""

    def test_limit_is_filled_after_skipping_existing(self, monkeypatch, dapp_worker_module):
        crawl_result = {
            "addresses": [ADDR_A, ADDR_B, ADDR_C],
            "interaction_count": 3,
        }
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result, already_used=0)
        session = _session_with_dedup({ADDR_A})
        job = _job(request={"dapp_urls": ["https://example.com"], "analyze_limit": 2})

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        created_addresses = [call["address"] for call in spies["create_calls"]]
        assert created_addresses == [ADDR_B, ADDR_C]


class TestDuplicateAddressesFromCrawler:
    """Duplicate crawler results are only processed once."""

    def test_duplicate_address_only_checked_once(self, monkeypatch, dapp_worker_module):
        crawl_result = {
            "addresses": [ADDR_A, ADDR_A],
            "interaction_count": 2,
        }
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)

        # Track per-address execute calls
        execute_addrs: list[str | None] = []

        def fake_execute(stmt):
            result = MagicMock()
            addr = _extract_address_from_stmt(stmt)
            execute_addrs.append(addr)
            result.scalar_one_or_none.return_value = None
            return result

        session = MagicMock()
        session.execute.side_effect = fake_execute
        job = _job()

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert len(spies["create_calls"]) == 1
        # Contract lookup for each raw address + Job dedup check (only unique)
        addr_calls = [a for a in execute_addrs if a is not None]
        # ADDR_A appears twice in crawl results → 2 Contract lookups + 1 Job lookup
        assert addr_calls == [ADDR_A, ADDR_A, ADDR_A]


class TestChildRequestPropagation:
    """Verify lineage and RPC fields are correctly propagated to child requests."""

    def test_root_job_id_from_request(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [ADDR_A], "interaction_count": 1}
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job(
            request={
                "dapp_urls": ["https://example.com"],
                "root_job_id": "custom-root-123",
            }
        )

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert spies["create_calls"][0]["parent_job_id"] == str(job.id)
        assert spies["create_calls"][0]["root_job_id"] == "custom-root-123"

    def test_root_job_id_defaults_to_job_id(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [ADDR_A], "interaction_count": 1}
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job()

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert spies["create_calls"][0]["parent_job_id"] == str(job.id)
        assert spies["create_calls"][0]["root_job_id"] == str(job.id)

    def test_rpc_url_propagated(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [ADDR_A], "interaction_count": 1}
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job(
            request={
                "dapp_urls": ["https://example.com"],
                "rpc_url": "https://rpc.mainnet.example",
            }
        )

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert spies["create_calls"][0]["rpc_url"] == "https://rpc.mainnet.example"
