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
        "company": None,
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
    protocol_calls: list[tuple[str, str | None]] = []

    monkeypatch.setattr(worker_module, "crawl_dapp", lambda urls, chain_id=1, wait=10, progress=None: crawl_result)

    def fake_get_or_create_protocol(session, name, official_domain=None):
        protocol_calls.append((name, official_domain))
        return SimpleNamespace(id=1, name=name, official_domain=official_domain)

    monkeypatch.setattr(worker_module, "get_or_create_protocol", fake_get_or_create_protocol)
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
        "protocol_calls": protocol_calls,
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
        # analyze_limit defaults to 5, but already_used=4 leaves room for 1
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result, already_used=4)
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
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result, already_used=5)
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
    """Chain is propagated to children — explicit from request, per-address from crawl, or defaulted from chain_id."""

    def test_chain_key_forwarded(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [ADDR_A], "interaction_count": 1}
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job(request={"dapp_urls": ["https://example.com"], "chain": "ethereum"})

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert spies["create_calls"][0]["chain"] == "ethereum"

    def test_chain_defaulted_from_chain_id_when_omitted(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [ADDR_A], "interaction_count": 1}
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job()

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # chain_id defaults to 1 → "ethereum"
        assert spies["create_calls"][0]["chain"] == "ethereum"

    def test_per_address_inferred_chain_wins(self, monkeypatch, dapp_worker_module):
        crawl_result = {
            "addresses": [ADDR_A],
            "interaction_count": 1,
            "address_details": [{"address": ADDR_A, "chain": "arbitrum", "source_urls": []}],
        }
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job()

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert spies["create_calls"][0]["chain"] == "arbitrum"


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


class TestProtocolCreation:
    """Protocol row is created / looked up from URL hostname when company is absent."""

    def test_hostname_derived_when_no_company(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [], "interaction_count": 0}
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job(request={"dapp_urls": ["https://ether.fi/stake"]})

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # get_or_create_protocol called with hostname-derived name and domain
        assert spies["protocol_calls"] == [("ether.fi", "ether.fi")]
        assert job.protocol_id == 1
        assert job.company == "ether.fi"

    def test_www_stripped_from_hostname(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [], "interaction_count": 0}
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job(request={"dapp_urls": ["https://www.uniswap.org"]})

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert spies["protocol_calls"] == [("uniswap.org", "uniswap.org")]

    def test_company_prefers_over_hostname(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [], "interaction_count": 0}
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job(company="Ether.fi", request={"dapp_urls": ["https://stake.ether.fi"]})

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        # Name comes from job.company; domain still derived from URL
        assert spies["protocol_calls"] == [("Ether.fi", "stake.ether.fi")]
        assert job.company == "Ether.fi"


class TestCompanyPropagatedToChildren:
    def test_child_request_carries_company(self, monkeypatch, dapp_worker_module):
        crawl_result = {"addresses": [ADDR_A], "interaction_count": 1}
        spies = _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job(request={"dapp_urls": ["https://ether.fi"]})

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        assert spies["create_calls"][0]["company"] == "ether.fi"
        assert spies["create_calls"][0]["protocol_id"] == 1


class TestInteractionPersistence:
    def test_interactions_written_to_session(self, monkeypatch, dapp_worker_module):
        crawl_result = {
            "addresses": [ADDR_A],
            "interaction_count": 2,
            "interactions": [
                {
                    "type": "sendTransaction",
                    "url": "https://ether.fi/stake",
                    "timestamp": 1700000000000,
                    "to": ADDR_A.upper(),
                    "value": "0x0",
                    "data": "0xabcdef01",
                    "method_selector": "0xabcdef01",
                    "is_permit": False,
                },
                {
                    "type": "personal_sign",
                    "url": "https://ether.fi/",
                    "timestamp": 1700000001000,
                    "to": None,
                    "message": "sign in",
                    "is_permit": False,
                },
            ],
        }
        _patch_worker_deps(monkeypatch, dapp_worker_module, crawl_result=crawl_result)
        session = _session_with_dedup(set())
        job = _job()

        worker = dapp_worker_module.DAppCrawlWorker()
        with pytest.raises(JobHandledDirectly):
            worker.process(session, cast(Any, job))

        added = [call.args[0] for call in session.add.call_args_list]
        interactions = [row for row in added if type(row).__name__ == "DAppInteraction"]
        assert len(interactions) == 2
        types = {row.type for row in interactions}
        assert types == {"sendTransaction", "personal_sign"}
        # `to` is lowercased
        send_row = next(row for row in interactions if row.type == "sendTransaction")
        assert send_row.to_address == ADDR_A.lower()
        assert send_row.method_selector == "0xabcdef01"
