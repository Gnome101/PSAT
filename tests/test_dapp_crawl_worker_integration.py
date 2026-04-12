"""Integration tests for DAppCrawlWorker with a real Postgres-backed queue.

These tests do not drive a real browser because Playwright is not part of the
default test environment. Instead, they serve a local fake DApp page over HTTP
and patch only the crawler entrypoint so the worker still runs against the real
queue, job, artifact, and completion code paths.
"""

from __future__ import annotations

import importlib
import os
import re
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock
from urllib.request import urlopen

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.models import Artifact, Base, Job, JobStage, JobStatus, SourceFile
from db.queue import create_job, get_artifact
from workers.base import JobHandledDirectly

DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")

ADDR_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDR_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
ADDR_C = "0xcccccccccccccccccccccccccccccccccccccccc"

_FAKE_DAPP_HTML = f"""\
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Fake DApp</title>
  </head>
  <body>
    <div id="app">Fake DApp integration fixture</div>
    <script>
      window.__contracts = ["{ADDR_A}", "{ADDR_A}", "{ADDR_B}"];
      window.__registry = "{ADDR_C}";
    </script>
  </body>
</html>
"""


def _can_connect() -> bool:
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(not _can_connect(), reason="PostgreSQL not available")


class _FakeDappHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = _FAKE_DAPP_HTML.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *args) -> None:
        return


@pytest.fixture()
def db_session():
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    session = Session(engine, expire_on_commit=False)
    try:
        yield session
    finally:
        session.rollback()
        # Only delete jobs with test addresses (cascades to artifacts/source_files)
        test_addrs = [ADDR_A, ADDR_B, ADDR_C]
        for j in session.execute(
            select(Job).where(Job.address.in_(test_addrs))
        ).scalars():
            session.delete(j)
        # Delete the crawl job itself (no address, identified by dapp_urls in request)
        for j in session.execute(
            select(Job).where(Job.address.is_(None), Job.name.like("DApp crawl%"))
        ).scalars():
            session.delete(j)
        session.commit()
        session.close()
        engine.dispose()


@pytest.fixture()
def fake_dapp_url():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeDappHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.fixture()
def dapp_worker_module(monkeypatch: pytest.MonkeyPatch):
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


@requires_postgres
def test_process_runs_against_real_queue_and_fake_dapp(
    db_session: Session,
    fake_dapp_url: str,
    dapp_worker_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, int] = {}

    def fake_crawl(
        urls: list[str],
        *,
        chain_id: int = 1,
        wait: int = 10,
        progress=None,
    ) -> dict[str, object]:
        captured["chain_id"] = chain_id
        captured["wait"] = wait
        addresses: list[str] = []
        for url in urls:
            with urlopen(url, timeout=5) as response:
                html = response.read().decode("utf-8")
            addresses.extend(re.findall(r"0x[a-fA-F0-9]{40}", html))
        return {
            "addresses": addresses,
            "interaction_count": len(addresses),
        }

    monkeypatch.setattr(dapp_worker_module, "crawl_dapp", fake_crawl)

    root_job_id = str(uuid.uuid4())
    create_job(
        db_session,
        {
            "address": ADDR_A,
            "name": "existing_analysis_job",
            "root_job_id": root_job_id,
        },
    )
    job = create_job(
        db_session,
        {
            "dapp_urls": [fake_dapp_url],
            "root_job_id": root_job_id,
            "analyze_limit": 3,
            "chain_id": 137,
            "wait": 7,
            "chain": "polygon",
            "rpc_url": "https://rpc.example",
        },
        initial_stage=JobStage.dapp_crawl,
    )

    worker = dapp_worker_module.DAppCrawlWorker()
    with pytest.raises(JobHandledDirectly):
        worker.process(db_session, job)

    db_session.refresh(job)
    assert captured == {"chain_id": 137, "wait": 7}
    assert job.stage == JobStage.done
    assert job.status == JobStatus.completed
    assert job.name == "DApp crawl (1 URLs)"
    assert job.detail == "DApp crawl complete: 4 addresses found, 2 queued"

    crawl_results = get_artifact(db_session, job.id, "dapp_crawl_results")
    assert isinstance(crawl_results, dict)
    assert crawl_results["urls_crawled"] == [fake_dapp_url]
    assert crawl_results["addresses_found"] == 4
    assert crawl_results["addresses"] == [ADDR_A, ADDR_A, ADDR_B, ADDR_C]
    assert crawl_results["interaction_count"] == 4

    summary = get_artifact(db_session, job.id, "discovery_summary")
    assert isinstance(summary, dict)
    assert summary["mode"] == "dapp_crawl"
    assert summary["discovered_count"] == 4
    assert summary["analyzed_count"] == 2
    assert [entry["address"] for entry in summary["child_jobs"]] == [ADDR_B, ADDR_C]

    child_jobs = (
        db_session.execute(
            select(Job)
            .where(
                Job.address.isnot(None),
                Job.request["parent_job_id"].as_string() == str(job.id),
            )
            .order_by(Job.created_at, Job.id)
        )
        .scalars()
        .all()
    )
    assert [child.address for child in child_jobs] == [ADDR_B, ADDR_C]
    assert all(child.stage == JobStage.discovery for child in child_jobs)
    assert all(child.status == JobStatus.queued for child in child_jobs)
    req0 = child_jobs[0].request
    assert isinstance(req0, dict)
    assert req0["root_job_id"] == root_job_id
    assert req0["parent_job_id"] == str(job.id)
    assert req0["rpc_url"] == "https://rpc.example"
    assert req0["chain"] == "polygon"
