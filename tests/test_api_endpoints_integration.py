"""Integration tests for API endpoints.

Covers:
- POST /api/analyze with company and address payloads, plus validation
- GET /api/analyses proxy flagging via contract_flags artifact
- GET /api/analyses/{run_name} impl-to-proxy artifact fallback
- POST/GET/DELETE /api/watched-proxies proxy monitoring endpoints
- GET /api/proxy-events upgrade event listing
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fake_api_job(
    job_id: str | None = None,
    address: str | None = None,
    company: str | None = None,
    name: str | None = None,
    status: str = "queued",
    stage: str = "discovery",
    request: dict | None = None,
):
    """Build a MagicMock that behaves like db.models.Job."""
    job = MagicMock()
    uid = uuid.UUID(job_id) if job_id else uuid.uuid4()
    job.id = uid
    job.address = address
    job.company = company
    job.name = name
    job.status = MagicMock(value=status)
    job.stage = MagicMock(value=stage)
    job.detail = "Test detail"
    job.request = request or {}
    job.error = None
    job.worker_id = None
    job.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    job.updated_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    job.to_dict.return_value = {
        "job_id": str(uid),
        "address": address,
        "company": company,
        "name": name,
        "status": status,
        "stage": stage,
        "detail": "Test detail",
        "request": request or {},
        "error": None,
        "worker_id": None,
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    return job


def _mock_session_ctx(mock_session_cls, mock_session):
    """Wire up a mock SessionLocal so `with SessionLocal() as session:` works."""
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)


def _make_client() -> TestClient:
    import api

    return TestClient(api.app)


# ---------------------------------------------------------------------------
# 1. POST /api/analyze — company payload
# ---------------------------------------------------------------------------


@patch("api.SessionLocal")
@patch("api.create_job")
def test_analyze_company_creates_job(mock_create_job, mock_session_cls):
    """Submitting {"company": "etherfi"} should create a job with company set,
    address null, stage=discovery, status=queued."""
    client = _make_client()

    fake_job = _fake_api_job(
        company="etherfi",
        status="queued",
        stage="discovery",
        request={
            "company": "etherfi",
            "name": None,
            "address": None,
            "chain": None,
            "analyze_limit": 5,
            "rpc_url": None,
        },
    )
    mock_create_job.return_value = fake_job

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    response = client.post("/api/analyze", json={"company": "etherfi"})

    assert response.status_code == 200
    body = response.json()
    assert body["company"] == "etherfi"
    assert body["address"] is None
    assert body["stage"] == "discovery"
    assert body["status"] == "queued"

    # Verify create_job was called with the model-dumped dict
    call_args = mock_create_job.call_args
    req_dict = call_args[0][1]  # positional arg: (session, request_dict)
    assert req_dict["company"] == "etherfi"
    assert req_dict.get("address") is None


# ---------------------------------------------------------------------------
# 1b. POST /api/analyze — mutual exclusion validation
# ---------------------------------------------------------------------------


def test_analyze_rejects_both_address_and_company():
    """Providing both address and company should return 422."""
    client = _make_client()
    response = client.post(
        "/api/analyze",
        json={
            "address": "0x1111111111111111111111111111111111111111",
            "company": "etherfi",
        },
    )
    assert response.status_code == 422


def test_analyze_rejects_neither_address_nor_company():
    """Providing neither address nor company should return 422."""
    client = _make_client()
    response = client.post("/api/analyze", json={})
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# 2. POST /api/analyze — address payload
# ---------------------------------------------------------------------------


@patch("api.SessionLocal")
@patch("api.create_job")
def test_analyze_address_creates_job(mock_create_job, mock_session_cls):
    """Submitting {"address": "0x1111..."} should create a job with address set
    and company null."""
    client = _make_client()
    addr = "0x1111111111111111111111111111111111111111"

    fake_job = _fake_api_job(
        address=addr,
        status="queued",
        stage="discovery",
        request={
            "address": addr,
            "name": None,
            "company": None,
            "chain": None,
            "analyze_limit": 5,
            "rpc_url": None,
        },
    )
    mock_create_job.return_value = fake_job

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    response = client.post("/api/analyze", json={"address": addr})

    assert response.status_code == 200
    body = response.json()
    assert body["address"] == addr
    assert body["company"] is None
    assert body["stage"] == "discovery"
    assert body["status"] == "queued"

    call_args = mock_create_job.call_args
    req_dict = call_args[0][1]
    assert req_dict["address"] == addr
    assert req_dict.get("company") is None


# ---------------------------------------------------------------------------
# 3. GET /api/analyses — proxy flagging via contract_flags artifact
# ---------------------------------------------------------------------------


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_analyses_list_proxy_flagging(mock_session_cls, mock_get_artifact):
    """A completed proxy job + its impl job should merge into one entry
    that carries the is_proxy, proxy_type, and implementation_address
    fields from the contract_flags artifact.

    _merge_proxy_impl_entries hides standalone proxy entries whose impl
    child job hasn't completed — so we must include both the proxy job
    and its impl job for the merged entry to appear."""
    client = _make_client()
    proxy_job_id = uuid.uuid4()
    impl_job_id = uuid.uuid4()
    proxy_addr = "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    impl_addr = "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"

    proxy_job = _fake_api_job(
        job_id=str(proxy_job_id),
        address=proxy_addr,
        name="proxy_contract",
        status="completed",
        stage="done",
        request={"address": proxy_addr},
    )
    impl_job = _fake_api_job(
        job_id=str(impl_job_id),
        address=impl_addr,
        name="proxy_contract: (impl)",
        status="completed",
        stage="done",
        request={"address": impl_addr, "proxy_address": proxy_addr, "parent_job_id": str(proxy_job_id)},
    )

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    # Make impl_job.status match the real JobStatus.completed enum so the
    # analyses endpoint's impl-completion check passes.
    from db.models import JobStatus

    impl_job.status = JobStatus.completed

    # The analyses() endpoint calls session.execute() multiple times:
    #   1. select(Job).where(status==completed) → returns jobs
    #   2. select(Artifact.name).where(job_id==X) → per-job artifact names
    #   3. select(Job).where(address==impl) → impl job lookup (for proxy entries)
    #   4+ more artifact name queries
    call_count = 0

    def route_execute(stmt, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        if call_count == 1:
            result.scalars.return_value.all.return_value = [proxy_job, impl_job]
            return result
        # Impl job lookup — returns scalar_one_or_none
        if hasattr(result, "scalar_one_or_none") and call_count <= 5:
            result.scalar_one_or_none.return_value = impl_job
        result.scalars.return_value.all.return_value = [
            "contract_flags",
            "contract_analysis",
        ]
        return result

    mock_session.execute.side_effect = route_execute

    def fake_get_artifact(session, jid, name):
        if str(jid) == str(proxy_job_id) and name == "contract_flags":
            return {
                "is_proxy": True,
                "proxy_type": "ERC1967",
                "implementation": impl_addr,
            }
        if str(jid) == str(proxy_job_id) and name == "contract_analysis":
            return {
                "subject": {"name": "ProxyContract"},
                "summary": {"control_model": "proxy"},
            }
        if str(jid) == str(impl_job_id) and name == "contract_analysis":
            return {
                "subject": {"name": "VaultImpl"},
                "summary": {"control_model": "authority"},
            }
        if name == "contract_inventory":
            return None
        if name == "contract_flags":
            return None
        return None

    mock_get_artifact.side_effect = fake_get_artifact

    response = client.get("/api/analyses")

    assert response.status_code == 200
    entries = response.json()
    assert len(entries) >= 1

    # The merged entry carries proxy info via proxy_address_display and
    # proxy_type_display (not is_proxy — that field comes from the impl
    # entry base in the merge, where it's False).
    merged = entries[0]
    assert merged["proxy_address_display"] == proxy_addr
    assert merged["proxy_type_display"] == "ERC1967"
    # The impl's contract_name is used as the display_name
    assert merged["display_name"] == "VaultImpl"


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_analyses_list_non_proxy_has_is_proxy_false(mock_session_cls, mock_get_artifact):
    """A completed job without contract_flags or with is_proxy=False should
    appear with is_proxy=False."""
    client = _make_client()
    job_id = uuid.uuid4()

    fake_job = _fake_api_job(
        job_id=str(job_id),
        address="0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
        name="regular_contract",
        status="completed",
        stage="done",
        request={"address": "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"},
    )

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    mock_exec_jobs = MagicMock()
    mock_exec_jobs.scalars.return_value.all.return_value = [fake_job]

    mock_exec_artifacts = MagicMock()
    mock_exec_artifacts.scalars.return_value.all.return_value = ["contract_analysis"]

    def route_execute(stmt, *args, **kwargs):
        stmt_str = str(stmt)
        if "artifacts" in stmt_str.lower():
            return mock_exec_artifacts
        return mock_exec_jobs

    mock_session.execute.side_effect = route_execute

    def fake_get_artifact(session, jid, name):
        if name == "contract_flags":
            return None  # No flags artifact
        if name == "contract_analysis":
            return {"subject": {"name": "Regular"}, "summary": {}}
        if name == "contract_inventory":
            return None
        return None

    mock_get_artifact.side_effect = fake_get_artifact

    response = client.get("/api/analyses")

    assert response.status_code == 200
    entries = response.json()
    entry = next((e for e in entries if e.get("job_id") == str(job_id)), None)
    assert entry is not None
    assert entry["is_proxy"] is False


# ---------------------------------------------------------------------------
# 4. GET /api/analyses/{run_name} — impl-to-proxy artifact fallback
# ---------------------------------------------------------------------------


@patch("api.get_artifact")
@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_analysis_detail_falls_back_to_proxy_artifacts(mock_session_cls, mock_get_all_artifacts, mock_get_artifact):
    """When an impl job has proxy_address in its request but lacks
    dependency_graph_viz, the detail endpoint should fall back to the proxy
    job's dependency_graph_viz and dependencies artifacts."""
    client = _make_client()

    proxy_address = "0x2222222222222222222222222222222222222222"
    impl_job_id = uuid.uuid4()
    proxy_job_id = uuid.uuid4()

    # Impl job: has proxy_address in request, no dependency_graph_viz
    impl_job = _fake_api_job(
        job_id=str(impl_job_id),
        address="0x3333333333333333333333333333333333333333",
        name="impl_contract",
        status="completed",
        stage="done",
        request={"proxy_address": proxy_address},
    )

    # Proxy job: has the dependency_graph_viz and dependencies artifacts
    proxy_job = _fake_api_job(
        job_id=str(proxy_job_id),
        address=proxy_address,
        name="proxy_contract",
        status="completed",
        stage="done",
        request={"address": proxy_address},
    )

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    # The detail endpoint does:
    # 1. select(Job).where(Job.name == run_name) -> returns impl_job
    # 2. select(Job).where(Job.address == proxy_address) -> returns proxy_job
    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        # First execute: lookup impl job by name
        if call_count["n"] == 1:
            result.scalar_one_or_none.return_value = impl_job
        # Second execute: lookup proxy job by address
        else:
            result.scalar_one_or_none.return_value = proxy_job
        return result

    mock_session.execute.side_effect = route_execute

    # Impl job's artifacts: has contract_analysis but NOT dependency_graph_viz
    mock_get_all_artifacts.return_value = {
        "contract_analysis": {
            "subject": {"name": "ImplContract"},
            "summary": {"control_model": "ownable"},
        },
    }

    proxy_dep_graph = {
        "nodes": [{"id": "0x111"}, {"id": "0x222"}],
        "edges": [{"from": "0x111", "to": "0x222"}],
    }
    proxy_dependencies = {
        "dependencies": ["0x4444444444444444444444444444444444444444"],
    }

    # get_artifact is called for proxy job's fallback artifacts
    def fake_get_artifact(session, jid, name):
        if str(jid) == str(proxy_job_id):
            if name == "dependency_graph_viz":
                return proxy_dep_graph
            if name == "dependencies":
                return proxy_dependencies
        return None

    mock_get_artifact.side_effect = fake_get_artifact

    response = client.get("/api/analyses/impl_contract")

    assert response.status_code == 200
    body = response.json()
    assert body["run_name"] == "impl_contract"
    assert body["proxy_address"] == proxy_address
    # Should have inherited dependency_graph_viz from proxy job
    assert body["dependency_graph_viz"] == proxy_dep_graph
    # Should have inherited dependencies from proxy job
    assert body["dependencies"] == proxy_dependencies


@patch("api.get_artifact")
@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_analysis_detail_no_fallback_when_impl_has_artifacts(
    mock_session_cls, mock_get_all_artifacts, mock_get_artifact
):
    """When the impl job already has dependency_graph_viz, the detail endpoint
    should NOT fall back to the proxy job's artifacts."""
    client = _make_client()

    proxy_address = "0x2222222222222222222222222222222222222222"
    impl_job_id = uuid.uuid4()

    impl_job = _fake_api_job(
        job_id=str(impl_job_id),
        address="0x3333333333333333333333333333333333333333",
        name="impl_with_deps",
        status="completed",
        stage="done",
        request={"proxy_address": proxy_address},
    )

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    mock_exec = MagicMock()
    mock_exec.scalar_one_or_none.return_value = impl_job
    mock_session.execute.return_value = mock_exec

    impl_dep_graph = {"nodes": [{"id": "own"}], "edges": []}
    impl_dependencies = {"dependencies": ["0x5555555555555555555555555555555555555555"]}

    # Impl job already has dependency_graph_viz and dependencies
    mock_get_all_artifacts.return_value = {
        "contract_analysis": {
            "subject": {"name": "ImplContract"},
            "summary": {},
        },
        "dependency_graph_viz": impl_dep_graph,
        "dependencies": impl_dependencies,
    }

    response = client.get("/api/analyses/impl_with_deps")

    assert response.status_code == 200
    body = response.json()
    # Should use impl's own artifacts, not proxy's
    assert body["dependency_graph_viz"] == impl_dep_graph
    assert body["dependencies"] == impl_dependencies
    # get_artifact may still be called for upgrade_history (which the impl
    # doesn't have), but dependency_graph_viz and dependencies must NOT be
    # fetched from the proxy since they already exist on the impl.
    for call_args in mock_get_artifact.call_args_list:
        artifact_name = call_args[0][2] if len(call_args[0]) >= 3 else call_args[1].get("name")
        assert artifact_name not in ("dependency_graph_viz", "dependencies"), (
            f"Fallback should not fetch {artifact_name} when impl already has it"
        )


@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_analysis_detail_no_fallback_without_proxy_address(mock_session_cls, mock_get_all_artifacts):
    """When the job has no proxy_address in its request, no fallback should
    occur even if dependency_graph_viz is missing."""
    client = _make_client()
    job_id = uuid.uuid4()

    job = _fake_api_job(
        job_id=str(job_id),
        address="0x5555555555555555555555555555555555555555",
        name="standalone_job",
        status="completed",
        stage="done",
        request={"address": "0x5555555555555555555555555555555555555555"},
    )

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    mock_exec = MagicMock()
    mock_exec.scalar_one_or_none.return_value = job
    mock_session.execute.return_value = mock_exec

    # No dependency_graph_viz in artifacts
    mock_get_all_artifacts.return_value = {
        "contract_analysis": {
            "subject": {"name": "Standalone"},
            "summary": {},
        },
    }

    response = client.get("/api/analyses/standalone_job")

    assert response.status_code == 200
    body = response.json()
    assert body["proxy_address"] is None
    assert "dependency_graph_viz" not in body


# ---------------------------------------------------------------------------
# 5. GET /api/analyses/{run_name} — proxy detail inherits impl artifacts
# ---------------------------------------------------------------------------


@patch("api.get_all_artifacts")
@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_analysis_detail_proxy_inherits_impl_artifacts(mock_session_cls, mock_get_artifact, mock_get_all_artifacts):
    """When loading a proxy job's detail, analysis artifacts (contract_analysis,
    effective_permissions, etc.) should be inherited from the impl child job.
    This is the reverse of the impl->proxy fallback for dependency artifacts."""
    client = _make_client()

    proxy_addr = "0x1111111111111111111111111111111111111111"
    impl_addr = "0x2222222222222222222222222222222222222222"
    proxy_job_id = uuid.uuid4()
    impl_job_id = uuid.uuid4()

    proxy_job = _fake_api_job(
        job_id=str(proxy_job_id),
        address=proxy_addr,
        name="MyProxy",
        status="completed",
        stage="done",
        request={"address": proxy_addr},
    )
    impl_job = _fake_api_job(
        job_id=str(impl_job_id),
        address=impl_addr,
        name="MyProxy: (impl)",
        status="completed",
        stage="done",
        request={"address": impl_addr, "proxy_address": proxy_addr},
    )

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    # First execute returns proxy_job (name lookup), second returns impl_job
    # (implementation address lookup from proxy->impl fallback)
    call_count = 0

    def route_execute(stmt, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        if call_count == 1:
            result.scalar_one_or_none.return_value = proxy_job
        else:
            result.scalar_one_or_none.return_value = impl_job
        return result

    mock_session.execute.side_effect = route_execute
    mock_session.get.return_value = None

    # Proxy job has only dependency artifacts (no analysis)
    mock_get_all_artifacts.return_value = {
        "dependencies": {"address": proxy_addr, "dependencies": {}},
        "dependency_graph_viz": {"nodes": [], "edges": []},
    }

    # get_artifact returns contract_flags for proxy, and impl artifacts
    impl_analysis = {
        "subject": {"name": "VaultImpl"},
        "summary": {"control_model": "authority"},
    }
    impl_permissions = {"functions": [{"function": "pause()", "selector": "0x12"}]}
    impl_all_artifacts = {
        "contract_analysis": impl_analysis,
        "effective_permissions": impl_permissions,
        "principal_labels": {"principals": []},
        "resolved_control_graph": {"nodes": [], "edges": []},
        "control_snapshot": {"controller_values": {}},
        "analysis_report": "Some report text",
    }

    def fake_get_artifact(session, jid, name):
        if str(jid) == str(proxy_job_id) and name == "contract_flags":
            return {"is_proxy": True, "proxy_type": "eip1967", "implementation": impl_addr}
        return None

    mock_get_artifact.side_effect = fake_get_artifact

    # Second call to get_all_artifacts is for the impl job
    call_count_artifacts = 0
    original_return = mock_get_all_artifacts.return_value

    def fake_get_all(session, jid):
        nonlocal call_count_artifacts
        call_count_artifacts += 1
        if call_count_artifacts == 1:
            return original_return  # proxy artifacts
        return impl_all_artifacts  # impl artifacts

    mock_get_all_artifacts.side_effect = fake_get_all

    response = client.get("/api/analyses/MyProxy")

    assert response.status_code == 200
    body = response.json()

    # Should have proxy's own dependency artifacts
    assert "dependencies" in body
    assert "dependency_graph_viz" in body

    # Should have inherited impl's analysis artifacts
    assert body["contract_analysis"]["summary"]["control_model"] == "authority"
    assert body["effective_permissions"]["functions"][0]["function"] == "pause()"
    assert "principal_labels" in body
    assert "resolved_control_graph" in body
    assert body["analysis_report"] == "Some report text"
    assert body["contract_name"] == "VaultImpl"
    assert body["implementation_address"] == impl_addr


# ---------------------------------------------------------------------------
# Watched proxy endpoints
# ---------------------------------------------------------------------------

PROXY_ADDR = "0x" + "ab" * 20


def _fake_watched_proxy(**overrides):
    """Build a MagicMock that behaves like db.models.WatchedProxy."""
    proxy = MagicMock()
    proxy.id = overrides.get("id", uuid.uuid4())
    proxy.proxy_address = overrides.get("proxy_address", PROXY_ADDR)
    proxy.chain = overrides.get("chain", "ethereum")
    proxy.label = overrides.get("label", "TestProxy")
    proxy.proxy_type = overrides.get("proxy_type", "eip1967")
    proxy.needs_polling = overrides.get("needs_polling", False)
    proxy.last_known_implementation = overrides.get("last_known_implementation", "0x" + "cc" * 20)
    proxy.last_scanned_block = overrides.get("last_scanned_block", 100)
    proxy.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return proxy


@patch("services.monitoring.proxy_watcher.resolve_current_implementation", return_value="0x" + "cc" * 20)
@patch("services.discovery.classifier.classify_single", return_value={"type": "proxy", "proxy_type": "eip1967"})
@patch("services.monitoring.proxy_watcher.get_latest_block", return_value=12345)
@patch("api.SessionLocal")
def test_add_watched_proxy(mock_session_cls, mock_block, mock_classify, mock_resolve):
    """POST /api/watched-proxies creates a proxy watch and returns its details."""
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    proxy = _fake_watched_proxy(last_scanned_block=12345)
    mock_session.refresh.side_effect = lambda p: (
        setattr(p, "id", proxy.id) or setattr(p, "created_at", proxy.created_at)
    )
    # Make the committed proxy look like our fake for the response serialization
    mock_session.add.side_effect = lambda p: [
        setattr(p, attr, getattr(proxy, attr))
        for attr in (
            "id",
            "proxy_address",
            "chain",
            "label",
            "proxy_type",
            "needs_polling",
            "last_known_implementation",
            "last_scanned_block",
            "created_at",
        )
    ]

    response = client.post("/api/watched-proxies", json={"address": PROXY_ADDR})
    assert response.status_code == 200
    body = response.json()
    assert body["proxy_address"] == PROXY_ADDR
    assert body["proxy_type"] == "eip1967"
    assert body["last_scanned_block"] == 12345


def test_add_watched_proxy_invalid_address():
    """POST /api/watched-proxies rejects addresses not starting with 0x."""
    client = _make_client()
    response = client.post("/api/watched-proxies", json={"address": "not_an_address_at_all_no_0x_prefix_here_xx"})
    assert response.status_code == 400


@patch("api.SessionLocal")
def test_add_watched_proxy_ssrf_blocked(mock_session_cls):
    """POST /api/watched-proxies rejects internal RPC URLs."""
    client = _make_client()
    for url in ["http://localhost:8545", "http://127.0.0.1:8545", "http://10.0.0.1:8545"]:
        response = client.post(
            "/api/watched-proxies",
            json={"address": PROXY_ADDR, "rpc_url": url},
        )
        assert response.status_code == 400, f"Expected 400 for {url}"
        assert "internal" in response.json()["detail"].lower()


@patch("api.SessionLocal")
def test_list_watched_proxies(mock_session_cls):
    """GET /api/watched-proxies returns all watched proxies."""
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    proxy = _fake_watched_proxy()
    mock_session.execute.return_value.scalars.return_value.all.return_value = [proxy]

    response = client.get("/api/watched-proxies")
    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["proxy_address"] == PROXY_ADDR
    assert body[0]["proxy_type"] == "eip1967"


@patch("api.SessionLocal")
def test_remove_watched_proxy(mock_session_cls):
    """DELETE /api/watched-proxies/{id} removes a proxy."""
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    proxy = _fake_watched_proxy()
    mock_session.get.return_value = proxy

    response = client.delete(f"/api/watched-proxies/{proxy.id}")
    assert response.status_code == 200
    assert response.json()["status"] == "removed"
    mock_session.delete.assert_called_once_with(proxy)


@patch("api.SessionLocal")
def test_remove_watched_proxy_not_found(mock_session_cls):
    """DELETE /api/watched-proxies/{id} returns 404 when proxy doesn't exist."""
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)
    mock_session.get.return_value = None

    response = client.delete(f"/api/watched-proxies/{uuid.uuid4()}")
    assert response.status_code == 404


@patch("api.SessionLocal")
def test_list_proxy_events(mock_session_cls):
    """GET /api/proxy-events returns upgrade events."""
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    event = MagicMock()
    event.id = uuid.uuid4()
    event.watched_proxy_id = uuid.uuid4()
    event.block_number = 999
    event.tx_hash = "0x" + "ff" * 32
    event.old_implementation = "0x" + "aa" * 20
    event.new_implementation = "0x" + "bb" * 20
    event.event_type = "upgraded"
    event.detected_at = datetime(2026, 1, 2, tzinfo=timezone.utc)

    mock_session.execute.return_value.scalars.return_value.all.return_value = [event]

    response = client.get("/api/proxy-events")
    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["event_type"] == "upgraded"
    assert body[0]["block_number"] == 999
    assert body[0]["new_implementation"] == "0x" + "bb" * 20
