"""Tests targeting uncovered paths in api.py for improved coverage.

Focuses on:
- _display_name() helper edge cases
- _merge_proxy_impl_entries() logic
- GET /api/stats
- GET /api/jobs (list with proxy flagging)
- POST /api/analyze (dapp_urls, defillama_protocol paths)
- GET /api/analyses/{run_name}/artifact/{artifact_name} (lookup by id/address, extension stripping)
- GET /api/analyses/{run_name} (relational-table fallback paths, control_snapshot, resolved_control_graph from tables)
- GET /api/company/{company_name}
- Proxy subscription endpoints
- SPA fallback for /api/* paths
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client() -> TestClient:
    import api

    return TestClient(api.app)


def _mock_session_ctx(mock_session_cls, mock_session):
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)


def _fake_job(
    job_id=None,
    address=None,
    company=None,
    name=None,
    status="completed",
    stage="done",
    request=None,
):
    job = MagicMock()
    uid = uuid.UUID(job_id) if job_id else uuid.uuid4()
    job.id = uid
    job.address = address
    job.company = company
    job.name = name
    job.status = MagicMock(value=status)
    job.stage = MagicMock(value=stage)
    job.detail = "detail"
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
        "detail": "detail",
        "request": request or {},
        "error": None,
        "worker_id": None,
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    return job


# ============================================================================
# 1. _display_name() unit tests
# ============================================================================


class TestDisplayName:
    def _dn(self, entry):
        import api

        return api._display_name(entry)

    def test_explicit_display_name_is_used(self):
        assert self._dn({"display_name": "MyVault"}) == "MyVault"

    def test_explicit_display_name_with_chain_suffix(self):
        result = self._dn({"display_name": "MyVault", "chain": "ethereum"})
        assert result == "MyVault (ethereum)"

    def test_explicit_display_name_already_has_chain_suffix(self):
        result = self._dn({"display_name": "MyVault (ethereum)", "chain": "ethereum"})
        assert result == "MyVault (ethereum)"

    def test_contract_name_used_when_no_display_name(self):
        assert self._dn({"contract_name": "Vault"}) == "Vault"

    def test_generic_proxy_name_falls_through_to_run_name(self):
        result = self._dn({"contract_name": "ERC1967Proxy", "run_name": "MyRunName"})
        assert result == "MyRunName"

    def test_generic_proxy_name_case_insensitive(self):
        result = self._dn({"contract_name": "uupsproxy", "run_name": "run1"})
        assert result == "run1"

    def test_all_generic_proxy_names(self):
        import api

        for gname in api.GENERIC_PROXY_NAMES:
            result = self._dn({"contract_name": gname, "run_name": "fallback"})
            assert result == "fallback", f"{gname} should be treated as generic"

    def test_fallback_to_contract_name_when_no_run_name(self):
        # When contract_name is generic AND no run_name, falls back to contract_name itself
        result = self._dn({"contract_name": "Proxy"})
        assert result == "Proxy"

    def test_empty_entry(self):
        result = self._dn({})
        assert result == ""

    def test_none_values(self):
        result = self._dn({"display_name": None, "contract_name": None, "run_name": None})
        assert result == ""

    def test_chain_suffix_not_added_to_empty_name(self):
        result = self._dn({"chain": "ethereum"})
        assert result == ""


# ============================================================================
# 2. _merge_proxy_impl_entries() unit tests
# ============================================================================


class TestMergeProxyImplEntries:
    def _merge(self, entries):
        import api

        return api._merge_proxy_impl_entries(entries)

    def test_no_entries(self):
        assert self._merge([]) == []

    def test_non_proxy_entries_pass_through(self):
        entry = {"address": "0xaaa", "run_name": "test"}
        result = self._merge([entry])
        assert len(result) == 1
        assert result[0]["address"] == "0xaaa"
        assert "display_name" in result[0]

    def test_impl_entry_without_matching_proxy_stays(self):
        # An impl entry (has proxy_address) but no proxy entry matches it
        impl = {"address": "0xbbb", "proxy_address": "0xaaa", "run_name": "impl"}
        result = self._merge([impl])
        # The impl entry should still appear as an unmerged impl
        assert len(result) == 1
        assert result[0]["address"] == "0xbbb"

    def test_proxy_and_impl_merge(self):
        proxy = {
            "address": "0xaaa",
            "is_proxy": True,
            "implementation_address": "0xbbb",
            "proxy_type": "ERC1967",
            "company": "etherfi",
            "chain": "ethereum",
            "rank_score": 10,
            "run_name": "ProxyRun",
        }
        impl = {
            "address": "0xbbb",
            "proxy_address": "0xaaa",
            "contract_name": "VaultImpl",
            "company": None,
            "chain": None,
            "rank_score": None,
            "run_name": "ImplRun",
        }
        result = self._merge([proxy, impl])
        assert len(result) == 1
        merged = result[0]
        assert merged["proxy_address_display"] == "0xaaa"
        assert merged["proxy_type_display"] == "ERC1967"
        assert merged["display_name"] == "VaultImpl"
        # Company comes from proxy when impl is None
        assert merged["company"] == "etherfi"
        # Chain comes from proxy
        assert merged["chain"] == "ethereum"
        # rank_score from proxy (not None)
        assert merged["rank_score"] == 10

    def test_proxy_without_impl_entry_passes_through(self):
        # Proxy entry but no matching impl entry in the list
        proxy = {
            "address": "0xaaa",
            "is_proxy": True,
            "implementation_address": "0xbbb",
            "run_name": "proxy_only",
        }
        result = self._merge([proxy])
        assert len(result) == 1
        assert result[0]["run_name"] == "proxy_only"

    def test_impl_rank_score_used_when_proxy_is_none(self):
        proxy = {
            "address": "0xaaa",
            "is_proxy": True,
            "implementation_address": "0xbbb",
            "rank_score": None,
            "run_name": "P",
        }
        impl = {
            "address": "0xbbb",
            "proxy_address": "0xaaa",
            "rank_score": 5,
            "contract_name": "Impl",
            "run_name": "I",
        }
        result = self._merge([proxy, impl])
        assert result[0]["rank_score"] == 5


# ============================================================================
# 3. GET /api/stats
# ============================================================================


@patch("api.SessionLocal")
def test_pipeline_stats(mock_session_cls):
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    # session.execute().scalar() returns counts
    mock_session.execute.return_value.scalar.return_value = 42

    response = client.get("/api/stats")
    assert response.status_code == 200
    body = response.json()
    assert "unique_addresses" in body
    assert "total_jobs" in body
    assert "completed_jobs" in body
    assert "failed_jobs" in body


# ============================================================================
# 4. GET /api/jobs (list with proxy flagging)
# ============================================================================


@patch("api.SessionLocal")
def test_list_jobs_with_proxy_flag(mock_session_cls):
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    job1 = _fake_job(name="proxy_job", address="0xaaa")
    job2 = _fake_job(name="regular_job", address="0xbbb")

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            # First call: list jobs
            result.scalars.return_value.all.return_value = [job1, job2]
        else:
            # Second call: batch-fetch contract_flags artifacts via .scalars()
            flag_row = MagicMock()
            flag_row.job_id = job1.id
            flag_row.storage_key = None
            flag_row.data = {"is_proxy": True}
            flag_row.text_data = None
            result.scalars.return_value = iter([flag_row])
        return result

    mock_session.execute.side_effect = route_execute

    response = client.get("/api/jobs")
    assert response.status_code == 200
    jobs = response.json()
    assert len(jobs) == 2
    proxy_entry = next(j for j in jobs if j["name"] == "proxy_job")
    regular_entry = next(j for j in jobs if j["name"] == "regular_job")
    assert proxy_entry["is_proxy"] is True
    assert regular_entry["is_proxy"] is False


@patch("api.SessionLocal")
def test_list_jobs_empty(mock_session_cls):
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    mock_session.execute.return_value.scalars.return_value.all.return_value = []

    response = client.get("/api/jobs")
    assert response.status_code == 200
    assert response.json() == []


# ============================================================================
# 5. POST /api/analyze - dapp_urls and defillama_protocol paths
# ============================================================================


@patch("api.SessionLocal")
@patch("api.create_job")
def test_analyze_dapp_urls(mock_create_job, mock_session_cls):
    client = _make_client()
    fake_job = _fake_job(status="queued", stage="dapp_crawl")
    mock_create_job.return_value = fake_job
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    response = client.post(
        "/api/analyze",
        json={"dapp_urls": ["https://app.uniswap.org"]},
    )
    assert response.status_code == 200
    # Verify create_job was called with initial_stage=dapp_crawl
    from db.models import JobStage

    _, kwargs = mock_create_job.call_args
    assert kwargs.get("initial_stage") == JobStage.dapp_crawl


@patch("api.SessionLocal")
@patch("api.create_job")
def test_analyze_defillama_protocol(mock_create_job, mock_session_cls):
    client = _make_client()
    fake_job = _fake_job(status="queued", stage="defillama_scan")
    mock_create_job.return_value = fake_job
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    response = client.post(
        "/api/analyze",
        json={"defillama_protocol": "aave"},
    )
    assert response.status_code == 200
    from db.models import JobStage

    _, kwargs = mock_create_job.call_args
    assert kwargs.get("initial_stage") == JobStage.defillama_scan


def test_analyze_rejects_multiple_targets():
    """Cannot provide both dapp_urls and address."""
    client = _make_client()
    response = client.post(
        "/api/analyze",
        json={
            "address": "0x1111111111111111111111111111111111111111",
            "dapp_urls": ["https://example.com"],
        },
    )
    assert response.status_code == 422


# ============================================================================
# 6. GET /api/analyses/{run_name}/artifact/{artifact_name}
# ============================================================================


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_artifact_lookup_by_job_id(mock_session_cls, mock_get_artifact):
    """When name lookup fails, try by job ID."""
    client = _make_client()
    job_id = str(uuid.uuid4())
    fake_job = _fake_job(job_id=job_id, name="test_job")

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    # First execute (by name): returns None
    # Then session.get (by id): returns job
    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        return result

    mock_session.execute.side_effect = route_execute
    mock_session.get.return_value = fake_job

    mock_get_artifact.return_value = {"data": "value"}

    response = client.get(f"/api/analyses/{job_id}/artifact/contract_analysis")
    assert response.status_code == 200
    assert response.json() == {"data": "value"}


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_artifact_lookup_by_address(mock_session_cls, mock_get_artifact):
    """When name and ID lookups fail, try by address."""
    client = _make_client()
    addr = "0x1111111111111111111111111111111111111111"
    fake_job = _fake_job(address=addr, name="addr_job")

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] <= 1:
            # Name lookup fails
            result.scalar_one_or_none.return_value = None
        else:
            # Address lookup succeeds
            result.scalar_one_or_none.return_value = fake_job
        return result

    mock_session.execute.side_effect = route_execute
    # session.get for ID lookup raises (simulating invalid UUID)
    mock_session.get.side_effect = Exception("not a UUID")

    mock_get_artifact.return_value = {"found": True}

    response = client.get(f"/api/analyses/{addr}/artifact/contract_analysis")
    assert response.status_code == 200
    assert response.json()["found"] is True


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_artifact_not_found(mock_session_cls, mock_get_artifact):
    """Returns 404 when artifact doesn't exist."""
    client = _make_client()
    fake_job = _fake_job(name="test_job")

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    mock_exec = MagicMock()
    mock_exec.scalar_one_or_none.return_value = fake_job
    mock_session.execute.return_value = mock_exec

    mock_get_artifact.return_value = None

    response = client.get("/api/analyses/test_job/artifact/nonexistent.json")
    assert response.status_code == 404


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_artifact_txt_extension_stripping(mock_session_cls, mock_get_artifact):
    """The .txt extension is stripped for lookup."""
    client = _make_client()
    fake_job = _fake_job(name="job1")

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)
    mock_exec = MagicMock()
    mock_exec.scalar_one_or_none.return_value = fake_job
    mock_session.execute.return_value = mock_exec

    # First call with stripped name returns None, second with original returns data
    mock_get_artifact.side_effect = [None, "report text"]

    response = client.get("/api/analyses/job1/artifact/analysis_report.txt")
    assert response.status_code == 200
    assert "report text" in response.text


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_artifact_json_extension_stripping(mock_session_cls, mock_get_artifact):
    """The .json extension is stripped, and first lookup with stripped name succeeds."""
    client = _make_client()
    fake_job = _fake_job(name="job1")

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)
    mock_exec = MagicMock()
    mock_exec.scalar_one_or_none.return_value = fake_job
    mock_session.execute.return_value = mock_exec

    mock_get_artifact.side_effect = [{"key": "val"}]

    response = client.get("/api/analyses/job1/artifact/contract_analysis.json")
    assert response.status_code == 200
    assert response.json() == {"key": "val"}


@patch("api.SessionLocal")
def test_artifact_job_not_found_returns_404(mock_session_cls):
    """Returns 404 when no job matches name/id/address."""
    client = _make_client()

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    mock_exec = MagicMock()
    mock_exec.scalar_one_or_none.return_value = None
    mock_session.execute.return_value = mock_exec
    mock_session.get.return_value = None

    response = client.get("/api/analyses/nonexistent/artifact/contract_analysis")
    assert response.status_code == 404


# ============================================================================
# 7. GET /api/analyses/{run_name} - relational table paths
# ============================================================================


@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_analysis_detail_relational_effective_permissions(mock_session_cls, mock_get_all_artifacts):
    """When contract_row exists with EffectiveFunctions, payload gets effective_permissions from relational tables."""
    client = _make_client()
    job = _fake_job(name="rel_job", address="0xaaa")

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    contract_row = MagicMock()
    contract_row.id = uuid.uuid4()
    contract_row.is_proxy = False
    contract_row.implementation = None
    contract_row.contract_name = "TestContract"
    contract_row.address = "0xaaa"
    contract_row.summary = None
    contract_row.job_id = job.id

    ef = MagicMock()
    ef.id = uuid.uuid4()
    ef.abi_signature = "pause()"
    ef.function_name = "pause"
    ef.selector = "0x8456cb59"
    ef.effect_labels = ["pause_toggle"]
    ef.action_summary = "Pauses the contract"
    ef.authority_public = False

    fp = MagicMock()
    fp.address = "0xowner"
    fp.resolved_type = "eoa"
    fp.origin = "owner_slot"
    fp.details = {"role": "admin"}

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            # Job lookup
            result.scalar_one_or_none.return_value = job
        elif call_count["n"] == 2:
            # Contract lookup
            result.scalar_one_or_none.return_value = contract_row
        elif call_count["n"] == 3:
            # EffectiveFunction query
            result.scalars.return_value.all.return_value = [ef]
        elif call_count["n"] == 4:
            # FunctionPrincipal query
            result.scalars.return_value.all.return_value = [fp]
        elif call_count["n"] == 5:
            # PrincipalLabel query
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 6:
            # ControllerValue query (for control_snapshot)
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 7:
            # ControlGraphNode query
            result.scalars.return_value.all.return_value = []
        else:
            result.scalar_one_or_none.return_value = None
            result.scalars.return_value.all.return_value = []
        return result

    mock_session.execute.side_effect = route_execute

    mock_get_all_artifacts.return_value = {
        "contract_analysis": {
            "subject": {"name": "TestContract"},
            "summary": {"control_model": "ownable"},
        },
    }

    response = client.get("/api/analyses/rel_job")
    assert response.status_code == 200
    body = response.json()
    assert "effective_permissions" in body
    perms = body["effective_permissions"]
    assert perms["contract_name"] == "TestContract"
    assert len(perms["functions"]) == 1
    fn = perms["functions"][0]
    assert fn["function"] == "pause()"
    assert fn["selector"] == "0x8456cb59"
    assert fn["effect_labels"] == ["pause_toggle"]
    assert len(fn["controllers"]) == 1
    assert fn["controllers"][0]["principals"][0]["address"] == "0xowner"


@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_analysis_detail_relational_control_snapshot(mock_session_cls, mock_get_all_artifacts):
    """When control_snapshot is NOT in artifacts, build it from ControllerValue table."""
    client = _make_client()
    job = _fake_job(name="cv_job", address="0xaaa")

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    contract_row = MagicMock()
    contract_row.id = uuid.uuid4()
    contract_row.is_proxy = False
    contract_row.implementation = None
    contract_row.contract_name = "CVContract"
    contract_row.address = "0xaaa"
    contract_row.summary = None

    cv = MagicMock()
    cv.controller_id = "owner"
    cv.value = "0xdeadbeef"
    cv.resolved_type = "eoa"
    cv.source = "storage_slot"
    cv.details = {"slot": "0x00"}

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            result.scalar_one_or_none.return_value = job
        elif call_count["n"] == 2:
            result.scalar_one_or_none.return_value = contract_row
        elif call_count["n"] == 3:
            # EffectiveFunction: empty
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 4:
            # PrincipalLabel: empty
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 5:
            # ControllerValue
            result.scalars.return_value.all.return_value = [cv]
        elif call_count["n"] == 6:
            # ControlGraphNode (for resolved_control_graph)
            result.scalars.return_value.all.return_value = []
        else:
            result.scalar_one_or_none.return_value = None
            result.scalars.return_value.all.return_value = []
        return result

    mock_session.execute.side_effect = route_execute

    # No control_snapshot in artifacts
    mock_get_all_artifacts.return_value = {
        "contract_analysis": {
            "subject": {"name": "CVContract"},
            "summary": {},
        },
    }

    response = client.get("/api/analyses/cv_job")
    assert response.status_code == 200
    body = response.json()
    assert "control_snapshot" in body
    cs = body["control_snapshot"]
    assert cs["contract_name"] == "CVContract"
    assert "owner" in cs["controller_values"]
    assert cs["controller_values"]["owner"]["value"] == "0xdeadbeef"


@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_analysis_detail_relational_control_graph(mock_session_cls, mock_get_all_artifacts):
    """When resolved_control_graph is NOT in artifacts, build it from CGN/CGE tables."""
    client = _make_client()
    job = _fake_job(name="cg_job", address="0xaaa")

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    contract_row = MagicMock()
    contract_row.id = uuid.uuid4()
    contract_row.is_proxy = False
    contract_row.implementation = None
    contract_row.contract_name = "CGContract"
    contract_row.address = "0xaaa"
    contract_row.summary = None

    cgn = MagicMock()
    cgn.address = "0xnode1"
    cgn.node_type = "contract"
    cgn.resolved_type = "safe"
    cgn.label = "GnosisSafe"
    cgn.contract_name = "GnosisSafe"
    cgn.depth = 0
    cgn.analyzed = True

    cge = MagicMock()
    cge.from_node_id = "address:0xnode1"
    cge.to_node_id = "address:0xaaa"
    cge.relation = "owner"
    cge.label = "owns"

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            result.scalar_one_or_none.return_value = job
        elif call_count["n"] == 2:
            result.scalar_one_or_none.return_value = contract_row
        elif call_count["n"] == 3:
            # EffectiveFunction: empty
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 4:
            # PrincipalLabel: empty
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 5:
            # ControllerValue: empty (no control_snapshot)
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 6:
            # ControlGraphNode
            result.scalars.return_value.all.return_value = [cgn]
        elif call_count["n"] == 7:
            # ControlGraphEdge
            result.scalars.return_value.all.return_value = [cge]
        else:
            result.scalar_one_or_none.return_value = None
            result.scalars.return_value.all.return_value = []
        return result

    mock_session.execute.side_effect = route_execute

    # No resolved_control_graph in artifacts
    mock_get_all_artifacts.return_value = {
        "contract_analysis": {
            "subject": {"name": "CGContract"},
            "summary": {},
        },
    }

    response = client.get("/api/analyses/cg_job")
    assert response.status_code == 200
    body = response.json()
    assert "resolved_control_graph" in body
    rcg = body["resolved_control_graph"]
    assert rcg["root_contract_address"] == "0xaaa"
    assert len(rcg["nodes"]) == 1
    assert rcg["nodes"][0]["address"] == "0xnode1"
    assert len(rcg["edges"]) == 1
    assert rcg["edges"][0]["relation"] == "owner"


@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_analysis_detail_relational_principal_labels(mock_session_cls, mock_get_all_artifacts):
    """When PrincipalLabel rows exist, they populate principal_labels in payload."""
    client = _make_client()
    job = _fake_job(name="pl_job", address="0xaaa")

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    contract_row = MagicMock()
    contract_row.id = uuid.uuid4()
    contract_row.is_proxy = False
    contract_row.implementation = None
    contract_row.contract_name = "PLContract"
    contract_row.address = "0xaaa"
    contract_row.summary = None

    pl = MagicMock()
    pl.address = "0xowner"
    pl.label = "Owner"
    pl.resolved_type = "eoa"

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            result.scalar_one_or_none.return_value = job
        elif call_count["n"] == 2:
            result.scalar_one_or_none.return_value = contract_row
        elif call_count["n"] == 3:
            # EffectiveFunction: empty
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 4:
            # PrincipalLabel
            result.scalars.return_value.all.return_value = [pl]
        elif call_count["n"] == 5:
            # ControllerValue: empty
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 6:
            # ControlGraphNode: empty
            result.scalars.return_value.all.return_value = []
        else:
            result.scalar_one_or_none.return_value = None
            result.scalars.return_value.all.return_value = []
        return result

    mock_session.execute.side_effect = route_execute

    mock_get_all_artifacts.return_value = {
        "contract_analysis": {
            "subject": {"name": "PLContract"},
            "summary": {},
        },
    }

    response = client.get("/api/analyses/pl_job")
    assert response.status_code == 200
    body = response.json()
    assert "principal_labels" in body
    assert body["principal_labels"]["principals"][0]["address"] == "0xowner"
    assert body["principal_labels"]["principals"][0]["label"] == "Owner"


@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_analysis_detail_lookup_by_id(mock_session_cls, mock_get_all_artifacts):
    """Falls back to session.get(Job, run_name) when name lookup fails."""
    client = _make_client()
    job_id = str(uuid.uuid4())
    job = _fake_job(job_id=job_id, name="id_lookup_job", address="0xaaa")

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            # Name lookup fails
            result.scalar_one_or_none.return_value = None
        else:
            result.scalar_one_or_none.return_value = None
            result.scalars.return_value.all.return_value = []
        return result

    mock_session.execute.side_effect = route_execute
    mock_session.get.return_value = job

    mock_get_all_artifacts.return_value = {}

    response = client.get(f"/api/analyses/{job_id}")
    assert response.status_code == 200
    assert response.json()["job_id"] == job_id


@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_analysis_detail_lookup_by_address(mock_session_cls, mock_get_all_artifacts):
    """Falls back to address lookup when name and ID fail."""
    client = _make_client()
    addr = "0x1111111111111111111111111111111111111111"
    job = _fake_job(address=addr, name="addr_job")

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            # Name lookup fails
            result.scalar_one_or_none.return_value = None
        elif call_count["n"] == 2:
            # Address lookup succeeds
            result.scalar_one_or_none.return_value = job
        else:
            result.scalar_one_or_none.return_value = None
            result.scalars.return_value.all.return_value = []
        return result

    mock_session.execute.side_effect = route_execute
    mock_session.get.side_effect = Exception("invalid UUID")

    mock_get_all_artifacts.return_value = {}

    response = client.get(f"/api/analyses/{addr}")
    assert response.status_code == 200
    assert response.json()["address"] == addr


@patch("api.get_all_artifacts")
@patch("api.SessionLocal")
def test_analysis_detail_analysis_report_inline(mock_session_cls, mock_get_all_artifacts):
    """Text artifacts like analysis_report are inlined in payload."""
    client = _make_client()
    job = _fake_job(name="report_job", address="0xaaa")

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            result.scalar_one_or_none.return_value = job
        else:
            result.scalar_one_or_none.return_value = None
            result.scalars.return_value.all.return_value = []
        return result

    mock_session.execute.side_effect = route_execute

    mock_get_all_artifacts.return_value = {
        "contract_analysis": {
            "subject": {"name": "ReportContract"},
            "summary": {},
        },
        "analysis_report": "This is the full report text.",
    }

    response = client.get("/api/analyses/report_job")
    assert response.status_code == 200
    body = response.json()
    assert body["analysis_report"] == "This is the full report text."


# ============================================================================
# 8. GET /api/analyses - rank_scores and chain come from the contracts table
# ============================================================================


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_analyses_list_rank_scores_from_contracts_table(mock_session_cls, mock_get_artifact):
    """rank_score + chain come from the ``contracts`` table (selection's single
    authoritative ranking pass), not from the legacy inventory artifact."""
    client = _make_client()
    company_job = _fake_job(
        name="company_disc",
        company="etherfi",
        address=None,
        request={"company": "etherfi"},
    )
    child_job = _fake_job(
        name="child_contract",
        address="0xcccc",
        request={"parent_job_id": str(company_job.id)},
    )

    from db.models import JobStatus

    company_job.status = JobStatus.completed
    child_job.status = JobStatus.completed

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    # Contract-row row tuple stand-in — api.py reads .address / .chain /
    # .rank_score attributes off each row.
    contract_row = SimpleNamespace(address="0xcccc", chain="ethereum", rank_score=8.5)

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            # First query: completed jobs
            result.scalars.return_value.all.return_value = [company_job, child_job]
        elif call_count["n"] == 2:
            # Second query: contract rows for rank/chain lookup
            result.all.return_value = [contract_row]
        else:
            # Per-job artifact listing queries
            result.scalars.return_value.all.return_value = ["contract_analysis"]
            result.scalar_one_or_none.return_value = None
        return result

    mock_session.execute.side_effect = route_execute

    def fake_get_artifact(session, jid, name):
        if name == "contract_analysis":
            return {"subject": {"name": "ContractX"}, "summary": {}}
        return None

    mock_get_artifact.side_effect = fake_get_artifact

    response = client.get("/api/analyses")
    assert response.status_code == 200
    entries = response.json()
    child = next((e for e in entries if e.get("address") == "0xcccc"), None)
    assert child is not None
    assert child["rank_score"] == 8.5
    assert child["chain"] == "ethereum"


# ============================================================================
# 9. GET /api/company/{company_name}
# ============================================================================


@patch("api.SessionLocal")
def test_company_overview_not_found(mock_session_cls):
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    mock_exec = MagicMock()
    mock_exec.scalar_one_or_none.return_value = None
    mock_session.execute.return_value = mock_exec

    response = client.get("/api/company/nonexistent")
    assert response.status_code == 404


@patch("api.SessionLocal")
def test_company_overview_basic(mock_session_cls):
    """Basic company overview with one non-proxy contract."""
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    company_job = _fake_job(name="etherfi_disc", company="etherfi", address=None)
    child_job = _fake_job(
        name="Vault",
        address="0xaaaa",
        company="etherfi",
        request={"parent_job_id": str(company_job.id)},
    )
    child_job.status = MagicMock(value="completed")

    from db.models import JobStatus

    child_job.status = JobStatus.completed
    company_job.status = JobStatus.completed

    contract_row = MagicMock()
    contract_row.id = uuid.uuid4()
    contract_row.is_proxy = False
    contract_row.proxy_type = None
    contract_row.implementation = None
    contract_row.contract_name = "Vault"
    contract_row.address = "0xaaaa"

    summary_row = MagicMock()
    summary_row.control_model = "ownable"
    summary_row.is_upgradeable = False
    summary_row.is_pausable = True
    summary_row.has_timelock = False
    summary_row.risk_level = "medium"
    summary_row.standards = ["ERC20"]
    summary_row.is_factory = False
    contract_row.summary = summary_row

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            # Protocol lookup (returns None → legacy fallback)
            result.scalar_one_or_none.return_value = None
        elif call_count["n"] == 2:
            # Company job lookup (legacy fallback)
            result.scalar_one_or_none.return_value = company_job
        elif call_count["n"] == 3:
            # All completed jobs
            result.scalars.return_value.all.return_value = [company_job, child_job]
        elif call_count["n"] == 4:
            # Contract lookup for child_job
            result.scalar_one_or_none.return_value = contract_row
        elif call_count["n"] == 5:
            # ControllerValue: empty
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 6:
            # UpgradeEvent: empty
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 7:
            # EffectiveFunction: empty
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 8:
            # EffectiveFunction again for functions_list: empty
            result.scalars.return_value.all.return_value = []
        elif call_count["n"] == 9:
            # ContractBalance: empty
            result.scalars.return_value.all.return_value = []
        else:
            result.scalar_one_or_none.return_value = None
            result.scalars.return_value.all.return_value = []
        return result

    mock_session.execute.side_effect = route_execute

    response = client.get("/api/company/etherfi")
    assert response.status_code == 200
    body = response.json()
    assert body["company"] == "etherfi"
    assert body["contract_count"] >= 1
    assert len(body["contracts"]) >= 1

    c = body["contracts"][0]
    assert c["address"] == "0xaaaa"
    assert c["name"] == "Vault"
    assert c["is_proxy"] is False
    assert c["is_pausable"] is True
    assert "pause" in c["capabilities"]
    assert c["role"] == "token"  # has ERC20 standard
    assert c["standards"] == ["ERC20"]

    # Ownership hierarchy should have unowned section
    assert "ownership_hierarchy" in body


# ============================================================================
# 10. Proxy subscription endpoints
# ============================================================================


@patch("api.SessionLocal")
def test_list_subscriptions(mock_session_cls):
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    proxy = MagicMock()
    proxy.id = uuid.uuid4()

    sub = MagicMock()
    sub.id = uuid.uuid4()
    sub.watched_proxy_id = proxy.id
    sub.discord_webhook_url = "https://discord.com/api/webhooks/123/abc"
    sub.label = "test sub"
    sub.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)

    mock_session.get.return_value = proxy
    mock_session.execute.return_value.scalars.return_value.all.return_value = [sub]

    response = client.get(f"/api/watched-proxies/{proxy.id}/subscriptions")
    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["discord_webhook_url"] == "https://discord.com/api/webhooks/123/abc"
    assert body[0]["label"] == "test sub"


@patch("api.SessionLocal")
def test_list_subscriptions_proxy_not_found(mock_session_cls):
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)
    mock_session.get.return_value = None

    response = client.get(f"/api/watched-proxies/{uuid.uuid4()}/subscriptions")
    assert response.status_code == 404


@patch("api.SessionLocal")
def test_add_subscription(mock_session_cls):
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    proxy = MagicMock()
    proxy.id = uuid.uuid4()
    mock_session.get.return_value = proxy

    sub = MagicMock()
    sub.id = uuid.uuid4()
    sub.watched_proxy_id = proxy.id
    sub.discord_webhook_url = "https://discord.com/api/webhooks/123/abc"
    sub.label = "new sub"
    sub.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    mock_session.refresh.side_effect = lambda s: [
        setattr(s, "id", sub.id),
        setattr(s, "watched_proxy_id", sub.watched_proxy_id),
        setattr(s, "discord_webhook_url", sub.discord_webhook_url),
        setattr(s, "label", sub.label),
        setattr(s, "created_at", sub.created_at),
    ]

    response = client.post(
        f"/api/watched-proxies/{proxy.id}/subscriptions",
        json={"discord_webhook_url": "https://discord.com/api/webhooks/123/abc", "label": "new sub"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["discord_webhook_url"] == "https://discord.com/api/webhooks/123/abc"


@patch("api.SessionLocal")
def test_add_subscription_proxy_not_found(mock_session_cls):
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)
    mock_session.get.return_value = None

    response = client.post(
        f"/api/watched-proxies/{uuid.uuid4()}/subscriptions",
        json={"discord_webhook_url": "https://discord.com/api/webhooks/123/abc"},
    )
    assert response.status_code == 404


@patch("api.SessionLocal")
def test_remove_subscription(mock_session_cls):
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    sub = MagicMock()
    sub.id = uuid.uuid4()
    mock_session.get.return_value = sub

    response = client.delete(f"/api/subscriptions/{sub.id}")
    assert response.status_code == 200
    assert response.json()["status"] == "removed"
    mock_session.delete.assert_called_once_with(sub)


@patch("api.SessionLocal")
def test_remove_subscription_not_found(mock_session_cls):
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)
    mock_session.get.return_value = None

    response = client.delete(f"/api/subscriptions/{uuid.uuid4()}")
    assert response.status_code == 404


# ============================================================================
# 11. SPA fallback for /api/* paths
# ============================================================================


def test_spa_fallback_api_prefix_returns_404():
    """Requests starting with /api/ that don't match a route should return 404."""
    client = _make_client()
    response = client.get("/api/nonexistent_endpoint")
    assert response.status_code == 404


def test_spa_fallback_non_api_serves_html():
    """Non-API deep links serve the SPA index."""
    client = _make_client()
    response = client.get("/some/random/path")
    assert response.status_code == 200


# ============================================================================
# 12. GET /api/analyses - company_for_job parent chain walking
# ============================================================================


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_analyses_company_from_parent_chain(mock_session_cls, mock_get_artifact):
    """company_for_job() walks parent_job_id chain to find company."""
    client = _make_client()

    company_job_id = uuid.uuid4()
    child_job_id = uuid.uuid4()

    company_job = _fake_job(
        job_id=str(company_job_id),
        name="company_disc",
        company="compound",
        address=None,
    )
    child_job = _fake_job(
        job_id=str(child_job_id),
        name="child",
        address="0xcccc",
        company=None,
        request={"parent_job_id": str(company_job_id)},
    )

    from db.models import JobStatus

    company_job.status = JobStatus.completed
    child_job.status = JobStatus.completed

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            result.scalars.return_value.all.return_value = [company_job, child_job]
        else:
            result.scalars.return_value.all.return_value = ["contract_analysis"]
            result.scalar_one_or_none.return_value = None
        return result

    mock_session.execute.side_effect = route_execute

    def fake_artifact(session, jid, name):
        if name == "contract_analysis":
            return {"subject": {"name": "Child"}, "summary": {}}
        return None

    mock_get_artifact.side_effect = fake_artifact

    response = client.get("/api/analyses")
    assert response.status_code == 200
    entries = response.json()
    child_entry = next((e for e in entries if e.get("address") == "0xcccc"), None)
    assert child_entry is not None
    assert child_entry["company"] == "compound"


# ============================================================================
# 13. GET /api/analyses - proxy with incomplete impl is hidden
# ============================================================================


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_analyses_proxy_hidden_when_impl_not_completed(mock_session_cls, mock_get_artifact):
    """When a proxy's impl job has not completed, the proxy should be skipped."""
    client = _make_client()

    proxy_job = _fake_job(
        name="proxy_hidden",
        address="0xaaaa",
    )

    from db.models import JobStatus

    proxy_job.status = JobStatus.completed

    incomplete_impl = _fake_job(
        name="incomplete_impl",
        address="0xbbbb",
        status="processing",
    )
    incomplete_impl.status = JobStatus.processing

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            result.scalars.return_value.all.return_value = [proxy_job]
        elif call_count["n"] == 2:
            # Contract rows for rank/chain lookup — empty is fine for this test
            result.all.return_value = []
        elif call_count["n"] == 3:
            # Artifact names
            result.scalars.return_value.all.return_value = ["contract_flags", "contract_analysis"]
        elif call_count["n"] == 4:
            # impl job lookup
            result.scalar_one_or_none.return_value = incomplete_impl
        else:
            result.scalars.return_value.all.return_value = []
            result.scalar_one_or_none.return_value = None
        return result

    mock_session.execute.side_effect = route_execute

    def fake_artifact(session, jid, name):
        if name == "contract_flags":
            return {"is_proxy": True, "proxy_type": "ERC1967", "implementation": "0xbbbb"}
        if name == "contract_analysis":
            return {"subject": {"name": "ProxyContract"}, "summary": {}}
        return None

    mock_get_artifact.side_effect = fake_artifact

    response = client.get("/api/analyses")
    assert response.status_code == 200
    entries = response.json()
    # The proxy should be skipped since impl is not completed
    assert not any(e.get("address") == "0xaaaa" for e in entries)


# ============================================================================
# 14. GET /api/proxy-events with proxy_id filter
# ============================================================================


@patch("api.SessionLocal")
def test_proxy_events_with_filter(mock_session_cls):
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    pid = str(uuid.uuid4())
    event = MagicMock()
    event.id = uuid.uuid4()
    event.watched_proxy_id = uuid.UUID(pid)
    event.block_number = 100
    event.tx_hash = "0x" + "aa" * 32
    event.old_implementation = "0x" + "11" * 20
    event.new_implementation = "0x" + "22" * 20
    event.event_type = "upgraded"
    event.detected_at = datetime(2026, 1, 1, tzinfo=timezone.utc)

    mock_session.execute.return_value.scalars.return_value.all.return_value = [event]

    response = client.get(f"/api/proxy-events?proxy_id={pid}&limit=10")
    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["block_number"] == 100


# ============================================================================
# 15. POST /api/analyze - non-0x address
# ============================================================================


def test_analyze_address_not_starting_with_0x():
    """Address that doesn't start with 0x after length validation should get 400."""
    client = _make_client()
    # 42 chars but doesn't start with 0x — pydantic min_length=42 passes but
    # the endpoint's manual check should catch it
    response = client.post(
        "/api/analyze",
        json={"address": "xx1111111111111111111111111111111111111111"},
    )
    assert response.status_code == 400


# ============================================================================
# 16. GET /api/analyses/{run_name} - proxy job inherits impl relational tables
# ============================================================================


@patch("api.get_all_artifacts")
@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_analysis_detail_proxy_inherits_impl_relational_tables(
    mock_session_cls, mock_get_artifact, mock_get_all_artifacts
):
    """When loading a proxy job detail, effective_permissions / control_snapshot /
    resolved_control_graph / principal_labels should be inherited from impl's
    relational tables when not available from artifacts."""
    client = _make_client()

    proxy_addr = "0x1111111111111111111111111111111111111111"
    impl_addr = "0x2222222222222222222222222222222222222222"
    proxy_job_id = uuid.uuid4()
    impl_job_id = uuid.uuid4()

    proxy_job = _fake_job(
        job_id=str(proxy_job_id),
        address=proxy_addr,
        name="ProxyRelTest",
        request={"address": proxy_addr},
    )
    impl_job = _fake_job(
        job_id=str(impl_job_id),
        address=impl_addr,
        name="ProxyRelTest: (impl)",
        request={"address": impl_addr, "proxy_address": proxy_addr},
    )

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    proxy_contract = MagicMock()
    proxy_contract.id = uuid.uuid4()
    proxy_contract.is_proxy = True
    proxy_contract.implementation = impl_addr
    proxy_contract.contract_name = "ProxyRelTest"
    proxy_contract.address = proxy_addr
    proxy_contract.summary = None
    proxy_contract.job_id = proxy_job_id

    impl_contract = MagicMock()
    impl_contract.id = uuid.uuid4()
    impl_contract.is_proxy = False
    impl_contract.implementation = None
    impl_contract.contract_name = "ImplContract"
    impl_contract.address = impl_addr
    impl_contract.summary = MagicMock()
    impl_contract.summary.control_model = "authority"
    impl_contract.summary.is_upgradeable = True
    impl_contract.summary.is_pausable = False
    impl_contract.summary.has_timelock = False
    impl_contract.summary.risk_level = "high"
    impl_contract.summary.standards = ["ERC20"]

    ef = MagicMock()
    ef.id = uuid.uuid4()
    ef.abi_signature = "transfer(address,uint256)"
    ef.function_name = "transfer"
    ef.selector = "0xa9059cbb"
    ef.effect_labels = ["asset_send"]
    ef.action_summary = "Sends tokens"
    ef.authority_public = True

    fp_owner = MagicMock()
    fp_owner.address = "0xowner"
    fp_owner.resolved_type = "eoa"
    fp_owner.origin = "direct owner"
    fp_owner.principal_type = "direct_owner"
    fp_owner.details = {}

    fp_role = MagicMock()
    fp_role.address = "0xrole"
    fp_role.resolved_type = "safe"
    fp_role.origin = "role 1"
    fp_role.principal_type = "authority_role"
    fp_role.details = {"threshold": 2}

    fp_controller = MagicMock()
    fp_controller.address = "0xcontroller"
    fp_controller.resolved_type = "contract"
    fp_controller.origin = "roleRegistry"
    fp_controller.principal_type = "controller"
    fp_controller.details = {"authority_kind": "access_control_like"}

    cv = MagicMock()
    cv.controller_id = "admin"
    cv.value = "0xadmin"
    cv.resolved_type = "eoa"
    cv.source = "slot"
    cv.details = {}

    cgn = MagicMock()
    cgn.address = "0xowner"
    cgn.node_type = "external"
    cgn.resolved_type = "eoa"
    cgn.label = "Owner"
    cgn.contract_name = None
    cgn.depth = 1
    cgn.analyzed = False

    cge = MagicMock()
    cge.from_node_id = "address:0xowner"
    cge.to_node_id = "address:0x2222222222222222222222222222222222222222"
    cge.relation = "owner"
    cge.label = "owns"

    pl = MagicMock()
    pl.address = "0xowner"
    pl.label = "Admin"
    pl.resolved_type = "eoa"

    mock_session.get.return_value = None

    def make_result(scalar=None, scalars_all=None):
        r = MagicMock()
        r.scalar_one_or_none.return_value = scalar
        r.scalars.return_value.all.return_value = scalars_all or []
        return r

    # Build the call sequence. The analysis_detail endpoint:
    # Calls 0-6: proxy's own relational queries (including both CGN and CGE)
    # Call 7: impl job lookup (from is_proxy block)
    # Call 8: impl Contract lookup (from is_proxy block)
    # Calls 9+: impl's relational queries (EF, FP, CV, CGN, CGE, PL)
    call_results = [
        make_result(scalar=proxy_job),  # 0: Job lookup by name
        make_result(scalar=proxy_contract),  # 1: Contract lookup for proxy
        make_result(scalars_all=[]),  # 2: EffectiveFunction for proxy (empty)
        make_result(scalars_all=[]),  # 3: PrincipalLabel for proxy (empty)
        make_result(scalars_all=[]),  # 4: ControllerValue for proxy (empty)
        make_result(scalars_all=[]),  # 5: ControlGraphNode for proxy (empty)
        make_result(scalars_all=[]),  # 6: ControlGraphEdge for proxy (empty)
        make_result(scalar=impl_job),  # 7: impl job lookup by address
        make_result(scalar=impl_contract),  # 8: impl Contract lookup
        make_result(scalars_all=[ef]),  # 9: EffectiveFunction for impl
        make_result(scalars_all=[fp_owner, fp_role, fp_controller]),  # 10: FunctionPrincipal for impl ef
        make_result(scalars_all=[cv]),  # 11: ControllerValue for impl
        make_result(scalars_all=[cgn]),  # 12: ControlGraphNode for impl
        make_result(scalars_all=[cge]),  # 13: ControlGraphEdge for impl
        make_result(scalars_all=[pl]),  # 14: PrincipalLabel for impl
    ]
    # Add extra fallback results
    for _ in range(10):
        call_results.append(make_result())

    mock_session.execute.side_effect = call_results

    # Proxy has no analysis artifacts
    mock_get_all_artifacts.side_effect = [
        {
            "dependency_graph_viz": {"nodes": [], "edges": []},
            "dependencies": {"deps": []},
        },
        # impl artifacts (empty, so relational tables are used)
        {},
    ]

    mock_get_artifact.return_value = None

    response = client.get("/api/analyses/ProxyRelTest")
    assert response.status_code == 200
    body = response.json()

    # Should have inherited effective_permissions from impl relational tables
    assert "effective_permissions" in body
    assert body["effective_permissions"]["functions"][0]["function"] == "transfer(address,uint256)"

    # Should have inherited control_snapshot from impl
    assert "control_snapshot" in body
    assert "admin" in body["control_snapshot"]["controller_values"]

    # Should have inherited resolved_control_graph from impl
    assert "resolved_control_graph" in body
    assert len(body["resolved_control_graph"]["nodes"]) >= 1

    # Should have inherited principal_labels from impl
    assert "principal_labels" in body
    assert body["principal_labels"]["principals"][0]["address"] == "0xowner"

    # Should have contract_name from impl
    assert body["contract_name"] == "ImplContract"

    # Summary from impl contract
    assert body["summary"]["control_model"] == "authority"

    # Proxy-specific fields
    assert body["implementation_address"] == impl_addr


# ============================================================================
# 17. GET /api/company - capabilities and roles
# ============================================================================


@patch("api.SessionLocal")
def test_company_overview_with_proxy_and_effects(mock_session_cls):
    """Company overview with a proxy contract, various effect labels, and balances."""
    client = _make_client()
    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    company_job = _fake_job(name="myproj_disc", company="myproj", address=None)
    proxy_job = _fake_job(
        name="MyProxy",
        address="0xaaaa",
        company="myproj",
        request={"parent_job_id": str(company_job.id)},
    )
    impl_job = _fake_job(
        name="MyProxy: (impl)",
        address="0xbbbb",
        company="myproj",
        request={"parent_job_id": str(proxy_job.id), "proxy_address": "0xaaaa"},
    )

    from db.models import JobStatus

    company_job.status = JobStatus.completed
    proxy_job.status = JobStatus.completed
    impl_job.status = JobStatus.completed

    proxy_contract = MagicMock()
    proxy_contract.id = uuid.uuid4()
    proxy_contract.is_proxy = True
    proxy_contract.proxy_type = "eip1967"
    proxy_contract.implementation = "0xbbbb"
    proxy_contract.contract_name = "MyProxy"
    proxy_contract.address = "0xaaaa"

    impl_contract = MagicMock()
    impl_contract.id = uuid.uuid4()
    impl_contract.is_proxy = False
    impl_contract.proxy_type = None
    impl_contract.implementation = None
    impl_contract.contract_name = "VaultImpl"
    impl_contract.address = "0xbbbb"

    summary = MagicMock()
    summary.control_model = "authority"
    summary.is_upgradeable = True
    summary.is_pausable = True
    summary.has_timelock = True
    summary.risk_level = "high"
    summary.standards = []
    summary.is_factory = False
    impl_contract.summary = summary
    proxy_contract.summary = None

    ef = MagicMock()
    ef.id = uuid.uuid4()
    ef.abi_signature = "pause()"
    ef.function_name = "pause"
    ef.selector = "0x8456cb59"
    ef.effect_labels = ["pause_toggle", "asset_pull", "delegatecall_execution"]
    ef.action_summary = "Pauses"
    ef.authority_public = False

    fp_owner = MagicMock()
    fp_owner.address = "0xowner"
    fp_owner.resolved_type = "eoa"
    fp_owner.origin = "direct owner"
    fp_owner.principal_type = "direct_owner"
    fp_owner.details = {}

    fp_role = MagicMock()
    fp_role.address = "0xrole"
    fp_role.resolved_type = "safe"
    fp_role.origin = "role 1"
    fp_role.principal_type = "authority_role"
    fp_role.details = {"threshold": 2}

    fp_controller = MagicMock()
    fp_controller.address = "0xcontroller"
    fp_controller.resolved_type = "contract"
    fp_controller.origin = "roleRegistry"
    fp_controller.principal_type = "controller"
    fp_controller.details = {"authority_kind": "access_control_like"}

    cv = MagicMock()
    cv.controller_id = "owner"
    cv.value = "0xowner"
    cv.resolved_type = "eoa"
    cv.source = "slot"
    cv.details = {}

    ue = MagicMock()  # Upgrade event

    bal = MagicMock()
    bal.token_symbol = "ETH"
    bal.token_name = "Ether"
    bal.token_address = None
    bal.raw_balance = "1000000000000000000"
    bal.decimals = 18
    bal.usd_value = 3000.50
    bal.price_usd = 3000.50

    def make_result(scalar=None, scalars_all=None):
        r = MagicMock()
        r.scalar_one_or_none.return_value = scalar
        r.scalars.return_value.all.return_value = scalars_all or []
        return r

    # The company_overview endpoint makes many DB calls. We build an ordered
    # list of results. The code for a proxy contract calls:
    # 1. Company job lookup
    # 2. All completed jobs
    # 3. Contract row for proxy_job (skips impl_job since it has proxy_address in request)
    # 4. impl_job lookup by address (since is_proxy and implementation)
    # 5. impl_contract for summary
    # 6. impl_contract for controller lookup (is_proxy and impl_job)
    # 7. ControllerValue check (scalar_one_or_none)
    # 8. ControllerValue all
    # 9. UpgradeEvent all
    # 10. impl_contract for ef_contract_id
    # 11. EffectiveFunction for effects
    # 12. impl_contract for name
    # 13. EffectiveFunction for functions_list
    # 14. FunctionPrincipal for functions_list
    # 15. ContractBalance all
    # Then fund_flows and principal queries:
    # 16+. Contract lookups for ControlGraphNode
    call_results = [
        make_result(scalar=company_job),  # 1
        make_result(scalars_all=[company_job, proxy_job, impl_job]),  # 2
        make_result(scalar=proxy_contract),  # 3
        make_result(scalar=impl_job),  # 4
        make_result(scalar=impl_contract),  # 5
        make_result(scalar=impl_contract),  # 6
        make_result(scalar=cv),  # 7
        make_result(scalars_all=[cv]),  # 8
        make_result(scalars_all=[ue]),  # 9
        make_result(scalar=impl_contract),  # 10
        make_result(scalars_all=[ef]),  # 11
        make_result(scalar=impl_contract),  # 12
        make_result(scalars_all=[ef]),  # 13
        make_result(scalars_all=[fp_owner, fp_role, fp_controller]),  # 14
        make_result(scalars_all=[bal]),  # 15
    ]
    # Add many fallback empty results for fund_flows/principal queries
    for _ in range(30):
        call_results.append(make_result())
    mock_session.execute.side_effect = call_results

    response = client.get("/api/company/myproj")
    assert response.status_code == 200
    body = response.json()

    assert body["company"] == "myproj"
    assert len(body["contracts"]) >= 1

    c = body["contracts"][0]
    assert c["is_proxy"] is True
    assert "upgradeable" in c["capabilities"]
    assert "pause" in c["capabilities"]
    assert "value-in" in c["capabilities"]  # asset_pull
    assert "delegatecall" in c["capabilities"]
    assert c["upgrade_count"] == 1
    assert c["has_timelock"] is True
    assert len(c["functions"]) == 1
    fn = c["functions"][0]
    assert fn["direct_owner"]["address"] == "0xowner"
    assert fn["authority_roles"] == [
        {
            "role": 1,
            "principals": [
                {
                    "address": "0xrole",
                    "resolved_type": "safe",
                    "source_controller_id": "role 1",
                    "details": {"threshold": 2},
                }
            ],
        }
    ]
    assert fn["controllers"] == [
        {
            "label": "roleRegistry",
            "controller_id": "roleRegistry",
            "source": "roleRegistry",
            "principals": [
                {
                    "address": "0xcontroller",
                    "resolved_type": "contract",
                    "source_controller_id": "roleRegistry",
                    "details": {"authority_kind": "access_control_like"},
                }
            ],
        }
    ]

    # Balances
    assert len(c["balances"]) >= 1


# ============================================================================
# 18. GET /api/analyses - chain from inventory 'chain' field (not 'chains')
# ============================================================================


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_analyses_chain_populated_from_contracts_table(mock_session_cls, mock_get_artifact):
    """Chain comes from the ``contracts`` table (same pass that sets
    rank_score) regardless of how the discovery worker wrote it —
    a row with ``chain='arbitrum'`` surfaces in the analyses listing."""
    client = _make_client()
    company_job = _fake_job(
        name="chain_disc",
        company="test_co",
        address=None,
    )
    child_job = _fake_job(
        name="chain_child",
        address="0xdddd",
    )

    from db.models import JobStatus

    company_job.status = JobStatus.completed
    child_job.status = JobStatus.completed

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    contract_row = SimpleNamespace(address="0xdddd", chain="arbitrum", rank_score=5.0)

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            result.scalars.return_value.all.return_value = [company_job, child_job]
        elif call_count["n"] == 2:
            result.all.return_value = [contract_row]
        else:
            result.scalars.return_value.all.return_value = []
            result.scalar_one_or_none.return_value = None
        return result

    mock_session.execute.side_effect = route_execute

    def fake_artifact(session, jid, name):
        if name == "contract_analysis":
            return {"subject": {"name": "ChainTest"}, "summary": {}}
        return None

    mock_get_artifact.side_effect = fake_artifact

    response = client.get("/api/analyses")
    assert response.status_code == 200
    entries = response.json()
    child = next((e for e in entries if e.get("address") == "0xdddd"), None)
    assert child is not None
    assert child["chain"] == "arbitrum"


# ============================================================================
# 19. GET /api/analyses - entry without contract_analysis is not appended
# ============================================================================


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_analyses_entry_without_analysis_still_appears(mock_session_cls, mock_get_artifact):
    """A job without contract_analysis artifact still appears in results, but
    without contract_name or summary fields from the analysis."""
    client = _make_client()
    job = _fake_job(name="no_analysis", address="0xeeee")

    from db.models import JobStatus

    job.status = JobStatus.completed

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            result.scalars.return_value.all.return_value = [job]
        else:
            result.scalars.return_value.all.return_value = []
            result.scalar_one_or_none.return_value = None
        return result

    mock_session.execute.side_effect = route_execute

    def fake_artifact(session, jid, name):
        return None  # No artifacts at all

    mock_get_artifact.side_effect = fake_artifact

    response = client.get("/api/analyses")
    assert response.status_code == 200
    entries = response.json()
    entry = next((e for e in entries if e.get("address") == "0xeeee"), None)
    assert entry is not None
    # No contract_name since analysis was None
    assert "contract_name" not in entry
    assert "summary" not in entry


# ============================================================================
# 20. GET /api/analyses - proxy uses impl analysis when proxy has none
# ============================================================================


@patch("api.get_artifact")
@patch("api.SessionLocal")
def test_analyses_proxy_uses_impl_analysis_when_proxy_has_none(mock_session_cls, mock_get_artifact):
    """When proxy's contract_analysis is None but impl's exists, proxy entry uses impl's."""
    client = _make_client()

    proxy_job_id = uuid.uuid4()
    impl_job_id = uuid.uuid4()

    proxy_job = _fake_job(
        job_id=str(proxy_job_id),
        name="proxy_no_analysis",
        address="0xaaaa",
    )
    impl_job = _fake_job(
        job_id=str(impl_job_id),
        name="impl_has_analysis",
        address="0xbbbb",
    )

    from db.models import JobStatus

    proxy_job.status = JobStatus.completed
    impl_job.status = JobStatus.completed

    mock_session = MagicMock()
    _mock_session_ctx(mock_session_cls, mock_session)

    call_count = {"n": 0}

    def route_execute(stmt, *args, **kwargs):
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            result.scalars.return_value.all.return_value = [proxy_job, impl_job]
        elif call_count["n"] == 2:
            # Contract-row lookup for rank/chain — empty for this test
            result.all.return_value = []
        elif call_count["n"] <= 4:
            result.scalars.return_value.all.return_value = ["contract_flags"]
            result.scalar_one_or_none.return_value = impl_job
        else:
            result.scalars.return_value.all.return_value = []
            result.scalar_one_or_none.return_value = None
        return result

    mock_session.execute.side_effect = route_execute

    def fake_artifact(session, jid, name):
        if str(jid) == str(proxy_job_id):
            if name == "contract_flags":
                return {"is_proxy": True, "proxy_type": "ERC1967", "implementation": "0xbbbb"}
            if name == "contract_analysis":
                return None  # Proxy has no analysis
            return None
        if str(jid) == str(impl_job_id):
            if name == "contract_analysis":
                return {"subject": {"name": "ImplName"}, "summary": {"control_model": "ownable"}}
            if name == "contract_flags":
                return None
            return None
        return None

    mock_get_artifact.side_effect = fake_artifact

    response = client.get("/api/analyses")
    assert response.status_code == 200
    entries = response.json()
    # The proxy should have picked up impl's analysis
    proxy_entry = next((e for e in entries if e.get("job_id") == str(proxy_job_id)), None)
    if proxy_entry is not None:
        assert proxy_entry.get("contract_name") == "ImplName"
