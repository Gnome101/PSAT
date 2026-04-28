"""Unit tests for utils.exa HTTP client."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils import exa


class _FakeResp:
    def __init__(self, *, status_code: int = 200, payload=None, text: str = ""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self):
        return self._payload


# ---------------------------------------------------------------------------
# error helpers
# ---------------------------------------------------------------------------


def test_normalize_error_full():
    err = exa.normalize_error("boom", status_code=429, retryable=True, detail="rate limited")
    assert err["provider"] == "exa"
    assert err["status_code"] == 429
    assert err["retryable"] is True
    assert err["detail"] == "rate limited"


def test_error_from_exception_exa_error():
    original = exa.normalize_error("oops", retryable=False)
    out = exa.error_from_exception(exa.ExaError(original))
    assert out == original


def test_error_from_exception_generic():
    out = exa.error_from_exception(KeyError("missing"))
    assert out["provider"] == "exa"
    assert out["retryable"] is False


# ---------------------------------------------------------------------------
# api key
# ---------------------------------------------------------------------------


def test_get_api_key_missing(monkeypatch):
    monkeypatch.delenv("EXA_API_KEY", raising=False)
    monkeypatch.setattr(exa, "load_dotenv", lambda *a, **kw: None)
    with pytest.raises(exa.ExaError) as ei:
        exa._get_api_key()
    assert "EXA_API_KEY" in ei.value.error["error"]


def test_get_api_key_present(monkeypatch):
    monkeypatch.setenv("EXA_API_KEY", "  abc  ")
    monkeypatch.setattr(exa, "load_dotenv", lambda *a, **kw: None)
    assert exa._get_api_key() == "abc"


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


def test_search_rejects_empty_query():
    with pytest.raises(ValueError):
        exa.search("   ", max_results=5)


def test_search_rejects_zero_results():
    with pytest.raises(ValueError):
        exa.search("q", max_results=0)


def test_search_rejects_unsupported_mode():
    with pytest.raises(ValueError):
        exa.search("q", max_results=5, mode="bogus")


@pytest.mark.parametrize(
    "alias,resolved", [("regular", "auto"), ("instant", "keyword"), ("auto", "auto"), ("deep-lite", "deep-lite")]
)
def test_search_mode_aliases(monkeypatch, alias, resolved):
    monkeypatch.setattr(exa, "_get_api_key", lambda: "k")
    captured = {}

    def fake_post(url, json, headers, timeout):
        captured["payload"] = json
        return _FakeResp(payload={"results": []})

    monkeypatch.setattr(exa.requests, "post", fake_post)
    exa.search("ether.fi", max_results=3, mode=alias)
    assert captured["payload"]["type"] == resolved
    assert captured["payload"]["numResults"] == 3
    assert captured["payload"]["query"] == "ether.fi"


def test_search_happy_path_normalizes(monkeypatch):
    monkeypatch.setattr(exa, "_get_api_key", lambda: "k")

    payload = {
        "results": [
            {
                "url": "https://a.example.com",
                "title": "  Title A  ",
                "text": "snip-a",
                "score": 0.9,
            },
            # text as dict
            {
                "url": "https://b.example.com",
                "title": "B",
                "text": {"text": "from-dict"},
                "score": 0.5,
            },
            # content fallback when no text
            {"url": "https://c.example.com", "content": "from-content"},
            # skipped: no url
            {"title": "no url"},
        ]
    }
    monkeypatch.setattr(exa.requests, "post", lambda *a, **kw: _FakeResp(payload=payload))
    out = exa.search("q", max_results=4)
    assert len(out) == 3
    assert out[0]["title"] == "Title A"
    assert out[0]["score"] == 0.9
    assert out[1]["content"] == "from-dict"
    assert out[2]["content"] == "from-content"


def test_search_include_text_false_omits_contents(monkeypatch):
    monkeypatch.setattr(exa, "_get_api_key", lambda: "k")
    captured = {}

    def fake_post(url, json, headers, timeout):
        captured["payload"] = json
        return _FakeResp(payload={"results": []})

    monkeypatch.setattr(exa.requests, "post", fake_post)
    exa.search("q", max_results=1, include_text=False)
    assert "contents" not in captured["payload"]


def test_search_network_error(monkeypatch):
    monkeypatch.setattr(exa, "_get_api_key", lambda: "k")

    def fake_post(*a, **kw):
        raise requests.ConnectionError("dropped")

    monkeypatch.setattr(exa.requests, "post", fake_post)
    with pytest.raises(exa.ExaError) as ei:
        exa.search("q", max_results=1)
    assert ei.value.error["retryable"] is True


@pytest.mark.parametrize("status,retryable", [(429, True), (502, True), (400, False), (401, False)])
def test_search_http_error(monkeypatch, status, retryable):
    monkeypatch.setattr(exa, "_get_api_key", lambda: "k")
    monkeypatch.setattr(
        exa.requests,
        "post",
        lambda *a, **kw: _FakeResp(status_code=status, text="boom"),
    )
    with pytest.raises(exa.ExaError) as ei:
        exa.search("q", max_results=1)
    assert ei.value.error["retryable"] is retryable
    assert ei.value.error["status_code"] == status


def test_search_truncates_content_to_1000(monkeypatch):
    monkeypatch.setattr(exa, "_get_api_key", lambda: "k")
    long_text = "y" * 5000
    monkeypatch.setattr(
        exa.requests,
        "post",
        lambda *a, **kw: _FakeResp(payload={"results": [{"url": "https://x.example.com", "text": long_text}]}),
    )
    out = exa.search("q", max_results=1)
    assert len(out[0]["content"]) == 1000


# ---------------------------------------------------------------------------
# deep_research
# ---------------------------------------------------------------------------


def test_deep_research_happy_path(monkeypatch):
    # time.sleep is imported inside the function — stub it via the module.
    import time as _time

    monkeypatch.setattr(_time, "sleep", lambda _s: None)
    monkeypatch.setattr(exa, "_get_api_key", lambda: "k")

    create_resp = _FakeResp(payload={"id": "task-123"})
    poll_resp = _FakeResp(
        payload={
            "status": "completed",
            "data": {"auditReports": [{"auditor": "Trail of Bits", "url": "https://example.com/a"}]},
        }
    )
    monkeypatch.setattr(exa.requests, "post", lambda *a, **kw: create_resp)
    monkeypatch.setattr(exa.requests, "get", lambda *a, **kw: poll_resp)

    out = exa.deep_research("find audits", timeout_seconds=60)
    assert out["task_id"] == "task-123"
    assert out["status"] == "completed"
    assert out["data"]["auditReports"][0]["url"] == "https://example.com/a"


def test_deep_research_create_http_error(monkeypatch):
    monkeypatch.setattr(exa, "_get_api_key", lambda: "k")
    monkeypatch.setattr(
        exa.requests,
        "post",
        lambda *a, **kw: _FakeResp(status_code=500, text="server boom"),
    )
    with pytest.raises(exa.ExaError) as ei:
        exa.deep_research("inst")
    assert ei.value.error["status_code"] == 500
    assert "create" in ei.value.error["error"]


def test_deep_research_no_task_id(monkeypatch):
    monkeypatch.setattr(exa, "_get_api_key", lambda: "k")
    monkeypatch.setattr(exa.requests, "post", lambda *a, **kw: _FakeResp(payload={"foo": "bar"}))
    with pytest.raises(exa.ExaError) as ei:
        exa.deep_research("inst")
    assert "no task id" in ei.value.error["error"]


def test_deep_research_poll_http_error(monkeypatch):
    import time as _time

    monkeypatch.setattr(_time, "sleep", lambda _s: None)
    monkeypatch.setattr(exa, "_get_api_key", lambda: "k")
    monkeypatch.setattr(exa.requests, "post", lambda *a, **kw: _FakeResp(payload={"id": "t1"}))
    monkeypatch.setattr(exa.requests, "get", lambda *a, **kw: _FakeResp(status_code=503, text="unavail"))
    with pytest.raises(exa.ExaError) as ei:
        exa.deep_research("inst", timeout_seconds=60)
    assert ei.value.error["status_code"] == 503
    assert "poll" in ei.value.error["error"]


def test_deep_research_failed_status(monkeypatch):
    import time as _time

    monkeypatch.setattr(_time, "sleep", lambda _s: None)
    monkeypatch.setattr(exa, "_get_api_key", lambda: "k")
    monkeypatch.setattr(exa.requests, "post", lambda *a, **kw: _FakeResp(payload={"id": "t1"}))
    monkeypatch.setattr(
        exa.requests,
        "get",
        lambda *a, **kw: _FakeResp(payload={"status": "failed", "error": "model down"}),
    )
    with pytest.raises(exa.ExaError) as ei:
        exa.deep_research("inst", timeout_seconds=60)
    assert "failed" in ei.value.error["error"]


def test_deep_research_timeout(monkeypatch):
    """When poll never returns terminal status, deadline expires and raises."""
    import time as _time

    # Fake monotonic that jumps past the deadline on the second tick.
    ticks = iter([0.0, 0.0, 100.0, 100.0])
    monkeypatch.setattr(_time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(_time, "sleep", lambda _s: None)
    monkeypatch.setattr(exa, "_get_api_key", lambda: "k")
    monkeypatch.setattr(exa.requests, "post", lambda *a, **kw: _FakeResp(payload={"id": "t1"}))
    monkeypatch.setattr(
        exa.requests,
        "get",
        lambda *a, **kw: _FakeResp(payload={"status": "running"}),
    )
    with pytest.raises(exa.ExaError) as ei:
        exa.deep_research("inst", timeout_seconds=10)
    assert "timed out" in ei.value.error["error"]
