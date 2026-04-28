"""Unit tests for utils.brave HTTP client."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils import brave


class _FakeResp:
    def __init__(self, *, status_code: int = 200, payload=None, text: str = ""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self):
        return self._payload


def test_normalize_error_minimal():
    err = brave.normalize_error("boom")
    assert err == {"provider": "brave", "error": "boom"}


def test_normalize_error_full():
    err = brave.normalize_error("http 500", status_code=500, retryable=True, detail="server exploded")
    assert err["status_code"] == 500
    assert err["retryable"] is True
    assert err["detail"] == "server exploded"


def test_error_from_exception_brave_error():
    original = brave.normalize_error("oops", retryable=False)
    exc = brave.BraveError(original)
    out = brave.error_from_exception(exc)
    assert out == original
    # error_from_exception must return a copy, not the same dict
    out["error"] = "mutated"
    assert exc.error["error"] == "oops"


def test_error_from_exception_generic_exception():
    out = brave.error_from_exception(ValueError("bad input"))
    assert out["provider"] == "brave"
    assert out["error"] == "bad input"
    assert out["retryable"] is False


def test_get_api_key_missing(monkeypatch, tmp_path):
    monkeypatch.delenv("BRAVE_API_KEY", raising=False)
    # Point load_dotenv at an empty file so it can't pick up a real key.
    fake_env = tmp_path / ".env"
    fake_env.write_text("")
    monkeypatch.setattr(brave, "load_dotenv", lambda *a, **kw: None)
    with pytest.raises(brave.BraveError) as ei:
        brave._get_api_key()
    assert ei.value.error["retryable"] is False
    assert "BRAVE_API_KEY" in ei.value.error["error"]


def test_get_api_key_present(monkeypatch):
    monkeypatch.setenv("BRAVE_API_KEY", "  test-key  ")
    monkeypatch.setattr(brave, "load_dotenv", lambda *a, **kw: None)
    assert brave._get_api_key() == "test-key"


def test_search_rejects_empty_query():
    with pytest.raises(ValueError):
        brave.search("   ", max_results=5)


def test_search_rejects_zero_results():
    with pytest.raises(ValueError):
        brave.search("hello", max_results=0)


def test_search_happy_path(monkeypatch):
    monkeypatch.setattr(brave, "_get_api_key", lambda: "k")
    captured = {}

    def fake_get(url, params, headers, timeout):
        captured["url"] = url
        captured["params"] = params
        captured["headers"] = headers
        return _FakeResp(
            payload={
                "web": {
                    "results": [
                        {
                            "url": "https://example.com/a",
                            "title": "A",
                            "description": "snip-a",
                        },
                        {
                            "url": "https://example.com/b",
                            "title": "B",
                            "snippet": "snip-b",
                        },
                        # Skipped: no url
                        {"title": "no-url"},
                    ]
                }
            },
        )

    monkeypatch.setattr(brave.requests, "get", fake_get)
    out = brave.search("ether.fi audits", max_results=2)
    assert captured["url"] == brave.BRAVE_SEARCH_URL
    assert captured["params"]["q"] == "ether.fi audits"
    assert captured["params"]["count"] == 2
    assert captured["headers"]["X-Subscription-Token"] == "k"
    assert len(out) == 2
    assert out[0] == {
        "url": "https://example.com/a",
        "title": "A",
        "content": "snip-a",
        "score": None,
    }
    # Snippet field falls through when description is missing.
    assert out[1]["content"] == "snip-b"


def test_search_clamps_count_to_20(monkeypatch):
    monkeypatch.setattr(brave, "_get_api_key", lambda: "k")
    captured = {}

    def fake_get(url, params, headers, timeout):
        captured["params"] = params
        return _FakeResp(payload={"web": {"results": []}})

    monkeypatch.setattr(brave.requests, "get", fake_get)
    brave.search("q", max_results=99)
    assert captured["params"]["count"] == 20


def test_search_mode_arg_ignored(monkeypatch):
    """mode parameter is intentionally a no-op for parity with exa.search."""
    monkeypatch.setattr(brave, "_get_api_key", lambda: "k")
    monkeypatch.setattr(brave.requests, "get", lambda *a, **kw: _FakeResp(payload={"web": {"results": []}}))
    assert brave.search("q", max_results=1, mode="anything") == []


def test_search_network_error(monkeypatch):
    monkeypatch.setattr(brave, "_get_api_key", lambda: "k")

    def fake_get(*a, **kw):
        raise requests.ConnectionError("boom")

    monkeypatch.setattr(brave.requests, "get", fake_get)
    with pytest.raises(brave.BraveError) as ei:
        brave.search("q", max_results=1)
    assert ei.value.error["retryable"] is True
    assert "network error" in ei.value.error["error"]


@pytest.mark.parametrize(
    "status,retryable",
    [(429, True), (500, True), (502, True), (503, True), (504, True), (400, False), (401, False)],
)
def test_search_http_error_retryable_classification(monkeypatch, status, retryable):
    monkeypatch.setattr(brave, "_get_api_key", lambda: "k")
    monkeypatch.setattr(
        brave.requests,
        "get",
        lambda *a, **kw: _FakeResp(status_code=status, text="server says no"),
    )
    with pytest.raises(brave.BraveError) as ei:
        brave.search("q", max_results=1)
    assert ei.value.error["status_code"] == status
    assert ei.value.error["retryable"] is retryable


def test_search_skips_items_without_url_and_truncates_content(monkeypatch):
    monkeypatch.setattr(brave, "_get_api_key", lambda: "k")
    long_desc = "x" * 5000
    monkeypatch.setattr(
        brave.requests,
        "get",
        lambda *a, **kw: _FakeResp(
            payload={
                "web": {
                    "results": [
                        {"url": "", "title": "no-url"},
                        {"url": "https://ok.example.com", "description": long_desc},
                    ]
                }
            }
        ),
    )
    out = brave.search("q", max_results=2)
    assert len(out) == 1
    assert len(out[0]["content"]) == 1000
