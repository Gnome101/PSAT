# tests/test_extractor.py
from unittest.mock import patch
from services.discovery.docs.extractor import DocsExtractor

def test_parse_json_strips_markdown_fences():
    e = DocsExtractor()
    assert e._parse_json('```json\n{"key": "val"}\n```') == {"key": "val"}
    assert e._parse_json('```\n{"key": "val"}\n```') == {"key": "val"}
    assert e._parse_json('{"key": "val"}') == {"key": "val"}

def test_parse_json_raises_on_invalid():
    import pytest
    with pytest.raises(ValueError):
        DocsExtractor()._parse_json("not json")

def test_is_relevant_returns_safe_default_on_parse_failure():
    with patch("services.discovery.docs.extractor.chat", return_value="not json"):
        result = DocsExtractor().is_relevant("some content", "Aave")
    assert result.is_relevant is False
    assert result.confidence == 0.0

def test_extract_signals_returns_minimal_on_failure():
    with patch("services.discovery.docs.extractor.chat", return_value="not json"):
        result = DocsExtractor().extract_signals("some content", "docs_page")
    assert result.is_security_relevant is False
    assert result.doc_type == "docs_page"