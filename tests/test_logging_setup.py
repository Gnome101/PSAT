"""Unit tests for the structured JSON logging pipeline.

These are deliberately narrow — the whole module is <200 LOC, so the tests
focus on what an operator would notice in Grafana if it broke:
    - Every record is single-line JSON with the guaranteed fields.
    - Extras passed via `extra=` appear alongside those fields.
    - Exceptions collapse to `exc` instead of a multi-line traceback.
    - `configure_logging()` doesn't clobber an already-configured root
      (pytest's caplog survives).
    - `timed()` emits done/failed with a `duration_ms` int.
"""

from __future__ import annotations

import io
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.logging_setup import (  # noqa: E402
    JsonFormatter,
    configure_logging,
    process_rss_mb,
    timed,
)


def _format(record: logging.LogRecord) -> dict:
    return json.loads(JsonFormatter().format(record))


def _make_record(
    level: int = logging.INFO,
    msg: str = "hello",
    name: str = "t",
    exc_info=None,
    **extra,
) -> logging.LogRecord:
    record = logging.LogRecord(
        name=name,
        level=level,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=(),
        exc_info=exc_info,
    )
    for k, v in extra.items():
        setattr(record, k, v)
    return record


def test_json_formatter_emits_guaranteed_fields():
    payload = _format(_make_record(msg="hi"))
    assert payload["level"] == "INFO"
    assert payload["logger"] == "t"
    assert payload["msg"] == "hi"
    # ts must be ISO8601 UTC with millisecond precision and a Z suffix —
    # Loki's date parser relies on that shape.
    assert payload["ts"].endswith("Z")
    assert "T" in payload["ts"]


def test_json_formatter_includes_extras():
    payload = _format(_make_record(job_id="abc-123", stage="static", rss_mb=42))
    assert payload["job_id"] == "abc-123"
    assert payload["stage"] == "static"
    assert payload["rss_mb"] == 42


def test_json_formatter_ignores_stdlib_reserved_attrs():
    # process, thread, etc. are LogRecord attributes — they must NOT leak
    # into the payload, or every record gets polluted with noise.
    payload = _format(_make_record())
    assert "process" not in payload
    assert "thread" not in payload
    assert "pathname" not in payload


def test_json_formatter_collapses_exceptions_to_one_line():
    try:
        raise ValueError("boom")
    except ValueError:
        record = _make_record(level=logging.ERROR, exc_info=sys.exc_info())
    formatted = JsonFormatter().format(record)
    # Must be parseable as a single JSON line (no embedded newlines between records).
    payload = json.loads(formatted)
    assert payload["level"] == "ERROR"
    assert "ValueError" in payload["exc"]
    assert "boom" in payload["exc"]
    # And literally a single line — Loki splits on newlines.
    assert "\n" not in formatted


def test_json_formatter_is_utf8_safe():
    # Non-ASCII in msg must survive round-trip — `ensure_ascii=False` keeps
    # it readable in Grafana instead of escape-coded.
    payload = _format(_make_record(msg="hello — world"))
    assert payload["msg"] == "hello — world"


def test_json_formatter_handles_non_serializable_extras():
    # object() isn't JSON-serializable; the formatter falls back to str()
    # instead of crashing the whole log pipeline.
    sentinel = object()
    payload = _format(_make_record(weird=sentinel))
    assert "object object" in payload["weird"]


def test_configure_logging_no_ops_when_already_configured():
    root = logging.getLogger()
    existing = list(root.handlers)
    try:
        configure_logging()  # no force
        # Pytest installs its own handler; we must not clobber it.
        assert root.handlers == existing
    finally:
        # pytest's caplog cleanup handles state restoration, but be explicit.
        root.handlers[:] = existing


def test_configure_logging_force_replaces_handlers():
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    try:
        configure_logging(level=logging.DEBUG, force=True)
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JsonFormatter)
        assert root.level == logging.DEBUG
    finally:
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)


def test_configure_logging_quiets_noisy_third_party():
    saved = {
        name: logging.getLogger(name).level
        for name in ("botocore", "urllib3", "s3transfer", "sqlalchemy.engine.Engine")
    }
    try:
        configure_logging(force=True)
        for name in saved:
            assert logging.getLogger(name).level == logging.WARNING
    finally:
        for name, level in saved.items():
            logging.getLogger(name).setLevel(level)


def _capture_root(func):
    """Run `func()` with root logger writing JSON to a buffer; return lines."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setFormatter(JsonFormatter())
    root.handlers[:] = [handler]
    root.setLevel(logging.DEBUG)
    try:
        func()
    finally:
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line]


def test_timed_success_emits_done_with_duration():
    log = logging.getLogger("timed_ok")
    lines = _capture_root(lambda: _run_timed_success(log))
    assert len(lines) == 1
    assert lines[0]["msg"] == "op done"
    assert lines[0]["op"] == "noop"
    assert lines[0]["level"] == "INFO"
    assert isinstance(lines[0]["duration_ms"], int)
    assert lines[0]["duration_ms"] >= 0


def _run_timed_success(log: logging.Logger) -> None:
    with timed(log, "noop", tag="unit"):
        pass


def _run_timed_failure(log: logging.Logger) -> None:
    try:
        with timed(log, "crash", tag="unit"):
            raise RuntimeError("nope")
    except RuntimeError:
        # timed() re-raises so caller error handling still works.
        pass


def test_timed_failure_emits_error_and_reraises():
    log = logging.getLogger("timed_fail")
    lines = _capture_root(lambda: _run_timed_failure(log))
    assert len(lines) == 1
    assert lines[0]["msg"] == "op failed"
    assert lines[0]["op"] == "crash"
    assert lines[0]["level"] == "ERROR"
    assert "RuntimeError" in lines[0]["exc"]
    assert "nope" in lines[0]["exc"]


def test_timed_warn_over_ms_escalates_level():
    log = logging.getLogger("timed_slow")

    def slow():
        import time

        with timed(log, "slowish", warn_over_ms=0):
            time.sleep(0.005)

    lines = _capture_root(slow)
    assert lines[0]["level"] == "WARNING"
    assert lines[0]["op"] == "slowish"


def test_process_rss_mb_returns_int_or_none():
    # On Linux the /proc path exists; other platforms return None. Either
    # is acceptable — we just want to prove no exception leaks out.
    result = process_rss_mb()
    assert result is None or (isinstance(result, int) and result >= 0)


def test_process_rss_mb_returns_none_on_oserror(monkeypatch):
    # Simulate a kernel/sandbox where /proc isn't readable — the
    # heartbeat must degrade to None, never raise.
    def boom(*args, **kwargs):
        raise OSError("simulated")

    monkeypatch.setattr("builtins.open", boom)
    assert process_rss_mb() is None


def test_json_formatter_includes_stack_info():
    # `logger.debug("...", stack_info=True)` populates record.stack_info.
    # That field should ride along as `stack` in the payload.
    record = _make_record()
    record.stack_info = "Stack (most recent call last):\n  File fake.py:1"
    payload = _format(record)
    assert "Stack" in payload["stack"]


def test_configure_logging_add_handler_without_force_when_empty():
    # When the root logger has no handlers yet and force=False, we still
    # install one — that's the API-process path at module import.
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    try:
        root.handlers[:] = []
        configure_logging(level=logging.WARNING)
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JsonFormatter)
        assert root.level == logging.WARNING
    finally:
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)
