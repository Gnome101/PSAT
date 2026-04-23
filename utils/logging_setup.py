"""Structured JSON logging for Grafana/Loki.

One line per log record, flat payload so Loki's `| json` filter indexes every
field. `configure_logging()` is safe to call more than once: it no-ops when
handlers already exist (so pytest's caplog fixture isn't clobbered) unless
`force=True` — which the worker entrypoints pass to claim the root logger
from any leftover config.

Pass structured fields via `extra=`:

    logger.info("claimed job", extra={"job_id": jid, "stage": "static"})

Wrap timed operations with `timed()`; it emits a single done/failed record
with `op` and `duration_ms` fields:

    with timed(logger, "fetch_artifact", artifact_name="contract_flags"):
        value = _resolve_artifact_value(art)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from typing import Any, Iterator

_RESERVED_LOGRECORD_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "message",
    "module",
    "msecs",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    """Emit each record as a single-line JSON object.

    Guaranteed fields: ts (ISO8601 UTC, millisecond precision), level, logger,
    msg. Any attribute on the LogRecord that's not a stdlib reserved name
    (i.e. came from `extra=`) is merged into the payload. Exceptions are
    collapsed into an `exc` string so Loki sees one line, not a multi-line
    traceback spread across records.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED_LOGRECORD_ATTRS or key.startswith("_"):
                continue
            payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = record.stack_info
        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(level: int = logging.INFO, *, force: bool = False) -> None:
    """Install the JSON formatter on the root logger.

    Safe to call at module import: no-ops when handlers already exist unless
    `force=True`. Worker entrypoints pass `force=True` to guarantee a clean
    slate on a fresh process; the API layer calls it without force so
    pytest's caplog handler survives test runs.
    """
    root = logging.getLogger()
    if root.handlers and not force:
        return

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())
    if force:
        root.handlers[:] = [handler]
    else:
        root.addHandler(handler)
    root.setLevel(level)

    # Silence noisy third-party loggers that otherwise drown the signal.
    for noisy in ("botocore", "urllib3", "s3transfer", "sqlalchemy.engine.Engine"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


@contextmanager
def timed(
    logger: logging.Logger,
    op: str,
    *,
    level: int = logging.INFO,
    warn_over_ms: int | None = None,
    **fields: Any,
) -> Iterator[None]:
    """Emit one log record when the block completes, with `duration_ms`.

    On success: logs at `level` (or WARNING if `warn_over_ms` is set and the
    block ran longer). On exception: logs at ERROR with `exc`, then re-raises
    so the caller's error handling still runs.
    """
    started = time.monotonic()
    try:
        yield
    except Exception:
        duration_ms = int((time.monotonic() - started) * 1000)
        logger.error(
            "op failed",
            extra={"op": op, "duration_ms": duration_ms, **fields},
            exc_info=True,
        )
        raise
    duration_ms = int((time.monotonic() - started) * 1000)
    effective_level = level
    if warn_over_ms is not None and duration_ms >= warn_over_ms:
        effective_level = logging.WARNING
    logger.log(
        effective_level,
        "op done",
        extra={"op": op, "duration_ms": duration_ms, **fields},
    )


def process_rss_mb() -> int | None:
    """Current process RSS in MiB, or None if /proc isn't available.

    Used by the worker heartbeat — pure-stdlib so it works on the Fly
    machines without pulling in psutil.
    """
    try:
        with open(f"/proc/{os.getpid()}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        return None
    return None
