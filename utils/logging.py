"""Structured JSON logging + ``trace_id`` propagation.

Every API request and worker job advances under a 16-char hex ``trace_id``.
Binding it onto a :mod:`contextvars` ``ContextVar`` lets every ``logger.X``
call inside that request/job ride the same id without having to thread it
through function signatures.

Level contract (downstream agents enforce per call site, not done here):

    ``ERROR``    Job-failing. The pipeline cannot continue for *this* job
                 and a stage_errors artifact must be produced.
    ``WARNING``  Degraded but continuing. The job advances, but a partial
                 outcome was reached (e.g. a single source failed in a
                 fan-out, missing optional data, fallback path taken).
                 Sites that emit WARNING under degradation will be
                 required to also write a ``stage_errors`` artifact.
    ``INFO``     Lifecycle: process boot, job claim, stage advance, job
                 completion. One per real event, not per loop iteration.
    ``DEBUG``    Per-RPC noise: individual ``eth_getCode`` / Etherscan
                 batch tracebacks, per-future thread-pool timings, etc.

Threading note: ``contextvars`` are per-thread by default. When fanning
out via :class:`concurrent.futures.ThreadPoolExecutor`, wrap every
submission with :func:`contextvars.copy_context().run` so the worker
threads see the parent job's ``trace_id`` instead of an empty context.
:mod:`utils.concurrency.parallel_map` already does this for callers, so
prefer it over a bare ``executor.submit`` whenever possible.
"""

from __future__ import annotations

import contextvars
import json
import logging
import os
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

trace_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("psat_trace_id", default=None)
job_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("psat_job_id", default=None)
stage_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("psat_stage", default=None)
worker_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("psat_worker_id", default=None)
address_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("psat_address", default=None)
chain_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("psat_chain", default=None)

# Standard ``LogRecord`` attributes we shouldn't echo into the JSON body —
# the formatter already promotes the ones it cares about (level, logger,
# message), and the rest are noise (pathname, lineno, processName, …).
# Anything passed via ``extra={...}`` lands on the record as a new attr and
# is surfaced; this set is the deny-list for the catch-all loop below.
_RESERVED_RECORD_ATTRS = frozenset(
    {
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
)


class JsonFormatter(logging.Formatter):
    """Render each ``LogRecord`` as a single-line JSON object.

    Pulls ``trace_id`` / ``job_id`` / ``stage`` / ``worker_id`` /
    ``address`` / ``chain`` from the matching :mod:`contextvars` and any
    user-supplied ``extra={...}`` keys (e.g. ``duration_ms``) directly
    off the record. Unset context fields are omitted rather than
    serialized as ``null`` so log shippers don't index empty cardinality.
    """

    _CONTEXT_FIELDS: tuple[tuple[str, contextvars.ContextVar[str | None]], ...] = (
        ("trace_id", trace_id_var),
        ("job_id", job_id_var),
        ("stage", stage_var),
        ("worker_id", worker_id_var),
        ("address", address_var),
        ("chain", chain_var),
    )

    def format(self, record: logging.LogRecord) -> str:
        # ``logging.Formatter.formatTime`` routes through ``time.strftime``
        # which doesn't expand ``%f`` to microseconds — build the ISO
        # timestamp by hand off the record's ``created`` epoch instead.
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds")
        if ts.endswith("+00:00"):
            ts = ts[: -len("+00:00")] + "Z"
        payload: dict[str, Any] = {
            "timestamp": ts,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, var in self._CONTEXT_FIELDS:
            value = var.get()
            if value is not None:
                payload[key] = value
        for attr, value in record.__dict__.items():
            if attr in _RESERVED_RECORD_ATTRS or attr.startswith("_"):
                continue
            if attr in payload:
                continue
            payload[attr] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(payload, default=str)


_CONFIGURED_FLAG = "_psat_json_logging_configured"


def configure_logging(level: int | str | None = None) -> None:
    """Install :class:`JsonFormatter` on the root logger once per process.

    On first call this clears any pre-existing handlers (e.g. from
    ``logging.basicConfig`` in a worker ``main()``) and installs a JSON
    stream handler on stderr. Subsequent calls in the same process
    short-circuit so test harnesses (notably pytest's ``caplog``) can
    add their own handlers without our re-init wiping them mid-test.

    Reads the level from ``PSAT_LOG_LEVEL`` (default INFO) when *level*
    is omitted.
    """
    root = logging.getLogger()
    if getattr(root, _CONFIGURED_FLAG, False):
        return
    if level is None:
        level = os.getenv("PSAT_LOG_LEVEL", "INFO").upper()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)
    setattr(root, _CONFIGURED_FLAG, True)


@contextmanager
def bind_trace_context(
    *,
    trace_id: str | None = None,
    job_id: str | None = None,
    stage: str | None = None,
    worker_id: str | None = None,
    address: str | None = None,
    chain: str | None = None,
) -> Iterator[None]:
    """Bind logging context for the duration of a ``with`` block.

    Every keyword maps to its matching ``ContextVar``. ``None`` skips the
    bind so callers can pass through partials (e.g. the API ingress only
    knows ``trace_id``; the worker fills in ``job_id``/``stage``/...).
    Tokens are reset on exit so nested binds nest cleanly.
    """
    tokens: list[tuple[contextvars.ContextVar[str | None], contextvars.Token[str | None]]] = []
    bindings: tuple[tuple[contextvars.ContextVar[str | None], str | None], ...] = (
        (trace_id_var, trace_id),
        (job_id_var, job_id),
        (stage_var, stage),
        (worker_id_var, worker_id),
        (address_var, address),
        (chain_var, chain),
    )
    for var, value in bindings:
        if value is not None:
            tokens.append((var, var.set(value)))
    try:
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


__all__ = [
    "JsonFormatter",
    "bind_trace_context",
    "configure_logging",
    "trace_id_var",
    "job_id_var",
    "stage_var",
    "worker_id_var",
    "address_var",
    "chain_var",
]
