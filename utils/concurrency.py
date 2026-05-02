"""Worker-fleet concurrency primitives shared across pipeline stages.

The pipeline is dominated by JSON-RPC and Etherscan I/O — the few bits of CPU
work between requests don't justify processes, but the cumulative RTT cost on
serial loops is the dominant share of every worker's wall time. These helpers
give every fan-out site a uniform, threading-only way to stack RTTs while
keeping the request shape, ordering guarantees, and error semantics identical
to the sequential version.

``parallel_map`` is the generic per-item fan-out (one task = one item).
``RpcExecutor`` is the process-wide thread pool every site shares so we don't
spawn a new pool per call. ``parallel_rpc_calls`` chunks a JSON-RPC batch and
submits each chunk through the pool — the chunking already exists in
``rpc_batch_request_with_status``, but it runs serially; here we parallelize
across chunks so a 2000-call batch finishes in roughly the time of one chunk.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any, TypeVar

from utils.rpc import MAX_BATCH_SIZE, rpc_batch_request_with_status

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


def _max_fanout() -> int:
    """Resolve ``PSAT_RPC_FANOUT`` at every call so tests can flip it via monkeypatch."""
    try:
        value = int(os.getenv("PSAT_RPC_FANOUT", "16"))
    except ValueError:
        return 16
    return max(1, value)


def parallel_map(
    fn: Callable[[T], R],
    items: Iterable[T],
    *,
    max_workers: int | None = None,
    heartbeat: Callable[[], None] | None = None,
) -> list[tuple[T, R | BaseException]]:
    """Apply *fn* to each item concurrently and return results in input order.

    Each entry in the returned list is ``(item, result)`` on success or
    ``(item, exc)`` on failure — exceptions never propagate out of this helper,
    so callers can treat parallel failures the same way they'd treat per-item
    failures in a serial loop.

    *max_workers* falls back to ``PSAT_RPC_FANOUT`` (default 16). When set to 1
    the function executes sequentially in-thread, which is the parity mode
    tests use to assert behavioural equivalence with the serial path.

    *heartbeat*, if provided, is called once per completion. It runs on the
    submitting thread (after ``as_completed`` yields) so DB sessions captured
    in the closure stay on the worker's thread.
    """
    items_list = list(items)
    if not items_list:
        return []

    workers = max_workers if max_workers is not None else _max_fanout()
    workers = max(1, min(workers, len(items_list)))

    results: list[tuple[T, R | BaseException]] = [(item, None) for item in items_list]  # type: ignore[misc]

    if workers == 1:
        for idx, item in enumerate(items_list):
            try:
                results[idx] = (item, fn(item))
            except BaseException as exc:  # noqa: BLE001 — preserve every exception type for callers
                results[idx] = (item, exc)
            if heartbeat is not None:
                try:
                    heartbeat()
                except Exception:
                    logger.exception("parallel_map: heartbeat raised — continuing")
        return results

    executor = RpcExecutor.get()
    futures: dict[Future[R], int] = {}
    # Cap concurrency at *workers* by submitting in waves of that size; the
    # shared ``RpcExecutor`` is intentionally larger than any one site needs
    # so callers can declare their own ceiling without starving siblings.
    semaphore = threading.Semaphore(workers)

    def _submit(idx: int, item: T) -> Future[R]:
        semaphore.acquire()

        def _wrapped() -> R:
            try:
                return fn(item)
            finally:
                semaphore.release()

        return executor.submit(_wrapped)

    for idx, item in enumerate(items_list):
        futures[_submit(idx, item)] = idx

    for fut in as_completed(futures):
        idx = futures[fut]
        try:
            results[idx] = (items_list[idx], fut.result())
        except BaseException as exc:  # noqa: BLE001
            results[idx] = (items_list[idx], exc)
        if heartbeat is not None:
            try:
                heartbeat()
            except Exception:
                logger.exception("parallel_map: heartbeat raised — continuing")

    return results


def unwrap_results(results: Sequence[tuple[T, R | BaseException]]) -> list[R]:
    """Convenience: flatten ``parallel_map`` output, raising the first exception encountered.

    Use this when the call site has no per-item recovery story and would have
    raised on the first failure in a serial loop anyway.
    """
    out: list[R] = []
    for item, result in results:
        if isinstance(result, BaseException):
            raise result
        out.append(result)
    return out


class RpcExecutor:
    """Process-wide ``ThreadPoolExecutor`` shared across every fan-out site.

    Sized from ``PSAT_RPC_FANOUT`` (default 16). Constructed lazily on first
    access, never shut down — the pool lives for the lifetime of the worker
    process and threads are reused across jobs so we never pay
    pthread-creation cost in the hot path.
    """

    _instance: ThreadPoolExecutor | None = None
    _lock = threading.Lock()

    @classmethod
    def get(cls) -> ThreadPoolExecutor:
        if cls._instance is not None:
            return cls._instance
        with cls._lock:
            if cls._instance is None:
                workers = _max_fanout()
                cls._instance = ThreadPoolExecutor(
                    max_workers=workers,
                    thread_name_prefix="psat-rpc",
                )
        return cls._instance

    @classmethod
    def submit(cls, fn: Callable[..., R], *args: Any, **kwargs: Any) -> Future[R]:
        return cls.get().submit(fn, *args, **kwargs)

    @classmethod
    def reset_for_tests(cls) -> None:
        """Drop the singleton so tests that change ``PSAT_RPC_FANOUT`` get a fresh pool."""
        with cls._lock:
            inst, cls._instance = cls._instance, None
        if inst is not None:
            inst.shutdown(wait=False, cancel_futures=False)


def submit_rpc(fn: Callable[..., R], *args: Any, **kwargs: Any) -> Future[R]:
    """Module-level shortcut for ``RpcExecutor.submit``."""
    return RpcExecutor.submit(fn, *args, **kwargs)


def parallel_rpc_calls(
    rpc_url: str,
    calls: list[tuple[str, list[Any]]],
) -> list[tuple[Any, bool]]:
    """Drop-in replacement for ``rpc_batch_request_with_status`` that parallelizes across chunks.

    For ``len(calls) <= MAX_BATCH_SIZE`` this just delegates — there is no
    second chunk to stack. For larger batches each chunk is submitted to the
    shared executor and the results are reassembled in input order. The
    return shape (``[(result, had_error)]``) is identical so call sites are
    drop-in.
    """
    if not calls:
        return []
    if len(calls) <= MAX_BATCH_SIZE:
        return rpc_batch_request_with_status(rpc_url, calls)

    chunks: list[tuple[int, list[tuple[str, list[Any]]]]] = []
    for chunk_start in range(0, len(calls), MAX_BATCH_SIZE):
        chunks.append((chunk_start, calls[chunk_start : chunk_start + MAX_BATCH_SIZE]))

    results: list[tuple[Any, bool]] = [(None, True)] * len(calls)
    futures: dict[Future[list[tuple[Any, bool]]], int] = {}
    for offset, chunk in chunks:
        futures[RpcExecutor.submit(rpc_batch_request_with_status, rpc_url, chunk)] = offset

    for fut in as_completed(futures):
        offset = futures[fut]
        try:
            chunk_results = fut.result()
        except Exception:
            logger.exception("parallel_rpc_calls: chunk at offset %d failed wholesale", offset)
            continue
        for i, item in enumerate(chunk_results):
            results[offset + i] = item
    return results


__all__ = [
    "RpcExecutor",
    "parallel_map",
    "parallel_rpc_calls",
    "submit_rpc",
    "unwrap_results",
]
