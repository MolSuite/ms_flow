"""Persistent dispatch pool for non-blocking adapter.submit() calls."""

from __future__ import annotations

import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from threading import RLock
from typing import Any


@dataclass
class PendingDispatch:
    job_id: str
    chunk_id: str
    executor_name: str
    cpu_required: int
    gpu_required: int
    future: Future
    submitted_at: float  # time.monotonic()
    abandoned_reason: str = ""


@dataclass(frozen=True)
class DispatchCompletion:
    dispatch: PendingDispatch
    handle_id: Any = None
    error: str | None = None
    abandoned: bool = False


class DispatchPool:
    """Non-blocking wrapper around adapter.submit().
    
    Instead of calling adapter.submit() synchronously (which blocks the
    entire dispatch loop if an adapter hangs), we submit to a 
    ThreadPoolExecutor and poll results in the main loop.
    
    Timeout: if a submit doesn't complete within `timeout_s`, the chunk
    is marked as failed.
    """

    def __init__(self, *, max_workers: int = 4, timeout_s: float = 30.0):
        self._max_workers = max(1, max_workers)
        self._timeout_s = max(1.0, timeout_s)
        self._pool = ThreadPoolExecutor(
            max_workers=self._max_workers,
            thread_name_prefix="molsuite-dispatch",
        )
        self._pending: dict[str, PendingDispatch] = {}  # chunk_id -> PendingDispatch
        self._abandoned: dict[str, PendingDispatch] = {}
        self._lock = RLock()
        self._submitted_total = 0
        self._completed_total = 0
        self._timed_out_total = 0
        self._failed_total = 0
        self._last_submit_at = 0.0
        self._last_completion_at = 0.0
        self._last_error = ""
        self._closed = False

    def submit(
        self,
        *,
        chunk_id: str,
        job_id: str,
        executor_name: str,
        cpu_required: int,
        fn: callable,
        args: tuple = (),
        on_done: callable | None = None,
        gpu_required: int = 0,
    ) -> None:
        future = self._pool.submit(fn, *args)
        if on_done is not None:
            future.add_done_callback(lambda _future: on_done())
        with self._lock:
            self._submitted_total += 1
            self._last_submit_at = time.monotonic()
            self._pending[chunk_id] = PendingDispatch(
                job_id=job_id,
                chunk_id=chunk_id,
                executor_name=executor_name,
                cpu_required=cpu_required,
                gpu_required=gpu_required,
                future=future,
                submitted_at=time.monotonic(),
            )

    @staticmethod
    def _resolve_future(item: PendingDispatch, *, abandoned: bool) -> DispatchCompletion:
        try:
            return DispatchCompletion(
                dispatch=item,
                handle_id=item.future.result(),
                abandoned=abandoned,
            )
        except Exception as exc:
            return DispatchCompletion(
                dispatch=item,
                error=f"Dispatch failed: {exc}",
                abandoned=abandoned,
            )

    def poll_completed(self) -> list[DispatchCompletion]:
        """Return normal completions and observe every abandoned submit future."""
        results: list[DispatchCompletion] = []
        now = time.monotonic()
        with self._lock:
            for chunk_id in list(self._pending):
                item = self._pending[chunk_id]
                if item.future.done():
                    del self._pending[chunk_id]
                    completion = self._resolve_future(item, abandoned=False)
                    if completion.error is None:
                        self._completed_total += 1
                        self._last_completion_at = now
                    else:
                        self._failed_total += 1
                        self._last_error = completion.error
                    results.append(completion)
                elif now - item.submitted_at > self._timeout_s:
                    del self._pending[chunk_id]
                    item.abandoned_reason = (
                        f"adapter.submit() timed out after {self._timeout_s}s "
                        f"for executor={item.executor_name}"
                    )
                    item.future.cancel()
                    self._abandoned[chunk_id] = item
                    self._timed_out_total += 1
                    self._last_error = item.abandoned_reason
                    results.append(DispatchCompletion(dispatch=item, error=item.abandoned_reason))

            for chunk_id in list(self._abandoned):
                item = self._abandoned[chunk_id]
                if not item.future.done():
                    continue
                del self._abandoned[chunk_id]
                results.append(self._resolve_future(item, abandoned=True))
        return results

    def is_dispatching(self, chunk_id: str) -> bool:
        with self._lock:
            return chunk_id in self._pending or chunk_id in self._abandoned

    def cancel_job(self, job_id: str) -> list[PendingDispatch]:
        canceled: list[PendingDispatch] = []
        with self._lock:
            for chunk_id in list(self._pending):
                item = self._pending[chunk_id]
                if item.job_id == job_id:
                    item.abandoned_reason = "dispatch canceled before adapter.submit() completed"
                    item.future.cancel()
                    del self._pending[chunk_id]
                    self._abandoned[chunk_id] = item
                    canceled.append(item)
        return canceled

    def active_chunk_ids(self, job_id: str) -> set[str]:
        with self._lock:
            return {
                item.chunk_id
                for item in (*self._pending.values(), *self._abandoned.values())
                if item.job_id == job_id
            }

    def get_total_cpu_required(self) -> int:
        with self._lock:
            return sum(item.cpu_required for item in self._pending.values())

    def get_total_gpu_required(self) -> int:
        with self._lock:
            return sum(item.gpu_required for item in self._pending.values())

    def get_job_cpu_usage(self, job_id: str) -> int:
        with self._lock:
            return sum(
                item.cpu_required
                for item in self._pending.values()
                if item.job_id == job_id
            )

    def snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        with self._lock:
            pending_items = list(self._pending.values())
            abandoned_items = list(self._abandoned.values())
            active_items = pending_items + abandoned_items
            oldest_pending_age_s = None
            if active_items:
                oldest_pending_age_s = max(0.0, now - min(item.submitted_at for item in active_items))
            saturation = (len(active_items) / self._max_workers) if self._max_workers > 0 else 0.0
            ok = (
                not self._closed
                and (
                    oldest_pending_age_s is None
                    or oldest_pending_age_s <= (self._timeout_s * 1.5)
                )
            )
            return {
                "ok": ok,
                "closed": self._closed,
                "active_tasks": len(active_items),
                "pending_tasks": len(pending_items),
                "abandoned_tasks": len(abandoned_items),
                "max_workers": self._max_workers,
                "timeout_s": self._timeout_s,
                "oldest_pending_age_s": oldest_pending_age_s,
                "saturation": round(float(saturation), 6),
                "submitted_total": self._submitted_total,
                "completed_total": self._completed_total,
                "timed_out_total": self._timed_out_total,
                "failed_total": self._failed_total,
                "last_error": self._last_error,
                "last_submit_age_s": (
                    max(0.0, now - self._last_submit_at) if self._last_submit_at > 0 else None
                ),
                "last_completion_age_s": (
                    max(0.0, now - self._last_completion_at) if self._last_completion_at > 0 else None
                ),
            }

    def shutdown(self):
        with self._lock:
            for item in (*self._pending.values(), *self._abandoned.values()):
                item.future.cancel()
            self._pending.clear()
            self._abandoned.clear()
            self._closed = True
        self._pool.shutdown(wait=False, cancel_futures=True)
