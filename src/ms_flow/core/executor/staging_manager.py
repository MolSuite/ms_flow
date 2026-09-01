from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from threading import RLock
from typing import Any, Callable


@dataclass
class StagingTaskInfo:
    token: str
    kind: str
    job_id: str
    chunk_id: str
    future: Future


class StagingManager:
    def __init__(self, *, total_cpu: int, max_workers: int | None = None):
        self.total_cpu = max(1, int(total_cpu))
        self._max_workers = max(1, int(max_workers)) if max_workers is not None else None
        self._pool: ThreadPoolExecutor | None = None
        self._tasks: dict[str, StagingTaskInfo] = {}
        self._lock = RLock()
        self.ensure_pool()

    @property
    def configured_max_workers(self) -> int | None:
        return self._max_workers

    def configure(self, *, max_workers: int | None = None):
        desired = max(1, int(max_workers)) if max_workers is not None else None
        if desired == self._max_workers:
            return
        self._max_workers = desired
        self.shutdown()
        self.ensure_pool()

    def ensure_pool(self):
        if self._pool is not None:
            return
        self._pool = ThreadPoolExecutor(
            max_workers=self.capacity(default_if_missing=True),
            thread_name_prefix="molsuite-staging",
        )

    def ready(self) -> bool:
        return bool(self._pool is not None and not bool(getattr(self._pool, "_shutdown", False)))

    def capacity(self, *, default_if_missing: bool = False) -> int:
        if self._pool is None:
            if default_if_missing:
                return self._max_workers or max(2, min(8, self.total_cpu))
            return 0
        return max(1, int(getattr(self._pool, "_max_workers", 1)))

    def active_count(self) -> int:
        with self._lock:
            return len(self._tasks)

    def available_slots(self) -> int:
        return max(0, self.capacity() - self.active_count())

    def snapshot(self) -> list[StagingTaskInfo]:
        with self._lock:
            return list(self._tasks.values())

    def submit(
        self,
        *,
        token: str,
        kind: str,
        job_id: str,
        chunk_id: str,
        call_with_optional_context: Callable[..., Any],
        fn: Callable[..., Any],
        payload: dict[str, Any],
        context: dict[str, Any],
        on_done: Callable[[], Any] | None = None,
    ) -> Future:
        self.ensure_pool()
        assert self._pool is not None
        future = self._pool.submit(call_with_optional_context, fn, payload, context)
        if on_done is not None:
            future.add_done_callback(lambda _future: on_done())
        with self._lock:
            self._tasks[token] = StagingTaskInfo(
                token=token,
                kind=kind,
                job_id=job_id,
                chunk_id=chunk_id,
                future=future,
            )
        return future

    def pop_completed(self) -> list[StagingTaskInfo]:
        completed: list[StagingTaskInfo] = []
        with self._lock:
            tokens = [token for token, meta in self._tasks.items() if meta.future.done()]
            for token in tokens:
                meta = self._tasks.pop(token, None)
                if meta is not None:
                    completed.append(meta)
        return completed

    def cancel_job(self, job_id: str) -> list[Future]:
        futures: list[Future] = []
        with self._lock:
            tokens = [token for token, meta in self._tasks.items() if meta.job_id == job_id]
            for token in tokens:
                meta = self._tasks.get(token)
                if meta is not None:
                    canceled = meta.future.cancel()
                    futures.append(meta.future)
                    if canceled:
                        self._tasks.pop(token, None)
        return futures

    def shutdown(self):
        futures = self.cancel_all()
        for future in futures:
            future.cancel()
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=True)
            self._pool = None

    def cancel_all(self) -> list[Future]:
        with self._lock:
            metas = list(self._tasks.values())
            self._tasks.clear()
        return [meta.future for meta in metas]
