from __future__ import annotations

import threading
import time

from ms_flow.core.executor.services.sink_writer_pool import SinkWriteTask, SinkWriterPool


class _ConcurrencyProbe:
    def __init__(self, *, concurrent: bool):
        self.supports_concurrent_writers = concurrent
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def write_staged(self, staged):
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        time.sleep(0.05)
        with self._lock:
            self.active -= 1
        return {"staged": staged}


def _run_tasks(handler: _ConcurrencyProbe) -> _ConcurrencyProbe:
    completed = threading.Event()
    pool = SinkWriterPool(
        max_pending=8,
        on_completion=completed.set,
    )
    try:
        for index in range(4):
            pool.submit(
                SinkWriteTask(
                    job_id="job",
                    chunk_id=f"chunk-{index}",
                    handler=handler,
                    staged=index,
                    output_json="{}",
                )
            )
        deadline = time.time() + 3.0
        completions = []
        while len(completions) < 4 and time.time() < deadline:
            completed.wait(timeout=0.2)
            completed.clear()
            completions.extend(pool.drain_completions())
        assert len(completions) == 4
        assert not any(item.error for item in completions)
        return handler
    finally:
        pool.shutdown()


def test_sink_writer_pool_serializes_sqlite_writes():
    probe = _run_tasks(_ConcurrencyProbe(concurrent=False))
    assert probe.max_active == 1


def test_sink_writer_pool_serializes_all_writes_in_sqlite_only_mode():
    probe = _run_tasks(_ConcurrencyProbe(concurrent=True))
    assert probe.max_active == 1
