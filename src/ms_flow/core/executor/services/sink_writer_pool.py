from __future__ import annotations

import queue
import threading
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class SinkWriteTask:
    job_id: str
    chunk_id: str
    handler: Any
    staged: Any
    output_json: str


@dataclass(frozen=True)
class SinkWriteCompletion:
    task: SinkWriteTask
    receipt: dict[str, Any] | None = None
    error: str = ""


class SinkWriterPool:
    """Bounded SQLite project-data writer, independent from lifecycle persistence.

    Result payloads are already spooled to disk before they enter this queue.
    The queue only carries compact staged-output references, so bounding it is a
    backpressure control for local compute, not a memory-retention strategy.
    """

    def __init__(
        self,
        *,
        max_pending: int,
        on_completion: Callable[[], None],
    ):
        capacity = max(1, int(max_pending))
        self._queue: queue.Queue[SinkWriteTask | None] = queue.Queue(maxsize=capacity)
        self._completions: queue.SimpleQueue[SinkWriteCompletion] = queue.SimpleQueue()
        self._on_completion = on_completion
        self._threads = [
            threading.Thread(
                target=self._worker,
                args=(self._queue,),
                name="molsuite-sink-sqlite",
                daemon=True,
            )
        ]
        for thread in self._threads:
            thread.start()

    def submit(self, task: SinkWriteTask, *, timeout: float = 5.0) -> None:
        self._queue.put(task, timeout=max(0.0, float(timeout)))

    def _worker(self, work_queue: queue.Queue[SinkWriteTask | None]) -> None:
        deferred: deque[SinkWriteTask | None] = deque()
        while True:
            task = deferred.popleft() if deferred else work_queue.get()
            tasks = [task]
            try:
                if task is None:
                    return
                batch_size = max(1, int(getattr(task.handler, "write_batch_size", 1)))
                while len(tasks) < batch_size:
                    try:
                        candidate = work_queue.get(timeout=0.002)
                    except queue.Empty:
                        break
                    if candidate is not None and candidate.handler is task.handler:
                        tasks.append(candidate)
                    else:
                        deferred.append(candidate)
                        break
                try:
                    write_batch = getattr(task.handler, "write_batch", None)
                    if callable(write_batch):
                        receipts = write_batch([item.staged for item in tasks])
                    else:
                        receipts = {
                            item.chunk_id: task.handler.write_staged(item.staged)
                            for item in tasks
                        }
                except Exception as exc:
                    completions = [
                        SinkWriteCompletion(task=item, error=str(exc))
                        for item in tasks
                    ]
                else:
                    completions = [
                        SinkWriteCompletion(
                            task=item,
                            receipt=dict(receipts.get(item.chunk_id) or {}),
                        )
                        for item in tasks
                    ]
                for completion in completions:
                    self._completions.put(completion)
                self._on_completion()
            finally:
                work_queue.task_done()
                for _ in tasks[1:]:
                    work_queue.task_done()

    def drain_completions(self) -> list[SinkWriteCompletion]:
        items: list[SinkWriteCompletion] = []
        while True:
            try:
                items.append(self._completions.get_nowait())
            except queue.Empty:
                return items

    def pending_count(self) -> int:
        return self._queue.qsize()

    def shutdown(self) -> None:
        for _ in self._threads:
            self._queue.put(None)
        for thread in self._threads:
            thread.join(timeout=5.0)
