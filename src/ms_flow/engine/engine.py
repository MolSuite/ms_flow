"""The engine: one object inside the application process that runs jobs of chunks.

The state lives in memory and belongs to the loop thread alone. Everything else reaches
it through one queue: callers leave orders, executors leave finished chunks, the delivery
thread leaves confirmations. The loop only moves state and tokens and decides what is
dispatched next; results reach user callbacks from the delivery thread.
"""
from __future__ import annotations

import itertools
import logging
import os
import queue
import threading
import time
import traceback
from collections import deque
from dataclasses import dataclass

from .executors import DONE, ERROR, LOST
from .tokens import Tokens, memory_budget_mb

log = logging.getLogger(__name__)

RUNNING = "running"
COMPLETED = "completed"
COMPLETED_WITH_ERRORS = "completed_with_errors"
FAILED = "failed"
CANCELED = "canceled"
INTERRUPTED = "interrupted"


@dataclass(frozen=True, slots=True)
class JobSnapshot:
    id: int
    name: str
    status: str
    chunks_total: int | None  # unknown until the source of chunks is exhausted
    chunks_done: int
    chunks_failed: int
    chunks_inflight: int
    error: str | None
    peak_mem_mb: int
    started_at: float
    finished_at: float | None


class _Job:
    __slots__ = ("id", "name", "slot", "fn", "source", "cost", "window", "max_running", "priority",
                 "on_result", "on_error", "on_terminal", "status", "total", "done", "failed", "open",
                 "flying", "seq", "exhausted", "fatal", "error", "peak_mem_mb", "started_at",
                 "finished_at", "finished")

    def snapshot(self) -> JobSnapshot:
        return JobSnapshot(self.id, self.name, self.status, self.total, self.done, self.failed, self.open,
                           self.error, self.peak_mem_mb, self.started_at, self.finished_at)


class _Chunk:
    """A chunk between dispatch and its result being delivered. Finished ones are only counted."""

    __slots__ = ("job", "key", "payload", "handle", "alone")

    def __init__(self, job: _Job, key, payload):
        self.job, self.key, self.payload, self.handle, self.alone = job, key, payload, None, False


class _Slot:
    """A registered executor and what the loop tracks about it."""

    __slots__ = ("impl", "inflight", "suspects", "isolating")

    def __init__(self, impl):
        self.impl, self.inflight, self.suspects, self.isolating = impl, 0, deque(), False


class Engine:
    def __init__(self, cpus: int | None = None, gpus: int = 0, mem_mb: int | None = None):
        """`mem_mb` is the memory budget for chunks; by default physical RAM minus a reserve."""
        self.tokens = Tokens(cpus or os.cpu_count() or 1, gpus,
                             memory_budget_mb() if mem_mb is None else mem_mb)
        self._executors: dict[str, _Slot] = {}
        self._jobs: dict[int, _Job] = {}
        self._active: list[_Job] = []  # running jobs, by priority and then arrival
        self._ids = itertools.count(1)
        self._events: queue.SimpleQueue = queue.SimpleQueue()  # into the loop
        self._deliveries: queue.SimpleQueue = queue.SimpleQueue()  # user callbacks, out of the loop
        self._closed = False
        self._threads = [threading.Thread(target=self._loop, name="msflow-engine", daemon=True),
                         threading.Thread(target=self._deliver, name="msflow-delivery", daemon=True)]
        for thread in self._threads:
            thread.start()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --- Callers ----------------------------------------------------------------------

    def register(self, name: str, executor) -> None:
        """Add an executor (see `executors` for the contract)."""
        executor.bind(self._chunk_done)
        self._executors = {**self._executors, name: _Slot(executor)}  # the loop iterates the old one

    def submit(self, executor: str, fn, chunks, *, cpu: float = 1, gpu: float = 0, mem_mb: float = 0,
               window: int | None = None, max_cpu: float | None = None, priority: int = 0,
               total: int | None = None, name: str = "", on_result=None, on_error=None,
               on_terminal=None) -> int:
        """Run `fn(payload)` for every payload in `chunks` on `executor`. Returns the job id.

        `cpu`, `gpu` and `mem_mb` are what one chunk needs; `max_cpu` caps the job as a whole.
        `window` is how many chunks may be between dispatch and delivered result.
        `on_result(key, result)`, `on_error(key, error)` and `on_terminal(snapshot)` run on
        the delivery thread, in order; a chunk counts as done once `on_result` returns.
        """
        if self._closed:
            raise RuntimeError("engine is closed")
        slot = self._executors.get(executor)
        if slot is None:
            raise ValueError(f"unknown executor {executor!r}; registered: {sorted(self._executors)}")
        job = _Job()
        job.cost = job.max_running = None
        if slot.impl.uses_tokens:
            job.cost = (cpu, gpu, mem_mb)
            if not self.tokens.ever_fits(job.cost) or (max_cpu is not None and max_cpu < cpu):
                raise ValueError(f"a chunk needing (cpu, gpu, mem_mb)={job.cost} can never run: the engine "
                                 f"has {self.tokens.total} and max_cpu={max_cpu}")
            if max_cpu is not None and cpu:
                job.max_running = int(max_cpu // cpu)
        job.id, job.name, job.slot, job.fn, job.source = next(self._ids), name, slot, fn, iter(chunks)
        job.window = window or 2 * slot.impl.capacity
        job.priority, job.on_result, job.on_error, job.on_terminal = priority, on_result, on_error, on_terminal
        job.status = RUNNING
        job.total = len(chunks) if total is None and hasattr(chunks, "__len__") else total
        job.done = job.failed = job.open = job.seq = job.peak_mem_mb = 0
        job.flying, job.exhausted, job.fatal, job.error = set(), False, None, None
        job.started_at, job.finished_at, job.finished = time.time(), None, threading.Event()
        self._jobs[job.id] = job
        self._events.put((self._on_submit, job))
        return job.id

    def status(self, job_id: int) -> JobSnapshot:
        return self._jobs[job_id].snapshot()

    def jobs(self) -> list[JobSnapshot]:
        return [job.snapshot() for job in tuple(self._jobs.values())]

    def wait(self, job_id: int, timeout: float | None = None) -> JobSnapshot:
        """Block until the job is terminal and its callbacks have run, or `timeout` passes."""
        job = self._jobs[job_id]
        job.finished.wait(timeout)
        return job.snapshot()

    def cancel(self, job_id: int) -> None:
        self._events.put((self._on_stop_job, self._jobs[job_id], CANCELED))

    def close(self) -> None:
        """Interrupt what is running, let pending callbacks finish and stop the executors."""
        if self._closed:
            return
        self._closed = True
        self._events.put((self._on_close,))
        self._events.put(None)
        self._threads[0].join()
        self._deliveries.put(None)
        self._threads[1].join()
        for slot in self._executors.values():
            slot.impl.shutdown()

    # --- Loop thread ------------------------------------------------------------------

    def _loop(self) -> None:
        get, get_nowait = self._events.get, self._events.get_nowait
        while True:
            event = get()
            while True:  # everything that is already waiting, then one dispatch pass
                if event is None:
                    return
                try:
                    event[0](*event[1:])
                except Exception:
                    log.exception("engine loop: %s failed", event[0].__name__)
                try:
                    event = get_nowait()
                except queue.Empty:
                    break
            try:
                self._dispatch()
            except Exception:
                log.exception("engine loop: dispatch failed")

    def _on_submit(self, job: _Job) -> None:
        self._active.append(job)
        self._active.sort(key=lambda j: (-j.priority, j.id))

    def _on_stop_job(self, job: _Job, status: str) -> None:
        # ponytail: chunks already running are not killed; their tokens come back when they
        # end and the result is dropped. Killing one arrives with the per-chunk time limit.
        if job.status != RUNNING:
            return
        for chunk in tuple(job.flying):
            job.slot.impl.cancel(chunk.handle)
        self._finish(job, status)

    def _on_close(self) -> None:
        for job in tuple(self._active):
            self._on_stop_job(job, INTERRUPTED)

    def _chunk_done(self, chunk, status, result, error, peak_mb=0) -> None:
        """What executors call. Any thread."""
        self._events.put((self._on_done, chunk, status, result, error, peak_mb))

    def _on_done(self, chunk: _Chunk, status: str, result, error, peak_mb: int) -> None:
        job = chunk.job
        slot = job.slot
        slot.inflight -= 1
        if chunk.alone:
            slot.isolating = False
        job.flying.discard(chunk)
        if job.cost is not None:
            self.tokens.give(job.cost)
        if job.status != RUNNING:
            return
        if peak_mb > job.peak_mem_mb:
            job.peak_mem_mb = peak_mb
        if status == DONE:
            if job.on_result is None:
                self._settle(job, chunk.key, None)
            else:
                self._deliveries.put((job.on_result, (chunk.key, result), (job, chunk.key)))
        elif status == LOST and not chunk.alone:
            slot.suspects.append(chunk)  # collateral or culprit: running it alone will tell
        elif status == LOST:
            self._settle(job, chunk.key, "the worker process died while running this chunk\n" + error)
        else:
            self._settle(job, chunk.key, error or status)

    def _settle(self, job: _Job, key, error: str | None) -> None:
        """A chunk reached its end: count it and forget it. Also the delivery thread's confirmation."""
        if job.status != RUNNING:
            return
        job.open -= 1
        if error is None:
            job.done += 1
        else:
            job.failed += 1
            job.error = job.error or error
            if job.on_error is not None:
                self._deliveries.put((job.on_error, (key, error), None))
        self._finish_if_over(job)

    def _finish_if_over(self, job: _Job) -> None:
        if job.exhausted and not job.open:
            nothing_worked = job.fatal or (job.failed and not job.done)
            self._finish(job, FAILED if nothing_worked else COMPLETED_WITH_ERRORS if job.failed else COMPLETED)

    def _finish(self, job: _Job, status: str) -> None:
        job.status, job.finished_at, job.source = status, time.time(), None
        job.error = job.fatal or job.error
        self._active.remove(job)
        self._deliveries.put((self._announce, (job,), None))

    def _dispatch(self) -> None:
        for slot in self._executors.values():
            if slot.suspects and not slot.inflight:
                self._retry_suspect(slot)
        # ponytail: strict priority then arrival, no reservation. A job whose chunks need many
        # CPUs can wait behind a stream of small ones; add ageing if that shows up.
        progress = True
        while progress:
            progress = False
            for job in tuple(self._active):
                if self._start_one(job):
                    progress = True

    def _start_one(self, job: _Job) -> bool:
        slot = job.slot
        if (job.exhausted or job.open >= job.window or slot.inflight >= slot.impl.capacity
                or slot.suspects or slot.isolating):
            return False
        if job.cost is not None and (not self.tokens.fits(job.cost)
                                     or (job.max_running is not None and len(job.flying) >= job.max_running)):
            return False
        try:
            # ponytail: the source of chunks runs here, so a slow generator stalls every job.
            # It moves to a feeder thread per job together with the content key.
            payload = next(job.source)
        except StopIteration:
            self._exhausted(job, None)
            return False
        except Exception:
            self._exhausted(job, "the source of chunks failed\n" + traceback.format_exc())
            return False
        chunk = _Chunk(job, job.seq, payload)
        job.seq += 1
        job.open += 1
        self._launch(chunk, alone=False)
        return True

    def _exhausted(self, job: _Job, fatal: str | None) -> None:
        job.exhausted, job.fatal, job.total = True, fatal, job.seq
        self._finish_if_over(job)

    def _retry_suspect(self, slot: _Slot) -> None:
        # ponytail: after a worker death every chunk the pool held is rerun one at a time,
        # so the one that kills its worker again is known. Bisect if a death gets expensive.
        while slot.suspects and not slot.inflight:
            chunk = slot.suspects[0]
            job = chunk.job
            if job.status == RUNNING:
                if job.cost is not None and not self.tokens.fits(job.cost):
                    return
                slot.suspects.popleft()
                self._launch(chunk, alone=True)
            else:
                slot.suspects.popleft()

    def _launch(self, chunk: _Chunk, alone: bool) -> None:
        job = chunk.job
        slot = job.slot
        if job.cost is not None:
            self.tokens.take(job.cost)
        slot.inflight += 1
        slot.isolating = chunk.alone = alone
        job.flying.add(chunk)
        try:
            chunk.handle = slot.impl.submit(job.fn, chunk.payload, chunk)
        except Exception:
            self._on_done(chunk, ERROR, None, "the executor refused the chunk\n" + traceback.format_exc(), 0)

    # --- Delivery thread --------------------------------------------------------------

    def _deliver(self) -> None:
        while True:
            item = self._deliveries.get()
            if item is None:
                return
            fn, args, confirm = item
            error = None
            try:
                fn(*args)
            except Exception:
                error = traceback.format_exc()
                if confirm is None:
                    log.error("job callback %r failed\n%s", fn, error)
            if confirm is not None:
                self._events.put((self._settle, *confirm, error))

    def _announce(self, job: _Job) -> None:
        try:
            if job.on_terminal is not None:
                job.on_terminal(job.snapshot())
        finally:
            job.finished.set()
