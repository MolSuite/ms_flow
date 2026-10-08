"""The engine: one object inside the application process that runs jobs of chunks.

The state lives in memory and belongs to the loop thread alone. Everything else reaches
it through one queue: callers leave orders, executors leave finished chunks, the delivery
thread leaves confirmations. The loop only moves state and tokens and decides what is
dispatched next. User code runs elsewhere: the source of chunks on a feeder thread per
job, the callbacks on the delivery thread, the disk on the writer thread of each journal.
"""
from __future__ import annotations

import hashlib
import itertools
import json
import logging
import os
import pickle
import queue
import threading
import time
import traceback
import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from .executors import DONE, ERROR, LOST
from .journal import INTERRUPTED, Journal, own
from .tokens import Tokens, memory_budget_mb

log = logging.getLogger(__name__)

WAITING = "waiting"  # for the jobs it was submitted after
RUNNING = "running"
COMPLETED = "completed"
COMPLETED_WITH_ERRORS = "completed_with_errors"
FAILED = "failed"
CANCELED = "canceled"

_PER_CHUNK = ("_cpu_required", "_gpu_required", "_mem_mb")  # in the order of a cost


def content_key(payload) -> bytes:
    """16 bytes that identify a chunk by what it carries, the same in every run.

    Equal keys mean equal payloads. The reverse is not promised: a payload built another
    way (dict order, a set) gets another key, so on resume it is repeated, never skipped.
    """
    try:
        data = pickle.dumps(payload, protocol=4)
    except Exception:
        return uuid.uuid4().bytes  # nothing to recognise it by in another run
    return hashlib.blake2b(data, digest_size=16).digest()


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
    key: str | None  # what the job is called in its journal; submitting it again continues it


class _Job:
    __slots__ = ("id", "name", "slot", "fn", "cost", "window", "max_cpu", "priority",
                 "on_result", "on_error", "on_terminal", "status", "total", "done", "failed", "open",
                 "flying", "cpu_used", "seq", "ready", "room", "hungry", "fed_all", "fatal", "error",
                 "peak_mem_mb", "started_at", "finished_at", "finished", "journal", "n", "uid", "spec",
                 "skip", "after", "chunks")

    def snapshot(self) -> JobSnapshot:
        return JobSnapshot(self.id, self.name, self.status, self.total, self.done, self.failed, self.open,
                           self.error, self.peak_mem_mb, self.started_at, self.finished_at, self.uid)


class _Chunk:
    """A chunk between dispatch and its result being delivered. Finished ones are only counted."""

    __slots__ = ("job", "key", "payload", "cost", "handle", "alone")

    def __init__(self, job: _Job, key, payload, cost):
        self.job, self.key, self.payload, self.cost = job, key, payload, cost
        self.handle, self.alone = None, False


class _Slot:
    """A registered executor and what the loop tracks about it."""

    __slots__ = ("impl", "inflight", "suspects", "isolating")

    def __init__(self, impl):
        self.impl, self.inflight, self.suspects, self.isolating = impl, 0, deque(), False


class Engine:
    def __init__(self, cpus: int | None = None, gpus: int = 0, mem_mb: int | None = None, state_dir=None):
        """`mem_mb` is the memory budget for chunks; by default physical RAM minus a reserve.

        With `state_dir`, this is the one engine that shares out the local machine: it owns
        `engine.lock` there, and a second process gets `Busy` instead of the same CPUs.
        """
        self._lock = own(Path(state_dir) / "engine.lock") if state_dir is not None else None
        self.tokens = Tokens(cpus or os.cpu_count() or 1, gpus,
                             memory_budget_mb() if mem_mb is None else mem_mb)
        self._executors: dict[str, _Slot] = {}
        self._journals: dict[str, Journal] = {}
        self._jobs: dict[int, _Job] = {}
        self._active: list[_Job] = []  # running jobs, by priority and then arrival
        self._waiting: list[_Job] = []
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

    def attach(self, name: str, path) -> None:
        """Open the journal at `path`; jobs submitted with `journal=name` are recorded in it.

        Raises `Busy` when another process owns that file.
        """
        if name in self._journals:
            raise ValueError(f"a journal is already attached as {name!r}")
        self._journals = {**self._journals, name: Journal(path)}

    def detach(self, name: str) -> None:
        """Interrupt the jobs recorded in this journal, wait until it is written and close it."""
        if self._closed:
            return
        journal = self._journals[name]
        self._journals = {k: v for k, v in self._journals.items() if k != name}
        told = threading.Event()
        self._events.put((self._on_detach, journal, told))
        told.wait()
        journal.close()

    def submit(self, executor: str, fn, chunks, *, cpu: float = 1, gpu: float = 0, mem_mb: float = 0,
               window: int | None = None, max_cpu: float | None = None, priority: int = 0,
               total: int | None = None, name: str = "", journal: str | None = None, key: str | None = None,
               after=(), on_result=None, on_error=None, on_terminal=None) -> int:
        """Run `fn(payload)` for every payload in `chunks` on `executor`. Returns the job id.

        `cpu`, `gpu` and `mem_mb` are what one chunk needs; a dict payload may say otherwise
        for itself with `_cpu_required`, `_gpu_required` and `_mem_mb`, which `fn` never sees.
        `max_cpu` caps the job as a whole; a chunk above the cap runs alone in its job.
        `chunks` is read on its own thread. `window` is how many chunks may be between dispatch
        and delivered result, and how many more may be read ahead.
        A chunk's key is `content_key(payload)`, or the `_key` of a dict payload.
        `on_result(key, result)`, `on_error(key, error)` and `on_terminal(snapshot)` run on
        the delivery thread, in order; a chunk counts as done once `on_result` returns.
        `journal` names an attached journal; without it nothing about the job reaches the disk.
        `key` is what the job is called there. Submitting an interrupted job again under its key
        continues it: the chunks it had done are recognised by their own keys and not run again.
        `after` is the ids of the jobs that must end first; `chunks` is not read until they do,
        and the job fails if one of them fails, is canceled or is interrupted.
        """
        if self._closed:
            raise RuntimeError("engine is closed")
        slot = self._executors.get(executor)
        if slot is None:
            raise ValueError(f"unknown executor {executor!r}; registered: {sorted(self._executors)}")
        job = _Job()
        job.journal = job.n = job.uid = job.spec = job.skip = None
        job.after = [self._jobs[job_id] for job_id in after]
        if journal is not None:
            job.journal = self._journals.get(journal)
            if job.journal is None or job.journal.failed:
                raise ValueError(f"no usable journal {journal!r}; attached: {sorted(self._journals)}")
            job.uid = key or uuid.uuid4().hex
            job.spec = json.dumps({
                "executor": executor, "fn": f"{getattr(fn, '__module__', '')}:{getattr(fn, '__qualname__', repr(fn))}",
                "cpu": cpu, "gpu": gpu, "mem_mb": mem_mb, "window": window, "max_cpu": max_cpu, "priority": priority})
        job.cost = (cpu, gpu, mem_mb) if slot.impl.uses_tokens else None
        if job.cost is not None and not self.tokens.ever_fits(job.cost):
            raise ValueError(f"a chunk needing (cpu, gpu, mem_mb)={job.cost} can never run: "
                             f"the engine has {self.tokens.total}")
        job.id, job.name, job.slot, job.fn, job.max_cpu = next(self._ids), name, slot, fn, max_cpu
        job.window = window or 2 * slot.impl.capacity
        job.priority, job.on_result, job.on_error, job.on_terminal = priority, on_result, on_error, on_terminal
        job.status, job.chunks = WAITING if job.after else RUNNING, chunks
        job.total = len(chunks) if total is None and hasattr(chunks, "__len__") else total
        job.done = job.failed = job.open = job.seq = job.peak_mem_mb = 0
        if key is not None and job.journal is not None:
            resumed = job.journal.claim(key)
            if resumed is not None:
                job.n, job.skip = resumed
                job.done = len(job.skip)
        job.flying, job.cpu_used, job.fatal, job.error = set(), 0, None, None
        job.ready, job.room, job.hungry, job.fed_all = deque(), threading.Event(), False, False
        job.started_at, job.finished_at, job.finished = time.time(), None, threading.Event()
        self._jobs[job.id] = job
        self._events.put((self._on_submit, job))
        return job.id

    def recorded(self, journal: str) -> list[dict]:
        """The jobs on record in an attached journal, oldest first, as rows of its `jobs` table."""
        return self._journals[journal].jobs()

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
        for journal in self._journals.values():
            journal.close()
        if self._lock is not None:
            self._lock.close()

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
                for journal in self._journals.values():
                    if journal.failed is None:
                        journal.wait_if_behind()
                    else:
                        self._on_stop_jobs_of(journal, FAILED, "the journal could not be written\n" + journal.failed)
            except Exception:
                log.exception("engine loop: dispatch failed")

    def _on_submit(self, job: _Job) -> None:
        if job.journal is not None:
            job.journal.job_created(job)
            if job.journal not in self._journals.values():  # detached between submit() and here
                job.fatal = "the journal of this job was detached"
                return self._finish(job, FAILED)
        if job.after:
            self._waiting.append(job)
            self._on_dependency_end(job)
        else:
            self._start(job)

    def _on_dependency_end(self, job: _Job) -> None:
        """Start a waiting job once every job before it ended well; fail it if one did not."""
        ended = {dep.status: dep for dep in job.after}
        for status in ended.keys() - {WAITING, RUNNING, COMPLETED, COMPLETED_WITH_ERRORS}:
            job.fatal = f"job {ended[status].id}, which this one comes after, ended as {status}"
            return self._finish(job, FAILED)
        if WAITING not in ended and RUNNING not in ended:
            self._waiting.remove(job)
            self._start(job)

    def _start(self, job: _Job) -> None:
        job.status, job.after, chunks, job.chunks = RUNNING, (), job.chunks, None
        self._active.append(job)
        self._active.sort(key=lambda j: (-j.priority, j.id))
        threading.Thread(target=self._feed, args=(job, chunks), name=f"msflow-feed-{job.id}", daemon=True).start()

    def _on_stop_job(self, job: _Job, status: str) -> None:
        # ponytail: chunks already running are not killed; their tokens come back when they
        # end and the result is dropped. Killing one arrives with the per-chunk time limit.
        if job.status not in (RUNNING, WAITING):
            return
        for chunk in tuple(job.flying):
            job.slot.impl.cancel(chunk.handle)
        self._finish(job, status)

    def _on_stop_jobs_of(self, journal: Journal, status: str, fatal: str | None = None) -> None:
        for job in (*self._waiting, *self._active):  # waiting ones first: they are stopped, not failed
            if job.journal is journal:
                job.fatal = fatal or job.fatal
                self._on_stop_job(job, status)

    def _on_detach(self, journal: Journal, told: threading.Event) -> None:
        self._on_stop_jobs_of(journal, INTERRUPTED)
        self._deliveries.put((told.set, (), None))  # behind the callbacks of the jobs just stopped

    def _on_close(self) -> None:
        for job in (*self._waiting, *self._active):  # waiting ones first: they are stopped, not failed
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
        if chunk.cost is not None:
            self.tokens.give(chunk.cost)
            job.cpu_used -= chunk.cost[0]
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
        if job.journal is not None:
            job.journal.chunk_finished(job, key, error)
        if error is None:
            job.done += 1
        else:
            job.failed += 1
            job.error = job.error or error
            if job.on_error is not None:
                self._deliveries.put((job.on_error, (key, error), None))
        self._finish_if_over(job)

    def _finish_if_over(self, job: _Job) -> None:
        if job.fed_all and not job.ready and not job.open:
            nothing_worked = job.fatal or (job.failed and not job.done)
            self._finish(job, FAILED if nothing_worked else COMPLETED_WITH_ERRORS if job.failed else COMPLETED)

    def _finish(self, job: _Job, status: str) -> None:
        job.status, job.finished_at, job.chunks, job.skip = status, time.time(), None, None
        job.error = job.fatal or job.error
        job.ready.clear()
        job.room.set()  # a feeder waiting for room wakes up, sees the status and leaves
        for jobs in (self._active, self._waiting):
            if job in jobs:
                jobs.remove(job)
        for other in [j for j in self._waiting if job in j.after]:
            self._on_dependency_end(other)
        if job.journal is not None:
            job.journal.job_ended(job, forget_done=status != INTERRUPTED)  # an interrupted job resumes
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
        if job.open >= job.window or slot.inflight >= slot.impl.capacity or slot.suspects or slot.isolating:
            return False
        ready = job.ready
        if not ready:
            job.hungry = True  # the feeder wakes the loop after its next chunk...
            if not ready:  # ...unless that chunk arrived in between
                return False
        chunk = ready[0]
        cost = chunk.cost
        if cost is not None and self.tokens.ever_fits(cost) and (
                not self.tokens.fits(cost)
                or (job.max_cpu is not None and job.cpu_used and job.cpu_used + cost[0] > job.max_cpu)):
            return False
        ready.popleft()
        if 2 * len(ready) <= job.window and not job.room.is_set():
            job.room.set()  # half empty: one wake-up refills it, instead of one per chunk
        job.seq += 1
        job.open += 1
        if cost is None or self.tokens.ever_fits(cost):
            self._launch(chunk, alone=False)
        else:
            self._settle(job, chunk.key, f"a chunk needing (cpu, gpu, mem_mb)={cost} can never run: "
                                         f"the engine has {self.tokens.total}")
        return True

    def _on_fed(self) -> None:
        """Nothing to record: the dispatch pass that follows every event picks the chunk up."""

    def _on_source_end(self, job: _Job, fatal: str | None, matched: int) -> None:
        if job.status == RUNNING:
            if job.skip:
                if not matched:
                    log.warning("job %s (%s): the source brought back none of its %d done chunks, so all of it "
                                "runs again; payloads must be the same in every run", job.id, job.name, len(job.skip))
                job.done -= len(job.skip) - matched  # done chunks the source no longer brings do not count
                job.skip = None
            job.fed_all, job.fatal, job.total = True, fatal, job.seq + len(job.ready) + matched
            self._finish_if_over(job)

    def _retry_suspect(self, slot: _Slot) -> None:
        # ponytail: after a worker death every chunk the pool held is rerun one at a time,
        # so the one that kills its worker again is known. Bisect if a death gets expensive.
        while slot.suspects and not slot.inflight:
            chunk = slot.suspects[0]
            job = chunk.job
            if job.status == RUNNING:
                if chunk.cost is not None and not self.tokens.fits(chunk.cost):
                    return
                slot.suspects.popleft()
                self._launch(chunk, alone=True)
            else:
                slot.suspects.popleft()

    def _launch(self, chunk: _Chunk, alone: bool) -> None:
        job = chunk.job
        slot = job.slot
        if chunk.cost is not None:
            self.tokens.take(chunk.cost)
            job.cpu_used += chunk.cost[0]
        slot.inflight += 1
        slot.isolating = chunk.alone = alone
        job.flying.add(chunk)
        try:
            chunk.handle = slot.impl.submit(job.fn, chunk.payload, chunk)
        except Exception:
            self._on_done(chunk, ERROR, None, "the executor refused the chunk\n" + traceback.format_exc(), 0)

    # --- Feeder threads, one per job --------------------------------------------------

    def _feed(self, job: _Job, chunks) -> None:
        fatal, matched, skip = None, 0, job.skip
        try:
            ready, room, window = job.ready, job.room, job.window
            for payload in chunks:
                while len(ready) >= window:  # at most `window` chunks read ahead
                    room.clear()
                    if len(ready) >= window:
                        room.wait()
                if job.status != RUNNING:
                    return
                cost, key = job.cost, None
                if type(payload) is dict and ("_key" in payload or not payload.keys().isdisjoint(_PER_CHUNK)):
                    payload = dict(payload)
                    key = payload.pop("_key", None)
                    own = [payload.pop(name, None) for name in _PER_CHUNK]
                    if cost is not None:
                        cost = tuple(c if o is None else o for c, o in zip(cost, own))
                if key is None:
                    key = content_key(payload)
                if skip and key in skip:  # done before the job was interrupted
                    matched += 1
                    continue
                ready.append(_Chunk(job, key, payload, cost))
                if job.hungry:
                    job.hungry = False
                    self._events.put((self._on_fed,))
        except Exception:
            fatal = "the source of chunks failed\n" + traceback.format_exc()
        self._events.put((self._on_source_end, job, fatal, matched))

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
            if job.journal is not None:
                job.journal.flush()  # whoever hears that the job ended finds it on disk
            if job.on_terminal is not None:
                job.on_terminal(job.snapshot())
        finally:
            job.finished.set()
