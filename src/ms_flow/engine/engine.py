"""The engine: one object inside the application process that runs jobs of chunks.

The state lives in memory and belongs to the loop thread alone. Everything else reaches
it through one queue: callers leave orders, executors leave finished chunks, the delivery
thread leaves confirmations. The loop only moves state and tokens and decides what is
dispatched next. User code runs elsewhere: the source of chunks on a feeder thread per
job, the callbacks on the delivery thread, the disk on the writer thread of each journal.
"""
from __future__ import annotations

import hashlib
import heapq
import itertools
import json
import logging
import os
import pickle
import queue
import reprlib
import threading
import time
import traceback
import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from .executors import DONE, ERROR, LOST
from .journal import INTERRUPTED, RESUMABLE, Journal, own
from .tokens import Tokens, memory_budget_mb, usable_cpus

log = logging.getLogger(__name__)

WAITING = "waiting"  # for the jobs it was submitted after
RUNNING = "running"
COMPLETED = "completed"
COMPLETED_WITH_ERRORS = "completed_with_errors"
FAILED = "failed"
CANCELED = "canceled"

_PER_CHUNK = ("_cpu_required", "_gpu_required", "_mem_mb")  # in the order of a cost
# Chunks shorter than this share a task, as many as it takes to fill it and at most _PACK_MAX.
# A task costs about 1 ms to hand out with every core busy, so 100 ms keeps that near 1 %.
# ponytail: one figure for every executor; let the executor name its own when a remote one,
# whose tasks cost far more than a local process's, starts packing.
_TASK_SECONDS, _PACK_MAX = 0.1, 64
_REMEMBERED = 1000  # ended jobs whose last snapshot the engine still answers for
_DELIVER_MAX = 256  # results of a job that reach its callback in one go
_RECOUNT = 30.0  # seconds between two counts of what a source has left


_brief = reprlib.Repr()  # a payload as a failed chunk is told apart: short whatever its size
_brief.maxstring = _brief.maxother = 80
_brief.maxdict = 8


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
    id: int | None  # None for one read from a journal that this engine did not run
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
    chunks_retried: int  # attempts beyond the first, over all its chunks
    progress: float | None  # 0 to 1, with how far the running chunks are; None while the total is unknown


class _Job:
    __slots__ = ("id", "name", "slot", "fn", "cost", "window", "max_cpu", "priority", "on_finish", "finishing",
                 "on_result", "on_results", "on_error", "on_terminal", "status", "total", "done", "failed", "open",
                 "flying", "cpu_used", "seq", "ready", "room", "hungry", "fed_all", "fatal", "error",
                 "peak_mem_mb", "started_at", "finished_at", "finished", "journal", "n", "uid", "spec",
                 "skip", "after", "chunks", "pace", "pack", "elastic", "retries", "retry_wait", "timeout",
                 "retried", "batch", "batched", "source", "fine", "needs")

    def snapshot(self) -> JobSnapshot:
        total, progress = self.total, None
        if total is not None:
            # ponytail: a task that ended stops counting here a moment before its chunks count
            # as done (they do once delivered), so this can step back a little. A state to show.
            running = self.slot.impl.progress(self.id) if self.slot.tells and self.flying else 0
            progress = min(1.0, (self.done + self.failed + running) / total) if total else 1.0
        return JobSnapshot(self.id, self.name, self.status, total, self.done, self.failed, self.open,
                           self.error, self.peak_mem_mb, self.started_at, self.finished_at, self.uid,
                           self.retried, progress)


class _Chunk:
    """A chunk between dispatch and its result being delivered. Finished ones are only counted."""

    __slots__ = ("job", "key", "payload", "cost", "handle", "alone", "pack", "started", "attempt", "doomed",
                 "spared")

    def __init__(self, job: _Job, key, payload, cost):
        self.job, self.key, self.payload, self.cost = job, key, payload, cost
        self.handle, self.alone, self.pack = None, False, None  # `pack`: the chunks sharing its task
        self.attempt = 1
        self.doomed = None  # why the engine is stopping it, when it is
        self.spared = False  # running on an executor the engine is stopping for another chunk's sake


class _Slot:
    """A registered executor and what the loop tracks about it."""

    __slots__ = ("impl", "inflight", "suspects", "isolating", "packs", "kills", "killing", "tells", "costs")

    def __init__(self, impl):
        self.impl, self.inflight, self.suspects, self.isolating = impl, 0, deque(), False
        self.packs, self.kills, self.killing = hasattr(impl, "submit_many"), hasattr(impl, "kill"), False
        self.tells, self.costs = hasattr(impl, "progress"), getattr(impl, "wants_cost", False)


class Engine:
    def __init__(self, cpus: int | None = None, gpus: int = 0, mem_mb: int | None = None, state_dir=None):
        """`mem_mb` is the memory budget for chunks; by default physical RAM minus a reserve.

        With `state_dir`, this is the one engine that shares out the local machine: it owns
        `engine.lock` there, and a second process gets `Busy` instead of the same CPUs.
        """
        self._lock = own(Path(state_dir) / "engine.lock") if state_dir is not None else None
        self.tokens = Tokens(cpus or usable_cpus(), gpus,
                             memory_budget_mb() if mem_mb is None else mem_mb)
        self._executors: dict[str, _Slot] = {}
        self._journals: dict[str, Journal] = {}
        self._jobs: dict[int, _Job] = {}  # the ones that have not ended
        self._ended: dict[int, JobSnapshot] = {}  # the last _REMEMBERED that have, oldest first
        self._active: list[_Job] = []  # running jobs, by priority and then arrival
        self._waiting: list[_Job] = []
        self._ids = itertools.count(1)
        self._timers: list = []  # (when, tie-break, fn, args), a heap; the loop's own
        self._ticks = itertools.count()
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
        """Add an executor (see `executors` for the contract), in the place of the one that had
        the name: see `unregister`."""
        self.unregister(name)
        executor.bind(self._chunk_done)
        if hasattr(executor, "bind_parts"):
            executor.bind_parts(self._part_done)
        self._executors = {**self._executors, name: _Slot(executor)}  # the loop iterates the old one

    def unregister(self, name: str) -> None:
        """Shut down the executor registered as `name`, if there is one.

        Raises `ValueError` while a job submitted to it has not ended: an executor changes
        between jobs, never under one.
        """
        slot = self._executors.get(name)
        if slot is None:
            return
        # ponytail: whoever changes executors is whoever submits; a submit from another thread
        # at this very moment could still land on the one going away.
        if any(job.slot is slot for job in list(self._jobs.values())):
            raise ValueError(f"executor {name!r} has jobs that have not ended")
        self._executors = {key: kept for key, kept in self._executors.items() if key != name}
        slot.impl.shutdown()

    def executor(self, name: str):
        """The executor registered as `name`."""
        return self._executors[name].impl

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
        told.clear()
        self._events.put((told.set,))  # behind the loop taking note of what those callbacks took
        told.wait()
        journal.close()

    def submit(self, executor: str, fn, chunks, *, cpu: float = 1, gpu: float = 0, mem_mb: float = 0,
               window: int | None = None, max_cpu: float | None = None, priority: int = 0,
               total: int | None = None, name: str = "", journal: str | None = None, key: str | None = None,
               after=(), retries: int = 0, retry_wait: float = 1.0, timeout: float | None = None,
               batch: int | None = None, batched: bool = False, progress: bool | None = None,
               on_result=None, on_results=None, on_error=None, on_finish=None, on_terminal=None) -> int:
        """Run `fn(payload)` for every payload in `chunks` on `executor`. Returns the job id.

        `cpu`, `gpu` and `mem_mb` are what one chunk needs; a dict payload may say otherwise
        for itself with `_cpu_required`, `_gpu_required` and `_mem_mb`, which `fn` never sees.
        `max_cpu` caps the job as a whole; a chunk above the cap runs alone in its job.
        `chunks` is read on its own thread. `window` is how many chunks may be between dispatch
        and delivered result, and how many more may be read ahead.
        Chunks that take well under a task's worth of time travel several to a task where the
        executor allows it; each still succeeds, fails and is recorded on its own. Left to
        itself the window grows with them, since such chunks are small by construction.
        `batch` settles it instead: that many chunks to a task, fewer only in the last one and
        where chunks with different needs meet. With `batched`, `fn` takes the list of payloads
        of a task and returns one result for each, in order; if it raises, all of them failed.
        A chunk's key is `content_key(payload)`, or the `_key` of a dict payload.
        `on_result(key, result)`, `on_error(key, error)` and `on_terminal(snapshot)` run on
        the delivery thread, in order; a chunk counts as done once `on_result` returns.
        `on_results(pairs)` takes its place for whoever writes results somewhere: it gets every
        `(key, result)` of the job that is waiting, up to `_DELIVER_MAX`, and all of them count as
        done once it returns. When it raises, each pair is offered again alone and only the ones
        that raise again fail, so it must leave nothing behind when it raises: one transaction.
        `on_finish()` is the job's last word: called once every chunk has ended, on the delivery
        thread, before the job counts as completed and the jobs after it start. If it raises, the
        job fails with that error. A job that failed, was canceled or interrupted never gets there.
        `chunks` may know more than how to be read. With `remaining()`, how many chunks it has yet
        to give, the job has a total while it runs: counted at its first chunk and every
        `_RECOUNT` seconds after, exact once the source is read. With `open(state)`, it is called
        before the first read and what it returns (JSON) is kept with the job in its journal and
        passed back as `state` when the job continues; `state` is None the first time.
        `progress` says whether the tasks of the job are watched, for `JobSnapshot.progress` to
        count how far the running chunks are and not only the ones that ended (see
        `executors.report`). By default a task of several chunks is, and a task of one when the
        job has at most two for each worker.
        `journal` names an attached journal; without it nothing about the job reaches the disk.
        `key` is what the job is called there. Submitting an interrupted or canceled job again
        under its key continues it: the chunks it had done are recognised by their own keys and
        not run again. `discard` is how such a job is given up instead.
        `after` is the ids of the jobs that must end first; `chunks` is not read until they do,
        and the job fails if one of them fails, is canceled or is interrupted.
        A chunk that fails runs again up to `retries` times, `retry_wait` seconds after the first
        failure and twice as long after each one that follows. Failing is raising, running past
        `timeout` seconds or killing its worker; falling with somebody else's worker is not, and
        costs the chunk nothing. `on_error` hears of a chunk only once it has failed for good.
        `timeout` needs an executor that can stop a running chunk; the process pool stops all
        of its chunks to stop one, and the others start again.
        """
        if self._closed:
            raise RuntimeError("engine is closed")
        slot = self._executors.get(executor)
        if slot is None:
            raise ValueError(f"unknown executor {executor!r}; registered: {sorted(self._executors)}")
        if on_result is not None and on_results is not None:
            raise ValueError("on_result or on_results, not both")
        batch = batch or getattr(slot.impl, "batch", None)
        if (batch or batched) and not slot.packs:
            raise ValueError(f"executor {executor!r} takes one chunk to a task")
        if timeout is not None and not slot.kills:
            raise ValueError(f"executor {executor!r} cannot stop a running chunk, so its chunks cannot have a timeout")
        job = _Job()
        job.journal = job.n = job.uid = job.spec = job.skip = job.source = None
        job.retries, job.retry_wait, job.timeout, job.retried = retries, retry_wait, timeout, 0
        job.after = [self._jobs.get(job_id) or self._ended[job_id] for job_id in after]
        if journal is not None:
            job.journal = self._journals.get(journal)
            if job.journal is None or job.journal.failed:
                raise ValueError(f"no usable journal {journal!r}; attached: {sorted(self._journals)}")
            job.uid = key or uuid.uuid4().hex
            job.spec = json.dumps({
                "executor": executor, "fn": f"{getattr(fn, '__module__', '')}:{getattr(fn, '__qualname__', repr(fn))}",
                "cpu": cpu, "gpu": gpu, "mem_mb": mem_mb, "window": window, "max_cpu": max_cpu, "priority": priority,
                "retries": retries, "retry_wait": retry_wait, "timeout": timeout, "batch": batch})
        job.cost = (cpu, gpu, mem_mb) if slot.impl.uses_tokens else None
        job.needs = (cpu, gpu, mem_mb)
        if job.cost is not None and not self.tokens.ever_fits(job.cost):
            raise ValueError(f"a chunk needing (cpu, gpu, mem_mb)={job.cost} can never run: "
                             f"the engine has {self.tokens.total}")
        job.id, job.name, job.slot, job.fn, job.max_cpu = next(self._ids), name, slot, fn, max_cpu
        job.batch, job.batched, job.fine = batch or 0, batched, progress
        job.window = max(window or 2 * slot.impl.capacity * (batch or 1), batch or 1)
        job.pace, job.pack, job.elastic = 0.0, batch or 1, window is None
        job.priority, job.on_result, job.on_error, job.on_terminal = priority, on_result, on_error, on_terminal
        job.on_results, job.on_finish, job.finishing = on_results, on_finish, False
        job.status, job.chunks = WAITING if job.after else RUNNING, chunks
        job.total = len(chunks) if total is None and hasattr(chunks, "__len__") else total
        job.done = job.failed = job.open = job.seq = job.peak_mem_mb = 0
        if key is not None and job.journal is not None:
            resumed = job.journal.claim(key)
            if resumed is not None:
                job.n, job.skip, job.source = resumed  # `skip`: how many chunks it had done
                job.done = job.skip
        job.flying, job.cpu_used, job.fatal, job.error = set(), 0, None, None
        job.ready, job.room, job.hungry, job.fed_all = deque(), threading.Event(), False, False
        job.started_at, job.finished_at, job.finished = time.time(), None, threading.Event()
        self._jobs[job.id] = job
        self._events.put((self._on_submit, job))
        return job.id

    def recorded(self, journal: str) -> list[dict]:
        """The jobs on record in an attached journal, oldest first, as rows of its `jobs` table."""
        return self._journals[journal].jobs()

    def discard(self, journal: str, key: str) -> None:
        """Drop the job recorded as `key`, which must not be running: what it had done is forgotten
        and the key is free again. An interrupted or canceled job keeps a row per done chunk until
        it is continued or discarded.
        """
        self._journals[journal].discard(key)

    def status(self, job_id: int) -> JobSnapshot:
        """Where the job is. An ended job is forgotten once `_REMEMBERED` others ended after it;
        what a journal recorded of it stays in `recorded()`."""
        job = self._jobs.get(job_id)
        return self._ended[job_id] if job is None else job.snapshot()

    def jobs(self) -> list[JobSnapshot]:
        ended = list(self._ended.values())  # first: a job ending right now shows twice, never zero times
        return ended + [job.snapshot() for job in tuple(self._jobs.values())]

    def wait(self, job_id: int, timeout: float | None = None) -> JobSnapshot:
        """Block until the job is terminal and its callbacks have run, or `timeout` passes."""
        job = self._jobs.get(job_id)
        if job is None:
            return self._ended[job_id]
        job.finished.wait(timeout)
        return job.snapshot()

    def cancel(self, job_id: int) -> None:
        job = self._jobs.get(job_id)
        if job is not None:
            self._events.put((self._on_stop_job, job, CANCELED))
        elif job_id not in self._ended:
            raise KeyError(job_id)

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
        while not self._events.empty():  # what the last callbacks took, with the loop gone
            event = self._events.get()
            if event is not None and event[0] == self._on_delivered:
                event[0](*event[1:])
        for slot in self._executors.values():
            slot.impl.shutdown()
        for journal in self._journals.values():
            journal.close()
        if self._lock is not None:
            self._lock.close()

    # --- Loop thread ------------------------------------------------------------------

    def _loop(self) -> None:
        get, get_nowait, timers = self._events.get, self._events.get_nowait, self._timers
        while True:
            try:
                event = get(timeout=max(0.0, timers[0][0] - time.monotonic())) if timers else get()
            except queue.Empty:
                event = (self._on_fed,)  # nothing came: a timer is due
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
            while timers and timers[0][0] <= time.monotonic():
                _, _, fn, args = heapq.heappop(timers)
                try:
                    fn(*args)
                except Exception:
                    log.exception("engine loop: %s failed", fn.__name__)
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
        if job.timeout is not None:
            self._watch(job)

    def _later(self, seconds: float, fn, *args) -> None:
        heapq.heappush(self._timers, (time.monotonic() + seconds, next(self._ticks), fn, args))

    def _watch(self, job: _Job) -> None:
        """Stop the chunks of `job` that ran past its time limit. Runs every tenth of the limit."""
        if job.status != RUNNING:
            return
        now = time.perf_counter()
        late = [chunk for chunk in job.flying if now - chunk.started > job.timeout]
        for chunk in late:
            chunk.doomed = f"timed out: it ran for more than {job.timeout} s"
        if late:
            self._kill(job.slot)
        self._later(max(0.01, min(1.0, job.timeout / 10)), self._watch, job)

    def _kill(self, slot: _Slot) -> None:
        """Stop everything running on `slot`. What runs there for no fault of its own goes back in line."""
        for job in self._active:
            if job.slot is slot:
                for chunk in job.flying:
                    chunk.spared = chunk.doomed is None
        slot.killing = True  # nothing new starts there until all of it has come back
        slot.impl.kill()

    def _on_stop_job(self, job: _Job, status: str) -> None:
        # An executor that cannot kill lets its running chunks end: their tokens come back
        # then and the result is dropped.
        if job.status not in (RUNNING, WAITING):
            return
        running = bool(job.flying)
        for chunk in tuple(job.flying):
            job.slot.impl.cancel(chunk.handle)
        self._finish(job, status)
        if running and job.slot.kills:
            self._kill(job.slot)

    def _on_stop_jobs_of(self, journal: Journal, status: str, fatal: str | None = None) -> None:
        for job in (*self._waiting, *self._active):  # waiting ones first: they are stopped, not failed
            if job.journal is journal:
                job.fatal = fatal or job.fatal
                self._on_stop_job(job, status)

    def _on_detach(self, journal: Journal, told: threading.Event) -> None:
        self._on_stop_jobs_of(journal, INTERRUPTED)
        self._deliveries.put((told.set, ()))  # behind the callbacks of the jobs just stopped

    def _on_close(self) -> None:
        for job in (*self._waiting, *self._active):  # waiting ones first: they are stopped, not failed
            self._on_stop_job(job, INTERRUPTED)

    def _chunk_done(self, chunk, status, result, error, peak_mb=0) -> None:
        """What executors call. Any thread."""
        self._events.put((self._on_done, chunk, status, result, error, peak_mb))

    def _part_done(self, chunk, index, status, result, error) -> None:
        """What executors call for one chunk of a task that is still running. Any thread."""
        self._events.put((self._on_part, chunk, index, status, result, error))

    def _on_part(self, chunk: _Chunk, index: int, status: str, result, error) -> None:
        job = chunk.job
        member = chunk.pack and chunk.pack[index]
        if job.status != RUNNING or not member:
            return
        chunk.pack[index] = None  # ended: the task has nothing more to say of it
        if member is chunk:  # the task still goes by this one, so what ends, and may run again, is its twin
            member = _Chunk(job, chunk.key, chunk.payload, chunk.cost)
            member.attempt = chunk.attempt
        self._chunk_ended(member, status, result, error)

    def _on_done(self, chunk: _Chunk, status: str, result, error, peak_mb: int) -> None:
        job = chunk.job
        slot = job.slot
        slot.inflight -= 1
        if not slot.inflight:
            slot.killing = False
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
        pack, chunk.pack = chunk.pack, None
        if slot.packs and status == DONE:
            # Seconds per chunk as seen from here, so with the trip to the worker and back: a
            # payload that is expensive to send is never mistaken for a short chunk.
            took = max(time.perf_counter() - chunk.started, 1e-6) / (len(pack) if pack else 1)
            job.pace = took if took > job.pace else (job.pace + took) / 2  # slow down at once, speed up warily
            if not job.batch:
                job.pack = max(1, min(int(_TASK_SECONDS / job.pace), _PACK_MAX))
                if job.elastic:
                    job.window = 2 * slot.impl.capacity * job.pack
        if status == LOST and chunk.spared:  # it fell with a chunk the engine stopped
            for member in reversed(pack or (chunk,)):
                if member is not None:
                    self._requeue(member)
        elif pack is None:
            self._chunk_ended(chunk, status, result, error)
        elif status == DONE:  # the task ran: every chunk has its own outcome, unless it ended ahead
            for member, outcome in zip(pack, result):
                if member is not None:
                    self._chunk_ended(member, ERROR if outcome[1] else DONE, *outcome)
        else:  # lost, or stopped for running late: which one did it shows when each runs alone
            chunk.doomed = None
            for member in pack:
                if member is not None:
                    self._chunk_ended(member, status, None, error)

    def _chunk_ended(self, chunk: _Chunk, status: str, result, error) -> None:
        job = chunk.job
        if status == DONE:
            if job.on_result is None and job.on_results is None:
                self._settle(job, chunk.key, chunk.attempt, None)
            else:
                self._deliveries.put((job, chunk.key, chunk.attempt, result))
        elif status == LOST and chunk.doomed:
            self._attempt_failed(chunk, chunk.doomed)
        elif status == LOST and not chunk.alone:
            job.slot.suspects.append(chunk)  # collateral or culprit: running it alone will tell
        elif status == LOST:
            self._attempt_failed(chunk, "the worker process died while running this chunk\n" + error)
        else:
            self._attempt_failed(chunk, error or status)

    def _attempt_failed(self, chunk: _Chunk, error: str) -> None:
        job = chunk.job
        if chunk.attempt > job.retries:
            return self._settle(job, chunk.key, chunk.attempt, error, _brief.repr(chunk.payload))
        job.retried += 1
        self._later(job.retry_wait * 2 ** (chunk.attempt - 1), self._requeue, chunk)
        chunk.attempt += 1

    def _requeue(self, chunk: _Chunk) -> None:
        """Back to the head of the line, as if it had never left."""
        job = chunk.job
        if job.status == RUNNING:
            job.ready.appendleft(chunk)
            job.open -= 1
            job.seq -= 1

    def _on_delivered(self, job: _Job, outcomes: list) -> None:
        """The delivery thread's confirmation: these results reached the job's callback."""
        for key, attempt, error in outcomes:
            self._settle(job, key, attempt, error)

    def _settle(self, job: _Job, key, attempt: int, error: str | None, label: str | None = None) -> None:
        """A chunk reached its end: count it and forget it. `label` tells a failed one apart in
        the journal; there is none when its result is what could not be delivered."""
        if job.status != RUNNING:
            # Stopped with this result on its way to the callback. It got there: the job that
            # continues this one must not run it again.
            if error is None and job.journal is not None and job.status in RESUMABLE:
                job.done += 1
                job.journal.chunk_finished(job, key, attempt, None)
            return
        job.open -= 1
        if job.journal is not None:
            job.journal.chunk_finished(job, key, attempt, error, label)
        if error is None:
            job.done += 1
        else:
            job.failed += 1
            job.error = job.error or error
            if job.on_error is not None:
                self._deliveries.put((job.on_error, (key, error)))
        self._finish_if_over(job)

    def _finish_if_over(self, job: _Job) -> None:
        if job.fed_all and not job.ready and not job.open and not job.finishing:
            nothing_worked = job.fatal or (job.failed and not job.done)
            status = FAILED if nothing_worked else COMPLETED_WITH_ERRORS if job.failed else COMPLETED
            if status == FAILED or job.on_finish is None:
                self._finish(job, status)
            else:
                job.finishing = True
                self._deliveries.put((self._run_finish, (job, status)))

    def _run_finish(self, job: _Job, status: str) -> None:
        """On the delivery thread, behind every result of the job: its `on_finish`."""
        try:
            job.on_finish()
            error = None
        except Exception:
            error = traceback.format_exc()
        self._events.put((self._on_finished, job, status, error))

    def _on_finished(self, job: _Job, status: str, error: str | None) -> None:
        # ponytail: a job stopped while its `on_finish` runs keeps the status it was stopped
        # with, whatever `on_finish` got to do.
        if job.status == RUNNING:
            job.fatal = error
            self._finish(job, FAILED if error else status)

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
            job.journal.job_ended(job, forget_done=status not in RESUMABLE)
        self._deliveries.put((self._announce, (job,)))

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
        if (job.open >= job.window or slot.inflight >= slot.impl.capacity or slot.suspects or slot.isolating
                or slot.killing):
            return False
        ready = job.ready
        need = 1 if job.fed_all else job.batch or 1  # a batch of a set size waits until it is whole
        if len(ready) < need:
            job.hungry = True  # the feeder wakes the loop once that many are there...
            if len(ready) < need:  # ...unless they arrived in between
                return False
        chunk = ready[0]
        cost = chunk.cost
        if cost is not None and self.tokens.ever_fits(cost) and (
                not self.tokens.fits(cost)
                or (job.max_cpu is not None and job.cpu_used and job.cpu_used + cost[0] > job.max_cpu)):
            return False
        ready.popleft()
        job.seq += 1
        job.open += 1
        if cost is None or self.tokens.ever_fits(cost):
            if job.pack > 1 or job.batched:  # the ones behind it with the same needs share its task
                pack, size = [chunk], job.pack
                if job.fed_all and not job.batch:  # the tail is shared out, or the last tasks take it all and workers idle
                    size = min(size, -(-(len(ready) + 1) // slot.impl.capacity))
                while len(pack) < size and job.open < job.window and ready and ready[0].cost == cost:
                    pack.append(ready.popleft())
                    job.seq += 1
                    job.open += 1
                if len(pack) > 1 or job.batched:
                    chunk.pack = pack
            self._launch(chunk, alone=False)
        else:
            self._settle(job, chunk.key, 1, f"a chunk needing (cpu, gpu, mem_mb)={cost} can never run: "
                                         f"the engine has {self.tokens.total}")
        if 2 * len(ready) <= job.window and not job.room.is_set():
            job.room.set()  # half empty: one wake-up refills it, instead of one per chunk
        return True

    def _on_fed(self) -> None:
        """Nothing to record: the dispatch pass that follows every event picks the chunk up."""

    def _on_source_end(self, job: _Job, fatal: str | None) -> None:
        if job.status == RUNNING:
            # What a continued job had done counts whether or not the source brings it back: whoever
            # submits may well leave out what is already done.
            job.fed_all, job.fatal, job.total = True, fatal, job.seq + len(job.ready) + (job.skip or 0)
            job.skip = None
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
                if job.batched:
                    chunk.pack = [chunk]
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
        chunk.doomed, chunk.spared = None, False
        job.flying.add(chunk)
        chunk.started = time.perf_counter()
        # Watched: a task of several chunks, which counts them by itself, and a task of one when
        # its job has few (or says so), where how far each one is makes the difference.
        fine = job.fine
        if fine is None:
            fine = chunk.pack is not None or (job.total is not None and job.total <= 2 * slot.impl.capacity)
        watch = {"owner": job.id} if fine and slot.tells else {}
        if slot.costs:
            # ponytail: what the job says, not what one chunk says of itself (`cpu` in its payload):
            # that is read only where the engine does the accounting.
            watch["cost"] = job.needs
        try:
            if chunk.pack is None:
                chunk.handle = slot.impl.submit(job.fn, chunk.payload, chunk, **watch)
            else:  # one task, so one worker and one chunk's tokens: they run one after another
                chunk.handle = slot.impl.submit_many(job.fn, [c.payload for c in chunk.pack], chunk, job.batched,
                                                     **watch)
        except Exception:
            self._on_done(chunk, ERROR, None, "the executor refused the chunk\n" + traceback.format_exc(), 0)

    # --- Feeder threads, one per job --------------------------------------------------

    def _feed(self, job: _Job, chunks) -> None:
        fatal = None
        seen, close = job.journal.done_before(job.n) if job.skip else (None, None)
        try:
            ready, room = job.ready, job.room
            remaining, fresh, recount = getattr(chunks, "remaining", None), 0, 0.0
            if hasattr(chunks, "open"):
                state = chunks.open(json.loads(job.source) if job.source else None)
                job.source = None if state is None else json.dumps(state)
            for payload in chunks:
                while len(ready) >= job.window:  # at most `window` chunks read ahead
                    room.clear()
                    if len(ready) >= job.window:
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
                if seen is not None and seen(key):  # done before the job was interrupted
                    continue
                ready.append(_Chunk(job, key, payload, cost))
                if remaining is not None:
                    # First at the first chunk that is not done already: what a continued job had
                    # done is mostly what its source brings first, and counting it twice would
                    # show a total too high until the next count.
                    fresh += 1
                    if (now := time.monotonic()) >= recount:
                        job.total, recount = (job.skip or 0) + fresh + remaining(), now + _RECOUNT
                if job.hungry and len(ready) >= job.batch:
                    job.hungry = False
                    self._events.put((self._on_fed,))
        except Exception:
            fatal = "the source of chunks failed\n" + traceback.format_exc()
        finally:
            if close is not None:
                close()
        self._events.put((self._on_source_end, job, fatal))

    # --- Delivery thread --------------------------------------------------------------

    def _deliver(self) -> None:
        get, get_nowait, held = self._deliveries.get, self._deliveries.get_nowait, []
        while True:
            item = held.pop() if held else get()
            if item is None:
                return
            if type(item[0]) is not _Job:
                fn, args = item
                try:
                    fn(*args)
                except Exception:
                    log.exception("job callback %r failed", fn)
                continue
            # A result, as (job, key, attempt, result), and the ones of its job waiting right behind
            # it: one call for the callback, one event for the loop.
            # ponytail: results of two jobs that arrive interleaved go one at a time; a buffer per
            # job if two jobs of short chunks writing at once turns out to be a common case.
            job, batch = item[0], [item]
            while len(batch) < _DELIVER_MAX:
                try:
                    item = get_nowait()
                except queue.Empty:
                    break
                if item is None or item[0] is not job:
                    held.append(item)
                    break
                batch.append(item)
            self._events.put((self._on_delivered, job, self._hand_over(job, batch)))

    @staticmethod
    def _hand_over(job: _Job, batch: list) -> list:
        """Give `batch` to the job's callback. Returns `(key, attempt, error)` for each result."""
        errors = [None] * len(batch)
        if job.on_results is None:
            for i, (_, key, _, result) in enumerate(batch):
                try:
                    job.on_result(key, result)
                except Exception:
                    errors[i] = traceback.format_exc()
        else:
            pairs = [(key, result) for _, key, _, result in batch]
            try:
                job.on_results(pairs)
            except Exception:
                if len(pairs) == 1:
                    errors[0] = traceback.format_exc()
                else:  # which ones it cannot take shows when each goes alone
                    for i, pair in enumerate(pairs):
                        try:
                            job.on_results([pair])
                        except Exception:
                            errors[i] = traceback.format_exc()
        return [(key, attempt, error) for (_, key, attempt, _), error in zip(batch, errors)]

    def _announce(self, job: _Job) -> None:
        try:
            if job.journal is not None:
                job.journal.flush()  # whoever hears that the job ended finds it on disk
            if job.on_terminal is not None:
                job.on_terminal(job.snapshot())
        finally:
            # The engine lets go of the job, and with it of its function and callbacks.
            self._ended[job.id] = job.snapshot()  # before leaving `_jobs`: never in neither
            self._jobs.pop(job.id, None)
            if len(self._ended) > _REMEMBERED:
                del self._ended[next(iter(self._ended))]
            job.finished.set()
