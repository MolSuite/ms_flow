"""Executors: where a chunk actually runs.

The engine knows an executor only through this contract:

- `capacity`: how many chunks it holds at once.
- `uses_tokens`: whether its chunks spend the engine's local CPU, GPU and memory.
- `bind(on_done)`: called once, at registration. The executor then calls
  `on_done(tag, status, result, error, peak_mb)` exactly once per submitted chunk, from
  whichever thread of the main process learns that the chunk ended. Nothing is polled
  and nothing travels to the workers.
- `submit(fn, payload, tag) -> handle`: `tag` is opaque and comes back in `on_done`.
- `cancel(handle)`: best effort; a chunk that already started runs to its end.
- `shutdown()`.
- `submit_many(fn, payloads, tag, whole) -> handle`, optional: several chunks as one task, for
  the executors where a task costs more than a short chunk. Its `DONE` result is one
  `(result, error)` per payload, in order; any other status speaks for all of them. With
  `whole`, `fn` is called once with the list and returns one result per payload.
- `bind_parts(on_part)`, optional, called once at registration: for the executors that learn how
  each chunk of a `submit_many` task ended before the task is over. They call
  `on_part(tag, index, status, result, error)`, with `DONE` or `ERROR`, for the chunk at `index`
  of the payloads, at most once and before `on_done`; whatever `on_done` then says of that chunk
  is ignored.
- `kill()`, optional: stop whatever is running, now. Every chunk in flight comes back `LOST`.
  Without it a chunk cannot have a time limit, and one that started runs to its end.
- `progress(owner) -> float`, optional: how many chunks' worth of work the tasks in flight that
  were submitted with `owner=` that value have done. With it `submit` and `submit_many` take
  `owner`, and the engine passes it for the tasks worth watching.
- `batch`, optional: how many chunks the executor wants in each of its tasks, for the jobs that
  do not say.
- `wants_cost`, optional: an executor that does its own accounting sets it, and `submit` and
  `submit_many` then take `cost=(cpu, gpu, mem_mb)`: what the job says each of its chunks needs.

`status` is `DONE`, `ERROR` (the function raised), `LOST` (the worker went away: nobody
knows whether the chunk ran) or `CANCELED`.
"""
from __future__ import annotations

import mmap
import os
import sys
import tempfile
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from functools import partial

try:
    import resource
except ImportError:  # Windows
    resource = None

DONE, ERROR, LOST, CANCELED = "done", "error", "lost", "canceled"


def _outcome(future, lost: tuple = ()):
    if future.cancelled():
        return CANCELED, None, None
    exc = future.exception()
    if exc is None:
        return DONE, future.result(), None
    return (LOST if isinstance(exc, lost) else ERROR), None, "".join(traceback.format_exception(exc))


class ThreadExecutor:
    def __init__(self, max_workers: int, uses_tokens: bool = False):
        self.capacity = max_workers
        self.uses_tokens = uses_tokens
        self._pool = ThreadPoolExecutor(max_workers, thread_name_prefix="msflow-chunk")

    def bind(self, on_done) -> None:
        self._on_done = on_done

    def submit(self, fn, payload, tag):
        future = self._pool.submit(fn, payload)
        future.add_done_callback(partial(self._finished, tag))
        return future

    def _finished(self, tag, future) -> None:
        self._on_done(tag, *_outcome(future), 0)

    def cancel(self, handle) -> None:
        handle.cancel()

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)


class ProcessExecutor:
    """loky process pool. A dead worker costs the pool, never the executor: it is rebuilt."""

    uses_tokens = True

    def __init__(self, max_workers: int):
        from loky.process_executor import BrokenProcessPool, ProcessPoolExecutor

        self.capacity = max_workers
        self._lost = (BrokenProcessPool,)
        self._new_pool = partial(ProcessPoolExecutor, max_workers, initializer=_die_with_parent,
                                 initargs=(os.getpid(),))
        self._pool = self._new_pool()
        self._slots = _Slots()

    def bind(self, on_done) -> None:
        self._on_done = on_done

    def submit(self, fn, payload, tag, call=None, owner=None):
        index = None if owner is None else self._slots.take(owner)
        task = (call or _call, fn, payload)
        if index is not None:
            task = (_watched, self._slots.path, index, *task)
        try:
            future = self._pool.submit(*task)
        except self._lost:  # a worker died: loky refuses this pool for good
            self._pool.shutdown(wait=False, kill_workers=True)
            self._pool = self._new_pool()
            future = self._pool.submit(*task)
        future.add_done_callback(partial(self._finished, tag, index))
        return future

    def submit_many(self, fn, payloads, tag, whole=False, owner=None):
        return self.submit(fn, payloads, tag, _call_whole if whole else _call_many, owner)

    def progress(self, owner) -> float:
        done = self._slots.done
        return sum(done[index] for index, whose in enumerate(self._slots.owners) if whose == owner)

    def _finished(self, tag, index, future) -> None:
        if index is not None:
            self._slots.free(index)
        status, value, error = _outcome(future, self._lost)
        result, peak_mb = value if status == DONE else (None, 0)
        self._on_done(tag, status, result, error, peak_mb)

    def cancel(self, handle) -> None:
        handle.cancel()

    def kill(self) -> None:
        # ponytail: loky gives up on the whole pool when one worker dies, so stopping one
        # chunk stops them all. A time limit raised inside the worker (SIGALRM) would spare
        # the rest, where the chunk is not stuck in native code; add it if limits fire often.
        from loky.backend.utils import kill_process_tree

        for process in list(self._pool._processes.values()):  # no public way to a busy worker
            kill_process_tree(process)  # and the programs it launched

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True, kill_workers=True)  # waiting is what lets loky clean up after itself
        try:
            os.unlink(self._slots.path)  # ponytail: stays behind on Windows, where a mapped file cannot go
        except OSError:
            pass


class RayExecutor:
    """Tasks on a Ray cluster, which shares out its own CPUs, GPUs and memory.

    `address=None` starts a cluster on this machine and stops it at `shutdown`; `"auto"` or
    `host:port` joins one that is running, on this machine or on others. What `ray.init` takes
    goes through. The functions and what they import have to be installed on every node.
    """

    # ponytail: no `kill`, so no time limit for a chunk on Ray, and no `progress`. A forced
    # `ray.cancel` of each task in flight is the first; the second needs a way back from the nodes.
    uses_tokens, wants_cost = False, True

    def __init__(self, address: str | None = None, capacity: int | None = None, **init):
        import ray

        self._ray = ray
        ray.init(address=address, **{"include_dashboard": False, "log_to_driver": False, **init})
        # How many tasks are handed to Ray at once: it queues those it has no room for.
        self.capacity = capacity or int(ray.cluster_resources().get("CPU", 1))
        self._lost = (ray.exceptions.WorkerCrashedError, ray.exceptions.NodeDiedError,
                      ray.exceptions.ObjectLostError, ray.exceptions.OutOfMemoryError)
        self._run = ray.remote(max_retries=0)(_apply)  # the engine is the one that runs a chunk again
        self._sized = {}  # cost -> `_run` asking Ray for that much

    def bind(self, on_done) -> None:
        self._on_done = on_done

    def submit(self, fn, payload, tag, call=None, cost=(1, 0, 0)):
        run = self._sized.get(cost)
        if run is None:
            cpu, gpu, mem_mb = cost
            run = self._sized[cost] = self._run.options(num_cpus=cpu, num_gpus=gpu,
                                                        memory=int(mem_mb * 2 ** 20) or None)
        ref = run.remote(call or _call, fn, payload)
        ref.future().add_done_callback(partial(self._finished, tag))
        return ref

    def submit_many(self, fn, payloads, tag, whole=False, cost=(1, 0, 0)):
        return self.submit(fn, payloads, tag, _call_whole if whole else _call_many, cost)

    def _finished(self, tag, future) -> None:
        status, value, error = _outcome(future, self._lost)
        if status == ERROR and isinstance(future.exception(), self._ray.exceptions.TaskCancelledError):
            status = CANCELED
        result, peak_mb = value if status == DONE else (None, 0)
        self._on_done(tag, status, result, error, peak_mb)

    def cancel(self, handle) -> None:
        self._ray.cancel(handle)

    def shutdown(self) -> None:
        self._ray.shutdown()


class _Slots:
    """One float for each task being watched, in a file the workers map: how many chunks' worth of
    its work the task has done. The workers write and `progress` reads: nothing queues, nothing
    wakes anybody up, and only the last value matters.
    """

    def __init__(self, n: int = 1024):  # ponytail: the tasks watched at once; one more runs unwatched
        fd, self.path = tempfile.mkstemp(prefix="msflow-progress-")
        os.ftruncate(fd, 8 * n)
        self.done = memoryview(mmap.mmap(fd, 8 * n)).cast("d")
        os.close(fd)
        self.owners = [None] * n
        self._free = list(range(n))

    def take(self, owner) -> int | None:
        try:
            index = self._free.pop()
        except IndexError:
            return None
        self.done[index], self.owners[index] = 0.0, owner
        return index

    def free(self, index: int) -> None:
        self.owners[index] = None
        self._free.append(index)


# --- Worker side --------------------------------------------------------------------

_slots = None  # (path, floats): the executor's progress file as this worker maps it
_at = None  # [slot, chunks of the task done, chunks `fn` is working on]: set while the task is watched


def report(fraction: float) -> None:
    """From a task: how far the element it is working on is, from 0 to 1.

    Does nothing unless the task is watched: a job of few elements, or one submitted with
    `progress=True`.
    """
    if _at is not None:
        _slots[1][_at[0]] = _at[1] + fraction * _at[2]


def _watched(path, index, call, fn, payload):
    global _slots, _at
    if _slots is None or _slots[0] != path:
        with open(path, "r+b") as file:
            _slots = (path, memoryview(mmap.mmap(file.fileno(), 0)).cast("d"))
    _at = [index, 0, 1]
    try:
        return call(fn, payload)
    finally:
        _at = None


def _call(fn, payload):
    return fn(payload), _peak_mb()


def _apply(call, fn, payload):
    return call(fn, payload)


def _call_many(fn, payloads):
    outcomes = []
    for payload in payloads:
        try:
            outcomes.append((fn(payload), None))
        except Exception:  # one chunk failing is not the others failing
            outcomes.append((None, traceback.format_exc()))
        if _at is not None:
            _at[1] = _slots[1][_at[0]] = len(outcomes)
    return outcomes, _peak_mb()


def _call_whole(fn, payloads):
    if _at is not None:
        _at[2] = len(payloads)
    results = list(fn(payloads))
    if len(results) != len(payloads):
        raise ValueError(f"{len(results)} results for {len(payloads)} payloads: a batched function returns one for each")
    return [(result, None) for result in results], _peak_mb()


def _peak_mb() -> int:
    """High-water mark of this worker plus its largest subprocess (the docking binary)."""
    if resource is None:
        return 0
    peak = (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            + resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
    return peak >> (20 if sys.platform == "darwin" else 10)  # bytes on macOS, KB elsewhere


def _die_with_parent(parent: int) -> None:
    """Worker initializer: exit when the process that owns the engine is gone."""

    def watch():
        # ponytail: POSIX only, getppid never changes on Windows. There it needs a
        # WaitForSingleObject on the parent handle. Subprocesses of the worker are not reaped.
        while os.getppid() == parent:
            time.sleep(1.0)
        os._exit(1)

    threading.Thread(target=watch, name="msflow-parent-watch", daemon=True).start()
