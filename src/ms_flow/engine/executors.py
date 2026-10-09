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
- `kill()`, optional: stop whatever is running, now. Every chunk in flight comes back `LOST`.
  Without it a chunk cannot have a time limit, and one that started runs to its end.

`status` is `DONE`, `ERROR` (the function raised), `LOST` (the worker went away: nobody
knows whether the chunk ran) or `CANCELED`.
"""
from __future__ import annotations

import os
import sys
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

    def bind(self, on_done) -> None:
        self._on_done = on_done

    def submit(self, fn, payload, tag, call=None):
        call = call or _call
        try:
            future = self._pool.submit(call, fn, payload)
        except self._lost:  # a worker died: loky refuses this pool for good
            self._pool.shutdown(wait=False, kill_workers=True)
            self._pool = self._new_pool()
            future = self._pool.submit(call, fn, payload)
        future.add_done_callback(partial(self._finished, tag))
        return future

    def submit_many(self, fn, payloads, tag, whole=False):
        return self.submit(fn, payloads, tag, _call_whole if whole else _call_many)

    def _finished(self, tag, future) -> None:
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


# --- Worker side --------------------------------------------------------------------

def _call(fn, payload):
    return fn(payload), _peak_mb()


def _call_many(fn, payloads):
    outcomes = []
    for payload in payloads:
        try:
            outcomes.append((fn(payload), None))
        except Exception:  # one chunk failing is not the others failing
            outcomes.append((None, traceback.format_exc()))
    return outcomes, _peak_mb()


def _call_whole(fn, payloads):
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
