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

import hashlib
import itertools
import mmap
import os
import shutil
import sys
import tempfile
import threading
import time
import traceback
import weakref
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from .files import Dir, swap

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

    `shared` says that every node sees the disk of this machine, as a cluster started here does.
    Without it each `File` of an element goes once to Ray's object store and a node keeps its
    copy in its temporary directory, and what a result names under a `Dir` comes back with it.
    """

    # ponytail: the time limit of a chunk counts from when Ray takes its task, so a job whose
    # chunks ask for more CPUs than Ray has free spends part of it waiting there. The task
    # saying when it began, through the tally, is what would count only the run.
    uses_tokens, wants_cost = False, True

    def __init__(self, address: str | None = None, capacity: int | None = None, shared: bool | None = None, **init):
        import ray

        self._ray, self.shared = ray, address is None if shared is None else shared
        # (path, when it changed, size) -> _Carried, for as long as a task that reads it is with
        # Ray: after the last one nothing holds it and the object store lets it go.
        self._carried = weakref.WeakValueDictionary()
        self._flying, self._killed = set(), set()  # the tasks Ray has, and those of them `kill` stopped
        # How far each watched task is: the nodes tell an actor, and what it holds is asked for
        # whenever `progress` is, so that one answers with what the time before brought.
        self._tally, self._watched, self._said, self._asking = None, {}, {}, False
        self._ids = itertools.count()
        ray.init(address=address, **{"include_dashboard": False, "log_to_driver": False, **init})
        # How many tasks are handed to Ray at once: it queues those it has no room for.
        self.capacity = capacity or int(ray.cluster_resources().get("CPU", 1))
        self._lost = (ray.exceptions.WorkerCrashedError, ray.exceptions.NodeDiedError,
                      ray.exceptions.ObjectLostError, ray.exceptions.OutOfMemoryError)
        # max_retries=0: the engine is the one that runs a chunk again
        self._run = ray.remote(max_retries=0)(_apply if self.shared else _apply_far)
        self._sized = {}  # cost -> `_run` asking Ray for that much

    def bind(self, on_done) -> None:
        self._on_done = on_done

    def submit(self, fn, payload, tag, call=None, cost=(1, 0, 0), owner=None):
        run = self._sized.get(cost)
        if run is None:
            cpu, gpu, mem_mb = cost
            run = self._sized[cost] = self._run.options(num_cpus=cpu, num_gpus=gpu,
                                                        memory=int(mem_mb * 2 ** 20) or None)
        dirs, held = [], []  # where it writes, and the files it reads, held until it ends
        if not self.shared:
            payload = swap(payload, partial(self._mark, dirs, held))
        call, index = call or _call, None
        if owner is not None:
            if self._tally is None:
                self._tally = self._ray.remote(num_cpus=0)(_Tally).remote()
            index = next(self._ids)
            self._watched[index] = owner
            call = partial(_told, self._tally, index, call)
        ref = run.remote(call, fn, payload)
        self._flying.add(ref)
        ref.future().add_done_callback(partial(self._finished, tag, ref, index, dirs, held))
        return ref

    def progress(self, owner) -> float:
        if self._tally is not None and not self._asking:
            self._asking = True
            self._tally.all.remote().future().add_done_callback(self._heard)
        said = self._said
        return sum(said.get(index, 0.0) for index, whose in list(self._watched.items()) if whose == owner)

    def _heard(self, future) -> None:
        self._asking = False
        if not future.cancelled() and future.exception() is None:
            self._said = future.result()

    def kill(self) -> None:
        # Everything Ray has, which is what the engine asks for: it could stop one task alone.
        for ref in list(self._flying):
            self._killed.add(ref)
            self._ray.cancel(ref, force=True)

    def _mark(self, dirs: list, held: list, mark):
        if isinstance(mark, Dir):
            if mark not in dirs:
                dirs.append(str(mark))
            return _Out(dirs.index(mark))
        at = os.stat(mark)
        key = (str(mark), at.st_mtime_ns, at.st_size)
        carried = self._carried.get(key)
        if carried is None:
            with open(mark, "rb") as file:
                carried = self._carried[key] = _Carried(
                    os.path.basename(mark), hashlib.blake2b(repr(key).encode(), digest_size=8).hexdigest(),
                    self._ray.put(file.read()))
        held.append(carried)
        return carried

    def submit_many(self, fn, payloads, tag, whole=False, cost=(1, 0, 0), owner=None):
        return self.submit(fn, payloads, tag, _call_whole if whole else _call_many, cost, owner)

    def _finished(self, tag, ref, index, dirs, held, future) -> None:
        self._flying.discard(ref)
        self._watched.pop(index, None)
        status, value, error = _outcome(future, self._lost)
        if status == ERROR and isinstance(future.exception(), self._ray.exceptions.TaskCancelledError):
            status = CANCELED
        if ref in self._killed:
            self._killed.discard(ref)
            if status != DONE:  # as when a pool is killed: the engine knows which one it was after
                status, error = LOST, error or "stopped with its worker"
        if status == DONE and not self.shared:
            (result, peak_mb), root, files = value

            def here(text):  # a path of the node's directory, as the one it stands for here
                if not text.startswith(root + os.sep):
                    return text
                n, _, rest = text[len(root) + 1:].partition(os.sep)
                return os.path.join(dirs[int(n)], rest) if rest else dirs[int(n)]

            # ponytail: written on Ray's own thread; a thread of the executor once files get large.
            for name, data in files.items():
                target = here(os.path.join(root, name))
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with open(target, "wb") as file:
                    file.write(data)
            value = swap(result, here, str), peak_mb
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


class _Tally:
    """The actor of a `RayExecutor` that holds how far each of its watched tasks is."""

    def __init__(self):
        self.done = {}

    def tell(self, index, done):
        self.done[index] = done

    def drop(self, index):
        self.done.pop(index, None)

    def all(self):
        return self.done


def _told(tally, index, call, fn, payload):
    """`_watched` on a node: what the task says goes to the tally, twice a second at most."""
    global _slots, _at
    said, over = [0.0], threading.Event()

    def tell():
        sent = 0.0
        while not over.wait(0.5):
            if said[0] != sent:
                sent = said[0]
                tally.tell.remote(index, sent)

    threading.Thread(target=tell, daemon=True).start()
    _slots, _at = (None, said), [0, 0, 1]
    try:
        return call(fn, payload)
    finally:
        _at = None
        over.set()
        tally.drop.remote(index)  # ponytail: a task stopped with its worker leaves its number there


class _Carried:
    """A file in Ray's object store."""

    def __init__(self, name, id, ref):
        self.name, self.id, self.ref = name, id, ref

    def land(self) -> str:
        """Its path on this node, which keeps it for the tasks that follow."""
        # ponytail: nothing takes the copies away but whatever cleans the node's temporary
        # directory; a sweep at `shutdown` once a cluster that lives for weeks fills its disks.
        path = os.path.join(tempfile.gettempdir(), "msflow-files", self.id, self.name)
        if not os.path.exists(path):
            import ray

            os.makedirs(os.path.dirname(path), exist_ok=True)
            fd, coming = tempfile.mkstemp(dir=os.path.dirname(path))
            with os.fdopen(fd, "wb") as file:
                file.write(ray.get(self.ref))
            os.replace(coming, path)  # whole or not there: another task of this node may be bringing it too
        return path


class _Out:
    """The n-th `Dir` of a task."""

    def __init__(self, n):
        self.n = n


def _apply_far(call, fn, payload):
    """`_apply` on a node with a disk of its own: the files come first, and what the result names goes back."""
    root = tempfile.mkdtemp(prefix="msflow-out-")
    try:
        def land(mark):
            if isinstance(mark, _Carried):
                return mark.land()
            os.makedirs(os.path.join(root, str(mark.n)), exist_ok=True)
            return os.path.join(root, str(mark.n))

        value, files = call(fn, swap(payload, land, (_Carried, _Out))), {}

        def take(text):
            if text.startswith(root + os.sep):
                inside = [os.path.join(folder, name) for folder, _, names in os.walk(text) for name in names]
                # ponytail: the files go back in memory with the result; a file of the store for
                # each once a task leaves more than fits there.
                for path in inside or [text] * os.path.isfile(text):
                    with open(path, "rb") as file:
                        files[os.path.relpath(path, root)] = file.read()
            return text

        swap(value[0], take, str)
        return value, root, files
    finally:
        shutil.rmtree(root, ignore_errors=True)


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
