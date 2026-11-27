"""Executors: where a chunk actually runs.

The engine knows an executor only through this contract:

- `capacity`: how many chunks it holds at once.
- `uses_tokens`: whether its chunks spend the engine's local CPU, GPU and memory.
- `bind(on_done)`: called once, at registration. The executor then calls
  `on_done(tag, status, result, error, used)` exactly once per submitted chunk, from
  whichever thread of the main process learns that the chunk ended. `used` is what the task
  used, the dict `_Usage.report()` gives (or its peak of memory in MB, or 0, when it cannot
  tell). Nothing is polled and nothing travels to the workers.
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
- `progress(owner) -> dict`, optional: how many chunks' worth of work each task in flight that
  was submitted with `owner=` that value has done, by its `tag`. With it `submit` and
  `submit_many` take `owner`, and the engine passes it for the tasks worth watching.
- `hosts() -> dict`, optional: for an executor of several machines, the one each task in flight
  runs on, by its `tag`, as far as it is known yet.
- `watches_all`, optional: an executor for which watching a task costs nothing sets it, and every
  task of a job that does not say otherwise is worth watching.
- `batch`, optional: how many chunks the executor wants in each of its tasks, for the jobs that
  do not say.
- `threads`, optional: an executor that keeps a worker for each task and can spare it sets it.
  `submit` and `submit_many` then take `thread=True` for the tasks of a job that only wait for a
  program they start (see `launched`).
- `wants_cost`, optional: an executor that does its own accounting sets it, and `submit` and
  `submit_many` then take `cost=(cpu, gpu, mem_mb)`: what the chunks of the task need, as the
  job says or each chunk of itself.
- `outlives`, optional: an executor whose tasks go on without this process sets it. `submit` and
  `submit_many` then take `key`, the key of the job in its journal (`None` for a job without
  one), and `keys`, the key of each chunk of the task. A job with a key is one the executor is
  asked for again: the same chunks, in tasks that need not be the same. `cancel` on a task of
  such a job stops nothing by itself; the engine says how the job ended with
  `ended(key, kept)`, and only with `kept` (the job was interrupted and may be submitted again)
  is what the executor has of it to go on.

`status` is `DONE`, `ERROR` (the function raised), `LOST` (the worker went away: nobody
knows whether the chunk ran) or `CANCELED`.
"""
from __future__ import annotations

import collections
import hashlib
import itertools
import math
import mmap
import os
import pickle
import shutil
import socket
import sys
import tempfile
import threading
import time
import traceback
import uuid
import weakref
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from types import SimpleNamespace

from ..core.data.companions import companions, whole
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
    """loky process pool. A dead worker costs the pool, never the executor: it is rebuilt.

    The tasks of a job submitted with `thread=True` run instead on threads of one more process,
    the same for all of them (see `_Host`), and no worker is started or kept for them: they
    count against the same `capacity`.
    A worker, or that process, with nothing to do for `idle` seconds leaves, with the memory of
    what it imported, and one is started again when there is work for it; `None` keeps them all.
    With `gpus`, a task that needs some runs with `CUDA_VISIBLE_DEVICES` set to the cards it
    holds, of those this process sees, the least held first; it runs in a worker, never on a thread.
    """

    uses_tokens = threads = watches_all = True  # watching: a float in a file the worker has mapped
    autobatch, batch_max, task_seconds = True, 4096, 0.0  # see `Engine.submit(autobatch=)`

    def __init__(self, max_workers: int, idle: float | None = 60.0, gpus: int = 0):
        from loky.process_executor import BrokenProcessPool, ProcessPoolExecutor

        self.capacity = max_workers
        self._lost = (BrokenProcessPool,)
        self._new_pool = partial(ProcessPoolExecutor, max_workers, timeout=idle, initializer=_die_with_parent,
                                 initargs=(os.getpid(),))
        self._pool = self._new_pool()
        self._slots = _Slots()
        self._idle = idle
        self._host, self._hosting = None, threading.Lock()  # the `_Host`, started with the first task for it
        seen = os.environ.get("CUDA_VISIBLE_DEVICES", "").replace(" ", "").split(",")
        names = [name for name in seen if name] or [str(card) for card in range(gpus)]
        self.wants_cost, self._held, self._carding = gpus > 0, dict.fromkeys(names[:gpus], 0.0), threading.Lock()

    def bind(self, on_done) -> None:
        self._on_done = on_done

    def submit(self, fn, payload, tag, call=None, owner=None, thread=False, cost=None):
        index = None if owner is None else self._slots.take(owner, tag)
        task = (call or _call, fn, payload)
        if index is not None:
            task = (_watched, self._slots.path, index, *task)
        cards = self._take_cards(cost[1] if cost else 0)
        if cards:
            task, thread = (_on_cards, ",".join(cards), *task), False
        if thread:
            with self._hosting:
                if self._host is None or self._host.over:
                    self._host = _Host(self.capacity, self._idle, self._hosting)
                future = self._host.submit(task)
            future.add_done_callback(partial(self._landed, tag, index))
            return future
        try:
            future = self._pool.submit(*task)
        except self._lost:  # a worker died: loky refuses this pool for good
            self._pool.shutdown(wait=False, kill_workers=True)
            self._pool = self._new_pool()
            future = self._pool.submit(*task)
        future.add_done_callback(partial(self._finished, tag, index))
        if cards:
            future.add_done_callback(lambda _: self._free_cards(cards, cost[1]))
        return future

    def submit_many(self, fn, payloads, tag, whole=False, owner=None, thread=False, cost=None):
        return self.submit(fn, payloads, tag, _call_whole if whole else _call_many, owner, thread, cost)

    def _take_cards(self, gpu: float) -> list:
        # The engine's tokens keep the sum of what is held within the cards: here, only which ones.
        if not gpu or not self._held:
            return []
        with self._carding:
            cards = sorted(self._held, key=self._held.get)[:max(1, math.ceil(gpu))]
            for card in cards:
                self._held[card] += gpu / len(cards)
        return cards

    def _free_cards(self, cards: list, gpu: float) -> None:
        with self._carding:
            for card in cards:
                self._held[card] -= gpu / len(cards)

    def progress(self, owner) -> dict:
        slots = self._slots
        return {slots.tags[index]: slots.done[index] for index, whose in enumerate(slots.owners) if whose == owner}

    def _finished(self, tag, index, future) -> None:
        if index is not None:
            self._slots.free(index)
        status, value, error = _outcome(future, self._lost)
        result, used = value if status == DONE else (None, 0)
        self._on_done(tag, status, result, error, used)

    def _landed(self, tag, index, future) -> None:
        if index is not None:
            self._slots.free(index)
        # ponytail: no peak of memory. The process is not the task's, and the program it runs
        # is not measured: its `Popen` would have to be waited with `os.wait4`.
        status, value, error = future.result()
        result, used = value if status == DONE else (None, 0)
        self._on_done(tag, status, result, error, used)

    def cancel(self, handle) -> None:
        host = getattr(handle, "host", None)
        if host is None:
            handle.cancel()
        else:
            host.cancel(handle.id)

    def kill(self) -> None:
        # ponytail: loky gives up on the whole pool when one worker dies, so stopping one
        # chunk stops them all. A time limit raised inside the worker (SIGALRM) would spare
        # the rest, where the chunk is not stuck in native code; add it if limits fire often.
        from loky.backend.utils import kill_process_tree

        with self._hosting:  # first: loky takes its time over each worker
            if self._host is not None and self._host.pending:
                self._host.kill()
        for process in list(self._pool._processes.values()):  # no public way to a busy worker
            kill_process_tree(process)  # and the programs it launched

    def shutdown(self) -> None:
        with self._hosting:
            if self._host is not None:
                self._host.kill()
        self._pool.shutdown(wait=True, kill_workers=True)  # waiting is what lets loky clean up after itself
        try:
            os.unlink(self._slots.path)  # ponytail: stays behind on Windows, where a mapped file cannot go
        except OSError:
            pass


def _cluster(ray, capacity: int | None = None) -> tuple[int, tuple]:
    """How many tasks go to Ray at once, and the (cpu, gpu, mem_mb) the engine fills with them:
    a task Ray has no room for waits in its queue, and would count as running all the while."""
    have = ray.cluster_resources()
    total = (have.get("CPU", 1), have.get("GPU", 0), int(have.get("memory", 0) / 2 ** 20))
    return capacity or int(total[0]), total


class RayExecutor:
    """Tasks on a Ray cluster, which shares out its own CPUs, GPUs and memory.

    `address=None` starts a cluster on this machine and stops it at `shutdown`; `"auto"` or
    `host:port` joins one that is running, on this machine or on others. What `ray.init` takes
    goes through. The functions and what they import have to be installed on every node.

    `shared` says that every node sees the disk of this machine, as a cluster started here does.
    Without it each `File` of an element goes to Ray's object store in pieces, lands once on each
    node that reads it, in its temporary directory, until `shutdown`, and the tasks that read it
    go where it already is when there is room. What a result names under a `Dir` comes back in
    pieces too, and a thread of this executor writes it, whole or not there, before the result
    is given.

    `names` takes each node's hostname to the name its tasks are shown under in `hosts`; without
    it the nodes are not asked where they are.
    """

    # ponytail: the time limit of a chunk counts from when Ray takes its task, so a job whose
    # chunks ask for more CPUs than Ray has free spends part of it waiting there. The task
    # saying when it began, through the tally, is what would count only the run.
    uses_tokens, wants_cost = False, True
    autobatch, batch_max, task_seconds = True, 4096, 0.0  # see `Engine.submit(autobatch=)`

    def __init__(self, address: str | None = None, capacity: int | None = None, shared: bool | None = None,
                 names: dict | None = None, **init):
        import ray

        self._ray, self.shared = ray, address is None if shared is None else shared
        # (path, when it changed, size) -> _Carried, for as long as a task that reads it is with
        # Ray: after the last one nothing holds it and the object store lets it go.
        self._carried = weakref.WeakValueDictionary()
        self._flying, self._killed = set(), set()  # the tasks Ray has, and those of them `kill` stopped
        # How far each watched task is: the nodes tell an actor, and what it holds is asked for
        # whenever `progress` is, so that one answers with what the time before brought.
        # `_watched` holds every task that tells the tally something, by its index: (owner, tag),
        # `owner` None for one that only says where it runs.
        self._tally, self._watched, self._said, self._asking = None, {}, {}, False
        self._names, self._at = names, {}  # hostname -> shown name; index -> hostname, as told
        self._ids = itertools.count()
        # What Ray starts on this machine dies with the thread that started it: this one stays
        # until `shutdown`, so whoever makes the executor may do it from a thread that ends.
        self._over, started, failed = threading.Event(), threading.Event(), []

        def keep():
            try:
                ray.init(address=address, **{"include_dashboard": False, "log_to_driver": False, **init})
            except BaseException as error:  # raised below, in the thread that asked
                failed.append(error)
                return
            finally:
                started.set()
            self._over.wait()

        threading.Thread(target=keep, name="msflow-ray", daemon=True).start()
        started.wait()
        if failed:
            raise failed[0]
        # How many tasks are handed to Ray at once: it queues those it has no room for.
        self.capacity, self.total = _cluster(ray, capacity)
        self._lost = (ray.exceptions.WorkerCrashedError, ray.exceptions.NodeDiedError,
                      ray.exceptions.ObjectLostError, ray.exceptions.OutOfMemoryError)
        # max_retries=0: the engine is the one that runs a chunk again
        self._sized = {}  # (cost, node) -> `_run` asking Ray for that much, there
        if self.shared:
            self._run = ray.remote(max_retries=0)(_apply)
            return
        self._lost += (ray.exceptions.OwnerDiedError,)
        self._home = uuid.uuid4().hex[:12]  # where the nodes keep what this executor carried or left
        self._run = ray.remote(max_retries=0)(_apply_far)
        # What a task leaves that is too large to come with its result comes after it, from one
        # process on its node that stays (a task would wait for Ray to start one, seconds each
        # time): piece by piece, each its own object of the store, at most two ahead of the one
        # being written. Not a streaming generator: waiting for its next piece holds the GIL.
        self._fetcher = ray.remote(num_cpus=0, max_restarts=0, max_concurrency=4)(_Fetcher)
        self._fetchers, self._fetchers_lock = {}, threading.Lock()  # node -> its fetcher
        self._at_nodes, self._turn = {}, itertools.count()  # file id -> nodes it landed on
        # ponytail: four writers; more once many tasks give back large files at once.
        self._writer = ThreadPoolExecutor(4, thread_name_prefix="msflow-ray-write")

    def bind(self, on_done) -> None:
        self._on_done = on_done

    def node_resources(self, *, resources=None, key=None) -> tuple:
        return tuple(
            (have.get("CPU", 0), have.get("GPU", 0), int(have.get("memory", 0) / 2**20))
            for node in self._ray.nodes() if node["Alive"]
            if (have := node["Resources"])
        )

    def submit(self, fn, payload, tag, call=None, cost=(1, 0, 0), owner=None):
        dirs, held = [], []  # where it writes, and the files it reads, held until it ends
        if not self.shared:
            payload = swap(payload, partial(self._mark, dirs, held))
        node = self._near(held)
        run = self._sized.get((cost, node))
        if run is None:
            cpu, gpu, mem_mb = cost
            near = {}
            if node is not None:  # soft: a node without room for it now spills it to another
                from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

                near = {"scheduling_strategy": NodeAffinitySchedulingStrategy(node, soft=True, _spill_on_unavailable=True)}
            run = self._sized[cost, node] = self._run.options(num_cpus=cpu, num_gpus=gpu,
                                                              memory=int(mem_mb * 2 ** 20) or None, **near)
        call, index = call or _call, None
        if owner is not None or self._names is not None:
            if self._tally is None:
                self._tally = self._ray.remote(num_cpus=0)(_Tally).remote()
            index = next(self._ids)
            self._watched[index] = owner, tag
            if owner is not None:
                call = partial(_told, self._tally, index, call)
            if self._names is not None:
                call = partial(_placed, self._tally, index, call)
        if cost[1]:
            call = partial(_on_cards, None, call)  # Ray names the cards: only which ones, for the log
        ref = run.remote(call, fn, payload) if self.shared else run.remote(self._home, call, fn, payload)
        self._flying.add(ref)
        ref.future().add_done_callback(partial(self._finished, tag, ref, index, dirs, held))
        return ref

    def _near(self, held: list):
        """A node where a file the task reads has landed already, taking turns among them."""
        nodes = [node for carried in held for node in self._at_nodes.get(carried.id, ())]
        return nodes[next(self._turn) % len(nodes)] if nodes else None

    def progress(self, owner) -> dict:
        self._ask()
        said = self._said
        return {tag: said.get(index, 0.0) for index, (whose, tag) in list(self._watched.items()) if whose == owner}

    def hosts(self) -> dict:
        self._ask()
        at, names = self._at, self._names or {}
        return {tag: names.get(at[index], at[index]) for index, (_, tag) in list(self._watched.items()) if index in at}

    def nodes(self) -> dict:
        """Where each node of the cluster says what its machine is using, by the name `hosts`
        gives it (this machine is `local`): the URL of the metrics Ray exports there, as
        Prometheus text, CPU, memory and GPUs of the whole machine. Asks Ray, so not from a
        thread that cannot wait."""
        here, names, urls = self._ray.get_runtime_context().get_node_id(), self._names or {}, {}
        # ponytail: two nodes on one machine are shown as the first, the head before any other.
        for node in sorted(self._ray.nodes(), key=lambda node: node["NodeID"] != here):
            if node["Alive"]:
                name = "local" if node["NodeID"] == here else names.get(node["NodeManagerHostname"], node["NodeManagerHostname"])
                urls.setdefault(name, f"http://{node['NodeManagerAddress']}:{node['MetricsExportPort']}/metrics")
        return urls

    def _ask(self) -> None:
        if self._tally is not None and not self._asking:
            self._asking = True
            self._tally.all.remote().future().add_done_callback(self._heard)

    def _heard(self, future) -> None:
        self._asking = False
        if not future.cancelled() and future.exception() is None:
            self._said, self._at = future.result()

    def kill(self) -> None:
        # Everything Ray has, which is what the engine asks for: it could stop one task alone.
        for ref in list(self._flying):
            self._killed.add(ref)
            self._ray.cancel(ref, force=True)
        if not self.shared:
            with self._fetchers_lock:
                fetchers, self._fetchers = self._fetchers, {}
            for fetcher in fetchers.values():  # what it was bringing comes back lost
                self._ray.kill(fetcher)

    def _mark(self, dirs: list, held: list, mark):
        if isinstance(mark, Dir):
            if mark not in dirs:
                dirs.append(str(mark))
            return _Out(dirs.index(mark))
        paths = [str(mark), *companions(mark)]  # a container goes with its index (I14)
        key = tuple((path, at.st_mtime_ns, at.st_size) for path in paths for at in [os.stat(path)])
        carried = self._carried.get(key)
        if carried is None:
            refs = [(os.path.basename(path), [self._ray.put(piece) for piece in _pieces(path)]) for path in paths]
            id_ = os.path.join(self._home, hashlib.blake2b(repr(key).encode(), digest_size=8).hexdigest())
            carried = self._carried[key] = _Carried(id_, refs)
            weakref.finalize(carried, self._at_nodes.pop, id_, None)
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
            node, value, root, files = value
            if node not in self._fetchers:  # ready before a task of that node leaves large files
                self._fetcher_on(node)
            for carried in held:
                nodes = self._at_nodes.setdefault(carried.id, [])
                if node not in nodes:
                    nodes.append(node)
            if files:  # not on Ray's thread
                self._writer.submit(self._land, tag, dirs, node, value, root, files)
                return
        result, used = value if status == DONE else (None, 0)
        self._on_done(tag, status, result, error, used)

    def _land(self, tag, dirs: list, node: str, value, root: str, files: list) -> None:
        """Each file the result names, here: data before index (I14), each whole or not there,
        then the result. `files` is `[(name, bytes)]`, or `[(name, size)]` for those left on the node."""
        from ..core.data.shard import _fsync_dir

        def here(text):  # a path of the node's directory, as the one it stands for here
            if not text.startswith(root + os.sep):
                return text
            n, _, rest = text[len(root) + 1:].partition(os.sep)
            return os.path.join(dirs[int(n)], rest) if rest else dirs[int(n)]

        fetcher = None if isinstance(files[0][1], bytes) else self._fetcher_on(node)
        ray, folders, target = self._ray, set(), None

        # Here before taken: a get that waits keeps the object store's lock, and freeing a piece
        # waits for that lock holding the GIL, which stops this whole process a second at a time.
        def get(ref):
            ray.wait([ref], fetch_local=True)
            return ray.get(ref)

        try:
            for name, data in files:
                target = here(os.path.join(root, name))
                folder = os.path.dirname(target)
                if folder not in folders:
                    os.makedirs(folder, exist_ok=True)
                    folders.add(folder)
                with open(target + ".tmp", "wb") as file:
                    if fetcher is None:
                        file.write(data)
                    else:
                        ahead, path = collections.deque(), os.path.join(root, name)
                        for at in range(0, data, _CHUNK):
                            ahead.append(fetcher.piece.remote(path, at, min(_CHUNK, data - at)))
                            if len(ahead) > 2:
                                file.write(get(ahead.popleft()))
                        while ahead:
                            file.write(get(ahead.popleft()))
                    file.flush()
                    os.fsync(file.fileno())
                os.replace(target + ".tmp", target)
                target = None
            for folder in folders:
                _fsync_dir(folder)
        except BaseException as exc:
            if target is not None and os.path.exists(target + ".tmp"):
                os.unlink(target + ".tmp")
            # What Ray could not bring is lost with the node; what could not be written here is an error.
            status = LOST if isinstance(exc, self._ray.exceptions.RayError) else ERROR
            return self._on_done(tag, status, None, "".join(traceback.format_exception(exc)), 0)
        finally:
            if fetcher is not None:
                fetcher.drop.remote(root)
        result, used = value
        self._on_done(tag, DONE, swap(result, here, str), None, used)

    def _fetcher_on(self, node: str):
        """The fetcher of `node`, started the first time. Hard affinity: only that node has the
        files; a node that went away takes its fetcher with it, and its tasks run again."""
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        with self._fetchers_lock:
            fetcher = self._fetchers.get(node)
            if fetcher is None:
                fetcher = self._fetchers[node] = self._fetcher.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(node, soft=False)).remote()
            return fetcher

    def cancel(self, handle) -> None:
        self._ray.cancel(handle)

    def shutdown(self) -> None:
        if not self.shared:
            self._writer.shutdown(wait=True)
            self._sweep()
        self._ray.shutdown()
        self._over.set()

    def _sweep(self) -> None:
        """Each node lets go of what it kept of the files this executor carried, and of those its
        tasks left there."""
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        sweep = self._ray.remote(num_cpus=0, max_retries=0)(_sweep)
        refs = [sweep.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node["NodeID"], soft=False)).remote(self._home)
                for node in self._ray.nodes() if node["Alive"]]
        self._ray.wait(refs, num_returns=len(refs), timeout=30)  # ponytail: a node that does not answer keeps them


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
        self.owners, self.tags = [None] * n, [None] * n
        self._free = list(range(n))

    def take(self, owner, tag) -> int | None:
        try:
            index = self._free.pop()
        except IndexError:
            return None
        self.done[index], self.tags[index], self.owners[index] = 0.0, tag, owner
        return index

    def free(self, index: int) -> None:
        self.owners[index] = self.tags[index] = None
        self._free.append(index)


class _Host:
    """The process where the tasks submitted with `thread=True` wait for their programs, on as many
    threads as the pool has workers.

    Whatever Python they run between programs (imports, reading what a program left) takes the
    GIL of that process, not of this one, where the threads of the engine and of an app's
    interface need it. It starts without the app's `__main__` and imports only what the tasks
    bring. One that dies, is killed or leaves after `idle` seconds without a task is over: its
    tasks come back `LOST` and the next one starts another.
    """

    # ponytail: one process, so one GIL for every task on a thread. Spread them over several
    # once its CPU is full (pure-Python work between the programs).

    def __init__(self, workers: int, idle: float | None, lock: threading.Lock):
        from loky.backend.context import get_context

        context = get_context("loky")
        self._conn, theirs = context.Pipe(duplex=True)
        self.process = context.Process(target=_serve, args=(theirs, workers), name="msflow-host", daemon=True)
        self.process.start()
        theirs.close()  # its end lives there now: when it dies, this one reads the end
        self._idle, self._lock, self._ids = idle, lock, itertools.count()
        self.pending, self.over = {}, False  # id -> Future of each task sent; whether it takes no more
        threading.Thread(target=self._read, name="msflow-host-reader", daemon=True).start()

    # `submit` and `kill` are called holding `lock`, which `cancel` and `_read` take.

    def submit(self, task) -> Future:
        from loky.backend.reduction import dumps

        task = bytes(dumps(task))  # unpickled on a thread there, where importing what it needs holds back nothing else
        future = Future()
        future.host, future.id = self, next(self._ids)
        self.pending[future.id] = future
        try:
            self._conn.send(("run", future.id, task))
        except OSError:  # it died: `_read` is about to learn it, and this task with the rest come back LOST
            pass
        return future

    def cancel(self, id) -> None:
        with self._lock:
            if not self.over:
                try:
                    self._conn.send(("cancel", id, None))  # one that has not begun; it says so
                except OSError:
                    pass

    def kill(self) -> None:
        from loky.backend.utils import kill_process_tree

        self.over = True
        kill_process_tree(self.process)  # and the programs it launched

    def _read(self) -> None:
        conn, pending = self._conn, self.pending
        while True:
            try:
                if not conn.poll(self._idle):
                    with self._lock:
                        if not pending and not self.over:
                            self.over = True  # it goes; whatever comes next starts another
                            conn.send(("stop", None, None))
                    continue
                id, status, value, error = conn.recv()
            except (EOFError, OSError):
                break
            if status == DONE:
                try:
                    value = pickle.loads(value)
                except Exception:  # what the task returned means nothing here
                    status, value, error = ERROR, None, traceback.format_exc()
            pending.pop(id).set_result((status, value, error))
        with self._lock:
            self.over = True
            lost = list(pending.values())
            pending.clear()
        conn.close()
        self.process.join()
        for future in lost:
            future.set_result((LOST, None, None))


def _serve(conn, workers: int) -> None:
    """The main thread of a `_Host`: the tasks go to its threads, and how each ended goes back."""
    _yield_cpu()
    from loky.backend.reduction import dumps
    from loky.backend.utils import kill_process_tree

    pool, running, sending = ThreadPoolExecutor(workers, thread_name_prefix="msflow-task"), {}, threading.Lock()

    def ended(id, future):
        running.pop(id, None)
        status, value, error = _outcome(future)
        if status == DONE:
            try:
                value = bytes(dumps(value))
            except Exception:
                status, value, error = ERROR, None, traceback.format_exc()
        with sending:
            conn.send((id, status, value, error))

    while True:
        try:
            kind, id, task = conn.recv()
        except EOFError:  # the engine is gone, and nobody waits for what runs here
            kill_process_tree(SimpleNamespace(pid=os.getpid(), kill=partial(os._exit, 1), join=None))
        if kind == "run":
            future = running[id] = pool.submit(_hosted, task)
            future.add_done_callback(partial(ended, id))
        elif kind == "cancel":
            future = running.get(id)
            if future is not None:
                future.cancel()
        else:  # "stop", with nothing running
            pool.shutdown()
            return


def _hosted(task):
    call, *task = pickle.loads(task)
    result, used = call(*task)
    # ponytail: where and how long only; cores and memory would be the process's, shared by its threads.
    return result, {"where": used["where"], "seconds": used["seconds"]}


# --- Worker side --------------------------------------------------------------------

_slots = None  # (path, floats): the executor's progress file as this process maps it
# What the task running on this thread goes by, since the tasks of a `_Host` share a process:
# `at` = [slot, chunks of the task done, chunks `fn` is working on] while it is watched.
_task = threading.local()


def report(fraction: float) -> None:
    """From a task, on the thread it was called on: how far the element it is working on is,
    from 0 to 1.

    Does nothing unless the task is watched: any on the process pool; elsewhere one of a job of few
    elements, or of a job submitted with `progress=True`.
    """
    at = getattr(_task, "at", None)
    if at is not None:
        _slots[1][at[0]] = at[1] + fraction * at[2]


def launched(program) -> None:
    """From a task: a program it started (a `subprocess.Popen`) and waits for.

    Does nothing: a task on a thread runs in a process of the pool's (see `_Host`), and a worker
    or that process is killed with whatever it started. Kept for its callers from when the tasks
    on a thread ran in the engine's process.
    """


def _watched(path, index, call, fn, payload):
    global _slots
    if _slots is None or _slots[0] != path:
        with open(path, "r+b") as file:
            _slots = (path, memoryview(mmap.mmap(file.fileno(), 0)).cast("d"))
    _task.at = [index, 0, 1]
    try:
        return call(fn, payload)
    finally:
        _task.at = None


class _Usage:
    """What the task in this block used: where it ran (host, and its cards), its seconds, the
    cores it kept busy (its CPU time and that of the programs it waited for, over its seconds)
    and `mb`, `_peak_mb()`. Two clock reads and no thread: cheap enough for every task.
    """

    # ponytail: no RAM/VRAM sampling. Tried (nvidia-smi + psutil on a thread): memory never
    # bound the size in the GPU runs and the guesses cost throughput; a lost task halves it.

    def __enter__(self):
        self.began, self.cpu = time.perf_counter(), sum(os.times()[:4])
        return self

    def __exit__(self, *_):
        self.seconds = time.perf_counter() - self.began
        self.cpu = (sum(os.times()[:4]) - self.cpu) / max(self.seconds, 1e-6)

    def report(self) -> dict:
        cards = getattr(_task, "cards", None)
        return {"where": socket.gethostname() + (f":{cards}" if cards else ""), "seconds": self.seconds,
                "cpu": self.cpu, "mb": _peak_mb()}


def _call(fn, payload):
    with _Usage() as used:
        result = fn(payload)
    return result, used.report()


def _on_cards(cards, call, *task):
    if cards is not None:
        os.environ["CUDA_VISIBLE_DEVICES"] = cards  # what the programs it starts see too
    _task.cards = os.environ.get("CUDA_VISIBLE_DEVICES") or None  # on Ray, the cards it gave: for the log
    try:
        return call(*task)
    finally:
        _task.cards = None


def _apply(call, fn, payload):
    return call(fn, payload)


class _Tally:
    """The actor of a `RayExecutor` that holds how far each of its watched tasks is, and where it runs."""

    def __init__(self):
        self.done, self.at = {}, {}

    def tell(self, index, done):
        self.done[index] = done

    def place(self, index, host):
        self.at[index] = host

    def drop(self, index):
        self.done.pop(index, None)
        self.at.pop(index, None)

    def all(self):
        return self.done, self.at


def _placed(tally, index, call, fn, payload):
    """A task on a node that says first which machine that is."""
    tally.place.remote(index, socket.gethostname())
    try:
        return call(fn, payload)
    finally:
        tally.drop.remote(index)


def _told(tally, index, call, fn, payload):
    """`_watched` on a node: what the task says goes to the tally, twice a second at most."""
    global _slots
    said, over = [0.0], threading.Event()

    def tell():
        sent = 0.0
        while not over.wait(0.5):
            if said[0] != sent:
                sent = said[0]
                tally.tell.remote(index, sent)

    threading.Thread(target=tell, daemon=True).start()
    _slots, _task.at = (None, said), [0, 0, 1]
    try:
        return call(fn, payload)
    finally:
        _task.at = None
        over.set()
        tally.drop.remote(index)  # ponytail: a task stopped with its worker leaves its number there


_CHUNK = 64 << 20  # the most of a file in one object of Ray's store


def _pieces(path: str):
    """The file at `path` in pieces of at most `_CHUNK` bytes, each read into the same buffer:
    whoever takes one copies it before asking for the next."""
    import numpy as np

    with open(path, "rb", buffering=0) as file:
        buffer = np.empty(min(os.fstat(file.fileno()).st_size, _CHUNK), np.uint8)
        while n := file.readinto(buffer):
            yield buffer[:n]


def _sweep(home: str) -> None:
    shutil.rmtree(os.path.join(tempfile.gettempdir(), "msflow-files", home), ignore_errors=True)


class _Carried:
    """A file in Ray's object store, and its companions: `refs` is `[(name, [ref of each piece])]`,
    the file first. `id` is unique to the executor that carries it."""

    def __init__(self, id, refs):
        self.id, self.refs = id, refs

    def land(self) -> str:
        """Its path on this node, which keeps it for the tasks that follow, until the executor's
        `shutdown`. Its companions land beside it, after it: the last one there means all of them
        are. The tasks of a node that want it at once wait for the first to bring it."""
        import fcntl  # ponytail: POSIX nodes only

        folder = os.path.join(tempfile.gettempdir(), "msflow-files", self.id)
        last = os.path.join(folder, self.refs[-1][0])
        if os.path.exists(last):
            return os.path.join(folder, self.refs[0][0])
        os.makedirs(folder, exist_ok=True)
        with open(folder + ".lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if not os.path.exists(last):
                import ray

                for name, refs in self.refs:
                    fd, coming = tempfile.mkstemp(dir=folder)
                    with os.fdopen(fd, "wb") as file:
                        for ref in refs:  # one piece in memory at a time
                            file.write(ray.get(ref))
                    os.replace(coming, os.path.join(folder, name))
        return os.path.join(folder, self.refs[0][0])


class _Out:
    """The n-th `Dir` of a task."""

    def __init__(self, n):
        self.n = n


_INLINE = 1 << 20  # the files a task leaves come back with its result when they are this, together, at most


def _apply_far(home, call, fn, payload):
    """`_apply` on a node with a disk of its own: the files come first, and what the result names
    goes back. Returns `(node, value, root, files)`, `files` in the order they are written: data
    before index (I14). Small, they are `[(name, bytes)]`; larger, `[(name, size)]`, left in
    `root` for `_Fetcher`."""
    import ray

    out = os.path.join(tempfile.gettempdir(), "msflow-files", home, "out")
    os.makedirs(out, exist_ok=True)
    root, left = tempfile.mkdtemp(dir=out), False
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
                named = [name for name in whole(text) if os.path.isfile(name)]  # a container, with its index
                for path in inside or named:
                    files[os.path.relpath(path, root)] = os.path.getsize(path)
            return text

        swap(value[0], take, str)
        names = sorted(files, key=lambda name: whole(name)[0] != name)
        node = ray.get_runtime_context().get_node_id()
        if sum(files.values()) > _INLINE:
            left = True
            return node, value, root, [(name, files[name]) for name in names]
        brought = []
        for name in names:
            with open(os.path.join(root, name), "rb") as file:
                brought.append((name, file.read()))
        return node, value, root, brought
    finally:
        if not left:
            shutil.rmtree(root, ignore_errors=True)


class _Fetcher:
    """On a node, what its tasks left there: the files, in pieces, and then the directory."""

    def piece(self, path: str, at: int, n: int):
        import numpy as np

        buffer = np.empty(n, np.uint8)
        with open(path, "rb", buffering=0) as file:
            file.seek(at)
            if file.readinto(buffer) != n:
                raise OSError(f"{path} is shorter than its task said")
        return buffer

    def drop(self, root: str) -> None:
        shutil.rmtree(root, ignore_errors=True)


def _call_many(fn, payloads):
    with _Usage() as used:
        outcomes = _each(fn, payloads)
    return outcomes, used.report()


def _each(fn, payloads):
    outcomes = []
    for payload in payloads:
        try:
            outcomes.append((fn(payload), None))
        except Exception:  # one chunk failing is not the others failing
            outcomes.append((None, traceback.format_exc()))
        at = getattr(_task, "at", None)
        if at is not None:
            at[1] = _slots[1][at[0]] = len(outcomes)
    return outcomes


def _call_whole(fn, payloads):
    at = getattr(_task, "at", None)
    if at is not None:
        at[2] = len(payloads)
    with _Usage() as used:
        results = list(fn(payloads))
    if len(results) != len(payloads):
        raise ValueError(f"{len(results)} results for {len(payloads)} payloads: a batched function returns one for each")
    return [(result, None) for result in results], used.report()


def _peak_mb() -> int:
    """High-water mark of this worker plus its largest subprocess (an external binary)."""
    if resource is None:
        return 0
    peak = (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            + resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
    return peak >> (20 if sys.platform == "darwin" else 10)  # bytes on macOS, KB elsewhere


WORKER_NICE = 10


def _yield_cpu() -> None:
    """A worker, and the programs it starts, give way to the app's interface for the CPU: with
    every core busy, opening a big view took twice as long (380 -> 770 ms; 470 at nice 10). Nothing
    is lost on a machine with nothing else to run."""
    if hasattr(os, "nice"):  # ponytail: POSIX only; Windows would need SetPriorityClass
        os.nice(WORKER_NICE)
    # A worker holds the cores its tokens give, no more: `import numpy` started an OpenBLAS thread
    # per core that burned ~1 s of CPU per process. Set before any task imports numpy; a value the
    # user set is kept. ponytail: a task with cpu > 1 that wants BLAS threads sets its own.
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")


def _die_with_parent(parent: int) -> None:
    """Worker initializer: exit when the process that owns the engine is gone, and yield the CPU
    (`_yield_cpu`)."""
    _yield_cpu()

    def watch():
        # ponytail: POSIX only, getppid never changes on Windows. There it needs a
        # WaitForSingleObject on the parent handle. Subprocesses of the worker are not reaped.
        while os.getppid() == parent:
            time.sleep(1.0)
        os._exit(1)

    threading.Thread(target=watch, name="msflow-parent-watch", daemon=True).start()
