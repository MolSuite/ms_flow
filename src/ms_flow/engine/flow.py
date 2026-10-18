"""Tasks and jobs as their author writes them, run on the engine.

    @task(cpu=1, retries=1)
    def dock(pair, params):                 # one element; `params` only if it names it
        return rows

    @job(task=dock, output=table_sink("docking_results"), reads=("ligands",))
    def docking(params, project):           # the source: gives the elements one by one
        yield from pending_pairs(project, params)

    Flow(engine, project).submit(docking, {"exhaustiveness": 8})

A job may write its results itself: `output` is then a function `(params, project)` that
returns its writer.

    class Shards:
        tables = ("screening_shards",)      # optional: the jobs reading them wait for this one

        def write(self, results): ...       # what a group of elements returned; nothing left behind if it raises
        def finish(self): ...               # optional: once every element has ended; if it raises, the job fails
"""
import functools
import importlib
import inspect
import json
import logging
import threading
import uuid
from contextlib import ExitStack, contextmanager
from pathlib import Path

log = logging.getLogger(__name__)


def task(*, cpu: float = 1, gpu: float = 0, mem_mb: float = 0, retries: int = 0, batch: int | None = None,
         batched: bool = False, label=None, thread: bool = False):
    """Mark a function as a task: what runs on one element, and what it needs to run.

    `batch` is how many elements the author wants to travel together; `batched` says the
    function takes the list of them and returns one result for each.
    `label` is what an element goes by in the monitor and in a failure: a template its keys
    fill (`"dock-R{receptor}-L{ligand}"`) or a function `(element)` that returns the text.
    `thread` is for a function that only waits for a program it starts (a docking binary): on
    this machine it runs on a thread of the engine's process and no worker is kept for it. It
    tells of the program with `launched`, which is how it is stopped.
    """
    def mark(fn):
        fn.__task__ = {"cpu": cpu, "gpu": gpu, "mem_mb": mem_mb, "retries": retries, "batch": batch,
                       "batched": batched, "label": label, "thread": thread}
        return fn
    return mark


_scope = threading.local()  # `stack`: the clean-up of the chain running on this thread


@contextmanager
def cleanup():
    """What a task leaves to tidy up, as an `ExitStack`:

        with cleanup() as later:
            later.callback(os.unlink, path)

    Alone, it is done when the task's `with` ends; within a `chain`, once the last step has ended,
    so what one step leaves on disk is still there for the next.
    """
    stack = getattr(_scope, "stack", None)
    if stack is not None:
        yield stack
        return
    with ExitStack() as stack:
        yield stack


class _Chain:
    """Tasks run one after the other on each element, as one task (see `chain`)."""

    def __init__(self, steps, needs, on_error):
        self.steps, self.on_error = steps, on_error
        self.names = [step.__name__ for step in steps]
        self.needs = {name: frozenset(needs.get(name, self.names[:place])) for place, name in enumerate(self.names)}
        self.__qualname__ = self.__name__ = "+".join(self.names)
        self.__module__ = steps[0].__module__

    def __call__(self, element, params=None):
        merged, failed, first = {}, set(), None
        outer = getattr(_scope, "stack", None)
        with ExitStack() as own:
            _scope.stack = outer or own
            try:
                for name, step in zip(self.names, self.steps):
                    if self.needs[name] & failed:
                        failed.add(name)  # what it would build on is not there
                        continue
                    wants = inspect.signature(step).parameters
                    given = {key: value for key, value in (("params", params), ("previous", merged)) if key in wants}
                    try:
                        result = step(element, **given)
                    except Exception as error:  # noqa: BLE001 - one step failing is not the next failing
                        failed.add(name)
                        first = first or error
                        log.warning("chain %s: step %s failed", self.__name__, name, exc_info=True)
                        result = self.on_error(name, element, error) if self.on_error is not None else None
                    for node, rows in (result if isinstance(result, dict) else {name: result or ()}).items():
                        merged.setdefault(node, []).extend([rows] if isinstance(rows, dict) else rows or ())
            finally:
                _scope.stack = outer
        if len(failed) == len(self.steps):
            raise first
        return merged


def chain(*steps, needs=None, on_error=None, thread=None):
    """Tasks that run one after the other on each element, as one task whose result is for a `graph_sink`.

    A step is a task. It gets the element, and `previous` (what the steps before it returned,
    `{node: rows}`) and `params` if it names them; it returns `{node: rows}`, or rows for a node
    named after it. `needs` maps a step's name to the steps it builds on, by default every step
    before it: a step whose needs failed is skipped, the others still run and what they returned
    is written. The element fails only when every step did. `on_error(step, element, error)` may
    return `{node: rows}` for the step that failed (a failures table); otherwise it is only logged.

    The chain needs what its hungriest step needs and keeps the first step's retries, batch and
    label. It runs on a thread only if every step does (`thread` overrides that).
    ponytail: each step reports its own progress, so the element's bar restarts at each step.
    """
    if not steps or not all(hasattr(step, "__task__") for step in steps):
        raise TypeError("chain takes tasks: decorate each step with @task(...)")
    if any(step.__task__["batched"] for step in steps):
        raise TypeError("a batched task cannot be a step of a chain")
    run = _Chain(steps, dict(needs or {}), on_error)
    first = steps[0].__task__
    run.__task__ = {**first, **{need: max(step.__task__[need] for step in steps) for need in ("cpu", "gpu", "mem_mb")},
                    "thread": all(step.__task__.get("thread", False) for step in steps) if thread is None else thread}
    return run


def job(*, task, name: str = "", params_model=None, output=None, reads=(), finalize=None):
    """Mark a function `(params, project)` as a job: the elements it gives go to `task`, the results to `output`.

    It is a generator, or returns whatever can be read one element at a time: `ms_flow.query.Rows`
    for the rows of a table, which also gives the job its total and the table it reads.
    `output` is a `table_sink` (the task returns rows) or a `graph_sink` (it returns
    `{node: rows}`; a `$ref` names its row within what the element returned), a `file_sink`
    (it returns text or bytes, written as they come, or anything else, written as JSON) or a
    function `(params, project)` that returns the job's own writer (see the module's note).
    `reads` is the tables the source reads, besides the ones it says itself: the job waits for
    the jobs writing them.
    `finalize(params, project, snapshot)` is called once the job has ended, however it ended:
    what there is to tidy up when it failed or was canceled.
    """
    if not hasattr(task, "__task__"):
        raise TypeError(f"{task!r} is not a task: decorate it with @task(...)")
    if output is not None and not (hasattr(output, "table") or hasattr(output, "fmt") or callable(output)):
        raise TypeError("output is a table_sink, a graph_sink, a file_sink or a function that returns a writer")

    def mark(source):
        source.__job__ = {"task": task, "name": name or source.__name__, "params_model": params_model,
                          "output": output, "reads": frozenset(reads), "finalize": finalize}
        return source
    return mark


class _Table:
    """A `table_sink` or a `graph_sink` as a writer: one transaction for each group of results."""

    def __init__(self, output, project):
        self.output, self.project = output, project
        graph = output.meta["graph"]["nodes"] if output.mode == "graph" else ()
        # and ms_payloads, where the Payloads of its rows go: a compaction waits for it
        self.tables = [node["table"] for node in graph] or [output.table]
        self.tables.append("ms_payloads")

    def write(self, results: list) -> None:
        data = _merged(self.output, results)
        if data:
            self.project.persist_output_spec(self.output, data)


class _File:
    """A `file_sink` as a writer: one file that grows (`append`), a numbered file for each group
    of results (`per_batch`) or, with neither, one file written when the job completes."""

    def __init__(self, spec, project):
        path = Path(spec.path).expanduser()
        self.spec, self.kept = spec, []
        self.path = path if path.is_absolute() else Path(project.project_dir or project.db_path.parent) / path
        if spec.ensure_parent:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.files = len(list(self.path.parent.glob(f"{self.path.stem}_*{self.path.suffix}")))

    def _bytes(self, results: list) -> bytes:
        encoding = self.spec.encoding
        if self.spec.fmt == "json":
            return json.dumps(results, ensure_ascii=False).encode(encoding)
        return b"".join(result if isinstance(result, bytes) else result.encode(encoding) if isinstance(result, str)
                        else (json.dumps(result, ensure_ascii=False) + "\n").encode(encoding) for result in results)

    def write(self, results: list) -> None:
        # ponytail: a write cut short by a full disk leaves its half in the file.
        if self.spec.append:
            with open(self.path, "ab") as file:
                file.write(self._bytes(results))
        elif self.spec.per_batch:
            self.files += 1  # after the ones a job that continues finds there
            name = f"{self.path.stem}_{self.files:05d}{self.path.suffix}"
            self.path.with_name(name).write_bytes(self._bytes(results))
        else:
            # ponytail: in memory until the end, and a job that continues has only what this run
            # gave. For results that fit; `append` or `per_batch` otherwise.
            self.kept.extend(results)

    def finish(self) -> None:
        if not (self.spec.append or self.spec.per_batch):
            self.path.write_bytes(self._bytes(self.kept))


def writer(output, project):
    """A `table_sink`, a `graph_sink` or a `file_sink` as the writer the engine makes of it: for
    a job's own writer that changes what its tasks returned and leaves the writing to a sink."""
    return _Table(output, project) if hasattr(output, "table") else _File(output, project)


def _writer(output, params, project):
    return writer(output, project) if hasattr(output, "table") or hasattr(output, "fmt") else output(params, project)


def _merged(output, results: list):
    """What several elements returned, as one payload for the sink."""
    if output.mode != "graph":
        return [row for result in results for row in ([result] if isinstance(result, dict) else result)]
    graph = output.meta["graph"]
    refs = {node["name"]: [node["ref_field"]] for node in graph["nodes"]}
    for relation in graph["relations"]:
        refs[relation["source_node"]].append(relation["source_ref_field"])
    # A ref names a row within what one element returned, and the store keeps the last row it
    # sees under a ref: written together, each ref gets its element's place in the group in front.
    # ponytail: not when a ref field is a column too (or the columns are unknown until the model
    # is read). It is data then, and has to be unique in the job by itself.
    apart = len(results) > 1 and all(node["columns"] and not set(refs[node["name"]]) & set(node["columns"])
                                     for node in graph["nodes"])
    merged: dict[str, list] = {}
    for place, result in enumerate(results):
        for node, rows in result.items():
            rows = [rows] if isinstance(rows, dict) else rows or ()
            if apart:
                for row in rows:
                    for field in refs.get(node, ()):
                        if row.get(field) is not None:
                            row[field] = f"{place}:{row[field]}"
            merged.setdefault(node, []).extend(rows)
    return merged


def named(where: str):
    """What `module:qualname` names."""
    module, _, name = where.partition(":")
    return functools.reduce(getattr, name.split("."), importlib.import_module(module))


def _origin(job, params, options: dict):
    """What it takes to submit `job` again, or None when it cannot be written down."""
    where = f"{job.__module__}:{job.__qualname__}"
    try:
        if named(where) is not job:
            return None
        params = params.model_dump(mode="json") if hasattr(params, "model_dump") else params
        kept = {name: value for name, value in options.items() if isinstance(value, (bool, int, float, str))}
        return json.loads(json.dumps({"job": where, "params": params, "options": kept}))
    except Exception:
        return None


class Flow:
    """Submits jobs for one project: `project` is what a source receives and where the sinks write."""

    def __init__(self, engine, project):
        self.engine, self.project = engine, project
        self._writing: dict[int, frozenset] = {}  # the tables each job that has not ended writes
        self._lock = threading.Lock()

    def submit(self, job, params=None, *, executor: str = "process", batch: int | None = None,
               cpu: float | None = None, thread: bool | None = None, after=(), on_terminal=None, **engine) -> int:
        """Start `job` and return its id. `batch`, `cpu` and `thread` override the task's; `after`
        adds jobs to wait for.

        What else `Engine.submit` takes (`key`, `journal`, `priority`, `total`...) passes through.
        The sink is written on the engine's delivery thread, one transaction for each group of
        results, and an element is done once its rows are in.
        A job that goes to a journal always has a key, its own or one made here, and is recorded
        with what it was submitted with: see `resume`.
        """
        spec = job.__job__
        fn, output = spec["task"], spec["output"]
        # How many elements to a task: whoever submits, then the executor (a job in a queue takes
        # what its settings say), then the author of the task.
        own = getattr(self.engine.executor(executor), "batch", None)
        needs = {**fn.__task__, "batch": batch or own or fn.__task__["batch"], "cpu": cpu or fn.__task__["cpu"],
                 "thread": fn.__task__.get("thread", False) if thread is None else thread}
        if spec["params_model"] is not None:
            params = spec["params_model"].model_validate(params or {})
        if engine.get("journal") is not None:
            engine["key"] = engine.get("key") or uuid.uuid4().hex
            engine["origin"] = _origin(job, params, {"executor": executor, "batch": batch, "cpu": cpu, "thread": thread, **{
                name: value for name, value in engine.items() if name not in ("journal", "key")}})
        if "params" in inspect.signature(fn).parameters:
            fn = functools.partial(fn, params=params)  # pickled with the function: once for each batch

        writer = None if output is None else _writer(output, params, self.project)

        def write(pairs):
            results = [result for _, result in pairs if result]
            if results:
                writer.write(results)

        def ended(snapshot):
            with self._lock:
                self._writing.pop(snapshot.id, None)
            try:
                if spec["finalize"] is not None:
                    spec["finalize"](params, self.project, snapshot)
            finally:
                if on_terminal is not None:
                    on_terminal(snapshot)

        source = job(params, self.project)
        reads = spec["reads"] | frozenset(getattr(source, "reads", ()))
        with self._lock:  # so `ended` cannot run before the job is in `_writing`
            after = [*after, *(job_id for job_id, tables in self._writing.items() if tables & reads)]
            job_id = self.engine.submit(executor, fn, source, name=spec["name"], after=after,
                                        on_results=write if writer is not None else None,
                                        on_finish=getattr(writer, "finish", None), on_terminal=ended,
                                        **needs, **engine)
            self._writing[job_id] = frozenset(getattr(writer, "tables", ()))
        return job_id

    def origin(self, key: str, journal: str) -> dict:
        """What the job recorded as `key` in `journal` was submitted with: `job` (as `named` reads
        it), `params` and `options`."""
        spec = next((row["spec"] for row in self.engine.recorded(journal) if row["job_id"] == key), None)
        origin = json.loads(spec or "{}").get("origin")
        if origin is None:
            raise ValueError(f"job {key!r} is not on record with what it takes to submit it again: "
                             "its function has to be importable and its params JSON")
        return origin

    def resume(self, key: str, *, journal: str, **options) -> int:
        """Submit again the job recorded as `key` in `journal`, with what it was submitted with:
        one that was interrupted or canceled continues. `options` say otherwise for this run,
        and bring what a record cannot keep (`on_terminal`, `after`)."""
        origin = self.origin(key, journal)
        return self.submit(named(origin["job"]), origin["params"], key=key, journal=journal,
                           **{**origin["options"], **options})
