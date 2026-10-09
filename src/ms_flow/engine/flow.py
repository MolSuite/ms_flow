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
import inspect
import json
import threading
from pathlib import Path


def task(*, cpu: float = 1, gpu: float = 0, mem_mb: float = 0, retries: int = 0, batch: int | None = None,
         batched: bool = False):
    """Mark a function as a task: what runs on one element, and what it needs to run.

    `batch` is how many elements the author wants to travel together; `batched` says the
    function takes the list of them and returns one result for each.
    """
    def mark(fn):
        fn.__task__ = {"cpu": cpu, "gpu": gpu, "mem_mb": mem_mb, "retries": retries, "batch": batch,
                       "batched": batched}
        return fn
    return mark


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
        self.tables = [node["table"] for node in graph] or [output.table]

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


def _writer(output, params, project):
    if hasattr(output, "table"):
        return _Table(output, project)
    return _File(output, project) if hasattr(output, "fmt") else output(params, project)


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


class Flow:
    """Submits jobs for one project: `project` is what a source receives and where the sinks write."""

    def __init__(self, engine, project):
        self.engine, self.project = engine, project
        self._writing: dict[int, frozenset] = {}  # the tables each job that has not ended writes
        self._lock = threading.Lock()

    def submit(self, job, params=None, *, executor: str = "process", batch: int | None = None, after=(),
               on_terminal=None, **engine) -> int:
        """Start `job` and return its id. `batch` overrides the task's; `after` adds jobs to wait for.

        What else `Engine.submit` takes (`key`, `journal`, `priority`, `total`...) passes through.
        The sink is written on the engine's delivery thread, one transaction for each group of
        results, and an element is done once its rows are in.
        """
        spec = job.__job__
        fn, output = spec["task"], spec["output"]
        needs = {**fn.__task__, "batch": batch or fn.__task__["batch"]}
        if spec["params_model"] is not None:
            params = spec["params_model"].model_validate(params or {})
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
