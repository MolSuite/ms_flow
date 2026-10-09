"""Tasks and jobs as their author writes them, run on the engine.

    @task(cpu=1, retries=1)
    def dock(pair, params):                 # one element; `params` only if it names it
        return rows

    @job(task=dock, output=table_sink("docking_results"), reads=("ligands",))
    def docking(params, project):           # the source: gives the elements one by one
        yield from pending_pairs(project, params)

    Flow(engine, project).submit(docking, {"exhaustiveness": 8})
"""
import functools
import inspect
import threading


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


def job(*, task, name: str = "", params_model=None, output=None, reads=()):
    """Mark a function `(params, project)` as a job: the elements it gives go to `task`, the results to `output`.

    It is a generator, or returns whatever can be read one element at a time: `ms_flow.query.Rows`
    for the rows of a table, which also gives the job its total and the table it reads.
    `output` is a `table_sink` (the task returns rows) or a `graph_sink` (it returns
    `{node: rows}`; a `$ref` names its row within what the element returned). `reads` is the
    tables the source reads, besides the ones it says itself: the job waits for the jobs
    writing them.
    """
    if not hasattr(task, "__task__"):
        raise TypeError(f"{task!r} is not a task: decorate it with @task(...)")
    if output is not None and not hasattr(output, "table"):
        # ponytail: tables only. File sinks when a job needs one (none in AMDockVS does).
        raise TypeError("output is a table_sink or a graph_sink")

    def mark(source):
        source.__job__ = {"task": task, "name": name or source.__name__, "params_model": params_model,
                          "output": output, "reads": frozenset(reads)}
        return source
    return mark


def _tables(output) -> frozenset:
    if output is None:
        return frozenset()
    if output.mode == "graph":
        return frozenset(node["table"] for node in output.meta["graph"]["nodes"])
    return frozenset((output.table,))


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

        def write(pairs):
            data = _merged(output, [result for _, result in pairs if result])
            if data:
                self.project.persist_output_spec(output, data)

        def ended(snapshot):
            with self._lock:
                self._writing.pop(snapshot.id, None)
            if on_terminal is not None:
                on_terminal(snapshot)

        source = job(params, self.project)
        reads = spec["reads"] | frozenset(getattr(source, "reads", ()))
        with self._lock:  # so `ended` cannot run before the job is in `_writing`
            after = [*after, *(job_id for job_id, tables in self._writing.items() if tables & reads)]
            job_id = self.engine.submit(executor, fn, source, name=spec["name"], after=after,
                                        on_results=write if output is not None else None, on_terminal=ended,
                                        **needs, **engine)
            self._writing[job_id] = _tables(output)
        return job_id
