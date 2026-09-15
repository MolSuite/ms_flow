# API guide

Use `MolSuite` as the public facade. A `MolSuite` instance owns the active
project and its executors, so close it with `shutdown()` when the work is done.

```python
from ms_flow.api import MolSuite
```

## Projects

Projects are isolated per `app_id`. Opening a project activates it; jobs run
against the active project.

```python
ms = MolSuite(app_id="demo")
try:
    context = ms.create_or_open_project(name="demo", folder="./demo-project")
    print(context.id)

    ms.close_project()          # the executors stay alive
    ms.open_project(context.id)
finally:
    ms.shutdown()               # stops the whole runtime
```

`close_project()` and `open_project()` fail explicitly if the active project
cannot drain its running jobs.

## Run a job

`run()` is the shortest path: `input → process → output`. It returns a job
identifier immediately; `wait_for_job()` blocks until the job ends and returns
a `JobSnapshot`.

```python
from workers import double_value

job_id = ms.run(
    name="double-values",
    input=[{"value": 1}, {"value": 2}, {"value": 3}],
    process=double_value,
    executor="thread",
    store_results=True,
)
final = ms.wait_for_job(job_id)
print(final.status, final.chunks_done)
print(ms.get_job_outputs(job_id))
```

!!! note "Workers must be importable"
    The `process` callable has to live in an importable module. Functions
    defined in the script you launch (`__main__`), lambdas, nested functions,
    and notebook cells are rejected, because process and remote executors must
    re-import the worker by name.

Each worker receives one payload dictionary and returns a dictionary.

## Persist results with sinks

By default outputs are not kept. Pass a sink to write results incrementally,
and keep `store_results=False` so the operational database does not grow with
every result.

```python
from ms_flow.api import file_sink, table_sink

job_id = ms.run(
    name="double-values",
    input=[{"value": value} for value in range(1000)],
    process=double_value,
    output=table_sink("results", columns=("value", "double_value")),
    flush_every=100,
    executor="thread",
)
```

| Sink | Destination |
| --- | --- |
| `table_sink(table, columns=...)` | A table in the project database; the table must exist |
| `file_sink(path, fmt="json")` | A file under the project folder |
| `graph_sink(nodes=..., relations=...)` | Several related tables written in one transaction |

## Read project data

`db_input_for()` describes a query instead of materialising rows. The chunk
carries the specification and the rows are read inside the worker.

```python
from ms_flow.query import db_input_for
from workers import summarize_molecules

job_id = ms.run(
    name="summarize-ready",
    input=[
        {
            "molecules": db_input_for(
                "molecules",
                fields=("id", "name"),
                filters={"status": "ready"},
                order=("id",),
            )
        }
    ],
    process=summarize_molecules,
    executor="thread",
    store_results=True,
)
```

## Reusable definitions

Separate definition from execution with `workflow()`:

```python
from ms_flow.api import workflow

spec = workflow(
    name="double-values",
    input=[{"value": 1}, {"value": 2}],
    process=double_value,
    executor="thread",
    store_results=True,
)
job_id = ms.run(spec=spec)
```

When a job is shared between scripts, a UI, and an application runtime, declare
it once with `streaming_job()` (one payload per chunk) or `batch_job()` (one
batch per chunk) and submit it with parameters:

```python
from ms_flow.api import streaming_job
from workers import double_value, value_chunks

double_job = streaming_job(
    name="double-values",
    run_chunk=double_value,
    chunker=value_chunks,
    executor="thread",
    store_results=True,
)
job_id = ms.submit_job(double_job, params={"count": 10})
```

## Follow progress

`watch_job()` renders a tqdm progress bar in a terminal or notebook and returns
the final snapshot. Pass `follow=False` for a non-blocking snapshot.

```python
from ms_flow import watch_job

snapshot = watch_job(ms, job_id)
```

Cancel a running job with `ms.cancel_job(job_id)`.

## Application runtimes

Subclass `BaseRuntime` to build a domain facade on top of `MolSuite`. The
runtime fixes the `app_id` once and exposes the engine as `self.molsuite`.

```python
from ms_flow.runtime import BaseRuntime


class DemoRuntime(BaseRuntime):
    def __init__(self):
        super().__init__("demoapp")

    def double(self, values):
        return self.run(
            name="double-values",
            input=[{"value": value} for value in values],
            process=double_value,
            executor="thread",
            store_results=True,
        )
```

## Choose the API level

| Need | Recommended API |
| --- | --- |
| Run `input → process → output` | `MolSuite.run()` |
| Reuse a common definition | `workflow()`, `streaming_job()`, or `batch_job()` |
| Describe project queries | `ms_flow.query` |
| Control tasks, staging, and finalize | `ms_flow.tasking` |
| Build a domain facade | `BaseRuntime` / `AppRuntime` |
| Extend the engine | `ms_flow.advanced` and, exceptionally, `ms_flow.core` |
