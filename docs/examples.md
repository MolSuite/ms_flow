# Examples

Every example imports its worker functions from `workers.py`, because workers
must be importable. Save this file next to the scripts:

```python
# workers.py
def double_value(payload: dict) -> dict:
    value = int(payload["value"])
    return {"value": value, "double_value": value * 2}


def summarize_molecules(payload: dict) -> dict:
    molecules = list(payload["molecules"])
    return {
        "molecule_count": len(molecules),
        "molecule_names": [row["name"] for row in molecules],
    }


def value_chunks(params: dict):
    for value in range(int(params["count"])):
        yield {"value": value}
```

Each example creates its project in the current directory, so run them in a
scratch folder.

## Persist results to a table

```python
import sqlite3
from pathlib import Path

from ms_flow.api import MolSuite, table_sink
from workers import double_value

ms = MolSuite(app_id="demo")
try:
    ms.create_or_open_project(
        name="table-demo",
        folder=Path("./table-demo").resolve(),
        activate=True,
    )

    # A table sink writes to the project database: the table has to exist.
    with sqlite3.connect(str(ms.project_db.db_path)) as conn:
        conn.execute("DROP TABLE IF EXISTS results")
        conn.execute("CREATE TABLE results (value INTEGER, double_value INTEGER)")

    job_id = ms.run(
        name="double-values",
        input=[{"value": value} for value in (1, 2, 3, 4)],
        process=double_value,
        output=table_sink("results", columns=("value", "double_value")),
        flush_every=2,
        executor="thread",
    )
    final = ms.wait_for_job(job_id, poll_s=0.05)
    print(final.status, final.chunks_done)

    with sqlite3.connect(str(ms.project_db.db_path)) as conn:
        rows = conn.execute("SELECT value, double_value FROM results ORDER BY value")
        print(rows.fetchall())
finally:
    ms.shutdown()
```

## Feed a job from project rows

The chunk carries the query, not the rows: they are read inside the worker.

```python
import sqlite3
from pathlib import Path

from ms_flow.api import MolSuite
from ms_flow.query import db_input_for
from workers import summarize_molecules

ms = MolSuite(app_id="demo")
try:
    ms.create_or_open_project(
        name="query-demo",
        folder=Path("./query-demo").resolve(),
        activate=True,
    )

    with sqlite3.connect(str(ms.project_db.db_path)) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS molecules "
            "(id INTEGER PRIMARY KEY, name TEXT, status TEXT)"
        )
        conn.execute("DELETE FROM molecules")
        conn.executemany(
            "INSERT INTO molecules (id, name, status) VALUES (?, ?, ?)",
            [(1, "MolA", "ready"), (2, "MolB", "ready"), (3, "MolC", "archived")],
        )

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
    final = ms.wait_for_job(job_id, poll_s=0.05)
    print(final.status, ms.get_job_outputs(job_id))
finally:
    ms.shutdown()
```

## Reusable streaming job

Declare the job once and submit it with different parameters.

```python
from pathlib import Path

from ms_flow import watch_job
from ms_flow.api import MolSuite, streaming_job
from workers import double_value, value_chunks

double_job = streaming_job(
    name="double-values",
    run_chunk=double_value,
    chunker=value_chunks,
    executor="thread",
    store_results=True,
)

ms = MolSuite(app_id="demo")
try:
    ms.create_or_open_project(
        name="streaming-demo",
        folder=Path("./streaming-demo").resolve(),
        activate=True,
    )
    job_id = ms.submit_job(double_job, params={"count": 5})
    final = watch_job(ms, job_id)
    print(final.status, len(ms.get_job_outputs(job_id)))
finally:
    ms.shutdown()
```

## Application runtime

A domain facade over `MolSuite` for an application.

```python
from pathlib import Path

from ms_flow.runtime import BaseRuntime
from workers import double_value


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


runtime = DemoRuntime()
try:
    runtime.create_or_open_project(
        name="runtime-demo",
        folder=Path("./runtime-demo").resolve(),
    )
    job_id = runtime.double([1, 2, 3])
    final = runtime.molsuite.wait_for_job(job_id, poll_s=0.05)
    print(final.status, runtime.molsuite.get_job_outputs(job_id))
finally:
    runtime.shutdown()
```
