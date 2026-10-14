# ms_flow

`ms_flow` runs the jobs of a scientific app in the app's own process: no
broker, no daemon, no server. It works from a Qt app, a notebook or a script.

A **task** is what runs on one element. A **job** gives the elements, one by
one, and says where the results go. The engine shares the elements out among
the workers, keeps a journal of what is done, and continues a job that was cut
short without running again what it had done.

## Install

```bash
pip install ms_flow
pip install "ms_flow[ray]"   # to run on the machines of a network with Ray
```

The package requires Python 3.12 or newer. The desktop widgets (projects,
job monitor) are in the separate `ms_components` package.

## First use

Tasks and jobs live in a module the workers can import, never in `__main__`.

`workers.py`:

```python
from ms_flow import job, table_sink, task


@task()
def double_value(value: int) -> dict:
    return {"value": value, "double_value": 2 * value}


@job(task=double_value, output=table_sink("results", columns=("value", "double_value")))
def double_values(params, project):
    yield from range(1000)
```

`run.py`:

```python
import sqlite3

from ms_flow import Workspace
from workers import double_values

ms = Workspace(app_id="demo")
try:
    ms.create_or_open_project(name="demo", folder="./demo_project", activate=True)
    with sqlite3.connect(ms.project_db.db_path) as db:
        db.execute("CREATE TABLE IF NOT EXISTS results (value INTEGER, double_value INTEGER)")

    job_id = ms.submit(double_values)        # on the pool of processes of this machine
    print(ms.engine.wait(job_id).status)     # completed
finally:
    ms.shutdown()
```

## Where next

- The [guide](guide.md) explains tasks and jobs, results, following and
  continuing jobs, where jobs run, and the engine without a project.
- The [API reference](api_reference.md) lists the public names.

## License

`ms_flow` is beta software released under the MIT License.
