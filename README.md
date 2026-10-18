# ms_flow

In-process, desktop-first, domain-agnostic orchestrator for massive data flows:
jobs, executors and persistence in the same process as your app — no broker, no
daemon, no server. Works from a Qt app, a notebook or a plain script.

`workers.py` (tasks and jobs live in a module the workers can import):

```python
from sqlmodel import Field, SQLModel

from ms_flow import job, table_sink, task


class Result(SQLModel, table=True):      # the project creates this table when it opens
    __tablename__ = "results"
    id: int | None = Field(default=None, primary_key=True)
    value: int
    double_value: int


@task()
def double_value(value: int) -> dict:
    return {"value": value, "double_value": 2 * value}


@job(task=double_value, output=table_sink("results", columns=("value", "double_value")))
def double_values(params, project):
    yield from range(1000)
```

`run.py`:

```python
from ms_flow import Workspace
from workers import Result, double_values

ms = Workspace(app_id="demo")
try:
    ms.create_or_open_project(name="demo", folder="./demo_project", activate=True)
    print(ms.engine.wait(ms.submit(double_values)).status)   # completed
    print(ms.project_db.count(Result))                       # 1000
finally:
    ms.shutdown()
```

## Install

```bash
pip install ms_flow            # engine + projects: SQLite and loky
```

## Documentation

<https://molsuite.github.io/ms_flow/>: a guide to tasks, jobs, sinks, following and
continuing jobs and where they run, and the API reference.

## Status

Beta. The public API (`ms_flow`, `ms_flow.api`, `ms_flow.engine`) is the stable surface; `ms_flow.core.*` may change without notice.

## License

MIT — see [LICENSE](LICENSE).
