# ms_flow

In-process, desktop-first, domain-agnostic orchestrator for massive data flows:
jobs, executors and persistence in the same process as your app — no broker, no
daemon, no server. Works from a Qt app, a notebook or a plain script.

```python
import sqlite3

from ms_flow import Workspace, job, table_sink, task


@task()
def double_value(value: int) -> dict:
    return {"value": value, "double_value": 2 * value}


@job(task=double_value, output=table_sink("results", columns=("value", "double_value")))
def double_values(params, project):
    yield from range(1000)


ms = Workspace(app_id="demo")
ms.create_or_open_project(name="demo", folder="./demo_project", activate=True)
with sqlite3.connect(ms.project_db.db_path) as db:
    db.execute("CREATE TABLE IF NOT EXISTS results (value INTEGER, double_value INTEGER)")

print(ms.engine.wait(ms.submit(double_values)).status)
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
