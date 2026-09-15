# ms_flow

## What it is

`ms_flow` is the MolSuite orchestrator for scientific desktop applications,
scripts and notebooks. It runs inside your own Python process: no broker,
daemon or separate server is required.

## What it does

- Runs `input → process → output` jobs on threads, processes or distributed
  backends (Ray).
- Splits the input into chunks, dispatches them to executors and records the
  state of every job, so progress can be observed while it runs.
- Persists projects and results in SQLite, isolated per `app_id`.
- Keeps domain and infrastructure apart: workers are plain Python functions
  that do not need to know where the data comes from or where it goes.

## Installation

Requires Python 3.12 or newer.

```bash
pip install ms_flow
```

For distributed execution with Ray:

```bash
pip install "ms_flow[ray]"
```

Desktop widgets (project browser, job monitor) live in the separate
`ms_components` package.

## Example

The worker function must live in an importable module, so process and remote
executors can rebuild it.

`workers.py`:

```python
def double_value(payload: dict) -> dict:
    value = int(payload["value"])
    return {"value": value, "double_value": value * 2}
```

`run.py`:

```python
from ms_flow.api import MolSuite
from workers import double_value

ms = MolSuite(app_id="demo")
try:
    ms.create_or_open_project(
        name="demo",
        folder="./demo-project",
        activate=True,
    )
    job_id = ms.run(
        name="double-values",
        input=[{"value": 1}, {"value": 2}, {"value": 3}],
        process=double_value,
        executor="thread",
    )
    final = ms.wait_for_job(job_id)
    print(final.status)  # completed
finally:
    ms.shutdown()
```

```bash
python run.py
```

Continue with the [API guide](api_guide.md), copy a complete
[example](examples.md), or browse the generated [API reference](api_reference.md).

## License

`ms_flow` is beta software released under the MIT License.
