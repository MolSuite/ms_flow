# ms_flow

In-process, desktop-first, domain-agnostic orchestrator for massive data flows:
jobs, executors and persistence in the same process as your app — no broker, no
daemon, no server. Works from a Qt app, a notebook or a plain script.

```python
from ms_flow.api import MolSuite, table_sink
from workers import double_value  # el handler vive en un modulo importable

ms = MolSuite(app_id="demo")
ms.create_or_open_project(name="demo", folder="./demo_project", activate=True)

job_id = ms.run(
    name="double_values",
    input=[{"value": v} for v in range(1000)],
    process=double_value,
    output=table_sink("results", columns=("value", "double_value")),
    executor="thread",
)
print(ms.wait_for_job(job_id).status)
```

## Install

```bash
pip install ms_flow            # core: control plane + SQLite + loky
pip install "ms_flow[ray]"     # executor Ray
pip install "ms_flow[ui]"      # PySide6 components
```

## Where to look

- [5-minute quickstart](docs/guides/quickstart_5min.md) — first working job.
- [Runnable examples](examples/README.md) — scripts y notebook.
- [Public API](docs/guides/public_api.md) — API.
- [Docs index](docs/README.md) — guides, contracts, operation
- [Benchmarks](benchmarks/README.md) — performance.

## Status

Beta. The public API (`ms_flow.api`) is the stable surface; `ms_flow.core.*` may change without notice.

## License

MIT — see [LICENSE](LICENSE).
