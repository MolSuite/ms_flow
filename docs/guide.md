# Guide

## A task and a job

A task is a function marked with `@task`. It takes one element and returns its
result; it takes `params` too when it names it.

```python
from ms_flow import task


@task(cpu=1, retries=1, label="mol-{id}")
def weigh(molecule: dict, params) -> dict:
    return {"molecule_id": molecule["id"], "atoms": len(molecule["smiles"]) * params["scale"]}
```

| Argument | What it is |
|---|---|
| `cpu=`, `gpu=`, `mem_mb=` | What one element needs; the engine admits no more than the machine has |
| `retries=` | How many times a failed element runs again |
| `label=` | What an element goes by in the monitor and in a failure: a template its keys fill, or a function |
| `batch=`, `batched=` | How many elements travel together; with `batched` the function takes the list and returns one result for each |
| `thread=` | The function only waits for a program it starts: it runs on a thread and no worker is kept for it |

A job is a function `(params, project)` marked with `@job`. It gives the
elements, and names the task that takes them and where the results go.

```python
from ms_flow import job, table_sink
from ms_flow.query import QuerySpec, Rows


@job(task=weigh, output=table_sink("weights", columns=("molecule_id", "atoms")))
def weigh_all(params, project):
    return Rows(project, QuerySpec(table="molecules", fields=("id", "smiles")))
```

The source is a generator, or anything read one element at a time. `Rows`
reads a table of the project in pages, gives the job its total, and stops at
the rows the table had when the job started. `params` is what `submit` was
given, `None` when nothing was; `params_model=` validates it with a pydantic
model.

Tasks and jobs live in a module the workers can import, never in `__main__`.

## Running a job

A `Workspace` keeps the projects of an app and runs jobs in the active one.

```python
from ms_flow import Workspace

ms = Workspace(app_id="demo")
ms.create_or_open_project(name="demo", folder="./demo_project", activate=True)

job_id = ms.submit(weigh_all, {"scale": 2})
snapshot = ms.engine.wait(job_id)
print(snapshot.status, snapshot.chunks_done, snapshot.chunks_total)   # completed 200 200
ms.shutdown()
```

- `submit` returns at once. `ms.engine.status(job_id)` and
  `ms.engine.wait(job_id)` return a `JobSnapshot`.
- `executor=` says where the job runs: `local` (the pool of processes of this
  machine, the default), `thread`, or another active executor.
- `batch=`, `cpu=` and `thread=` override the task's; `after=` names jobs to
  wait for. A job also waits for the jobs that write the tables it reads.
- `ms.engine.cancel(job_id)` stops a job.

## Where the results go

| Sink | The task returns | Written |
|---|---|---|
| `table_sink(table, columns=...)` | A row (dict), or a list of them | Into a table of the project |
| `graph_sink(nodes=..., relations=...)` | `{node: rows}` | Into several tables, the keys of one row filled into the others |
| `file_sink(path, fmt=...)` | Text, bytes, or anything as JSON | Into a file under the project: one at the end, one that grows (`append=True`) or one per group (`per_batch=True`) |

```python
from ms_flow import file_sink


@task()
def describe(value: int) -> str:
    return f"{value}\t{value * value}\n"


@job(task=describe, output=file_sink("squares.tsv", fmt="text", append=True))
def squares(params, project):
    yield from range(10)
```

Each group of results is written in one transaction, on the engine's
delivery thread, and an element counts as done once its results are in. The
tables must exist: the app creates them.

A job may write its results itself: `output` is then a function
`(params, project)` that returns an object with `write(results)` and,
optionally, `finish()` and `tables`. `writer(sink, project)` gives the writer
of a sink, for one that changes the results before a sink writes them.

## Following a job

| Call | What it gives |
|---|---|
| `ms.engine.status(job_id)` | `JobSnapshot`: `status`, `chunks_done`, `chunks_failed`, `chunks_total`, `progress`, `tasks_running`, `cpu_used`, `error`, `key`... |
| `ms.jobs()` | Every job of the active project, earlier sessions too, read from its journal; safe to poll |
| `ms.job_tasks(job_id)` | The tasks in flight of a job of this session |
| `ms.job_failures(key)` | The elements that failed: `attempt`, `error` (the traceback) and `input` |

A job ends `completed`, `completed_with_errors` (some elements failed),
`failed`, `canceled` or `interrupted`. A task tells how far it is on its
element with `report(fraction)`; the snapshot's `progress` counts it.

## Continuing a job

Every job of a project is recorded in its journal (`executor.db`) under a
key: the one given with `key=`, or one made for it.

- Closing the project, or the app, interrupts its jobs.
- `ms.resume(key)` submits the job again with what it was submitted with.
- A job that was interrupted or canceled continues: the elements it had done
  are recognised by their own keys and not run again.

```python
job_id = ms.submit(weigh_all, {"scale": 1}, key="weigh")
...
ms.engine.cancel(job_id)          # canceled with 68 of 200 done
ms.engine.wait(ms.resume("weigh"))  # completed: the other 132 ran
```

An element is recognised by `content_key(element)`, or by the `_key` of a dict
element. Results written by an element that was running when the job stopped
may be written again when it continues: write them so that a second write
changes nothing (a conflict key, or skipping what is there).

`ms.engine.discard("project", key)` gives a recorded job up.

## Where jobs run

Machines are kept as **hosts**, and who runs jobs on them as **executors**,
by name, in the settings of `~/.molsuite` (`$MOLSUITE_HOME` moves it).

| Executor `kind` | What it is |
|---|---|
| `loky` | The pool of processes of this machine: `local` |
| `ray` | A Ray cluster started from this machine over its hosts; without `local` among them this machine runs none of its tasks |
| `queue` | The queue of an HPC (`slurm`), its host the login node |

```python
ms.save_host("lab", address="me@lab-server", cpus=32)      # as `ssh` knows it
ms.save_executor("lab-ray", kind="ray", hosts=["local", "lab"])
ms.activate("lab-ray")                     # starts it; jobs go to it by name
ms.submit(weigh_all, {"scale": 2}, executor="lab-ray")
ms.move(key, "local")                      # a job that runs continues elsewhere
```

- Commands go over the system's `ssh`, through `ms_env`: keys and users come
  from `~/.ssh/config`, no credential is handled.
- Give each host an address that does not change: a name (`ssh` resolves it
  from `~/.ssh/config`, DNS or mDNS, such as `lab-server.local`) or a fixed IP
  (a DHCP reservation in the router). An address the network gives anew each
  time makes the host unreachable the next time its machine joins.
- A Ray cluster takes only the machines on the network of the first host:
  each one joins the address of this machine as that host sees it.
- A host with no `env` is given the environment of the app by the app
  (`Workspace.host_env`).
- `ms.executors()` lists them with their state; `deactivate` stops one.

## The engine without a project

`ms_flow.engine.Engine` runs a function on every element of an iterable, with
no project and no journal.

```python
from ms_flow.engine import Engine, ProcessExecutor
from workers import square_of

with Engine(cpus=4) as engine:
    engine.register("process", ProcessExecutor(4))
    results = {}
    job_id = engine.submit("process", square_of, range(100),
                           on_result=lambda key, result: results.update({key: result}))
    engine.wait(job_id)
```

`engine.attach(name, path)` gives it a journal, and `journal=` and `key=` in
`submit` record a job there. `Flow(engine, project)` runs `@job`s with it.
