# Examples

They run from the repo with nothing installed (they put `src/` in `sys.path`):

```bash
python examples/run_to_table.py
python examples/query_project_rows.py
```

Each creates its project in the directory you launch it from, so run them in a
temporary folder.

| File | What it shows |
|---|---|
| `run_to_table.py` | `ms.submit(...)` of a job over a list, results in a table through `table_sink(...)` |
| `query_project_rows.py` | a job fed by the rows of a project table (`Rows`), read in pages by the job |
| `workers.py` | the tables, tasks and jobs of both |

## Why `workers.py` exists

A worker process imports the task again by its `module:qualname`. A function
defined in the script you launch (`python my_script.py`) lives in `__main__` and
is rejected; so is a lambda, a nested function or a notebook cell. That is why
both examples import from `workers.py`. The tables are SQLModel models there
too: the project creates them when it opens, as long as the module was
imported before.
