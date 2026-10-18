# Payloads

A payload is the bytes of one entity: an SDF, a PDBQT, a pose. Payloads live in
append-only volume files under `volumes/`, next to `project.db`. The `ms_payloads`
table says where each one is. Your tables keep only an id:

```sql
CREATE TABLE ligands (
    id INTEGER PRIMARY KEY,
    name TEXT,
    structure_payload_id INTEGER REFERENCES ms_payloads(id)
);
```

ms_flow never needs your schema. It finds the columns that hold payload ids by
their `REFERENCES ms_payloads(id)` foreign key. Moving bytes, for example when
compacting, rewrites `ms_payloads` and never your tables. A payload id never
changes.

## Write payloads from a job

A task returns `Payload` objects in place of ids. The sink stores the bytes and
writes the ids, in the same transaction as the rows:

```python
from ms_flow.engine import job, task
from ms_flow.payloads import Payload
from ms_flow.sinks import table_sink


@task()
def prepare(name: str) -> dict:
    sdf = build_sdf(name)  # your code
    return {"name": name, "structure_payload_id": Payload(sdf, kind="sdf", series="ligands")}


@job(task=prepare, output=table_sink("ligands", columns=("name", "structure_payload_id")))
def import_ligands(params, project):
    yield from params["names"]
```

`graph_sink` works the same way, in any node.

- `series` groups payloads with the same lifetime, so they share volumes and
  compact well together. Use one series per kind of data (`"ligands"`, `"poses"`).
- `codec` is `"zstd"` (the default), `"zlib"` or `"none"`. Use `"none"` for
  data that is already compressed.
- The payload is compressed when it is pickled, so the compression happens in
  the worker. The process that owns the project only appends bytes.

When the sink commits, the bytes are already on disk (fdatasync). A crash never
leaves a row that points to missing bytes.

## Compression dictionaries

A zstd dictionary trained on a series compresses small records much better:
5.9× against 4.3× without it on real SDF records. It is also faster. Train one
when a series has enough payloads (10 000 are sampled across the whole series):

```python
project.payloads.train_dictionary("ligands")
```

A job hands the dictionary to its tasks as a `File`. Executors without the
project's disk carry it once per job:

```python
@task()
def prepare(element: dict) -> dict:
    sdf = build_sdf(element["name"])
    return {"name": element["name"], "structure_payload_id":
            Payload(sdf, series="ligands", dictionary=element["dictionary"])}


@job(task=prepare, output=table_sink("ligands", columns=("name", "structure_payload_id")))
def import_ligands(params, project):
    dictionary = project.payloads.dictionary("ligands")  # None until one is trained
    return [{"name": name, "dictionary": dictionary} for name in params["names"]]
```

The dictionary id travels inside each zstd frame, so reading needs nothing
else. Payloads compressed with an older dictionary stay readable. Compaction
moves them to the series' current dictionary.

## Read payloads

```python
store = project.payloads
data = store.get(payload_id)            # bytes
found = store.get_many(ids)             # {id: bytes}, read in disk order
for payload_id, data in store.iter_bytes(ids):  # bounded memory, for exports
    ...
```

A job whose tasks need the bytes uses `with_payloads` as its source. Each row
gets the bytes of its payload under the key you name, so a task never sees a
path:

```python
from ms_flow.payloads import with_payloads
from ms_flow.query import QuerySpec, Rows


@job(task=dock, output=...)
def docking(params, project):
    rows = Rows(project, QuerySpec(table="ligands"))
    return with_payloads(project, rows, {"structure_payload_id": "structure"})
```

Rows are read 500 at a time in disk order, so memory stays bounded however big
the table is. A program that only reads paths gets a file in two ways:

- `store.materialize(payload_id, directory, suffix)` writes the payload once
  and returns it as a `File`. Use it for a receptor shared by every task.
- `scratch_file(data, suffix)` is a context manager. It gives a temporary file
  (in `/dev/shm` when it has room) and deletes it on leaving.

Corrupt or missing bytes raise `PayloadCorrupt`, which carries the payload id.

## Delete payloads

Delete your rows, then release the payload ids they held:

```python
with sqlite3.connect(project.db_path) as conn:
    ids = [r[0] for r in conn.execute(
        "DELETE FROM ligands WHERE name LIKE 'tmp%' RETURNING structure_payload_id")]
project.payloads.release(ids)
```

`release` only drops payloads that no foreign key references, so releasing an
id still in use does nothing. A volume left with no live payload is deleted at
once, without copying anything.

## Maintenance

| Call | What it does |
|---|---|
| `check(full=False)` | Returns a `Report`: missing volumes, references past a file's end, unreferenced payloads, empty volumes, dead share per volume, missing dictionaries, orphan files. `full=True` also reads and verifies every payload. `report.ok` means every referenced payload is readable; `report.clean` also means there is nothing to clean up. |
| `compact(min_dead=0.5, order_by=None)` | Copies the live payloads of sealed volumes that are at least half dead (or small) into new volumes, then deletes the old ones. Ids do not change. `order_by` is SQL that returns payload ids in the order you want them next to each other on disk (`SELECT structure_payload_id FROM ligands ORDER BY score`). |
| `repair()` | Clears what crashes leave behind: unreferenced payloads, orphan volume files, empty volumes. Never deletes a referenced payload. Returns a new `check()`. |

Compaction can run while other processes read. A reader whose volume
disappears reads the new location of the payload. To order compaction with
your other jobs, submit it as a job:

```python
from ms_flow.payloads import compaction

flow.submit(compaction())
```

The compaction job waits for jobs writing payloads to finish, and jobs reading
payloads wait for it.

On a 100 000-molecule project (225 MB stored), a cold `check(full=True)` takes
4.3 s. Compaction copies live data at about 34 MB/s, limited by the disk.

## Reference

::: ms_flow.payloads
    options:
      members:
        - Payload
        - PayloadStore
        - Report
        - PayloadCorrupt
        - with_payloads
        - compaction
        - scratch_file
