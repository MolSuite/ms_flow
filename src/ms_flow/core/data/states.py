"""What is already done of a sharded dataset, per shard, without a row per shard.

A shard is the unit of work and of state: its records processed by a step or not. Which shards are
done is a sidecar Parquet of their ids — one file per ``(kind, key)``, grown in place — and not
a table, so a dataset of 100M records costs a few hundred files of a few MB instead of 400k
rows. The database keeps one upserted row per key with the fraction (`progress()`), which is
what a UI shows; the ids are read only when something needs the difference.

Per shard, never per record: a record that fails is isolated inside its shard (one bad record
must not kill a task that handles hundreds), and a shard that fails is simply repeated whole.

Comparing two hosts is then comparing two of these files: what the remote has done, what
the local has done, and (with the shard rows) what exists at all. The sidecar replaces the
per-file sha256 of the days when every shard travelled as its own file.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Iterable, Mapping

KIND = "shard_state"
FOLDER = "states"
COMPLETE, PARTIAL, INACTIVE = "complete", "partial", "inactive"


def _root(project_db) -> Path:
    from ms_flow.core.database import resolve_project_store

    db_path = resolve_project_store(project_db).db_path
    if db_path is None:
        raise RuntimeError("shard states need a project on disk")
    return Path(db_path).parent


def _registry(project_db):
    from ms_flow.artifacts import ArtifactRegistry

    return ArtifactRegistry(project_db)


def _file(project_db, kind: str, key: str) -> Path:
    """One file per ``(kind, key)``: the key readable in the name, a digest against collisions."""
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", f"{kind}-{key}")[:80]
    digest = hashlib.sha1(f"{kind}/{key}".encode("utf-8")).hexdigest()[:8]
    return _root(project_db) / FOLDER / f"{name}.{digest}.parquet"


WHOLE = ""  # the tag of a shard done whole; any other tag is the exclusions key it was done under
DONE, FAILED = "done", "failed"  # which of a shard's records `ids` lists: the fewer of the two


def _schema():
    import pyarrow as pa

    return pa.schema([("shard", pa.uint64()), ("narrowed", pa.string()), ("n_total", pa.uint32()),
                      ("n_done", pa.uint32()), ("listed", pa.string()), ("ids", pa.list_(pa.uint64()))])


def _conform(table):
    """Any sidecar in today's columns: one of before the counts has them null (unknown)."""
    import pyarrow as pa

    schema = _schema()
    return pa.Table.from_arrays(
        [table[f.name].cast(f.type) if f.name in table.column_names else pa.nulls(table.num_rows, f.type)
         for f in schema], schema=schema)


def _table(project_db, kind: str, key: str):
    """The done set of ``(kind, key)``, a row per shard sorted by shard (empty before the first)."""
    import pyarrow.parquet as pq

    path = _file(project_db, kind, key)
    return _conform(pq.read_table(path)) if path.exists() else _schema().empty_table()


def _strings(values):
    import pyarrow as pa

    return pa.array(list(values), pa.string())


def _write(project_db, kind: str, key: str, table) -> None:
    """Written whole, fsynced and renamed into place: after the commit of what it says is done."""
    import pyarrow.parquet as pq

    from ms_flow.core.data.shard import _fsync_dir

    path = _file(project_db, kind, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "wb") as handle:
        pq.write_table(table, handle)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _union(mine, new):
    """``new`` rows replace those of ``mine`` for the same shard (the later one of ``new`` wins);
    a shard done whole stays done whole."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc

    whole = mine.filter(pc.equal(mine["narrowed"], WHOLE))["shard"]
    narrowed = pc.if_else(pc.is_in(new["shard"], value_set=whole), WHOLE, new["narrowed"])
    new = new.set_column(new.schema.get_field_index("narrowed"), "narrowed", narrowed)
    table = pa.concat_tables([mine, new])
    shards = table["shard"].to_numpy()
    _, last = np.unique(shards[::-1], return_index=True)  # the last row of each shard
    return table.take(np.sort(len(shards) - 1 - last)).sort_by("shard")


def _rows(shards: Iterable[int], narrowed_by: str, records) -> tuple[Any, int]:
    """The rows of ``shards`` done now, and how many of them failed whole (no row: they rerun)."""
    import numpy as np
    import pyarrow as pa

    records = records or {}
    out = {name: [] for name in _schema().names}
    failed_whole = 0
    for s in dict.fromkeys(int(s) for s in shards):
        if s in records:
            ok, bad = (np.unique(np.asarray(part, dtype=np.uint64)) for part in records[s])
            if len(bad) and not len(ok):
                failed_whole += 1
                continue
            listed, ids = (FAILED, bad) if len(bad) <= len(ok) else (DONE, ok)
            total, n_done = len(ok) + len(bad), len(ok)
        else:
            listed, ids, total, n_done = DONE, np.empty(0, np.uint64), None, None
        for name, value in zip(out, (s, str(narrowed_by or WHOLE), total, n_done, listed, ids)):
            out[name].append(value)
    return pa.table(out, schema=_schema()), failed_whole


def _counts(table) -> tuple[int, int]:
    """Records done and failed of the shards whose counts are known."""
    import pyarrow.compute as pc

    done_ = pc.sum(table["n_done"]).as_py() or 0
    return int(done_), int((pc.sum(table["n_total"]).as_py() or 0) - done_)


def done(project_db, kind: str, key: str, *, narrowed_by: str | None = None):
    """The shard ids already done for ``(kind, key)``, sorted: the ones done whole, plus the ones
    done while the exclusions ``narrowed_by`` cut part of them. Empty before the first one."""
    import pyarrow.compute as pc

    table = _table(project_db, kind, key)
    tags = [WHOLE] if narrowed_by is None else [WHOLE, narrowed_by]
    return table.filter(pc.is_in(table["narrowed"], value_set=_strings(tags)))["shard"].to_numpy()


def pending(project_db, kind: str, key: str, shards: Iterable[int], *,
            narrowed_by: str | None = None) -> list[int]:
    """Of ``shards``, the ones still to run: what a re-run (or a transport) has to carry. A shard
    done under other exclusions than ``narrowed_by`` (a cut since undone) runs again, whole."""
    import numpy as np

    wanted = np.fromiter((int(s) for s in shards), dtype=np.uint64)
    return [int(s) for s in np.setdiff1d(wanted, done(project_db, kind, key, narrowed_by=narrowed_by))]


def done_by_file(project_db, kind: str, key: str) -> dict[int, int]:
    """``{file id: shards done}`` of ``(kind, key)``, whatever they were done under: for a view."""
    import numpy as np

    from ms_flow.core.data.catalog import GROUP_BITS

    files, counts = np.unique(_table(project_db, kind, key)["shard"].to_numpy() >> np.uint64(GROUP_BITS),
                              return_counts=True)
    return dict(zip(files.tolist(), counts.tolist()))


def mark_done(
    project_db,
    kind: str,
    key: str,
    shards: Iterable[int],
    *,
    records: Mapping[int, tuple[Any, Any]] | None = None,
    failed: int = 0,
    total: int | None = None,
    records_total: int | None = None,
    producer_job_id: str = "",
    narrowed_by: str = WHOLE,
    generation_id: int | None = None,
) -> int:
    """Add ``shards`` to the done set of ``(kind, key)``; how many are done after it.

    ``records``: per shard, ``(done ids, failed ids)`` of its records as it ran; the sidecar keeps
    the fewer of the two. A shard whose every record failed failed whole: it is not done (it runs
    again whole) and counts in the failed shards. A shard without an entry is done with its counts
    unknown. ``narrowed_by``: the key of the exclusions that cut part of these shards when they ran
    (`Exclusions.key`); empty = done whole. A shard done whole stays so; done again, its counts are
    the new run's. ``generation_id``: the generation the ids belong to, kept by
    `activate_generation` while this state is not forgotten. ``failed`` adds to the failed
    shards; ``total`` and ``records_total``: what there is to do, in shards and in records.
    ponytail: one writer at a time — the parent process that collects results — so the file is
    read, merged and `os.replace`d, with no lock. Call it after the commit of what the shards made.
    """
    row = _state_row(project_db, kind, key)
    stored = json.loads(getattr(row, "metadata_json", "") or "{}").get("generation_id")
    # Ids of another generation mean nothing here: a key that moved on starts again (and lets
    # its old generation go).
    moved = generation_id is not None and stored is not None and int(stored) != int(generation_id)
    new, failed_whole = _rows(shards, narrowed_by, records)
    table = _union(_schema().empty_table() if moved else _table(project_db, kind, key), new)
    _write(project_db, kind, key, table)
    _record(project_db, kind, key, table, failed=failed + failed_whole, total=total,
            records_total=records_total, job_id=producer_job_id, generation_id=generation_id, reset=moved)
    return table.num_rows


def failed(project_db, kind: str, key: str, shard: int, ids: Any = None):
    """The failed records of ``shard`` under ``(kind, key)``, sorted (uint64): none for a shard not
    done (it runs again whole) or done without its counts. When the done ones are the listed
    side, ``ids`` (the records the shard ran on) gives the rest; without it, ValueError."""
    import numpy as np
    import pyarrow.compute as pc

    table = _table(project_db, kind, key)
    hit = table.filter(pc.equal(table["shard"], int(shard)))
    if not hit.num_rows or hit["n_total"][0].as_py() is None:
        return np.empty(0, np.uint64)
    listed = np.asarray(hit["ids"][0].as_py(), dtype=np.uint64)
    if hit["listed"][0].as_py() == FAILED:
        return listed
    if ids is None:
        raise ValueError(f"shard {shard} lists its done records: pass the ids it ran on")
    return np.setdiff1d(np.asarray(ids, dtype=np.uint64), listed)


def merge(project_db, kind: str, key: str, path: str | Path, *, generation_id: int | None = None) -> int:
    """Union the done set of ``(kind, key)`` with the one in the sidecar ``path`` — what another
    host did, as its own file, brought back whole; how many are done after it. A shard done whole
    anywhere stays done whole; a shard both did keeps the counts of ``path``. The file of a key
    that has none here is simply the union with nothing."""
    import pyarrow.parquet as pq

    row = _state_row(project_db, kind, key)
    stored = json.loads(getattr(row, "metadata_json", "") or "{}").get("generation_id")
    moved = generation_id is not None and stored is not None and int(stored) != int(generation_id)
    theirs = _conform(pq.read_table(path))
    table = _union(_schema().empty_table() if moved else _table(project_db, kind, key), theirs)
    _write(project_db, kind, key, table)
    _record(project_db, kind, key, table, generation_id=generation_id, reset=moved)
    return table.num_rows


def sidecar(project_db, kind: str, key: str) -> Path:
    """The file holding the done set of ``(kind, key)``: what travels to and from another host."""
    return _file(project_db, kind, key)


def forget(project_db, kind: str, key: str, shards: Iterable[int] | None = None) -> int:
    """Forget what is done so it runs again: all of ``(kind, key)``, or just ``shards``."""
    path = _file(project_db, kind, key)
    if shards is None:
        path.unlink(missing_ok=True)
        row = _state_row(project_db, kind, key)
        if row is not None:
            _registry(project_db).update_artifact(int(row.id), status=INACTIVE,
                                                  metadata={"coverage_ready": 0, "coverage_failed": 0,
                                                            "records_done": 0, "records_failed": 0})
        return 0
    import pyarrow.compute as pc

    table = _table(project_db, kind, key)
    table = table.filter(pc.invert(pc.is_in(table["shard"], value_set=_ids(shards))))
    _write(project_db, kind, key, table)
    _record(project_db, kind, key, table)
    return table.num_rows


def _ids(shards: Iterable[int]):
    import pyarrow as pa

    return pa.array([int(s) for s in shards], pa.uint64())


def progress(project_db, kind: str | None = None) -> list[dict[str, Any]]:
    """Per key, from the database alone: shards and records done, failed and in total (a total is
    None until a caller gave it). The shape `catalog.progress` gives a derived step too."""
    rows = _registry(project_db).find(entity_kind=KIND, artifact_kind=kind, role="scope_state",
                                      status=None)
    out = []
    for row in rows:
        meta = json.loads(row.metadata_json or "{}")
        out.append(summary(kind=row.artifact_kind, key=row.ref, status=row.status,
                           generation_id=meta.get("generation_id"),
                           shards_done=int(meta.get("coverage_ready") or 0),
                           shards_failed=int(meta.get("coverage_failed") or 0),
                           shards_total=meta.get("coverage_total"),
                           records_done=int(meta.get("records_done") or 0),
                           records_failed=int(meta.get("records_failed") or 0),
                           records_total=meta.get("records_total")))
    return out


def summary(*, kind: str, key: str, status: str, generation_id: int | None, shards_done: int,
            shards_failed: int, shards_total: int | None, records_done: int, records_failed: int,
            records_total: int | None) -> dict[str, Any]:
    """The one shape of a state's progress."""
    return {"kind": kind, "key": key, "status": status, "generation_id": generation_id,
            "shards_done": shards_done, "shards_failed": shards_failed, "shards_total": shards_total,
            "records_done": records_done, "records_failed": records_failed, "records_total": records_total}


def _state_row(project_db, kind: str, key: str):
    return _registry(project_db).current_scope_state(scope_kind=KIND, snapshot_ref=key,
                                                     state_kind=kind)


def _record(project_db, kind: str, key: str, table, *, failed: int = 0,
            total: int | None = None, records_total: int | None = None, job_id: str = "",
            generation_id: int | None = None, reset: bool = False) -> None:
    """The one database row of this key, upserted: the shards of ``table`` (the done set), the
    records its counts say (recomputed, never added to), the failed shards added to."""
    row = _state_row(project_db, kind, key)
    meta = json.loads(getattr(row, "metadata_json", "") or "{}")
    if total is None:
        total = meta.get("coverage_total")
    if records_total is None:
        records_total = meta.get("records_total")
    failed = (0 if reset else int(meta.get("coverage_failed") or 0)) + int(failed)
    if generation_id is None:
        generation_id = meta.get("generation_id")
    records_done, records_failed = _counts(table)
    _registry(project_db).record_scope_state(
        scope_kind=KIND, snapshot_ref=key, state_kind=kind,
        status=COMPLETE if total is not None and table.num_rows >= int(total) else PARTIAL,
        producer_job_id=job_id, coverage_total=total, coverage_ready=table.num_rows,
        coverage_failed=failed, metadata={"generation_id": generation_id, "records_done": records_done,
                                          "records_failed": records_failed, "records_total": records_total},
    )


__all__ = ["DONE", "FAILED", "WHOLE", "done", "done_by_file", "failed", "forget", "mark_done", "merge",
           "pending", "progress", "sidecar", "summary"]
