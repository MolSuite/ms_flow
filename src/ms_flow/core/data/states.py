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
from typing import Any, Iterable

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


def _read(project_db, kind: str, key: str) -> dict[int, str]:
    """``{shard id: tag}`` of ``(kind, key)``: ``WHOLE``, or the exclusions key a shard was done under."""
    import pyarrow.parquet as pq

    path = _file(project_db, kind, key)
    if not path.exists():
        return {}
    table = pq.read_table(path)
    return dict(zip(table["shard"].to_pylist(), table["narrowed"].to_pylist()))


def _write(project_db, kind: str, key: str, state: dict[int, str]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = _file(project_db, kind, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    ids = sorted(state)
    tmp = path.with_suffix(".tmp")
    pq.write_table(pa.table({"shard": pa.array(ids, pa.uint64()),
                             "narrowed": pa.array([state[i] for i in ids], pa.string())}), tmp)
    os.replace(tmp, path)


def done(project_db, kind: str, key: str, *, narrowed_by: str | None = None):
    """The shard ids already done for ``(kind, key)``, sorted: the ones done whole, plus the ones
    done while the exclusions ``narrowed_by`` cut part of them. Empty before the first one."""
    import numpy as np

    ids = [s for s, tag in _read(project_db, kind, key).items() if tag == WHOLE or tag == narrowed_by]
    return np.array(sorted(ids), dtype=np.uint64)


def pending(project_db, kind: str, key: str, shards: Iterable[int], *,
            narrowed_by: str | None = None) -> list[int]:
    """Of ``shards``, the ones still to run: what a re-run (or a transport) has to carry. A shard
    done under other exclusions than ``narrowed_by`` (a cut since undone) runs again, whole."""
    import numpy as np

    wanted = np.fromiter((int(s) for s in shards), dtype=np.uint64)
    return [int(s) for s in np.setdiff1d(wanted, done(project_db, kind, key, narrowed_by=narrowed_by))]


def done_by_file(project_db, kind: str, key: str) -> dict[int, int]:
    """``{file id: shards done}`` of ``(kind, key)``, whatever they were done under: for a view."""
    from collections import Counter

    from ms_flow.core.data.catalog import GROUP_BITS

    return dict(Counter(int(s) >> GROUP_BITS for s in _read(project_db, kind, key)))


def mark_done(
    project_db,
    kind: str,
    key: str,
    shards: Iterable[int],
    *,
    failed: int = 0,
    total: int | None = None,
    producer_job_id: str = "",
    narrowed_by: str = WHOLE,
    generation_id: int | None = None,
) -> int:
    """Add ``shards`` to the done set of ``(kind, key)``; how many are done after it.

    ``narrowed_by``: the key of the exclusions that cut part of these shards when they ran
    (`Exclusions.key`); empty = done whole. A shard done whole stays so. ``generation_id``: the
    generation the ids belong to, kept by `activate_generation` while this state is not
    forgotten. ``failed`` adds to the failed count of the fraction (a shard that failed is not
    done, so it comes back as `pending`). ponytail: one writer at a time — the parent process
    that collects results — so the file is read, merged and `os.replace`d, with no lock.
    """
    row = _state_row(project_db, kind, key)
    stored = json.loads(getattr(row, "metadata_json", "") or "{}").get("generation_id")
    # Ids of another generation mean nothing here: a key that moved on starts again (and lets
    # its old generation go).
    moved = generation_id is not None and stored is not None and int(stored) != int(generation_id)
    state = {} if moved else _read(project_db, kind, key)
    for s in shards:
        if state.get(int(s)) != WHOLE:
            state[int(s)] = str(narrowed_by or WHOLE)
    _write(project_db, kind, key, state)
    _record(project_db, kind, key, len(state), failed=failed, total=total, job_id=producer_job_id,
            generation_id=generation_id, reset=moved)
    return len(state)


def merge(project_db, kind: str, key: str, path: str | Path, *, generation_id: int | None = None) -> int:
    """Union the done set of ``(kind, key)`` with the one in the sidecar ``path`` — what another
    host did, as its own file, brought back whole; how many are done after it. A shard done whole
    anywhere stays done whole. The file of a key that has none here is simply the union with
    nothing."""
    import pyarrow.parquet as pq

    table = pq.read_table(path)
    theirs = dict(zip(table["shard"].to_pylist(), table["narrowed"].to_pylist()))
    whole = [s for s, tag in theirs.items() if tag == WHOLE]
    total = mark_done(project_db, kind, key, whole, generation_id=generation_id)
    for tag in {tag for tag in theirs.values() if tag != WHOLE}:
        total = mark_done(project_db, kind, key, [s for s, t in theirs.items() if t == tag],
                          narrowed_by=tag, generation_id=generation_id)
    return total


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
                                                  metadata={"coverage_ready": 0, "coverage_failed": 0})
        return 0
    state = _read(project_db, kind, key)
    for s in shards:
        state.pop(int(s), None)
    _write(project_db, kind, key, state)
    _record(project_db, kind, key, len(state))
    return len(state)


def progress(project_db, kind: str | None = None) -> list[dict[str, Any]]:
    """The fraction per key, from the database alone: ``done`` of ``total``, and ``failed``."""
    rows = _registry(project_db).find(entity_kind=KIND, artifact_kind=kind, role="scope_state",
                                      status=None)
    out = []
    for row in rows:
        meta = json.loads(row.metadata_json or "{}")
        out.append({"kind": row.artifact_kind, "key": row.ref, "status": row.status,
                    "done": int(meta.get("coverage_ready") or 0),
                    "failed": int(meta.get("coverage_failed") or 0),
                    "total": meta.get("coverage_total"),
                    "generation_id": meta.get("generation_id")})
    return out


def _state_row(project_db, kind: str, key: str):
    return _registry(project_db).current_scope_state(scope_kind=KIND, snapshot_ref=key,
                                                     state_kind=kind)


def _record(project_db, kind: str, key: str, n_done: int, *, failed: int = 0,
            total: int | None = None, job_id: str = "", generation_id: int | None = None,
            reset: bool = False) -> None:
    """The one database row of this key: the fraction, upserted."""
    row = _state_row(project_db, kind, key)
    meta = json.loads(getattr(row, "metadata_json", "") or "{}")
    if total is None:
        total = meta.get("coverage_total")
    failed = (0 if reset else int(meta.get("coverage_failed") or 0)) + int(failed)
    if generation_id is None:
        generation_id = meta.get("generation_id")
    _registry(project_db).record_scope_state(
        scope_kind=KIND, snapshot_ref=key, state_kind=kind,
        status=COMPLETE if total is not None and int(n_done) >= int(total) else PARTIAL,
        producer_job_id=job_id, coverage_total=total, coverage_ready=int(n_done),
        coverage_failed=failed, metadata={"generation_id": generation_id},
    )


__all__ = ["WHOLE", "done", "done_by_file", "forget", "mark_done", "merge", "pending", "progress",
           "sidecar"]
