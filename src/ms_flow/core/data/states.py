"""What is already done of a sharded library, per shard, without a row per shard.

A shard is the unit of work and of state: 256 records prepared, docked or not. Which shards are
done is a sidecar Parquet of their ids — one file per ``(kind, key)``, grown in place — and not
a table, so a library of 100M records costs a few hundred files of a few MB instead of 400k
rows. The database keeps one upserted row per key with the fraction (`progress()`), which is
what a UI shows; the ids are read only when something needs the difference.

Per shard, never per record: a record that fails is isolated inside its shard (a dead molecule
must not kill a Uni-Dock task), and a shard that fails is simply repeated whole.

Comparing two libraries is then comparing two of these files: what the remote has done, what
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


def done(project_db, kind: str, key: str):
    """The shard ids already done for ``(kind, key)``, sorted. Empty before the first one."""
    import numpy as np
    import pyarrow.parquet as pq

    path = _file(project_db, kind, key)
    if not path.exists():
        return np.empty(0, dtype=np.uint64)
    return pq.read_table(path)["shard"].to_numpy()


def pending(project_db, kind: str, key: str, shards: Iterable[int]) -> list[int]:
    """Of ``shards``, the ones still to run: what a re-run (or a transport) has to carry."""
    import numpy as np

    wanted = np.fromiter((int(s) for s in shards), dtype=np.uint64)
    return [int(s) for s in np.setdiff1d(wanted, done(project_db, kind, key))]


def mark_done(
    project_db,
    kind: str,
    key: str,
    shards: Iterable[int],
    *,
    failed: int = 0,
    total: int | None = None,
    producer_job_id: str = "",
) -> int:
    """Add ``shards`` to the done set of ``(kind, key)``; how many are done after it.

    ``failed`` adds to the failed count of the fraction (a shard that failed is not done, so it
    comes back as `pending`). ponytail: one writer at a time — the parent process that collects
    receipts — so the file is read, unioned and `os.replace`d, with no lock.
    """
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = _file(project_db, kind, key)
    added = np.fromiter((int(s) for s in shards), dtype=np.uint64)
    merged = np.union1d(done(project_db, kind, key), added) if len(added) else done(project_db, kind, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    pq.write_table(pa.table({"shard": merged}), tmp)
    os.replace(tmp, path)
    _record(project_db, kind, key, len(merged), failed=failed, total=total, job_id=producer_job_id)
    return int(len(merged))


def forget(project_db, kind: str, key: str, shards: Iterable[int] | None = None) -> int:
    """Forget what is done so it runs again: all of ``(kind, key)``, or just ``shards``."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = _file(project_db, kind, key)
    if shards is None:
        path.unlink(missing_ok=True)
        row = _state_row(project_db, kind, key)
        if row is not None:
            _registry(project_db).update_artifact(int(row.id), status=INACTIVE,
                                                  metadata={"coverage_ready": 0, "coverage_failed": 0})
        return 0
    kept = np.setdiff1d(done(project_db, kind, key),
                        np.fromiter((int(s) for s in shards), dtype=np.uint64))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    pq.write_table(pa.table({"shard": kept}), tmp)
    os.replace(tmp, path)
    _record(project_db, kind, key, len(kept))
    return int(len(kept))


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
                    "total": meta.get("coverage_total")})
    return out


def _state_row(project_db, kind: str, key: str):
    return _registry(project_db).current_scope_state(scope_kind=KIND, snapshot_ref=key,
                                                     state_kind=kind)


def _record(project_db, kind: str, key: str, n_done: int, *, failed: int = 0,
            total: int | None = None, job_id: str = "") -> None:
    """The one database row of this key: the fraction, upserted."""
    row = _state_row(project_db, kind, key)
    meta = json.loads(getattr(row, "metadata_json", "") or "{}")
    if total is None:
        total = meta.get("coverage_total")
    failed = int(meta.get("coverage_failed") or 0) + int(failed)
    _registry(project_db).record_scope_state(
        scope_kind=KIND, snapshot_ref=key, state_kind=kind,
        status=COMPLETE if total is not None and int(n_done) >= int(total) else PARTIAL,
        producer_job_id=job_id, coverage_total=total, coverage_ready=int(n_done),
        coverage_failed=failed,
    )


__all__ = ["done", "forget", "mark_done", "pending", "progress"]
