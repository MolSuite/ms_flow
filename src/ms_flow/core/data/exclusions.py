"""Records left out of a sharded dataset without rewriting it.

A Parquet shard is immutable (its footer holds byte offsets), so a cut is a sidecar: the ids it
removes, one file per cut, registered as a `project_artifacts` row. What is out is the union of
the active cuts; undoing one flips its status. The idea of Iceberg v2 position deletes and Delta
deletion vectors, without the dependency.

An id is unique inside its *scope*: whatever the app numbers records within (one import, one upload...).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping
from uuid import uuid4

KIND = "exclusion"
ACTIVE, INACTIVE = "active", "inactive"
FOLDER = "exclusions"


def _root(project_db) -> Path:
    from ms_flow.core.database import resolve_project_store

    db_path = resolve_project_store(project_db).db_path
    if db_path is None:
        raise RuntimeError("exclusions need a project on disk")
    return Path(db_path).parent


def _registry(project_db):
    from ms_flow.artifacts import ArtifactRegistry

    return ArtifactRegistry(project_db)


def exclude(
    project_db,
    keys: Mapping[int, Iterable[int]],
    *,
    reason: str = "",
    producer_job_id: str = "",
) -> int | None:
    """Leave out ``{scope: ids}``; the artifact id of the cut, or None when it is empty."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    scopes, ids = [], []
    for scope, values in sorted(keys.items()):
        kept = np.unique(np.fromiter((int(v) for v in values), dtype=np.uint64))
        scopes.append(np.full(len(kept), int(scope), dtype=np.uint64))
        ids.append(kept)
    n = int(sum(len(a) for a in ids))
    if not n:
        return None
    root = _root(project_db)
    rel = Path(FOLDER) / f"{uuid4().hex}.parquet"
    (root / FOLDER).mkdir(parents=True, exist_ok=True)
    tmp = root / rel.with_suffix(".tmp")
    pq.write_table(pa.table({"scope": np.concatenate(scopes), "id": np.concatenate(ids)}), tmp)
    os.replace(tmp, root / rel)
    row = _registry(project_db).register(
        entity_kind=KIND, artifact_kind=KIND, path=str(rel), format="parquet", status=ACTIVE,
        producer_job_id=producer_job_id, metadata={"reason": reason, "n": n, "scopes": len(keys)},
    )
    _refresh(project_db)
    return int(row.id)


def _refresh(project_db) -> None:
    """The catalog's active counts follow the cuts (one footer read per file)."""
    from ms_flow.core.data.catalog import refresh_active

    refresh_active(project_db)


def set_exclusion_active(project_db, artifact_id: int, active: bool) -> None:
    """Undo (or redo) one cut; its file stays, so this is free both ways."""
    _registry(project_db).update_artifact(int(artifact_id), status=ACTIVE if active else INACTIVE)
    _refresh(project_db)


def list_exclusions(project_db) -> list[dict[str, Any]]:
    """Every cut, newest first: what a UI lists with an undo beside each."""
    return [
        {"id": int(row.id), "active": row.status == ACTIVE, "producer_job_id": row.producer_job_id,
         **json.loads(row.metadata_json or "{}")}
        for row in _registry(project_db).find(entity_kind=KIND, status=None)
    ]


class Exclusions:
    """The union of the active cuts: one sorted id array per scope."""

    def __init__(self, by_scope: Mapping[int, Any] | None = None, *, cuts: Iterable[int] = ()):
        self.by_scope = dict(by_scope or {})
        # Which cuts are in it: the identity of this state, for a caller that keys work by it
        # (a step's receipt must not count as "the whole shard" if a cut hid part of it).
        self.key = ".".join(str(int(cut)) for cut in sorted(cuts))

    def __bool__(self) -> bool:
        return bool(self.by_scope)

    def __len__(self) -> int:
        return int(sum(len(a) for a in self.by_scope.values()))

    def in_range(self, scope: int, lo: int, hi: int):
        """The excluded ids of ``scope`` in ``lo <= id < hi``, sorted."""
        import numpy as np

        ids = self.by_scope.get(int(scope))
        if ids is None:
            return np.empty(0, dtype=np.uint64)
        return ids[np.searchsorted(ids, lo):np.searchsorted(ids, hi)]

    def narrow(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        scope_key: str = "scope",
        path_key: str = "path",
        count_key: str = "n_records",
    ) -> Iterator[dict[str, Any]]:
        """Shard rows minus what is excluded: a fully excluded shard is dropped, a partial one
        gets ``record_ids`` (what to read of it) and its count lowered. A row that already has
        ``record_ids`` (a Top-N) keeps those that are not excluded.

        A row's ids are ``base_id <= id < base_id + slot_count``; without them (a file per
        shard) the file's own header says, read from its footer only.
        """
        for row in rows:
            row = dict(row)
            if not self.by_scope or row.get(scope_key) is None or int(row[scope_key]) not in self.by_scope:
                yield row
                continue
            lo, hi = _id_range(row, path_key)
            out = self.in_range(int(row[scope_key]), lo, hi)
            if not len(out):
                yield row
                continue
            gone = set(out.tolist())
            if row.get("record_ids") is not None:
                kept = [int(i) for i in row["record_ids"] if int(i) not in gone]
                count = len(kept)
            else:
                # Every slot, not the records: where the holes (a record that failed the import)
                # are is in the file, and asking for a hole reads as nothing. The count is the
                # one approximation — a cut naming a hole lowers it by one too many.
                kept = [i for i in range(lo, hi) if i not in gone]
                count = max(0, int(row.get(count_key) or 0) - len(out))
            if kept and count:
                row["record_ids"], row[count_key] = kept, count
                yield row


def _id_range(row: Mapping[str, Any], path_key: str) -> tuple[int, int]:
    if row.get("base_id") is not None and row.get("slot_count") is not None:
        return int(row["base_id"]), int(row["base_id"]) + int(row["slot_count"])
    from ms_flow.core.data.shard import METADATA_KEY, _footer

    meta, _spans, _header = _footer(Path(str(row[path_key])))
    header = json.loads((meta.metadata or {})[METADATA_KEY].decode("utf-8"))
    return int(header["base_id"]), int(header["base_id"]) + int(header["slot_count"])


def active_exclusions(project_db) -> Exclusions:
    """What is out now: the union of every active cut, read once per call."""
    import numpy as np
    import pyarrow.parquet as pq

    rows = _registry(project_db).find(entity_kind=KIND, status=ACTIVE)
    if not rows:
        return Exclusions()
    root = _root(project_db)
    parts: dict[int, list] = {}
    for row in rows:
        table = pq.read_table(root / row.path)
        scope, ids = table["scope"].to_numpy(), table["id"].to_numpy()
        for value in np.unique(scope):
            parts.setdefault(int(value), []).append(ids[scope == value])
    # ponytail: the whole union in memory (8 bytes an id: 650k ids = 5 MB). Compact the cut
    # files into one when they pile up; stream per scope if a union reaches hundreds of millions.
    return Exclusions({scope: np.unique(np.concatenate(arrays)) for scope, arrays in parts.items()},
                      cuts=[int(row.id) for row in rows])


__all__ = ["Exclusions", "active_exclusions", "exclude", "list_exclusions", "set_exclusion_active"]
