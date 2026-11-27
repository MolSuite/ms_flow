"""Repack (M12b, D2-A, D4): after a strong cut, the active generation's records into a new one of
fuller shards, every record id kept, and the old one kept.

    reason = offer(db)                  # None, or why not (a later step works on it)
    new = start(db, folder)             # at submit; `elements(db, new, records=256)` in the job
    receipts = [part(e) for e in elements]   # by workers: the records re-encoded as parts (I12)
    ... committed into the new generation (`sinks.catalog_sink` / `catalog.ContainerWriter`)
    activate(db, new)                   # the previous one `kept`, in the same transaction

`go_back` activates a kept one and deletes nothing; `delete` is the explicit action, refused while
a state names the generation. Neighbouring shards only: same scope, in id order, a new shard
spanning at most `max_slots` ids.
"""
from __future__ import annotations

import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from ms_flow.core.data import catalog

MAX_SLOTS = 65536  # ids one repacked shard may span


def _named(project_db, generation_id: int) -> list[dict]:
    from ms_flow.core.data.states import INACTIVE, progress

    return [row for row in progress(project_db)
            if row["generation_id"] == generation_id and row["status"] != INACTIVE]


def offer(project_db) -> str | None:
    """None when the active generation can be repacked (or left); else why not: a later step works
    on it (a state, or files it made), and its ids would change under it (M12c lifts this)."""
    from sqlmodel import select

    from ms_flow.core.database.project_models import CatalogFile

    active = catalog.active_generation_id(project_db)
    if active is None:
        return "there is no library yet"
    named = _named(project_db, active)
    if named:
        return f"a later step keeps a state on it ({named[0]['kind']} {named[0]['key']})"
    with project_db.get_session() as session:
        role = session.exec(select(CatalogFile.role).where(CatalogFile.generation_id == active,
                                                           CatalogFile.role != catalog.SOURCE)).first()
    if role is not None:
        return f"a later step made files on it ({role})"
    return None


def start(project_db, folder) -> int:
    """The new generation a repack fills (parented to the active one, not active yet), its parts
    under `folder`. At submit: what a resumed job rebuilds its elements from (trap 17)."""
    reason = offer(project_db)
    if reason:
        raise ValueError(f"Not repacked: {reason}.")
    return catalog.create_generation(project_db, step="repack", folder=str(folder))


def elements(project_db, generation_id: int, *, records: int, max_slots: int = MAX_SLOTS) -> list[dict]:
    """What fills `generation_id` (from `start`), the same every time it is asked: runs of
    neighbouring shards of its parent (same scope, id order) holding at most `records` usable
    records and spanning at most `max_slots` ids, each with the ids its cuts leave. A shard the
    cuts empty goes."""
    from ms_flow.core.database.project_models import ShardGeneration

    with project_db.get_session() as session:
        made = session.get(ShardGeneration, int(generation_id))
    if made is None or made.parent_id is None:
        raise ValueError(f"No repack generation {generation_id} to fill.")
    spec = catalog.shard_spec(generation_id=int(made.parent_id))
    shards = sorted(catalog.shards(project_db, spec), key=lambda row: (row["scope"] or 0, row["base_id"]))
    runs: list[list[dict]] = []
    used, scope = 0, object()
    for row in shards:  # already narrowed by the cuts: `n_records` usable, `record_ids` which
        usable = int(row["n_records"])
        if usable <= 0:
            continue
        end = row["base_id"] + row["slot_count"]
        if (not runs or row["scope"] != scope or used + usable > records
                or end - runs[-1][0]["base_id"] > max_slots):
            runs.append([])
            used, scope = 0, row["scope"]
        runs[-1].append({"path": row["path"], "shard": row["shard_index"], "base_id": row["base_id"],
                         "slot_count": row["slot_count"], "scope": row["scope"],
                         "keep": None if row.get("record_ids") is None else [int(i) for i in row["record_ids"]]})
        used += usable
    out = str(Path(made.folder) / "parts")
    return [{"shards": run, "scope": run[0]["scope"], "base_id": run[0]["base_id"],
             "slot_count": run[-1]["base_id"] + run[-1]["slot_count"] - run[0]["base_id"], "out": out,
             "part": f"repack:{generation_id}:{run[0]['scope']}:{run[0]['base_id']}", "generation_id": generation_id}
            for run in runs]


def plan(project_db, folder, *, records: int, max_slots: int = MAX_SLOTS) -> tuple[int, list[dict]]:
    """`start` and its `elements`, at once."""
    new = start(project_db, folder)
    return new, elements(project_db, new, records=records, max_slots=max_slots)


def part(element: dict) -> dict:
    """A worker's task: the records of `element`'s shards that no cut leaves out (drops too), as
    one part spanning its ids, ids kept; its receipt (`catalog.file_row`)."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.ipc as ipc

    from ms_flow.core.data import container
    from ms_flow.core.data.shard import SUFFIX, PartWriter, _index

    out = Path(element["out"]) / f"{element['scope']}-{element['base_id']}-{uuid.uuid4().hex[:8]}{SUFFIX}"
    out.parent.mkdir(parents=True, exist_ok=True)
    first = element["shards"][0]
    _info, _rows, header, _groups = _index(Path(first["path"]))
    like = SimpleNamespace(base_id=element["base_id"], slot_count=element["slot_count"])
    with PartWriter(out, kind=int(header["kind"]), serializer=int(header["serializer"]), like=like,
                    scope=element["scope"], meta=dict(header.get("metadata") or {})) as writer:
        for shard in element["shards"]:
            info, rows, _header, _groups = _index(Path(shard["path"]))
            names = set(ipc.read_schema(ipc.read_message(container.read_head(shard["path"], info))).names)
            columns = ["id", "record", *[name for name in ("source_index", "reason") if name in names]]
            table = container.read_table(shard["path"], info, [rows[shard["shard"]]], columns)
            if shard["keep"] is not None:  # what the cuts leave; a drop stays a drop
                kept = pa.array(np.asarray(shard["keep"], dtype=np.uint64)).cast(table["id"].type)
                table = table.filter(pc.or_(pc.is_in(table["id"], value_set=kept), pc.is_null(table["record"])))
            ids = table["id"].to_pylist()
            data = table["record"].cast(pa.binary()).to_pylist()  # as `Shard.open` reads it
            sources = table["source_index"].to_pylist() if "source_index" in columns else [None] * len(ids)
            reasons = table["reason"].to_pylist() if "reason" in columns else [None] * len(ids)
            for id_, blob, source, reason in zip(ids, data, sources, reasons):
                if blob is None:
                    writer.drop(id_, reason or "")
                else:
                    writer.add(id_, blob, source)
    return catalog.file_row(out, role=catalog.SOURCE, generation_id=element["generation_id"], part=element["part"])


def activate(project_db, generation_id: int) -> None:
    """The repacked generation is the dataset now; the one it was made from is kept (D4)."""
    catalog.activate_generation(project_db, generation_id, keep_previous=True)


def go_back(project_db, generation_id: int) -> None:
    """Back to a kept generation: the one left is kept too, and nothing is deleted. Refused while
    a later step works on the one left: its states are per key, and working on under the same key
    there would lose them (M12c lifts this)."""
    reason = offer(project_db)
    if reason:
        raise ValueError(f"Not gone back: {reason}.")
    catalog.activate_generation(project_db, generation_id, keep_previous=True, drop=False)


def delete(project_db, generation_id: int) -> None:
    """A generation and its files gone, by hand: refused for the active one and while a state names
    it. Undoing a cut made before a repack needs the generation before it: this takes that away."""
    from sqlmodel import select

    from ms_flow.core.database.project_models import CatalogFile, ShardGeneration

    if generation_id == catalog.active_generation_id(project_db):
        raise ValueError("The active generation is not deleted: go back to another one first.")
    named = _named(project_db, generation_id)
    if named:
        raise ValueError(f"A state names generation {generation_id} ({named[0]['kind']} {named[0]['key']}).")
    with project_db.get_session() as session:
        files = list(session.exec(select(CatalogFile).where(CatalogFile.generation_id == generation_id)).all())
        for child in session.exec(select(ShardGeneration).where(ShardGeneration.parent_id == generation_id)).all():
            child.parent_id = None
            session.add(child)
        paths = {str(row.path) for row in files}
        locks = {str(catalog._lock_path(Path(row.path).parent, row.role, generation_id)) for row in files}
        for row in files:
            session.delete(row)
        row = session.get(ShardGeneration, int(generation_id))
        if row is not None:
            session.delete(row)
        session.commit()
        still = set(session.exec(select(CatalogFile.path)).all())
    for path in (paths - still) | locks:  # files last: a row never names a file that went
        catalog._unlink(path)


__all__ = ["MAX_SLOTS", "activate", "delete", "elements", "go_back", "offer", "part", "plan", "start"]
