"""The catalog of a sharded library: which shards there are, minus what is excluded.

The app writes records and says who is in or out; ms_flow keeps where they are. A row of
`ms_shards` is one logical shard, `(path, [base_id, base_id + slot_count))`; reads come pinned
to the active generation and narrowed by the active exclusions (`exclusions.py`), so a caller
never filters either itself.

A *generation* is one rewrite of the whole library. A step that renumbers records writes a new
one that nothing reads until `activate_generation` flips one flag, so a job that dies halfway
leaves an inert generation, not a library that is half the old one and half the new.
Activating forgets the grandparent, rows and files: the current state and the one it came from
are what a redo needs, and keeping every generation keeps the library once per step.
"""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from ms_flow.query import QuerySpec, db_count, db_pages

TABLE = "ms_shards"
FIELDS = ("id", "generation_id", "scope", "shard_index", "path", "base_id", "slot_count", "n_records")


def shard_spec(**filters: Any) -> QuerySpec:
    """Shards matching ``filters`` (``scope=``, ``generation_id=`` ...); unpinned until read."""
    return QuerySpec(table=TABLE, fields=FIELDS, filters=dict(filters), order=("id",))


def active_generation_id(project_db) -> int | None:
    """Which rewrite of the library is the current one. None before the first one."""
    from sqlmodel import select

    from ms_flow.core.database.project_models import ShardGeneration

    with project_db.get_session() as session:
        row = session.exec(
            select(ShardGeneration.id)
            .where(ShardGeneration.is_active == True)  # noqa: E712 - SQL, not Python truthiness
            .order_by(ShardGeneration.id.desc())
        ).first()
        return int(row) if row is not None else None


def create_generation(project_db, *, step: str, folder: str | Path = "", job_id: str = "") -> int:
    """Open a generation parented to the active one. Not active: nothing reads it yet."""
    from ms_flow.core.database.project_models import ShardGeneration

    parent_id = active_generation_id(project_db)
    with project_db.get_session() as session:
        row = ShardGeneration(parent_id=parent_id, step=str(step or ""), folder=str(folder or ""),
                              job_id=str(job_id or ""))
        session.add(row)
        session.commit()
        session.refresh(row)
        return int(row.id)


def activate_generation(project_db, generation_id: int, *, on_drop=None) -> None:
    """Make one generation the library and forget the ones outside the window, rows and files.

    ``on_drop(session, shards)``: the app's turn to clear what it keeps per shard (a prepared
    twin...) before a generation goes, in the same transaction; it returns the paths of files
    of its own to delete with the shards'. A generation the app still references through a
    foreign key (docking receipts...) is kept as it is: what was computed on it stays readable,
    and the next activation tries again once that reference is gone.
    """
    from sqlalchemy.exc import IntegrityError
    from sqlmodel import select

    from ms_flow.core.database.project_models import CatalogShard, ShardGeneration

    paths: set[str] = set()
    with project_db.get_session() as session:
        generations = {int(row.id): row for row in session.exec(select(ShardGeneration)).all()}
        target = generations.get(int(generation_id))
        if target is None:
            raise ValueError(f"No shard generation {generation_id} to activate.")
        for row in generations.values():
            row.is_active = row.id == target.id
            session.add(row)
        keep = {int(target.id)} | ({int(target.parent_id)} if target.parent_id is not None else set())
        doomed = [gen_id for gen_id in generations if gen_id not in keep]
        for row in generations.values():
            # Its parent may go: cut the link first, or that delete trips the foreign key.
            if row.parent_id is not None and int(row.parent_id) in doomed:
                row.parent_id = None
                session.add(row)
        session.flush()
        dropped: set[str] = set()
        for gen_id in doomed:
            shards = list(session.exec(select(CatalogShard).where(CatalogShard.generation_id == gen_id)).all())
            savepoint = session.begin_nested()
            try:
                extra = list(on_drop(session, shards) or ()) if on_drop is not None and shards else []
                for shard in shards:
                    session.delete(shard)
                session.delete(generations[gen_id])
                session.flush()
            except IntegrityError:
                savepoint.rollback()  # still referenced: kept, retried at the next activation
                continue
            savepoint.commit()
            dropped |= {str(shard.path or "") for shard in shards} | {str(path) for path in extra}
        # A file holds many shards: it goes only when no surviving row points at it.
        paths = dropped - set(session.exec(select(CatalogShard.path)).all())
        session.commit()
    # Files last: a row that survives a missing file is a bug you can see, a file that survives
    # its row is an orphan nothing will ever look at again.
    for text in paths:
        if text:
            Path(text).unlink(missing_ok=True)


def add_shards(project_db, rows: Iterable[Mapping[str, Any]], *, generation_id: int | None) -> None:
    """Register shards already on disk (never before their file is closed) in one commit.
    A row: ``scope, shard_index, path, base_id, slot_count, n_records, error``."""
    from ms_flow.core.database.project_models import CatalogShard

    columns = set(CatalogShard.model_fields) - {"id", "generation_id"}
    with project_db.get_session() as session:
        session.add_all(
            CatalogShard(generation_id=generation_id, **{k: v for k, v in dict(row).items() if k in columns})
            for row in rows
        )
        session.commit()


def compact(project_db, generation_id: int) -> int:
    """The whole-file shards of a generation (a worker's file per task) merged into big files,
    one row group each, a file per scope and folder up to `MAX_RECORDS` ids. Returns how many
    files went. For the parent once a step is done, before anything derives from its paths
    (a twin named after a shard's file would be left behind).

    Crash-safe in order: the big file is complete before a row points at it, and the small
    files go only after the rows moved.
    """
    import json
    from uuid import uuid4

    import pyarrow.parquet as pq
    from sqlmodel import select

    from ms_flow.core.data.shard import MAX_RECORDS, METADATA_KEY, merge_shards
    from ms_flow.core.database.project_models import CatalogShard

    with project_db.get_session() as session:
        rows = list(session.exec(select(CatalogShard).where(
            CatalogShard.generation_id == int(generation_id), CatalogShard.base_id == None,  # noqa: E711 - SQL
        )).all())
    groups: dict[tuple[Any, str], list[tuple[int, int, Any]]] = {}
    for row in rows:
        path = Path(str(row.path))
        if not path.is_file():
            continue  # ponytail: left as it is; a missing file is the reader's error to raise
        header = json.loads(pq.read_metadata(path).metadata[METADATA_KEY])
        groups.setdefault((row.scope, str(path.parent)), []).append(
            (int(header["base_id"]), int(header["slot_count"]), row))
    moved: dict[int, tuple[str, dict[str, int]]] = {}
    for (_scope, folder), members in groups.items():
        members.sort(key=lambda item: item[0])
        files: list[list[tuple[int, int, Any]]] = []
        for member in members:  # a new file when the id span would pass what one file holds
            if not files or member[0] + member[1] - files[-1][0][0] > MAX_RECORDS:
                files.append([])
            files[-1].append(member)
        for batch in files:
            if len(batch) < 2:
                continue  # one file already
            dest = Path(folder) / f"merged_{uuid4().hex[:12]}{Path(str(batch[0][2].path)).suffix}"
            ranges = merge_shards([(row.path, None) for _base, _slots, row in batch], dest)
            moved.update({int(row.id): (str(dest), span) for (_b, _s, row), span in zip(batch, ranges)})
    if not moved:
        return 0
    with project_db.get_session() as session:
        old = set()
        for row in session.exec(select(CatalogShard).where(CatalogShard.id.in_(list(moved)))).all():
            dest, span = moved[int(row.id)]
            old.add(str(row.path))
            row.path, row.base_id, row.slot_count = dest, span["base_id"], span["slot_count"]
            session.add(row)
        session.commit()
    for text in old:
        Path(text).unlink(missing_ok=True)
    return len(old)


def _pinned(project_db, spec: QuerySpec | None) -> QuerySpec:
    """Any shard query, on the active generation. A spec that names one is left alone: that is
    how a redo reads its parent, and how a step counts what it has written so far."""
    spec = spec or shard_spec()
    if "generation_id" in spec.filters:
        return spec
    generation_id = active_generation_id(project_db)
    if generation_id is None:
        return spec
    return replace(spec, filters={**spec.filters, "generation_id": generation_id})


def shards(
    project_db, spec: QuerySpec | None = None, *, whole: bool = False, page_size: int = 1000
) -> Iterator[dict[str, Any]]:
    """Shard rows, minus the active exclusions: a shard left with nothing is not yielded, a
    partial one carries ``record_ids``. ``whole``: every record, for a step that transforms the
    library rather than selects from it (so undoing a cut needs no redo of it)."""
    from ms_flow.core.data.exclusions import active_exclusions

    rows = db_pages(project_db, _pinned(project_db, spec), page_size=max(1, int(page_size)))
    yield from rows if whole else active_exclusions(project_db).narrow(rows)


def count(project_db, spec: QuerySpec | None = None, *, whole: bool = False) -> int:
    """How many shards `shards` yields: what a job declares as its work."""
    from ms_flow.core.data.exclusions import active_exclusions

    if whole or not active_exclusions(project_db):
        return db_count(project_db, _pinned(project_db, spec))
    return sum(1 for _row in shards(project_db, spec))


def record_count(project_db, spec: QuerySpec | None = None, *, whole: bool = False) -> int:
    """How many records those shards hold, without opening a file.

    ponytail: summed over the shard rows, not in SQL. Thousands of rows at worst; a SUM query
    if a library ever reaches millions of shards.
    """
    return sum(int(row.get("n_records") or 0) for row in shards(project_db, spec, whole=whole))


__all__ = [
    "FIELDS", "TABLE", "activate_generation", "active_generation_id", "add_shards", "compact", "count",
    "create_generation", "record_count", "shard_spec", "shards",
]
